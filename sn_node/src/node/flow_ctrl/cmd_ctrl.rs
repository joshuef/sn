// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::{
        cmds::{Cmd, CmdJob},
        dispatcher::Dispatcher,
        event_channel::EventSender,
    },
    CmdProcessEvent, Error, Event, RateLimits, Result,
};

use custom_debug::Debug;
use priority_queue::PriorityQueue;
use std::time::SystemTime;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::RwLock, time::Instant};

type Priority = i32;

const MAX_RETRIES: usize = 1; // drops on first error..
const SLEEP_TIME: Duration = Duration::from_millis(20);

const ORDER: Ordering = Ordering::SeqCst;

/// A module for enhanced flow control.
///
/// Orders the incoming cmds (work) according to their priority,
/// and executes them in a controlled way, taking into
/// account the rate limits of our system load monitoring.
///
/// Stacking up direct method calls in the async runtime is sort of
/// like saying to a node "do everything everyone is asking of you, now".
/// We're now saying to a node "do as much as you can without choking,
/// and start with the most important things first".
#[derive(Clone)]
pub(crate) struct CmdCtrl {
    cmd_queue: Arc<RwLock<PriorityQueue<EnqueuedJob, Priority>>>,
    attempted: MsgThroughput,
    monitoring: RateLimits,
    stopped: Arc<RwLock<bool>>,
    pub(crate) dispatcher: Arc<Dispatcher>,
    id_counter: Arc<AtomicUsize>,
    event_sender: EventSender,
}

impl CmdCtrl {
    pub(crate) fn new(
        dispatcher: Dispatcher,
        monitoring: RateLimits,
        event_sender: EventSender,
    ) -> Self {
        let session = Self {
            cmd_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            attempted: MsgThroughput::default(),
            monitoring,
            stopped: Arc::new(RwLock::new(false)),
            dispatcher: Arc::new(dispatcher),
            id_counter: Arc::new(AtomicUsize::new(0)),
            event_sender,
        };

        let session_clone = session.clone();
        let _ = tokio::task::spawn_local(async move { session_clone.keep_sending().await });

        session
    }

    pub(crate) fn node(&self) -> Arc<RwLock<crate::node::Node>> {
        self.dispatcher.node()
    }

    pub(crate) async fn push(&self, cmd: Cmd) -> Result<SendWatcher> {
        self.push_internal(cmd, None).await
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    #[allow(unused)]
    pub(crate) async fn stop(self) {
        *self.stopped.write().await = true;
        self.cmd_queue.write().await.clear();
    }

    async fn extend(&self, cmds: Vec<Cmd>, parent_id: Option<usize>) -> Vec<Result<SendWatcher>> {
        let mut results = vec![];

        for cmd in cmds {
            results.push(self.push_internal(cmd, parent_id).await);
        }

        results
    }

    async fn push_internal(&self, cmd: Cmd, parent_id: Option<usize>) -> Result<SendWatcher> {
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        let priority = cmd.priority();

        let (watcher, reporter) = status_watching();

        let job = EnqueuedJob {
            job: CmdJob::new(
                self.id_counter.fetch_add(1, ORDER),
                parent_id,
                cmd,
                SystemTime::now(),
            ),
            retries: 0,
            reporter,
        };

        let _ = self.cmd_queue.write().await.push(job, priority);

        Ok(watcher)
    }

    // could be accessed via a clone
    async fn stopped(&self) -> bool {
        *self.stopped.read().await
    }

    async fn notify(&self, event: Event) {
        self.event_sender.send(event).await
    }

    async fn keep_sending(&self) {
        loop {
            if self.stopped().await {
                break;
            }

            let expected_rate = self.monitoring.max_cmds_per_s().await;
            let actual_rate = self.attempted.value();
            if actual_rate > expected_rate {
                let diff = actual_rate - expected_rate;
                let diff_ms = Duration::from_millis((diff * 1000_f64) as u64);
                tokio::time::sleep(diff_ms).await;
                continue;
            } else if self.cmd_queue.read().await.is_empty() {
                tokio::time::sleep(SLEEP_TIME).await;
                continue;
            }

            {
                let queue = self.cmd_queue.read().await;
                debug!("Cmd queue length: {}", queue.len());
            }

            let queue_res = { self.cmd_queue.write().await.pop() };
            if let Some((mut enqueued, prio)) = queue_res {
                if enqueued.retries >= MAX_RETRIES {
                    // break this loop, report error to other nodes
                    // await decision on how to continue
                    // (or send event on a channel (to report error to other nodes), then sleep for a very long time, then try again?)

                    enqueued
                        .reporter
                        .send(CtrlStatus::MaxRetriesReached(enqueued.retries));

                    continue; // this means we will drop this cmd entirely!
                }
                let clone = self.clone();
                let _ = tokio::task::spawn_local(async move {
                    if enqueued.retries == 0 {
                        clone
                            .notify(Event::CmdProcessing(CmdProcessEvent::Started {
                                job: enqueued.job.clone(),
                                time: SystemTime::now(),
                            }))
                            .await;
                    } else {
                        clone
                            .notify(Event::CmdProcessing(CmdProcessEvent::Retrying {
                                job: enqueued.job.clone(),
                                retry: enqueued.retries,
                                time: SystemTime::now(),
                            }))
                            .await;
                    }
                    match clone
                        .dispatcher
                        .process_cmd(enqueued.job.cmd().clone())
                        .await
                    {
                        Ok(cmds) => {
                            enqueued.reporter.send(CtrlStatus::Finished);

                            clone.monitoring.increment_cmds().await;

                            // evaluate: handle these watchers?
                            let _watchers = clone.extend(cmds, enqueued.job.parent_id()).await;
                            clone
                                .notify(Event::CmdProcessing(CmdProcessEvent::Finished {
                                    job: enqueued.job,
                                    time: SystemTime::now(),
                                }))
                                .await;
                        }
                        Err(error) => {
                            clone
                                .notify(Event::CmdProcessing(CmdProcessEvent::Failed {
                                    job: enqueued.job.clone(),
                                    retry: enqueued.retries,
                                    time: SystemTime::now(),
                                    error: format!("{:?}", error),
                                }))
                                .await;
                            enqueued.retries += 1;
                            enqueued.reporter.send(CtrlStatus::Error(Arc::new(error)));
                            let _ = clone.cmd_queue.write().await.push(enqueued, prio);
                        }
                    }
                    clone.attempted.increment(); // both on fail and success
                });
            }
        }
    }
}

#[derive(Clone, Debug)]
struct MsgThroughput {
    msgs: Arc<AtomicUsize>,
    since: Instant,
}

impl Default for MsgThroughput {
    fn default() -> Self {
        Self {
            msgs: Arc::new(AtomicUsize::new(0)),
            since: Instant::now(),
        }
    }
}

impl MsgThroughput {
    fn increment(&self) {
        let _ = self.msgs.fetch_add(1, Ordering::SeqCst);
    }

    // msgs / s
    fn value(&self) -> f64 {
        let msgs = self.msgs.load(Ordering::SeqCst);
        let seconds = (Instant::now() - self.since).as_secs();
        msgs as f64 / f64::max(1.0, seconds as f64)
    }
}

#[derive(Debug)]
pub(crate) struct EnqueuedJob {
    job: CmdJob,
    retries: usize,
    reporter: StatusReporting,
}

impl PartialEq for EnqueuedJob {
    fn eq(&self, other: &Self) -> bool {
        self.job.id() == other.job.id()
    }
}

impl Eq for EnqueuedJob {}

impl std::hash::Hash for EnqueuedJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        //self.job.hash(state);
        self.job.id().hash(state);
        self.retries.hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CtrlStatus {
    Enqueued,
    Finished,
    Error(Arc<Error>),
    MaxRetriesReached(usize),
    #[allow(unused)]
    WatcherDropped,
}

pub(crate) struct SendWatcher {
    receiver: tokio::sync::watch::Receiver<CtrlStatus>,
}

impl SendWatcher {
    /// Reads current status
    #[allow(unused)]
    pub(crate) fn status(&self) -> CtrlStatus {
        self.receiver.borrow().clone()
    }

    /// Waits until a new status arrives.
    pub(crate) async fn await_change(&mut self) -> CtrlStatus {
        if self.receiver.changed().await.is_ok() {
            self.receiver.borrow_and_update().clone()
        } else {
            CtrlStatus::WatcherDropped
        }
    }
}

#[derive(Debug)]
struct StatusReporting {
    sender: tokio::sync::watch::Sender<CtrlStatus>,
}

impl StatusReporting {
    fn send(&self, status: CtrlStatus) {
        // todo: ok to drop error here?
        let _ = self.sender.send(status);
    }
}

fn status_watching() -> (SendWatcher, StatusReporting) {
    let (sender, receiver) = tokio::sync::watch::channel(CtrlStatus::Enqueued);
    (SendWatcher { receiver }, StatusReporting { sender })
}
