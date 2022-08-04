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

const SLEEP_TIME: Duration = Duration::from_millis(10);

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
pub(crate) struct CmdCtrl {
    cmd_queue: PriorityQueue<EnqueuedJob, Priority>,
    attempted: CmdThroughput,
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
        Self {
            cmd_queue: PriorityQueue::new(),
            attempted: CmdThroughput::default(),
            monitoring,
            stopped: Arc::new(RwLock::new(false)),
            dispatcher: Arc::new(dispatcher),
            id_counter: Arc::new(AtomicUsize::new(0)),
            event_sender,
        }

        // let session_clone = session.clone();
        // let _ = tokio::task::spawn_local(async move { session_clone.keep_processing().await });

        // session
    }

    pub(crate) fn node(&self) -> Arc<RwLock<crate::node::Node>> {
        self.dispatcher.node()
    }

    pub(crate) async fn push(&mut self, cmd: Cmd) -> Result<SendWatcher> {
        self.push_internal(cmd, None).await
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    #[allow(unused)]
    pub(crate) async fn stop(mut self) {
        *self.stopped.write().await = true;
        self.cmd_queue.clear();
    }

    async fn extend(&mut self, cmds: Vec<Cmd>, parent_id: usize) -> Vec<Result<SendWatcher>> {
        let mut results = vec![];

        for cmd in cmds {
            results.push(self.push_internal(cmd, Some(parent_id)).await);
        }

        results
    }

    async fn push_internal(&mut self, cmd: Cmd, parent_id: Option<usize>) -> Result<SendWatcher> {
        debug!("pushing cmd {cmd:?}");
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
            reporter,
        };

        let _ = self.cmd_queue.push(job, priority);
        debug!("cmd pushed to q");
        Ok(watcher)
    }

    // could be accessed via a clone
    async fn stopped(&self) -> bool {
        *self.stopped.read().await
    }

    async fn notify(&self, event: Event) {
        self.event_sender.send(event).await
    }

    /// Processes the next priority cmd
    pub(crate) async fn process_next_cmd(&mut self) -> Result<()> {
        debug!("processing next cmd");
        // loop {
        if self.stopped().await {
            return Err(Error::CmdCtrlStopped);
        }

        let expected_rate = self.monitoring.max_cmds_per_s().await;
        let actual_rate = self.attempted.value();
        if actual_rate > expected_rate {
            let diff = actual_rate - expected_rate;
            let diff_ms = Duration::from_millis((diff * 10_f64) as u64);
            tokio::time::sleep(diff_ms).await;
            return Ok(());
        } else if self.cmd_queue.is_empty() {
            tokio::time::sleep(SLEEP_TIME).await;
            return Ok(());
        }

        #[cfg(feature = "test-utils")]
        {
            let queue = self.cmd_queue;
            debug!("Cmd queue length: {}", queue.len());
        }

        debug!("q lennn {}", self.cmd_queue.len());
        let queue_res = self.cmd_queue.pop();
        if let Some((enqueued, _prio)) = queue_res {
            let cmd_ctrl = self;
            cmd_ctrl
                .notify(Event::CmdProcessing(CmdProcessEvent::Started {
                    job: enqueued.job.clone(),
                    time: SystemTime::now(),
                }))
                .await;

            // let _ = tokio::task::spawn_local(async move {
            match cmd_ctrl
                .dispatcher
                .process_cmd(enqueued.job.cmd().clone())
                .await
            {
                Ok(cmds) => {
                    enqueued.reporter.send(CtrlStatus::Finished);

                    cmd_ctrl.monitoring.increment_cmds().await;

                    // evaluate: handle these watchers?
                    let _watchers = cmd_ctrl.extend(cmds, enqueued.job.id()).await;
                    cmd_ctrl
                        .notify(Event::CmdProcessing(CmdProcessEvent::Finished {
                            job: enqueued.job,
                            time: SystemTime::now(),
                        }))
                        .await;
                }
                Err(error) => {
                    cmd_ctrl
                        .notify(Event::CmdProcessing(CmdProcessEvent::Failed {
                            job: enqueued.job.clone(),
                            time: SystemTime::now(),
                            error: format!("{:?}", error),
                        }))
                        .await;
                    enqueued.reporter.send(CtrlStatus::Error(Arc::new(error)));
                }
            }
            cmd_ctrl.attempted.increment(); // both on fail and success
                                            // });
        }

        Ok(())
        // }
    }
}

#[derive(Clone, Debug)]
struct CmdThroughput {
    msgs: Arc<AtomicUsize>,
    since: Instant,
}

impl Default for CmdThroughput {
    fn default() -> Self {
        Self {
            msgs: Arc::new(AtomicUsize::new(0)),
            since: Instant::now(),
        }
    }
}

impl CmdThroughput {
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
        self.job.id().hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CtrlStatus {
    Enqueued,
    Finished,
    Error(Arc<Error>),
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
