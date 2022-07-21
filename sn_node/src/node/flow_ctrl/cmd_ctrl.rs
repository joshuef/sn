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
use sn_interface::messaging::WireMsg;
use sn_interface::types::Peer;
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

    pub(crate) async fn q_len(&self) -> usize {
        self.cmd_queue.read().await.len()
    }

    pub(crate) fn node(&self) -> Arc<RwLock<crate::node::Node>> {
        self.dispatcher.node()
    }

    pub(crate) async fn push_and_merge(&self, cmd: Cmd) -> Result<Option<SendWatcher>> {
        self.push_and_merge_internal(cmd, None).await
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    #[allow(unused)]
    pub(crate) async fn stop(self) {
        *self.stopped.write().await = true;
        self.cmd_queue.write().await.clear();
    }

    async fn extend(
        &self,
        cmds: Vec<Cmd>,
        parent_id: Option<usize>,
    ) -> Vec<Result<Option<SendWatcher>>> {
        let mut results = vec![];

        for cmd in cmds {
            results.push(self.push_and_merge_internal(cmd, parent_id).await);
        }

        results
    }

    // TODO: rework this to pass in the queue so we can test it easily
    /// Certain commands can me merged without losing anything
    /// eg, `CleanupPeerLinks` commands can be merged if there is one in the queue.
    /// two back to back should not do anything new.
    /// SendMsg commands can be merged if the message content is the same
    /// as the send functions will update the destination per peer, so we just have to ensure
    /// that `recipients` are updated with any new.
    /// A `debug` log will be emitted noting the CmdId merge for tracing purposes
    async fn merge_if_existing_compatible_cmd_exists(&self, cmd: &Cmd) -> Option<usize> {
        match cmd {
            Cmd::CleanupPeerLinks => {
                let q = self.cmd_queue.read().await;
                for (enqueued_job, _prio) in q.iter() {
                    let queued_cmd = enqueued_job.job.cmd();
                    if matches!(queued_cmd, Cmd::CleanupPeerLinks) {
                        debug!("Matching CleanupPeerLinks");

                        let existing_cmd_id = enqueued_job.job.id();
                        return Some(existing_cmd_id);
                    }
                }

                None
            }
            Cmd::SendMsg {
                recipients: new_cmd_recipients,
                wire_msg: new_cmd_msg,
            } => {
                let mut all_recipients = new_cmd_recipients.clone();
                let (msg_match, exact_same_existing_cmd_id) = self
                    .check_if_matching_send_msg_exists(new_cmd_recipients, new_cmd_msg)
                    .await;
                if msg_match {
                    debug!("Matching SendMsg");
                    // dropping msg and exiting out early as we have the same exact cmd already waiting to be parsed
                    if let Some(existing_cmd_id) = exact_same_existing_cmd_id {
                        return Some(existing_cmd_id);
                    }

                    // otherwise, continue into lock
                    // we can merge it then!
                    let mut q = self.cmd_queue.write().await;

                    for (enqueued_job, _prio) in q.iter_mut() {
                        let queued_cmd = enqueued_job.job.cmd();
                        if let Cmd::SendMsg {
                            wire_msg,
                            recipients,
                        } = queued_cmd
                        {
                            // if we've the same msg and different recipients, we should merge
                            if wire_msg == new_cmd_msg {
                                let existing_cmd_id = enqueued_job.job.id();

                                // double check here, in case more than one cmd was somehow queued.... (is that possible?)
                                if recipients == &all_recipients {
                                    return Some(existing_cmd_id);
                                };

                                // now we flesh out and add a new job with both recipients merged.
                                all_recipients.extend(recipients);

                                let new_cmd = Cmd::SendMsg {
                                    recipients: all_recipients,
                                    wire_msg: wire_msg.clone(),
                                };
                                enqueued_job.job.update_cmd(new_cmd);
                                return Some(existing_cmd_id);
                            }
                        }
                    }
                }

                None
            }
            Cmd::ValidateMsg {
                origin,
                wire_msg,
                original_bytes: bytes,
            } => {
                let q = self.cmd_queue.read().await;

                for (enqueued_job, _prio) in q.iter() {
                    let queued_cmd = enqueued_job.job.cmd();
                    if let Cmd::ValidateMsg {
                        origin,
                        wire_msg,
                        original_bytes,
                    } = queued_cmd
                    {
                        if original_bytes == bytes {
                            debug!("Matching ValidateMsg")
                        }

                        let existing_cmd_id = enqueued_job.job.id();
                        return Some(existing_cmd_id);
                    }
                }
                None
            }

            _ => None,
        }
    }

    /// checks for a matching SendMsg, returns (true,..) if one exists and (.., Some(CmdId)) if recipients match too
    /// That way we can exit early with never taking a write lock on the cmd struct, and log that we've dropped this
    /// duplicate Cmd.
    ///
    /// Ie, returns (msg matches, recipients are the same)
    async fn check_if_matching_send_msg_exists(
        &self,
        cmd_recipients: &Vec<Peer>,
        cmd_wire_msg: &WireMsg,
    ) -> (bool, Option<usize>) {
        let q = self.cmd_queue.read().await;

        for (enqueued_job, _prio) in q.iter() {
            let queued_cmd = enqueued_job.job.cmd();
            if let Cmd::SendMsg {
                recipients,
                wire_msg,
            } = queued_cmd
            {
                // if we have the same wire msg, it's a match
                let msg_match = wire_msg == cmd_wire_msg;
                let existing_cmd_id = enqueued_job.job.id();

                if msg_match {
                    if recipients == cmd_recipients {
                        // we know the Cmd and it matches completely so we drop it
                        return (msg_match, Some(existing_cmd_id));
                    } else {
                        return (msg_match, None);
                    }
                }
            }
        }

        (false, None)
    }

    async fn push_and_merge_internal(
        &self,
        cmd: Cmd,
        parent_id: Option<usize>,
    ) -> Result<Option<SendWatcher>> {
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        // // TODO: should each merge bump prio?
        // if let Some(existing_queued_cmd_id) =
        //     self.merge_if_existing_compatible_cmd_exists(&cmd).await
        // {
        //     trace!(
        //         "New Cmd was merged into {:?}, (Cmd was: {:?})",
        //         existing_queued_cmd_id,
        //         cmd
        //     );

        //     return Ok(None);
        // }

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

        Ok(Some(watcher))
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

            #[cfg(feature = "test-utils")]
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
