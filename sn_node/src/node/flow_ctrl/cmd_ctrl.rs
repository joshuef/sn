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
use itertools::Itertools;
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

// const MAX_RETRIES: usize = 1; // drops on first error..
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
        let session = Self {
            cmd_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            attempted: CmdThroughput::default(),
            monitoring,
            stopped: Arc::new(RwLock::new(false)),
            dispatcher: Arc::new(dispatcher),
            id_counter: Arc::new(AtomicUsize::new(0)),
            event_sender,
        };

        let session_clone = session.clone();
        let _ = tokio::task::spawn_local(async move { session_clone.keep_processing().await });

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
    async fn merge_if_existing_compatible_cmd_exists(&self, new_cmd: &Cmd, new_cmd_prio: i32) -> Option<usize> {
        // let the_q = self.cmd_queue.read().await;
        let mut q_vec =  self.cmd_queue.write().await.iter().map(|(enqueued_job, _prio)| {
            (enqueued_job.job.priority(), enqueued_job.job.cmd().clone(), enqueued_job.job.id() )
        }).collect_vec() ;

        // drop(the_q);

        for (queued_job_prio, queued_cmd, existing_cmd_id) in q_vec {
            // first do an easy discard based on matching prios
            if queued_job_prio != new_cmd_prio {
                continue
            }

            match queued_cmd {
                Cmd::CleanupPeerLinks => {
                    debug!("matched CleanupPeerLinks, dropping");
                    return Some(existing_cmd_id)
                },
                Cmd::TellEldersToStartConnectivityTest(node) => {
                    if let Cmd::TellEldersToStartConnectivityTest(some_node) = new_cmd {
                        if &node == some_node {
                            debug!("matched ConnectivityTest");
                            return Some(existing_cmd_id)
                            }


                    }
                },
                Cmd::SendMsg { recipients: enqeued_cmd_recipients, msg: enqeued_outgoing_msg, .. } => {
                    // Cmd::SendMsg {
                    //     recipients: new_cmd_recipients,
                    //     wire_msg: new_cmd_msg,
                    // } => {
                    let mut queued_peers = enqeued_cmd_recipients.clone();
                    if let Cmd::SendMsg { recipients, msg, .. } = new_cmd {
                        let msg_match = msg == &enqeued_outgoing_msg;
                        if msg_match {
                            debug!(
                                "MsgMatching cmdid {existing_cmd_id} found, going to: {:?}",
                                recipients
                            );
                            if recipients == &queued_peers {
                                debug!("matched SendMsg exactly, so dropping");
                                debug!("Recipients where the same for {existing_cmd_id}");
                                // we know the Cmd and it matches completely so we drop it
                                return Some(existing_cmd_id);
                            }
                            else {
                                // here we modify the existing Cmd
                                match queued_peers {

                                }
                                // return (msg_match, None);
                            }
                        }

                    }

                    // if msg_match {
                    //     // dropping msg and exiting out early as we have the same exact cmd already waiting to be parsed
                    //     if let Some(existing_cmd_id) = exact_same_existing_cmd_id {
                    //         debug!("matched SendMsg exactly, so dropping");

                    //         return Some(existing_cmd_id);
                    //     }
                    // }
                }
                _ => {}
            }

        }

        None
    }

    // /// checks for a matching SendMsg, returns (true,..) if one exists and (.., Some(CmdId)) if recipients match too
    // /// That way we can exit early with never taking a write lock on the cmd struct, and log that we've dropped this
    // /// duplicate Cmd.
    // ///
    // /// Ie, returns (msg matches, recipients are the same)
    // async fn check_if_matching_send_msg_exists(
    //     &self,
    //     cmd_recipients: &Vec<Peer>,
    //     cmd_wire_msg: &WireMsg,
    // ) -> (bool, Option<usize>) {
    //     let q = self.cmd_queue.read().await;

    //     for (enqueued_job, prio) in q.iter() {
    //         // if prio !==
    //         let queued_cmd = enqueued_job.job.cmd();
    //         if let Cmd::SendMsg {
    //             recipients,
    //             wire_msg,
    //         } = queued_cmd
    //         {
    //             // if we have the same wire msg, it's a match
    //             let msg_match = wire_msg == cmd_wire_msg;
    //             let existing_cmd_id = enqueued_job.job.id();

    //             if msg_match {
    //                 debug!(
    //                     "MsgMatching cmdid {existing_cmd_id} found, going to: {:?}",
    //                     wire_msg.dst_location()
    //                 );
    //                 if recipients == cmd_recipients {
    //                     debug!("Recipients where the same for {existing_cmd_id}");
    //                     // we know the Cmd and it matches completely so we drop it
    //                     return (msg_match, Some(existing_cmd_id));
    //                 } else {
    //                     return (msg_match, None);
    //                 }
    //             }
    //         }
    //     }

    //     (false, None)
    // }

    async fn push_and_merge_internal(
        &self,
        cmd: Cmd,
        parent_id: Option<usize>,
    ) -> Result<Option<SendWatcher>> {
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        let priority = cmd.priority();
        // TODO: should each merge bump prio?
        if let Some(existing_queued_cmd_id) =
            self.merge_if_existing_compatible_cmd_exists(&cmd, priority).await
        {
            trace!(
                "New Cmd would be merged into {:?}, (Cmd was: {:?})",
                existing_queued_cmd_id,
                cmd
            );

            return Ok(None);
        }


        let (watcher, reporter) = status_watching();

        let job = EnqueuedJob {
            job: CmdJob::new(
                self.id_counter.fetch_add(1, ORDER),
                parent_id,
                cmd,
                SystemTime::now(),
            ),
            // retries: 0,
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

    async fn keep_processing(&self) {
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
                // if enqueued.retries >= MAX_RETRIES {
                //     // break this loop, report error to other nodes
                //     // await decision on how to continue
                //     // (or send event on a channel (to report error to other nodes), then sleep for a very long time, then try again?)

                //     enqueued
                //         .reporter
                //         .send(CtrlStatus::MaxRetriesReached(enqueued.retries));

                //     continue; // this means we will drop this cmd entirely!
                // }
                let cmd_ctrl = self.clone();
                cmd_ctrl
                    .notify(Event::CmdProcessing(CmdProcessEvent::Started {
                        job: enqueued.job.clone(),
                        time: SystemTime::now(),
                    }))
                    .await;

                let _ = tokio::task::spawn_local(async move {
                    match cmd_ctrl
                        .dispatcher
                        .process_cmd(enqueued.job.cmd().clone())
                        .await
                    {
                        Ok(cmds) => {
                            enqueued.reporter.send(CtrlStatus::Finished);

                            cmd_ctrl.monitoring.increment_cmds().await;

                            // evaluate: handle these watchers?
                            let _watchers = cmd_ctrl.extend(cmds, enqueued.job.parent_id()).await;
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
                                    // retry: enqueued.retries,
                                    time: SystemTime::now(),
                                    error: format!("{:?}", error),
                                }))
                                .await;
                            // enqueued.retries += 1;
                            enqueued.reporter.send(CtrlStatus::Error(Arc::new(error)));
                            // let _ = cmd_ctrl.cmd_queue.write().await.push(enqueued, prio);
                        }
                    }
                    cmd_ctrl.attempted.increment(); // both on fail and success
                });
            }
        }
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
    // retries: usize,
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
        // match self.job.cmd() {
        //     Cmd::CleanupPeerLinks => "cleanup".hash(state),
        //     Cmd::TellEldersToStartConnectivityTest(node) => fmt!("connectivity {:?}", node).hash(state),

        //     _ => {
        //         self.job.id().hash(state);
        //     }
        // }
        // // self.retries.hash(state);
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
