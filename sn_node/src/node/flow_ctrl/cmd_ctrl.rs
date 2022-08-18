// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::log_sleep;
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

const EMPTY_QUEUE_SLEEP_TIME: Duration = Duration::from_millis(100);

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
    cmd_queue: PriorityQueue<CmdJob, Priority>,
    attempted: CmdThroughput,
    monitoring: RateLimits,
    stopped: bool,
    pub(crate) dispatcher: Arc<Dispatcher>,
    id_counter: usize,
}

impl CmdCtrl {
    pub(crate) fn new(dispatcher: Dispatcher, monitoring: RateLimits) -> Self {
        Self {
            cmd_queue: PriorityQueue::new(),
            attempted: CmdThroughput::default(),
            monitoring,
            stopped: false,
            dispatcher: Arc::new(dispatcher),
            id_counter: 0,
        }
    }

    pub(crate) fn node(&self) -> Arc<RwLock<crate::node::Node>> {
        self.dispatcher.node()
    }

    pub(crate) async fn push(&mut self, cmd: Cmd) -> Result<()> {
        self.push_internal(cmd, None).await
    }

    /// Does the cmd_queue contain _anything_
    pub(crate) fn has_items_queued(&self) -> bool {
        !self.cmd_queue.is_empty()
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    #[allow(unused)]
    pub(crate) async fn stop(mut self) {
        self.stopped = true;
        self.cmd_queue.clear();
    }

    async fn push_internal(&mut self, cmd: Cmd, parent_id: Option<usize>) -> Result<()> {
        if self.stopped().await {
            // should not happen (be reachable)
            return Err(Error::InvalidState);
        }

        self.id_counter += 1;

        let job = CmdJob::new(self.id_counter, parent_id, cmd, SystemTime::now());

        let prio = job.priority();
        let _ = self.cmd_queue.push(job, prio);
        Ok(())
    }

    // could be accessed via a clone
    async fn stopped(&self) -> bool {
        self.stopped
    }

    /// Get the next Cmd going off of priority
    pub(crate) fn next_cmd(&mut self) -> Option<CmdJob> {
        debug!("gonna pop cmd");
        self.cmd_queue.pop().map(|(job, _prio)| job)
    }

    /// Wait if required by the cmd rate monitoring
    pub(crate) async fn wait_if_not_processing_at_expected_rate(&self) {
        let expected_rate = self.monitoring.max_cmds_per_s().await;
        let actual_rate = self.attempted.value();
        if actual_rate > expected_rate {
            let diff = actual_rate - expected_rate;
            debug!("Cmd throughput is too high, waiting to reduce throughput");
            log_sleep!(Duration::from_millis((diff * 10_f64) as u64));
        } else if self.cmd_queue.is_empty() {
            trace!("Empty queue, waiting {EMPTY_QUEUE_SLEEP_TIME:?} to not loop heavily");
            log_sleep!(EMPTY_QUEUE_SLEEP_TIME);
        }
    }

    /// Processes the next priority cmd
    pub(crate) async fn process_cmd_job(
        &mut self,
        job: CmdJob,
        cmd_process_api: tokio::sync::mpsc::Sender<(Cmd, Option<usize>)>,
        node_event_sender: EventSender,
    ) -> Result<()> {
        if self.stopped().await {
            return Err(Error::CmdCtrlStopped);
        }

        #[cfg(feature = "test-utils")]
        {
            debug!("Cmd queue length: {}", self.cmd_queue.len());
        }

        let id = job.id();
        let cmd = job.clone().into_cmd();

        let cmd_string = cmd.clone().to_string();
        let priority = job.priority();

        let throughpout = self.attempted.clone();
        let monitoring = self.monitoring.clone();

        node_event_sender
            .send(Event::CmdProcessing(CmdProcessEvent::Started {
                id,
                parent_id: job.parent_id(),
                priority,
                cmd_creation_time: job.created_at(),
                time: SystemTime::now(),
                cmd_string: cmd.to_string(),
            }))
            .await;

        let dispatcher = self.dispatcher.clone();
        let _ = tokio::task::spawn_local(async move {
            match dispatcher.process_cmd(cmd).await {
                Ok(cmds) => {
                    monitoring.increment_cmds().await;

                    for cmd in cmds {
                        match cmd_process_api.send((cmd, Some(id))).await {
                            Ok(_) => {
                                //no issues
                            }
                            Err(error) => {
                                error!("Could not enqueue child Cmd: {:?}", error);
                            }
                        }
                    }
                    node_event_sender
                        .send(Event::CmdProcessing(CmdProcessEvent::Finished {
                            id,
                            priority,
                            cmd_string,
                            time: SystemTime::now(),
                        }))
                        .await;
                }
                Err(error) => {
                    node_event_sender
                        .send(Event::CmdProcessing(CmdProcessEvent::Failed {
                            id,
                            priority,
                            time: SystemTime::now(),
                            cmd_string,
                            error: format!("{:?}", error),
                        }))
                        .await;
                }
            }
            throughpout.increment(); // both on fail and success
        });
        Ok(())
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
