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
        cmds::{Cmd, CmdJob, VALIDATE_MSG_PRIO},
        dispatcher::Dispatcher,
        event_channel::EventSender,
    },
    CmdProcessEvent, Event,
};

use priority_queue::PriorityQueue;
use std::time::SystemTime;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

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
    pub(crate) dispatcher: Arc<Dispatcher>,
    id_counter: usize,
}

impl CmdCtrl {
    pub(crate) fn new(dispatcher: Dispatcher) -> Self {
        Self {
            cmd_queue: PriorityQueue::new(),
            dispatcher: Arc::new(dispatcher),
            id_counter: 0,
        }
    }

    pub(crate) fn node(&self) -> Arc<RwLock<crate::node::Node>> {
        self.dispatcher.node()
    }

    pub(crate) async fn push(&mut self, cmd: Cmd, parent_id: Option<usize>) {
        self.push_internal(cmd, parent_id).await
    }

    /// Does the cmd_queue contain _anything_
    pub(crate) fn has_items_queued(&self) -> bool {
        !self.cmd_queue.is_empty()
    }

    async fn push_internal(&mut self, cmd: Cmd, parent_id: Option<usize>) {
        self.id_counter += 1;

        // First, check if we already have this
        // This should just queue up any one request from a given client if its not been validated yet
        if let Cmd::ValidateMsg {
            wire_msg: original_wire_msg,
            origin: original_origin,
        } = &cmd
        {
            for (job, job_prio) in &self.cmd_queue {
                // skip anything that's been validated...
                if job_prio != &VALIDATE_MSG_PRIO {
                    continue;
                }

                if let Cmd::ValidateMsg {
                    wire_msg: job_wire_msg,
                    origin: job_origin,
                } = job.cmd()
                {
                    if original_origin == job_origin && original_wire_msg == job_wire_msg {
                        debug!(
                            "Existing ValidateMsg that matches this found. Dropping Cmd: {cmd:?}"
                        );
                        return;
                    }
                }
            }
        }

        let job = CmdJob::new(self.id_counter, parent_id, cmd, SystemTime::now());
        let prio = job.priority();
        let _ = self.cmd_queue.push(job, prio);
    }

    /// Get the next Cmd going off of priority
    pub(crate) fn next_cmd(&mut self) -> Option<CmdJob> {
        self.cmd_queue.pop().map(|(job, _prio)| job)
    }

    /// Wait if required by the cmd rate monitoring
    pub(crate) async fn wait_if_not_processing_at_expected_rate(&self) {
        if self.cmd_queue.is_empty() {
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
    ) {
        #[cfg(feature = "test-utils")]
        {
            debug!("Cmd queue length: {}", self.cmd_queue.len());
        }

        let id = job.id();
        let cmd = job.clone().into_cmd();

        trace!("Processing cmd: {cmd:?}");

        let cmd_string = cmd.clone().to_string();
        let priority = job.priority();

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

        trace!("about to spawn for processing cmd: {cmd:?}");
        let dispatcher = self.dispatcher.clone();
        let _ = tokio::task::spawn_local(async move {
            dispatcher
                .node()
                .read()
                .await
                .statemap_log_state(cmd.statemap_state());

            match dispatcher.process_cmd(cmd).await {
                Ok(cmds) => {
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
                    debug!("Error when processing command: {:?}", error);
                    node_event_sender
                        .send(Event::CmdProcessing(CmdProcessEvent::Failed {
                            id,
                            priority,
                            time: SystemTime::now(),
                            cmd_string,
                            error: format!("{:?}", &error.to_string()),
                        }))
                        .await;
                }
            }
            dispatcher
                .node()
                .read()
                .await
                .statemap_log_state(sn_interface::statemap::State::Idle);
        });
    }
}
