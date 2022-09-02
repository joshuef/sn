// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(crate) mod cmd_ctrl;
pub(crate) mod cmds;
pub(super) mod dispatcher;
pub(super) mod event;
pub(super) mod event_channel;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) use self::cmd_ctrl::CmdCtrl;
mod periodic_checks;

use event_channel::EventSender;

use crate::comm::MsgEvent;

use crate::node::{flow_ctrl::cmds::Cmd, Error, Node, Result};

use sn_interface::types::log_markers::LogMarker;

use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        RwLock,
    },
    time::Instant,
};

// const PROCESS_BATCH_COUNT: usize = 25;

/// Listens for incoming msgs and forms Cmds for each,
/// Periodically triggers other Cmd Processes (eg health checks, dysfunction etc)
pub(crate) struct FlowCtrl {
    node: Arc<RwLock<Node>>,
    cmd_ctrl: CmdCtrl,
    incoming_msg_events: mpsc::Receiver<MsgEvent>,
    incoming_cmds_from_apis: mpsc::Receiver<(Cmd, Option<usize>)>,
    cmd_sender_channel: mpsc::Sender<(Cmd, Option<usize>)>,
    outgoing_node_event_sender: EventSender,
}

impl FlowCtrl {
    pub(crate) fn new(
        cmd_ctrl: CmdCtrl,
        incoming_msg_events: mpsc::Receiver<MsgEvent>,
        outgoing_node_event_sender: EventSender,
    ) -> (Self, mpsc::Sender<(Cmd, Option<usize>)>) {
        let node = cmd_ctrl.node();
        let (cmd_sender_channel, incoming_cmds_from_apis) = mpsc::channel(100);

        (
            Self {
                cmd_ctrl,
                node,
                incoming_msg_events,
                incoming_cmds_from_apis,
                cmd_sender_channel: cmd_sender_channel.clone(),
                outgoing_node_event_sender,
            },
            cmd_sender_channel,
        )
    }

    /// Process the next pending cmds
    async fn process_next_cmd(&mut self) {
        if let Some(next_cmd_job) = self.cmd_ctrl.next_cmd() {
            self.cmd_ctrl
                .process_cmd_job(
                    next_cmd_job,
                    self.cmd_sender_channel.clone(),
                    self.outgoing_node_event_sender.clone(),
                )
                .await
        }
    }

    /// Pull and queue up all pending cmds from the CmdChannel
    async fn enqeue_new_cmds_from_channel(&mut self) -> Result<()> {
        loop {
            match self.incoming_cmds_from_apis.try_recv() {
                Ok((cmd, id)) => self.fire_and_forget(cmd, id).await,
                Err(TryRecvError::Empty) => {
                    // do nothing else
                    return Ok(());
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Senders to `incoming_cmds_from_apis` have disconnected.");
                    return Err(Error::CmdCtrlChannelDropped);
                }
            }
        }
    }

    /// Pull and queue up all pending msgs from the MsgSender
    async fn enqueue_new_incoming_msgs(&mut self) -> Result<()> {
        loop {
            match self.incoming_msg_events.try_recv() {
                Ok(msg) => {
                    let cmd = match self.handle_new_msg_event(msg).await {
                        Ok(cmd) => cmd,
                        Err(error) => {
                            error!("Could not handle incoming msg event: {error:?}");
                            continue;
                        }
                    };

                    // dont use sender here incase channel gets full
                    self.fire_and_forget(cmd, None).await;
                }
                Err(TryRecvError::Empty) => {
                    // do nothing else
                    return Ok(());
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Senders to `incoming_cmds_from_apis` have disconnected.");
                    return Err(Error::MsgChannelDropped);
                }
            }
        }
    }

    /// This is a never ending loop as long as the node is live.
    /// This loop drives the periodic events internal to the node.
    pub(crate) async fn process_messages_and_periodic_checks(mut self) {
        debug!("Starting internal processing...");
        let mut last_probe = Instant::now();
        let mut last_section_probe = Instant::now();
        let mut last_adult_health_check = Instant::now();
        let mut last_elder_health_check = Instant::now();
        let mut last_vote_check = Instant::now();
        let mut last_data_batch_check = Instant::now();
        let mut last_link_cleanup = Instant::now();
        let mut last_dysfunction_check = Instant::now();

        // the internal process loop
        loop {
            // if we want to throttle cmd throughput, we do that here.
            // if there is nothing in the cmd queue, we wait here too.
            self.cmd_ctrl
                .wait_if_not_processing_at_expected_rate()
                .await;

            if let Err(error) = self.enqueue_new_incoming_msgs().await {
                error!("{error:?}");
                break;
            }

            if let Err(error) = self.enqeue_new_cmds_from_channel().await {
                error!("{error:?}");
                break;
            }

            // we go through all pending cmds in this loop
            while self.cmd_ctrl.has_items_queued() {
                self.process_next_cmd().await;

                if let Err(error) = self.enqeue_new_cmds_from_channel().await {
                    error!("{error:?}");
                    break;
                }
            }

            self.enqueue_cmds_for_standard_periodic_checks(
                &mut last_link_cleanup,
                &mut last_data_batch_check,
            )
            .await;

            if !self.node.read().await.is_elder() {
                self.enqueue_cmds_for_adult_periodic_checks(&mut last_section_probe)
                    .await;

                // we've pushed what we have as an adult and processed incoming msgs
                // and cmds... so we can continue
                continue;
            }

            self.enqueue_cmds_for_elder_periodic_checks(
                &mut last_probe,
                &mut last_adult_health_check,
                &mut last_elder_health_check,
                &mut last_vote_check,
                &mut last_dysfunction_check,
            )
            .await;
        }

        error!("Internal processing ended.")
    }

    /// Does not await the completion of the cmd.
    pub(crate) async fn fire_and_forget(&mut self, cmd: Cmd, parent_id: Option<usize>) {
        trace!("Enqueuing cmd: {cmd:?}");
        self.cmd_ctrl.push(cmd, parent_id).await
    }

    // Listen for a new incoming connection event and handle it.
    async fn handle_new_msg_event(&self, event: MsgEvent) -> Result<Cmd> {
        match event {
            MsgEvent::Received { sender, wire_msg } => {
                let (header, dst, payload) = wire_msg.serialize()?;
                let original_bytes_len = header.len() + dst.len() + payload.len();

                debug!(
                    "New message ({} bytes) received from: {:?}",
                    original_bytes_len, sender
                );

                let span = {
                    // let name = node_info.name();
                    trace_span!("handle_message", ?sender, msg_id = ?wire_msg.msg_id())
                };
                let _span_guard = span.enter();

                trace!(
                    "{:?} from {:?} length {}",
                    LogMarker::DispatchHandleMsgCmd,
                    sender,
                    original_bytes_len
                );

                #[cfg(feature = "test-utils")]
                let wire_msg = if let Ok(msg) = wire_msg.into_msg() {
                    wire_msg.set_payload_debug(msg)
                } else {
                    wire_msg
                };

                Ok(Cmd::ValidateMsg {
                    origin: sender,
                    wire_msg,
                })
            }
        }
    }
}
