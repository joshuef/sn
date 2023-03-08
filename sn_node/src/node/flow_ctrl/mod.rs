// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(crate) mod cmd_ctrl;
pub(crate) mod cmds;
pub(super) mod dispatcher;
pub(super) mod fault_detection;
mod periodic_checks;

#[cfg(test)]
pub(crate) mod tests;
pub(crate) use cmd_ctrl::CmdCtrl;

use super::{core::NodeContext, node_starter::CmdChannel, DataStorage};
use periodic_checks::PeriodicChecksTimestamps;

use crate::node::{
    flow_ctrl::{
        cmds::Cmd,
        fault_detection::{FaultChannels, FaultsCmd},
    },
    messaging::Recipients,
    Error, MyNode, STANDARD_CHANNEL_SIZE,
};

use sn_comms::{CommEvent, MsgReceived};
use sn_fault_detection::FaultDetection;
use sn_interface::{
    messaging::system::{JoinRejectReason, NodeDataCmd, NodeMsg},
    messaging::{AntiEntropyMsg, NetworkMsg},
    types::{log_markers::LogMarker, DataAddress, NodeId, Participant},
};

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use xor_name::XorName;

/// Keep this as 1 so we properly feedback if we're not popping things out of the channel fast enough
const CMD_CHANNEL_SIZE: usize = 10_000;

/// Sent via the rejoin_network_tx to restart the join process.
/// This would only occur when joins are not allowed, or non-recoverable states.
#[derive(Debug)]
pub enum RejoinReason {
    /// Happens when trying to join; we will wait a moment and then try again.
    /// NB: Relocated nodes that try to join, are accepted even if joins are disallowed.
    JoinsDisallowed,
    /// Happens when already part of the network; we need to start from scratch.
    RemovedFromSection,
    /// Unrecoverable error, requires node operator network config.
    NodeNotReachable(SocketAddr),
}

impl RejoinReason {
    pub(crate) fn from_reject_reason(reason: JoinRejectReason) -> RejoinReason {
        use JoinRejectReason::*;
        match reason {
            JoinsDisallowed => RejoinReason::JoinsDisallowed,
            NodeNotReachable(add) => RejoinReason::NodeNotReachable(add),
        }
    }
}

/// Events for the msg handling loop
#[derive(Debug)]
pub(crate) enum ReadOnlyProcessingEvent {
    /// A msg was received from comms
    Cmd(Cmd),
    UpdateContext(NodeContext),
}

/// Listens for incoming msgs and forms Cmds for each,
/// Periodically triggers other Cmd Processes (eg health checks, fault detection etc)
pub(crate) struct FlowCtrl {
    cmd_sender_channel: CmdChannel,
    preprocess_cmd_sender_channel: Sender<ReadOnlyProcessingEvent>,
    fault_channels: FaultChannels,
    timestamps: PeriodicChecksTimestamps,
}

impl FlowCtrl {
    /// Constructs a FlowCtrl instance, spawnning a task which starts processing messages,
    /// returning the channel where it can receive commands on
    pub(crate) async fn start(
        node: MyNode,
        mut cmd_ctrl: CmdCtrl,
        join_retry_timeout: Duration,
        mut incoming_msg_events: Receiver<CommEvent>,
        data_replication_receiver: Receiver<(Vec<DataAddress>, NodeId)>,
        fault_cmds_channels: (Sender<FaultsCmd>, Receiver<FaultsCmd>),
    ) -> (CmdChannel, Receiver<RejoinReason>) {
        let node_context = node.context();
        let (mutating_cmd_sender_channel, mut incoming_mutating_cmds_from_apis) =
            mpsc::channel(CMD_CHANNEL_SIZE);
        let (rejoin_network_tx, rejoin_network_rx) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        let (read_only_event_sender, mut read_only_cmd_event_reciever) =
            mpsc::channel(STANDARD_CHANNEL_SIZE);

        let all_members = node_context
            .network_knowledge
            .adults()
            .iter()
            .map(|node_id| node_id.name())
            .collect::<BTreeSet<XorName>>();
        let elders = node_context
            .network_knowledge
            .elders()
            .iter()
            .map(|node_id| node_id.name())
            .collect::<BTreeSet<XorName>>();
        let fault_channels = {
            let tracker = FaultDetection::new(all_members, elders);
            // start FaultDetection in a new thread
            let faulty_nodes_receiver = Self::start_fault_detection(tracker, fault_cmds_channels.1);
            FaultChannels {
                cmds_sender: fault_cmds_channels.0,
                faulty_nodes_receiver,
            }
        };

        let flow_ctrl = Self {
            cmd_sender_channel: mutating_cmd_sender_channel.clone(),
            preprocess_cmd_sender_channel: read_only_event_sender.clone(),
            fault_channels,
            timestamps: PeriodicChecksTimestamps::now(),
        };

        // first start listening for msgs
        let cmd_channel_for_msgs = mutating_cmd_sender_channel.clone();

        // a clone for sending in updates to context
        let msg_handler_event_sender_clone = read_only_event_sender.clone();
        let read_only_event_sender_clone_for_processing = read_only_event_sender.clone();

        // simple pipe through of CommEvents - > ReadOnlyProcessingEvent
        let _handle = tokio::spawn(async move {
            while let Some(event) = incoming_msg_events.recv().await {
                let cmd = match event {
                    CommEvent::Error { node_id, error } => Cmd::HandleCommsError {
                        participant: Participant::from_node(node_id),
                        error,
                    },
                    CommEvent::Msg(MsgReceived {
                        sender,
                        wire_msg,
                        send_stream,
                    }) => {
                        let start = Instant::now();
                        if let Ok((header, dst, payload)) = wire_msg.serialize() {
                            let original_bytes_len = header.len() + dst.len() + payload.len();
                            let span =
                                trace_span!("handle_message", ?sender, msg_id = ?wire_msg.msg_id());
                            let _span_guard = span.enter();
                            trace!(
                                "{:?} from {sender:?} length {original_bytes_len}",
                                LogMarker::MsgReceived,
                            );
                        } else {
                            // this should be unreachable
                            trace!(
                                "{:?} from {sender:?}, unknown length due to serialization issues.",
                                LogMarker::MsgReceived,
                            );
                        }

                        debug!("Receiving msg parsing tooook: {:?}", start.elapsed());
                        /// Total concurrent msg parsing would be limited by cmd channel capacity
                        // let cmd_sender = cmd_channel.clone();

                        // let context = context.clone();
                        // is msg parsing just off the feedback loop??
                        Cmd::HandleMsg {
                            sender,
                            wire_msg,
                            send_stream,
                        }
                    }
                };

                // TODO: does this processing need to go off thread

                let msg_handler_event_sender_clone = read_only_event_sender.clone();

                let _handle = tokio::spawn(async move {
                    if let Err(e) = msg_handler_event_sender_clone
                        .send(ReadOnlyProcessingEvent::Cmd(cmd))
                        .await
                    {
                        warn!("MsgHandler event channel send failed: {e:?}");
                    }
                });
            }
        });

        Self::listen_for_msg_handling_events(
            node_context.clone(),
            read_only_cmd_event_reciever,
            cmd_channel_for_msgs.clone(),
        );

        // second do this until join
        let node = flow_ctrl
            .join_processing(
                node,
                &mut cmd_ctrl,
                join_retry_timeout,
                &mut incoming_mutating_cmds_from_apis,
                &rejoin_network_tx,
            )
            .await;

        let _handle = tokio::task::spawn(flow_ctrl.process_cmds_and_periodic_checks(
            node,
            cmd_ctrl,
            incoming_mutating_cmds_from_apis,
            rejoin_network_tx,
            read_only_event_sender_clone_for_processing,
        ));

        Self::send_out_data_for_replication(
            node_context.data_storage,
            data_replication_receiver,
            mutating_cmd_sender_channel.clone(),
        )
        .await;

        (cmd_channel_for_msgs, rejoin_network_rx)
    }

    /// This runs the join process until we detect we are a network node
    /// At that point it returns our MyNode instance for further use.
    async fn join_processing(
        &self,
        mut node: MyNode,
        cmd_ctrl: &mut CmdCtrl,
        join_retry_timeout: Duration,
        incoming_cmds_from_apis: &mut Receiver<(Cmd, Vec<usize>)>,
        rejoin_network_tx: &Sender<RejoinReason>,
    ) -> MyNode {
        let mut is_member = false;
        let preprocess_cmd_channel = self.preprocess_cmd_sender_channel.clone();

        // Fire cmd to join the network
        let mut last_join_attempt = Instant::now();
        self.send_join_network_cmd().await;

        loop {
            // first do any pending processing
            while let Ok((cmd, cmd_id)) = incoming_cmds_from_apis.try_recv() {
                trace!("Taking cmd off stack: {cmd:?}");
                cmd_ctrl
                    .process_cmd_job(
                        &mut node,
                        cmd,
                        cmd_id,
                        preprocess_cmd_channel.clone(),
                        rejoin_network_tx.clone(),
                    )
                    .await;
            }

            if is_member {
                debug!("we joined; breaking join loop!!!");
                break;
            }

            // second, check if we've joined... if not fire off cmds for that
            // this must come _after_ clearing the cmd channel
            if last_join_attempt.elapsed() > join_retry_timeout {
                last_join_attempt = Instant::now();
                debug!("we're not joined so firing off cmd");
                self.send_join_network_cmd().await;
            }

            // cheeck if we are a member
            // await for join retry time
            let our_name = node.info().name();
            is_member = node.network_knowledge.is_section_member(&our_name);

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        node
    }

    // Helper to send the TryJoinNetwork cmd
    async fn send_join_network_cmd(&self) {
        let cmd_channel_clone = self.preprocess_cmd_sender_channel.clone();
        // send the join message...
        if let Err(error) = cmd_channel_clone
            .send(ReadOnlyProcessingEvent::Cmd(Cmd::TryJoinNetwork))
            // .send((Cmd::TryJoinNetwork, vec![]))
            .await
            .map_err(|e| {
                error!("Failed join: {:?}", e);
                Error::JoinTimeout
            })
        {
            error!("Could not join the network: {error:?}");
        }
        debug!("Sent TryJoinNetwork command");
    }

    /// This is a never ending loop as long as the node is live.
    /// This loop processes cmds pushed via the CmdChannel and
    /// runs the periodic events internal to the node.
    async fn process_cmds_and_periodic_checks(
        mut self,
        mut node: MyNode,
        cmd_ctrl: CmdCtrl,
        mut incoming_cmds_from_apis: Receiver<(Cmd, Vec<usize>)>,
        rejoin_network_tx: Sender<RejoinReason>,
        read_only_processing: Sender<ReadOnlyProcessingEvent>,
    ) {
        // let cmd_channel = self.cmd_sender_channel.clone();
        // first do any pending processing
        while let Some((cmd, cmd_id)) = incoming_cmds_from_apis.recv().await {
            trace!("Taking cmd off stack: {cmd:?}");

            cmd_ctrl
                .process_cmd_job(
                    &mut node,
                    cmd,
                    cmd_id,
                    read_only_processing.clone(),
                    rejoin_network_tx.clone(),
                )
                .await;

            if let Err(e) = read_only_processing
                .send(ReadOnlyProcessingEvent::UpdateContext(node.context()))
                .await
            {
                warn!("Errrrrrrrrrrrrrrrrrrr {e}");
            };

            // also see if we need to do any of thissss
            self.perform_periodic_checks(&mut node).await;
        }
    }
    /// Listens on data_replication_receiver on a new thread, sorts and batches data, generating SendMsg Cmds
    async fn send_out_data_for_replication(
        node_data_storage: DataStorage,
        mut data_replication_receiver: Receiver<(Vec<DataAddress>, NodeId)>,
        cmd_channel: Sender<(Cmd, Vec<usize>)>,
    ) {
        // start a new thread to kick off data replication
        let _handle = tokio::task::spawn(async move {
            // is there a simple way to dedupe common data going to many nodes?
            // is any overhead reduction worth the increased complexity?
            while let Some((data_addresses, node_id)) = data_replication_receiver.recv().await {
                let send_cmd_channel = cmd_channel.clone();
                let data_storage = node_data_storage.clone();
                // move replication off thread so we don't block the receiver
                let _handle = tokio::task::spawn(async move {
                    debug!(
                        "{:?} Data {data_addresses:?} to: {node_id:?}",
                        LogMarker::SendingMissingReplicatedData,
                    );

                    let mut data_bundle = vec![];

                    for address in data_addresses.iter() {
                        match data_storage.get_from_local_store(address).await {
                            Ok(data) => {
                                data_bundle.push(data);
                            }
                            Err(error) => {
                                error!("Error getting {address:?} from local storage during data replication flow: {error:?}");
                            }
                        };
                    }
                    trace!("Sending out data batch to {node_id:?}");
                    let msg = NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(data_bundle));

                    let cmd =
                        Cmd::send_msg(msg, Recipients::Single(Participant::from_node(node_id)));
                    if let Err(error) = send_cmd_channel.send((cmd, vec![])).await {
                        error!("Failed to enqueue send msg command for replication of data batch to {node_id:?}: {error:?}");
                    }
                });
            }
        });
    }

    // starts a new thread to convert comm event to cmds
    fn listen_for_msg_handling_events(
        context: NodeContext,
        mut incoming_msg_events: Receiver<ReadOnlyProcessingEvent>,
        mutating_cmd_channel: CmdChannel,
    ) {
        // we'll update this as we go
        let mut context = context;

        // TODO: make this handle cmds itself... and we weither send to modifying loop
        // or here...
        let _handle = tokio::task::spawn(async move {
            while let Some(event) = incoming_msg_events.recv().await {
                let capacity = mutating_cmd_channel.capacity();

                if capacity < 30 {
                    warn!("CmdChannel capacity severely reduced");
                }
                if capacity == 0 {
                    error!("CmdChannel capacity exceeded. We cannot receive messages right now!");
                }

                debug!(
                    "CommEvent received: {event:?}. Current capacity on the CmdChannel: {:?}",
                    capacity
                );

                match event {
                    ReadOnlyProcessingEvent::UpdateContext(new_context) => {
                        context = new_context;
                        continue;
                    }
                    ReadOnlyProcessingEvent::Cmd(incoming_cmd) => {
                        let context = context.clone();
                        let mutating_cmd_channel = mutating_cmd_channel.clone();

                        // Go off thread for parsing and handling by default
                        // we only punt certain cmds back into the mutating channel
                        let _handle = tokio::spawn(async move {
                            let results = handle_cmd_off_thread_or_pass_to_mutating_channel(
                                incoming_cmd,
                                context.clone(),
                                mutating_cmd_channel.clone(),
                            )
                            .await?;
                            let mut offspring = results;

                            while !offspring.is_empty() {
                                let mut new_cmds = vec![];

                                for cmd in offspring {
                                    let cmds = handle_cmd_off_thread_or_pass_to_mutating_channel(
                                        cmd,
                                        context.clone(),
                                        mutating_cmd_channel.clone(),
                                    )
                                    .await?;
                                    new_cmds.extend(cmds);
                                    // TODO: extract this out into two cmd handler channels
                                }

                                offspring = new_cmds;
                            }
                            Ok::<(), Error>(())
                        });
                    }
                };
            }
        });
    }
}

async fn handle_cmd_off_thread_or_pass_to_mutating_channel(
    cmd: Cmd,
    context: NodeContext,
    mutating_cmd_channel: CmdChannel,
) -> Result<Vec<Cmd>, Error> {
    let mut new_cmds = vec![];
    match cmd {
        Cmd::HandleMsg {
            sender,
            wire_msg,
            send_stream,
        } => new_cmds.extend(MyNode::handle_msg(context, sender, wire_msg, send_stream).await?),
        Cmd::ProcessClientMsg {
            msg_id,
            msg,
            auth,
            sender,
            send_stream,
        } => {
            if let Some(stream) = send_stream {
                let fresh = MyNode::handle_client_msg_for_us(
                    context.clone(),
                    msg_id,
                    msg,
                    auth,
                    sender,
                    stream,
                )
                .await?;
                // let fresh = MyNode::process_cmd_with_context(cmd, context.clone()).await?;
                new_cmds.extend(fresh);
            } else {
                debug!("dropping client cmd w/ no response stream")
            }
        }
        Cmd::SendMsg {
            msg,
            msg_id,
            recipients,
        } => {
            let recipients = recipients.into_iter().map(NodeId::from).collect();

            MyNode::send_msg(msg, msg_id, recipients, context.clone())?;
        }
        Cmd::SendMsgEnqueueAnyResponse {
            msg,
            msg_id,
            recipients,
        } => {
            debug!("send msg enque cmd...?");
            MyNode::send_and_enqueue_any_response(msg, msg_id, context, recipients)?;
        }
        Cmd::SendNodeMsgResponse {
            msg,
            msg_id,
            correlation_id,
            node_id,
            send_stream,
        } => {
            if let Some(cmd) = MyNode::send_node_msg_response(
                msg,
                msg_id,
                correlation_id,
                node_id,
                context,
                send_stream,
            )
            .await?
            {
                new_cmds.push(cmd)
            }
        }
        Cmd::SendDataResponse {
            msg,
            msg_id,
            correlation_id,
            send_stream,
            client_id,
        } => {
            if let Some(x) = MyNode::send_data_response(
                msg,
                msg_id,
                correlation_id,
                send_stream,
                context.clone(),
                client_id,
            )
            .await?
            {
                new_cmds.push(x);
            }
        }
        Cmd::TrackNodeIssue { name, issue } => {
            context.track_node_issue(name, issue);
        }
        Cmd::SendAndForwardResponseToClient {
            wire_msg,
            targets,
            client_stream,
            client_id,
        } => {
            MyNode::send_and_forward_response_to_client(
                wire_msg,
                context.clone(),
                targets,
                client_stream,
                client_id,
            )?;
        }
        Cmd::UpdateCaller {
            caller,
            correlation_id,
            kind,
            section_tree_update,
        } => {
            info!("Sending ae response msg for {correlation_id:?}");

            new_cmds.push(Cmd::send_network_msg(
                NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                    section_tree_update,
                    kind,
                }),
                Recipients::Single(Participant::from_node(caller)), // we're doing a mapping again here.. but this is a necessary evil while transitioning to more clarity and type safety, i.e. TO BE FIXED
            ));
            // Ok(vec![Cmd::send_network_msg(
            //     NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
            //         section_tree_update,
            //         kind,
            //     }),
            //     Recipients::Single(Participant::from_node(caller)), // we're doing a mapping again here.. but this is a necessary evil while transitioning to more clarity and type safety, i.e. TO BE FIXED
            // )])
        }
        Cmd::UpdateCallerOnStream {
            caller,
            msg_id,
            kind,
            section_tree_update,
            correlation_id,
            stream,
        } => {
            if let Some(cmd) = MyNode::send_ae_response(
                AntiEntropyMsg::AntiEntropy {
                    kind,
                    section_tree_update,
                },
                msg_id,
                caller,
                correlation_id,
                stream,
                context,
            )
            .await?
            {
                new_cmds.push(cmd);
            }
        }
        _ => {
            debug!("child process not handled in thread: {cmd:?}");
            if let Err(error) = mutating_cmd_channel.send((cmd, vec![])).await {
                error!("Error sending msg onto cmd channel {error:?}");
            }
        }
    }

    Ok(new_cmds)
}
