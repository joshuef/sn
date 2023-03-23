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

use super::{DataStorage, Result};
use periodic_checks::PeriodicChecksTimestamps;

use crate::node::{
    api::NodeEvent,
    flow_ctrl::{
        cmds::Cmd,
        fault_detection::{FaultChannels, FaultsCmd},
    },
    messaging::Recipients,
    CmdChannel, Error, MyNode, NodeContext, NodeEventsChannel, STANDARD_CHANNEL_SIZE,
};

use sn_comms::{CommEvent, MsgReceived};
use sn_fault_detection::FaultDetection;
use sn_interface::{
    messaging::system::{NodeDataCmd, NodeMsg},
    messaging::{AntiEntropyMsg, NetworkMsg},
    network_knowledge::RelocationState,
    types::{log_markers::LogMarker, ClientId, DataAddress, NodeId, Participant},
};

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use xor_name::XorName;

/// Sent via the rejoin_network_tx to restart the join process.
/// This would only occur when joins are not allowed, or non-recoverable states.
#[derive(Debug, Clone)]
pub enum RejoinReason {
    /// Happens when trying to join; we will wait a moment and then try again.
    /// NB: Relocated nodes that try to join, are accepted even if joins are disallowed.
    JoinsDisallowed,
    /// Happens when already part of the network; we need to start from scratch.
    RemovedFromSection,
}

/// Flow ctrl of node cmds by . This determines if to run in blocking
/// context or no
#[derive(Debug)]
pub(crate) enum FlowCtrlCmd {
    /// Process this cmd, in blocking thread or off-thread where possible
    Handle(Cmd),
    /// Updates the node context passed to off-thread Cmds
    UpdateContext(NodeContext),
}

/// Listens for incoming msgs and forms Cmds for each,
/// Periodically triggers other Cmd Processes (eg health checks, fault detection etc)
pub(crate) struct FlowCtrl {
    preprocess_cmd_sender_channel: Sender<FlowCtrlCmd>,
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
        incoming_msg_events: Receiver<CommEvent>,
        data_replication_receiver: Receiver<(Vec<DataAddress>, NodeId)>,
        fault_cmds_channels: (Sender<FaultsCmd>, Receiver<FaultsCmd>),
    ) -> Result<CmdChannel> {
        let node_context = node.context();
        let (blocking_cmd_sender_channel, mut blocking_cmds_receiver) =
            mpsc::channel(STANDARD_CHANNEL_SIZE);

        // Our channel to process _all_ cmds. If it can, they are processed off thread with latest context,
        // otherwise they are sent to the blocking process channel
        let (flow_ctrl_cmd_sender, flow_ctrl_cmd_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        // separate channel to update the periodics loop
        let (context_updater_for_periodic, context_receiver_for_periodics) =
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
            preprocess_cmd_sender_channel: flow_ctrl_cmd_sender.clone(),
            fault_channels,
            timestamps: PeriodicChecksTimestamps::now(),
        };

        // incoming events from comms
        Self::handle_comm_events(incoming_msg_events, flow_ctrl_cmd_sender.clone());

        // handle all incoming cmds, make decisions about pushing them off thread
        // or onto the blocking loop
        Self::listen_for_flow_ctrl_cmds(
            node_context.clone(),
            flow_ctrl_cmd_sender.clone(),
            flow_ctrl_cmd_receiver,
            blocking_cmd_sender_channel.clone(),
            node.node_events_sender.clone(),
        );

        // second do this until join
        let node = flow_ctrl
            .join_processing(
                node,
                &mut cmd_ctrl,
                join_retry_timeout,
                flow_ctrl_cmd_sender.clone(), // sending of updates to context
                context_updater_for_periodic.clone(), // sending of updates to context
                &mut blocking_cmds_receiver,
            )
            .await?;

        // set up the periodic check loop
        flow_ctrl.listen_for_flow_ctrl_cmds_for_periodics(
            node.context(),
            context_receiver_for_periodics,
        );

        let _handle = tokio::task::spawn(FlowCtrl::process_blocking_cmds(
            node,
            cmd_ctrl,
            blocking_cmds_receiver,
            flow_ctrl_cmd_sender.clone(), // sending of updates to context
            context_updater_for_periodic.clone(), // sending of updates to context
        ));

        Self::send_out_data_for_replication(
            node_context.data_storage,
            data_replication_receiver,
            flow_ctrl_cmd_sender.clone(),
        )
        .await;

        Ok(blocking_cmd_sender_channel)
    }

    /// This runs the join process until we detect we are a network node
    /// At that point it returns our MyNode instance for further use.
    async fn join_processing(
        &self,
        mut node: MyNode,
        cmd_ctrl: &mut CmdCtrl,
        join_retry_timeout: Duration,
        flow_ctrl_cmd_processing: Sender<FlowCtrlCmd>,
        context_udpated_for_periodics: Sender<FlowCtrlCmd>,
        blocking_cmds_receiver: &mut Receiver<(Cmd, Vec<usize>)>,
    ) -> Result<MyNode> {
        let mut is_member = false;
        let preprocess_cmd_channel = self.preprocess_cmd_sender_channel.clone();

        // Fire cmd to join the network
        let mut last_join_attempt = Instant::now();
        self.send_join_network_cmd().await;

        loop {
            // first do any pending processing
            while let Ok((cmd, cmd_id)) = blocking_cmds_receiver.try_recv() {
                trace!("Taking cmd off stack: {cmd:?}");
                cmd_ctrl
                    .process_blocking_cmd_job(
                        &mut node,
                        cmd,
                        cmd_id,
                        preprocess_cmd_channel.clone(),
                    )
                    .await;

                // update our context in flow ctrl cmd processor with each cmd
                flow_ctrl_cmd_processing
                    .send(FlowCtrlCmd::UpdateContext(node.context()))
                    .await
                    .map_err(|e| {
                        Error::TokioChannel(format!("cmd_processing send failed {:?}", e))
                    })?;

                // update our context in periodic flow ctrl loop with each cmd
                context_udpated_for_periodics
                    .send(FlowCtrlCmd::UpdateContext(node.context()))
                    .await
                    .map_err(|e| {
                        Error::TokioChannel(format!("cmd_processing send failed {:?}", e))
                    })?;
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

        Ok(node)
    }

    // Helper to send the TryJoinNetwork cmd
    async fn send_join_network_cmd(&self) {
        let cmd_channel_clone = self.preprocess_cmd_sender_channel.clone();
        // send the join message...
        if let Err(error) = cmd_channel_clone
            .send(FlowCtrlCmd::Handle(Cmd::TryJoinNetwork))
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
    async fn process_blocking_cmds(
        mut node: MyNode,
        cmd_ctrl: CmdCtrl,
        mut incoming_cmds_from_apis: Receiver<(Cmd, Vec<usize>)>,
        cmd_processing: Sender<FlowCtrlCmd>,
        context_updater_for_periodics: Sender<FlowCtrlCmd>,
    ) -> Result<()> {
        let mut current_context = node.context();
        // first do any pending processing
        while let Some((cmd, cmd_id)) = incoming_cmds_from_apis.recv().await {
            trace!("Taking cmd off stack: {cmd:?}");

            cmd_ctrl
                .process_blocking_cmd_job(&mut node, cmd, cmd_id, cmd_processing.clone())
                .await;

            // See if any changes to context mean we have finished a relocation
            // check if we can join the dst section

            if let RelocationState::ReadyToJoinNewSection(proof) = &node.relocation_state {
                let prev_name = proof.previous_name();
                let new_name = proof.new_name();
                if node.network_knowledge.is_section_member(&new_name) {
                    node.node_events_sender.broadcast(NodeEvent::RelocateEnd);
                    info!(
                        "{} for previous node: {:?} of age: {:?}: The new name is: {:?}, and new age is: {:?} (and pass in context age: {:?})",
                        LogMarker::RelocateEnd,
                        prev_name,
                        proof.previous_age(),
                        new_name,
                        proof.new_age(),
                        node.info().age()
                    );
                    info!("We've joined a section, dropping the relocation proof.");
                    node.relocation_state = RelocationState::NoRelocation;
                }
            }

            let context_was_updated = node.update_context(&mut current_context);

            if context_was_updated {
                // Finally update our context in read only processor with each cmd
                cmd_processing
                    .send(FlowCtrlCmd::UpdateContext(current_context.clone()))
                    .await
                    .map_err(|e| {
                        Error::TokioChannel(format!("cmd_processing send failed {:?}", e))
                    })?;

                // Finally update our context in periodic loop
                context_updater_for_periodics
                    .send(FlowCtrlCmd::UpdateContext(current_context.clone()))
                    .await
                    .map_err(|e| {
                        Error::TokioChannel(format!("cmd_processing send failed {:?}", e))
                    })?;
            }
        }

        Ok(())
    }

    /// Listens on data_replication_receiver on a new thread, sorts and batches data, generating SendMsg Cmds
    async fn send_out_data_for_replication(
        node_data_storage: DataStorage,
        mut data_replication_receiver: Receiver<(Vec<DataAddress>, NodeId)>,
        flow_ctrl_cmd_channel: Sender<FlowCtrlCmd>,
    ) {
        // start a new thread to kick off data replication
        let _handle = tokio::task::spawn(async move {
            // is there a simple way to dedupe common data going to many nodes?
            // is any overhead reduction worth the increased complexity?
            while let Some((data_addresses, node_id)) = data_replication_receiver.recv().await {
                let flow_ctrl_cmd_channel = flow_ctrl_cmd_channel.clone();
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
                    if let Err(error) = flow_ctrl_cmd_channel.send(FlowCtrlCmd::Handle(cmd)).await {
                        error!("Failed to enqueue send msg command for replication of data batch to {node_id:?}: {error:?}");
                    }
                });
            }
        });
    }

    /// Spawns a task to listen for flow ctrl cmds.
    fn listen_for_flow_ctrl_cmds(
        context: NodeContext,
        flow_ctrl_cmd_sender: Sender<FlowCtrlCmd>,
        mut flow_ctrl_cmd_reciever: Receiver<FlowCtrlCmd>,
        blocking_cmd_channel: CmdChannel,
        node_events_sender: NodeEventsChannel,
    ) {
        // we'll update this as we go
        let mut context = context;

        // TODO: make this handle cmds itself... and we either send to modifying loop
        // or here...
        let _handle = tokio::task::spawn(async move {
            while let Some(cmd) = flow_ctrl_cmd_reciever.recv().await {
                let capacity = flow_ctrl_cmd_sender.capacity();

                if capacity < 30 {
                    warn!("FlowCtrlCmdChannel capacity severely reduced");
                }
                if capacity == 0 {
                    error!("FlowCtrlCmdChannel capacity exceeded. We cannot receive messages right now!");
                }

                debug!(
                    "Current capacity on the FlowCtrlCmdChannel: {:?}; CommEvent FlowCtrlCmd: {cmd:?}.",
                    capacity
                );

                match cmd {
                    FlowCtrlCmd::UpdateContext(new_context) => {
                        node_events_sender
                            .broadcast(NodeEvent::ContextUpdated(new_context.clone()));
                        context = new_context;
                        continue;
                    }
                    FlowCtrlCmd::Handle(incoming_cmd) => {
                        let blocking_cmd_channel = blocking_cmd_channel.clone();
                        let flow_ctrl_cmd_sender = flow_ctrl_cmd_sender.clone();

                        if let Err(error) = handle_cmd_off_thread(
                            incoming_cmd,
                            &context,
                            flow_ctrl_cmd_sender.clone(),
                            blocking_cmd_channel.clone(),
                        )
                        .await
                        {
                            error!("HandleCmd: {error:?}");
                        };
                    }
                };
            }
        });
    }

    /// Spawns a task to listen for flow ctrl cmds.
    fn listen_for_flow_ctrl_cmds_for_periodics(
        mut self,
        context: NodeContext,
        mut context_receiver_for_periodics: Receiver<FlowCtrlCmd>,
    ) {
        // we'll update this as we go
        let mut context = context;

        // now loop and check periodics with context...
        let _handle = tokio::task::spawn(async move {
            loop {
                while let Ok(FlowCtrlCmd::UpdateContext(new_context)) =
                    context_receiver_for_periodics.try_recv()
                {
                    debug!("Periodic context updated");
                    context = new_context;
                }

                self.perform_periodic_checks(&context).await;

                tokio::time::sleep(Duration::from_millis(100)).await
            }
        });
    }

    /// Simple mapping of of CommEvents -> HandleMsg / HandleCommsError.
    fn handle_comm_events(
        mut incoming_msg_events: Receiver<CommEvent>,
        flow_ctrl_cmd_sender: Sender<FlowCtrlCmd>,
    ) {
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
                        let span =
                            trace_span!("handle_message", ?sender, msg_id = ?wire_msg.msg_id());
                        let _span_guard = span.enter();

                        Cmd::HandleMsg {
                            sender,
                            wire_msg,
                            send_stream,
                        }
                    }
                };

                if let Err(e) = flow_ctrl_cmd_sender.send(FlowCtrlCmd::Handle(cmd)).await {
                    warn!("MsgHandler event channel send failed: {e:?}");
                }
            }
        });
    }
}

/// Handles Cmd, either spawn a fresh thread if non blocking, or pass to the blocking processor thread
async fn handle_cmd_off_thread(
    cmd: Cmd,
    context: &NodeContext,
    flow_ctrl_cmd_sender: Sender<FlowCtrlCmd>,
    blocking_cmd_channel: CmdChannel,
) -> Result<(), Error> {
    let (_, blocking_cmd) = process_cmd_non_blocking(cmd, context, flow_ctrl_cmd_sender).await?;

    if let Some(blocking_cmd) = blocking_cmd {
        if let Err(error) = blocking_cmd_channel.send((blocking_cmd, vec![])).await {
            error!("Error sending msg onto cmd channel {error:?}");
        }
        // early exit, no cmds produced here...
        return Ok(());
    }
    Ok(())
}

// If a command is handled via a new task, then we return the JoinHandle for it. This is primarily used in tests.
// Also optionally return the unprocessed command if it should be processed in a blocking fashion, this should be sent
// to the blocking CmdChannel
async fn process_cmd_non_blocking(
    cmd: Cmd,
    context: &NodeContext,
    flow_ctrl_cmd_sender: Sender<FlowCtrlCmd>,
) -> Result<(Option<JoinHandle<Result<(), Error>>>, Option<Cmd>), Error> {
    let start = Instant::now();

    let cmd_string = format!("{:?}", cmd);

    let handle = match cmd {
        Cmd::HandleMsg {
            sender,
            wire_msg,
            send_stream,
        } => {
            let network_knowledge = context.network_knowledge.clone();
            let our_name = context.name.clone();
            let is_elder = context.is_elder;
            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];
                new_cmds.extend(
                    MyNode::handle_msg(
                        &network_knowledge,
                        our_name,
                        is_elder,
                        sender,
                        wire_msg,
                        send_stream,
                    )
                    .await?,
                );

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });

            Some(handle)
        }
        Cmd::ProcessClientMsg {
            msg_id,
            msg,
            auth,
            client_id,
            send_stream,
        } => {
            let network_knowledge = context.network_knowledge.clone();
            let our_name = context.name.clone();
            let data_storage = context.data_storage.clone();
            let is_elder = context.is_elder;
            let joins_allowed = context.joins_allowed;
            let reward_key = context.reward_key;
            let store_cost = context.store_cost;
            let ed_keypair = context.keypair.clone();
            let joins_allowed_until_split = context.joins_allowed_until_split;
            let section_keys_provider = context.section_keys_provider.clone();

            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];
                // new_cmds.extend(MyNode::handle_msg(&context.network_knowledge, context.name, sender, wire_msg, send_stream).await?);
                if let Some(stream) = send_stream {
                    new_cmds.extend(
                        MyNode::handle_client_msg_for_us(
                            msg_id,
                            msg,
                            auth,
                            client_id,
                            stream,
                            our_name,
                            &network_knowledge,
                            ed_keypair,
                            &section_keys_provider,
                            reward_key,
                            store_cost,
                            joins_allowed,
                            joins_allowed_until_split,
                            is_elder,
                            data_storage,
                        )
                        .await?,
                    );
                } else {
                    debug!("dropping client cmd w/ no response stream")
                }

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::SendMsg {
            msg,
            msg_id,
            recipients,
        } => {
            let comm = context.comm.clone();
            let network_knowledge = context.network_knowledge.clone();
            let our_name = context.name.clone();

            let handle = tokio::spawn(async move {
                let recipients = recipients.into_iter().map(NodeId::from).collect();
                MyNode::send_msg(msg, msg_id, recipients, our_name, network_knowledge, comm)?;

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::SendMsgEnqueueAnyResponse {
            msg,
            msg_id,
            recipients,
        } => {
            let comm = context.comm.clone();
            let current_section_key = context.network_knowledge.section_key();
            let our_name = context.name.clone();
            let handle = tokio::spawn(async move {
                MyNode::send_and_enqueue_any_response(
                    msg,
                    msg_id,
                    our_name,
                    current_section_key,
                    comm,
                    recipients,
                )?;
                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::SendNodeMsgResponse {
            msg,
            msg_id,
            correlation_id,
            node_id,
            send_stream,
        } => {
            let current_section_key = context.network_knowledge.section_key();
            let our_name = context.name.clone();

            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];

                new_cmds.extend(
                    MyNode::send_node_msg_response(
                        msg,
                        msg_id,
                        correlation_id,
                        node_id,
                        our_name,
                        current_section_key,
                        send_stream,
                    )
                    .await?,
                );

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::SendDataResponse {
            msg,
            msg_id,
            correlation_id,
            send_stream,
            client_id,
        } => {
            let current_section_key = context.network_knowledge.section_key();
            let our_name = context.name.clone();

            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];
                new_cmds.extend(
                    MyNode::send_data_response(
                        msg,
                        msg_id,
                        correlation_id,
                        send_stream,
                        our_name,
                        current_section_key,
                        client_id,
                    )
                    .await?,
                );

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::TrackNodeIssue { name, issue } => {
            context.track_node_issue(name, issue);

            None
        }
        Cmd::SendAndForwardResponseToClient {
            wire_msg,
            targets,
            client_stream,
            client_id,
        } => {
            let comm = context.comm.clone();
            let current_section_key = context.network_knowledge.section_key();

            let handle = tokio::spawn(async move {
                MyNode::send_and_forward_response_to_client(
                    wire_msg,
                    comm,
                    current_section_key,
                    targets,
                    client_stream,
                    client_id,
                )?;

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::UpdateCaller {
            caller,
            correlation_id,
            kind,
            section_tree_update,
        } => {
            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];
                info!("Sending ae response msg for {correlation_id:?}");
                new_cmds.push(Cmd::send_network_msg(
                    NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                        section_tree_update,
                        kind,
                    }),
                    Recipients::Single(Participant::from_node(caller)), // we're doing a mapping again here.. but this is a necessary evil while transitioning to more clarity and type safety, i.e. TO BE FIXED
                ));

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::UpdateCallerOnStream {
            caller,
            msg_id,
            kind,
            section_tree_update,
            correlation_id,
            stream,
        } => {
            let current_section_key = context.network_knowledge.section_key();
            let our_name = context.name.clone();

            let handle = tokio::spawn(async move {
                let mut new_cmds = vec![];

                new_cmds.extend(
                    MyNode::send_ae_response(
                        AntiEntropyMsg::AntiEntropy {
                            kind,
                            section_tree_update,
                        },
                        msg_id,
                        caller,
                        correlation_id,
                        stream,
                        our_name,
                        current_section_key,
                    )
                    .await?,
                );

                for cmd in new_cmds {
                    flow_ctrl_cmd_sender
                        .send(FlowCtrlCmd::Handle(cmd))
                        .await
                        .map_err(|_| {
                            Error::TokioChannel(
                                "Putting fresh cmds in FlowCtrlCmd sender failed".to_string(),
                            )
                        })?;
                }

                Ok::<(), Error>(())
            });
            Some(handle)
        }
        Cmd::ProcessNodeMsg {
            msg_id,
            msg,
            node_id,
            send_stream,
        } => {
            match msg {
                NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data)) => {
                    // NB: We would only reach here if this is a Spentbook cmd.
                    trace!(
                        "Attempting to store data locally (off-thread): {:?}",
                        data.address()
                    );
                    let Some(stream) = send_stream else {
                        return Err(Error::NoClientResponseStream)
                    };
                    // NB!! `sender` is actually a node here and should not be casted to ClientId! But since we are reusing the
                    // `store_data_and_respond` fn which is used when a forwarded client cmd comes in, we have to cast to ClientId here.. TO BE FIXED.
                    let sender = ClientId::from(Participant::from_node(node_id));

                    let network_knowledge = context.network_knowledge.clone();
                    let node_keypair = context.keypair.clone();
                    let data_storage = context.data_storage.clone();
                    let is_elder = context.is_elder;
                    let joins_allowed = context.joins_allowed;
                    let joins_allowed_until_split = context.joins_allowed_until_split;
                    let handle = tokio::spawn(async move {
                        // store data and respond w/ack on the response stream
                        let new_cmds = MyNode::store_data_and_respond(
                            &network_knowledge,
                            node_keypair,
                            data_storage,
                            joins_allowed_until_split,
                            joins_allowed,
                            is_elder,
                            data,
                            stream,
                            sender,
                            msg_id,
                        )
                        .await?;

                        for cmd in new_cmds {
                            flow_ctrl_cmd_sender
                                .send(FlowCtrlCmd::Handle(cmd))
                                .await
                                .map_err(|_| {
                                    Error::TokioChannel(
                                        "Putting fresh cmds in FlowCtrlCmd sender failed"
                                            .to_string(),
                                    )
                                })?;
                        }

                        Ok::<(), Error>(())
                    });
                    Some(handle)
                }
                NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(data_collection)) => {
                    info!("ReplicateDataBatch MsgId: {:?}", msg_id);
                    let network_knowledge = context.network_knowledge.clone();
                    let node_keypair = context.keypair.clone();
                    let data_storage = context.data_storage.clone();
                    let joins_allowed = context.joins_allowed;

                    let handle = tokio::spawn(async move {
                        let cmds = MyNode::replicate_data_batch(
                            node_keypair,
                            data_storage,
                            &network_knowledge,
                            joins_allowed,
                            node_id,
                            data_collection,
                        )
                        .await?;

                        for cmd in cmds {
                            flow_ctrl_cmd_sender
                                .send(FlowCtrlCmd::Handle(cmd))
                                .await
                                .map_err(|_| {
                                    Error::TokioChannel(
                                        "Putting fresh cmds in FlowCtrlCmd sender failed"
                                            .to_string(),
                                    )
                                })?;
                        }

                        Ok::<(), Error>(())
                    });
                    Some(handle)
                }
                NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(
                    known_data_addresses,
                )) => {
                    let network_knowledge = context.network_knowledge.clone();
                    let data_storage = context.data_storage.clone();

                    let handle = tokio::spawn(async move {
                        info!(
                            "{:?} MsgId: {:?}",
                            LogMarker::RequestForAnyMissingData,
                            msg_id
                        );

                        if let Some(cmd) = MyNode::get_missing_data_for_node(
                            data_storage,
                            &network_knowledge,
                            node_id,
                            known_data_addresses,
                        )
                        .await
                        {
                            flow_ctrl_cmd_sender
                                .send(FlowCtrlCmd::Handle(cmd))
                                .await
                                .map_err(|_| {
                                    Error::TokioChannel(
                                        "Putting fresh cmds in FlowCtrlCmd sender failed"
                                            .to_string(),
                                    )
                                })?;
                        }

                        Ok::<(), Error>(())
                    });
                    Some(handle)
                }
                NodeMsg::DkgAE(session_id) => {
                    let dkg_voter = context.dkg_voter.clone();
                    let handle = tokio::spawn(async move {
                        trace!("Handling msg: DkgAE s{} from {}", session_id.sh(), node_id);
                        let cmds = MyNode::handle_dkg_anti_entropy_request(
                            dkg_voter, session_id, node_id,
                        )?;

                        for cmd in cmds {
                            flow_ctrl_cmd_sender
                                .send(FlowCtrlCmd::Handle(cmd))
                                .await
                                .map_err(|_| {
                                    Error::TokioChannel(
                                        "Putting fresh cmds in FlowCtrlCmd sender failed"
                                            .to_string(),
                                    )
                                })?;
                        }

                        Ok::<(), Error>(())
                    });
                    Some(handle)
                }
                NodeMsg::MembershipAE(gen) => {
                    let membership = context.membership.clone();
                    let handle = tokio::spawn(async move {
                        if let Some(cmd) = MyNode::handle_membership_anti_entropy_request(
                            &membership,
                            node_id,
                            gen,
                        ) {
                            flow_ctrl_cmd_sender
                                .send(FlowCtrlCmd::Handle(cmd))
                                .await
                                .map_err(|_| {
                                    Error::TokioChannel(
                                        "Putting fresh cmds in FlowCtrlCmd sender failed"
                                            .to_string(),
                                    )
                                })?;
                        }
                        Ok::<(), Error>(())
                    });
                    Some(handle)
                }
                msg => {
                    trace!("Node msg not handled off thread, sending to blocking channel: {msg:?}");

                    let cmd = Cmd::ProcessNodeMsg {
                        msg_id,
                        msg,
                        node_id,
                        send_stream,
                    };

                    return Ok((None, Some(cmd)));
                }
            }
        }
        _ => {
            debug!("process cannot be handled off thread: {cmd:?}");
            return Ok((None, Some(cmd)));
        }
    };

    debug!(
        "Off-thread handling of Cmd took {:?}: {cmd_string:?}",
        start.elapsed()
    );

    Ok((handle, None))
}
