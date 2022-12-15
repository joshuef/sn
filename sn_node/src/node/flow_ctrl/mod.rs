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
pub(super) mod dysfunction;
mod periodic_checks;

#[cfg(test)]
pub(crate) mod tests;
pub(crate) use cmd_ctrl::CmdCtrl;

use crate::comm::MsgFromPeer;
use crate::node::{
    flow_ctrl::{
        cmds::Cmd,
        dysfunction::{DysCmds, DysfunctionChannels},
    },
    messaging::Peers,
    MyNode, Result, STANDARD_CHANNEL_SIZE,
};
use periodic_checks::PeriodicChecksTimestamps;
use sn_dysfunction::DysfunctionDetection;
use sn_interface::{
    messaging::system::{NodeDataCmd, NodeMsg},
    types::{log_markers::LogMarker, DataAddress, Peer},
};

use super::DataStorage;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use xor_name::XorName;

/// Sent via the rejoin_network_tx to start the bootstrap process again
#[derive(Debug)]
pub struct RejoinNetwork;

/// Listens for incoming msgs and forms Cmds for each,
/// Periodically triggers other Cmd Processes (eg health checks, dysfunction etc)
pub(crate) struct FlowCtrl {
    node: Arc<RwLock<MyNode>>,
    cmd_sender_channel: mpsc::Sender<(Cmd, Vec<usize>)>,
    dysfunction_channels: DysfunctionChannels,
    timestamps: PeriodicChecksTimestamps,
}

impl FlowCtrl {
    /// Constructs a FlowCtrl instance, spawnning a task which starts processing messages,
    /// returning the channel where it can receive commands on
    pub(crate) async fn start(
        cmd_ctrl: CmdCtrl,
        mut incoming_msg_events: mpsc::Receiver<MsgFromPeer>,
        data_replication_receiver: mpsc::Receiver<(Vec<DataAddress>, Peer)>,
        dysfunction_cmds_channels: (mpsc::Sender<DysCmds>, mpsc::Receiver<DysCmds>),
    ) -> (
        mpsc::Sender<(Cmd, Vec<usize>)>,
        mpsc::Receiver<RejoinNetwork>,
    ) {
        debug!("[NODE READ]: flowctrl node context lock got");
        let node_context = cmd_ctrl.node().read().await.context();
        let (cmd_sender_channel, mut incoming_cmds_from_apis) =
            mpsc::channel(STANDARD_CHANNEL_SIZE);
        let (rejoin_network_tx, rejoin_network_rx) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let node_identifier = node_context.info.name();

        let dysfunction_channels = {
            let dysfunction = DysfunctionDetection::new(
                node_context
                    .network_knowledge
                    .members()
                    .iter()
                    .map(|peer| peer.name())
                    .collect::<Vec<XorName>>(),
            );
            // start DysfunctionDetection in a new thread
            let dysfunctional_nodes_receiver =
                Self::start_dysfunction_detection(dysfunction, dysfunction_cmds_channels.1);
            DysfunctionChannels {
                cmds_sender: dysfunction_cmds_channels.0,
                dys_nodes_receiver: dysfunctional_nodes_receiver,
            }
        };

        let flow_ctrl = Self {
            node: cmd_ctrl.node(),
            cmd_sender_channel: cmd_sender_channel.clone(),
            dysfunction_channels,
            timestamps: PeriodicChecksTimestamps::now(),
        };

        let _ =
            tokio::task::spawn(
                async move { flow_ctrl.process_messages_and_periodic_checks().await },
            );

        let cmd_channel = cmd_sender_channel.clone();
        let cmd_channel_for_msgs = cmd_sender_channel.clone();

        let node_arc_for_replication = cmd_ctrl.node();
        // start a new thread to kick off incoming cmds
        let _ = tokio::task::spawn(async move {
            // Get a stable identifier for statemap naming. This is NOT the node's current name.
            // It's the initial name... but will not change for the entire statemap
            while let Some((cmd, cmd_id)) = incoming_cmds_from_apis.recv().await {
                cmd_ctrl
                    .process_cmd_job(
                        cmd,
                        cmd_id,
                        node_identifier,
                        cmd_channel.clone(),
                        rejoin_network_tx.clone(),
                    )
                    .await
            }
        });

        Self::send_out_data_for_replication(
            node_arc_for_replication,
            node_context.data_storage,
            data_replication_receiver,
            cmd_sender_channel.clone(),
        )
        .await;

        // start a new thread to convert msgs to Cmds
        let _ = tokio::task::spawn(async move {
            while let Some(peer_msg) = incoming_msg_events.recv().await {
                let cmd = match Self::handle_new_msg_event(peer_msg).await {
                    Ok(cmd) => cmd,
                    Err(error) => {
                        error!("Could not handle incoming msg event: {error:?}");
                        continue;
                    }
                };

                if let Err(error) = cmd_channel_for_msgs.send((cmd.clone(), vec![])).await {
                    error!("Error sending msg onto cmd channel {error:?}");
                }
            }
        });

        (cmd_sender_channel, rejoin_network_rx)
    }

    /// Listens on data_replication_receiver on a new thread, sorts and batches data, generating SendMsg Cmds
    async fn send_out_data_for_replication(
        node_arc: Arc<RwLock<MyNode>>,
        node_data_storage: DataStorage,
        mut data_replication_receiver: mpsc::Receiver<(Vec<DataAddress>, Peer)>,
        cmd_channel_for_data_replication: mpsc::Sender<(Cmd, Vec<usize>)>,
    ) {
        // start a new thread to kick off data replication
        let _ = tokio::task::spawn(async move {
            // is there a simple way to dedupe common data going to many peers?
            // is any overhead reduction worth the increased complexity?
            while let Some((mut data_addresses, peer)) = data_replication_receiver.recv().await {
                // sort the addresses so that we're batching out closest data first
                data_addresses.sort_by(|lhs, rhs| peer.name().cmp_distance(lhs.name(), rhs.name()));

                // TODO: To what extent might we want to bundle these messages?
                let data_batch_size = 10; // at most bundle 10 pieces of data together into one message

                let mut data_batch = vec![];
                debug!(
                    "{:?} Data {:?} to: {:?}",
                    LogMarker::SendingMissingReplicatedData,
                    data_addresses,
                    peer,
                );

                for (i, address) in data_addresses.iter().enumerate() {
                    // enumerate is 0 indexed, let's correct for that for counting
                    // and then comparing to data_addresses
                    let iteration = i + 1;
                    match node_data_storage.get_from_local_store(address).await {
                        Ok(data) => {
                            data_batch.push(data);
                        }
                        Err(error) => {
                            error!("Error getting {address:?} from local storage during data replication flow: {error:?}");
                        }
                    };

                    // if we hit a multiple of the batch limit or we're at the last data to send...
                    if data_batch.len() == data_batch_size || iteration == data_addresses.len() {
                        trace!("Sending out data batch on i:{iteration:?} to {peer:?}");
                        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateData(data_batch));

                        let node_context = node_arc.read().await.context();

                        let cmd = Cmd::send_msg(msg, Peers::Single(peer), node_context.clone());
                        if let Err(error) =
                            cmd_channel_for_data_replication.send((cmd, vec![])).await
                        {
                            error!("Failed to enqueue send msg command for replication of data batch to {peer:?}: {error:?}");
                        }
                        data_batch = vec![];
                    }
                }
            }
        });
    }

    /// This is a never ending loop as long as the node is live.
    /// This loop drives the periodic events internal to the node.
    async fn process_messages_and_periodic_checks(mut self) {
        // the internal process loop
        loop {
            self.perform_periodic_checks().await;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    // Listen for a new incoming connection event and handle it.
    async fn handle_new_msg_event(msg: MsgFromPeer) -> Result<Cmd> {
        let MsgFromPeer {
            sender,
            wire_msg,
            send_stream,
        } = msg;

        let (header, dst, payload) = wire_msg.serialize()?;
        let original_bytes_len = header.len() + dst.len() + payload.len();

        let span = trace_span!("handle_message", ?sender, msg_id = ?wire_msg.msg_id());
        let _span_guard = span.enter();

        trace!(
            "{:?} from {sender:?} length {original_bytes_len}",
            LogMarker::DispatchHandleMsgCmd,
        );

        Ok(Cmd::HandleMsg {
            origin: sender,
            wire_msg,
            send_stream,
        })
    }
}
