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

use crate::comm::MsgEvent;
use crate::node::{flow_ctrl::cmds::Cmd, messaging::Peers, Error, Node, Result};

use sn_interface::{
    messaging::{
        data::{DataQuery, DataQueryVariant, ServiceMsg},
        system::{NodeCmd, SystemMsg},
        AuthorityProof, MsgId, ServiceAuth, WireMsg,
    },
    types::{log_markers::LogMarker, ChunkAddress, PublicKey, Signature},
};

use signature::Signer;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, RwLock},
    task::{self, JoinHandle},
    time::MissedTickBehavior,
};

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const MISSING_VOTE_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(feature = "back-pressure")]
const BACKPRESSURE_INTERVAL: Duration = Duration::from_secs(60);
const SECTION_PROBE_INTERVAL: Duration = Duration::from_secs(300);
const LINK_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
const DATA_BATCH_INTERVAL: Duration = Duration::from_millis(50);
const DYSFUNCTION_CHECK_INTERVAL: Duration = Duration::from_secs(5);
// 30 adult nodes checked per minute., so each node should be queried 10x in 10 mins
// Which should hopefully trigger dysfunction if we're not getting responses back
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct FlowCtrl {
    node: Arc<RwLock<Node>>,
    cmd_ctrl: CmdCtrl,
}

impl FlowCtrl {
    pub(crate) fn new(cmd_ctrl: CmdCtrl, incoming_conns: mpsc::Receiver<MsgEvent>) -> Self {
        let node = cmd_ctrl.node();
        let ctrl = Self { cmd_ctrl, node };

        ctrl.clone().start_connection_listening(incoming_conns);
        ctrl.clone().start_network_probing();
        ctrl.clone().start_running_health_checks();
        ctrl.clone().start_checking_for_missed_votes();
        ctrl.clone().start_section_probing();
        ctrl.clone().start_data_replication();
        ctrl.clone().start_dysfunction_detection();
        ctrl.clone().start_cleaning_peer_links();
        #[cfg(feature = "back-pressure")]
        ctrl.clone().start_backpressure_reporting();

        ctrl
    }

    /// Does not await the completion of the cmd.
    pub(crate) async fn fire_and_forget(&self, cmd: Cmd) -> Result<()> {
        let _ = self.cmd_ctrl.push_and_merge(cmd).await?;
        Ok(())
    }

    /// Awaits the completion of the cmd.
    #[allow(unused)]
    pub(crate) async fn await_result(&self, cmd: Cmd) -> Result<()> {
        use cmd_ctrl::CtrlStatus;

        let mut watcher = self.cmd_ctrl.push_and_merge(cmd).await?;

        if let Some(mut watcher) = watcher {
            loop {
                match watcher.await_change().await {
                    CtrlStatus::Finished => {
                        return Ok(());
                    }
                    CtrlStatus::Enqueued => {
                        // this block should be unreachable, as Enqueued is the initial state
                        // but let's handle it anyway..
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    CtrlStatus::MaxRetriesReached(retries) => {
                        return Err(Error::MaxCmdRetriesReached(retries));
                    }
                    CtrlStatus::WatcherDropped => {
                        // the send job is dropped for some reason,
                        return Err(Error::CmdJobWatcherDropped);
                    }
                    CtrlStatus::Error(error) => {
                        continue; // await change on the same recipient again
                    }
                }
            }
        } else {
            /// Cmd was merged into an existing Cmd, so no watcher returned
            return Ok(());
        }
    }

    fn start_connection_listening(self, incoming_conns: mpsc::Receiver<MsgEvent>) {
        // Start listening to incoming connections.
        let _handle = task::spawn_local(handle_connection_events(self, incoming_conns));
    }

    fn start_running_health_checks(self) {
        info!("Starting to check the section's health");
        let _handle: JoinHandle<Result<()>> = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;
                let node = self.node.read().await;

                if node.is_not_elder() {
                    // don't send health checks as an adult
                    continue;
                }

                // random chunk addr will be sent to relevant nodes in the section.
                let chunk_addr = xor_name::rand::random();
                // lets make sure it's relevant to our section, to avoid any
                // potential discards
                let our_prefix = node.network_knowledge.prefix();

                let chunk_addr = our_prefix.substituted_in(chunk_addr);

                let msg = ServiceMsg::Query(DataQuery {
                    variant: DataQueryVariant::GetChunk(ChunkAddress(chunk_addr)),
                    adult_index: 0,
                });

                let msg_id = MsgId::new();
                let our_info = node.info();
                let origin = our_info.peer();

                let auth = auth(&node, &msg)?;

                // generate the cmds, and ensure we go through dysfunction tracking
                let cmds = node
                    .handle_valid_service_msg(
                        msg_id,
                        msg,
                        auth,
                        origin,
                        #[cfg(feature = "traceroute")]
                        vec![],
                    )
                    .await?;

                for cmd in cmds {
                    info!(
                        "Sending healthcheck chunk query to {:?} {:?}",
                        chunk_addr, msg_id
                    );
                    if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                        error!("Error sending a health check msg to the network: {:?}", e);
                    }
                }
            }
        });
    }

    fn start_network_probing(self) {
        info!("Starting to probe network");
        let _handle = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;
                let node = self.node.read().await;
                let is_elder = node.is_elder();
                let prefix = node.network_knowledge().prefix();
                drop(node);

                // Send a probe message if we are an elder
                if is_elder && !prefix.is_empty() {
                    let probe = self.node.read().await.generate_probe_msg();

                    match probe {
                        Ok(cmd) => {
                            info!("Sending probe msg");
                            if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                                error!("Error sending a probe msg to the network: {:?}", e);
                            }
                        }
                        Err(error) => error!("Problem generating probe msg: {:?}", error),
                    }
                }
            }
        });
    }

    fn start_section_probing(self) {
        info!("Starting to probe section");
        let _handle = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(SECTION_PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                let prefix = self.node.read().await.network_knowledge().prefix();

                // Send a probe message to an elder
                if !prefix.is_empty() {
                    let cmd = self.node.read().await.generate_section_probe_msg();
                    info!("Sending section probe msg");
                    if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                        error!("Error sending section probe msg: {:?}", e);
                    }
                }
            }
        });
    }

    /// Checks the interval since last vote received during a generation
    fn start_checking_for_missed_votes(self) {
        info!("Starting to check for missed votes");
        let _handle: JoinHandle<Result<()>> = tokio::task::spawn_local(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(MISSING_VOTE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                if !dispatcher.node.read().await.is_elder() {
                    continue;
                }
                trace!("looping vote check in elder");
                let node = dispatcher.node.read().await;
                let membership = &node.membership;

                if let Some(membership) = &membership {
                    let last_received_vote_time = membership.last_received_vote_time();

                    if let Some(time) = last_received_vote_time {
                        // we want to resend the prev vote
                        if time.elapsed() >= MISSING_VOTE_INTERVAL {
                            debug!("Vote consensus appears stalled...");
                            if let Some(cmd) = node.resend_our_last_vote_to_elders().await {
                                trace!("Vote resending cmd");
                                if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                                    error!("Error resending a vote msg to the network: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Periodically loop over any pending data batches and queue up `send_msg` for those
    fn start_data_replication(self) {
        info!("Starting sending any queued data for replication in batches");

        let _handle: JoinHandle<Result<()>> = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(DATA_BATCH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            use rand::seq::IteratorRandom;
            let mut rng = rand::rngs::OsRng;

            loop {
                let _ = interval.tick().await;

                let mut this_batch_address = None;

                let data_queued = {
                    let node = self.node.read().await;
                    // choose a data to replicate at random
                    let data_queued = node
                        .pending_data_to_replicate_to_peers
                        .iter()
                        .choose(&mut rng)
                        .map(|(address, _)| *address);
                    data_queued
                };

                if let Some(data_addr) = data_queued {
                    this_batch_address = Some(data_addr);
                }

                if let Some(address) = this_batch_address {
                    trace!("Data found in queue to send out");

                    let target_peer = {
                        // careful now, if we're holding any ref into the read above we'll lock here.
                        let mut node = self.node.write().await;
                        let len = node.pending_data_to_replicate_to_peers.len();

                        trace!("LEN of pending data to repl q at start {len}");
                        let x = node.pending_data_to_replicate_to_peers.remove(&address);
                        let len = node.pending_data_to_replicate_to_peers.len();
                        trace!("LEN of pending data to repl q after remove {len}");

                        x
                    };

                    if let Some(data_recipients) = target_peer {
                        debug!("Data queued to be replicated");

                        if data_recipients.is_empty() {
                            continue;
                        }

                        let data_to_send = self
                            .node
                            .read()
                            .await
                            .data_storage
                            .get_from_local_store(&address)
                            .await?;

                        debug!(
                            "{:?} Data {:?} to: {:?}",
                            LogMarker::SendingMissingReplicatedData,
                            address,
                            data_recipients,
                        );

                        let msg = SystemMsg::NodeCmd(NodeCmd::ReplicateData(vec![data_to_send]));
                        let node = self.node.read().await;
                        let cmd = node.send_system_msg(msg, Peers::Multiple(data_recipients));

                        if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                            error!("Error in data replication loop: {:?}", e);
                        }
                        // }
                    }
                }
            }
        });
    }

    fn start_cleaning_peer_links(self) {
        info!("Starting cleaning up network links");
        let _handle = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(LINK_CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                let _ = interval.tick().await;
                debug!("CMD Q len before: {:?}", self.cmd_ctrl.q_len().await);
                if let Err(e) = self.cmd_ctrl.push_and_merge(Cmd::CleanupPeerLinks).await {
                    error!(
                        "Error requesting a cleaning up of unused PeerLinks: {:?}",
                        e
                    );
                }
                if let Err(e) = self.cmd_ctrl.push_and_merge(Cmd::CleanupPeerLinks).await {
                    error!(
                        "Error requesting a cleaning up of unused PeerLinks: {:?}",
                        e
                    );
                }
                debug!("CMD Q len after: {:?}", self.cmd_ctrl.q_len().await);
            }
        });
    }

    fn start_dysfunction_detection(self) {
        info!("Starting dysfunction checking");
        let _handle = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(DYSFUNCTION_CHECK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                let dysfunctional_nodes = self.node.write().await.get_dysfunctional_node_names();
                let unresponsive_nodes = match dysfunctional_nodes {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        error!("Error getting dysfunctional nodes: {error}");
                        BTreeSet::default()
                    }
                };

                if !unresponsive_nodes.is_empty() {
                    debug!("{:?} : {unresponsive_nodes:?}", LogMarker::ProposeOffline);
                    for name in &unresponsive_nodes {
                        if let Err(e) = self
                            .cmd_ctrl
                            .push_and_merge(Cmd::TellEldersToStartConnectivityTest(*name))
                            .await
                        {
                            error!("Error sending TellEldersToStartConnectivityTest for dysfunctional nodes: {e:?}");
                        }
                    }
                    if let Err(e) = self
                        .cmd_ctrl
                        .push_and_merge(Cmd::ProposeOffline(unresponsive_nodes))
                        .await
                    {
                        error!("Error sending Propose Offline for dysfunctional nodes: {e:?}");
                    }
                }
            }
        });
    }

    /// Periodically send back-pressure reports to our section.
    ///
    /// We do not send reports outside of the section as most messages will come from within our section
    /// (and there's no easy way to determine what incoming mesages are spam, or joining nodes etc)
    /// Worst case is after a split, nodes sending messaging from a sibling section to update us may not
    /// know about our load just now. Though that would only be AE messages... and if backpressure is working we should
    /// not be overloaded...
    #[cfg(feature = "back-pressure")]
    fn start_backpressure_reporting(self) {
        info!("Firing off backpressure reports");
        let _handle = tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(BACKPRESSURE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                let _ = interval.tick().await;

                let node = self.node.read().await;
                let our_info = node.info();
                let mut members = node.network_knowledge().members();
                let _ = members.remove(&our_info.peer());

                drop(node);

                if let Some(load_report) =
                    self.cmd_ctrl.dispatcher.comm().tolerated_msgs_per_s().await
                {
                    trace!("New BackPressure report to disseminate: {:?}", load_report);
                    // TODO: use comms to send report to anyone connected? (can we ID end users there?)
                    let node = self.node.read().await;
                    let cmd = node.send_system_msg(
                        SystemMsg::BackPressure(load_report),
                        Peers::Multiple(members),
                    );
                    if let Err(e) = self.cmd_ctrl.push_and_merge(cmd).await {
                        error!("Error sending backpressure to section members: {e:?}");
                    }
                }
            }
        });
    }
}

// Listen for incoming connection events and handle them.
async fn handle_connection_events(ctrl: FlowCtrl, mut incoming_conns: mpsc::Receiver<MsgEvent>) {
    while let Some(event) = incoming_conns.recv().await {
        match event {
            MsgEvent::Received {
                sender,
                wire_msg,
                original_bytes,
            } => {
                debug!(
                    "New message ({} bytes) received from: {:?}",
                    original_bytes.len(),
                    sender
                );

                let span = {
                    let node = &ctrl.node;
                    let name = node.read().await.info().name();
                    trace_span!("handle_message", name = %name, ?sender, msg_id = ?wire_msg.msg_id())
                };
                let _span_guard = span.enter();

                trace!(
                    "{:?} from {:?} length {}",
                    LogMarker::DispatchHandleMsgCmd,
                    sender,
                    original_bytes.len(),
                );

                #[cfg(feature = "test-utils")]
                let wire_msg = if let Ok(msg) = wire_msg.into_msg() {
                    wire_msg.set_payload_debug(msg)
                } else {
                    wire_msg
                };

                let cmd = Cmd::ValidateMsg {
                    origin: sender,
                    wire_msg,
                    original_bytes,
                };

                let _res = ctrl.cmd_ctrl.push_and_merge(cmd).await;
            }
        }
    }

    error!("Fatal error, the stream for incoming connections has been unexpectedly closed. No new connections or messages can be received from the network from here on.");
}

fn auth(node: &Node, msg: &ServiceMsg) -> Result<AuthorityProof<ServiceAuth>> {
    let keypair = node.keypair.clone();
    let payload = WireMsg::serialize_msg_payload(&msg)?;
    let signature = keypair.sign(&payload);

    let auth = ServiceAuth {
        public_key: PublicKey::Ed25519(keypair.public),
        signature: Signature::Ed25519(signature),
    };

    Ok(AuthorityProof::verify(auth, payload)?)
}
