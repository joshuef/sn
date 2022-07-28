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

use ed25519_dalek::Signer;
#[cfg(feature = "traceroute")]
use sn_interface::messaging::Traceroute;
use sn_interface::{
    messaging::{
        data::{DataQuery, DataQueryVariant, ServiceMsg},
        system::{NodeCmd, SystemMsg},
        AuthorityProof, MsgId, ServiceAuth, WireMsg,
    },
    network_knowledge::NodeInfo,
    types::log_markers::LogMarker,
    types::{ChunkAddress, PublicKey, Signature},
};

use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        RwLock,
    },
    time::{sleep, Instant},
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
// to prevent cpu racing
const LOOP_SLEEP_INTERVAL: Duration = Duration::from_millis(10);

/// Listens for incoming msgs and forms Cmds for each,
/// Periodically triggers other Cmd Processes (eg health checks, dysfunction etc)
pub(crate) struct FlowCtrl {
    node: Arc<RwLock<Node>>,
    cmd_ctrl: CmdCtrl,
    incoming_msg_events: mpsc::Receiver<MsgEvent>,
    incoming_cmds_from_apis: mpsc::Receiver<Cmd>,
}

impl FlowCtrl {
    pub(crate) fn new(
        cmd_ctrl: CmdCtrl,
        incoming_msg_events: mpsc::Receiver<MsgEvent>,
    ) -> (Self, mpsc::Sender<Cmd>) {
        let node = cmd_ctrl.node();
        let (cmd_sender, incoming_cmds_from_apis) = mpsc::channel(1);

        (
            Self {
                cmd_ctrl,
                node,
                incoming_msg_events,
                incoming_cmds_from_apis,
            },
            cmd_sender,
        )
    }

    /// This is a never ending loop as long as the node is live.
    /// This loop drives the periodic events internal to the node.
    pub(crate) async fn process_messages_and_periodic_checks(mut self) {
        debug!("Starting internal processing...");
        let mut last_probe = Instant::now();
        let mut last_section_probe = Instant::now();
        let mut last_health_check = Instant::now();
        let mut last_vote_check = Instant::now();
        let mut last_data_batch_check = Instant::now();
        let mut last_link_cleanup = Instant::now();
        let mut last_dysfunction_check = Instant::now();
        #[cfg(feature = "back-pressure")]
        let mut last_backpressure_check = Instant::now();

        // the internal process loop
        loop {
            let now = Instant::now();
            let mut cmds = vec![];

            let (info, is_elder) = {
                let node = self.node.read().await;

                (node.info(), node.is_elder())
            };

            // Finally, handle any incoming conn messages
            // this requires mut self
            match self.incoming_cmds_from_apis.try_recv() {
                Ok(cmd) => cmds.push(cmd),
                Err(TryRecvError::Empty) => {
                    // do nothing
                }
                Err(TryRecvError::Disconnected) => {
                    trace!("Senders to `incoming_cmds_from_apis` have disconnected.");
                }
            }

            // Finally, handle any incoming conn messages
            // this requires mut self
            match self.incoming_msg_events.try_recv() {
                Ok(msg) => cmds.push(self.handle_new_msg_event(info.clone(), msg).await),
                Err(TryRecvError::Empty) => {
                    // do nothing
                }
                Err(TryRecvError::Disconnected) => {
                    trace!("Senders to `incoming_msg_events` have disconnected. Stopping node periodic tasks.");
                    break;
                }
            }

            // happens regardless of if elder or adult
            if last_link_cleanup.elapsed() > LINK_CLEANUP_INTERVAL {
                last_link_cleanup = now;
                cmds.push(Cmd::CleanupPeerLinks);
            }

            #[cfg(feature = "back-pressure")]
            if last_backpressure_check.elapsed() > BACKPRESSURE_INTERVAL {
                last_backpressure_check = now;

                if let Some(cmd) =
                    Self::report_backpressure(self.node.clone(), &self.cmd_ctrl).await
                {
                    cmds.push(cmd)
                }
            }

            // Things that should only happen to non elder nodes
            if !is_elder {
                // if we've passed enough time, section probe
                if last_section_probe.elapsed() > SECTION_PROBE_INTERVAL {
                    last_section_probe = now;
                    if let Some(cmd) = Self::probe_the_section(self.node.clone()).await {
                        cmds.push(cmd);
                    }
                }

                let no_cmds = cmds.is_empty();

                for cmd in cmds {
                    if let Err(error) = self.fire_and_forget(cmd).await {
                        error!("Error pushing node process cmd to controller: {error:?}");
                    }
                }

                if no_cmds {
                    // prevent cpu racing
                    sleep(LOOP_SLEEP_INTERVAL).await;
                }

                // remaining cmds are for elders only.
                // we've pushed what we have so we can continue
                continue;
            }

            // Okay, so the node is currently an elder...

            // if we've passed enough time, network probe
            if last_probe.elapsed() > PROBE_INTERVAL {
                last_probe = now;
                if let Some(cmd) = Self::probe_the_network(self.node.clone()).await {
                    cmds.push(cmd);
                }
            }

            // if we've passed enough time, batch outgoing data
            if last_data_batch_check.elapsed() > DATA_BATCH_INTERVAL {
                last_data_batch_check = now;
                if let Some(cmd) = match Self::replicate_queued_data(self.node.clone()).await {
                    Ok(cmd) => cmd,
                    Err(error) => {
                        error!("Error handling getting cmds for data queued for replication: {error:?}");
                        None
                    }
                } {
                    cmds.push(cmd);
                }
            }

            if last_health_check.elapsed() > HEALTH_CHECK_INTERVAL {
                last_health_check = now;
                let health_cmds = match Self::perform_health_checks(self.node.clone()).await {
                    Ok(cmds) => cmds,
                    Err(error) => {
                        error!("Error handling service msg to perform health check: {error:?}");
                        vec![]
                    }
                };
                cmds.extend(health_cmds);
            }

            if last_vote_check.elapsed() > MISSING_VOTE_INTERVAL {
                last_vote_check = now;
                if let Some(cmd) = Self::check_for_missed_votes(self.node.clone()).await {
                    cmds.push(cmd);
                };
            }

            if last_dysfunction_check.elapsed() > DYSFUNCTION_CHECK_INTERVAL {
                last_dysfunction_check = now;
                let dysf_cmds = Self::check_for_dysfunction(self.node.clone()).await;
                cmds.extend(dysf_cmds);
            }

            let no_cmds = cmds.is_empty();

            for cmd in cmds {
                if let Err(error) = self.fire_and_forget(cmd).await {
                    error!("Error pushing node process cmd to controller: {error:?}");
                }
            }

            if no_cmds {
                // prevent cpu racing
                sleep(LOOP_SLEEP_INTERVAL).await;
            }
        }

        error!("Internal processing ended.")
    }

    /// Does not await the completion of the cmd.
    pub(crate) async fn fire_and_forget(&self, cmd: Cmd) -> Result<()> {
        let _ = self.cmd_ctrl.push(cmd).await?;
        Ok(())
    }

    /// Awaits the completion of the cmd.
    #[allow(unused)]
    pub(crate) async fn await_result(&self, cmd: Cmd) -> Result<()> {
        use cmd_ctrl::CtrlStatus;

        let mut watcher = self.cmd_ctrl.push(cmd).await?;

        loop {
            match watcher.await_change().await {
                CtrlStatus::Finished => {
                    return Ok(());
                }
                CtrlStatus::Enqueued => {
                    // this block should be unreachable, as Enqueued is the initial state
                    // but let's handle it anyway..
                    sleep(Duration::from_millis(100)).await;
                    continue;
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
    }

    /// Initiates and generates all the subsequent Cmds to perform a healthcheck
    async fn perform_health_checks(node: Arc<RwLock<Node>>) -> Result<Vec<Cmd>> {
        info!("Starting to check the section's health");
        let node = node.read().await;
        let our_prefix = node.network_knowledge.prefix();
        let _our_info = node.info();
        let _keypair = node.keypair.clone();

        // random chunk addr will be sent to relevant nodes in the section.
        let chunk_addr = xor_name::rand::random();

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
        node.handle_valid_service_msg(
            msg_id,
            msg,
            auth,
            origin,
            #[cfg(feature = "traceroute")]
            Traceroute(vec![]),
        )
        .await
    }

    /// Generates a probe msg, which goes to a random section in order to
    /// passively maintain network knowledge over time
    async fn probe_the_network(node: Arc<RwLock<Node>>) -> Option<Cmd> {
        let node = node.read().await;
        let prefix = node.network_knowledge().prefix();

        // Send a probe message if we are an elder
        // but dont bother if we're the first section
        if !prefix.is_empty() {
            info!("Probing network");
            match node.generate_probe_msg() {
                Ok(cmd) => Some(cmd),
                Err(error) => {
                    error!("Could not generate probe msg: {error:?}");
                    None
                }
            }
        } else {
            None
        }
    }

    /// Generates a probe msg, which goes to a random section in order to
    /// passively maintain network knowledge over time
    async fn probe_the_section(node: Arc<RwLock<Node>>) -> Option<Cmd> {
        let node = node.read().await;

        // Send a probe message to an elder
        info!("Starting to probe section");

        let prefix = node.network_knowledge().prefix();

        // Send a probe message to an elder
        if !prefix.is_empty() {
            Some(node.generate_section_probe_msg())
        } else {
            None
        }
    }

    /// Checks the interval since last vote received during a generation
    async fn check_for_missed_votes(node: Arc<RwLock<Node>>) -> Option<Cmd> {
        info!("Checking for missed votes");
        let node = node.read().await;
        let membership = &node.membership;

        if let Some(membership) = &membership {
            let last_received_vote_time = membership.last_received_vote_time();

            if let Some(time) = last_received_vote_time {
                // we want to resend the prev vote
                if time.elapsed() >= MISSING_VOTE_INTERVAL {
                    debug!("Vote consensus appears stalled...");
                    return node.resend_our_last_vote_to_elders().await;
                }
            }
        }

        None
    }

    /// Periodically loop over any pending data batches and queue up `send_msg` for those
    async fn replicate_queued_data(node: Arc<RwLock<Node>>) -> Result<Option<Cmd>> {
        info!("Starting sending any queued data for replication in batches");

        use rand::seq::IteratorRandom;
        let mut rng = rand::rngs::OsRng;

        let (_src_section_pk, _our_info, data_queued) = {
            let node = node.read().await;
            // get info for the WireMsg
            let src_section_pk = node.network_knowledge().section_key();
            let our_info = node.info();
            // choose a data to replicate at random
            let data_queued = node
                .pending_data_to_replicate_to_peers
                .iter()
                .choose(&mut rng)
                .map(|(address, _)| *address);

            (src_section_pk, our_info, data_queued)
        };

        if let Some(address) = data_queued {
            trace!("Data found in queue to send out");

            let target_peer = {
                // careful now, if we're holding any ref into the read above we'll lock here.
                let mut node = node.write().await;
                node.pending_data_to_replicate_to_peers.remove(&address)
            };

            if let Some(data_recipients) = target_peer {
                debug!("Data queued to be replicated");

                if data_recipients.is_empty() {
                    return Ok(None);
                }

                let data_to_send = node
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
                let node = node.read().await;
                return Ok(Some(
                    node.send_system_msg(msg, Peers::Multiple(data_recipients)),
                ));
            }
        }

        Ok(None)
    }

    async fn check_for_dysfunction(node: Arc<RwLock<Node>>) -> Vec<Cmd> {
        info!("Performing dysfunction checking");
        let mut cmds = vec![];
        let dysfunctional_nodes = node.write().await.get_dysfunctional_node_names();
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
                cmds.push(Cmd::TellEldersToStartConnectivityTest(*name))
            }
            cmds.push(Cmd::ProposeOffline(unresponsive_nodes))
        }

        cmds
    }

    /// Periodically send back-pressure reports to our section.
    ///
    /// We do not send reports outside of the section as most messages will come from within our section
    /// (and there's no easy way to determine what incoming mesages are spam, or joining nodes etc)
    /// Worst case is after a split, nodes sending messaging from a sibling section to update us may not
    /// know about our load just now. Though that would only be AE messages... and if backpressure is working we should
    /// not be overloaded...
    #[cfg(feature = "back-pressure")]
    async fn report_backpressure(the_node: Arc<RwLock<Node>>, cmd_ctrl: &CmdCtrl) -> Option<Cmd> {
        info!("Firing off backpressure reports");
        let node = the_node.read().await;
        let our_info = node.info();
        let mut members = node.network_knowledge().members();
        let _ = members.remove(&our_info.peer());

        drop(node);

        if let Some(load_report) = cmd_ctrl.dispatcher.comm().tolerated_msgs_per_s().await {
            trace!("New BackPressure report to disseminate: {:?}", load_report);
            // TODO: use comms to send report to anyone connected? (can we ID end users there?)
            let cmd = the_node.read().await.send_system_msg(
                SystemMsg::BackPressure(load_report),
                Peers::Multiple(members),
            );

            Some(cmd)
        } else {
            None
        }
    }

    // Listen for a new incoming connection event and handle it.
    async fn handle_new_msg_event(&self, node_info: NodeInfo, event: MsgEvent) -> Cmd {
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
                    let name = node_info.name();
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

                Cmd::ValidateMsg {
                    origin: sender,
                    wire_msg,
                    original_bytes,
                }
            }
        }
    }
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
