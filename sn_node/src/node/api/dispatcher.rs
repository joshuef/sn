// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Cmd;

use crate::node::{
    core::{DeliveryStatus, Node, Proposal},
    messages::WireMsgUtils,
    Result,
};
#[cfg(feature = "back-pressure")]
use sn_interface::messaging::DstLocation;
use sn_interface::types::{log_markers::LogMarker, Peer};
use sn_interface::{
    messaging::{
        system::{NodeCmd, SystemMsg},
        AuthKind, WireMsg,
    },
    types::ReplicatedDataAddress,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::time::MissedTickBehavior;
use tokio::{sync::watch, time};
use tracing::Instrument;

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(feature = "back-pressure")]
const BACKPRESSURE_INTERVAL: Duration = Duration::from_secs(60);
const LINK_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
const DYSFUNCTION_CHECK_INTERVAL: Duration = Duration::from_secs(60);

// A command/subcommand id e.g. "963111461", "963111461.0"
type CmdId = String;

// Cmd Dispatcher.
pub(crate) struct Dispatcher {
    pub(crate) node: Node,
    cancel_timer_tx: watch::Sender<bool>,
    cancel_timer_rx: watch::Receiver<bool>,
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        // Cancel all scheduled timers including any future ones.
        let _res = self.cancel_timer_tx.send(true);
    }
}

impl Dispatcher {
    pub(super) fn new(node: Node) -> Self {
        let (cancel_timer_tx, cancel_timer_rx) = watch::channel(false);
        Self {
            node,
            cancel_timer_tx,
            cancel_timer_rx,
        }
    }

    /// Enqueues the given cmd and handles whatever cmd is in the next priority queue and triggers handling after any required waits for higher priority tasks
    pub(super) async fn enqueue_and_handle_next_cmd_and_offshoots(
        self: Arc<Self>,
        cmd: Cmd,
        cmd_id: Option<CmdId>,
    ) -> Result<()> {
        let _ = tokio::spawn(async {
            let cmd_id: CmdId = cmd_id.unwrap_or_else(|| rand::random::<u32>().to_string());

            self.handle_cmd_and_offshoots(cmd, Some(cmd_id)).await
        });
        Ok(())
    }

    /// Handles cmd and transitively queues any new cmds that are
    /// produced during its handling. Trace logs will include the provided cmd id,
    /// and any sub-cmds produced will have it as a common root cmd id.
    /// If a cmd id string is not provided a random one will be generated.
    pub(super) async fn handle_cmd_and_offshoots(
        self: Arc<Self>,
        cmd: Cmd,
        cmd_id: Option<CmdId>,
    ) -> Result<()> {
        let cmd_id = cmd_id.unwrap_or_else(|| rand::random::<u32>().to_string());
        let cmd_id_clone = cmd_id.clone();
        let cmd_display = cmd.to_string();
        let _task = tokio::spawn(async move {
            match self.process_cmd(cmd, &cmd_id).await {
                Ok(cmds) => {
                    for (sub_cmd_count, cmd) in cmds.into_iter().enumerate() {
                        let sub_cmd_id = format!("{}.{}", &cmd_id, sub_cmd_count);
                        // Error here is only related to queueing, and so a dropped cmd will be logged
                        let _result = self.clone().spawn_cmd_handling(cmd, sub_cmd_id);
                    }
                }
                Err(err) => {
                    error!("Failed to handle cmd {:?} with error {:?}", cmd_id, err);
                }
            }
        });

        trace!(
            "{:?} {} cmd_id={}",
            LogMarker::CmdHandlingSpawned,
            cmd_display,
            &cmd_id_clone
        );
        Ok(())
    }

    // Note: this indirecton is needed. Trying to call `spawn(self.handle_cmds(...))` directly
    // inside `handle_cmds` causes compile error about type check cycle.
    fn spawn_cmd_handling(self: Arc<Self>, cmd: Cmd, cmd_id: String) -> Result<()> {
        let _task = tokio::spawn(self.enqueue_and_handle_next_cmd_and_offshoots(cmd, Some(cmd_id)));
        Ok(())
    }

    pub(super) async fn start_network_probing(self: Arc<Self>) {
        info!("Starting to probe network");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                // Send a probe message if we are an elder
                let node = &dispatcher.node;
                if node.is_elder().await && !node.network_knowledge().prefix().await.is_empty() {
                    match node.generate_probe_msg().await {
                        Ok(cmd) => {
                            info!("Sending probe msg");
                            if let Err(e) = dispatcher
                                .clone()
                                .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                                .await
                            {
                                error!("Error sending a probe msg to the network: {:?}", e);
                            }
                        }
                        Err(error) => error!("Problem generating probe msg: {:?}", error),
                    }
                }
            }
        });
    }
    pub(super) async fn start_cleaning_peer_links(self: Arc<Self>) {
        info!("Starting cleaning up network links");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(LINK_CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                let _ = interval.tick().await;
                let cmd = Cmd::CleanupPeerLinks;
                if let Err(e) = dispatcher
                    .clone()
                    .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                    .await
                {
                    error!(
                        "Error requesting a cleaning up of unused PeerLinks: {:?}",
                        e
                    );
                }
            }
        });
    }
    pub(super) async fn check_for_dysfunction_periodically(self: Arc<Self>) {
        info!("Starting dysfunction checking");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(DYSFUNCTION_CHECK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                let unresponsive_nodes = match dispatcher.node.get_dysfunctional_node_names().await
                {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        error!("Error getting dysfunctional nodes: {error}");
                        BTreeSet::default()
                    }
                };

                if !unresponsive_nodes.is_empty() {
                    debug!("{:?} : {unresponsive_nodes:?}", LogMarker::ProposeOffline);
                    let cmd = Cmd::ProposeOffline(unresponsive_nodes);
                    if let Err(e) = dispatcher
                        .clone()
                        .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                        .await
                    {
                        error!("Error sending Propose Offline for dysfunctional nodes: {e:?}");
                    }
                }
            }
        });
    }

    #[cfg(feature = "back-pressure")]
    /// Periodically send back-pressure reports to our section.
    ///
    /// We do not send reports outside of the section as most messages will come from within our section
    /// (and there's no easy way to determine what incoming mesages are spam, or joining nodes etc)
    /// Worst case is after a split, nodes sending messaging from a sibling section to update us may not
    /// know about our load just now. Though that would only be AE messages... and if backpressure is working we should
    /// not be overloaded...
    pub(super) async fn report_backpressure_to_our_section_periodically(self: Arc<Self>) {
        info!("Firing off backpressure reports");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(BACKPRESSURE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                let _ = interval.tick().await;

                let members = dispatcher.node.network_knowledge().section_members().await;
                let section_pk = dispatcher.node.network_knowledge().section_key().await;

                if let Some(load_report) = dispatcher.node.comm.tolerated_msgs_per_s().await {
                    trace!("New BackPressure report to disseminate: {:?}", load_report);

                    // TODO: use comms to send report to anyone connected? (can we ID end users there?)
                    for member in members {
                        let our_name = dispatcher.node.info.read().await.name();
                        let peer = member.peer();

                        if peer.name() == our_name {
                            continue;
                        }

                        let wire_msg = match WireMsg::single_src(
                            &*dispatcher.node.info.read().await,
                            DstLocation::Node {
                                name: peer.name(),
                                section_pk,
                            },
                            SystemMsg::BackPressure(load_report),
                            section_pk,
                        ) {
                            Ok(msg) => msg,
                            Err(e) => {
                                error!(
                                    "Error forming backpressure message to section member {:?}",
                                    e
                                );
                                continue;
                            }
                        };

                        let cmd = Cmd::SendMsg {
                            wire_msg,
                            recipients: vec![*peer],
                        };

                        if let Err(e) = dispatcher
                            .clone()
                            .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                            .await
                        {
                            error!(
                                "Error sending backpressure report to section member {:?}: {:?}",
                                peer, e
                            );
                        }
                    }
                }
            }
        });
    }

    pub(super) async fn write_prefixmap_to_disk(self: Arc<Self>) {
        info!("Writing our PrefixMap to disk");
        self.clone().node.write_prefix_map().await
    }

    /// Handles a single cmd.
    pub(super) async fn process_cmd(&self, cmd: Cmd, cmd_id: &str) -> Result<Vec<Cmd>> {
        // Create a tracing span containing info about the current node. This is very useful when
        // analyzing logs produced by running multiple nodes within the same process, for example
        // from integration tests.
        let span = {
            let node = &self.node;

            let prefix = node.network_knowledge().prefix().await;
            let is_elder = node.is_elder().await;
            let section_key = node.network_knowledge().section_key().await;
            let age = node.info.read().await.age();
            trace_span!(
                "process_cmd",
                name = %node.info.read().await.name(),
                prefix = format_args!("({:b})", prefix),
                age,
                elder = is_elder,
                cmd_id = %cmd_id,
                section_key = ?section_key,
                %cmd,
            )
        };

        async {
            let cmd_display = cmd.to_string();
            trace!(
                "{:?} {:?} - {}",
                LogMarker::CmdProcessStart,
                cmd_id,
                cmd_display
            );

            let res = match self.try_processing_cmd(cmd).await {
                Ok(outcome) => {
                    trace!(
                        "{:?} {:?} - {}",
                        LogMarker::CmdProcessEnd,
                        cmd_id,
                        cmd_display
                    );
                    Ok(outcome)
                }
                Err(error) => {
                    error!(
                        "Error encountered when processing cmd (cmd_id {}): {:?}",
                        cmd_id, error
                    );
                    trace!(
                        "{:?} {}: {:?}",
                        LogMarker::CmdProcessingError,
                        cmd_display,
                        error
                    );
                    Err(error)
                }
            };
            res
        }
        .instrument(span)
        .await
    }

    /// Actually process the cmd
    async fn try_processing_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        match cmd {
            Cmd::CleanupPeerLinks => {
                self.node.cleanup_non_section_peers().await;
                Ok(vec![])
            }
            Cmd::SignOutgoingSystemMsg { msg, dst } => {
                let src_section_pk = self.node.network_knowledge().section_key().await;
                let wire_msg =
                    WireMsg::single_src(&*self.node.info.read().await, dst, msg, src_section_pk)?;

                let mut cmds = vec![];
                cmds.extend(self.node.send_msg_to_nodes(wire_msg).await?);

                Ok(cmds)
            }
            Cmd::HandleMsg {
                sender,
                wire_msg,
                original_bytes,
            } => self.node.handle_msg(sender, wire_msg, original_bytes).await,
            Cmd::HandleTimeout(token) => self.node.handle_timeout(token).await,
            Cmd::HandleAgreement { proposal, sig } => {
                self.node.handle_general_agreements(proposal, sig).await
            }
            Cmd::HandleNewNodeOnline(auth) => {
                self.node
                    .handle_online_agreement(auth.value.into_state(), auth.sig)
                    .await
            }
            Cmd::HandleNodeLeft(auth) => {
                self.node
                    .handle_node_left(auth.value.into_state(), auth.sig)
                    .await
            }
            Cmd::HandleNewEldersAgreement { proposal, sig } => match proposal {
                Proposal::NewElders(section_auth) => {
                    self.node
                        .handle_new_elders_agreement(section_auth, sig)
                        .await
                }
                _ => {
                    error!("Other agreement messages should be handled in `HandleAgreement`, which is non-blocking ");
                    Ok(vec![])
                }
            },
            Cmd::HandlePeerLost(peer) => self.node.handle_peer_lost(&peer.addr()).await,
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
                generation,
            } => {
                self.node
                    .handle_dkg_outcome(section_auth, outcome, generation)
                    .await
            }
            Cmd::HandleDkgFailure(signeds) => self
                .node
                .handle_dkg_failure(signeds)
                .await
                .map(|cmd| vec![cmd]),
            Cmd::SendMsg {
                recipients,
                wire_msg,
            } => self.send_msg(&recipients, recipients.len(), wire_msg).await,
            Cmd::ThrottledSendBatchData {
                throttle_duration,
                recipient,
                mut data_batches,
            } => {
                // TODO, if we have outgoing msg deduplication, we can put all relevant nodes in recipients here...
                self.send_throttled_batch_data(recipient, &mut data_batches, throttle_duration)
                    .await
            }
            Cmd::SendMsgDeliveryGroup {
                recipients,
                delivery_group_size,
                wire_msg,
            } => {
                self.send_msg(&recipients, delivery_group_size, wire_msg)
                    .await
            }
            Cmd::ScheduleTimeout { duration, token } => Ok(self
                .handle_schedule_timeout(duration, token)
                .await
                .into_iter()
                .collect()),
            Cmd::ProposeOffline(names) => self.node.cast_offline_proposals(&names).await,
            Cmd::StartConnectivityTest(name) => Ok(vec![
                self.node
                    .send_msg_to_our_elders(SystemMsg::StartConnectivityTest(name))
                    .await?,
            ]),
            Cmd::TestConnectivity(name) => {
                if let Some(member_info) = self
                    .node
                    .network_knowledge()
                    .get_section_member(&name)
                    .await
                {
                    if self
                        .node
                        .comm
                        .is_reachable(&member_info.addr())
                        .await
                        .is_err()
                    {
                        self.node.log_comm_issue(member_info.name()).await?
                    }
                }
                Ok(vec![])
            }
        }
    }

    async fn send_msg(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let cmds = match wire_msg.msg_kind() {
            AuthKind::Node(_) | AuthKind::NodeBlsShare(_) => {
                self.deliver_msgs(recipients, delivery_group_size, wire_msg)
                    .await?
            }
            AuthKind::Service(_) => {
                // we should never be sending such a msg to more than one recipient
                // need refactors further up to solve in a nicer way
                if recipients.len() > 1 {
                    warn!("Unexpected number of client recipients {:?} for msg {:?}. Only sending to first.",
                    recipients.len(), wire_msg);
                }
                if let Some(recipient) = recipients.get(0) {
                    if let Err(err) = self
                        .node
                        .comm
                        .send_to_client(recipient, wire_msg.clone())
                        .await
                    {
                        error!(
                            "Failed sending message {:?} to client {:?} with error {:?}",
                            wire_msg, recipient, err
                        );
                    }
                }

                vec![]
            }
        };

        Ok(cmds)
    }

    /// Sends a batch of data every `throttle_duration` so as not to overwhelm the nodes,
    /// or cause too much memory pressure holding data in mem locally
    async fn send_throttled_batch_data(
        &self,
        recipient: Peer,
        data_batches: &mut Vec<Vec<ReplicatedDataAddress>>,
        throttle_duration: Duration,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        debug!("Data batch waiting to be sent to {recipient}");
        let mut interval = tokio::time::interval(throttle_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            let _instant = interval.tick().await;
            if let Some(data_batch) = data_batches.pop() {
                debug!("Forming data batch msg to {recipient}");
                // get info for the WireMsg
                let section_pk = self.node.section_key_by_name(&recipient.name()).await;
                let src_section_pk = self.node.network_knowledge().section_key().await;
                let our_info = &*self.node.info.read().await;
                let dst = sn_interface::messaging::DstLocation::Node {
                    name: recipient.name(),
                    section_pk,
                };

                let mut data_to_send = vec![];
                for data_address in data_batch {
                    let data = self
                        .node
                        .data_storage
                        .get_from_local_store(&data_address)
                        .await?;
                    data_to_send.push(data)
                }

                let system_msg = SystemMsg::NodeCmd(NodeCmd::ReplicateData(data_to_send));

                let wire_msg = WireMsg::single_src(our_info, dst, system_msg, src_section_pk)?;

                cmds.extend(self.send_msg(&[recipient], 1, wire_msg).await?)
            } else {
                info!("Finished queing sending a batch of messages");
                break;
            }
        }

        Ok(cmds)
    }

    async fn deliver_msgs(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let status = self
            .node
            .comm
            .send(recipients, delivery_group_size, wire_msg)
            .await?;

        match status {
            DeliveryStatus::MinDeliveryGroupSizeReached(failed_recipients)
            | DeliveryStatus::MinDeliveryGroupSizeFailed(failed_recipients) => {
                Ok(failed_recipients
                    .into_iter()
                    .map(Cmd::HandlePeerLost)
                    .collect())
            }
            _ => Ok(vec![]),
        }
    }

    async fn handle_schedule_timeout(&self, duration: Duration, token: u64) -> Option<Cmd> {
        let mut cancel_rx = self.cancel_timer_rx.clone();

        if *cancel_rx.borrow() {
            // Timers are already cancelled, do nothing.
            return None;
        }

        tokio::select! {
            _ = time::sleep(duration) => Some(Cmd::HandleTimeout(token)),
            _ = cancel_rx.changed() => None,
        }
    }
}
