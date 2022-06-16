// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Cmd;
use crate::node::{messages::WireMsgUtils, Result};
#[cfg(feature = "back-pressure")]
use sn_interface::messaging::DstLocation;
use sn_interface::messaging::{
    system::{NodeCmd, SystemMsg},
    WireMsg,
};
use sn_interface::types::log_markers::LogMarker;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::Dispatcher;

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const MISSING_VOTE_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(feature = "back-pressure")]
const BACKPRESSURE_INTERVAL: Duration = Duration::from_secs(60);
const SECTION_PROBE_INTERVAL: Duration = Duration::from_secs(300);
const LINK_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
const DATA_BATCH_INTERVAL: Duration = Duration::from_secs(1);
const DYSFUNCTION_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 10); // every 10 minutes

impl Dispatcher {
    pub(crate) async fn start_network_probing(self: Arc<Self>) {
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

    /// Checks the interval since last vote received during a generation
    pub(crate) async fn check_for_missed_votes(self: Arc<Self>) {
        info!("Starting to check for missed votes");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(MISSING_VOTE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut prev_vote = *dispatcher.node.last_membership_vote.read().await;

            loop {
                let instant = interval.tick().await;

                let last_recorded_vote_time = *dispatcher.node.last_membership_vote.read().await;
                // we want to resend the prev vote
                if last_recorded_vote_time == prev_vote {
                    let cmds = self.node.resend_our_last_vote_to_elders().await?;

                    info!("Resending vote msg");

                    for cmd in cmds {
                        if let Err(e) = dispatcher
                            .clone()
                            .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                            .await
                        {
                            error!("Error resending a vote msg to the network: {:?}", e);
                        }
                    }
                }

                prev_vote = last_recorded_vote_time;
            }

            Result::<()>::Ok(())
        });
    }

    pub(crate) async fn start_section_probing(self: Arc<Self>) {
        info!("Starting to probe section");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(SECTION_PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                // Send a probe message to an elder
                let node = &dispatcher.node;
                if !node.network_knowledge().prefix().await.is_empty() {
                    match node.generate_section_probe_msg().await {
                        Ok(cmd) => {
                            info!("Sending section probe msg");
                            if let Err(e) = dispatcher
                                .clone()
                                .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                                .await
                            {
                                error!("Error sending section probe msg: {:?}", e);
                            }
                        }
                        Err(error) => error!("Problem generating section probe msg: {:?}", error),
                    }
                }
            }
        });
    }

    pub(crate) async fn start_cleaning_peer_links(self: Arc<Self>) {
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

    /// Periodically loop over any pending data batches and queue up send_msg for those
    pub(crate) async fn start_sending_any_data_batches(self: Arc<Self>) {
        info!("Starting sending any queued data for replication in batches");

        let _handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(DATA_BATCH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = interval.tick().await;

            loop {
                use rand::seq::IteratorRandom;
                let mut rng = rand::rngs::OsRng;
                let mut cmds = vec![];
                let mut this_batch_address = None;

                // choose a data to replicate at random
                if let Some(data_queued) = self
                    .pending_data_to_replicate_to_peers
                    .iter()
                    .choose(&mut rng)
                {
                    this_batch_address = Some(*data_queued.key());
                }

                if let Some(address) = this_batch_address {
                    if let Some((data_address, data_recipients)) =
                        self.pending_data_to_replicate_to_peers.remove(&address)
                    {
                        // get info for the WireMsg
                        let src_section_pk = self.node.network_knowledge().section_key().await;
                        let our_info = &*self.node.info.read().await;

                        let mut recipients = vec![];

                        for peer in data_recipients.read().await.iter() {
                            recipients.push(*peer);
                        }

                        if recipients.is_empty() {
                            continue;
                        }

                        let name = recipients[0].name();

                        let dst = sn_interface::messaging::DstLocation::Node {
                            name,
                            section_pk: src_section_pk,
                        };

                        let data_to_send = self
                            .node
                            .data_storage
                            .get_from_local_store(&data_address)
                            .await?;

                        let system_msg =
                            SystemMsg::NodeCmd(NodeCmd::ReplicateData(vec![data_to_send]));
                        let wire_msg =
                            WireMsg::single_src(our_info, dst, system_msg, src_section_pk)?;

                        debug!(
                            "{:?} to: {:?} w/ {:?} ",
                            LogMarker::SendingMissingReplicatedData,
                            recipients,
                            wire_msg.msg_id()
                        );

                        cmds.extend(
                            self.send_msg(&recipients, recipients.len(), wire_msg)
                                .await?,
                        )
                    }
                }

                for cmd in cmds {
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

                let _ = interval.tick().await;
            }
        });
    }

    pub(crate) async fn check_for_dysfunction_periodically(self: Arc<Self>) {
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
    pub(crate) async fn report_backpressure_to_our_section_periodically(self: Arc<Self>) {
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

    pub(crate) async fn write_prefixmap_to_disk(self: Arc<Self>) {
        info!("Writing our PrefixMap to disk");
        self.clone().node.write_prefix_map().await
    }
}
