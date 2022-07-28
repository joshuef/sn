// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{messages::WireMsgUtils, Cmd, Node, Result};

use crate::comm::{Comm, DeliveryStatus};
use sn_interface::{
    messaging::{data::ServiceMsg, system::SystemMsg, WireMsg},
    types::Peer,
};

use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{sync::watch, sync::RwLock, time};

#[cfg(feature = "traceroute")]
use sn_interface::messaging::Entity;
#[cfg(feature = "traceroute")]
use sn_interface::types::PublicKey;

// Cmd Dispatcher.
pub(crate) struct Dispatcher {
    node: Arc<RwLock<Node>>,
    comm: Comm,
    dkg_timeout: Arc<DkgTimeout>,
}

impl Dispatcher {
    pub(crate) fn new(node: Arc<RwLock<Node>>, comm: Comm) -> Self {
        let (cancel_timer_tx, cancel_timer_rx) = watch::channel(false);
        let dkg_timeout = Arc::new(DkgTimeout {
            cancel_timer_tx,
            cancel_timer_rx,
        });

        Self {
            node,
            dkg_timeout,
            comm,
        }
    }

    pub(crate) fn node(&self) -> Arc<RwLock<Node>> {
        self.node.clone()
    }

    #[cfg(feature = "back-pressure")]
    // Currently only used in cmd ctrl backpressure features
    pub(crate) fn comm(&self) -> &Comm {
        &self.comm
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        match cmd {
            Cmd::CleanupPeerLinks => {
                let members = { self.node.read().await.network_knowledge.section_members() };
                self.comm.cleanup_peers(members).await?;
                Ok(vec![])
            }
            Cmd::SignOutgoingSystemMsg {
                msg,
                dst,
                #[cfg(feature = "traceroute")]
                mut traceroute,
            } => {
                let node = self.node.read().await;
                let info = node.info();

                let src_section_pk = node.network_knowledge().section_key();

                #[allow(unused_mut)]
                let mut wire_msg = WireMsg::single_src(&info, dst, msg, src_section_pk)?;

                #[cfg(feature = "traceroute")]
                {
                    let key = node.info().keypair.public;
                    traceroute.push(Entity::Adult(PublicKey::Ed25519(key)));
                    wire_msg.add_trace(&mut traceroute);
                }

                let mut cmds = vec![];
                cmds.extend(node.send_msg_on_to_nodes(wire_msg)?);

                Ok(cmds)
            }
            Cmd::TrackNodeIssueInDysfunction { name, issue } => {
                let mut node = self.node.write().await;
                node.log_node_issue(name, issue)?;

                Ok(vec![])
            }
            Cmd::ValidateMsg {
                origin,
                wire_msg,
                original_bytes,
            } => {
                let node = self.node.read().await;
                node.validate_msg(origin, wire_msg, original_bytes).await
            }
            Cmd::HandleValidServiceMsg {
                msg_id,
                msg,
                origin,
                auth,
                #[cfg(feature = "traceroute")]
                traceroute,
            } => match msg {
                ServiceMsg::Query(_) => {
                    let mut node = self.node.write().await;

                    node.handle_valid_query_msg(
                        msg_id,
                        msg,
                        auth,
                        origin,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await
                }
                _ => {
                    let node = self.node.read().await;

                    node.handle_valid_service_msg(
                        msg_id,
                        msg,
                        origin,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await
                }
            },
            Cmd::HandleValidSystemMsg {
                origin,
                msg_id,
                msg,
                msg_authority,
                wire_msg_payload,
                #[cfg(feature = "traceroute")]
                traceroute,
            } => {
                let mut node = self.node.write().await;

                if let Some(msg_authority) = node
                    .aggregate_system_msg(msg_id, msg_authority, wire_msg_payload)
                    .await
                {
                    node.handle_valid_system_msg(
                        msg_id,
                        msg_authority,
                        msg,
                        origin,
                        &self.comm,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await
                } else {
                    Ok(vec![])
                }
            }
            Cmd::HandleDkgTimeout(token) => {
                let node = self.node.read().await;

                node.handle_dkg_timeout(token)
            }
            Cmd::HandleAgreement { proposal, sig } => {
                let mut node = self.node.write().await;

                node.handle_general_agreements(proposal, sig).await
            }
            Cmd::HandleMembershipDecision(decision) => {
                let mut node = self.node.write().await;

                node.handle_membership_decision(decision).await
            }
            Cmd::HandleNewEldersAgreement { new_elders, sig } => {
                let mut node = self.node.write().await;

                node.handle_new_elders_agreement(new_elders, sig).await
            }
            Cmd::HandlePeerFailedSend(peer) => {
                let mut node = self.node.write().await;

                node.handle_failed_send(&peer.addr())?;

                Ok(vec![])
            }
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
            } => {
                let mut node = self.node.write().await;

                node.handle_dkg_outcome(section_auth, outcome).await
            }
            Cmd::HandleDkgFailure(signeds) => {
                let mut node = self.node.write().await;

                node.handle_dkg_failure(signeds).map(|cmd| vec![cmd])
            }
            Cmd::SendMsg {
                recipients,
                wire_msg,
            } => self.send_msg_via_comms(&recipients, wire_msg).await,
            Cmd::EnqueueDataForReplication {
                // throttle_duration,
                recipient,
                data_batch,
            } => {
                // we should queue this
                let mut node = self.node.write().await;
                for data in data_batch {
                    trace!("data being enqueued for replication {:?}", data);
                    if let Some(peers_set) = node.pending_data_to_replicate_to_peers.get_mut(&data)
                    {
                        debug!("data already queued, adding peer");
                        let _existed = peers_set.insert(recipient);
                    } else {
                        let mut peers_set = BTreeSet::new();
                        let _existed = peers_set.insert(recipient);
                        let _existed = node
                            .pending_data_to_replicate_to_peers
                            .insert(data, peers_set);
                    };
                }
                Ok(vec![])
            }
            Cmd::ScheduleDkgTimeout { duration, token } => Ok(self
                .handle_scheduled_dkg_timeout(duration, token)
                .await
                .into_iter()
                .collect()),
            Cmd::ProposeOffline(names) => {
                let mut node = self.node.write().await;

                node.cast_offline_proposals(&names)
            }
            Cmd::TellEldersToStartConnectivityTest(name) => {
                let node = self.node.read().await;

                Ok(vec![node.send_msg_to_our_elders(
                    SystemMsg::StartConnectivityTest(name),
                )?])
            }
            Cmd::TestConnectivity(name) => {
                let node_state = self
                    .node
                    .read()
                    .await
                    .network_knowledge()
                    .get_section_member(&name);

                if let Some(member_info) = node_state {
                    if self.comm.is_reachable(&member_info.addr()).await.is_err() {
                        let mut node = self.node.write().await;

                        node.log_comm_issue(member_info.name())?
                    }
                }
                Ok(vec![])
            }
            Cmd::Comm(comm_cmd) => {
                self.comm.handle_cmd(comm_cmd).await;
                Ok(vec![])
            }
        }
    }

    /// Takes recipients and a WireMsg and will send to each recipient via the comms module.
    /// This ensures the DstLocation name is set correctly (so for many recipients, it's updated for each)
    ///
    /// Any failed sends are tracked via Cmd::HandlePeerFailedSend, which will log dysfunction for any peers
    /// in the section (otherwise ignoring failed send to out of section nodes or clients)
    async fn send_msg_via_comms(&self, recipients: &[Peer], wire_msg: WireMsg) -> Result<Vec<Cmd>> {
        let status = self.comm.send(recipients, wire_msg).await?;

        match status {
            DeliveryStatus::DeliveredToAll(failed_recipients)
            | DeliveryStatus::FailedToDeliverAll(failed_recipients) => Ok(failed_recipients
                .into_iter()
                .map(Cmd::HandlePeerFailedSend)
                .collect()),
            _ => Ok(vec![]),
        }
    }

    async fn handle_scheduled_dkg_timeout(&self, duration: Duration, token: u64) -> Option<Cmd> {
        let mut cancel_rx = self.dkg_timeout.cancel_timer_rx.clone();

        if *cancel_rx.borrow() {
            // Timers are already cancelled, do nothing.
            return None;
        }

        tokio::select! {
            _ = time::sleep(duration) => Some(Cmd::HandleDkgTimeout(token)),
            _ = cancel_rx.changed() => None,
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        // Cancel all scheduled timers including any future ones.
        let _res = self.dkg_timeout.cancel_timer_tx.send(true);
    }
}

struct DkgTimeout {
    cancel_timer_tx: watch::Sender<bool>,
    cancel_timer_rx: watch::Receiver<bool>,
}
