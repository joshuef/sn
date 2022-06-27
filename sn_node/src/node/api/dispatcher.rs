// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::Cmd,
    core::{DeliveryStatus, Node, Proposal},
    messages::WireMsgUtils,
    Result,
};

use sn_interface::{
    messaging::{system::SystemMsg, AuthKind, WireMsg},
    types::Peer,
};

use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{sync::watch, time, sync::RwLock};

// Cmd Dispatcher.
#[derive(Clone)]
pub(crate) struct Dispatcher {
    node: Arc<RwLock<Node>>,
    dkg_timeout: Arc<DkgTimeout>,
}

impl Dispatcher {
    pub(super) fn new(node: Arc<RwLock<Node>>) -> Self {
        let (cancel_timer_tx, cancel_timer_rx) = watch::channel(false);
        let dkg_timeout = Arc::new(DkgTimeout {
            cancel_timer_tx,
            cancel_timer_rx,
        });

        Self { node, dkg_timeout }
    }

    pub(crate) fn node(&self) -> Arc<RwLock<Node>> {
        self.node.clone()
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        let node = self.node.read().await;
        match cmd {
            Cmd::CleanupPeerLinks => {
                node.cleanup_non_elder_peers().await?;
                Ok(vec![])
            }
            Cmd::SignOutgoingSystemMsg { msg, dst } => {


                let src_section_pk = node.network_knowledge().section_key().await;
                let wire_msg =
                    WireMsg::single_src(&*node.info.read().await, dst, msg, src_section_pk)?;

                let mut cmds = vec![];
                cmds.extend(node.send_msg_to_nodes(wire_msg).await?);

                Ok(cmds)
            }
            Cmd::HandleMsg {
                sender,
                wire_msg,
                original_bytes,
            } => {
                let mut node = self.node.write().await;

                node.handle_msg(sender, wire_msg, original_bytes).await
            },
            Cmd::HandleDkgTimeout(token) => node.handle_dkg_timeout(token).await,
            Cmd::HandleAgreement { proposal, sig } => {
                let mut node = self.node.write().await;

                node.handle_general_agreements(proposal, sig).await
            }
            Cmd::HandleNewNodeOnline(auth) => {
                node
                    .handle_online_agreement(auth.value.into_state(), auth.sig)
                    .await
            }
            Cmd::HandleNodeLeft(auth) => {
                node
                    .handle_node_left(auth.value.into_state(), auth.sig)
                    .await
            }
            Cmd::HandleNewEldersAgreement { proposal, sig } => match proposal {
                Proposal::NewElders(section_auth) => {
                    let mut node = self.node.write().await;

                    node
                        .handle_new_elders_agreement(section_auth, sig)
                        .await
                }
                _ => {
                    error!("Other agreement messages should be handled in `HandleAgreement`, which is non-blocking ");
                    Ok(vec![])
                }
            },
            Cmd::HandlePeerLost(peer) => node.handle_peer_lost(&peer.addr()).await,
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
                generation,
            } => {
                let mut node = self.node.write().await;

                node
                    .handle_dkg_outcome(section_auth, outcome, generation)
                    .await
            }
            Cmd::HandleDkgFailure(signeds) => node
                .handle_dkg_failure(signeds)
                .await
                .map(|cmd| vec![cmd]),
            Cmd::SendMsg {
                recipients,
                wire_msg,
            } => self.send_msg(&recipients, recipients.len(), wire_msg).await,
            Cmd::EnqueueDataForReplication {
                // throttle_duration,
                recipient,
                data_batch,
            } => {
                // we should queue this
                for data in data_batch {
                    if let Some(mut data_entry) =
                        node.pending_data_to_replicate_to_peers.get_mut(&data)
                    {
                        let peer_set = data_entry.value_mut();
                        debug!("data already queued, adding peer");
                        let _existed = peer_set.insert(recipient);
                    } else {
                        // let queue = DashSet::new();
                        let mut peer_set = BTreeSet::new();
                        let _existed = peer_set.insert(recipient);
                        let _existed = node
                            .pending_data_to_replicate_to_peers
                            .insert(data, peer_set);
                    }
                }
                Ok(vec![])
            }
            Cmd::SendMsgDeliveryGroup {
                recipients,
                delivery_group_size,
                wire_msg,
            } => {
                self.send_msg(&recipients, delivery_group_size, wire_msg)
                    .await
            }
            Cmd::ScheduleDkgTimeout { duration, token } => Ok(self
                .handle_scheduled_dkg_timeout(duration, token)
                .await
                .into_iter()
                .collect()),
            Cmd::ProposeOffline(names) => node.cast_offline_proposals(&names).await,
            Cmd::StartConnectivityTest(name) => Ok(vec![
                node
                    .send_msg_to_our_elders(SystemMsg::StartConnectivityTest(name))
                    .await?,
            ]),
            Cmd::TestConnectivity(name) => {
                if let Some(member_info) = node
                    .network_knowledge()
                    .get_section_member(&name)
                    .await
                {
                    if node
                        .comm
                        .is_reachable(&member_info.addr())
                        .await
                        .is_err()
                    {
                        node.log_comm_issue(member_info.name()).await?
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
        let cmds = match wire_msg.auth_kind() {
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
                        .node.read().await
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

    async fn deliver_msgs(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let status = self
            .node.read().await
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
