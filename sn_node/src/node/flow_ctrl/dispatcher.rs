// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Cmd, Error, MyNode, Result, STANDARD_CHANNEL_SIZE};

use sn_interface::{
    network_knowledge::SectionTreeUpdate,
    types::{DataAddress, Peer},
};

use std::sync::Arc;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    RwLock,
};

// Cmd Dispatcher.
pub(crate) struct Dispatcher {
    node: Arc<RwLock<MyNode>>,
    data_replication_sender: Sender<(Vec<DataAddress>, Peer)>,
}

impl Dispatcher {
    /// Creates dispatcher and returns a receiver for enqueing DataAddresses for replication to specific peers
    pub(crate) fn new(node: Arc<RwLock<MyNode>>) -> (Self, Receiver<(Vec<DataAddress>, Peer)>) {
        let (data_replication_sender, data_replication_receiver) = channel(STANDARD_CHANNEL_SIZE);
        let dispatcher = Self {
            node,
            data_replication_sender,
        };

        (dispatcher, data_replication_receiver)
    }

    pub(crate) fn node(&self) -> Arc<RwLock<MyNode>> {
        self.node.clone()
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        match cmd {
            Cmd::TryJoinNetwork => {
                info!("[NODE READ]: getting lock for try_join_network");
                let context = self.node().read().await.context();
                info!("[NODE READ]: got lock for try_join_network");
                Ok(MyNode::try_join_network(&context).into_iter().collect())
            }
            // Used purely for locking Join process
            Cmd::SendLockingJoinMsg {
                msg,
                msg_id,
                recipients,
            } => {
                info!("[NODE READ]: getting lock for join sendmsg");
                let context = self.node().read().await.context();
                info!("[NODE READ]: got lock for join sendmsg");
                Ok(vec![Cmd::SendMsg {
                    msg,
                    msg_id,
                    recipients,
                    context,
                }])
            }
            Cmd::SendMsg {
                msg,
                msg_id,
                recipients,
                context,
            } => MyNode::send_msg(msg, msg_id, recipients, context).await,
            Cmd::SendNodeMsgResponse {
                msg,
                msg_id,
                recipient,
                send_stream,
                context,
            } => MyNode::send_node_msg_response(msg, msg_id, recipient, context, send_stream).await,
            Cmd::SendClientResponse {
                msg,
                correlation_id,
                send_stream,
                context,
                source_client,
            } => {
                MyNode::send_client_response(
                    msg,
                    correlation_id,
                    send_stream,
                    context,
                    source_client,
                )
                .await?;
                Ok(vec![])
            }
            Cmd::SendNodeDataResponse {
                msg,
                correlation_id,
                send_stream,
                context,
                requesting_peer,
            } => {
                MyNode::send_node_data_response(
                    msg,
                    correlation_id,
                    send_stream,
                    context,
                    requesting_peer,
                )
                .await?;
                Ok(vec![])
            }
            Cmd::SendMsgAndAwaitResponse {
                msg_id,
                msg,
                context,
                targets,
                client_stream,
                source_client,
            } => {
                MyNode::send_msg_and_await_response(
                    msg_id,
                    msg,
                    context,
                    targets,
                    client_stream,
                    source_client,
                )
                .await
            }
            Cmd::TrackNodeIssue { name, issue } => {
                let node = self.node.read().await;
                debug!("[NODE READ]: fault tracking read got");
                node.track_node_issue(name, issue);
                Ok(vec![])
            }
            Cmd::HandleMsg {
                origin,
                wire_msg,
                send_stream,
            } => MyNode::handle_msg(self.node.clone(), origin, wire_msg, send_stream).await,
            Cmd::UpdateNetworkAndHandleValidClientMsg {
                proof_chain,
                signed_sap,
                msg_id,
                msg,
                origin,
                auth,
                send_stream,
            } => {
                debug!("Updating network knowledge before handling message");
                let mut context = self.node.read().await.context();
                debug!("[NODE READ]: update client knowledge got");

                let name = context.name;
                let there_was_an_update = context.network_knowledge.update_knowledge_if_valid(
                    SectionTreeUpdate::new(signed_sap.clone(), proof_chain.clone()),
                    None,
                    &name,
                )?;

                if there_was_an_update {
                    // okay lets do it for real
                    let mut node = self.node.write().await;
                    debug!("[NODE WRITE]: update client write got");
                    let updated = node.network_knowledge.update_knowledge_if_valid(
                        SectionTreeUpdate::new(signed_sap, proof_chain),
                        None,
                        &name,
                    )?;
                    debug!("Network knowledge was updated: {updated}");
                }

                debug!("[NODE READ]: update & validate msg lock got");

                MyNode::handle_valid_client_msg(context, msg_id, msg, auth, origin, send_stream)
                    .await
            }
            Cmd::HandleSectionDecisionAgreement { proposal, sig } => {
                debug!("[NODE WRITE]: section decision agreements node write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: section decision agreements node write got");
                node.handle_section_decision_agreement(proposal, sig)
            }
            Cmd::HandleMembershipDecision(decision) => {
                debug!("[NODE WRITE]: membership decision agreements write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: membership decision agreements write got...");
                node.handle_membership_decision(decision).await
            }
            Cmd::HandleNewEldersAgreement { new_elders, sig } => {
                debug!("[NODE WRITE]: new elders decision agreements write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: new elders decision agreements write got...");
                node.handle_new_elders_agreement(new_elders, sig).await
            }
            Cmd::HandleNewSectionsAgreement {
                sap1,
                sig1,
                sap2,
                sig2,
            } => {
                debug!("[NODE WRITE]: new sections decision agreements write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: new sections decision agreements write got...");
                node.handle_new_sections_agreement(sap1, sig1, sap2, sig2)
                    .await
            }
            Cmd::HandleFailedSendToNode { peer, msg_id } => {
                warn!("Message sending failed to {peer}, for {msg_id:?}");
                let node = self.node.read().await;
                debug!("[NODE READ]: HandleFailedSendToNode agreements read got...");
                node.handle_failed_send(&peer.addr());
                Ok(vec![])
            }
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
            } => {
                debug!("[NODE WRITE]: HandleDKg agreements write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: HandleDKg agreements write got...");
                node.handle_dkg_outcome(section_auth, outcome).await
            }
            Cmd::EnqueueDataForReplication {
                recipient,
                data_batch,
            } => {
                self.data_replication_sender
                    .send((data_batch, recipient))
                    .await
                    .map_err(|_| Error::DataReplicationChannel)?;
                Ok(vec![])
            }
            Cmd::ProposeVoteNodesOffline(names) => {
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: propose offline write got");
                node.cast_offline_proposals(&names)
            }
            Cmd::SetJoinsAllowed(joins_allowed) => {
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: Setting joins allowed..");
                node.joins_allowed = joins_allowed;
                Ok(vec![])
            }
            Cmd::SetJoinsAllowedUntilSplit(joins_allowed_until_split) => {
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: Setting joins allowed until split..");
                node.joins_allowed = joins_allowed_until_split;
                node.joins_allowed_until_split = joins_allowed_until_split;
                Ok(vec![])
            }
        }
    }
}
