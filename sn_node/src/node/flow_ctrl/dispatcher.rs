// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{messaging::Peers, Cmd, Error, MyNode, Result, STANDARD_CHANNEL_SIZE};

use sn_interface::{
    messaging::{system::NodeMsg, Dst, MsgId, MsgKind, WireMsg},
    network_knowledge::{NetworkKnowledge, SectionTreeUpdate},
    types::{DataAddress, Peer},
};

use qp2p::UsrMsgBytes;

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    RwLock,
};
use xor_name::XorName;

// Cmd Dispatcher.
pub(crate) struct Dispatcher {
    node: Arc<RwLock<MyNode>>,
    data_replication_sender: Sender<(Vec<DataAddress>, Peer)>,
}

impl Dispatcher {
    /// Creates dispatcher and returns a receiver for enqueing DataAddresses for replication to specific peers
    pub(crate) fn new(node: Arc<RwLock<MyNode>>) -> (Self, Receiver<(Vec<DataAddress>, Peer)>) {
        let (data_replication_sender, data_replication_receiver) = channel(STANDARD_CHANNEL_SIZE);
        (
            Self {
                node,
                data_replication_sender,
            },
            data_replication_receiver,
        )
    }

    pub(crate) fn node(&self) -> Arc<RwLock<MyNode>> {
        self.node.clone()
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd) -> Result<Vec<Cmd>> {
        match cmd {
            // Used purely for locking Join process
            Cmd::SendLockingJoinMsg {
                msg,
                msg_id,
                recipients,
                send_stream,
            } => {
                info!("[NODE READ]: getting lock for joins endmsg");
                let context = self.node().read().await.context();
                info!("[NODE READ]: got lock for join sendmsg");
                Ok(vec![Cmd::SendMsg {
                    msg,
                    msg_id,
                    recipients,
                    send_stream,
                    context,
                }])
            }
            Cmd::SendMsg {
                msg,
                msg_id,
                recipients,
                send_stream,
                context,
            } => {
                trace!("Sending msg: {msg_id:?}");

                let peer_msgs = {
                    into_msg_bytes(
                        &context.network_knowledge,
                        context.name,
                        msg,
                        msg_id,
                        recipients,
                    )?
                };

                let comm = context.comm.clone();

                let tasks = peer_msgs
                    .into_iter()
                    .map(|(peer, msg)| comm.send_out_bytes(peer, msg_id, msg, send_stream.clone()));
                let results = futures::future::join_all(tasks).await;

                // Any failed sends are tracked via Cmd::HandlePeerFailedSend, which will log dysfunction for any peers
                // in the section (otherwise ignoring failed send to out of section nodes or clients)
                let cmds = results
                    .into_iter()
                    .filter_map(|result| match result {
                        Err(Error::FailedSend(peer)) => {
                            Some(Cmd::HandleFailedSendToNode { peer, msg_id })
                        }
                        _ => None,
                    })
                    .collect();

                Ok(cmds)
            }
            Cmd::TrackNodeIssueInDysfunction { name, issue } => {
                let node = self.node.read().await;
                debug!("[NODE READ]: dysf tracking read got");
                node.log_node_issue(name, issue);
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
            Cmd::HandleAgreement { proposal, sig } => {
                debug!("[NODE WRITE]: general agreements node write...");
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: general agreements node write got");

                node.handle_general_agreements(proposal, sig).await
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
                // throttle_duration,
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
            Cmd::SetStorageLevel(new_level) => {
                let mut node = self.node.write().await;
                debug!("[NODE WRITE]: Setting storage level");

                node.data_storage.set_storage_level(new_level);

                Ok(vec![])
            }
        }
    }
}

// Serializes and signs the msg if it's a Client message,
// and produces one [`WireMsg`] instance per recipient -
// the last step before passing it over to comms module.
fn into_msg_bytes(
    network_knowledge: &NetworkKnowledge,
    our_node_name: XorName,
    msg: NodeMsg,
    msg_id: MsgId,
    recipients: Peers,
) -> Result<Vec<(Peer, UsrMsgBytes)>> {
    let (kind, payload) = MyNode::serialize_node_msg(our_node_name, msg)?;
    let recipients = match recipients {
        Peers::Single(peer) => vec![peer],
        Peers::Multiple(peers) => peers.into_iter().collect(),
    };

    // we first generate the XorName
    let dst = Dst {
        name: xor_name::rand::random(),
        section_key: bls::SecretKey::random().public_key(),
    };

    let mut initial_wire_msg = wire_msg(msg_id, payload, kind, dst);

    let _bytes = initial_wire_msg.serialize_and_cache_bytes()?;

    let mut msgs = vec![];
    for peer in recipients {
        match network_knowledge.generate_dst(&peer.name()) {
            Ok(dst) => {
                // TODO log errror here isntead of throwing
                let all_the_bytes = initial_wire_msg.serialize_with_new_dst(&dst)?;
                msgs.push((peer, all_the_bytes));
            }
            Err(error) => {
                error!("Could not get route for {peer:?}: {error}");
            }
        }
    }

    Ok(msgs)
}

fn wire_msg(msg_id: MsgId, payload: Bytes, auth: MsgKind, dst: Dst) -> WireMsg {
    #[allow(unused_mut)]
    let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

    #[cfg(feature = "test-utils")]
    let wire_msg = wire_msg.set_payload_debug(msg);
    wire_msg
}
