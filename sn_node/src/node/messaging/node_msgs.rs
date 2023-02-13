// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{
        core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, MyNode, RejoinReason, Result,
    },
    storage::{Error as StorageError, StorageLevel},
};

use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{
        data::{ClientDataResponse, CmdResponse},
        system::{JoinResponse, NodeDataCmd, NodeEvent, NodeMsg},
        Dst, MsgId, WireMsg,
    },
    network_knowledge::{MembershipState, NetworkKnowledge},
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey, ReplicatedData},
};

use qp2p::{SendStream, UsrMsgBytes};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::RwLock;
use xor_name::XorName;

impl MyNode {
    /// Send a (`NodeMsg`) message to peers
    pub(crate) fn send_msg(
        msg: NodeMsg,
        msg_id: MsgId,
        recipients: Peers,
        context: NodeContext,
    ) -> Result<()> {
        debug!("Sending msg: {msg_id:?}");
        let peer_msgs = into_msg_bytes(
            &context.network_knowledge,
            context.name,
            msg,
            msg_id,
            recipients,
        )?;

        peer_msgs
            .into_iter()
            .for_each(|(peer, msg)| context.comm.send_out_bytes(peer, msg_id, msg));

        Ok(())
    }

    /// Send a (`NodeMsg`) message to all Elders in our section
    pub(crate) fn send_to_elders(context: &NodeContext, msg: NodeMsg) -> Cmd {
        let sap = context.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        Cmd::send_msg(msg, Peers::Multiple(recipients), context.clone())
    }

    /// Send a (`NodeMsg`) message to all Elders in our section, await all responses & enqueue
    pub(crate) fn send_to_elders_await_responses(context: NodeContext, msg: NodeMsg) -> Cmd {
        let sap = context.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        Cmd::SendMsgEnqueueAnyResponse {
            msg,
            msg_id: MsgId::new(),
            recipients,
            context,
        }
    }

    pub(crate) async fn store_data_and_respond(
        context: &NodeContext,
        data: ReplicatedData,
        send_stream: SendStream,
        source_client: Peer,
        correlation_id: MsgId,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(context.keypair.clone());
        let data_addr = data.address();

        trace!("About to store data from {correlation_id:?}: {data_addr:?}");

        // This may return a DatabaseFull error... but we should have
        // reported StorageError::NotEnoughSpace well before this
        let response = match context
            .data_storage
            .store(&data, section_pk, node_keypair.clone())
            .await
        {
            Ok(storage_level) => {
                info!("{correlation_id:?} Data has been stored: {data_addr:?}");
                if matches!(storage_level, StorageLevel::Updated(_level)) {
                    // we add a new node for every level increase of used space
                    cmds.push(Cmd::SetJoinsAllowed(true));
                } else if context.data_storage.has_reached_min_capacity()
                    && !context.joins_allowed_until_split
                {
                    // we accept new nodes until split, since we have reached the min capacity (i.e. storage limit)
                    cmds.push(Cmd::SetJoinsAllowedUntilSplit(true));
                }
                CmdResponse::ok(data)?
            }
            Err(StorageError::NotEnoughSpace) => {
                // storage full
                error!("Not enough space to store data {data_addr:?}");
                let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                    node_id: PublicKey::from(context.keypair.public),
                    data_address: data.address(),
                    full: true,
                });

                if context.is_elder && !context.joins_allowed {
                    // we accept new nodes until split, since we ran out of space
                    cmds.push(Cmd::SetJoinsAllowedUntilSplit(true));
                }

                cmds.push(MyNode::send_to_elders(context, msg));
                CmdResponse::err(data, StorageError::NotEnoughSpace.into())?
            }
            Err(error) => {
                // the rest seem to be non-problematic errors.. (?)
                // this could be an "we already have it" error... so we should continue with that...
                error!("Problem storing data {data_addr:?}, but it was ignored: {error}");
                CmdResponse::ok(data)?
            }
        };

        let msg = ClientDataResponse::CmdResponse {
            response,
            correlation_id,
        };
        cmds.push(Cmd::SendClientResponse {
            msg,
            correlation_id,
            send_stream,
            context: context.clone(),
            source_client,
        });

        Ok(cmds)
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_node_msg(
        node: Arc<RwLock<MyNode>>,
        context: NodeContext,
        msg_id: MsgId,
        msg: NodeMsg,
        sender: Peer,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        debug!("{:?}: {msg_id:?}", LogMarker::NodeMsgToBeHandled);

        match msg {
            NodeMsg::TryJoin(relocation) => {
                trace!("Handling msg {:?}: TryJoin from {}", msg_id, sender);
                MyNode::handle_join(node, &context, sender, relocation, send_stream)
                    .await
                    .map(|c| c.into_iter().collect())
            }
            NodeMsg::BeginRelocating(relocation_trigger) => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: BeginRelocating write gottt...");
                trace!("Handling BeginRelocating msg from {sender}: {msg_id:?}");
                Ok(node.handle_begin_relocating(relocation_trigger))
            }
            NodeMsg::RelocationRequest {
                relocation_node,
                relocation_trigger,
            } => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: RelocationRequest write gottt...");
                trace!("Handling RelocationRequest msg from {sender}: {msg_id:?}");
                Ok(node.handle_relocation_request(relocation_node, relocation_trigger)?)
            }

            NodeMsg::Relocate(signed_relocation) => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: Relocated write gottt...");
                trace!("Handling Relocate msg from {sender}: {msg_id:?}");
                Ok(node.relocate(signed_relocation)?.into_iter().collect())
            }
            NodeMsg::AntiEntropy {
                section_tree_update,
                kind,
            } => {
                trace!("Handling msg: AE from {sender}: {msg_id:?}");
                // as we've data storage reqs inside here for reorganisation, we have async calls to
                // the fs
                MyNode::handle_anti_entropy_msg(node, context, section_tree_update, kind, sender)
                    .await
            }
            // Respond to a probe msg
            // We always respond to probe msgs if we're an elder as health checks use this to see if a node is alive
            // and repsonsive, as well as being a method of keeping nodes up to date.
            NodeMsg::AntiEntropyProbe(section_key) => {
                trace!("Aeprobe in");

                let mut cmds = vec![];
                if !context.is_elder {
                    info!("Dropping AEProbe since we are not an elder");
                    // early return here as we do not get health checks as adults,
                    // normal AE rules should have applied
                    return Ok(cmds);
                }

                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                cmds.push(MyNode::send_ae_update_to_nodes(
                    &context,
                    Peers::Single(sender),
                    section_key,
                ));
                Ok(cmds)
            }
            // The approval or rejection of a join (approval both for new network joiner as well as
            // existing node relocated to the section) will be received here.
            NodeMsg::JoinResponse(join_response) => {
                match join_response {
                    JoinResponse::Rejected(reason) => Err(super::Error::RejoinRequired(
                        RejoinReason::from_reject_reason(reason),
                    )),
                    JoinResponse::Approved(decision) => {
                        info!("{}", LogMarker::ReceivedJoinApproval);
                        let target_sap = context.network_knowledge.signed_sap();

                        if let Err(e) = decision.validate(&target_sap.public_key_set()) {
                            error!("Failed to validate with {target_sap:?}, dropping invalid join decision: {e:?}");
                            return Ok(vec![]);
                        }

                        // Ensure this decision includes us as a joining node
                        if decision
                            .proposals
                            .keys()
                            .filter(|n| n.state() == MembershipState::Joined)
                            .all(|n| n.name() != context.name)
                        {
                            trace!("MyNode named: {:?} Ignore join approval decision not for us: {decision:?}", context.name);
                            return Ok(vec![]);
                        }

                        info!(
                            "=========>> This node ({:?} @ {:?}) has been approved to join the section at {:?} with age: {:?}!", context.name, context.info.addr,
                            target_sap.prefix(), context.info.age()
                        );

                        // if decision
                        //     .proposals
                        //     .keys()
                        //     .filter(|n| n.state() == MembershipState::Joined)
                        //     .filter(|n| n.name() == context.name)
                        //     .any(|n| n.previous_name().is_some())
                        // {
                        //     // We could clear the cached relocation proof here,
                        //     // but we have the periodic check doing it, so no need to duplicate the logic.
                        //     info!(
                        //         "{}: new age: {:?}",
                        //         LogMarker::RelocateEnded,
                        //         context.info.age()
                        //     );
                        // }

                        Ok(vec![])
                    }
                    JoinResponse::UnderConsideration => {
                        info!("Our join request is being considered by the network");
                        Ok(vec![])
                    }
                }
            }
            NodeMsg::HandoverVotes(votes) => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: handover votes write gottt...");
                node.handle_handover_msg(sender, votes)
            }
            NodeMsg::HandoverAE(gen) => {
                trace!("[NODE READ]: handover ae attempts");
                let node = node.read().await;
                trace!("[NODE READ]: handover ae got");

                Ok(node
                    .handle_handover_anti_entropy(sender, gen)
                    .into_iter()
                    .collect())
            }
            NodeMsg::MembershipVotes(votes) => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: MembershipVotes write.");
                let mut cmds = vec![];
                cmds.extend(node.handle_membership_votes(sender, votes)?);
                Ok(cmds)
            }
            NodeMsg::MembershipAE(gen) => {
                let (node_context, membership_context) = {
                    trace!("[NODE READ]: membership ae read ");
                    let membership = node.read().await.membership.clone();
                    trace!("[NODE READ]: membership ae read got");
                    (context, membership)
                };

                Ok(MyNode::handle_membership_anti_entropy(
                    membership_context,
                    node_context,
                    sender,
                    gen,
                )
                .into_iter()
                .collect())
            }
            NodeMsg::ProposeSectionState {
                proposal,
                sig_share,
            } => {
                let mut node = node.write().await;
                trace!("[NODE WRITE]: ProposeSectionState write.");
                if node.is_not_elder() {
                    trace!(
                        "Adult handling a ProposeSectionState msg from {}: {:?}",
                        sender,
                        msg_id
                    );
                }

                trace!(
                    "Handling ProposeSectionState msg: {proposal:?} from {}: {:?}",
                    sender,
                    msg_id
                );
                node.untrack_node_issue(sender.name(), IssueType::ElderVoting);
                node.handle_section_state_proposal(msg_id, proposal, sig_share, sender)
            }
            NodeMsg::DkgStart(session_id, elder_sig) => {
                trace!(
                    "Handling msg: DkgStart s{} {:?}: {} elders from {}",
                    session_id.sh(),
                    session_id.prefix,
                    session_id.elders.len(),
                    sender
                );

                let mut node = node.write().await;
                trace!("[NODE WRITE]: DKGstart write gottt...");
                node.untrack_node_issue(sender.name(), IssueType::Dkg);
                node.handle_dkg_start(session_id, elder_sig)
            }
            NodeMsg::DkgEphemeralPubKey {
                session_id,
                section_auth,
                pub_key,
                sig,
            } => {
                trace!(
                    "{} s{} from {}",
                    LogMarker::DkgHandleEphemeralPubKey,
                    session_id.sh(),
                    sender
                );
                let mut node = node.write().await;
                trace!("[NODE WRITE]: DKG Ephemeral write gottt...");
                node.handle_dkg_ephemeral_pubkey(&session_id, section_auth, pub_key, sig, sender)
            }
            NodeMsg::DkgVotes {
                session_id,
                pub_keys,
                votes,
            } => {
                trace!(
                    "{} s{} from {}: {:?}",
                    LogMarker::DkgVotesHandling,
                    session_id.sh(),
                    sender,
                    votes
                );
                let mut node = node.write().await;
                trace!("[NODE WRITE]: DKG Votes write gottt...");

                node.untrack_node_issue(sender.name(), IssueType::Dkg);

                node.handle_dkg_votes(&session_id, pub_keys, votes, sender)
            }
            NodeMsg::DkgAE(session_id) => {
                trace!("[NODE READ]: dkg ae read ");

                let node = node.read().await;
                trace!("[NODE READ]: dkg ae read got");
                trace!("Handling msg: DkgAE s{} from {}", session_id.sh(), sender);
                node.handle_dkg_anti_entropy(session_id, sender)
            }
            NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                node_id,
                data_address,
                full,
            }) => {
                info!("Processing CouldNotStoreData event with {msg_id:?} at: {data_address:?}, (node reporting full: {full})");

                if !context.is_elder {
                    error!("Received unexpected message while Adult");
                    return Ok(vec![]);
                }

                let mut cmds = vec![];

                if !context.joins_allowed {
                    cmds.push(Cmd::SetJoinsAllowed(true));
                    // NB: we do not also set allowed until split, since we
                    // do not expect another node to run out of space before we ourselves
                    // have reached the storage limit (i.e. the `min_capacity` variable, which
                    // should be set by the node operator to be a little bit lower than the actual space).
                }

                // only when the node is severely out of sync with the rest, do we vote it off straight away
                // othwerwise we report it as an issue (see further down)
                if full && context.data_storage.is_below_half_limit() {
                    warn!("Node {node_id} prematurely reported full. Voting it off..");
                    let nodes = BTreeSet::from([node_id.into()]);
                    cmds.push(Cmd::ProposeVoteNodesOffline(nodes));
                    return Ok(cmds);
                }

                // we report it as an issue, to give it some slack
                context.track_node_issue(node_id.into(), IssueType::Communication);

                Ok(cmds)
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data)) => {
                trace!("Attempting to store data locally: {:?}", data.address());
                // TODO: proper err
                let Some(stream) = send_stream else {
                    return Ok(vec![])
                };
                // store data and respond w/ack on the response stream
                MyNode::store_data_and_respond(&context, data, stream, sender, msg_id).await
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(data_collection)) => {
                info!("ReplicateDataBatch MsgId: {:?}", msg_id);
                MyNode::replicate_data_batch(&context, sender, data_collection).await
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(known_data_addresses)) => {
                info!(
                    "{:?} MsgId: {:?}",
                    LogMarker::RequestForAnyMissingData,
                    msg_id
                );

                Ok(
                    MyNode::get_missing_data_for_node(&context, sender, known_data_addresses)
                        .await
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::RequestHandover { sap, sig_share } => {
                info!("RequestHandover with msg_id {msg_id:?}");
                let mut node = node.write().await;

                trace!("[NODE WRITE]: RequestHandover write gottt...");
                node.handle_handover_request(msg_id, sap, sig_share, sender)
            }
            NodeMsg::SectionHandoverPromotion { sap, sig_share } => {
                info!("SectionHandoverPromotion with msg_id {msg_id:?}");
                let mut node = node.write().await;

                trace!("[NODE WRITE]: SectionHandoverPromotion write gottt...");
                node.handle_handover_promotion(msg_id, sap, sig_share, sender)
            }
            NodeMsg::SectionSplitPromotion {
                sap0,
                sig_share0,
                sap1,
                sig_share1,
            } => {
                info!("SectionSplitPromotion with msg_id {msg_id:?}");
                let mut node = node.write().await;

                trace!("[NODE WRITE]: SectionSplitPromotion write gottt...");
                node.handle_section_split_promotion(
                    msg_id, sap0, sig_share0, sap1, sig_share1, sender,
                )
            }
        }
    }
}

// Serializes the msg, producing one [`WireMsg`] instance
// per recipient - the last step before passing it over to comms module.
pub(crate) fn into_msg_bytes(
    network_knowledge: &NetworkKnowledge,
    our_node_name: XorName,
    msg: NodeMsg,
    msg_id: MsgId,
    recipients: Peers,
) -> Result<Vec<(Peer, UsrMsgBytes)>> {
    let (kind, payload) = MyNode::serialize_node_msg(our_node_name, &msg)?;
    let recipients = match recipients {
        Peers::Single(peer) => vec![peer],
        Peers::Multiple(peers) => peers.into_iter().collect(),
    };

    // we first generate the XorName
    let dst = Dst {
        name: xor_name::rand::random(),
        section_key: bls::SecretKey::random().public_key(),
    };

    let mut initial_wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
    let _bytes = initial_wire_msg.serialize_and_cache_bytes()?;

    let mut msgs = vec![];
    for peer in recipients {
        match network_knowledge.generate_dst(&peer.name()) {
            Ok(dst) => {
                trace!("Dst generated for outgoing msg is: {dst:?}");
                // TODO log error here isntead of throwing
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
