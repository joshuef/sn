// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{
        core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Recipients, MyNode, RejoinReason,
        Result,
    },
    storage::{Error as StorageError, StorageLevel},
};

use qp2p::{SendStream, UsrMsgBytes};
use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{
        data::{CmdResponse, DataResponse},
        system::{JoinResponse, NodeDataCmd, NodeEvent, NodeMsg},
        Dst, MsgId, NetworkMsg, WireMsg,
    },
    network_knowledge::{MembershipState, NetworkKnowledge},
    types::{
        log_markers::LogMarker, ClientId, Keypair, NodeId, Participant, PublicKey, ReplicatedData,
    },
};
use std::collections::BTreeSet;
use xor_name::XorName;

impl MyNode {
    /// Send a (`NetworkMsg`) to nodes
    /// NB: Since we are sending via comms, which only sends to nodes in our section
    /// this is only taking node ids as recipients.
    /// IF we need to send to clients here as well, we'll need to update the fn signature.
    pub(crate) fn send_msg(
        msg: NetworkMsg,
        msg_id: MsgId,
        recipients: BTreeSet<NodeId>,
        context: NodeContext,
    ) -> Result<()> {
        debug!("Sending msg: {msg_id:?}");
        let msgs = into_msg_bytes(
            &context.network_knowledge,
            context.name,
            msg,
            msg_id,
            recipients,
        )?;

        msgs.into_iter()
            .for_each(|(node_id, msg)| context.comm.send_out_bytes(node_id, msg_id, msg));

        Ok(())
    }

    /// Send a (`NodeMsg`) message to all Elders in our section
    pub(crate) fn send_to_elders(context: &NodeContext, msg: NodeMsg) -> Cmd {
        let sap = context.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        Cmd::send_msg(msg, Recipients::Multiple(recipients))
    }

    /// Send a (`NodeMsg`) message to all Elders in our section, await all responses & enqueue
    pub(crate) fn send_to_elders_await_responses(context: NodeContext, msg: NodeMsg) -> Cmd {
        let sap = context.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        Cmd::SendMsgEnqueueAnyResponse {
            msg,
            msg_id: MsgId::new(),
            recipients,
        }
    }

    pub(crate) async fn store_data_and_respond(
        context: &NodeContext,
        data: ReplicatedData,
        send_stream: SendStream,
        client_id: ClientId,
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

        let msg = DataResponse::CmdResponse {
            response,
            correlation_id,
        };
        cmds.push(Cmd::send_data_response(
            msg,
            correlation_id,
            client_id,
            send_stream,
        ));

        Ok(cmds)
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_node_msg(
        node: &mut MyNode,
        context: NodeContext,
        msg_id: MsgId,
        msg: NodeMsg,
        node_id: NodeId,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        debug!("{:?}: {msg_id:?}", LogMarker::NodeMsgToBeHandled);

        match msg {
            NodeMsg::TryJoin(relocation) => {
                trace!("Handling msg {:?}: TryJoin from {}", msg_id, node_id);
                MyNode::handle_join(node, &context, node_id, msg_id, relocation, send_stream)
                    .map(|c| c.into_iter().collect())
            }
            NodeMsg::PrepareToRelocate(relocation_trigger) => {
                trace!("Handling PrepareToRelocate msg from {node_id}: {msg_id:?}");
                Ok(node.prepare_to_relocate(relocation_trigger))
            }
            NodeMsg::ProceedRelocation(dst) => {
                trace!("Handling ProceedRelocation msg from {node_id}: {msg_id:?}");
                Ok(node.proceed_relocation(node_id.name(), dst)?)
            }

            NodeMsg::CompleteRelocation(signed_relocation) => {
                trace!("Handling CompleteRelocation msg from {node_id}: {msg_id:?}");
                Ok(node.relocate(signed_relocation)?.into_iter().collect())
            }
            // The approval or rejection of a join (approval both for new network joiner as well as
            // existing node relocated to the section) will be received here.
            NodeMsg::JoinResponse(join_response) => {
                if context.network_knowledge.is_section_member(&context.name) {
                    // we can ignore this reponse msg
                    trace!("Join response received when we're already a member. Ignoring.");
                    return Ok(vec![]);
                }

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
                            "=========>> This node ({:?} @ {:?}) has been approved to join the section at {:?}!", context.name, context.info.addr,
                            target_sap.prefix(),
                        );

                        Ok(vec![])
                    }
                    JoinResponse::UnderConsideration => {
                        info!("Our join request is being considered by the network");
                        Ok(vec![])
                    }
                }
            }
            NodeMsg::HandoverVotes(votes) => node.handle_handover_msg(node_id, votes),
            NodeMsg::HandoverAE(gen) => Ok(node
                .handle_handover_anti_entropy(node_id, gen)
                .into_iter()
                .collect()),
            NodeMsg::MembershipVotes(votes) => {
                let mut cmds = vec![];
                cmds.extend(node.handle_membership_votes(node_id, votes)?);
                Ok(cmds)
            }
            NodeMsg::MembershipAE(gen) => {
                Ok(
                    MyNode::handle_membership_anti_entropy_request(&node.membership, node_id, gen)
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::ProposeNodeOff {
                vote_node_off: proposal,
                sig_share,
            } => {
                if node.is_not_elder() {
                    trace!("Adult handling a ProposeSectionState msg from {node_id}: {msg_id:?}",);
                }

                trace!("Handling ProposeSectionState msg {proposal:?} from {node_id}: {msg_id:?}",);
                node.untrack_node_issue(node_id.name(), IssueType::ElderVoting);
                node.handle_section_state_proposal(msg_id, proposal, sig_share, node_id)
            }
            NodeMsg::DkgStart(session_id, elder_sig) => {
                trace!(
                    "Handling msg: DkgStart s{} {:?}: {} elders from {node_id}",
                    session_id.sh(),
                    session_id.prefix,
                    session_id.elders.len(),
                );

                node.untrack_node_issue(node_id.name(), IssueType::Dkg);
                node.handle_dkg_start(session_id, elder_sig)
            }
            NodeMsg::DkgEphemeralPubKey {
                session_id,
                section_auth,
                pub_key,
                sig,
            } => {
                trace!(
                    "{} s{} from {node_id}",
                    LogMarker::DkgHandleEphemeralPubKey,
                    session_id.sh(),
                );
                node.handle_dkg_ephemeral_pubkey(&session_id, section_auth, pub_key, sig, node_id)
            }
            NodeMsg::DkgVotes {
                session_id,
                pub_keys,
                votes,
            } => {
                trace!(
                    "{} s{} from {node_id}: {votes:?}",
                    LogMarker::DkgVotesHandling,
                    session_id.sh(),
                );
                node.untrack_node_issue(node_id.name(), IssueType::Dkg);

                node.handle_dkg_votes(&session_id, pub_keys, votes, node_id)
            }
            NodeMsg::DkgAE(session_id) => {
                trace!("Handling msg: DkgAE s{} from {}", session_id.sh(), node_id);
                node.handle_dkg_anti_entropy(session_id, node_id)
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
                // NB: We would only reach here if this is a Spentbook cmd.
                trace!("Attempting to store data locally: {:?}", data.address());
                // TODO: proper err
                let Some(stream) = send_stream else {
                    return Ok(vec![])
                };
                // NB!! `sender` is actually a node here and should not be casted to ClientId! But since we are reusing the
                // `store_data_and_respond` fn which is used when a forwarded client cmd comes in, we have to cast to ClientId here.. TO BE FIXED.
                let sender = ClientId::from(Participant::from_node(node_id));
                // store data and respond w/ack on the response stream
                MyNode::store_data_and_respond(&context, data, stream, sender, msg_id).await
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(data_collection)) => {
                info!("ReplicateDataBatch MsgId: {:?}", msg_id);
                MyNode::replicate_data_batch(&context, node_id, data_collection).await
            }
            NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(known_data_addresses)) => {
                info!(
                    "{:?} MsgId: {:?}",
                    LogMarker::RequestForAnyMissingData,
                    msg_id
                );

                Ok(
                    MyNode::get_missing_data_for_node(&context, node_id, known_data_addresses)
                        .await
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::RequestHandover { sap, sig_share } => {
                info!("RequestHandover with msg_id {msg_id:?}");
                node.handle_handover_request(msg_id, sap, sig_share, node_id)
            }
            NodeMsg::SectionHandoverPromotion { sap, sig_share } => {
                info!("SectionHandoverPromotion with msg_id {msg_id:?}");
                node.handle_handover_promotion(msg_id, sap, sig_share, node_id)
            }
            NodeMsg::SectionSplitPromotion {
                sap0,
                sig_share0,
                sap1,
                sig_share1,
            } => {
                info!("SectionSplitPromotion with msg_id {msg_id:?}");
                node.handle_section_split_promotion(
                    msg_id, sap0, sig_share0, sap1, sig_share1, node_id,
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
    msg: NetworkMsg,
    msg_id: MsgId,
    recipients: BTreeSet<NodeId>,
) -> Result<Vec<(NodeId, UsrMsgBytes)>> {
    let (kind, payload) = MyNode::serialize_msg(our_node_name, &msg)?;

    // we first generate the XorName
    let dst = Dst {
        name: xor_name::rand::random(),
        section_key: bls::SecretKey::random().public_key(),
    };

    let mut initial_wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
    let _bytes = initial_wire_msg.serialize_and_cache_bytes()?;

    let mut msgs = vec![];
    for node_id in recipients {
        match network_knowledge.generate_dst(&node_id.name()) {
            Ok(dst) => {
                debug!("Dst generated for outgoing msg is: {dst:?}");
                // TODO log error here isntead of throwing
                let all_the_bytes = initial_wire_msg.serialize_with_new_dst(&dst)?;
                msgs.push((node_id, all_the_bytes));
            }
            Err(error) => {
                error!("Could not get route for {node_id:?}: {error}");
            }
        }
    }

    Ok(msgs)
}
