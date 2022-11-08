// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{
        flow_ctrl::cmds::Cmd,
        messaging::{OutgoingMsg, Peers},
        Event, MembershipEvent, MyNode, Proposal as CoreProposal, Result, MIN_LEVEL_WHEN_FULL,
    },
    storage::Error as StorageError,
};
use sn_interface::types::ReplicatedData;

use qp2p::SendStream;
#[cfg(feature = "traceroute")]
use sn_interface::messaging::Traceroute;
use sn_interface::{
    messaging::{
        data::StorageLevel,
        system::{JoinResponse, NodeCmd, NodeEvent, NodeMsg, NodeQuery, Proposal as ProposalMsg},
        MsgId,
    },
    network_knowledge::NetworkKnowledge,
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey},
};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use xor_name::XorName;

impl MyNode {
    /// Send a (`NodeMsg`) message to all Elders in our section
    pub(crate) fn send_msg_to_our_elders(&self, msg: NodeMsg) -> Cmd {
        let sap = self.network_knowledge.section_auth();
        let recipients = sap.elders_set();
        self.send_system_msg(msg, Peers::Multiple(recipients))
    }

    /// Send a (`NodeMsg`) message to a node
    pub(crate) fn send_system_msg(&self, msg: NodeMsg, recipients: Peers) -> Cmd {
        self.trace_system_msg(
            msg,
            recipients,
            #[cfg(feature = "traceroute")]
            Traceroute(vec![]),
        )
    }

    /// Send a (`NodeMsg`) message to a node
    pub(crate) fn trace_system_msg(
        &self,
        msg: NodeMsg,
        recipients: Peers,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Cmd {
        trace!("{}: {:?}", LogMarker::SendToNodes, msg);
        Cmd::send_traced_msg(
            OutgoingMsg::Node(msg),
            recipients,
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    pub(crate) async fn store_data_as_adult_and_respond(
        &mut self,
        data: ReplicatedData,
        response_stream: Option<Arc<Mutex<SendStream>>>,
        target: Peer,
        original_msg_id: MsgId,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let section_pk = PublicKey::Bls(self.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(self.keypair.clone());
        let data_addr = data.address();
        trace!("About to store data from {original_msg_id:?}: {data_addr:?}");
        // TODO: Respond with errors etc over the bidi stream

        // We are an adult here, so just store away!
        // This may return a DatabaseFull error... but we should have reported storage increase
        // well before this
        match self
            .data_storage
            .store(&data, section_pk, node_keypair.clone())
            .await
        {
            Ok(level_report) => {
                info!("Storage level report: {:?}", level_report);
                cmds.extend(self.record_storage_level_if_any(
                    level_report,
                    #[cfg(feature = "traceroute")]
                    traceroute.clone(),
                )?);

                #[cfg(feature = "traceroute")]
                info!("End of message flow. Trace: {:?}", traceroute);
            }
            Err(StorageError::NotEnoughSpace) => {
                // storage full
                error!("Not enough space to store more data");
                let node_id = PublicKey::from(self.keypair.public);
                let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                    node_id,
                    data,
                    full: true,
                });

                cmds.push(self.send_msg_to_our_elders(msg))
            }
            Err(error) => {
                // the rest seem to be non-problematic errors.. (?)

                // this could be an "we already have it" error... so we should continue with that...
                error!("Problem storing data, but it was ignored: {error}");
            }
        }

        trace!("Data has been stored: {data_addr:?}");
        let msg = NodeMsg::NodeEvent(NodeEvent::DataStored(data_addr));
        let (kind, payload) = self.serialize_node_msg(msg)?;

        if let Some(stream) = response_stream {
            self.send_msg_on_stream(
                payload,
                kind,
                stream,
                Some(target),
                original_msg_id,
                traceroute.clone(),
            )
            .await?;
        } else {
            error!("Cannot respond over stream, none exists after storing! {data_addr:?}");
        }

        Ok(cmds)
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_valid_system_msg(
        node: Arc<RwLock<MyNode>>,
        msg_id: MsgId,
        msg: NodeMsg,
        sender: Peer,
        send_stream: Option<Arc<Mutex<SendStream>>>,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}: {msg_id:?}", LogMarker::NodeMsgToBeHandled);

        #[cfg(feature = "traceroute")]
        {
            if !traceroute.0.is_empty() {
                info!(
                    "Handling NodeMsg {}:{:?} with trace \n{:?}",
                    msg, msg_id, traceroute
                );
            }
        }

        match msg {
            NodeMsg::Relocate(node_state) => {
                let mut node = node.write().await;

                trace!("Handling msg: Relocate from {}: {:?}", sender, msg_id);
                Ok(node.handle_relocate(node_state)?.into_iter().collect())
            }
            NodeMsg::JoinAsRelocatedResponse(join_response) => {
                let mut node = node.write().await;
                trace!("Handling msg: JoinAsRelocatedResponse from {}", sender);
                if let Some(ref mut joining_as_relocated) = node.relocate_state {
                    if let Some(cmd) =
                        joining_as_relocated.handle_join_response(*join_response, sender.addr())?
                    {
                        return Ok(vec![cmd]);
                    }
                } else {
                    error!(
                        "No relocation in progress upon receiving {:?}",
                        join_response
                    );
                }

                Ok(vec![])
            }
            NodeMsg::AntiEntropy {
                section_tree_update,
                kind,
            } => {
                trace!("Handling msg: AE from {sender}: {msg_id:?}");
                let mut node = node.write().await;
                // as we've data storage reqs inside here for reorganisation, we have async calls to
                // the fs
                node.handle_anti_entropy_msg(
                    section_tree_update,
                    kind,
                    sender,
                    #[cfg(feature = "traceroute")]
                    traceroute,
                )
                .await
            }
            // Respond to a probe msg
            // We always respond to probe msgs if we're an elder as health checks use this to see if a node is alive
            // and repsonsive, as well as being a method of keeping nodes up to date.
            NodeMsg::AntiEntropyProbe(section_key) => {
                let node = node.read().await;
                let mut cmds = vec![];
                if !node.is_elder() {
                    // early return here as we do not get health checks as adults,
                    // normal AE rules should have applied
                    return Ok(cmds);
                }

                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                let mut recipients = BTreeSet::new();
                let _existed = recipients.insert(sender);
                cmds.push(node.send_ae_update_to_nodes(recipients, section_key));
                Ok(cmds)
            }
            // The AcceptedOnlineShare for relocation will be received here.
            NodeMsg::JoinResponse(join_response) => {
                let mut node = node.write().await;
                match *join_response {
                    JoinResponse::Approved {
                        section_tree_update,
                        ..
                    } => {
                        info!(
                            "Relocation: Aggregating received ApprovalShare from {:?}",
                            sender
                        );
                        info!("Relocation: Successfully aggregated ApprovalShares for joining the network");

                        if let Some(ref mut joining_as_relocated) = node.relocate_state {
                            let new_node = joining_as_relocated.node.clone();
                            let new_name = new_node.name();
                            let previous_name = node.info().name();
                            let new_keypair = new_node.keypair;

                            info!(
                                "Relocation: switching from {:?} to {:?}",
                                previous_name, new_name
                            );

                            let recipients: Vec<_> =
                                section_tree_update.signed_sap.elders().cloned().collect();

                            let section_tree = node.network_knowledge.section_tree().clone();
                            let new_network_knowledge =
                                NetworkKnowledge::new(section_tree, section_tree_update)?;

                            // TODO: confirm whether carry out the switch immediately here
                            //       or still using the cmd pattern.
                            //       As the sending of the JoinRequest as notification
                            //       may require the `node` to be switched to new already.

                            node.relocate(new_keypair.clone(), new_network_knowledge)?;

                            trace!(
                                "Relocation: Sending aggregated JoinRequest to {:?}",
                                recipients
                            );

                            // move off thread to keep fn sync
                            let event_sender = node.event_sender.clone();
                            let _handle = tokio::spawn(async move {
                                event_sender
                                    .send(Event::Membership(MembershipEvent::Relocated {
                                        previous_name,
                                        new_keypair,
                                    }))
                                    .await;
                            });

                            trace!("{}", LogMarker::RelocateEnd);
                        } else {
                            warn!("Relocation:  node.relocate_state is not in Progress");
                        }

                        Ok(vec![])
                    }
                    _ => {
                        debug!("Relocation: Ignoring unexpected join response message: {join_response:?}");
                        Ok(vec![])
                    }
                }
            }
            NodeMsg::HandoverVotes(votes) => {
                let mut node = node.write().await;

                node.handle_handover_msg(sender, votes)
            }
            NodeMsg::HandoverAE(gen) => {
                let node = node.read().await;
                Ok(node
                    .handle_handover_anti_entropy(sender, gen)
                    .into_iter()
                    .collect())
            }
            NodeMsg::JoinRequest(join_request) => {
                trace!("Handling msg {:?}: JoinRequest from {}", msg_id, sender);
                MyNode::handle_join_request(node, msg_id, sender, join_request)
                    .await
                    .map(|c| c.into_iter().collect())
            }
            NodeMsg::JoinAsRelocatedRequest(join_request) => {
                trace!("Handling msg: JoinAsRelocatedRequest from {}", sender);
                if node.read().await.is_not_elder()
                    && join_request.section_key == node.read().await.network_knowledge.section_key()
                {
                    return Ok(vec![]);
                }

                Ok(
                    MyNode::handle_join_as_relocated_request(node, sender, *join_request)
                        .await
                        .into_iter()
                        .collect(),
                )
            }
            NodeMsg::MembershipVotes(votes) => {
                let mut node = node.write().await;
                let mut cmds = vec![];
                cmds.extend(node.handle_membership_votes(sender, votes)?);
                Ok(cmds)
            }
            NodeMsg::MembershipAE(gen) => {
                let node = node.read().await;
                Ok(node
                    .handle_membership_anti_entropy(sender, gen)
                    .into_iter()
                    .collect())
            }
            NodeMsg::Propose {
                proposal,
                sig_share,
            } => {
                let mut node = node.write().await;
                if node.is_not_elder() {
                    trace!("Adult handling a Propose msg from {}: {:?}", sender, msg_id);
                }

                trace!(
                    "Handling proposal msg: {proposal:?} from {}: {:?}",
                    sender,
                    msg_id
                );

                // lets convert our message into a usable proposal for core
                let core_proposal = match proposal {
                    ProposalMsg::VoteNodeOffline(node_state) => {
                        CoreProposal::VoteNodeOffline(node_state)
                    }
                    ProposalMsg::SectionInfo(sap) => CoreProposal::SectionInfo(sap),
                    ProposalMsg::NewElders(sap) => CoreProposal::NewElders(sap),
                    ProposalMsg::JoinsAllowed(allowed) => CoreProposal::JoinsAllowed(allowed),
                };

                node.handle_proposal(msg_id, core_proposal, sig_share, sender)
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
                node.log_dkg_session(&sender.name());
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
                node.log_dkg_session(&sender.name());
                node.handle_dkg_votes(&session_id, pub_keys, votes, sender)
            }
            NodeMsg::DkgAE(session_id) => {
                let node = node.read().await;
                trace!("Handling msg: DkgAE s{} from {}", session_id.sh(), sender);
                node.handle_dkg_anti_entropy(session_id, sender)
            }
            NodeMsg::NodeCmd(NodeCmd::RecordStorageLevel { node_id, level, .. }) => {
                let mut node = node.write().await;
                let changed = node.set_storage_level(&node_id, level);
                if changed && level.value() == MIN_LEVEL_WHEN_FULL {
                    // ..then we accept a new node in place of the full node
                    node.joins_allowed = true;
                }
                Ok(vec![])
            }
            NodeMsg::NodeCmd(NodeCmd::ReceiveMetadata { metadata }) => {
                let mut node = node.write().await;
                info!("Processing received MetadataExchange packet: {:?}", msg_id);
                node.set_adult_levels(metadata);
                Ok(vec![])
            }
            NodeMsg::NodeEvent(NodeEvent::DataStored(address)) => {
                // data was stored. this should be sent over a response stream only.
                warn!("Unexpected DataStore ({address:?})node event received as direct msg. It should be sent as a response over a stream...");
                Ok(vec![])
            }
            NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                node_id,
                data,
                full,
            }) => {
                let mut node = node.write().await;
                info!(
                    "Processing CouldNotStoreData event with MsgId: {:?}",
                    msg_id
                );
                if node.is_not_elder() {
                    error!("Received unexpected message while Adult");
                    return Ok(vec![]);
                }

                if full {
                    let changed =
                        node.set_storage_level(&node_id, StorageLevel::from(StorageLevel::MAX)?);
                    if changed {
                        // ..then we accept a new node in place of the full node
                        node.joins_allowed = true;
                    }
                }

                let targets = node.target_data_holders(data.name());

                // TODO: handle responses where replication failed...
                let _results = node
                    .replicate_data_to_adults(
                        data,
                        msg_id,
                        targets,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await?;

                Ok(vec![])
            }
            NodeMsg::NodeCmd(NodeCmd::ReplicateOneData(data)) => {
                let node_read_lock = node.read().await;
                if node_read_lock.is_elder() {
                    error!("Received unexpected message while Elder");
                    return Ok(vec![]);
                }
                debug!(
                    "Attempting to store data locally as adult: {:?}",
                    data.address()
                );

                // ensure we drop the read lock as it's not needed
                drop(node_read_lock);

                // grab the write lock each time in the loop to not hold it over large data sets
                let mut node = node.write().await;

                debug!(
                    "Attempting to store data locally as adult:: write lock gotten: {:?}",
                    data.address()
                );
                // store data and respond w/ack on the response stream
                node.store_data_as_adult_and_respond(data, send_stream, sender, msg_id, traceroute)
                    .await
            }
            NodeMsg::NodeCmd(NodeCmd::ReplicateData(data_collection)) => {
                let node_read_lock = node.read().await;

                info!("ReplicateData MsgId: {:?}", msg_id);

                if node_read_lock.is_elder() {
                    error!("Received unexpected message while Elder");
                    return Ok(vec![]);
                }

                let mut cmds = vec![];

                let section_pk = PublicKey::Bls(node_read_lock.network_knowledge.section_key());
                let node_keypair = Keypair::Ed25519(node_read_lock.keypair.clone());
                // ensure we drop the read lock as it's not needed
                drop(node_read_lock);

                for data in data_collection {
                    // grab the write lock each time in the loop to not hold it over large data sets
                    let mut node = node.write().await;

                    // We are an adult here, so just store away!
                    // This may return a DatabaseFull error... but we should have reported storage increase
                    // well before this
                    match node
                        .data_storage
                        .store(&data, section_pk, node_keypair.clone())
                        .await
                    {
                        Ok(level_report) => {
                            info!("Storage level report: {:?}", level_report);
                            cmds.extend(node.record_storage_level_if_any(
                                level_report,
                                #[cfg(feature = "traceroute")]
                                traceroute.clone(),
                            )?);

                            #[cfg(feature = "traceroute")]
                            info!("End of message flow. Trace: {:?}", traceroute);
                        }
                        Err(StorageError::NotEnoughSpace) => {
                            // storage full
                            error!("Not enough space to store more data");
                            let node_id = PublicKey::from(node.keypair.public);
                            let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                                node_id,
                                data,
                                full: true,
                            });

                            cmds.push(node.send_msg_to_our_elders(msg))
                        }
                        Err(error) => {
                            // the rest seem to be non-problematic errors.. (?)
                            error!("Problem storing data, but it was ignored: {error}");
                        }
                    }
                }

                Ok(cmds)
            }
            NodeMsg::NodeCmd(NodeCmd::SendAnyMissingRelevantData(known_data_addresses)) => {
                let node = node.read().await;

                info!(
                    "{:?} MsgId: {:?}",
                    LogMarker::RequestForAnyMissingData,
                    msg_id
                );
                Ok(node
                    .get_missing_data_for_node(sender, known_data_addresses)
                    .await
                    .into_iter()
                    .collect())
            }
            NodeMsg::NodeQuery(NodeQuery::Data {
                query,
                auth,
                operation_id,
            }) => {
                let node = node.read().await;

                // A request from EndUser - via elders - for locally stored data
                debug!(
                    "Handle NodeQuery with msg_id {:?}, operation_id {}",
                    msg_id, operation_id
                );

                node.handle_data_query_at_adult(
                    operation_id,
                    &query,
                    auth,
                    sender,
                    msg_id,
                    send_stream,
                    #[cfg(feature = "traceroute")]
                    traceroute,
                )
                .await?;
                Ok(vec![])
            }
            // TODO: refactor msging to lose this type.
            NodeMsg::NodeQueryResponse { operation_id, .. } => {
                error!(
                    "This should no longer be seen aside from in a response stream! We have an issue here... This msg will not be handled.{:?}: op_id {}, sender: {sender} origin msg_id: {msg_id:?}",
                    LogMarker::ChunkQueryResponseReceviedFromAdult,
                    operation_id
                );

                //empty vec for now
                Ok(vec![])
            }
        }
    }

    fn record_storage_level_if_any(
        &self,
        level: Option<StorageLevel>,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        if let Some(level) = level {
            info!("Storage has now passed {} % used.", 10 * level.value());
            let node_id = PublicKey::from(self.keypair.public);
            let node_xorname = XorName::from(node_id);

            // we ask the section to record the new level reached
            let msg = NodeMsg::NodeCmd(NodeCmd::RecordStorageLevel {
                section: node_xorname,
                node_id,
                level,
            });

            let dst = Peers::Multiple(self.network_knowledge.elders());

            cmds.push(Cmd::send_traced_msg(
                OutgoingMsg::Node(msg),
                dst,
                #[cfg(feature = "traceroute")]
                traceroute,
            ));
        }

        Ok(cmds)
    }
}
