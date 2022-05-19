// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod agreement;
mod anti_entropy;
mod dkg;
mod handover;
mod join;
mod left;
mod membership;
mod proposals;
mod relocation;
mod resource_proof;
mod service_msgs;
mod update_section;

pub(crate) use proposals::handle_proposal;

use crate::dbs::Error as DbError;
use crate::node::{
    api::cmds::Cmd,
    core::{DkgSessionInfo, Node, Proposal as CoreProposal, DATA_QUERY_LIMIT},
    messages::WireMsgUtils,
    Error, Event, MessageReceived, Result, MIN_LEVEL_WHEN_FULL,
};
use sn_interface::messaging::{
    data::{ServiceMsg, StorageLevel},
    signature_aggregator::Error as AggregatorError,
    system::{
        JoinResponse, NodeCmd, NodeEvent, NodeMsgAuthorityUtils, NodeQuery,
        Proposal as ProposalMsg, SystemMsg,
    },
    AuthorityProof, DstLocation, MsgId, MsgType, NodeMsgAuthority, SectionAuth, WireMsg,
};
use sn_interface::network_knowledge::NetworkKnowledge;
use sn_interface::types::{log_markers::LogMarker, Peer, PublicKey};

use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use itertools::Itertools;
use sn_dysfunction::IssueType;
use std::collections::BTreeSet;
use tokio::time::Duration;
use xor_name::XorName;

const REPLICATION_BATCH_SIZE: usize = 50;
const REPLICATION_MSG_THROTTLE_DURATION: Duration = Duration::from_secs(5);

// Message handling
impl Node {
    #[instrument(skip(self, original_bytes))]
    pub(crate) async fn handle_msg(
        &self,
        sender: Peer,
        wire_msg: WireMsg,
        original_bytes: Option<Bytes>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        // Deserialize the payload of the incoming message
        let msg_id = wire_msg.msg_id();
        // payload needed for aggregation
        let payload = wire_msg.payload.clone();

        let message_type = match wire_msg.into_msg() {
            Ok(message_type) => message_type,
            Err(error) => {
                error!(
                    "Failed to deserialize message payload ({:?}): {:?}",
                    msg_id, error
                );
                return Ok(cmds);
            }
        };

        match message_type {
            MsgType::System {
                msg_id,
                msg_authority,
                dst_location,
                msg,
            } => {
                // Let's now verify the section key in the msg authority is trusted
                // based on our current knowledge of the network and sections chains.
                let mut known_keys: Vec<BlsPublicKey> = self
                    .network_knowledge
                    .section_chain()
                    .await
                    .keys()
                    .copied()
                    .collect();
                known_keys.extend(self.network_knowledge.prefix_map().section_keys());
                known_keys.push(*self.network_knowledge.genesis_key());

                if !NetworkKnowledge::verify_node_msg_can_be_trusted(
                    msg_authority.clone(),
                    msg.clone(),
                    &known_keys,
                ) {
                    warn!(
                        "Untrusted message ({:?}) dropped from {:?}: {:?} ",
                        msg_id, sender, msg
                    );
                    return Ok(cmds);
                }

                // Let's check for entropy before we proceed further
                // Adult nodes don't need to carry out entropy checking,
                // however the message shall always be handled.
                if self.is_elder().await {
                    // For the case of receiving a join request not matching our prefix,
                    // we just let the join request handler to deal with it later on.
                    // We also skip AE check on Anti-Entropy messages
                    //
                    // TODO: consider changing the join and "join as relocated" flows to
                    // make use of AntiEntropy retry/redirect responses.
                    match msg {
                        SystemMsg::AntiEntropyRetry { .. }
                        | SystemMsg::AntiEntropyUpdate { .. }
                        | SystemMsg::AntiEntropyRedirect { .. }
                        | SystemMsg::JoinRequest(_)
                        | SystemMsg::JoinAsRelocatedRequest(_) => {
                            trace!(
                                "Entropy check skipped for {:?}, handling message directly",
                                msg_id
                            );
                        }
                        _ => match dst_location.section_pk() {
                            None => {}
                            Some(dst_section_pk) => {
                                let msg_bytes = original_bytes.unwrap_or(wire_msg.serialize()?);

                                if let Some(ae_cmd) = self
                                    .check_for_entropy(
                                        // a cheap clone w/ Bytes
                                        msg_bytes,
                                        &msg_authority.src_location(),
                                        &dst_section_pk,
                                        dst_location.name(),
                                        &sender,
                                    )
                                    .await?
                                {
                                    // we want to log issues with an elder who is out of sync here...
                                    let knowledge = self.network_knowledge.elders().await;
                                    let mut known_elders = knowledge.iter().map(|peer| peer.name());

                                    if known_elders.contains(&sender.name()) {
                                        // we track a dysfunction against our elder here
                                        self.dysfunction_tracking
                                            .track_issue(sender.name(), IssueType::Knowledge)
                                            .await
                                            .map_err(Error::from)?;
                                    }

                                    // short circuit and send those AE responses
                                    cmds.push(ae_cmd);
                                    return Ok(cmds);
                                }

                                trace!("Entropy check passed. Handling verified msg {:?}", msg_id);
                            }
                        },
                    }
                }

                let handling_msg_cmds = self
                    .handle_system_msg(
                        sender,
                        msg_id,
                        msg_authority,
                        dst_location,
                        msg,
                        payload,
                        known_keys,
                    )
                    .await?;

                cmds.extend(handling_msg_cmds);

                Ok(cmds)
            }
            MsgType::Service {
                msg_id,
                msg,
                dst_location,
                auth,
            } => {
                let dst_name = match msg.dst_address() {
                    Some(name) => name,
                    None => {
                        error!(
                            "Service msg has been dropped since {:?} is not a valid msg to send from a client {}.",
                            msg, sender.addr()
                        );
                        return Ok(vec![]);
                    }
                };

                let src_location = wire_msg.msg_kind().src();

                if self.is_not_elder().await {
                    trace!("Redirecting from adult to section elders");
                    cmds.push(
                        self.ae_redirect_to_our_elders(sender, &src_location, &wire_msg)
                            .await?,
                    );
                    return Ok(cmds);
                }

                // First we check if it's query and we have too many on the go at the moment...
                if let ServiceMsg::Query(_) = msg {
                    // we have a query, check if we have too many on the go....
                    let pending_query_length = self.pending_data_queries.len().await;

                    if pending_query_length > DATA_QUERY_LIMIT {
                        // TODO: check if query is pending for this already.. add to that if that makes sense.
                        warn!("Pending queries length exceeded, dropping query {msg:?}");
                        return Ok(vec![]);
                    }
                }

                // Then we perform AE checks
                let received_section_pk = match dst_location.section_pk() {
                    Some(section_pk) => section_pk,
                    None => {
                        warn!("Dropping service message as there is no valid dst section_pk.");
                        return Ok(cmds);
                    }
                };

                let msg_bytes = original_bytes.unwrap_or(wire_msg.serialize()?);
                if let Some(cmd) = self
                    .check_for_entropy(
                        // a cheap clone w/ Bytes
                        msg_bytes,
                        &src_location,
                        &received_section_pk,
                        dst_name,
                        &sender,
                    )
                    .await?
                {
                    // short circuit and send those AE responses
                    cmds.push(cmd);
                    return Ok(cmds);
                }

                cmds.extend(
                    self.handle_service_msg(msg_id, msg, dst_location, auth, sender)
                        .await?,
                );

                Ok(cmds)
            }
        }
    }

    // Handler for all system messages
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_system_msg(
        &self,
        sender: Peer,
        msg_id: MsgId,
        mut msg_authority: NodeMsgAuthority,
        dst_location: DstLocation,
        msg: SystemMsg,
        payload: Bytes,
        known_keys: Vec<BlsPublicKey>,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}", LogMarker::SystemMsgToBeHandled);

        // We assume to be aggregated if it contains a BLS Share sig as authority.
        match self
            .aggregate_msg_and_stop(&mut msg_authority, payload)
            .await
        {
            Ok(false) => {
                self.handle_valid_msg(msg_id, msg_authority, dst_location, msg, sender, known_keys)
                    .await
            }
            Err(Error::InvalidSignatureShare) => {
                warn!(
                    "Invalid signature on received system message, dropping the message: {:?}",
                    msg_id
                );
                Ok(vec![])
            }
            Ok(true) => Ok(vec![]),
            Err(err) => {
                trace!("handle_system_msg got error {:?}", err);
                Ok(vec![])
            }
        }
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_valid_msg(
        &self,
        msg_id: MsgId,
        msg_authority: NodeMsgAuthority,
        dst_location: DstLocation,
        node_msg: SystemMsg,
        sender: Peer,
        known_keys: Vec<BlsPublicKey>,
    ) -> Result<Vec<Cmd>> {
        let src_name = msg_authority.name();
        match node_msg {
            SystemMsg::AntiEntropyUpdate {
                section_auth,
                section_signed,
                proof_chain,
                members,
            } => {
                trace!("Handling msg: AE-Update from {}: {:?}", sender, msg_id,);
                self.handle_anti_entropy_update_msg(
                    section_auth.into_state(),
                    section_signed,
                    proof_chain,
                    members,
                )
                .await
            }
            SystemMsg::Relocate(node_state) => {
                trace!("Handling msg: Relocate from {}: {:?}", sender, msg_id);
                Ok(self
                    .handle_relocate(node_state)
                    .await?
                    .into_iter()
                    .collect())
            }
            SystemMsg::StartConnectivityTest(name) => {
                trace!(
                    "Handling msg: StartConnectivityTest from {}: {:?}",
                    sender,
                    msg_id
                );
                if self.is_not_elder().await {
                    return Ok(vec![]);
                }

                Ok(vec![Cmd::TestConnectivity(name)])
            }
            SystemMsg::JoinAsRelocatedResponse(join_response) => {
                trace!("Handling msg: JoinAsRelocatedResponse from {}", sender);
                if let Some(ref mut joining_as_relocated) = *self.relocate_state.write().await {
                    if let Some(cmd) = joining_as_relocated
                        .handle_join_response(*join_response, sender.addr())
                        .await?
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
            SystemMsg::NodeMsgError {
                error,
                correlation_id,
            } => {
                trace!(
                    "From {:?}({:?}), received error {:?} correlated to {:?}",
                    msg_authority.src_location(),
                    msg_id,
                    error,
                    correlation_id
                );
                Ok(vec![])
            }
            SystemMsg::AntiEntropyRetry {
                section_auth,
                section_signed,
                proof_chain,
                bounced_msg,
            } => {
                trace!("Handling msg: AE-Retry from {}: {:?}", sender, msg_id,);
                self.handle_anti_entropy_retry_msg(
                    section_auth.into_state(),
                    section_signed,
                    proof_chain,
                    bounced_msg,
                    sender,
                )
                .await
            }
            SystemMsg::AntiEntropyRedirect {
                section_auth,
                section_signed,
                section_chain,
                bounced_msg,
            } => {
                trace!("Handling msg: AE-Redirect from {}: {:?}", sender, msg_id);
                self.handle_anti_entropy_redirect_msg(
                    section_auth.into_state(),
                    section_signed,
                    section_chain,
                    bounced_msg,
                    sender,
                )
                .await
            }
            SystemMsg::AntiEntropyProbe(_dst) => {
                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                Ok(vec![])
            }
            #[cfg(feature = "back-pressure")]
            SystemMsg::BackPressure(msgs_per_s) => {
                trace!(
                    "Handling msg: BackPressure with requested {} msgs/s, from {}: {:?}",
                    msgs_per_s,
                    sender,
                    msg_id
                );
                // TODO: Factor in med/long term backpressure into general node liveness calculations
                self.comm.regulate(&sender, msgs_per_s).await;
                Ok(vec![])
            }
            // The AcceptedOnlineShare for relocation will be received here.
            SystemMsg::JoinResponse(join_response) => {
                match *join_response {
                    JoinResponse::Approval {
                        section_auth,
                        section_chain,
                        ..
                    } => {
                        info!(
                            "Relocation: Aggregating received ApprovalShare from {:?}",
                            sender
                        );
                        info!("Relocation: Successfully aggregated ApprovalShares for joining the network");

                        if let Some(ref mut joining_as_relocated) =
                            *self.relocate_state.write().await
                        {
                            let new_node = joining_as_relocated.node.clone();
                            let new_name = new_node.name();
                            let previous_name = self.info.read().await.name();
                            let new_keypair = new_node.keypair.clone();

                            info!(
                                "Relocation: switching from {:?} to {:?}",
                                previous_name, new_name
                            );

                            let genesis_key = *self.network_knowledge.genesis_key();
                            let prefix_map = self.network_knowledge.prefix_map().clone();

                            let recipients = section_auth.value.elders.clone();

                            let new_network_knowledge = NetworkKnowledge::new(
                                genesis_key,
                                section_chain,
                                section_auth.into_authed_state(),
                                Some(prefix_map),
                            )?;

                            // TODO: confirm whether carry out the switch immediately here
                            //       or still using the cmd pattern.
                            //       As the sending of the JoinRequest as notification
                            //       may require the `node` to be switched to new already.

                            self.relocate(new_node, new_network_knowledge).await?;

                            trace!(
                                "Relocation: Sending aggregated JoinRequest to {:?}",
                                recipients
                            );

                            self.send_event(Event::Relocated {
                                previous_name,
                                new_keypair,
                            })
                            .await;

                            trace!("{}", LogMarker::RelocateEnd);
                        } else {
                            warn!("Relocation:  self.relocate_state is not in Progress");
                            return Ok(vec![]);
                        }

                        Ok(vec![])
                    }
                    _ => {
                        debug!("Relocation: Ignoring unexpected join response message: {join_response:?}");
                        Ok(vec![])
                    }
                }
            }
            SystemMsg::DkgFailureAgreement(sig_set) => {
                trace!("Handling msg: Dkg-FailureAgreement from {}", sender);
                self.handle_dkg_failure_agreement(&src_name, &sig_set).await
            }
            SystemMsg::HandoverVote(vote) => self.handle_handover_msg(vote).await,
            SystemMsg::JoinRequest(join_request) => {
                trace!("Handling msg: JoinRequest from {}", sender);
                self.handle_join_request(sender, *join_request).await
            }
            SystemMsg::JoinAsRelocatedRequest(join_request) => {
                trace!("Handling msg: JoinAsRelocatedRequest from {}", sender);
                if self.is_not_elder().await
                    && join_request.section_key == self.network_knowledge.section_key().await
                {
                    return Ok(vec![]);
                }

                self.handle_join_as_relocated_request(sender, *join_request, known_keys)
                    .await
            }
            SystemMsg::MembershipVotes(votes) => {
                let mut cmds = vec![];
                cmds.extend(self.handle_membership_votes(sender, votes).await?);

                Ok(cmds)
            }
            SystemMsg::MembershipAE(gen) => self.handle_membership_anti_entropy(sender, gen).await,
            SystemMsg::Propose {
                proposal,
                sig_share,
            } => {
                if self.is_not_elder().await {
                    trace!("Adult handling a Propose msg from {}: {:?}", sender, msg_id);
                }

                trace!("Handling msg: Propose from {}: {:?}", sender, msg_id);

                // lets convert our message into a usable proposal for core
                let core_proposal = match proposal {
                    ProposalMsg::Offline(node_state) => {
                        CoreProposal::Offline(node_state.into_state())
                    }
                    ProposalMsg::SectionInfo { sap, generation } => CoreProposal::SectionInfo {
                        sap: sap.into_state(),
                        generation,
                    },
                    ProposalMsg::NewElders(sap) => CoreProposal::NewElders(sap.into_authed_state()),
                    ProposalMsg::JoinsAllowed(allowed) => CoreProposal::JoinsAllowed(allowed),
                };

                handle_proposal(
                    msg_id,
                    core_proposal,
                    sig_share,
                    sender,
                    &self.network_knowledge,
                    &self.proposal_aggregator,
                )
                .await
            }
            SystemMsg::DkgStart(session_id) => {
                trace!("Handling msg: Dkg-Start {:?} from {}", session_id, sender);
                let our_name = self.info.read().await.name();
                if !session_id.contains_elder(our_name) {
                    return Ok(vec![]);
                }
                if let NodeMsgAuthority::Section(authority) = msg_authority {
                    let _existing = self.dkg_sessions.write().await.insert(
                        session_id.hash(),
                        DkgSessionInfo {
                            session_id: session_id.clone(),
                            authority,
                        },
                    );
                }
                self.handle_dkg_start(session_id).await
            }
            SystemMsg::DkgMessage {
                session_id,
                message,
            } => {
                trace!(
                    "Handling msg: Dkg-Msg ({:?} - {:?}) from {}",
                    session_id,
                    message,
                    sender
                );
                self.handle_dkg_msg(session_id, message, sender).await
            }
            SystemMsg::DkgFailureObservation {
                session_id,
                sig,
                failed_participants,
            } => {
                trace!("Handling msg: Dkg-FailureObservation from {}", sender);
                self.handle_dkg_failure_observation(session_id, &failed_participants, sig)
            }
            SystemMsg::DkgNotReady {
                message,
                session_id,
            } => {
                self.handle_dkg_not_ready(
                    sender,
                    message,
                    session_id,
                    self.network_knowledge.section_key().await,
                )
                .await
            }
            SystemMsg::DkgRetry {
                message_history,
                message,
                session_id,
            } => {
                self.handle_dkg_retry(&session_id, message_history, message, sender)
                    .await
            }
            SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel { node_id, level, .. }) => {
                let changed = self.set_storage_level(&node_id, level).await;
                if changed && level.value() == MIN_LEVEL_WHEN_FULL {
                    // ..then we accept a new node in place of the full node
                    *self.joins_allowed.write().await = true;
                }
                Ok(vec![])
            }
            SystemMsg::NodeCmd(NodeCmd::ReceiveMetadata { metadata }) => {
                info!("Processing received MetadataExchange packet: {:?}", msg_id);
                self.set_adult_levels(metadata).await;
                Ok(vec![])
            }
            SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                node_id,
                data,
                full,
            }) => {
                info!(
                    "Processing CouldNotStoreData event with MsgId: {:?}",
                    msg_id
                );

                if self.is_elder().await {
                    if full {
                        let changed = self
                            .set_storage_level(&node_id, StorageLevel::from(StorageLevel::MAX)?)
                            .await;
                        if changed {
                            // ..then we accept a new node in place of the full node
                            *self.joins_allowed.write().await = true;
                        }
                    }
                    self.replicate_data(data).await
                } else {
                    error!("Received unexpected message while Adult");
                    Ok(vec![])
                }
            }
            SystemMsg::NodeCmd(NodeCmd::ReplicateData(data_collection)) => {
                info!("ReplicateData MsgId: {:?}", msg_id);
                return if self.is_elder().await {
                    error!("Received unexpected message while Elder");
                    Ok(vec![])
                } else {
                    let mut cmds = vec![];

                    for data in data_collection {
                        // We are an adult here, so just store away!
                        // This may return a DatabaseFull error... but we should have reported storage increase
                        // well before this
                        match self.data_storage.store(&data).await {
                            Ok(level_report) => {
                                info!("Storage level report: {:?}", level_report);
                                cmds.extend(self.record_storage_level_if_any(level_report).await);
                            }
                            Err(error) => {
                                match error {
                                    DbError::NotEnoughSpace => {
                                        // db full
                                        error!("Not enough space to store more data");

                                        let node_id =
                                            PublicKey::from(self.info.read().await.keypair.public);
                                        let msg =
                                            SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                                                node_id,
                                                data,
                                                full: true,
                                            });

                                        cmds.push(self.send_msg_to_our_elders(msg).await?)
                                    }
                                    _ => {
                                        error!("Problem storing data, but it was ignored: {error}");
                                    } // the rest seem to be non-problematic errors.. (?)
                                }
                            }
                        }
                    }

                    Ok(cmds)
                };
            }
            SystemMsg::NodeCmd(NodeCmd::EnsureReplicationOfDataAddress(data_addresses)) => {
                info!("EnsureReplicationOfDataAddress MsgId: {:?}", msg_id);

                return if self.is_elder().await {
                    error!(
                        "Received unexpected EnsureReplicationOfDataAddress message while Elder"
                    );
                    Ok(vec![])
                } else {
                    // Collection of data addresses that we do not have
                    let mut data_not_present = vec![];

                    for data_address in data_addresses {
                        let mut holders = BTreeSet::default();
                        // first, check if we are actually responsible for this data
                        if let Some(membership) = &*self.membership.read().await {
                            let mut members = BTreeSet::default();
                            for name in membership.current_section_members().keys() {
                                let _prev = members.insert(*name);
                            }
                            holders = self.compute_holders(&data_address, &members);
                        }

                        // we're not a holder of this piece of data, so ignore it.
                        if !holders.contains(&self.info.read().await.name()) {
                            debug!("We're not a holder");
                            continue;
                        }

                        // Check if we already have the data
                        match self.data_storage.get_from_local_store(&data_address).await {
                            Err(crate::dbs::Error::NoSuchData(_))
                            | Err(crate::dbs::Error::ChunkNotFound(_)) => {
                                trace!("to-be-replicated data is not present, storing that data now: {:?}", data_address);

                                // We do not have the data which we are supposed to have since the new reorg
                                data_not_present.push(data_address);
                            }
                            Ok(_) => {
                                info!("We already have the data that was asked to be replicated");
                            }
                            Err(e) => {
                                error!("Error retrieving current data, in order to calculate FetchReplicateData cmd for replication: {e}");
                                return Ok(vec![]);
                            }
                        }
                    }

                    if data_not_present.is_empty() {
                        debug!("No data needed to be replicated, we have it all.");
                        return Ok(vec![]);
                    }

                    trace!("We are missing {:?} pieces of data", data_not_present.len());

                    let section_pk = self.section_key_by_name(&sender.name()).await;
                    let src_section_pk = self.network_knowledge().section_key().await;
                    let our_info = &*self.info.read().await;
                    let dst = sn_interface::messaging::DstLocation::Node {
                        name: sender.name(),
                        section_pk,
                    };
                    let mut wire_msgs = vec![];

                    // Chunks the collection into REPLICATION_BATCH_SIZE addresses in a batch. This avoids memory
                    // explosion in the network when the amount of data to be replicated is high
                    for chunked_data_address in
                        &data_not_present.into_iter().chunks(REPLICATION_BATCH_SIZE)
                    {
                        let system_msg = SystemMsg::NodeCmd(NodeCmd::FetchReplicateData(
                            chunked_data_address.collect_vec(),
                        ));

                        debug!(
                            "{:?} from : {:?} ",
                            LogMarker::FetchingMissingReplicateData,
                            dst
                        );
                        wire_msgs.push(WireMsg::single_src(
                            our_info,
                            dst,
                            system_msg,
                            src_section_pk,
                        )?);
                    }

                    let cmd = Cmd::ThrottledSendBatchMsgs {
                        throttle_duration: REPLICATION_MSG_THROTTLE_DURATION,
                        recipients: vec![sender],
                        wire_msgs,
                    };

                    Ok(vec![cmd])
                };
            }
            SystemMsg::NodeCmd(NodeCmd::FetchReplicateData(data_addresses)) => {
                let mut cmds = vec![];
                info!("FetchReplicateData MsgId: {:?}", msg_id);

                // Chunk the full list into REPLICATION_BATCH_SIZE addresses a batch
                let mut addresses = vec![];
                for chunked_data_address in
                    &data_addresses.into_iter().chunks(REPLICATION_BATCH_SIZE)
                {
                    let data_address_collection = chunked_data_address.collect_vec();
                    addresses.push(data_address_collection);
                }

                debug!(
                    "Data requested by a node for replication. {:?} batches formed",
                    addresses.len()
                );
                // Process each batch
                for chunked_addresses in addresses {
                    let mut data_collection = vec![];
                    for data_address in chunked_addresses {
                        match self.data_storage.get_for_replication(data_address).await {
                            Ok(data) => {
                                info!("Providing {data_address:?} for replication");

                                data_collection.push(data);
                            }
                            Err(e) => {
                                // elders are asked in case they have the data from being an adult
                                // (or perhaps from caching in the future)
                                if !self.is_elder().await {
                                    warn!("Error providing data for replication: {e}");
                                }
                                return Ok(vec![]);
                            }
                        }
                    }

                    cmds.push(Cmd::SignOutgoingSystemMsg {
                        msg: SystemMsg::NodeCmd(NodeCmd::ReplicateData(data_collection)),
                        dst: sn_interface::messaging::DstLocation::Node {
                            name: sender.name(),
                            section_pk: self.section_key_by_name(&sender.name()).await,
                        },
                    });
                }

                // Provide the requested data
                Ok(cmds)
            }
            SystemMsg::NodeCmd(node_cmd) => {
                self.send_event(Event::MessageReceived {
                    msg_id,
                    src: msg_authority.src_location(),
                    dst: dst_location,
                    msg: Box::new(MessageReceived::NodeCmd(node_cmd)),
                })
                .await;

                Ok(vec![])
            }
            SystemMsg::NodeQuery(node_query) => {
                match node_query {
                    // A request from EndUser - via elders - for locally stored data
                    NodeQuery::Data {
                        query,
                        auth,
                        origin,
                        correlation_id,
                    } => {
                        // There is no point in verifying a sig from a sender A or B here.
                        // Send back response to the sending elder
                        let sender_xorname = msg_authority.get_auth_xorname();
                        self.handle_data_query_at_adult(
                            correlation_id,
                            &query,
                            auth,
                            origin,
                            sender_xorname,
                        )
                        .await
                    }
                    _ => {
                        self.send_event(Event::MessageReceived {
                            msg_id,
                            src: msg_authority.src_location(),
                            dst: dst_location,
                            msg: Box::new(MessageReceived::NodeQuery(node_query)),
                        })
                        .await;
                        Ok(vec![])
                    }
                }
            }
            SystemMsg::NodeQueryResponse {
                response,
                correlation_id,
                user,
            } => {
                debug!(
                    "{:?}: op_id {:?}, correlation_id: {correlation_id:?}, sender: {sender}",
                    LogMarker::ChunkQueryResponseReceviedFromAdult,
                    response.operation_id()?
                );
                let sending_nodes_pk = match msg_authority {
                    NodeMsgAuthority::Node(auth) => PublicKey::from(auth.into_inner().node_ed_pk),
                    _ => return Err(Error::InvalidQueryResponseAuthority),
                };

                self.handle_data_query_response_at_elder(
                    correlation_id,
                    response,
                    user,
                    sending_nodes_pk,
                )
                .await
            }
            SystemMsg::DkgSessionUnknown {
                session_id,
                message,
            } => {
                if let Some(session_info) = self
                    .dkg_sessions
                    .read()
                    .await
                    .get(&session_id.hash())
                    .cloned()
                {
                    let message_cache = self.dkg_voter.get_cached_msgs(&session_info.session_id);
                    trace!(
                        "Sending DkgSessionInfo {{ {:?}, ... }} to {}",
                        &session_info.session_id,
                        &sender
                    );

                    let node_msg = SystemMsg::DkgSessionInfo {
                        session_id,
                        section_auth: session_info.authority,
                        message_cache,
                        message,
                    };
                    let section_pk = self.network_knowledge.section_key().await;
                    let wire_msg = WireMsg::single_src(
                        &self.info.read().await.clone(),
                        DstLocation::Node {
                            name: sender.name(),
                            section_pk,
                        },
                        node_msg,
                        section_pk,
                    )?;

                    Ok(vec![Cmd::SendMsg {
                        recipients: vec![sender],
                        wire_msg,
                    }])
                } else {
                    warn!("Unknown DkgSessionInfo: {:?} requested", &session_id);
                    Ok(vec![])
                }
            }
            SystemMsg::DkgSessionInfo {
                session_id,
                message_cache,
                section_auth,
                message,
            } => {
                let mut cmds = vec![];
                // Reconstruct the original DKG start message and verify the section signature
                let payload =
                    WireMsg::serialize_msg_payload(&SystemMsg::DkgStart(session_id.clone()))?;
                let auth = section_auth.clone().into_inner();
                if self.network_knowledge.section_key().await == auth.sig.public_key {
                    if let Err(err) = AuthorityProof::verify(auth, payload) {
                        error!("Error verifying signature for DkgSessionInfo: {:?}", err);
                        return Ok(cmds);
                    } else {
                        trace!("DkgSessionInfo signature verified");
                    }
                } else {
                    warn!(
                        "Cannot verify DkgSessionInfo: {:?}. Unknown key: {:?}!",
                        &session_id, auth.sig.public_key
                    );
                    let chain = self.network_knowledge().section_chain().await;
                    warn!("Chain: {:?}", chain);
                    return Ok(cmds);
                };
                let _existing = self.dkg_sessions.write().await.insert(
                    session_id.hash(),
                    DkgSessionInfo {
                        session_id: session_id.clone(),
                        authority: section_auth,
                    },
                );
                trace!("DkgSessionInfo handling {:?}", session_id);
                cmds.extend(self.handle_dkg_start(session_id.clone()).await?);
                cmds.extend(
                    self.handle_dkg_retry(&session_id, message_cache, message, sender)
                        .await?,
                );
                Ok(cmds)
            }
        }
    }

    async fn record_storage_level_if_any(&self, level: Option<StorageLevel>) -> Vec<Cmd> {
        let mut cmds = vec![];
        if let Some(level) = level {
            info!("Storage has now passed {} % used.", 10 * level.value());
            let node_id = PublicKey::from(self.info.read().await.keypair.public);
            let node_xorname = XorName::from(node_id);

            // we ask the section to record the new level reached
            let msg = SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel {
                section: node_xorname,
                node_id,
                level,
            });

            let dst = DstLocation::Section {
                name: node_xorname,
                section_pk: self.network_knowledge.section_key().await,
            };

            cmds.push(Cmd::SignOutgoingSystemMsg { msg, dst });
        }
        cmds
    }

    // Convert the provided NodeMsgAuthority to be a `Section` message
    // authority on successful accumulation. Also return 'true' if
    // current message shall not be processed any further.
    async fn aggregate_msg_and_stop(
        &self,
        msg_authority: &mut NodeMsgAuthority,
        payload: Bytes,
    ) -> Result<bool> {
        let bls_share_auth = if let NodeMsgAuthority::BlsShare(bls_share_auth) = msg_authority {
            bls_share_auth
        } else {
            return Ok(false);
        };

        match SectionAuth::try_authorize(
            self.message_aggregator.clone(),
            bls_share_auth.clone().into_inner(),
            &payload,
        )
        .await
        {
            Ok(section_auth) => {
                info!("Successfully aggregated message");
                *msg_authority = NodeMsgAuthority::Section(section_auth);
                Ok(false)
            }
            Err(AggregatorError::NotEnoughShares) => {
                info!("Not enough shares to aggregate received message");
                Ok(true)
            }
            Err(err) => {
                error!("Error accumulating message at dst: {:?}", err);
                Err(Error::InvalidSignatureShare)
            }
        }
    }
}
