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
mod join;
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
    messages::{NodeMsgAuthorityUtils, WireMsgUtils},
    Error, Event, MessageReceived, Result, MIN_LEVEL_WHEN_FULL,
};
use sn_interface::messaging::{
    data::{ServiceMsg, StorageLevel},
    signature_aggregator::Error as AggregatorError,
    system::{
        JoinRequest, JoinResponse, NodeCmd, NodeEvent, NodeQuery, Proposal as ProposalMsg,
        SectionAuth as SystemSectionAuth, SystemMsg,
    },
    AuthorityProof, DstLocation, MsgId, MsgType, NodeMsgAuthority, SectionAuth, WireMsg,
};
use sn_interface::network_knowledge::NetworkKnowledge;
use sn_interface::types::{log_markers::LogMarker, Peer, PublicKey};

use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use std::collections::BTreeSet;
use xor_name::XorName;

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

        // Apply backpressure if needed.
        if let Some(load_report) = self.comm.check_strain(&sender.addr()).await {
            let msg_src = wire_msg.msg_kind().src();
            if !msg_src.is_end_user() {
                cmds.push(Cmd::SignOutgoingSystemMsg {
                    msg: SystemMsg::BackPressure(load_report),
                    dst: msg_src.to_dst(),
                })
            }
        }

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

                if !msg_authority.verify_src_section_key_is_known(&known_keys) {
                    warn!(
                        "Untrusted message ({:?}) dropped from {:?}: {:?} ",
                        msg_id, sender, msg
                    );
                    return Ok(cmds);
                }

                trace!(
                    "Trusted msg authority in message ({:?}) from {:?}: {:?}",
                    msg_id,
                    sender,
                    msg
                );

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
                                    // short circuit and send those AE responses
                                    cmds.push(ae_cmd);
                                    return Ok(cmds);
                                }

                                trace!("Entropy check passed. Handling verified msg {:?}", msg_id);
                            }
                        },
                    }
                }

                cmds.push(Cmd::HandleSystemMsg {
                    sender,
                    msg_id,
                    msg_authority,
                    dst_location,
                    msg,
                    payload,
                    known_keys,
                });

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
        trace!("Handling non blocking message");
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
            SystemMsg::BackPressure(load_report) => {
                trace!("Handling msg: BackPressure from {}: {:?}", sender, msg_id);
                // TODO: Factor in med/long term backpressure into general node liveness calculations
                self.comm.regulate(sender.addr(), load_report).await;
                Ok(vec![])
            }
            // The AcceptedOnlineShare for relocation will be received here.
            SystemMsg::JoinResponse(join_response) => {
                match *join_response {
                    JoinResponse::ApprovalShare {
                        node_state,
                        sig_share,
                        section_chain,
                        members,
                        ..
                    } => {
                        let serialized_details = bincode::serialize(&node_state)?;

                        info!(
                            "Relocation: Aggregating received ApprovalShare from {:?}",
                            sender
                        );
                        match self
                            .proposal_aggregator
                            .add(&serialized_details, sig_share.clone())
                            .await
                        {
                            Ok(sig) => {
                                info!("Relocation: Successfully aggregated ApprovalShares for joining the network");
                                let mut cmds = vec![];

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

                                    let (recipients, signed_sap) = if let Ok(sap) =
                                        self.network_knowledge.section_by_name(&new_name)
                                    {
                                        if let Some(signed_sap) =
                                            prefix_map.get_signed(&sap.prefix())
                                        {
                                            (sap.elders().cloned().collect(), signed_sap)
                                        } else {
                                            warn!(
                                                "Relocation: cannot find signed_sap for {:?}",
                                                sap.prefix()
                                            );
                                            return Ok(vec![]);
                                        }
                                    } else {
                                        warn!("Relocation: cannot find recipients to send aggregated JoinApproval");
                                        return Ok(vec![]);
                                    };

                                    let new_network_knowledge = NetworkKnowledge::new(
                                        genesis_key,
                                        section_chain,
                                        signed_sap,
                                        Some(prefix_map),
                                    )?;
                                    let _ = new_network_knowledge.merge_members(
                                        members
                                            .into_iter()
                                            .map(|member| member.into_authed_state())
                                            .collect(),
                                    );

                                    // TODO: confirm whether carry out the switch immediately here
                                    //       or still using the cmd pattern.
                                    //       As the sending of the JoinRequest as notification
                                    //       may require the `node` to be switched to new already.

                                    self.relocate(new_node, new_network_knowledge).await?;

                                    let section_key = sig_share.public_key_set.public_key();
                                    let auth = SystemSectionAuth {
                                        value: node_state,
                                        sig,
                                    };
                                    let join_req = JoinRequest {
                                        section_key,
                                        resource_proof_response: None,
                                        aggregated: Some(auth),
                                    };

                                    trace!(
                                        "Relocation: Sending aggregated JoinRequest to {:?}",
                                        recipients
                                    );
                                    // Resend the JoinRequest now that
                                    // we have collected enough ApprovalShares from the Elders
                                    let node_msg = SystemMsg::JoinRequest(Box::new(join_req));
                                    let wire_msg = WireMsg::single_src(
                                        &self.info.read().await.clone(),
                                        DstLocation::Section {
                                            name: new_name,
                                            section_pk: section_key,
                                        },
                                        node_msg,
                                        section_key,
                                    )?;
                                    cmds.push(Cmd::SendMsg {
                                        recipients,
                                        wire_msg,
                                    });

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

                                Ok(cmds)
                            }
                            Err(AggregatorError::NotEnoughShares) => Ok(vec![]),
                            error => {
                                warn!(
                                    "Relocation: Error received as part of signature aggregation during join: {:?}",
                                    error
                                );
                                Ok(vec![])
                            }
                        }
                    }
                    _ => {
                        debug!(
                            "Relocation: Ignoring unexpected join response message: {:?}",
                            join_response
                        );
                        Ok(vec![])
                    }
                }
            }
            SystemMsg::DkgFailureAgreement(sig_set) => {
                trace!("Handling msg: Dkg-FailureAgreement from {}", sender);
                self.handle_dkg_failure_agreement(&src_name, &sig_set).await
            }
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
                    ProposalMsg::SectionInfo(sap) => CoreProposal::SectionInfo(sap.into_state()),
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
            SystemMsg::DkgStart {
                session_id,
                prefix,
                elders,
            } => {
                trace!("Handling msg: Dkg-Start {:?} from {}", session_id, sender);
                if !elders.contains_key(&self.info.read().await.name()) {
                    return Ok(vec![]);
                }
                if let NodeMsgAuthority::Section(authority) = msg_authority {
                    let _existing = self.dkg_sessions.write().await.insert(
                        session_id,
                        DkgSessionInfo {
                            prefix,
                            elders: elders.clone(),
                            authority,
                        },
                    );
                }
                self.handle_dkg_start(session_id, prefix, elders).await
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
                self.handle_dkg_retry(session_id, message_history, message, sender)
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
                return if self.is_elder().await {
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
                };
            }
            SystemMsg::NodeEvent(NodeEvent::DeviantsDetected(deviants)) => {
                info!(
                    "Received probable deviants nodes {deviants:?} Starting preemptive data replication"
                );
                debug!("{}", LogMarker::DeviantsDetected);

                return self.republish_data_for_deviant_nodes(deviants).await;
            }
            SystemMsg::NodeCmd(NodeCmd::ReplicateData(data)) => {
                info!("ReplicateData MsgId: {:?}", msg_id);
                return if self.is_elder().await {
                    error!("Received unexpected message while Elder");
                    Ok(vec![])
                } else {
                    // We are an adult here, so just store away!
                    // This may return a DatabaseFull error... but we should have reported storage increase
                    // well before this
                    match self.data_storage.store(&data).await {
                        Ok(level_report) => {
                            info!("Storage level report: {:?}", level_report);
                            return Ok(self.record_storage_level_if_any(level_report).await);
                        }
                        Err(error) => {
                            let full = match error {
                                //DbError::Io(_) | DbError::Sled(_) => false, // potential transient errors
                                DbError::NotEnoughSpace => true, // db full
                                _ => {
                                    error!("Problem storing data, but it was ignored: {error}");
                                    return Ok(vec![]);
                                } // the rest seem to be non-problematic errors.. (?)
                            };

                            if full {
                                error!("Not enough space to store more data");
                            }

                            let node_id = PublicKey::from(self.info.read().await.keypair.public);
                            let msg = SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                                node_id,
                                data,
                                full,
                            });

                            Ok(vec![self.send_msg_to_our_elders(msg).await?])
                        }
                    }
                };
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
                debug!("{:?}", LogMarker::ChunkQueryResponseReceviedFromAdult);
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
                if let Some(session_info) = self.dkg_sessions.read().await.get(&session_id).cloned()
                {
                    let DkgSessionInfo {
                        prefix,
                        elders,
                        authority: section_auth,
                    } = session_info;
                    let message_cache = self.dkg_voter.get_cached_msgs(&session_id);
                    trace!(
                        "Sending DkgSessionInfo {{ {:?}, elders {:?}, ... }} to {}",
                        &session_id,
                        elders,
                        &sender
                    );

                    let node_msg = SystemMsg::DkgSessionInfo {
                        session_id,
                        elders,
                        prefix,
                        section_auth,
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
                prefix,
                elders,
                message_cache,
                section_auth,
                message,
            } => {
                let mut cmds = vec![];
                // Reconstruct the original DKG start message and verify the section signature
                let payload = WireMsg::serialize_msg_payload(&SystemMsg::DkgStart {
                    session_id,
                    prefix,
                    elders: elders.clone(),
                })?;
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
                }
                let _existing = self.dkg_sessions.write().await.insert(
                    session_id,
                    DkgSessionInfo {
                        prefix,
                        elders: elders.clone(),
                        authority: section_auth,
                    },
                );
                trace!("DkgSessionInfo handling {:?} - {:?}", session_id, elders);
                cmds.extend(self.handle_dkg_start(session_id, prefix, elders).await?);
                cmds.extend(
                    self.handle_dkg_retry(session_id, message_cache, message, sender)
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

    async fn republish_data_for_deviant_nodes(
        &self,
        deviants: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        let our_adults = self
            .network_knowledge
            .adults()
            .await
            .iter()
            .map(|peer| peer.name())
            .collect::<BTreeSet<XorName>>();

        self.reorganize_data(
            self.info.read().await.name(),
            BTreeSet::new(),
            deviants,
            our_adults,
            true,
        )
        .await
        .map_err(crate::node::Error::from)
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
