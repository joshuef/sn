// Copyright 2021 MaidSafe.net limited.
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

use super::Core;
use crate::peer::{Peer, UnnamedPeer};
use crate::routing::{
    log_markers::LogMarker,
    messages::{NodeMsgAuthorityUtils, WireMsgUtils},
    network_knowledge::{NetworkKnowledge, SectionPeers},
    relocation::RelocateState,
    routing_api::command::Command,
    Error, Event, MessageReceived, Result, MIN_LEVEL_WHEN_FULL,
};
use crate::types::{Chunk, Keypair, PublicKey};
use crate::{
    messaging::{
        data::{ServiceMsg, StorageLevel},
        signature_aggregator::Error as AggregatorError,
        system::{
            JoinRequest, JoinResponse, NodeCmd, NodeQuery, SectionAuth as SystemSectionAuth,
            SystemMsg,
        },
        AuthorityProof, DstLocation, MessageId, MessageType, MsgKind, NodeMsgAuthority,
        SectionAuth, ServiceAuth, WireMsg,
    },
    routing::core::DkgSessionInfo,
};
use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use rand::rngs::OsRng;
use std::collections::BTreeSet;
use xor_name::XorName;

// Message handling
impl Core {
    #[instrument(skip(self, original_bytes))]
    pub(crate) async fn handle_message(
        &self,
        sender: UnnamedPeer,
        wire_msg: WireMsg,
        original_bytes: Option<Bytes>,
    ) -> Result<Vec<Command>> {
        let mut cmds = vec![];
        trace!("handling msg");

        // Apply backpressure if needed.
        if let Some(load_report) = self.comm.check_strain(sender.addr()).await {
            let msg_src = wire_msg.msg_kind().src();
            cmds.push(Command::PrepareNodeMsgToSend {
                msg: SystemMsg::BackPressure(load_report),
                dst: msg_src.to_dst(),
            })
        }

        // Deserialize the payload of the incoming message
        let payload = wire_msg.payload.clone();
        let msg_id = wire_msg.msg_id();

        let message_type = match wire_msg.into_message() {
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
            MessageType::System {
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

                let sender = sender.named(msg_authority.name());

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
                        | SystemMsg::AntiEntropyProbe(_)
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

                                if let Some(ae_command) = self
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
                                    cmds.push(ae_command);
                                    return Ok(cmds);
                                }

                                trace!("Entropy check passed. Handling verified msg {:?}", msg_id);
                            }
                        },
                    }
                }

                cmds.push(Command::HandleSystemMessage {
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
            MessageType::Service {
                msg_id,
                auth,
                msg,
                dst_location,
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
                let sender = sender.named(src_location.name());

                if self.is_not_elder().await {
                    trace!("Redirecting from adult to section elders");
                    cmds.push(
                        self.ae_redirect_to_our_elders(sender, &src_location, &wire_msg)
                            .await?,
                    );
                    return Ok(cmds);
                }

                // First we perform AE checks
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
                    self.handle_service_message(msg_id, auth, msg, dst_location, sender)
                        .await?,
                );

                Ok(cmds)
            }
        }
    }

    // Handler for all system messages
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_system_message(
        &self,
        sender: Peer,
        msg_id: MessageId,
        mut msg_authority: NodeMsgAuthority,
        dst_location: DstLocation,
        msg: SystemMsg,
        payload: Bytes,
        known_keys: Vec<BlsPublicKey>,
    ) -> Result<Vec<Command>> {
        trace!("{:?}", LogMarker::SystemMsgToBeHandled);

        // We assume to be aggregated if it contains a BLS Share sig as authority.
        match self
            .aggregate_message_and_stop(&mut msg_authority, payload)
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
            Ok(true) | Err(_) => Ok(vec![]),
        }
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    pub(crate) async fn handle_valid_msg(
        &self,
        msg_id: MessageId,
        msg_authority: NodeMsgAuthority,
        dst_location: DstLocation,
        node_msg: SystemMsg,
        sender: Peer,
        known_keys: Vec<BlsPublicKey>,
    ) -> Result<Vec<Command>> {
        self.network_knowledge().merge_connections([&sender]).await;

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
                    members.map(|members| {
                        SectionPeers::new(
                            members.into_iter().map(|member| member.into_authed_state()),
                        )
                    }),
                )
                .await
            }
            SystemMsg::Relocate(ref details) => {
                trace!("Handling msg: Relocate from {}: {:?}", sender, msg_id);
                if let NodeMsgAuthority::Section(section_signed) = msg_authority {
                    Ok(self
                        .handle_relocate(details.clone(), node_msg, section_signed)
                        .await?
                        .into_iter()
                        .collect())
                } else {
                    Err(Error::InvalidSrcLocation)
                }
            }
            SystemMsg::RelocatePromise(promise) => {
                trace!(
                    "Handling msg: RelocatePromise from {}: {:?}",
                    sender,
                    msg_id
                );
                self.handle_relocate_promise(promise, node_msg).await
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

                Ok(vec![Command::TestConnectivity(name)])
            }
            SystemMsg::JoinAsRelocatedResponse(join_response) => {
                trace!("Handling msg: JoinAsRelocatedResponse from {}", sender);
                if let Some(RelocateState::InProgress(ref mut joining_as_relocated)) =
                    *self.relocate_state.write().await
                {
                    if let Some(cmd) = joining_as_relocated
                        .handle_join_response(*join_response, sender.addr())
                        .await?
                    {
                        return Ok(vec![cmd]);
                    }
                } else {
                    error!("self.relocate_state is not in Progress");
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
                // #TODO: Factor in med/long term backpressure into general node liveness calculations
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
                                let mut commands = vec![];

                                if let Some(RelocateState::InProgress(
                                    ref mut joining_as_relocated,
                                )) = *self.relocate_state.write().await
                                {
                                    let new_node = joining_as_relocated.node.clone();
                                    let new_name = new_node.name();
                                    let previous_name = self.node.read().await.name();
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
                                    let _ = new_network_knowledge
                                        .merge_members(SectionPeers::from_msg(members));

                                    // TODO: confirm whether carry out the switch immediately here
                                    //       or still using the Command pattern.
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
                                        &self.node.read().await.clone(),
                                        DstLocation::Section {
                                            name: new_name,
                                            section_pk: section_key,
                                        },
                                        node_msg,
                                        section_key,
                                    )?;
                                    commands.push(Command::SendMessage {
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

                                Ok(commands)
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
                    // return Ok(vec![]);
                }

                trace!("Handling msg: Propose from {}: {:?}", sender, msg_id);
                self.handle_proposal(msg_id, proposal.into_state(), sig_share, sender)
                    .await
            }
            SystemMsg::DkgStart {
                session_id,
                prefix,
                elders,
            } => {
                trace!("Handling msg: Dkg-Start {:?} from {}", session_id, sender);
                if !elders.contains_key(&self.node.read().await.name()) {
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
                self.handle_dkg_message(session_id, message, sender).await
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
            // The following type of messages are all handled by upper sn_node layer.
            // TODO: In the future the sn-node layer won't be receiving Events
            SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel { node_id, level, .. }) => {

                debug!("RecordStorageLevel received");
                let changed = self.set_storage_level(&node_id, level).await;
                if changed && level.value() == MIN_LEVEL_WHEN_FULL {
                    // ..then we accept a new node in place of the full node
                    *self.joins_allowed.write().await = true;
                }
                Ok(vec![])
            }
            SystemMsg::NodeCmd(NodeCmd::ReceiveExistingData { metadata }) => {
                info!("Processing received DataExchange packet: {:?}", msg_id);

                self.register_storage.update(metadata.reg_data)?;
                self.update_chunks(metadata.chunk_data).await;
                Ok(vec![])
            }
            SystemMsg::NodeCmd(NodeCmd::StoreChunk { chunk, .. }) => {
                info!("Processing chunk write with MessageId: {:?}", msg_id);
                // There is no point in verifying a sig from a sender A or B here.

                // This may return a DatabasFull error... but we should have reported storage increase
                // well before this
                let level_report = self.chunk_storage.store(&chunk).await?;
                info!("Storage level report: {:?}", level_report);
                return Ok(self.record_if_any(level_report).await);
            }
            SystemMsg::NodeCmd(NodeCmd::ReplicateChunk(chunk)) => {
                info!(
                    "Processing replicate chunk cmd with MessageId: {:?}",
                    msg_id
                );

                return if self.is_elder().await {
                    self.republish_chunk(chunk).await
                } else {
                    // We are an adult here, so just store away!

                    // TODO: should this be a cmd returned for threading?
                    let level_report = self.chunk_storage.store_for_replication(chunk).await?;
                    Ok(self.record_if_any(level_report).await)
                };
            }
            SystemMsg::NodeCmd(NodeCmd::RepublishChunk(chunk)) => {
                info!(
                    "Republishing chunk {:?} with MessageId {:?}",
                    chunk.name(),
                    msg_id
                );

                return self.republish_chunk(chunk).await;
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
                    // A request from EndUser - via elders - for locally stored chunk
                    NodeQuery::GetChunk { origin, address } => {
                        // There is no point in verifying a sig from a sender A or B here.
                        // Send back response to the sending elder

                        let sender_xorname = msg_authority.get_auth_xorname();
                        self.handle_get_chunk_at_adult(msg_id, &address, origin, sender_xorname)
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
                    NodeMsgAuthority::Node(auth) => PublicKey::from(auth.into_inner().public_key),
                    _ => return Err(Error::InvalidQueryResponseAuthority),
                };

                self.handle_chunk_query_response_at_elder(
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
                    let message_cache = self.dkg_voter.get_cached_messages(&session_id);
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
                        &self.node.read().await.clone(),
                        DstLocation::Node {
                            name: sender.name(),
                            section_pk,
                        },
                        node_msg,
                        section_pk,
                    )?;

                    Ok(vec![Command::SendMessage {
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
                let mut commands = vec![];
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
                        return Ok(commands);
                    } else {
                        trace!("DkgSessionInfo signature verified");
                    }
                } else {
                    warn!(
                        "Cannot verify DkgSessionInfo: {:?}. Unknown key: {:?}!",
                        &session_id, auth.sig.public_key
                    );
                    warn!(
                        "Chain: {:?}",
                        self.network_knowledge().section_chain().await
                    );
                    return Ok(commands);
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
                commands.extend(self.handle_dkg_start(session_id, prefix, elders).await?);
                commands.extend(
                    self.handle_dkg_retry(session_id, message_cache, message, sender)
                        .await?,
                );
                Ok(commands)
            }
        }
    }

    async fn record_if_any(&self, level: Option<StorageLevel>) -> Vec<Command> {
        let mut cmds = vec![];
        if let Some(level) = level {
            info!("Storage has now passed {} % used.", 10 * level.value());
            let node_id = PublicKey::from(self.node.read().await.keypair.public);
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

            cmds.push(Command::PrepareNodeMsgToSend { msg, dst });
        }
        cmds
    }

    // Locate ideal chunk holders for this chunk, line up wiremsgs for those to instruct them to store the chunk
    async fn republish_chunk(&self, chunk: Chunk) -> Result<Vec<Command>> {
        if self.is_elder().await {
            let target_holders = self.get_adults_holding_chunk(chunk.name()).await;
            info!(
                "Republishing chunk {:?} to holders {:?}",
                chunk.name(),
                &target_holders,
            );

            let msg = SystemMsg::NodeCmd(NodeCmd::ReplicateChunk(chunk));
            let aggregation = false;

            self.send_node_msg_to_targets(msg, target_holders, aggregation)
                .await
        } else {
            error!("Received unexpected message while Adult");
            Ok(vec![])
        }
    }

    /// Takes a message and forms commands to send to specified targets
    pub(super) async fn send_node_msg_to_targets(
        &self,
        msg: SystemMsg,
        targets: BTreeSet<XorName>,
        aggregation: bool,
    ) -> Result<Vec<Command>> {
        let msg_id = MessageId::new();

        let our_name = self.node.read().await.name();

        // we create a dummy/random dst location,
        // we will set it correctly for each msg and target
        // let name = network.our_name().await;
        let section_pk = self.network_knowledge().section_key().await;

        let dummy_dst_location = DstLocation::Node {
            name: our_name,
            section_pk,
        };

        // separate this into form_wire_msg based on agg
        let mut wire_msg = if aggregation {
            let src = our_name;

            WireMsg::for_dst_accumulation(
                &self.key_share().await.map_err(|err| err)?,
                src,
                dummy_dst_location,
                msg,
                section_pk,
            )
        } else {
            WireMsg::single_src(
                &self.node.read().await.clone(),
                dummy_dst_location,
                msg,
                section_pk,
            )
        }?;

        wire_msg.set_msg_id(msg_id);

        let mut commands = vec![];

        for target in targets {
            debug!("sending {:?} to {:?}", wire_msg, target);
            let mut wire_msg = wire_msg.clone();
            let dst_section_pk = self.section_key_by_name(&target).await;
            wire_msg.set_dst_section_pk(dst_section_pk);
            wire_msg.set_dst_xorname(target);

            commands.push(Command::ParseAndSendWireMsg(wire_msg));
        }

        Ok(commands)
    }

    // Convert the provided NodeMsgAuthority to be a `Section` message
    // authority on successful accumulation. Also return 'true' if
    // current message shall not be processed any further.
    async fn aggregate_message_and_stop(
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

    // TODO: Dedupe this w/ node
    fn random_client_signature(client_msg: &ServiceMsg) -> Result<(MsgKind, Bytes)> {
        let mut rng = OsRng;
        let keypair = Keypair::new_ed25519(&mut rng);
        let payload = WireMsg::serialize_msg_payload(client_msg)?;
        let signature = keypair.sign(&payload);

        let msg = MsgKind::ServiceMsg(ServiceAuth {
            public_key: keypair.public_key(),
            signature,
        });

        Ok((msg, payload))
    }
}
