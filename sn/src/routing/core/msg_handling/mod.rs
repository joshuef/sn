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
use crate::messaging::{
    data::{ServiceMsg, StorageLevel},
    signature_aggregator::Error as AggregatorError,
    system::{NodeCmd, NodeQuery, SystemMsg},
    DstLocation, EndUser, MessageId, MessageType, MsgKind, NodeMsgAuthority, SectionAuth,
    ServiceAuth, SrcLocation, WireMsg,
};
use crate::routing::{
    log_markers::LogMarker,
    messages::{NodeMsgAuthorityUtils, WireMsgUtils},
    network_knowledge::SectionPeers,
    relocation::RelocateState,
    routing_api::command::Command,
    Error, Event, MessageReceived, Peer, Result,
};
use crate::types::{Chunk, Keypair, PublicKey};
use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use rand::rngs::OsRng;
use std::{collections::BTreeSet, net::SocketAddr};
use xor_name::XorName;

// Message handling
impl Core {
    #[instrument(skip(self, original_bytes))]
    pub(crate) async fn handle_message(
        &self,
        sender_addr: SocketAddr,
        wire_msg: WireMsg,
        original_bytes: Option<Bytes>,
    ) -> Result<Vec<Command>> {
        let mut cmds = vec![];
        trace!("handling msg");
        // Apply backpressure if needed.
        if let Some(load_report) = self.comm.check_strain(sender_addr).await {
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
                        msg_id, sender_addr, msg
                    );
                    return Ok(cmds);
                }

                trace!(
                    "Trusted msg authority in message ({:?}) from {:?}: {:?}",
                    msg_id,
                    sender_addr,
                    msg
                );

                // TODO: msg_authority.peer() exists, but will error if authority is
                // section/sectionshare. It's not clear if this would actually
                // represent a valid peer.
                let sender = Peer::new(msg_authority.name(), sender_addr);

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
                            msg, sender_addr
                        );
                        return Ok(vec![]);
                    }
                };
                let user = match self.comm.get_peer_connection_id(&sender_addr).await {
                    Some(name) => EndUser(name),
                    None => {
                        error!(
                            "Service msg has been dropped since client connection id for {} was not found: {:?}",
                            sender_addr, msg
                        );
                        return Ok(cmds);
                    }
                };

                let sender = Peer::new(user.0, sender_addr);
                let src_location = SrcLocation::EndUser(user);

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
                    self.handle_service_message(msg_id, auth, msg, dst_location, user)
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
            SystemMsg::JoinResponse(join_response) => {
                debug!(
                    "Ignoring unexpected join response message: {:?}",
                    join_response
                );
                Ok(vec![])
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
                    trace!("Dropping Propose msg from {}: {:?}", sender, msg_id);
                    return Ok(vec![]);
                }

                trace!("Handling msg: Propose from {}: {:?}", sender, msg_id);
                self.handle_proposal(msg_id, proposal, sig_share, sender)
                    .await
            }
            SystemMsg::DkgStart {
                session_id,
                prefix,
                elders,
            } => {
                trace!("Handling msg: Dkg-Start from {}", sender);
                if !elders.contains_key(&self.node.read().await.name()) {
                    return Ok(vec![]);
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
                self.handle_dkg_message(session_id, message, sender.name())
                    .await
            }
            SystemMsg::DkgFailureObservation {
                session_id,
                sig,
                failed_participants,
            } => {
                trace!("Handling msg: Dkg-FailureObservation from {}", sender);
                self.handle_dkg_failure_observation(session_id, &failed_participants, sig)
            }
            // The following type of messages are all handled by upper sn_node layer.
            // TODO: In the future the sn-node layer won't be receiving Events but just
            // plugging in msg handlers.
            SystemMsg::NodeCmd(node_cmd) => {
                match node_cmd {
                    NodeCmd::ReceiveExistingData { metadata } => {
                        info!("Processing received DataExchange packet: {:?}", msg_id);

                        self.register_storage.update(metadata.reg_data)?;
                        self.update_chunks(metadata.chunk_data).await;
                    }
                    NodeCmd::StoreChunk { chunk, .. } => {
                        info!("Processing chunk write with MessageId: {:?}", msg_id);
                        // There is no point in verifying a sig from a sender A or B here.
                        let level_report = self.chunk_storage.store(&chunk).await?;
                        return Ok(self.record_if_any(level_report).await);
                    }
                    NodeCmd::ReplicateChunk(chunk) => {
                        info!(
                            "Processing replicate chunk cmd with MessageId: {:?}",
                            msg_id
                        );

                        return if self.is_elder().await {
                            self.republish_chunk(chunk).await
                        } else {
                            // We are an adult here, so just store away!

                            // TODO: should this be a cmd returned for threading?
                            let level_report =
                                self.chunk_storage.store_for_replication(chunk).await?;
                            Ok(self.record_if_any(level_report).await)
                        };
                    }
                    NodeCmd::RepublishChunk(chunk) => {
                        info!(
                            "Republishing chunk {:?} with MessageId {:?}",
                            chunk.name(),
                            msg_id
                        );

                        return self.republish_chunk(chunk).await;
                    }
                    _ => {
                        self.send_event(Event::MessageReceived {
                            msg_id,
                            src: msg_authority.src_location(),
                            dst: dst_location,
                            msg: Box::new(MessageReceived::NodeCmd(node_cmd)),
                        })
                        .await;
                    }
                }

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
            let target_holders = self.get_chunk_holder_adults(chunk.name()).await;
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
                *msg_authority = NodeMsgAuthority::Section(section_auth);
                Ok(false)
            }
            Err(AggregatorError::NotEnoughShares) => Ok(true),
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
