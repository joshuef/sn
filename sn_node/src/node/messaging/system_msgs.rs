// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    comm::Comm,
    dbs::Error as DbError,
    node::{
        flow_ctrl::cmds::Cmd,
        messaging::{OutgoingMsg, Peers},
        DkgSessionInfo, Error, Event, MembershipEvent, Node, Proposal as CoreProposal, Result,
        MIN_LEVEL_WHEN_FULL,
    },
};

use std::collections::BTreeSet;

#[cfg(feature = "traceroute")]
use sn_interface::messaging::Traceroute;
use sn_interface::{
    messaging::{
        data::StorageLevel,
        signature_aggregator::Error as AggregatorError,
        system::{
            // SectionAuth is gonna cause issue
            JoinResponse,
            NodeCmd,
            NodeEvent,
            NodeMsgAuthorityUtils,
            NodeQuery,
            Proposal as ProposalMsg,
            SystemMsg,
        },
        AuthorityProof, MsgId, NodeMsgAuthority, SectionAuth, WireMsg,
    },
    network_knowledge::NetworkKnowledge,
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey},
};

use bytes::Bytes;
use xor_name::XorName;

impl Node {
    /// Send a (`SystemMsg`) message to all Elders in our section
    pub(crate) fn send_msg_to_our_elders(&self, msg: SystemMsg) -> Cmd {
        let sap = self.network_knowledge.authority_provider();
        let recipients = sap.elders_set();
        self.send_system_msg(msg, Peers::Multiple(recipients))
    }

    /// Send a (`SystemMsg`) message to a node
    pub(crate) fn send_system_msg(&self, msg: SystemMsg, recipients: Peers) -> Cmd {
        self.trace_system_msg(
            msg,
            recipients,
            #[cfg(feature = "traceroute")]
            Traceroute(vec![]),
        )
    }

    /// Send a (`SystemMsg`) message to a node
    pub(crate) fn trace_system_msg(
        &self,
        msg: SystemMsg,
        recipients: Peers,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Cmd {
        trace!("{}", LogMarker::SendToNodes);
        Cmd::send_traced_msg(
            OutgoingMsg::System(msg),
            recipients,
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    /// Aggregation of system messages
    /// Returns an updated NodeMsgAuthority if
    /// msg was aggregated, or same as input if not
    /// of type [`NodeMsgAuthority::BlsShare`], else [`None`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn aggregate_system_msg(
        &mut self,
        msg_id: MsgId,
        msg_authority: NodeMsgAuthority,
        payload: Bytes,
    ) -> Option<NodeMsgAuthority> {
        // We assume to be aggregated if it contains a BLS Share sig as authority.
        match self.aggregate_msg(msg_authority, payload) {
            Ok(msg_authority) => msg_authority,
            Err(Error::InvalidSignatureShare) => {
                warn!(
                    "Invalid signature on received system message, dropping the message: {:?}",
                    msg_id
                );
                None
            }
            Err(err) => {
                trace!("aggregate_system_msg got error {:?}", err);
                None
            }
        }
    }

    // Handler for data messages which have successfully
    // passed all signature checks and msg verifications
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_valid_system_msg(
        &mut self,
        msg_id: MsgId,
        msg_authority: NodeMsgAuthority,
        msg: SystemMsg,
        sender: Peer,
        comm: &Comm,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}", LogMarker::SystemMsgToBeHandled);

        #[cfg(feature = "traceroute")]
        {
            if !traceroute.0.is_empty() {
                info!(
                    "Handling SystemMsg {}:{:?} with trace \n{:?}",
                    msg, msg_id, traceroute
                );
            }
        }

        let src_name = msg_authority.name();
        match msg {
            SystemMsg::AntiEntropyUpdate {
                section_auth,
                section_signed,
                proof_chain,
                members,
            } => {
                // mark that we've received an AE update from this node
                // AEProbes are used in health checks for elders
                self.dysfunction_tracking
                    .ae_update_msg_received(&sender.name());

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
                if self.is_not_elder() {
                    return Ok(vec![]);
                }

                Ok(vec![Cmd::TestConnectivity(name)])
            }
            SystemMsg::JoinAsRelocatedResponse(join_response) => {
                trace!("Handling msg: JoinAsRelocatedResponse from {}", sender);
                if let Some(ref mut joining_as_relocated) = self.relocate_state {
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

                #[cfg(feature = "traceroute")]
                info!("Handling AE-Retry message with trace {:?}", traceroute);

                self.handle_anti_entropy_retry_msg(
                    section_auth.into_state(),
                    section_signed,
                    proof_chain,
                    bounced_msg,
                    sender,
                    #[cfg(feature = "traceroute")]
                    traceroute,
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
                    #[cfg(feature = "traceroute")]
                    traceroute,
                )
                .await
            }
            // Respond to a probe msg
            // We always respond to probe msgs. Health checks use this to see if a node is alive
            // and repsonsive, as well as being a method of keeping nodes up to date.
            SystemMsg::AntiEntropyProbe(section_key) => {
                let mut cmds = vec![];
                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                let mut recipients = BTreeSet::new();
                let _existed = recipients.insert(sender);
                cmds.push(self.send_ae_update_to_nodes(recipients, section_key));
                Ok(cmds)
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
                Ok(vec![Cmd::Comm(crate::comm::Cmd::Regulate {
                    peer: sender,
                    msgs_per_s,
                })])
            }
            // The AcceptedOnlineShare for relocation will be received here.
            SystemMsg::JoinResponse(join_response) => {
                match *join_response {
                    JoinResponse::Approved {
                        section_auth,
                        section_chain,
                        ..
                    } => {
                        info!(
                            "Relocation: Aggregating received ApprovalShare from {:?}",
                            sender
                        );
                        info!("Relocation: Successfully aggregated ApprovalShares for joining the network");

                        if let Some(ref mut joining_as_relocated) = self.relocate_state {
                            let new_node = joining_as_relocated.node.clone();
                            let new_name = new_node.name();
                            let previous_name = self.info().name();
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

                            self.relocate(new_keypair.clone(), new_network_knowledge)?;

                            trace!(
                                "Relocation: Sending aggregated JoinRequest to {:?}",
                                recipients
                            );

                            self.send_event(Event::Membership(MembershipEvent::Relocated {
                                previous_name,
                                new_keypair,
                            }))
                            .await;

                            trace!("{}", LogMarker::RelocateEnd);
                        } else {
                            warn!("Relocation:  self.relocate_state is not in Progress");
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
                self.handle_dkg_failure_agreement(&src_name, &sig_set)
            }
            SystemMsg::HandoverVotes(votes) => self.handle_handover_msg(sender, votes).await,
            SystemMsg::HandoverAE(gen) => Ok(self
                .handle_handover_anti_entropy(sender, gen)
                .into_iter()
                .collect()),
            SystemMsg::JoinRequest(join_request) => {
                trace!("Handling msg: JoinRequest from {}", sender);
                self.handle_join_request(sender, join_request, comm)
                    .await
                    .map(|c| c.into_iter().collect())
            }
            SystemMsg::JoinAsRelocatedRequest(join_request) => {
                trace!("Handling msg: JoinAsRelocatedRequest from {}", sender);
                if self.is_not_elder()
                    && join_request.section_key == self.network_knowledge.section_key()
                {
                    return Ok(vec![]);
                }
                Ok(self
                    .handle_join_as_relocated_request(sender, *join_request, comm)
                    .await
                    .into_iter()
                    .collect())
            }
            SystemMsg::MembershipVotes(votes) => {
                let mut cmds = vec![];
                cmds.extend(self.handle_membership_votes(sender, votes)?);
                Ok(cmds)
            }
            SystemMsg::MembershipAE(gen) => Ok(self
                .handle_membership_anti_entropy(sender, gen)
                .into_iter()
                .collect()),
            SystemMsg::Propose {
                proposal,
                sig_share,
            } => {
                if self.is_not_elder() {
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

                Node::handle_proposal(
                    msg_id,
                    core_proposal,
                    sig_share,
                    sender,
                    &self.network_knowledge,
                    &mut self.proposal_aggregator,
                )
            }
            SystemMsg::DkgStart(session_id) => {
                trace!("Handling msg: Dkg-Start {:?} from {}", session_id, sender);
                self.log_dkg_session(&sender.name());
                let our_name = self.info().name();
                if !session_id.contains_elder(our_name) {
                    return Ok(vec![]);
                }
                if let NodeMsgAuthority::Section(authority) = msg_authority {
                    let _existing = self.dkg_sessions.insert(
                        session_id.hash(),
                        DkgSessionInfo {
                            session_id: session_id.clone(),
                            authority,
                        },
                    );
                }
                self.handle_dkg_start(session_id)
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
                // We could receive a DkgStart BEFORE starts tracking it in dysfunction
                self.log_dkg_session(&sender.name());
                self.handle_dkg_msg(session_id, message, sender)
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
            } => Ok(vec![self.handle_dkg_not_ready(sender, message, session_id)]),
            SystemMsg::DkgRetry {
                message_history,
                message,
                session_id,
            } => self.handle_dkg_retry(&session_id, message_history, message, sender),
            SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel { node_id, level, .. }) => {
                let changed = self.set_storage_level(&node_id, level);
                if changed && level.value() == MIN_LEVEL_WHEN_FULL {
                    // ..then we accept a new node in place of the full node
                    self.joins_allowed = true;
                }
                Ok(vec![])
            }
            SystemMsg::NodeCmd(NodeCmd::ReceiveMetadata { metadata }) => {
                info!("Processing received MetadataExchange packet: {:?}", msg_id);
                self.set_adult_levels(metadata);
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
                if self.is_not_elder() {
                    error!("Received unexpected message while Adult");
                    return Ok(vec![]);
                }

                if full {
                    let changed =
                        self.set_storage_level(&node_id, StorageLevel::from(StorageLevel::MAX)?);
                    if changed {
                        // ..then we accept a new node in place of the full node
                        self.joins_allowed = true;
                    }
                }

                let targets = self.target_data_holders(data.name());

                Ok(vec![self.replicate_data(
                    data,
                    targets,
                    #[cfg(feature = "traceroute")]
                    traceroute,
                )])
            }
            SystemMsg::NodeCmd(NodeCmd::ReplicateData(data_collection)) => {
                info!("ReplicateData MsgId: {:?}", msg_id);

                if self.is_elder() {
                    error!("Received unexpected message while Elder");
                    return Ok(vec![]);
                }

                let mut cmds = vec![];

                let section_pk = PublicKey::Bls(self.network_knowledge.section_key());
                let own_keypair = Keypair::Ed25519(self.keypair.clone());

                for data in data_collection {
                    // We are an adult here, so just store away!
                    // This may return a DatabaseFull error... but we should have reported storage increase
                    // well before this
                    match self
                        .data_storage
                        .store(&data, section_pk, own_keypair.clone())
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
                        Err(DbError::NotEnoughSpace) => {
                            // db full
                            error!("Not enough space to store more data");

                            let node_id = PublicKey::from(self.keypair.public);
                            let msg = SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                                node_id,
                                data,
                                full: true,
                            });

                            cmds.push(self.send_msg_to_our_elders(msg))
                        }
                        Err(error) => {
                            // the rest seem to be non-problematic errors.. (?)
                            error!("Problem storing data, but it was ignored: {error}");
                        }
                    }
                }

                Ok(cmds)
            }
            SystemMsg::NodeCmd(NodeCmd::SendAnyMissingRelevantData(known_data_addresses)) => {
                info!(
                    "{:?} MsgId: {:?}",
                    LogMarker::RequestForAnyMissingData,
                    msg_id
                );
                Ok(self
                    .get_missing_data_for_node(sender, known_data_addresses)
                    .into_iter()
                    .collect())
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
                        debug!(
                            "Handle NodeQuery with msg_id {:?} and correlation_id {:?}",
                            msg_id, correlation_id,
                        );
                        // There is no point in verifying a sig from a sender A or B here.
                        // Send back response to the sending elder
                        Ok(vec![
                            self.handle_data_query_at_adult(
                                correlation_id,
                                &query,
                                auth,
                                origin,
                                sender,
                                #[cfg(feature = "traceroute")]
                                traceroute,
                            )
                            .await,
                        ])
                    }
                }
            }
            SystemMsg::NodeQueryResponse {
                response,
                correlation_id,
                user,
            } => {
                debug!(
                    "{:?}: op_id {:?}, correlation_id: {correlation_id:?}, sender: {sender} origin msg_id: {:?}",
                    LogMarker::ChunkQueryResponseReceviedFromAdult,
                    response.operation_id().map(|s| s.to_string()).unwrap_or_else(|_| "None".to_string()),
                    msg_id
                );
                let sending_nodes_pk = match msg_authority {
                    NodeMsgAuthority::Node(auth) => PublicKey::from(auth.into_inner().node_ed_pk),
                    _ => return Err(Error::InvalidQueryResponseAuthority),
                };
                Ok(self
                    .handle_data_query_response_at_elder(
                        correlation_id,
                        response,
                        user,
                        sending_nodes_pk,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
                    .await
                    .into_iter()
                    .collect())
            }
            SystemMsg::DkgSessionUnknown {
                session_id,
                message,
            } => {
                if let Some(session_info) = self.dkg_sessions.get(&session_id.hash()).cloned() {
                    let message_cache = self.dkg_voter.get_cached_msgs(&session_info.session_id);
                    trace!(
                        "Sending DkgSessionInfo {{ {:?}, ... }} to {}",
                        &session_info.session_id,
                        &sender
                    );

                    let msg = SystemMsg::DkgSessionInfo {
                        session_id,
                        section_auth: session_info.authority,
                        message_cache,
                        message,
                    };

                    Ok(vec![Cmd::send_traced_msg(
                        OutgoingMsg::System(msg),
                        Peers::Single(sender),
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )])
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
                if self.network_knowledge.section_key() == auth.sig.public_key {
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
                    let chain = self.network_knowledge().section_chain();
                    warn!("Chain: {:?}", chain);
                    return Ok(cmds);
                };
                let _existing = self.dkg_sessions.insert(
                    session_id.hash(),
                    DkgSessionInfo {
                        session_id: session_id.clone(),
                        authority: section_auth,
                    },
                );
                trace!("DkgSessionInfo handling {:?}", session_id);
                cmds.extend(self.handle_dkg_start(session_id.clone())?);
                cmds.extend(self.handle_dkg_retry(&session_id, message_cache, message, sender)?);
                Ok(cmds)
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
            let msg = SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel {
                section: node_xorname,
                node_id,
                level,
            });

            let dst = Peers::Multiple(self.network_knowledge.elders());

            cmds.push(Cmd::send_traced_msg(
                OutgoingMsg::System(msg),
                dst,
                #[cfg(feature = "traceroute")]
                traceroute,
            ));
        }

        Ok(cmds)
    }

    // Converts the provided NodeMsgAuthority to be a `Section` message
    // authority on successful accumulation.
    fn aggregate_msg(
        &mut self,
        msg_authority: NodeMsgAuthority,
        payload: Bytes,
    ) -> Result<Option<NodeMsgAuthority>> {
        let bls_share_auth = if let NodeMsgAuthority::BlsShare(bls_share_auth) = msg_authority {
            bls_share_auth
        } else {
            return Ok(Some(msg_authority));
        };

        match SectionAuth::try_authorize(
            &mut self.message_aggregator,
            bls_share_auth.into_inner(),
            &payload,
        ) {
            Ok(section_auth) => {
                info!("Successfully aggregated message");
                Ok(Some(NodeMsgAuthority::Section(section_auth)))
            }
            Err(AggregatorError::NotEnoughShares) => {
                info!("Not enough shares to aggregate received message");
                Ok(None)
            }
            Err(err) => {
                error!("Error accumulating message at dst: {:?}", err);
                Err(Error::InvalidSignatureShare)
            }
        }
    }
}
