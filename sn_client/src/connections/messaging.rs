// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{QueryResult, Session};
use crate::{Error, Result};
use async_recursion::async_recursion;
use itertools::Itertools;
use qp2p::UsrMsgBytes;
use sn_interface::{
    messaging::{
        data::{ClientMsg, DataQuery, DataQueryVariant, QueryResponse},
        system::{AntiEntropyKind, NodeMsg},
        ClientAuth, Dst, MsgId, MsgKind, MsgType, WireMsg,
    },
    network_knowledge::{supermajority, SectionTreeUpdate},
    types::{log_markers::LogMarker, ChunkAddress, Peer},
};
#[cfg(feature = "traceroute")]
use sn_interface::{
    messaging::{Entity, Traceroute},
    types::PublicKey,
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use futures::future::join_all;
use rand::{rngs::OsRng, seq::SliceRandom};
use std::{collections::BTreeSet, time::Duration};

use tracing::{debug, error, trace, warn};
use xor_name::XorName;

// Number of Elders subset to send queries to
pub(crate) const NUM_OF_ELDERS_SUBSET_FOR_QUERIES: usize = 3;

// Number of bootstrap nodes to attempt to contact per batch (if provided by the node_config)
pub(crate) const NODES_TO_CONTACT_PER_STARTUP_BATCH: usize = 3;

// Duration of wait for the node to have chance to pickup network knowledge at the beginning
const INITIAL_WAIT: u64 = 1;

impl Session {
    #[instrument(
        skip(self, auth, payload, client_pk),
        level = "debug",
        name = "session send cmd"
    )]
    pub(crate) async fn send_cmd(
        &mut self,
        dst_address: XorName,
        auth: ClientAuth,
        payload: Bytes,
        force_new_link: bool,
        #[cfg(feature = "traceroute")] client_pk: PublicKey,
    ) -> Result<()> {
        let endpoint = self.endpoint.clone();
        // TODO: Consider other approach: Keep a session per section!
        let (section_pk, elders) = self.get_cmd_elders(dst_address).await?;

        let elders_len = elders.len();
        let msg_id = MsgId::new();

        debug!(
            "Sending cmd w/id {msg_id:?}, from {}, to {elders_len} Elders w/ dst: {dst_address:?}",
            endpoint.public_addr(),
        );

        let dst = Dst {
            name: dst_address,
            section_key: section_pk,
        };

        let kind = MsgKind::Client(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        // Don't immediately fail if sending to one elder fails. This could prevent further sends
        // and further responses coming in...
        // Failing directly here could cause us to miss a send success
        let send_msg_res = self
            .send_many_msgs_and_await_responses(elders.clone(), wire_msg, msg_id, force_new_link)
            .await?;
        trace!("Cmd msg {:?} sent", msg_id);

        // We are not wait for the receive of majority of cmd Acks.
        // This could be further strict to wait for ALL the Acks get received.
        // The period is expected to have AE completed, hence no extra wait is required.
        self.we_have_sufficient_acks_for_cmd(msg_id, elders.clone(), send_msg_res)
            .await
    }

    /// Checks for acks for a given msg.
    /// Returns Ok if we've sufficient to call this cmd a success
    async fn we_have_sufficient_acks_for_cmd(
        &self,
        msg_id: MsgId,
        elders: Vec<Peer>,
        responses: Vec<(Peer, MsgType)>, // mut resp_rx: mpsc::Receiver<MsgResponse>,
    ) -> Result<()> {
        debug!("----> init of check for acks for {:?}", msg_id);
        let expected_acks = elders.len();
        let mut received_acks = BTreeSet::default();
        let mut received_errors = BTreeSet::default();

        for (peer, msg_response) in responses {
            let src = peer.addr();
            match msg_response {
                MsgType::Client {
                    msg_id,
                    auth: _,
                    dst: _,
                    msg:
                        ClientMsg::CmdResponse {
                            response,
                            correlation_id: _,
                        },
                } => {
                    match response.result() {
                        Ok(()) => {
                            debug!("got an OK result in the msg");
                            let preexisting =
                                !received_acks.insert(src) || received_errors.contains(&src);
                            debug!(
                                "ACK from {src:?} read from set for msg_id {msg_id:?} - preexisting??: {preexisting:?}",
                            );

                            if received_acks.len() >= expected_acks {
                                trace!("Good! We've at or above {expected_acks} expected_acks");
                                return Ok(());
                            }
                        }
                        Err(error) => {
                            let _ = received_errors.insert(peer.addr());
                            error!(
                                "{msg_id:?} received error {error:?} from {src:?}, so far {} respones and {} of them are errors",
                                received_acks.len() + received_errors.len(), received_errors.len()
                            );

                            // exit if too many errors:
                            if received_errors.len() >= expected_acks {
                                error!(
                                    "Received majority of error response for cmd {:?}: {:?}",
                                    msg_id, error
                                );
                                return Err(Error::CmdError {
                                    source: error.clone(),
                                    msg_id,
                                });
                            }
                        }
                    }
                }
                _ => {
                    // TODO: handle AE here...
                    warn!("{msg_id:?} Unexpected response to Cmd. Ignoring: {msg_response:?} from {peer:?}");
                }
            }
        }

        debug!("ACKs received from: {received_acks:?}");
        debug!("CmdErrors received from: {received_errors:?}");

        let missing_responses: Vec<Peer> = elders
            .iter()
            .cloned()
            .filter(|p| !received_acks.contains(&p.addr()))
            .filter(|p| !received_errors.contains(&p.addr()))
            .collect();

        debug!(
            "Missing Responses for {msg_id:?} from: {:?}",
            missing_responses
        );

        debug!(
            "Insufficient acks returned: {}/{expected_acks}",
            received_acks.len()
        );
        Err(Error::InsufficientAcksReceived {
            msg_id,
            expected: expected_acks,
            received: received_acks.len(),
        })
    }

    #[instrument(
        skip(self, auth, payload, client_pk),
        level = "debug",
        name = "session send query"
    )]
    #[allow(clippy::too_many_arguments)]
    /// Send a `ClientMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &mut self,
        query: DataQuery,
        auth: ClientAuth,
        payload: Bytes,
        dst_section_info: Option<(bls::PublicKey, Vec<Peer>)>,
        force_new_link: bool,
        #[cfg(feature = "traceroute")] client_pk: PublicKey,
    ) -> Result<QueryResult> {
        let endpoint = self.endpoint.clone();

        let chunk_addr = if let DataQueryVariant::GetChunk(address) = query.variant {
            Some(address)
        } else {
            None
        };

        let dst = query.variant.dst_name();

        let (section_pk, elders) = if let Some(section_info) = dst_section_info {
            section_info
        } else {
            self.get_query_elders(dst).await?
        };

        let elders_len = elders.len();
        let msg_id = MsgId::new();

        debug!(
            "Sending query message {:?}, from {}, {:?} to the {} Elders closest to data name: {:?}",
            msg_id,
            endpoint.public_addr(),
            query,
            elders_len,
            elders
        );

        let dst = Dst {
            name: dst,
            section_key: section_pk,
        };
        let kind = MsgKind::Client(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        let send_responses = self
            .send_many_msgs_and_await_responses(elders.clone(), wire_msg, msg_id, force_new_link)
            .await?;

        // TODO:
        // We are now simply accepting the very first valid response we receive,
        // but we may want to revisit this to compare multiple responses and validate them,
        // similar to what we used to do up to the following commit:
        // https://github.com/maidsafe/sn_client/blob/9091a4f1f20565f25d3a8b00571cc80751918928/src/connection_manager.rs#L328
        //
        // For Chunk responses we already validate its hash matches the xorname requested from,
        // so we don't need more than one valid response to prevent from accepting invalid responses
        // from byzantine nodes, however for mutable data (non-Chunk responses) we will
        // have to review the approach.
        self.check_query_responses(send_responses, msg_id, elders.clone(), chunk_addr)
            .await
    }

    async fn check_query_responses(
        &self,
        responses: Vec<(Peer, MsgType)>,
        msg_id: MsgId,
        elders: Vec<Peer>,
        chunk_addr: Option<ChunkAddress>,
    ) -> Result<QueryResult> {
        let mut discarded_responses: usize = 0;
        let mut error_response = None;
        let mut valid_response = None;
        let elders_len = elders.len();

        for (peer, msg) in responses {
            let peer_address = peer.addr();
            match msg {
                MsgType::Client {
                    msg_id,
                    auth: _,
                    dst: _,
                    msg:
                        ClientMsg::QueryResponse {
                            response,
                            correlation_id: _,
                        },
                } => {
                    match response {
                        QueryResponse::GetChunk(Ok(chunk)) => {
                            if let Some(chunk_addr) = chunk_addr {
                                // We are dealing with Chunk query responses, thus we validate its hash
                                // matches its xorname, if so, we don't need to await for more responses
                                debug!("Chunk QueryResponse received is: {:#?}", chunk);

                                if chunk_addr.name() == chunk.name() {
                                    trace!("Valid Chunk received for {:?}", msg_id);
                                    valid_response = Some(QueryResponse::GetChunk(Ok(chunk)));
                                    break;
                                } else {
                                    // the Chunk content doesn't match its XorName,
                                    // this is suspicious and it could be a byzantine node
                                    warn!("We received an invalid Chunk response from one of the nodes");
                                    discarded_responses += 1;
                                }
                            }
                        }
                        QueryResponse::GetRegister(Err(_))
                        | QueryResponse::ReadRegister(Err(_))
                        | QueryResponse::GetRegisterPolicy(Err(_))
                        | QueryResponse::GetRegisterOwner(Err(_))
                        | QueryResponse::GetRegisterUserPermissions(Err(_))
                        | QueryResponse::GetChunk(Err(_)) => {
                            debug!(
                                "QueryResponse error #{discarded_responses} for {msg_id:?} received \
                                (but may be overridden by a non-error response \
                                from another elder): {:#?}",
                                &response
                            );
                            error_response = Some(response);
                            discarded_responses += 1;
                        }

                        QueryResponse::GetRegister(Ok(ref register)) => {
                            debug!("okay got register from {peer_address:?}");
                            // TODO: properly merge all registers
                            if let Some(QueryResponse::GetRegister(Ok(prior_response))) =
                                &valid_response
                            {
                                if register.size() > prior_response.size() {
                                    debug!("longer register");
                                    // keep this new register
                                    valid_response = Some(response);
                                }
                            } else {
                                valid_response = Some(response);
                            }
                        }
                        QueryResponse::ReadRegister(Ok(ref register_set)) => {
                            debug!("okay _read_ register from {peer_address:?}");
                            // TODO: properly merge all registers
                            if let Some(QueryResponse::ReadRegister(Ok(prior_response))) =
                                &valid_response
                            {
                                if register_set.len() > prior_response.len() {
                                    debug!("longer register retrieved");
                                    // keep this new register
                                    valid_response = Some(response);
                                }
                            } else {
                                valid_response = Some(response);
                            }
                        }
                        QueryResponse::SpentProofShares(Ok(ref spentproof_set)) => {
                            debug!("okay _read_ spentproofs from {peer_address:?}");
                            // TODO: properly merge all registers
                            if let Some(QueryResponse::SpentProofShares(Ok(prior_response))) =
                                &valid_response
                            {
                                if spentproof_set.len() > prior_response.len() {
                                    // debug!("longer spentproof response retrieved");
                                    // keep this new register
                                    valid_response = Some(response);
                                }
                            } else {
                                valid_response = Some(response);
                            }
                        }
                        response => {
                            // we got a valid response
                            valid_response = Some(response)
                        }
                    }
                }
                _ => {
                    warn!("{msg_id:?} Non query response message returned {msg:?}. Ignoring it.");
                }
            }
        }

        // we've looped over all responses...
        // if any are valid, lets return it
        if let Some(response) = valid_response {
            debug!("valid response innnn!!! : {:?}", response);
            return Ok(QueryResult { response });
            // otherwise, if we've got an error in
            // we can return that too
        } else if let Some(response) = error_response {
            if discarded_responses > elders_len / 2 {
                return Ok(QueryResult { response });
            }
        }

        Err(Error::NoResponse {
            msg_id,
            peers: elders,
        })
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn make_contact_with_nodes(
        &mut self,
        nodes: Vec<Peer>,
        section_pk: bls::PublicKey,
        dst_address: XorName,
        auth: ClientAuth,
        payload: Bytes,
    ) -> Result<(), Error> {
        let endpoint = self.endpoint.clone();
        let msg_id = MsgId::new();

        debug!(
            "Making initial contact with nodes. Our PublicAddr: {:?}. Using {:?} to {} nodes: {:?}",
            endpoint.public_addr(),
            msg_id,
            nodes.len(),
            nodes
        );

        let dst = Dst {
            name: dst_address,
            section_key: section_pk,
        };
        let kind = MsgKind::Client(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        let initial_contacts = nodes
            .clone()
            .into_iter()
            .take(NODES_TO_CONTACT_PER_STARTUP_BATCH)
            .collect();

        let _responses = self
            .send_many_msgs_and_await_responses(initial_contacts, wire_msg.clone(), msg_id, false)
            .await?;

        let mut knowledge_checks = 0;
        let mut outgoing_msg_rounds = 1;
        let mut last_start_pos = 0;
        let mut tried_every_contact = false;

        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(1500),
            max_interval: Duration::from_secs(5),
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..Default::default()
        };

        // this seems needed for custom settings to take effect
        backoff.reset();

        // wait here to give a chance for AE responses to come in and be parsed
        tokio::time::sleep(Duration::from_secs(INITIAL_WAIT)).await;

        info!("Client startup... awaiting some network knowledge");

        let mut known_sap = self
            .network
            .read()
            .await
            .closest(&dst_address, None)
            .cloned();

        // wait until we have sufficient network knowledge
        while known_sap.is_none() {
            if tried_every_contact {
                return Err(Error::NetworkContact(nodes));
            }

            let stats = self.network.read().await.known_sections_count();
            debug!("Client still has not received a complete section's AE-Retry message... Current sections known: {:?}", stats);
            knowledge_checks += 1;

            // only after a couple of waits do we try contacting more nodes...
            // This just gives the initial contacts more time.
            if knowledge_checks > 2 {
                let mut start_pos = outgoing_msg_rounds * NODES_TO_CONTACT_PER_STARTUP_BATCH;
                outgoing_msg_rounds += 1;

                // if we'd run over known contacts, then we just go to the end
                if start_pos > nodes.len() {
                    start_pos = last_start_pos;
                }

                last_start_pos = start_pos;

                let next_batch_end = start_pos + NODES_TO_CONTACT_PER_STARTUP_BATCH;

                // if we'd run over known contacts, then we just go to the end
                let next_contacts = if next_batch_end > nodes.len() {
                    // but incase we _still_ dont know anything after this
                    let next = nodes[start_pos..].to_vec();
                    // mark as tried all
                    tried_every_contact = true;

                    next
                } else {
                    nodes[start_pos..start_pos + NODES_TO_CONTACT_PER_STARTUP_BATCH].to_vec()
                };

                trace!("Sending out another batch of initial contact msgs to new nodes");
                let _respones = self
                    .send_many_msgs_and_await_responses(
                        next_contacts,
                        wire_msg.clone(),
                        msg_id,
                        false,
                    )
                    .await?;

                let next_wait = backoff.next_backoff();
                trace!(
                    "Awaiting a duration of {:?} before trying new nodes",
                    next_wait
                );

                // wait here to give a chance for AE responses to come in and be parsed
                if let Some(wait) = next_wait {
                    tokio::time::sleep(wait).await;
                }

                known_sap = self
                    .network
                    .read()
                    .await
                    .closest(&dst_address, None)
                    .cloned();

                debug!("Known sap: {known_sap:?}");
            }
        }

        let stats = self.network.read().await.known_sections_count();
        debug!("Client has received updated network knowledge. Current sections known: {:?}. Sap for our startup-query: {:?}", stats, known_sap);

        Ok(())
    }

    /// Get DataSection elders details. Resort to own section if DataSection is not available.
    /// Takes a random subset (NUM_OF_ELDERS_SUBSET_FOR_QUERIES) of the avialable elders as targets
    pub(crate) async fn get_query_elders(
        &self,
        dst: XorName,
    ) -> Result<(bls::PublicKey, Vec<Peer>)> {
        let sap = self.network.read().await.closest(&dst, None).cloned();
        let (section_pk, mut elders) = if let Some(sap) = &sap {
            (sap.section_key(), sap.elders_vec())
        } else {
            return Err(Error::NoNetworkKnowledge(dst));
        };

        elders.shuffle(&mut OsRng);

        // We select the NUM_OF_ELDERS_SUBSET_FOR_QUERIES closest Elders we are querying
        let elders: Vec<_> = elders
            .into_iter()
            .take(NUM_OF_ELDERS_SUBSET_FOR_QUERIES)
            .collect();

        let elders_len = elders.len();
        if elders_len < NUM_OF_ELDERS_SUBSET_FOR_QUERIES && elders_len > 1 {
            return Err(Error::InsufficientElderConnections {
                connections: elders_len,
                required: NUM_OF_ELDERS_SUBSET_FOR_QUERIES,
            });
        }

        Ok((section_pk, elders))
    }

    async fn get_cmd_elders(&self, dst_address: XorName) -> Result<(bls::PublicKey, Vec<Peer>)> {
        let a_close_sap = self
            .network
            .read()
            .await
            .closest(&dst_address, None)
            .cloned();

        // Get DataSection elders details.
        if let Some(sap) = a_close_sap {
            let sap_elders = sap.elders_vec();
            let section_pk = sap.section_key();
            trace!("SAP elders found {:?}", sap_elders);

            // Supermajority of elders is expected.
            let targets_count = supermajority(sap_elders.len());

            // any SAP that does not hold elders_count() is indicative of a broken network (after genesis)
            if sap_elders.len() < targets_count {
                error!("Insufficient knowledge to send to address {:?}, elders for this section: {sap_elders:?} ({targets_count} needed), section PK is: {section_pk:?}", dst_address);
                return Err(Error::InsufficientElderKnowledge {
                    connections: sap_elders.len(),
                    required: targets_count,
                    section_pk,
                });
            }

            Ok((section_pk, sap_elders))
        } else {
            Err(Error::NoNetworkKnowledge(dst_address))
        }
    }

    #[instrument(skip_all, level = "trace")]
    /// All operations to the network return a response, either an ACK or a QueryResult.
    /// This sends a message to one node only
    pub(super) async fn send_msg_and_await_response(
        &mut self,
        peer: Peer,
        peer_index: usize,
        wire_msg: WireMsg,
        msg_id: MsgId,
        force_new_link: bool,
    ) -> Result<MsgType> {
        debug!("---> send msg {msg_id:?} going... will force new?: {force_new_link}");
        let bytes = wire_msg.serialize()?;

        let session = self.clone();
        let bytes = bytes.clone();

        let link = session
            .peer_links
            .get_or_create_link(&peer, force_new_link)
            .await;

        debug!("Trying to send msg to link {msg_id:?} to {peer:?}");
        let result = {
            match link.send_bi(bytes.clone(), msg_id).await {
                Ok(mut recv_stream) => {
                    debug!("That's {msg_id:?} sent to {peer:?}... spawning recieve listener");

                    let stream_id = recv_stream.id();
                    debug!("{msg_id:?}  Waiting for response msg on {stream_id} from {peer:?}");
                    let res = Self::read_msg_from_recvstream(&mut recv_stream).await;

                    // check for AE here...
                    if let Ok(MsgType::Node {
                        msg_id,
                        dst: _,
                        msg,
                    }) = res
                    {
                        trace!("{msg_id:?} System msg recieved...");
                        let result = self.handle_system_msg(msg, peer, peer_index).await?;

                        if let Some(res) = result {
                            trace!("{msg_id:?} Final response after handling system msg...");
                            return Ok(res);
                        } else {
                            return Err(Error::NoResponse {
                                msg_id,
                                peers: vec![peer],
                            });
                        }
                    }

                    // TODO: ???? once we drop the stream, do we know the connection is closed ???
                    trace!("{} to {}", LogMarker::StreamClosed, peer.addr());

                    res
                }
                #[cfg(features = "chaos")]
                Err(SendToOneError::ChaosNoConnection) => break Err(Error::ChoasSendFail),
                Err(error) => {
                    error!("Error sending {msg_id:?} bidi to {peer:?}: {error:?}");
                    Err(Error::FailedToInitateBiDiStream(msg_id))
                }
            }
        };

        if let Err(err) = &result {
            session.peer_links.remove_link_from_peer_links(&peer).await;
            warn!("Issue when sending {msg_id:?} to {peer:?}: {err:?}");
        }

        result
    }

    #[instrument(skip_all, level = "trace")]
    /// All operations to the network return a response, either an ACK or a QueryResult
    pub(super) async fn send_many_msgs_and_await_responses(
        &mut self,
        nodes: Vec<Peer>,
        wire_msg: WireMsg,
        msg_id: MsgId,
        force_new_link: bool,
    ) -> Result<Vec<(Peer, MsgType)>> {
        debug!("---> send msg {msg_id:?} going... will force new?: {force_new_link}");

        let mut tasks = vec![];
        let nodes_len = nodes.len();
        let mut last_error = None;
        let pub_addr = self.endpoint.public_addr();

        for (peer_index, peer) in nodes.iter().enumerate() {
            let mut session = self.clone();
            let wire_msg = wire_msg.clone();

            let task = async move {
                let msg = session
                    .send_msg_and_await_response(
                        *peer,
                        peer_index,
                        wire_msg,
                        msg_id,
                        force_new_link,
                    )
                    .await;

                (*peer, msg)
            };

            tasks.push(task)
        }

        // Let's await for all messages to be sent
        let results = join_all(tasks).await;

        let mut failures = nodes_len;
        // otherwise we can parse out the inner result now
        let mut final_msgs = vec![];

        results.into_iter().for_each(|(peer, result)| match result {
            Err(error) => last_error = Some(error),
            Ok(msg) => {
                failures -= 1;

                final_msgs.push((peer, msg));
            }
        });

        if failures > 0 {
            trace!(
                "Sending the message ({msg_id:?}) from {pub_addr} to {failures}/{nodes_len} of the \
                nodes failed: {nodes:?}"
            );

            if let Some(error) = last_error {
                warn!("The last error is: {error}");
                return Err(error);
            }
        }

        Ok(final_msgs)
    }

    async fn handle_system_msg(
        &mut self,
        msg: NodeMsg,
        src_peer: Peer,
        src_peer_index: usize,
    ) -> Result<Option<MsgType>, Error> {
        match msg {
            NodeMsg::AntiEntropy {
                section_tree_update,
                kind:
                    AntiEntropyKind::Redirect { bounced_msg } | AntiEntropyKind::Retry { bounced_msg },
            } => {
                debug!("AE-Redirect/Retry msg received");
                let result = self
                    .handle_ae_msg(section_tree_update, bounced_msg, src_peer, src_peer_index)
                    .await;
                if result.is_err() {
                    error!("Failed to handle AE msg from {src_peer:?}, {result:?}");
                }
                result
            }
            msg_type => {
                warn!("Unexpected msg type received: {msg_type:?}");
                Ok(None)
            }
        }
    }

    // Handle Anti-Entropy Redirect or Retry msgs
    #[instrument(skip_all, level = "debug")]
    #[async_recursion]
    async fn handle_ae_msg(
        &mut self,
        section_tree_update: SectionTreeUpdate,
        bounced_msg: UsrMsgBytes,
        src_peer: Peer,
        src_peer_index: usize,
    ) -> Result<Option<MsgType>, Error> {
        let target_sap = section_tree_update.signed_sap.value.clone();
        debug!("Received Anti-Entropy from {src_peer}, with SAP: {target_sap:?}");

        // Try to update our network knowledge first
        self.update_network_knowledge(section_tree_update, src_peer)
            .await;

        if let Some((msg_id, elders, service_msg, dst, auth)) =
            Self::new_target_elders(bounced_msg.clone(), &target_sap).await?
        {
            debug!("{msg_id:?} AE bounced msg going out again. Resending original message (sent to {src_peer:?}) to new section eldere");

            // The actual order of elders doesn't really matter. All that matters is we pass each AE response
            // we get through the same hoops, to then be able to ping a new elder on a 1-1 basis for the src_peer
            // we initially targetted.
            let deterministic_ordering = XorName::from_content(
                b"Arbitrary string that we use to sort new SAP elders consistently",
            );

            // here we send this to only one elder for each AE message we get in. We _should_ have one per elder we sent to.
            // deterministically sent to closest elder based upon the initial sender index
            let ordered_elders = elders
                .iter()
                .sorted_by(|lhs, rhs| deterministic_ordering.cmp_distance(&lhs.name(), &rhs.name()))
                .cloned()
                .collect_vec();

            let target_elder = ordered_elders.get(src_peer_index);

            // there should always be one
            if let Some(elder) = target_elder {
                let payload = WireMsg::serialize_msg_payload(&service_msg)?;
                let wire_msg =
                    WireMsg::new_msg(msg_id, payload, MsgKind::Client(auth.into_inner()), dst);

                debug!("Resending original message on AE-Redirect with updated details. Expecting an AE-Retry next");

                let response = self
                    .send_msg_and_await_response(*elder, src_peer_index, wire_msg, msg_id, false)
                    .await?;

                return Ok(Some(response));
            } else {
                return Err(Error::AntiEntropyNoSapElders);
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sn_interface::network_knowledge::{
        test_utils::{random_sap, section_signed},
        SectionTree,
    };

    use eyre::{eyre, Result};
    use qp2p::Config;
    use std::net::{Ipv4Addr, SocketAddr};
    use xor_name::Prefix;

    fn prefix(s: &str) -> Result<Prefix> {
        s.parse()
            .map_err(|err| eyre!("failed to parse Prefix '{}': {}", s, err))
    }

    fn new_network_network_contacts() -> (SectionTree, bls::SecretKey, bls::PublicKey) {
        let genesis_sk = bls::SecretKey::random();
        let genesis_pk = genesis_sk.public_key();

        let map = SectionTree::new(genesis_pk);

        (map, genesis_sk, genesis_pk)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cmd_sent_to_all_elders() -> Result<()> {
        let elders_len = 5;

        let prefix = prefix("0")?;
        let (section_auth, _, secret_key_set) = random_sap(prefix, elders_len, 0, None);
        let sap0 = section_signed(&secret_key_set.secret_key(), section_auth)?;
        let (mut network_contacts, _genesis_sk, _) = new_network_network_contacts();
        assert!(network_contacts.insert_without_chain(sap0));

        let session = Session::new(
            Config::default(),
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            network_contacts,
        )?;

        let mut rng = rand::thread_rng();
        let result = session.get_cmd_elders(XorName::random(&mut rng)).await?;
        assert_eq!(result.0, secret_key_set.public_keys().public_key());
        assert_eq!(result.1.len(), elders_len);

        Ok(())
    }
}
