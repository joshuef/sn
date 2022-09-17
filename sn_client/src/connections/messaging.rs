// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{QueryResult, Session};

use crate::{Error, Result};

#[cfg(feature = "traceroute")]
use sn_interface::{
    messaging::{Entity, Traceroute},
    types::PublicKey,
};

use sn_interface::{
    messaging::{
        data::{DataQuery, DataQueryVariant, OperationId, QueryResponse},
        AuthKind, Dst, MsgId, ServiceAuth, WireMsg,
    },
    network_knowledge::supermajority,
    types::{ChunkAddress, Peer, SendToOneError},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use futures::future::join_all;
use qp2p::{Close, ConnectionError, SendError};
use rand::{rngs::OsRng, seq::SliceRandom};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::task::JoinHandle;
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
        &self,
        dst_address: XorName,
        auth: ServiceAuth,
        payload: Bytes,
        msg_id: MsgId,
        force_new_link: bool,
        #[cfg(feature = "traceroute")] client_pk: PublicKey,
    ) -> Result<()> {
        let endpoint = self.endpoint.clone();
        // TODO: Consider other approach: Keep a session per section!
        let (section_pk, elders) = self.get_cmd_elders(dst_address).await?;

        let elders_len = elders.len();

        debug!(
            "Sending cmd w/id {msg_id:?}, from {}, to {elders_len} Elders w/ dst: {dst_address:?}",
            endpoint.public_addr(),
        );

        let dst = Dst {
            name: dst_address,
            section_key: section_pk,
        };

        let auth = AuthKind::Service(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        // Initial check incase we already have enough acks, we can end here
        if self
            .we_have_sufficient_acks_for_msg_id(msg_id, elders.clone())
            .await?
        {
            return Ok(());
        }

        // Don't immediately fail if sending to one elder fails. This could prevent further sends
        // and further responses coming in...
        // Failing directly here could cause us to miss a send success
        let send_msg_res = self
            .send_msg(elders.clone(), wire_msg, msg_id, force_new_link)
            .await;
        trace!("Cmd msg {:?} sent", msg_id);

        if send_msg_res.is_err() {
            trace!("Error when sending cmd msg out: {send_msg_res:?}");
        }

        // We are not wait for the receive of majority of cmd Acks.
        // This could be further strict to wait for ALL the Acks get received.
        // The period is expected to have AE completed, hence no extra wait is required.

        let mut ack_checks = 0;
        let max_ack_checks = 20;
        let interval = Duration::from_millis(50);

        loop {
            if self
                .we_have_sufficient_acks_for_msg_id(msg_id, elders.clone())
                .await?
            {
                return Ok(());
            }

            if ack_checks >= max_ack_checks {
                return Err(Error::InsufficientAcksReceived);
            }

            ack_checks += 1;

            trace!("{:?} current ack waiting loop count {}", msg_id, ack_checks,);
            tokio::time::sleep(interval).await;
        }
    }

    /// Checks self.pending_cmds for acks for a given msg id.
    /// Returns true if we've sufficient to call this cmd a success
    async fn we_have_sufficient_acks_for_msg_id(
        &self,
        msg_id: MsgId,
        elders: Vec<Peer>,
    ) -> Result<bool> {
        let mut received_responses_from = BTreeSet::default();
        let expected_acks = elders.len();

        if let Some(acks_we_have) = self.pending_cmds.get(&msg_id) {
            let acks = acks_we_have.value();

            let received_response_count = acks.len();

            let mut error_count = 0;
            let mut return_error = None;

            // track received errors
            for refmulti in acks.iter() {
                let (ack_src, error) = refmulti.key();
                if return_error.is_none() {
                    return_error = error.clone();
                }

                let _preexisting = received_responses_from.insert(*ack_src);

                if error.is_some() {
                    error!(
                        "received error response {:?} of cmd {:?} from {:?}, so far {} respones and {} of them are errors",
                        error, msg_id, ack_src, received_response_count, error_count
                    );
                    error_count += 1;
                }
            }

            // first exit if too many errors:
            if error_count >= expected_acks {
                error!(
                    "Received majority of error response for cmd {:?}: {:?}",
                    msg_id, return_error
                );
                // attempt to cleanup... though more acks may come in..
                let _ = self.pending_cmds.remove(&msg_id);

                if let Some(CmdError::Data(source)) = return_error {
                    return Err(Error::ErrorCmd { source, msg_id });
                }
            }

            let actual_ack_count = received_response_count - error_count;

            if actual_ack_count >= expected_acks {
                trace!("Good! We've at or above {expected_acks} expected_acks");

                return Ok(true);
            }

            let missing_responses: Vec<Peer> = elders
                .iter()
                .cloned()
                .filter(|p| !received_responses_from.contains(&p.addr()))
                .collect();

            warn!("Missing Responses from: {:?}", missing_responses);
            // return Err(Error::InsufficientAcksReceived);

            debug!("insufficient acks returned so far: {actual_ack_count}/{expected_acks}");
        }
        Ok(false)
    }

    #[instrument(
        skip(self, auth, payload, client_pk),
        level = "debug",
        name = "session send query"
    )]
    /// Send a `ServiceMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &self,
        query: DataQuery,
        auth: ServiceAuth,
        payload: Bytes,
        #[cfg(feature = "traceroute")] client_pk: PublicKey,
        dst_section_info: Option<(bls::PublicKey, Vec<Peer>)>,
        force_new_link: bool,
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
            "Sending query message {:?}, msg_id: {:?}, from {}, to the {} Elders closest to data name: {:?}",
            query,
            msg_id,
            endpoint.public_addr(),
            elders_len,
            elders
        );

        let operation_id = query
            .variant
            .operation_id()
            .map_err(|_| Error::UnknownOperationId)?;

        let dst = Dst {
            name: dst,
            section_key: section_pk,
        };
        let auth = AuthKind::Service(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        debug!("pre send");
        // Here we dont want to check before we resend... in case we're looking for an update
        //
        //
        // The important thing is not to fail due to one failed send, if we already havemsgs in.
        let send_response = self
            .send_msg(elders.clone(), wire_msg, msg_id, force_new_link)
            .await;

        if send_response.is_err() {
            trace!("Error when sending query msg out: {send_response:?}");
        }
        debug!("post send");

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
        // let mut discarded_responses: usize = 0;
        // let mut error_response = None;
        // let mut valid_response = None;

        let mut response_checks = 0;

        loop {
            debug!("looping send responses");
            if let Some(response) = self
                .check_query_responses(msg_id, operation_id, elders.clone(), chunk_addr)
                .await?
            {
                debug!("returning okkkkkkkkkkkkk");
                return Ok(response);
            }

            //stop mad looping
            tokio::time::sleep(Duration::from_millis(50)).await;

            if response_checks > 20 {
                return Err(Error::NoResponse(elders));
            }
            response_checks += 1;
        }

        // debug!(
        //     "Response obtained for query w/id {:?}: {:?}",
        //     msg_id, response
        // );

        // match response {
        //     Some(response) => {
        //         trace!(
        //             "Removing pending query map for {:?}",
        //             (msg_id, &operation_id)
        //         );
        //         let _prev = self.pending_queries.remove(&operation_id);
        //         Ok(QueryResult {
        //             response,
        //             operation_id,
        //         })
        //     }
        //     None => Err(Error::NoResponse(elders)),
        // }
    }

    async fn check_query_responses(
        &self,
        msg_id: MsgId,
        operation_id: OperationId,
        elders: Vec<Peer>,
        chunk_addr: Option<ChunkAddress>,
    ) -> Result<Option<QueryResult>> {
        let mut discarded_responses: usize = 0;
        let mut error_response = None;
        let mut valid_response = None;
        let elders_len = elders.len();

        if let Some(entry) = self.pending_queries.get(&operation_id) {
            let responses = entry.value();

            // lets see if we have a positive response...
            debug!("response so far: {:?}", responses);

            for refmulti in responses.iter() {
                let (_socket, response) = refmulti.key().clone();

                debug!("before matching response");
                match response {
                    QueryResponse::GetChunk(Ok(chunk)) => {
                        if let Some(chunk_addr) = chunk_addr {
                            // We are dealing with Chunk query responses, thus we validate its hash
                            // matches its xorname, if so, we don't need to await for more responses
                            debug!("Chunk QueryResponse received is: {:#?}", chunk);

                            if chunk_addr.name() == chunk.name() {
                                trace!("Valid Chunk received for {:?}", msg_id);
                                valid_response = Some(QueryResponse::GetChunk(Ok(chunk)));

                                // return Ok(Some(QueryResponse::GetChunk(Ok(chunk))));
                            } else {
                                // the Chunk content doesn't match its XorName,
                                // this is suspicious and it could be a byzantine node
                                warn!(
                                    "We received an invalid Chunk response from one of the nodes"
                                );
                                discarded_responses += 1;
                            }
                        }
                    }
                    QueryResponse::GetRegister((Err(_), _))
                    | QueryResponse::ReadRegister((Err(_), _))
                    | QueryResponse::GetRegisterPolicy((Err(_), _))
                    | QueryResponse::GetRegisterOwner((Err(_), _))
                    | QueryResponse::GetRegisterUserPermissions((Err(_), _))
                    | QueryResponse::GetChunk(Err(_)) => {
                        debug!("QueryResponse error received (but may be overridden by a non-error response from another elder): {:#?}", &response);
                        error_response = Some(response);
                        discarded_responses += 1;
                    }

                    QueryResponse::GetRegister((Ok(ref register), _)) => {
                        debug!("okay got register");
                        // TODO: properly merge all registers
                        if let Some(QueryResponse::GetRegister((Ok(prior_response), _))) =
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
                    QueryResponse::ReadRegister((Ok(ref register_set), _)) => {
                        debug!("okay _read_ register");
                        // TODO: properly merge all registers
                        if let Some(QueryResponse::ReadRegister((Ok(prior_response), _))) =
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
                    response => {
                        // we got a valid response
                        valid_response = Some(response)
                    }
                }
            }
        }

        if discarded_responses == elders_len {
            debug!("discarded equals elders");
            if let Some(response) = error_response {
                return Ok(Some(QueryResult {
                    response,
                    operation_id,
                }));
            }
            // return Ok(error_response);
        } else if let Some(response) = valid_response {
            debug!("valid response innnn!!! : {:?}", response);
            return Ok(Some(QueryResult {
                response,
                operation_id,
            }));
        }

        Ok(None)
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn make_contact_with_nodes(
        &self,
        nodes: Vec<Peer>,
        section_pk: bls::PublicKey,
        dst_address: XorName,
        auth: ServiceAuth,
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
        let auth = AuthKind::Service(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        let initial_contacts = nodes
            .clone()
            .into_iter()
            .take(NODES_TO_CONTACT_PER_STARTUP_BATCH)
            .collect();

        self.send_msg(initial_contacts, wire_msg.clone(), msg_id, false)
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
                self.send_msg(next_contacts, wire_msg.clone(), msg_id, false)
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
    pub(super) async fn send_msg(
        &self,
        nodes: Vec<Peer>,
        wire_msg: WireMsg,
        msg_id: MsgId,
        force_new_link: bool,
    ) -> Result<()> {
        let bytes = wire_msg.serialize()?;

        debug!("---> send msg going... will force new?: {force_new_link}");
        let mut last_error = None;
        // Send message to all Elders concurrently
        let mut tasks = Vec::default();

        let mut successful_sends = 0usize;

        for peer in nodes.clone() {
            let session = self.clone();
            let bytes = bytes.clone();
            let peer_name = peer.name();

            let task_handle: JoinHandle<(XorName, Result<()>)> = tokio::spawn(async move {
                let listen = |conn, incoming_msgs| {
                    Self::spawn_msg_listener_thread(session.clone(), peer, conn, incoming_msgs);
                };

                let session_clone = session.clone();

                let link = session_clone
                    .peer_links
                    .get_or_create_link(&peer, force_new_link)
                    .await;

                let result = match link.send(bytes.clone(), listen).await {
                    Ok(()) => Ok(()),
                    Err(SendToOneError::Connection(err)) => {
                        Err(Error::QuicP2pConnection { peer, error: err })
                    }
                    Err(SendToOneError::Send(err)) => Err(Error::QuicP2pSend { peer, error: err }),
                    #[cfg(features = "chaos")]
                    Err(SendToOneError::ChaosNoConnection) => Err(Error::ChoasSendFail),
                };

                (peer_name, result)
            });

            tasks.push(task_handle);
        }

        // Let's await for all messages to be sent
        let results = join_all(tasks).await;

        for r in results {
            match r {
                Ok((peer_name, send_result)) => match send_result {
                    Err(Error::QuicP2pSend {
                        peer,
                        error:
                            SendError::ConnectionLost(ConnectionError::Closed(Close::Application {
                                reason,
                                error_code,
                            })),
                    }) => {
                        warn!(
                            "Connection was closed by node {}, reason: {:?}",
                            peer_name,
                            String::from_utf8(reason.to_vec())
                        );
                        last_error = Some(Error::QuicP2pSend {
                            peer,
                            error: SendError::ConnectionLost(ConnectionError::Closed(
                                Close::Application { reason, error_code },
                            )),
                        });
                        self.peer_links.remove(&peer).await;
                    }
                    Err(Error::QuicP2pSend {
                        peer,
                        error: SendError::ConnectionLost(error),
                    }) => {
                        warn!("Connection to {} was lost: {:?}", peer_name, error);
                        last_error = Some(Error::QuicP2pSend {
                            peer,
                            error: SendError::ConnectionLost(error),
                        });

                        self.peer_links.remove(&peer).await;
                    }
                    Err(error) => {
                        warn!(
                            "Issue during {:?} send to {}: {:?}",
                            msg_id, peer_name, error
                        );
                        last_error = Some(error);
                        if let Some(peer) = self.peer_links.get_peer_by_name(&peer_name).await {
                            self.peer_links.remove(&peer).await;
                        }
                    }
                    Ok(_) => successful_sends += 1,
                },
                Err(join_error) => {
                    warn!("Tokio join error as we send: {:?}", join_error)
                }
            }
        }

        let failures = nodes.len() - successful_sends;

        if failures > 0 {
            trace!(
                "Sending the message ({:?}) from {} to {}/{} of the nodes failed: {:?}",
                msg_id,
                self.endpoint.public_addr(),
                failures,
                nodes.len(),
                nodes,
            );

            if let Some(error) = last_error {
                warn!("The relevant error is: {error}");
                return Err(error);
            }
        }

        Ok(())
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
        let sap0 = section_signed(secret_key_set.secret_key(), section_auth)?;
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
