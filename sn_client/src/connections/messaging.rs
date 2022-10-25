// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{MsgResponse, QueryResult, Session};

use crate::{Error, Result};

#[cfg(feature = "traceroute")]
use sn_interface::{
    messaging::{Entity, Traceroute},
    types::PublicKey,
};

use sn_interface::{
    messaging::{
        data::{DataQuery, DataQueryVariant, QueryResponse},
        AuthKind, ClientAuth, Dst, MsgId, WireMsg,
    },
    network_knowledge::supermajority,
    types::{ChunkAddress, Peer, SendToOneError},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use futures::future::join_all;
use qp2p::SendError;
use rand::{rngs::OsRng, seq::SliceRandom};
use std::{collections::BTreeSet, time::Duration};
use tokio::sync::mpsc;
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

        let auth = AuthKind::Client(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        // Don't immediately fail if sending to one elder fails. This could prevent further sends
        // and further responses coming in...
        // Failing directly here could cause us to miss a send success
        let (resp_tx, resp_rx) = mpsc::channel(elders_len);
        let send_msg_res = self
            .send_msg(elders.clone(), wire_msg, msg_id, force_new_link, resp_tx)
            .await;
        trace!("Cmd msg {:?} sent", msg_id);

        if let Err(err) = send_msg_res {
            trace!("Error when sending cmd msg out: {err:?}");
        }

        // We are not wait for the receive of majority of cmd Acks.
        // This could be further strict to wait for ALL the Acks get received.
        // The period is expected to have AE completed, hence no extra wait is required.
        self.we_have_sufficient_acks_for_cmd(msg_id, elders.clone(), resp_rx)
            .await
    }

    /// Checks for acks for a given msg.
    /// Returns Ok if we've sufficient to call this cmd a success
    async fn we_have_sufficient_acks_for_cmd(
        &self,
        msg_id: MsgId,
        elders: Vec<Peer>,
        mut resp_rx: mpsc::Receiver<MsgResponse>,
    ) -> Result<()> {
        let expected_acks = elders.len();
        let mut received_acks = BTreeSet::default();
        let mut received_errors = BTreeSet::default();

        while let Some(msg_resp) = resp_rx.recv().await {
            let (src, result) = match msg_resp {
                MsgResponse::CmdResponse(src, response) => (src, response.result().clone()),
                MsgResponse::QueryResponse(src, resp) => {
                    debug!("Ignoring unexpected query response received from {src:?} when awaiting a CmdAck: {resp:?}");
                    continue;
                }
            };
            match result {
                Ok(()) => {
                    let preexisting = !received_acks.insert(src) || received_errors.contains(&src);
                    debug!(
                        "ACK from {src:?} read from set for msg_id {msg_id:?} - preexisting??: {preexisting:?}",
                    );

                    if received_acks.len() >= expected_acks {
                        trace!("Good! We've at or above {expected_acks} expected_acks");
                        return Ok(());
                    }
                }
                Err(error) => {
                    let _ = received_errors.insert(src);
                    error!(
                        "received error {:?} of cmd {:?} from {:?}, so far {} respones and {} of them are errors",
                        error, msg_id, src, received_acks.len() + received_errors.len(), received_errors.len()
                    );

                    // exit if too many errors:
                    if received_errors.len() >= expected_acks {
                        error!(
                            "Received majority of error response for cmd {:?}: {:?}",
                            msg_id, error
                        );
                        return Err(Error::CmdError {
                            source: error,
                            msg_id,
                        });
                    }
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

        error!("Missing Responses from: {:?}", missing_responses);

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
        &self,
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
        let auth = AuthKind::Client(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        #[cfg(feature = "traceroute")]
        wire_msg.append_trace(&mut Traceroute(vec![Entity::Client(client_pk)]));

        let (resp_tx, resp_rx) = mpsc::channel(elders_len);
        let send_response = self
            .send_msg(elders.clone(), wire_msg, msg_id, force_new_link, resp_tx)
            .await;

        if send_response.is_err() {
            trace!("Error when sending query msg out: {send_response:?}");
        }

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
        self.check_query_responses(msg_id, elders.clone(), chunk_addr, resp_rx)
            .await
    }

    async fn check_query_responses(
        &self,
        msg_id: MsgId,
        elders: Vec<Peer>,
        chunk_addr: Option<ChunkAddress>,
        mut resp_rx: mpsc::Receiver<MsgResponse>,
    ) -> Result<QueryResult> {
        let mut discarded_responses: usize = 0;
        let mut error_response = None;
        let mut valid_response = None;
        let elders_len = elders.len();

        while let Some(msg_resp) = resp_rx.recv().await {
            let (peer_address, response) = match msg_resp {
                MsgResponse::QueryResponse(src, resp) => (src, resp),
                MsgResponse::CmdResponse(ack_src, _error) => {
                    debug!("Ignoring unexpected CmdAck response received from {ack_src:?} when awaiting a QueryResponse");
                    continue;
                }
            };

            // lets see if we have a positive response...
            debug!("response to {msg_id:?}: {:?}", response);

            match *response {
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
                        from {peer_address:?} (but may be overridden by a non-error response \
                        from another elder): {:#?}",
                        &response
                    );
                    error_response = Some(*response);
                    discarded_responses += 1;
                }

                QueryResponse::GetRegister(Ok(ref register)) => {
                    debug!("okay got register from {peer_address:?}");
                    // TODO: properly merge all registers
                    if let Some(QueryResponse::GetRegister(Ok(prior_response))) = &valid_response {
                        if register.size() > prior_response.size() {
                            debug!("longer register");
                            // keep this new register
                            valid_response = Some(*response);
                        }
                    } else {
                        valid_response = Some(*response);
                    }
                }
                QueryResponse::ReadRegister(Ok(ref register_set)) => {
                    debug!("okay _read_ register from {peer_address:?}");
                    // TODO: properly merge all registers
                    if let Some(QueryResponse::ReadRegister(Ok(prior_response))) = &valid_response {
                        if register_set.len() > prior_response.len() {
                            debug!("longer register retrieved");
                            // keep this new register
                            valid_response = Some(*response);
                        }
                    } else {
                        valid_response = Some(*response);
                    }
                }
                QueryResponse::SpentProofShares(Ok(ref spentproof_set)) => {
                    debug!("okay _read_ spentproofs from {peer_address:?}");
                    // TODO: properly merge all registers
                    if let Some(QueryResponse::SpentProofShares(Ok(prior_response))) =
                        &valid_response
                    {
                        if spentproof_set.len() > prior_response.len() {
                            debug!("longer spentproof response retrieved");
                            // keep this new register
                            valid_response = Some(*response);
                        }
                    } else {
                        valid_response = Some(*response);
                    }
                }
                response => {
                    // we got a valid response
                    valid_response = Some(response)
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

        Err(Error::NoResponse(elders))
    }

    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn make_contact_with_nodes(
        &self,
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
        let auth = AuthKind::Client(auth);
        let wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        let initial_contacts = nodes
            .clone()
            .into_iter()
            .take(NODES_TO_CONTACT_PER_STARTUP_BATCH)
            .collect();

        let (resp_tx, _rx) = mpsc::channel(nodes.len());
        self.send_msg(initial_contacts, wire_msg.clone(), msg_id, false, resp_tx)
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
                let (resp_tx, _rx) = mpsc::channel(next_contacts.len());
                self.send_msg(next_contacts, wire_msg.clone(), msg_id, false, resp_tx)
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
        mut force_new_link: bool,
        resp_tx: mpsc::Sender<MsgResponse>,
    ) -> Result<()> {
        debug!("---> send msg {msg_id:?} going... will force new?: {force_new_link}");
        let bytes = wire_msg.serialize()?;

        // Send message to all Elders concurrently
        let tasks = nodes
            .clone()
            .into_iter()
            .map(|peer| {
                let session = self.clone();
                let bytes = bytes.clone();
                let resp_tx_clone = resp_tx.clone();

                tokio::spawn(async move {
                    let mut link = session
                        .peer_links
                        .get_or_create_link(&peer, force_new_link)
                        .await;

                    let result = loop {
                        match link.send_bi(bytes.clone()).await {
                            Ok(recv_stream) => {
                                // let's spawn a task for each bi-stream to listen for responses
                                Self::spawn_recv_stream_listener(
                                    session.clone(),
                                    msg_id,
                                    peer,
                                    recv_stream,
                                    resp_tx_clone,
                                );
                                break Ok(());
                            }
                            #[cfg(features = "chaos")]
                            Err(SendToOneError::ChaosNoConnection) => {
                                break Err(Error::ChoasSendFail)
                            }
                            Err(SendToOneError::Connection(error)) => {
                                break Err(Error::QuicP2pConnection { peer, error });
                            }
                            Err(SendToOneError::Send(error @ SendError::ConnectionLost(_))) => {
                                warn!("Connection lost to {peer:?} (new link?: {force_new_link}): {error}");

                                // Unless we were forcing a new link to the peer up front, let's
                                // retry (only once) to reconnect to this peer and send the msg.
                                if force_new_link {
                                    break Err(Error::QuicP2pSend { peer, error });
                                } else {
                                    // let's retry once by forcing a new connection to this peer
                                    force_new_link = true;
                                    link = session
                                        .peer_links
                                        .get_or_create_link(&peer, force_new_link)
                                        .await;
                                    continue;
                                }
                            }
                            Err(SendToOneError::Send(error)) => {
                                break Err(Error::QuicP2pSend { peer, error });
                            }
                        }
                    };

                    if let Err(err) = &result {
                        session.peer_links.remove_link_from_peer_links(&peer).await;
                        warn!("Issue when sending {msg_id:?} to {peer:?}: {err:?}");
                    }

                    result
                })
            })
            .collect::<Vec<_>>();

        // Let's await for all messages to be sent
        let results = join_all(tasks).await;

        let nodes_len = nodes.len();
        let mut last_error = None;
        let mut failures = nodes_len;
        results.into_iter().for_each(|result| match result {
            Ok(Err(error)) => last_error = Some(error),
            Ok(Ok(())) => failures -= 1,
            Err(join_error) => warn!("Tokio join error as we send: {join_error:?}"),
        });

        if failures > 0 {
            trace!(
                "Sending the message ({msg_id:?}) from {} to {failures}/{nodes_len} of the \
                nodes failed: {nodes:?}",
                self.endpoint.public_addr(),
            );

            if let Some(error) = last_error {
                warn!("The last error is: {error}");
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
