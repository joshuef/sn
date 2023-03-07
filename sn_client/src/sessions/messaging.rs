// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{MsgResponse, Session};
use crate::{Error, Result};

use sn_interface::{
    messaging::{
        data::{DataQuery, QueryResponse},
        ClientAuth, Dst, MsgId, MsgKind, WireMsg,
    },
    network_knowledge::supermajority,
    types::{ChunkAddress, NodeId},
};

use bytes::Bytes;
use rand::{rngs::OsRng, seq::SliceRandom};
use std::collections::BTreeSet;
use tokio::task::JoinSet;
use tracing::{debug, error, trace, warn};
use xor_name::XorName;

// Number of Elders subset to send queries to
#[cfg(not(feature = "query-happy-path"))]
pub(crate) const NUM_OF_ELDERS_SUBSET_FOR_QUERIES: usize = 3;
#[cfg(feature = "query-happy-path")]
pub(crate) const NUM_OF_ELDERS_SUBSET_FOR_QUERIES: usize = 1;

impl Session {
    #[instrument(skip(self), level = "debug", name = "session setup conns")]
    /// Make a best effort to pre connect to only relevant nodes for a set of dst addresses
    /// This should reduce the number of connections attempts to the same elder set
    pub(crate) async fn setup_connections_to_relevant_nodes(
        &self,
        dst_addresses: Vec<XorName>,
    ) -> Result<()> {
        let mut relevant_elders = BTreeSet::new();
        // TODO: get relevant nodes
        for address in dst_addresses {
            let (_, elders) = self.get_cmd_elders(address).await?;
            for elder in elders {
                let _existed = relevant_elders.insert(elder);
            }
        }

        let mut tasks = vec![];
        for elder in relevant_elders {
            let session = self.clone();

            let task = async move {
                let connect_now = true;
                // We don't retry here.. if we fail it will be retried on a per message basis
                let _ = session
                    .node_links
                    .get_or_create_link(&elder, connect_now, None)
                    .await;
            };
            tasks.push(task);
        }

        let _ = futures::future::join_all(tasks).await;

        Ok(())
    }

    #[instrument(skip(self, auth, payload), level = "debug", name = "session send cmd")]
    pub(crate) async fn send_cmd(
        &self,
        dst_address: XorName,
        auth: ClientAuth,
        payload: Bytes,
        is_spend: bool,
        msg_id: MsgId,
    ) -> Result<()> {
        let endpoint = self.endpoint.clone();
        // TODO: Consider other approach: Keep a session per section!
        let (section_pk, elders) = self.get_cmd_elders(dst_address).await?;

        let elders_len = elders.len();
        debug!(
            "Sending cmd with {msg_id:?}, dst: {dst_address:?}, from {}, \
            to {elders_len} Elders: {elders:?}",
            endpoint.local_addr(),
        );

        let dst = Dst {
            name: dst_address,
            section_key: section_pk,
        };

        let kind = MsgKind::Client {
            auth,
            is_spend,
            query_index: None,
        };
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        let log_line = |elders_len_s: String| {
            debug!(
                "Sending cmd w/id {msg_id:?}, from {}, to {elders_len_s} w/ dst: {dst_address:?}",
                endpoint.local_addr(),
            )
        };

        if is_spend {
            log_line(format!("{elders_len}"));
            self.send_msg_and_check_acks(msg_id, elders.clone(), wire_msg)
                .await
        } else {
            #[cfg(feature = "cmd-happy-path")]
            {
                log_line(format!("1 Elder (or at most {elders_len})"));
                self.send_to_one_or_more(dst_address, elders.clone(), wire_msg)
                    .await
            }
            #[cfg(not(feature = "cmd-happy-path"))]
            {
                log_line(format!("{elders_len}"));
                self.send_msg_and_check_acks(msg_id, elders.clone(), wire_msg)
                    .await
            }
        }
    }

    async fn send_msg_and_check_acks(
        &self,
        msg_id: MsgId,
        elders: Vec<NodeId>,
        wire_msg: WireMsg,
    ) -> Result<()> {
        let send_cmd_tasks = self.send_msg(elders.clone(), wire_msg).await?;
        trace!("Cmd msg {msg_id:?} sent");
        // On non-happy-path, we currently expect all elders to ack.
        let expected_acks = elders.len();
        // We wait for ALL the expected acks get received.
        // The AE messages are handled by the tasks, hence no extra wait is required.
        match self
            .we_have_sufficient_acks_for_cmd(msg_id, elders, expected_acks, send_cmd_tasks)
            .await
        {
            Ok(()) => {
                trace!("Acks of Cmd {:?} received", msg_id);
                Ok(())
            }
            error => error,
        }
    }

    /// This function will try a happy path,
    /// successively expanding to all the other elders in case of failure.
    ///
    /// 1st attempt: Closest Elder (take 1) (take index 0)
    /// 2nd attempt: Next closest (skip 1, take 1) (skip idx 0, take idx 1)
    /// 3rd attempt: Next 2 closest (skip 2, take 2) (skip idx 0 and 1, take idx 2 and 3)
    /// 4th attempt: Next 3 closest (skip 4, take 3) (skip idx 0-3, take index 4, 5 and 6)
    #[cfg(feature = "cmd-happy-path")]
    async fn send_to_one_or_more(
        &self,
        target: XorName,
        all_elders: Vec<NodeId>,
        wire_msg: WireMsg,
    ) -> Result<()> {
        let msg_id = wire_msg.msg_id();
        // On happy path, we only require 1 ack.
        let expected_acks = 1;

        // this will do at most 4 attempts, eventually calling all 7 elders
        for skip in 0..3 {
            let take = if skip == 0 {
                1
            } else if skip == 3 {
                4
            } else {
                skip
            };

            let elders = self.pick_elders(target, all_elders.clone(), skip, take);

            trace!("Sending cmd {msg_id:?}, skipping {skip}, sending to {take} elders..");
            let send_cmd_tasks = self.send_msg(elders.clone(), wire_msg.clone()).await?;

            // We only require one ack, we wait it to get received.
            // Any AE message is handled by the tasks, hence no extra wait is required.
            if self
                .we_have_sufficient_acks_for_cmd(msg_id, elders, expected_acks, send_cmd_tasks)
                .await
                .is_ok()
            {
                trace!("Acks of Cmd {:?} received", msg_id);
                return Ok(());
            }
        }

        // we expected at least one ack, but got 0
        Err(Error::InsufficientAcksReceived {
            msg_id,
            expected: 1,
            received: 0,
        })
    }

    #[cfg(feature = "cmd-happy-path")]
    fn pick_elders(
        &self,
        target: XorName,
        elders: Vec<NodeId>,
        skip: usize,
        take: usize,
    ) -> Vec<NodeId> {
        use itertools::Itertools;
        elders
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .skip(skip)
            .take(take)
            .collect()
    }

    /// Checks for acks for a given msg.
    /// Returns Ok if we've sufficient to call this cmd a success
    async fn we_have_sufficient_acks_for_cmd(
        &self,
        msg_id: MsgId,
        elders: Vec<NodeId>,
        expected_acks: usize,
        mut send_cmd_tasks: JoinSet<MsgResponse>,
    ) -> Result<()> {
        debug!("----> Init of check for acks for {msg_id:?}");
        let mut received_acks = BTreeSet::default();
        let mut received_errors = BTreeSet::default();
        let mut failures = BTreeSet::default();

        while let Some(msg_resp) = send_cmd_tasks.join_next().await {
            debug!("Handling msg_resp sent to ack wait channel: {msg_resp:?}");
            let (src, result) = match msg_resp {
                Ok(MsgResponse::CmdResponse(src, response)) => (src, response.result().clone()),
                Ok(MsgResponse::QueryResponse(src, resp)) => {
                    debug!("Unexpected query response received from {src:?} for {msg_id:?} when awaiting a CmdAck: {resp:?}");
                    let _ = received_errors.insert(src);
                    continue;
                }
                Ok(MsgResponse::Failure(src, error)) => {
                    debug!("Failure occurred with msg {msg_id:?} from {src:?}: {error:?}");
                    let _ = failures.insert(src);
                    continue;
                }
                Err(join_err) => {
                    warn!("Join failure occurred with msg {msg_id:?}: {join_err:?}");
                    continue;
                }
            };

            match result {
                Ok(()) => {
                    let preexisting = !received_acks.insert(src) || received_errors.contains(&src);
                    if preexisting {
                        warn!("ACK from {src:?} for {msg_id:?} was received more than once");
                    }

                    if received_acks.len() >= expected_acks {
                        trace!("{msg_id:?} Good! We're at or above {expected_acks} expected_acks");
                        return Ok(());
                    }
                }
                Err(error) => {
                    let _ = received_errors.insert(src);
                    error!(
                        "Received error {error:?} of cmd {msg_id:?} from {src:?}, so far {} respones and {} of them are errors",
                        received_acks.len() + received_errors.len(), received_errors.len()
                    );

                    // exit if too many errors:
                    if failures.len() + received_errors.len() >= expected_acks {
                        error!("Received majority of error response for cmd {msg_id:?}: {error:?}");
                        return Err(Error::CmdError {
                            source: error,
                            msg_id,
                        });
                    }
                }
            }
        }

        debug!("ACKs for {msg_id:?} received from: {received_acks:?}");
        debug!("CmdErrors for {msg_id:?} received from: {received_errors:?}");
        debug!("Failures for {msg_id:?} with: {failures:?}");

        let missing_responses: Vec<NodeId> = elders
            .iter()
            .cloned()
            .filter(|p| {
                let addr = &p.addr();
                !received_acks.contains(addr)
                    && !received_errors.contains(addr)
                    && !failures.contains(addr)
            })
            .collect();

        debug!(
            "Insufficient CmdAcks returned for {msg_id:?}: {}/{expected_acks}. \
            Missing Responses from: {missing_responses:?}",
            received_acks.len()
        );
        Err(Error::InsufficientAcksReceived {
            msg_id,
            expected: expected_acks,
            received: received_acks.len(),
        })
    }

    #[instrument(
        skip(self, auth, payload),
        level = "debug",
        name = "session send query"
    )]
    #[allow(clippy::too_many_arguments)]
    /// Send a `ClientMsg` to the network awaiting for the response.
    pub(crate) async fn send_query(
        &self,
        query: DataQuery,
        query_node_index: usize,
        auth: ClientAuth,
        payload: Bytes,
        dst_section_info: Option<(bls::PublicKey, Vec<NodeId>)>,
    ) -> Result<QueryResponse> {
        let endpoint = self.endpoint.clone();

        let chunk_addr = if let DataQuery::GetChunk(address) = query {
            Some(address)
        } else {
            None
        };

        let dst = query.dst_name();

        let (section_pk, elders) = if let Some(section_info) = dst_section_info {
            section_info
        } else {
            self.get_query_elders(dst).await?
        };

        let elders_len = elders.len();
        let msg_id = MsgId::new();

        debug!(
            "Sending query message {msg_id:?}, from {}, {query:?} to \
            the {elders_len} Elders closest to data name: {elders:?}",
            endpoint.local_addr(),
        );

        let dst = Dst {
            name: dst,
            section_key: section_pk,
        };
        let kind = MsgKind::Client {
            auth,
            is_spend: false,
            query_index: Some(query_node_index),
        };
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        let send_query_tasks = self.send_msg(elders.clone(), wire_msg).await?;

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
        self.check_query_responses(msg_id, elders.clone(), chunk_addr, send_query_tasks)
            .await
    }

    async fn check_query_responses(
        &self,
        msg_id: MsgId,
        elders: Vec<NodeId>,
        chunk_addr: Option<ChunkAddress>,
        mut send_query_tasks: JoinSet<MsgResponse>,
    ) -> Result<QueryResponse> {
        let mut discarded_responses: usize = 0;
        let mut error_response = None;
        let mut last_error_response = None;
        let mut valid_response = None;
        let elders_len = elders.len();

        while let Some(msg_resp) = send_query_tasks.join_next().await {
            let (node_address, response) = match msg_resp {
                Ok(MsgResponse::QueryResponse(src, resp)) => (src, resp),
                Ok(MsgResponse::CmdResponse(src, resp)) => {
                    debug!("Unexpected cmd response received from {src:?} for {msg_id:?} when awaiting a QueryResponse: {resp:?}");
                    discarded_responses += 1;
                    continue;
                }
                Ok(MsgResponse::Failure(src, error)) => {
                    debug!("Failure occurred with msg {msg_id:?} from {src:?}: {error:?}");
                    last_error_response = Some(error);
                    discarded_responses += 1;
                    continue;
                }
                Err(join_err) => {
                    warn!("Join failure occurred with msg {msg_id:?}: {join_err:?}");
                    continue;
                }
            };

            // let's see if we have a positive response...
            debug!("Response to {msg_id:?}: {response:?}");

            match *response {
                QueryResponse::GetChunk(Ok(chunk)) => {
                    if let Some(chunk_addr) = chunk_addr {
                        // We are dealing with Chunk query responses, thus we validate its hash
                        // matches its xorname, if so, we don't need to await for more responses
                        debug!("Chunk QueryResponse received is: {chunk:#?}");

                        if chunk_addr.name() == chunk.name() {
                            trace!("Valid Chunk received for {msg_id:?}");
                            valid_response = Some(QueryResponse::GetChunk(Ok(chunk)));
                            break;
                        } else {
                            // the Chunk content doesn't match its XorName,
                            // this is suspicious and it could be a byzantine node
                            warn!("We received an invalid Chunk response from one of the nodes for {msg_id:?}");
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
                        from {node_address:?} (but may be overridden by a non-error response \
                        from another elder): {:#?}",
                        &response
                    );
                    error_response = Some(*response);
                    discarded_responses += 1;
                }
                QueryResponse::GetRegister(Ok(ref register)) => {
                    debug!("okay got register from {node_address:?}");
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
                QueryResponse::ReadRegister(Ok(_)) => {
                    debug!("okay _read_ register from {node_address:?}");
                    if valid_response.is_none() {
                        valid_response = Some(*response);
                    }
                }
                QueryResponse::SpentProofShares(Ok(ref spentproof_set)) => {
                    debug!("okay _read_ spentproofs from {node_address:?}");
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
            debug!("Valid response in!!!: {response:?}");
            return Ok(response);
            // otherwise, if we've got an error in
            // we can return that too
        } else if let Some(response) = error_response {
            if discarded_responses > elders_len / 2 {
                return Ok(response);
            }
        }

        if let Some(error) = last_error_response {
            Err(error)
        } else {
            Err(Error::NoResponse {
                msg_id,
                nodes: elders,
            })
        }
    }

    /// Get DataSection elders details. Resort to own section if DataSection is not available.
    /// Takes a random subset (NUM_OF_ELDERS_SUBSET_FOR_QUERIES) of the avialable elders as targets
    pub(crate) async fn get_query_elders(
        &self,
        dst: XorName,
    ) -> Result<(bls::PublicKey, Vec<NodeId>)> {
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

    async fn get_cmd_elders(&self, dst_address: XorName) -> Result<(bls::PublicKey, Vec<NodeId>)> {
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
            trace!("SAP elders found {sap_elders:?}");

            // Supermajority of elders is expected.
            let targets_count = supermajority(sap_elders.len());

            // any SAP that does not hold elders_count() is indicative of a broken network (after genesis)
            if sap_elders.len() < targets_count {
                error!(
                    "Insufficient knowledge to send to address {dst_address:?}, \
                    elders for this section: {sap_elders:?} ({targets_count} needed), \
                    section PK is: {section_pk:?}"
                );
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
        nodes: Vec<NodeId>,
        wire_msg: WireMsg,
    ) -> Result<JoinSet<MsgResponse>> {
        let msg_id = wire_msg.msg_id();
        debug!("---> Send msg {msg_id:?} going out.");
        let bytes = wire_msg.serialize()?;

        let mut tasks = JoinSet::new();

        for (node_index, node_id) in nodes.into_iter().enumerate() {
            let session = self.clone();
            let bytes = bytes.clone();

            let _abort_handle = tasks.spawn(async move {
                let mut connect_now = false;
                debug!("Trying to send msg {msg_id:?} to {node_id:?}");
                loop {
                    let link = session
                    .node_links
                    .get_or_create_link(&node_id, connect_now, Some(msg_id))
                    .await;
                    debug!("Link ready to send {msg_id:?} to {node_id:?}");
                    match link.send_bi(bytes.clone(), msg_id).await {
                        Ok(recv_stream) => {
                            debug!(
                                "That's {msg_id:?} sent to {node_id:?}... starting receive listener"
                            );
                            // let's listen for responses on the bi-stream
                            break session
                                .recv_stream_listener(msg_id, node_id, node_index, recv_stream)
                                .await;
                        }
                        Err(error) if !connect_now => {
                            // Let's retry (only once) to reconnect to this node and send the msg.
                            error!(
                                "Failed to send {msg_id:?} to {node_id:?} on a new \
                                bi-stream: {error:?}. Creating a new connection to retry once ..."
                            );
                            session.node_links.remove(&node_id).await;
                            connect_now = true;
                            continue;
                        }
                        Err(error) => {
                            error!("Error sending {msg_id:?} bidi to {node_id:?}: {error:?}");
                            session.node_links.remove(&node_id).await;
                            break MsgResponse::Failure(
                                node_id.addr(),
                                Error::FailedToInitateBiDiStream { msg_id, error },
                            );
                        }
                    }
                }
            });
        }

        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sn_interface::{
        network_knowledge::SectionTree,
        test_utils::{prefix, TestKeys, TestSapBuilder},
    };

    use eyre::Result;
    use std::net::{Ipv4Addr, SocketAddr};
    use xor_name::Prefix;

    fn new_network_network_contacts() -> (SectionTree, bls::SecretKey, bls::PublicKey) {
        let (genesis_sap, genesis_sk_set, ..) = TestSapBuilder::new(Prefix::default()).build();

        let genesis_sk = genesis_sk_set.secret_key();
        let genesis_pk = genesis_sk.public_key();
        let genesis_sap = TestKeys::get_section_signed(&genesis_sk, genesis_sap)
            .expect("Failed to sign genesis_key");
        let tree = SectionTree::new(genesis_sap).expect("SAP belongs to the genesis prefix");

        (tree, genesis_sk, genesis_pk)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cmd_sent_to_all_elders() -> Result<()> {
        let elders_len = 5;

        let prefix = prefix("0");
        let (sap, secret_key_set, ..) = TestSapBuilder::new(prefix).elder_count(elders_len).build();
        let sap0 = TestKeys::get_section_signed(&secret_key_set.secret_key(), sap)?;
        let (mut network_contacts, _genesis_sk, _) = new_network_network_contacts();
        assert!(network_contacts.insert_without_chain(sap0));

        let session = Session::new(
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
