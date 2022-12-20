// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::dysfunction::DysCmds, Cmd, Error, MyNode, Prefix, Result,
};

use sn_dysfunction::IssueType;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{ClientDataResponse, DataCmd, DataQuery, MetadataExchange, StorageLevel},
        system::{NodeDataCmd, NodeDataQuery, NodeDataResponse, NodeMsg, OperationId},
        AuthorityProof, ClientAuth, Dst, MsgId, MsgKind, MsgType, WireMsg,
    },
    types::{log_markers::LogMarker, Peer, PublicKey, ReplicatedData},
};

use qp2p::{SendStream, UsrMsgBytes};

use bytes::Bytes;
use futures::FutureExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{collections::BTreeSet, env::var, str::FromStr, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{timeout, Duration},
};
use tracing::info;
use xor_name::XorName;

/// Environment variable to set timeout value (in seconds) for data queries
/// forwarded to Adults. Default value (`ADULT_RESPONSE_DEFAULT_TIMEOUT`) is otherwise used.
const ENV_ADULT_RESPONSE_TIMEOUT: &str = "SN_ADULT_RESPONSE_TIMEOUT";

// Default timeout period set for data queries forwarded to Adult.
// TODO: how to determine this time properly?
const ADULT_RESPONSE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(70);

lazy_static! {
    static ref ADULT_RESPONSE_TIMEOUT: Duration = match var(ENV_ADULT_RESPONSE_TIMEOUT)
        .map(|v| u64::from_str(&v))
    {
        Ok(Ok(secs)) => {
            let timeout = Duration::from_secs(secs);
            info!("{ENV_ADULT_RESPONSE_TIMEOUT} env var set, Adult query response timeout set to {timeout:?}");
            timeout
        }
        Ok(Err(err)) => {
            warn!(
                "Failed to parse {ENV_ADULT_RESPONSE_TIMEOUT} value, using \
                default value ({ADULT_RESPONSE_DEFAULT_TIMEOUT:?}): {err:?}"
            );
            ADULT_RESPONSE_DEFAULT_TIMEOUT
        }
        Err(_) => ADULT_RESPONSE_DEFAULT_TIMEOUT,
    };
}

impl MyNode {
    // Locate ideal holders for this data, instruct them to store the data
    pub(crate) async fn replicate_data_to_adults(
        snapshot: &NodeContext,
        data: ReplicatedData,
        msg_id: MsgId,
        targets: BTreeSet<Peer>,
    ) -> Result<Vec<(Peer, Result<WireMsg>)>> {
        info!(
            "Replicating data from {msg_id:?} {:?} to holders {:?}",
            data.name(),
            &targets,
        );

        // TODO: general ReplicateData flow could go bidi?
        // Right now we've a new msg for just one datum.
        // Atm that's perhaps more bother than its worth..
        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateOneData(data));
        let mut send_tasks = vec![];

        let (kind, payload) = MyNode::serialize_node_msg(snapshot.name, msg)?;
        let section_key = snapshot.network_knowledge.section_key();

        // drop the read lock before we do anything async

        for target in targets {
            let bytes_to_adult = MyNode::form_usr_msg_bytes_to_node(
                section_key,
                payload.clone(),
                kind.clone(),
                Some(target),
                msg_id,
            )?;

            let comm = snapshot.comm.clone();
            info!("About to send {msg_id:?} to holder: {:?}", &target);

            send_tasks.push(
                async move {
                    (
                        target,
                        comm.send_out_bytes_to_peer_and_return_response(
                            target,
                            msg_id,
                            bytes_to_adult.clone(),
                        )
                        .await,
                    )
                }
                .boxed(),
            );
        }

        Ok(futures::future::join_all(send_tasks).await)
    }

    // Locate ideal holders for this data, instruct them to store the data
    pub(crate) async fn replicate_data_to_adults_and_ack_to_client(
        snapshot: &NodeContext,
        cmd: DataCmd,
        data: ReplicatedData,
        msg_id: MsgId,
        targets: BTreeSet<Peer>,
        client_response_stream: Arc<Mutex<SendStream>>,
    ) -> Result<()> {
        let targets_len = targets.len();

        let responses = MyNode::replicate_data_to_adults(snapshot, data, msg_id, targets).await?;
        let mut success_count = 0;
        let mut ack_response = None;
        let mut last_error = None;
        for (peer, the_response) in responses {
            match the_response {
                Ok(response) => {
                    success_count += 1;
                    debug!("Response in from {peer:?} for {msg_id:?} {response:?}");
                    ack_response = Some(response);
                }
                Err(error) => {
                    error!("{msg_id:?} Error when replicating to adult {peer:?}: {error:?}");
                    last_error = Some(error);
                }
            }
        }

        // everything went fine, tell the client that
        if success_count == targets_len {
            if let Some(response) = ack_response {
                MyNode::respond_to_client_on_stream(
                    snapshot,
                    response,
                    client_response_stream.clone(),
                )
                .await?;
            } else {
                // This should not be possible with above checks
                error!("No valid response to send from all responses for {msg_id:?}")
            }
        } else {
            error!("Storage was not completely successful for {msg_id:?}");

            if let Some(error) = last_error {
                MyNode::send_cmd_error_response_over_stream(
                    snapshot,
                    cmd,
                    error,
                    msg_id,
                    client_response_stream,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Parses WireMsg and if DataStored Ack, we send a response to the client
    async fn respond_to_client_on_stream(
        snapshot: &NodeContext,
        response: WireMsg,
        send_stream: Arc<Mutex<SendStream>>,
    ) -> Result<()> {
        if let MsgType::NodeDataResponse {
            msg:
                NodeDataResponse::CmdResponse {
                    response,
                    correlation_id,
                },
            ..
        } = response.into_msg()?
        {
            let client_msg = ClientDataResponse::CmdResponse {
                response,
                correlation_id,
            };

            let (kind, payload) = MyNode::serialize_client_msg_response(snapshot.name, client_msg)?;

            debug!("{correlation_id:?} sending cmd response ack back to client");
            MyNode::send_msg_on_stream(
                snapshot.network_knowledge.section_key(),
                payload,
                kind,
                send_stream,
                None, // we shouldn't need this...
                correlation_id,
            )
            .await
        } else {
            error!("Unexpected response to data cmd from node. Response: {response:?}");
            // TODO: handle this bad response
            Ok(())
        }
    }

    /// Find target adult, sends a bidi msg, awaiting response, and then sends this on to the client
    pub(crate) async fn read_data_from_adult_and_respond_to_client(
        snapshot: NodeContext,
        query: DataQuery,
        msg_id: MsgId,
        auth: AuthorityProof<ClientAuth>,
        source_client: Peer,
        client_response_stream: Arc<Mutex<SendStream>>,
    ) -> Result<Vec<Cmd>> {
        // We generate the operation id to track the response from the Adult
        // by using the query msg id, which shall be unique per query.
        let operation_id = OperationId::from(&Bytes::copy_from_slice(msg_id.as_ref()));
        let address = query.variant.address();
        trace!(
            "{:?} preparing to query adults for data at {:?} with op_id: {:?}",
            LogMarker::DataQueryReceviedAtElder,
            address,
            operation_id
        );

        let targets = MyNode::target_data_holders(&snapshot, *address.name());

        // Query only the nth adult
        let target = if let Some(peer) = targets.iter().nth(query.adult_index) {
            *peer
        } else {
            debug!("No targets found for {msg_id:?}");
            let error = Error::InsufficientAdults {
                prefix: snapshot.network_knowledge.prefix(),
                expected: query.adult_index as u8 + 1,
                found: targets.len() as u8,
            };

            MyNode::send_query_error_response_on_stream(
                snapshot,
                error,
                &query.variant,
                source_client,
                msg_id,
                client_response_stream,
            )
            .await?;
            // TODO: do error processing
            return Ok(vec![]);
        };

        // Form a msg to our adult
        let msg = NodeMsg::NodeDataQuery(NodeDataQuery {
            query: query.variant,
            auth: auth.into_inner(),
            operation_id,
        });

        let (kind, payload) = MyNode::serialize_node_msg(snapshot.name, msg)?;

        let comm = snapshot.comm.clone();

        let bytes_to_adult = MyNode::form_usr_msg_bytes_to_node(
            snapshot.network_knowledge.section_key(),
            payload,
            kind,
            Some(target),
            msg_id,
        )?;

        debug!("Sending out {msg_id:?} to Adult {target:?}");
        let response = match timeout(*ADULT_RESPONSE_TIMEOUT, async {
            comm.send_out_bytes_to_peer_and_return_response(target, msg_id, bytes_to_adult)
                .await
        })
        .await
        {
            Ok(resp) => resp,
            Err(_elapsed) => {
                error!(
                    "{msg_id:?}: No response from {target:?} after {:?} timeout. \
                    Marking adult as dysfunctional",
                    *ADULT_RESPONSE_TIMEOUT
                );
                return Ok(vec![Cmd::TrackNodeIssueInDysfunction {
                    name: target.name(),
                    // TODO: no need for op id tracking here, this can be a simple counter
                    issue: IssueType::RequestOperation(operation_id),
                }]);
            }
        }?;

        debug!("Response in from peer for query {msg_id:?} {response:?}");

        if let MsgType::NodeDataResponse {
            msg: NodeDataResponse::QueryResponse { response, .. },
            ..
        } = response.into_msg()?
        {
            let client_msg = ClientDataResponse::QueryResponse {
                response,
                correlation_id: msg_id,
            };

            let (kind, payload) = MyNode::serialize_client_msg_response(snapshot.name, client_msg)?;

            MyNode::send_msg_on_stream(
                snapshot.network_knowledge.section_key(),
                payload,
                kind,
                client_response_stream,
                Some(target),
                msg_id,
            )
            .await?;
        } else {
            error!(
                "Unexpected reponse to query from node. To : {msg_id:?}; response: {response:?}"
            );
        }

        // Everything went okay, so no further cmds to handle
        Ok(vec![])
    }

    /// Send an OutgoingMsg on a given stream
    pub(crate) async fn send_msg_on_stream(
        section_key: bls::PublicKey,
        payload: Bytes,
        kind: MsgKind,
        send_stream: Arc<Mutex<SendStream>>,
        target_peer: Option<Peer>,
        original_msg_id: MsgId,
    ) -> Result<()> {
        // TODO why do we need dst here?
        let bytes = MyNode::form_usr_msg_bytes_to_node(
            section_key,
            payload,
            kind,
            target_peer,
            original_msg_id,
        )?;
        trace!("Sending {original_msg_id:?} to recipient over stream");
        let stream_prio = 10;
        let mut send_stream = send_stream.lock_owned().await;
        let stream_id = send_stream.id();

        trace!("Stream {stream_id} locked for {original_msg_id:?} to {target_peer:?}");
        send_stream.set_priority(stream_prio);
        trace!("Prio set for {original_msg_id:?} to {target_peer:?}");
        if let Err(error) = send_stream.send_user_msg(bytes).await {
            error!(
                "Could not send query response {original_msg_id:?} to \
                peer {target_peer:?} over response {stream_id}: {error:?}"
            );
            return Err(error.into());
        }

        trace!("Msg away for {original_msg_id:?} to {target_peer:?}");

        // unblock + move finish off thread as it's not strictly related to the sending of the msg.
        let _handle = tokio::spawn(async move {
            // Attempt to gracefully terminate the stream.
            // If this errors it does _not_ mean our message has not been sent
            let _ = send_stream.finish().await;
            trace!("bidi stream finished for {original_msg_id:?} to: {target_peer:?}");
        });

        debug!("Sent the msg {original_msg_id:?} over {stream_id} to {target_peer:?}");

        Ok(())
    }

    pub(crate) fn form_usr_msg_bytes_to_node(
        section_key: bls::PublicKey,
        payload: Bytes,
        kind: MsgKind,
        target: Option<Peer>,
        msg_id: MsgId,
    ) -> Result<UsrMsgBytes> {
        let dst_name = target.map_or(XorName::default(), |peer| peer.name());
        // we first generate the XorName
        let dst = Dst {
            name: dst_name,
            section_key,
        };

        let mut wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        wire_msg
            .serialize_and_cache_bytes()
            .map_err(|_| Error::InvalidMessage)
    }

    pub(crate) fn get_metadata_of(&self, prefix: &Prefix) -> MetadataExchange {
        // Load tracked adult_levels
        let adult_levels = self.capacity.levels_matching(*prefix);
        MetadataExchange { adult_levels }
    }

    pub(crate) fn set_adult_levels(&mut self, adult_levels: MetadataExchange) {
        let MetadataExchange { adult_levels } = adult_levels;
        self.capacity.set_adult_levels(adult_levels)
    }

    /// Registered holders not present in provided list of members
    /// will be removed from `adult_storage_info` and no longer tracked for liveness.
    pub(crate) async fn liveness_retain_only(&mut self, members: BTreeSet<XorName>) {
        self.capacity.retain_members_only(&members);
        // stop tracking liveness of absent holders
        if let Err(error) = self
            .dysfunction_cmds_sender
            .send(DysCmds::RetainNodes(members))
            .await
        {
            warn!("Could not send RetainNodes through dysfunctional_cmds_tx: {error}");
        };
    }

    /// Adds the new adult to the Capacity and Liveness trackers.
    pub(crate) async fn add_new_adult_to_trackers(&mut self, adult: XorName) {
        info!("Adding new Adult: {adult} to trackers");
        self.capacity.add_new_adult(adult);
        if let Err(error) = self
            .dysfunction_cmds_sender
            .send(DysCmds::AddNode(adult))
            .await
        {
            warn!("Could not send AddNode through dysfunctional_cmds_tx: {error}");
        };
    }

    /// Set storage level of a given node.
    /// Returns whether the level changed or not.
    pub(crate) fn set_storage_level(&mut self, node_id: &PublicKey, level: StorageLevel) -> bool {
        info!("Setting new storage level..");
        let changed = self
            .capacity
            .set_adult_level(XorName::from(*node_id), level);
        let avg_usage = self.capacity.avg_usage();
        info!(
            "Avg storage usage among Adults is between {}-{} %",
            avg_usage * 10,
            (avg_usage + 1) * 10
        );
        changed
    }

    /// Used to fetch the list of holders for given name of data.
    /// Sorts adults by closeness to data address, returns data_copy_count of them
    pub(crate) fn target_data_holders(context: &NodeContext, target: XorName) -> BTreeSet<Peer> {
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = context.network_knowledge.adults();

        trace!("Total adults known about: {:?}", adults.len());

        let candidates = adults
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!("Target holders of {:?} are : {:?}", target, candidates,);

        candidates
    }
}
