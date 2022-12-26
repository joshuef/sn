// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::dysfunction::DysCmds, messaging::Peers, Cmd, Error, MyNode,
    Result,
};
use crate::storage::Error as StorageError;

use sn_dysfunction::IssueType;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{ClientDataResponse, DataCmd, DataQuery},
        system::{NodeDataCmd, NodeDataQuery, NodeDataResponse, NodeEvent, NodeMsg, OperationId},
        AuthorityProof, ClientAuth, Dst, MsgId, MsgKind, MsgType, WireMsg,
    },
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey, ReplicatedData},
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
    pub(crate) async fn replicate_data_to_nodes(
        context: &NodeContext,
        data: ReplicatedData,
        msg_id: MsgId,
        targets: BTreeSet<Peer>,
    ) -> Result<Vec<(Peer, Result<WireMsg>)>> {
        info!(
            "Replicating data from {msg_id:?} {:?} to holders: {targets:?}",
            data.name()
        );

        // TODO: general ReplicateData flow could go bidi?
        // Right now we've a new msg for just one datum.
        // Atm that's perhaps more bother than its worth..
        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data));
        let mut send_tasks = vec![];

        let (kind, payload) = MyNode::serialize_node_msg(context.name, msg)?;
        let section_key = context.network_knowledge.section_key();

        // We create a Dst with random dst name, but we'll update it for each target
        let mut dst = Dst {
            name: xor_name::rand::random(),
            section_key,
        };
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        for target in targets {
            dst.name = target.name();
            let bytes_to_adult = wire_msg.serialize_with_new_dst(&dst)?;

            let comm = context.comm.clone();
            info!("About to send {msg_id:?} to holder: {target:?}");

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
    pub(crate) async fn replicate_data_to_nodes_and_ack_to_client(
        context: &NodeContext,
        cmd: DataCmd,
        data: ReplicatedData,
        msg_id: MsgId,
        targets: BTreeSet<Peer>,
        client_response_stream: Arc<Mutex<SendStream>>,
    ) -> Result<()> {
        let targets_len = targets.len();

        let responses = MyNode::replicate_data_to_nodes(context, data, msg_id, targets).await?;
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
                    if let Error::AdultCmdSendError(peer) = error {
                        context.log_node_issue(peer.name(), IssueType::Communication);
                    }
                    last_error = Some(error);
                }
            }
        }

        // everything went fine, tell the client that
        if success_count == targets_len {
            if let Some(response) = ack_response {
                MyNode::respond_to_client_on_stream(
                    context,
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
                    context,
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
        context: &NodeContext,
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

            let (kind, payload) = MyNode::serialize_client_msg_response(context.name, client_msg)?;

            debug!("{correlation_id:?} sending cmd response ack back to client");
            MyNode::send_msg_on_stream(
                context.network_knowledge.section_key(),
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
        context: NodeContext,
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

        let targets = MyNode::target_data_holders(&context, *address.name());

        // Query only the nth adult
        let target = if let Some(peer) = targets.iter().nth(query.adult_index) {
            *peer
        } else {
            debug!("No targets found for {msg_id:?}");
            let error = Error::InsufficientAdults {
                prefix: context.network_knowledge.prefix(),
                expected: query.adult_index as u8 + 1,
                found: targets.len() as u8,
            };

            MyNode::send_query_error_response_on_stream(
                context,
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

        let (kind, payload) = MyNode::serialize_node_msg(context.name, msg)?;

        let comm = context.comm.clone();

        let bytes_to_adult = MyNode::form_usr_msg_bytes_to_node(
            context.network_knowledge.section_key(),
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

            let (kind, payload) = MyNode::serialize_client_msg_response(context.name, client_msg)?;

            MyNode::send_msg_on_stream(
                context.network_knowledge.section_key(),
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
        let stream_prio = 10;
        let mut send_stream = send_stream.lock_owned().await;
        let stream_id = send_stream.id();
        trace!("Sending {original_msg_id:?} to recipient over {stream_id}");

        trace!("Stream {stream_id} locked for {original_msg_id:?} to {target_peer:?}");
        send_stream.set_priority(stream_prio);
        trace!("Prio set for {original_msg_id:?} to {target_peer:?}, over {stream_id}");
        if let Err(error) = send_stream.send_user_msg(bytes).await {
            error!(
                "Could not send query response {original_msg_id:?} to \
                peer {target_peer:?} over response {stream_id}: {error:?}"
            );
            return Err(error.into());
        }

        trace!("Msg away for {original_msg_id:?} to {target_peer:?}, over {stream_id}");

        // unblock + move finish off thread as it's not strictly related to the sending of the msg.
        let stream_id_clone = stream_id.clone();
        let _handle = tokio::spawn(async move {
            // Attempt to gracefully terminate the stream.
            // If this errors it does _not_ mean our message has not been sent
            let result = send_stream.finish().await;
            trace!("bidi {stream_id_clone} finished for {original_msg_id:?} to {target_peer:?}: {result:?}");
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
        let dst = Dst {
            name: target.map_or(XorName::default(), |peer| peer.name()),
            section_key,
        };
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
        wire_msg.serialize().map_err(|_| Error::InvalidMessage)
    }

    /// Registered holders not present in provided list of members
    /// will be removed from full nodes (if present) and no longer tracked for liveness.
    pub(crate) async fn liveness_retain_only(&mut self, members: BTreeSet<XorName>) {
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
        if let Err(error) = self
            .dysfunction_cmds_sender
            .send(DysCmds::AddNode(adult))
            .await
        {
            warn!("Could not send AddNode through dysfunctional_cmds_tx: {error}");
        };
    }

    /// Used to fetch the list of holders for given name of data.
    /// Sorts members by closeness to data address, returns data_copy_count of them
    pub(crate) fn target_data_holders(context: &NodeContext, target: XorName) -> BTreeSet<Peer> {
        // TODO: reuse our_members_sorted_by_distance_to API when core is merged into upper layer
        let members = context.network_knowledge.members();

        trace!("Total members known about: {:?}", members.len());

        let candidates = members
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!("Target holders of {:?} are : {:?}", target, candidates,);

        candidates
    }

    /// Replicate data in the batch locally and then trigger further update reqeusts
    /// Requests for more data will go to sending node if there is more to come, or to the next
    /// furthest nodes if there was no data sent.
    pub(crate) async fn replicate_data_batch(
        context: &NodeContext,
        sender: Peer,
        data_batch: Vec<ReplicatedData>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(context.keypair.clone());

        let mut is_full = false;
        let data_batch_is_empty = !data_batch.is_empty();

        for data in data_batch {
            // grab the write lock each time in the loop to not hold it over large data sets
            let store_result = context
                .data_storage
                .store(&data, section_pk, node_keypair.clone())
                .await;

            // This may return a DatabaseFull error... but we should have reported StorageError::NotEnoughSpace
            // well before this
            match store_result {
                Ok(()) => debug!("Data item replicated."),
                Err(StorageError::NotEnoughSpace) => {
                    // storage full
                    error!("Not enough space to store more data");

                    let node_id = PublicKey::from(context.keypair.public);
                    let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                        node_id,
                        data,
                        full: true,
                    });
                    is_full = true;

                    cmds.push(MyNode::send_msg_to_our_elders(context, msg))
                }
                Err(error) => {
                    // the rest seem to be non-problematic errors.. (?)
                    error!("Problem storing data, but it was ignored: {error}");
                }
            }
        }

        // As long as the data batch is not empty, we send back a query again
        // to continue the replication process (like pageing).
        // This means there that there will be a number of repeated `give-me-data -> here_you_go` msg
        // exchanges, until there is no more data missing on this node.
        if !is_full && data_batch_is_empty {
            let data_i_have = context.data_storage.data_addrs().await;
            let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
            let cmd = MyNode::send_system_msg(msg, Peers::Single(sender), context.clone());
            cmds.push(cmd);
        }

        Ok(cmds)
    }
}
