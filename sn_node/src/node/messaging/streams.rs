// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, Cmd, Error, MyNode, Result};

use sn_comms::Error as CommsError;
use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{
        data::{ClientDataResponse, CmdResponse},
        system::{NodeDataCmd, NodeDataResponse, NodeMsg},
        Dst, MsgId, MsgKind, MsgType, WireMsg,
    },
    types::Peer,
};

use qp2p::SendStream;
use xor_name::XorName;

use bytes::Bytes;
use futures::FutureExt;
use lazy_static::lazy_static;
use std::{collections::BTreeSet, env::var, str::FromStr};
use tokio::time::{error::Elapsed, timeout, Duration};

/// Environment variable to set timeout value (in seconds) for data queries
/// forwarded to Adults. Default value (`NODE_RESPONSE_DEFAULT_TIMEOUT`) is otherwise used.
const ENV_NODE_RESPONSE_TIMEOUT: &str = "SN_NODE_RESPONSE_TIMEOUT";

// Default timeout period set for data queries forwarded to Adult.
// TODO: how to determine this time properly?
const NODE_RESPONSE_DEFAULT_TIMEOUT: Duration = Duration::from_secs(70);

lazy_static! {
    static ref NODE_RESPONSE_TIMEOUT: Duration = match var(ENV_NODE_RESPONSE_TIMEOUT)
        .map(|v| u64::from_str(&v))
    {
        Ok(Ok(secs)) => {
            let timeout = Duration::from_secs(secs);
            info!("{ENV_NODE_RESPONSE_TIMEOUT} env var set, Node data query response timeout set to {timeout:?}");
            timeout
        }
        Ok(Err(err)) => {
            warn!(
                "Failed to parse {ENV_NODE_RESPONSE_TIMEOUT} value, using \
                default value ({NODE_RESPONSE_DEFAULT_TIMEOUT:?}): {err:?}"
            );
            NODE_RESPONSE_DEFAULT_TIMEOUT
        }
        Err(_) => NODE_RESPONSE_DEFAULT_TIMEOUT,
    };
}

// Message handling over streams
impl MyNode {
    pub(crate) async fn send_node_msg_response(
        msg: NodeMsg,
        msg_id: MsgId,
        recipient: Peer,
        context: NodeContext,
        send_stream: SendStream,
    ) -> Result<Vec<Cmd>> {
        let stream_id = send_stream.id();
        trace!("Sending response msg {msg_id:?} over {stream_id}");
        let (kind, payload) = MyNode::serialize_node_msg(context.name, &msg)?;

        match send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            send_stream,
            recipient,
            msg_id,
        )
        .await
        {
            Ok(()) => Ok(vec![]),
            Err(err) => {
                error!(
                    "Could not send response msg {msg_id:?} \
                    to {recipient:?} over {stream_id}: {err:?}"
                );
                if let Error::Comms(_) = err {
                    Ok(vec![Cmd::HandleFailedSendToNode {
                        peer: recipient,
                        msg_id,
                    }])
                } else {
                    Ok(vec![])
                }
            }
        }
    }

    pub(crate) async fn send_client_response(
        msg: ClientDataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        context: NodeContext,
        source_client: Peer,
    ) -> Result<()> {
        trace!("Sending client response msg for {correlation_id:?}");
        let (kind, payload) = MyNode::serialize_client_msg_response(context.name, &msg)?;
        send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            send_stream,
            source_client,
            correlation_id,
        )
        .await
    }

    pub(crate) async fn send_node_data_response(
        msg: NodeDataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        context: NodeContext,
        requesting_peer: Peer,
    ) -> Result<()> {
        trace!("Sending node response msg for {correlation_id:?}");
        let (kind, payload) = MyNode::serialize_node_data_response(context.name, &msg)?;
        send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            send_stream,
            requesting_peer,
            correlation_id,
        )
        .await
    }

    /// Sends a msg via comms, and listens for any response
    /// The response is returned to be handled via the dispatcher (though a response is not necessarily expected)
    pub(crate) async fn send_msg_enqueue_any_response(
        msg: NodeMsg,
        msg_id: MsgId,
        context: NodeContext,
        recipients: BTreeSet<Peer>,
    ) -> Result<Vec<Cmd>> {
        let targets_len = recipients.len();
        debug!("Sending out + awaiting response of {msg_id:?} to {targets_len} holder node/s {recipients:?}");

        // TODO: Should we change this func to just return the futures and handlers can decide to wait on all
        // or process as they come in
        let results =
            send_to_target_peers_and_await_responses(msg_id, &msg, recipients, &context).await?;

        let mut output_cmds = vec![];
        results.into_iter().for_each(|(peer, result)| match result {
            Err(_elapsed) => {
                error!(
                    "{msg_id:?}: No response from {peer:?} after {:?} timeout.",
                    *NODE_RESPONSE_TIMEOUT
                );
                // output_cmds.push(Cmd::TrackNodeIssue {
                //     name: peer.name(),
                //     issue: IssueType::Communication,
                // });
            }
            Ok(Ok(wire_msg)) => {
                debug!("Response in from {peer:?} for {msg_id:?}: {wire_msg:?}");

                output_cmds.push(Cmd::HandleMsg { origin: peer, wire_msg, send_stream: None });
            }
            Ok(Err(comms_err)) => {
                error!("{msg_id:?} Error when sending request to node {peer:?}, marking node as faulty: {comms_err:?}");
                output_cmds.push(Cmd::TrackNodeIssue {
                    name: peer.name(),
                    issue: IssueType::Communication,
                });
            }
        });

        Ok(output_cmds)
    }
    pub(crate) async fn send_msg_await_response_and_send_to_client(
        msg_id: MsgId,
        msg: NodeMsg,
        context: NodeContext,
        targets: BTreeSet<Peer>,
        client_stream: SendStream,
        source_client: Peer,
    ) -> Result<Vec<Cmd>> {
        let targets_len = targets.len();
        debug!("Sending out {msg_id:?} to {targets_len} holder node/s {targets:?}");
        let results =
            send_to_target_peers_and_await_responses(msg_id, &msg, targets, &context).await?;

        let mut output_cmds = vec![];
        let mut success_count = 0;
        let mut last_success_response = None;
        let mut last_error = None;
        results.into_iter().for_each(|(peer, result)| match result {
            Err(_elapsed) => {
                error!(
                    "{msg_id:?}: No response from {peer:?} after {:?} timeout. Marking node as faulty",
                    *NODE_RESPONSE_TIMEOUT
                );
                output_cmds.push(Cmd::TrackNodeIssue {
                    name: peer.name(),
                    issue: IssueType::Communication,
                });
                // TODO: report timeout error to client?
            }
            Ok(Ok(response)) => {
                debug!("Response in from {peer:?} for {msg_id:?}: {response:?}");
                success_count += 1;
                last_success_response = Some(response);
            }
            Ok(Err(comms_err)) => {
                error!("{msg_id:?} Error when sending request to holder node {peer:?}, marking node as faulty: {comms_err:?}");
                if let CommsError::SendError(_) = comms_err {
                    output_cmds.push(Cmd::TrackNodeIssue {
                        name: peer.name(),
                        issue: IssueType::Communication,
                    });
                } else {
                    output_cmds.push(Cmd::TrackNodeIssue {
                        name: peer.name(),
                        issue: IssueType::RequestOperation,
                    });
                }

                last_error = Some(Error::Comms(comms_err));
            }
        });

        if success_count == targets_len {
            if let Some(response) = last_success_response {
                output_cmds.extend(build_and_send_response_to_client(
                    msg_id,
                    msg,
                    context,
                    Ok(response.into_msg()?),
                    client_stream,
                    source_client,
                ));
            } else {
                // This should not be possible with above checks
                error!("No valid response to send from all responses for {msg_id:?}");
            }
        } else {
            error!("Request to holder node/s was not completely successful for {msg_id:?}");
            if let Some(error) = last_error {
                output_cmds.extend(build_and_send_response_to_client(
                    msg_id,
                    msg,
                    context,
                    Err(error),
                    client_stream,
                    source_client,
                ));
            }
        }

        Ok(output_cmds)
    }
}

/// Verify what kind of response was received, and if that's the expected type based on
/// the type of msg sent to the nodes, then forward the corresponding response to the client
fn build_and_send_response_to_client(
    correlation_id: MsgId,
    msg_sent: NodeMsg,
    context: NodeContext,
    response: Result<MsgType>,
    send_stream: SendStream,
    source_client: Peer,
) -> Vec<Cmd> {
    let msg = match msg_sent {
        NodeMsg::NodeDataQuery(query) => {
            match response {
                Ok(MsgType::NodeDataResponse {
                    msg: NodeDataResponse::QueryResponse { response, .. },
                    ..
                }) => {
                    // We sent a data query and we received a query response,
                    // so let's forward it to the client
                    debug!("{correlation_id:?} sending query response back to client");
                    ClientDataResponse::QueryResponse {
                        response,
                        correlation_id,
                    }
                }
                Ok(other_resp) => {
                    // TODO: handle this bad response
                    error!("Unexpected response to query from node for {correlation_id:?}: {other_resp:?}");
                    return vec![];
                }
                Err(err) => ClientDataResponse::QueryResponse {
                    response: query.query.to_error_response(err.into()),
                    correlation_id,
                },
            }
        }
        NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(replicated_data)) => {
            match response {
                Ok(MsgType::NodeDataResponse {
                    msg: NodeDataResponse::CmdResponse { response, .. },
                    ..
                }) => {
                    // We sent a data cmd to store client data and we received a
                    // cmd response, so let's forward it to the client
                    debug!("{correlation_id:?} sending cmd response ACK back to client");
                    ClientDataResponse::CmdResponse {
                        response,
                        correlation_id,
                    }
                }
                Ok(other_resp) => {
                    // TODO: handle this bad response
                    error!("Unexpected response to cmd from node for {correlation_id:?}: {other_resp:?}");
                    return vec![];
                }
                Err(err) => {
                    match CmdResponse::err(replicated_data, err.into()) {
                        Ok(response) => ClientDataResponse::CmdResponse {
                            response,
                            correlation_id,
                        },
                        Err(err) => {
                            error!("Failed to generate cmd error response for {correlation_id:?}: {err:?}");
                            return vec![];
                        }
                    }
                }
            }
        }
        other_msg_sent => {
            // this shouldn't happen as we don't send other type of msg with Cmd::SendMsgAwaitResponseAndRespondToClient
            error!("Unexpected type of msg sent to holder node/s for {correlation_id:?}: {other_msg_sent:?}");
            return vec![];
        }
    };

    vec![Cmd::SendClientResponse {
        msg,
        correlation_id,
        send_stream,
        context,
        source_client,
    }]
}

// Send a msg to each of the targets, and await for the responses from all of them
async fn send_to_target_peers_and_await_responses(
    msg_id: MsgId,
    msg: &NodeMsg,
    targets: BTreeSet<Peer>,
    context: &NodeContext,
) -> Result<Vec<(Peer, Result<Result<WireMsg, CommsError>, Elapsed>)>> {
    let (kind, payload) = MyNode::serialize_node_msg(context.name, msg)?;

    // We create a Dst with random dst name, but we'll update it accordingly for each target
    let mut dst = Dst {
        name: XorName::default(),
        section_key: context.network_knowledge.section_key(),
    };
    let mut wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
    let _bytes = wire_msg.serialize_and_cache_bytes()?;

    let mut send_tasks = vec![];
    for target in targets {
        dst.name = target.name();
        let bytes_to_node = wire_msg.serialize_with_new_dst(&dst)?;

        let comm = context.comm.clone();
        info!("About to send {msg_id:?} to holder node: {target:?}");

        send_tasks.push(
            async move {
                let outcome = timeout(*NODE_RESPONSE_TIMEOUT, async {
                    comm.send_out_bytes_to_peer_and_return_response(target, msg_id, bytes_to_node)
                        .await
                })
                .await;

                (target, outcome)
            }
            .boxed(),
        );
    }

    Ok(futures::future::join_all(send_tasks).await)
}

// Send a msg on a given stream
async fn send_msg_on_stream(
    section_key: bls::PublicKey,
    payload: Bytes,
    kind: MsgKind,
    mut send_stream: SendStream,
    target_peer: Peer,
    correlation_id: MsgId,
) -> Result<()> {
    let dst = Dst {
        name: target_peer.name(),
        section_key,
    };
    let wire_msg = WireMsg::new_msg(correlation_id, payload, kind, dst);
    let bytes = wire_msg.serialize().map_err(|_| Error::InvalidMessage)?;

    let stream_id = send_stream.id();
    trace!("Sending response {correlation_id:?} to {target_peer:?} over {stream_id}");

    let stream_prio = 10;
    send_stream.set_priority(stream_prio);
    trace!("Prio set for {correlation_id:?} to {target_peer:?}, over {stream_id}");

    if let Err(error) = send_stream.send_user_msg(bytes).await {
        error!(
            "Could not send response {correlation_id:?} to peer {target_peer:?} \
            over response {stream_id}: {error:?}"
        );
        return Err(error.into());
    }

    trace!("Msg away for {correlation_id:?} to {target_peer:?}, over {stream_id}");

    // unblock + move finish off thread as it's not strictly related to the sending of the msg.
    let stream_id_clone = stream_id.clone();
    let _handle = tokio::spawn(async move {
        // Attempt to gracefully terminate the stream.
        // If this errors it does _not_ mean our message has not been sent
        let result = send_stream.finish().await;
        trace!("Response {correlation_id:?} sent to {target_peer:?} over {stream_id_clone}. Stream finished with result: {result:?}");
    });

    debug!("Sent the msg {correlation_id:?} to {target_peer:?} over {stream_id}");

    Ok(())
}
