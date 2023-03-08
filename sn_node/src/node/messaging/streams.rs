// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, Cmd, Error, MyNode, Result};

use sn_interface::{
    messaging::{
        data::DataResponse, system::NodeMsg, AntiEntropyMsg, Dst, MsgId, MsgKind, WireMsg,
    },
    types::{ClientId, NodeId, Participant},
};

use bytes::Bytes;
use qp2p::SendStream;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::collections::{BTreeMap, BTreeSet};
use xor_name::XorName;

// Message handling over streams
impl MyNode {
    pub(crate) async fn send_node_msg_response(
        msg: NodeMsg,
        msg_id: MsgId,
        correlation_id: MsgId,
        node_id: NodeId,
        context: NodeContext,
        send_stream: SendStream,
    ) -> Result<Option<Cmd>> {
        let stream_id = send_stream.id();
        info!("Sending response msg {msg_id:?} over {stream_id}");
        let (kind, payload) = MyNode::serialize_node_msg(context.name, &msg)?;
        send_msg_on_stream(
            msg_id,
            kind,
            payload,
            Participant::from_node(node_id),
            correlation_id,
            context.network_knowledge.section_key(),
            send_stream,
        )
        .await
    }

    pub(crate) async fn send_ae_response(
        msg: AntiEntropyMsg,
        msg_id: MsgId,
        participant: Participant,
        correlation_id: MsgId,
        send_stream: SendStream,
        context: NodeContext,
    ) -> Result<Option<Cmd>> {
        info!("Sending ae response msg for {correlation_id:?}");
        send_msg_on_stream(
            msg_id,
            MsgKind::AntiEntropy(context.name),
            WireMsg::serialize_msg_payload(&msg)?,
            participant,
            correlation_id,
            context.network_knowledge.section_key(),
            send_stream,
        )
        .await
    }

    pub(crate) async fn send_data_response(
        msg: DataResponse,
        msg_id: MsgId,
        correlation_id: MsgId,
        send_stream: SendStream,
        context: NodeContext,
        client_id: ClientId,
    ) -> Result<Option<Cmd>> {
        info!("Sending client response msg for {correlation_id:?}");
        send_msg_on_stream(
            msg_id,
            MsgKind::DataResponse(context.name),
            WireMsg::serialize_msg_payload(&msg)?,
            Participant::from_client(client_id),
            correlation_id,
            context.network_knowledge.section_key(),
            send_stream,
        )
        .await
    }

    /// Sends a msg, and listens for any response
    /// The response is returned to be handled via the dispatcher (though a response is not necessarily expected)
    pub(crate) fn send_and_enqueue_any_response(
        msg: NodeMsg,
        msg_id: MsgId,
        context: NodeContext,
        recipients: BTreeSet<NodeId>,
    ) -> Result<()> {
        let targets_len = recipients.len();
        trace!("Sending out + awaiting response of {msg_id:?} to {targets_len} holder node/s {recipients:?}");

        let (kind, payload) = MyNode::serialize_node_msg(context.name, &msg)?;

        // We create a Dst with random dst name, but we'll update it accordingly for each target
        let mut dst = Dst {
            name: XorName::default(),
            section_key: context.network_knowledge.section_key(),
        };
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
        let _bytes = wire_msg.serialize_and_cache_bytes()?;

        for target in recipients {
            dst.name = target.name();
            let bytes_to_node = wire_msg.serialize_with_new_dst(&dst)?;
            let comm = context.comm.clone();
            info!("About to send {msg_id:?} to holder node: {target:?}");
            comm.send_and_return_response(target, msg_id, bytes_to_node);
        }

        Ok(())
    }

    /// Send out msg and await response to forward on to client
    pub(crate) fn send_and_forward_response_to_client(
        wire_msg: WireMsg,
        context: NodeContext,
        targets: BTreeSet<NodeId>,
        client_stream: SendStream,
        client_id: ClientId,
    ) -> Result<()> {
        let msg_id = wire_msg.msg_id();
        let targets_len = targets.len();

        debug!(
            "Sending out {msg_id:?}, coming from {client_id}, to {targets_len} holder node/s {targets:?}",
        );

        let node_bytes: BTreeMap<_, _> = targets
            .into_par_iter()
            .filter_map(|target| {
                let dst = Dst {
                    name: target.name(),
                    section_key: context.network_knowledge.section_key(),
                };
                match wire_msg.serialize_with_new_dst(&dst) {
                    Ok(bytes_to_node) => Some((target, bytes_to_node)),
                    Err(error) => {
                        error!("Sending out {msg_id:?} to {target} failed due to {error}.");
                        None
                    }
                }
            })
            .collect();

        let dst_stream = (
            Dst {
                name: client_id.name(),
                section_key: context.network_knowledge.section_key(),
            },
            client_stream,
        );

        context
            .comm
            .send_and_respond_on_stream(msg_id, node_bytes, targets_len, dst_stream);

        Ok(())
    }
}

// Send a msg on a given stream
async fn send_msg_on_stream(
    msg_id: MsgId,
    kind: MsgKind,
    payload: Bytes,
    recipient: Participant,
    correlation_id: MsgId,
    section_key: bls::PublicKey,
    mut send_stream: SendStream,
) -> Result<Option<Cmd>> {
    let dst = Dst {
        name: recipient.name(),
        section_key,
    };
    let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);
    let bytes = wire_msg.serialize().map_err(|_| Error::InvalidMessage)?;

    let stream_id = send_stream.id();
    info!(
        "Sending response {msg_id:?} of msg {correlation_id:?}, to {recipient:?} over {stream_id}"
    );

    // let stream_prio = 10;
    // send_stream.set_priority(stream_prio);
    trace!("Prio set on stream {stream_id}, response {msg_id:?} of msg {correlation_id:?}, to {recipient:?}");

    if let Err(error) = send_stream.send_user_msg(bytes).await {
        error!(
            "Could not send response {msg_id:?} of msg {correlation_id:?}, to {recipient:?} \
            over response {stream_id}: {error:?}"
        );
        return Ok(Some(Cmd::HandleCommsError {
            participant: recipient,
            error: sn_comms::Error::from(error),
        }));
    }

    trace!(
        "Sent: Response {msg_id:?} of msg {correlation_id:?} to {recipient:?}, over {stream_id}."
    );

    // unblock + move finish off thread as it's not strictly related to the sending of the msg.
    let stream_id_clone = stream_id.clone();
    let _handle = tokio::spawn(async move {
        // Attempt to gracefully terminate the stream.
        // If this errors it does _not_ mean our message has not been sent
        let result = send_stream.finish().await;
        trace!("Response {msg_id:?} of msg {correlation_id:?} sent to {recipient:?} over {stream_id_clone}. Stream finished with result: {result:?}");
    });

    trace!(
        "Sent the response {msg_id:?} of msg {correlation_id:?} to {recipient:?} over {stream_id}"
    );

    Ok(None)
}
