// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::MsgFromPeer;

use sn_interface::{
    messaging::{MsgKind, WireMsg},
    types::{log_markers::LogMarker, Peer},
};

use qp2p::ConnectionIncoming;
use std::sync::Arc;
use tokio::{sync::mpsc, task};
use tracing::Instrument;

#[derive(Debug)]
pub(crate) enum ConnectionEvent {
    ConnectionClosed {
        peer: Peer,
        connection: Arc<qp2p::Connection>,
    },
}

#[derive(Clone)]
pub(crate) struct MsgListener {
    connection_events: mpsc::Sender<ConnectionEvent>,
    receive_msg: mpsc::Sender<MsgFromPeer>,
}

impl MsgListener {
    pub(crate) fn new(
        connection_events: mpsc::Sender<ConnectionEvent>,
        receive_msg: mpsc::Sender<MsgFromPeer>,
    ) -> Self {
        Self {
            connection_events,
            receive_msg,
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn listen(&self, conn: Arc<qp2p::Connection>, incoming_msgs: ConnectionIncoming) {
        let clone = self.clone();
        let _ = task::spawn(clone.listen_internal(conn, incoming_msgs).in_current_span());
    }

    #[tracing::instrument(skip_all)]
    async fn listen_internal(
        self,
        conn: Arc<qp2p::Connection>,
        mut incoming_msgs: ConnectionIncoming,
    ) {
        let conn_id = conn.id();
        let remote_address = conn.remote_address();

        let mut established_peer = None;

        while let Some(result) = incoming_msgs.next_with_stream().await.transpose() {
            match result {
                Ok((msg_bytes, send_stream)) => {
                    let stream_info = if let Some(stream) = &send_stream {
                        format!(" on {}", stream.id())
                    } else {
                        "".to_string()
                    };
                    debug!(
                        "New msg arrived over conn_id={conn_id} from {remote_address:?}{stream_info}"
                    );

                    let wire_msg = match WireMsg::from(msg_bytes) {
                        Ok(wire_msg) => wire_msg,
                        Err(error) => {
                            // TODO: should perhaps rather drop this connection.. as it is a spam vector
                            debug!("Failed to deserialize message received from {remote_address:?}{stream_info}: {error:?}");
                            continue;
                        }
                    };

                    // let mut is_node_join_msg = false;
                    let src_name = match wire_msg.kind() {
                        MsgKind::Client(auth) => auth.public_key.into(),
                        MsgKind::Node { name, .. } => {
                            // is_node_join_msg = *is_join;
                            *name
                        }
                        MsgKind::ClientDataResponse(name) | MsgKind::NodeDataResponse(name) => {
                            *name
                        }
                    };

                    let peer = Peer::new(src_name, remote_address);

                    if established_peer.is_none() {
                        established_peer = Some(peer);
                    }

                    let msg_id = wire_msg.msg_id();
                    debug!(
                        "Msg {msg_id:?} received, over conn_id={conn_id}, from: {peer:?}{stream_info} was: {wire_msg:?}"
                    );

                    if let Err(error) = self
                        .receive_msg
                        .send(MsgFromPeer {
                            sender: peer,
                            wire_msg,
                            send_stream,
                        })
                        .await
                    {
                        error!("Error pushing msg {msg_id:?} onto internal msg handling channel: {error:?}");
                    }
                }
                Err(error) => {
                    warn!("Error on connection {conn_id} with {remote_address}: {error:?}");
                }
            }
        }

        trace!(%conn_id, %remote_address, "{}", LogMarker::ConnectionClosed);

        // remove this closed connection
        if let Some(peer) = established_peer {
            trace!("Removing connection {conn_id} with node {established_peer:?} from cache");
            let _ = self
                .connection_events
                .send(ConnectionEvent::ConnectionClosed {
                    peer,
                    connection: conn,
                })
                .await;
        }
    }
}
