// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::MsgEvent;

use sn_interface::{
    messaging::WireMsg,
    types::{log_markers::LogMarker, Peer},
};

use qp2p::ConnectionIncoming;
use tokio::{sync::mpsc, task};
use tracing::Instrument;

#[derive(Debug)]
pub(crate) enum ListenerEvent {
    Connected {
        peer: Peer,
        connection: qp2p::Connection,
    },
}

#[derive(Clone)]
pub(crate) struct MsgListener {
    add_connection: mpsc::Sender<ListenerEvent>,
    receive_msg: mpsc::Sender<MsgEvent>,
    count_msg: mpsc::Sender<()>,
}

impl MsgListener {
    pub(crate) fn new(
        add_connection: mpsc::Sender<ListenerEvent>,
        receive_msg: mpsc::Sender<MsgEvent>,
        count_msg: mpsc::Sender<()>,
    ) -> Self {
        Self {
            add_connection,
            count_msg,
            receive_msg,
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn listen(&self, conn: qp2p::Connection, incoming_msgs: ConnectionIncoming) {
        let clone = self.clone();
        let _ = task::spawn_local(clone.listen_internal(conn, incoming_msgs).in_current_span());
    }

    #[tracing::instrument(skip_all)]
    async fn listen_internal(self, conn: qp2p::Connection, mut incoming_msgs: ConnectionIncoming) {
        let conn_id = conn.id();
        let remote_address = conn.remote_address();
        let mut first = true;

        while let Some(result) = incoming_msgs.next().await.transpose() {
            match result {
                Ok(msg_bytes) => {
                    let (header, dst, payload) = msg_bytes;
                    let wire_msg = match WireMsg::from(header, dst, payload) {
                        Ok(wire_msg) => wire_msg,
                        Err(error) => {
                            // TODO: should perhaps rather drop this connection.. as it is a spam vector
                            debug!("Failed to deserialize message: {:?}", error);
                            continue;
                        }
                    };

                    let src_name = wire_msg.auth().src_name();

                    if first {
                        first = false;
                        let _ = self
                            .add_connection
                            .send(ListenerEvent::Connected {
                                peer: Peer::new(src_name, remote_address),
                                connection: conn.clone(),
                            })
                            .await;
                    }

                    if let Err(error) = self
                        .receive_msg
                        .send(MsgEvent::Received {
                            sender: Peer::new(src_name, remote_address),
                            wire_msg,
                        })
                        .await
                    {
                        error!("Error pushing msg onto internal msg channel... {error:?}");
                    }

                    // count incoming msgs..
                    let _ = self.count_msg.send(());
                }
                Err(error) => {
                    // TODO: should we propagate this?
                    warn!("error on connection with {}: {:?}", remote_address, error);
                }
            }
        }

        trace!(%conn_id, %remote_address, "{}", LogMarker::ConnectionClosed);
    }

    // count outgoing msgs
    #[cfg(feature = "back-pressure")]
    pub(crate) async fn count_msg(&self) {
        if let Err(err) = self.count_msg.send(()).await {
            // this is really a problem as we rely on this counting, make sure this doesn't normally error!
            debug!("Error when trying to count outgoing msg..! {}", err);
        }
    }
}
