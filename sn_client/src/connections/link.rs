// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Peer;

use qp2p::{Connection, Endpoint, RecvStream, UsrMsgBytes};
use sn_interface::{messaging::MsgId, types::log_markers::LogMarker};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

type ConnId = String;

/// A link to a peer in the network.
///
/// The upper layers will add incoming connections to the link,
/// and use the link to send msgs.
/// Using the link will open a connection if there is none there.
/// The link is a way to keep connections to a peer in one place
/// and use them efficiently; converge to a single one regardless of concurrent
/// comms initiation between the peers, and so on.
#[derive(Clone, Debug)]
pub(crate) struct Link {
    peer: Peer,
    endpoint: Endpoint,
}

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint) -> Self {
        Self {
            peer,
            endpoint,
        }
    }

    pub(crate) async fn send_bi(
        &self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<RecvStream, LinkError> {
        let peer = self.peer;
        debug!("sending bidi msg out... {msg_id:?} to {peer:?}");
        let conn = self.get_or_connect(msg_id).await?;
        debug!("connection got {msg_id:?} to {peer:?}");
        let (mut send_stream, recv_stream) = conn.open_bi().await.map_err(LinkError::Connection)?;

        debug!("{msg_id:?} bidi opened");
        send_stream.set_priority(10);
        send_stream
            .send_user_msg(bytes.clone())
            .await
            .map_err(LinkError::Send)?;
        debug!("{msg_id:?} bidi msg sent");

        send_stream.finish().await.or_else(|error| match error {
            qp2p::SendError::StreamLost(qp2p::StreamError::Stopped(_)) => Ok(()),
            _ => Err(LinkError::Send(error)),
        })?;
        debug!("{msg_id:?} bidi finished");
        Ok(recv_stream)
    }

    // Get a connection or create a fresh one
    async fn get_or_connect(&self, msg_id: MsgId) -> Result<Connection, LinkError> {
            self.create_connection_if_none_exist(Some(msg_id)).await

    }

    /// Uses qp2p to create a connection and stores it on Self.
    /// Returns early without creating a new connection if an existing connection is found (which may have been created before we can get the write lock).
    ///
    /// (There is a strong chance for a client writing many chunks to find no connection for each chunk and then try and create connections...
    /// which could lead to connection after connection being created if we do not check here)
    pub(crate) async fn create_connection_if_none_exist(
        &self,
        msg_id: Option<MsgId>,
    ) -> Result<Connection, LinkError> {
        let peer = self.peer;

        debug!("{msg_id:?} creating connnnn to {peer:?}");
        let (conn, _incoming_msgs) = self
            .endpoint
            .connect_to(&peer.addr())
            .await
            .map_err(LinkError::Connection)?;

        debug!("{msg_id:?} conn creating done {peer:?}");
        trace!(
            "{} to {} (id: {})",
            LogMarker::ConnectionOpened,
            conn.remote_address(),
            conn.id()
        );

        Ok(conn)
    }
}

#[derive(Debug)]
/// Errors returned when using a Link
pub enum LinkError {
    /// Failed to connect to a peer
    Connection(qp2p::ConnectionError),
    /// Failed to send a msg to a peer
    Send(qp2p::SendError),
}
