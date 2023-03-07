// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use qp2p::{Connection, Endpoint, RecvStream, StreamError, UsrMsgBytes};
use sn_interface::{
    messaging::MsgId,
    types::{log_markers::LogMarker, NodeId},
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

type ConnId = String;

/// A link to a node in the network.
///
/// The upper layers will add incoming connections to the link,
/// and use the link to send msgs.
/// Using the link will open a connection if there is none there.
/// The link is a way to keep connections to a node in one place
/// and use them efficiently; converge to a single one regardless of concurrent
/// comms initiation between the client a node, and so on.
#[derive(Clone, Debug)]
pub(crate) struct Link {
    node_id: NodeId,
    endpoint: Endpoint,
    connections: Arc<RwLock<BTreeMap<ConnId, Arc<Connection>>>>,
}

impl Link {
    pub(crate) fn new(node_id: NodeId, endpoint: Endpoint) -> Self {
        Self {
            node_id,
            endpoint,
            connections: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) async fn send_bi(
        &self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<RecvStream, LinkError> {
        let node_id = self.node_id;
        debug!("sending bidi msg out... {msg_id:?} to {node_id:?}");
        let conn = self.get_or_connect(msg_id).await?;
        debug!(
            "connection got {msg_id:?} to {node_id:?}, conn_id={}",
            conn.id()
        );
        let (mut send_stream, recv_stream) = conn.open_bi().await.map_err(LinkError::Connection)?;

        debug!("{msg_id:?} to {node_id:?} bidi opened");
        send_stream.set_priority(10);
        send_stream
            .send_user_msg(bytes.clone())
            .await
            .map_err(LinkError::Send)?;
        debug!("{msg_id:?} bidi msg sent to {node_id:?}");

        // move this off thread
        let _handle = tokio::spawn(async move {
            // Attempt to gracefully terminate the stream.
            let result = match send_stream.finish().await {
                Ok(_) => Ok(()),
                // In case we get a `Stopped(0)` error, the other side is signalling it
                // already succesfully received all bytes. This might happen if we are calling finish late.
                Err(qp2p::SendError::StreamLost(StreamError::Stopped(0))) => Ok(()),
                // Propagate any other error, which means we should probably retry on a higher level.
                Err(err) => Err(LinkError::Send(err)),
            };
            debug!("{msg_id:?} to {node_id:?} bidi finished: {:?}", result);
        });

        Ok(recv_stream)
    }

    // Get a connection or create a fresh one
    async fn get_or_connect(&self, msg_id: MsgId) -> Result<Arc<Connection>, LinkError> {
        debug!("Attempting to get conn read lock... {msg_id:?}");
        let empty_conns = self.connections.read().await.is_empty();
        debug!("lock got {msg_id:?}");
        if empty_conns {
            // read again
            // first caller will find none again, but the subsequent callers
            // will access only after the first one finished creating a new connection
            // thus will find a connection here:
            debug!("{msg_id:?} creating conn with {:?}", self.node_id);
            self.create_connection_if_none_exist(Some(msg_id)).await
        } else {
            debug!("{msg_id:?} connections do exist...");
            // TODO: add in simple connection check when available.
            // we can then remove dead conns easily and return only valid conns
            let connections = self.connections.read().await;
            let conn = connections.iter().next().map(|(_, c)| c.clone());
            // we have to drop connection read here before we attempt to create (and write) connections
            drop(connections);
            Ok(conn.unwrap_or(self.create_connection_if_none_exist(Some(msg_id)).await?))
        }
    }

    /// Uses qp2p to create a connection and stores it on Self.
    /// Returns early without creating a new connection if an existing connection is found (which may have been created before we can get the write lock).
    ///
    /// (There is a strong chance for a client writing many chunks to find no connection for each chunk and then try and create connections...
    /// which could lead to connection after connection being created if we do not check here)
    pub(crate) async fn create_connection_if_none_exist(
        &self,
        msg_id: Option<MsgId>,
    ) -> Result<Arc<Connection>, LinkError> {
        let node_id = self.node_id;
        // grab write lock to prevent many many conns being opened at once
        debug!("[CONN WRITE]: {msg_id:?} to {node_id:?}");
        let mut conns_write_lock = self.connections.write().await;
        debug!("[CONN WRITE]: lock obtained {msg_id:?} to {node_id:?}");

        // let's double check we havent got a connection meanwhile
        if let Some(conn) = conns_write_lock.iter().next().map(|(_, c)| c.clone()) {
            debug!(
                "{msg_id:?} Connection already exists in Link to {node_id:?}, using that, conn_id={}",
                conn.id()
            );
            return Ok(conn);
        }

        // drop(conns_read_lock);
        // let mut conns_write_lock = self.connections.write().await;

        // let's double check we havent got a connection meanwhile
        if let Some(conn) = conns_write_lock.iter().next().map(|(_, c)| c.clone()) {
            debug!(
                "{msg_id:?} Connection already exists in Link to {node_id:?}, using that, conn_id={}",
                conn.id()
            );
            return Ok(conn);
        }

        debug!("{msg_id:?} creating conn to {node_id:?}");
        let (conn, _incoming_msgs) = self
            .endpoint
            .connect_to(&node_id.addr())
            .await
            .map_err(LinkError::Connection)?;

        debug!("{msg_id:?} conn creating done {node_id:?}");
        trace!(
            "{} to {} (id: {})",
            LogMarker::ConnectionOpened,
            conn.remote_address(),
            conn.id()
        );

        let conn_id = conn.id();
        let conn_arc = Arc::new(conn);
        let _ = conns_write_lock.insert(conn_id, conn_arc.clone());

        Ok(conn_arc)
    }
}

#[derive(Debug)]
/// Errors returned when using a Link
pub enum LinkError {
    /// Failed to connect to a node
    Connection(qp2p::ConnectionError),
    /// Failed to send a msg to a node
    Send(qp2p::SendError),
}
