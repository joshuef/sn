// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::MsgListener;

use dashmap::DashMap;
use qp2p::{Connection, Endpoint, UsrMsgBytes};
use sn_interface::messaging::MsgId;
use sn_interface::types::{log_markers::LogMarker, Peer};
use std::sync::Arc;
type ConnId = String;

/// A link to a peer in our network.
///
/// The upper layers will add incoming connections to the link,
/// and use the link to send msgs.
/// Using the link will open a connection if there is none there.
/// The link is a way to keep connections to a peer in one place
/// and use them efficiently; converge to a single one regardless of concurrent
/// comms initiation between the peers, and so on.
/// Unused connections will expire, so the Link is cheap to keep around.
/// The Link is kept around as long as the peer is deemed worth to keep contact with.
#[derive(Clone)]
pub(crate) struct Link {
    peer: Peer,
    endpoint: Endpoint,
    // pub(crate) connections: LinkConnections,
    listener: MsgListener,
}

pub(crate) type LinkConnections = Arc<DashMap<ConnId, Arc<Connection>>>;

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint, listener: MsgListener) -> Self {
        Self {
            peer,
            endpoint,
            // connections: Arc::new(DashMap::new()),
            listener,
        }
    }

    pub(crate) async fn new_with(
        peer: Peer,
        endpoint: Endpoint,
        listener: MsgListener,
        conn: Arc<Connection>,
    ) -> Self {
        let mut instance = Self::new(peer, endpoint, listener);
        instance.insert(conn);
        instance
    }

    pub(crate) fn peer(&self) -> &Peer {
        &self.peer
    }

    pub(crate) fn add(&mut self, conn: Arc<Connection>) {
        self.insert(conn);
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    #[instrument(skip_all)]
    pub(crate) async fn send_with_connection(
        bytes: UsrMsgBytes,
        priority: i32,
        conn: Arc<Connection>,
        // connections: LinkConnections,
    ) -> Result<(), SendToOneError> {
        match conn.send_with(bytes, priority).await {
            Ok(()) => Ok(()),
            Err(error) => {
                error!("Error sending out from link.. {:?}.", conn.id());

                // dont close just let the conn timeout incase msgs are coming in...
                // it's removed from out Peer tracking, so wont be used again for sending.
                Err(SendToOneError::Send(error))
            }
        }
    }

    /// Send a message using a bi-di stream and await response
    pub(crate) async fn send_on_new_bi_di_stream(
        &mut self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<UsrMsgBytes, SendToOneError> {
        trace!("Sending {msg_id:?} via a bi stream");

        let conn = match self.get_or_connect(msg_id).await {
            Ok(conn) => conn,
            Err(err) => {
                error!(
                    "{msg_id:?} Err getting connection during bi stream initialisation to: {:?}.",
                    self.peer()
                );
                return Err(err);
            }
        };

        let conn_id = conn.id();
        trace!("connection got to: {:?} {msg_id:?}", self.peer);
        let (mut send_stream, mut recv_stream) =
            match conn.open_bi().await.map_err(SendToOneError::Connection) {
                Ok(streams) => streams,
                Err(stream_opening_err) => {
                    error!("{msg_id:?} Error opening streams {stream_opening_err:?}");
                    // remove that broken conn
                    return Err(stream_opening_err);
                }
            };

        let stream_id = send_stream.id();
        trace!(
            "bidi {stream_id} openeed for {msg_id:?} to: {:?}",
            self.peer
        );
        send_stream.set_priority(10);
        match send_stream.send_user_msg(bytes.clone()).await {
            Ok(_) => {}
            Err(err) => {
                error!(
                    "Error sending bytes {msg_id:?} over stream {stream_id}: {:?}",
                    err
                );
            }
        }

        trace!("{msg_id:?} sent on {stream_id} to: {:?}", self.peer);
        send_stream.finish().await.map_err(SendToOneError::Send)?;
        // .or_else(|err| match err {
        //     qp2p::SendError::StreamLost(qp2p::StreamError::Stopped(_)) => Ok(()),
        //     _ => {
        //         error!("{msg_id:?} Error finishing up stream {stream_id}: {err:?}");
        //         Err(SendToOneError::Send(err))
        //     }
        // })?;

        trace!(
            "bidi {stream_id} finished for {msg_id:?} to: {:?}",
            self.peer
        );

        recv_stream
            .next()
            .await
            .map_err(SendToOneError::Recv)?
            .ok_or(SendToOneError::RecvClosed(self.peer))
    }

    // Gets an existing connection or creates a new one to the Link's Peer
    // Should only return still valid connections
    pub(crate) async fn get_or_connect(
        &mut self,
        msg_id: MsgId,
    ) -> Result<Arc<Connection>, SendToOneError> {
        // if self.connections.is_empty() {
        //     debug!(
        //         "{msg_id:?} attempting to create a connection to {:?}",
        //         self.peer
        //     );
        return self.create_connection(msg_id).await;
        // }

        // trace!(
        //     "{msg_id:?} Grabbing a connection from link.. {:?}",
        //     self.peer()
        // );
        // // TODO: add in simple connection check when available.
        // // we can then remove dead conns easily and return only valid conns
        // let connections = &self.connections;
        // let mut dead_conns = vec![];
        // let mut live_conn = None;

        // for entry in connections.iter() {
        //     let conn = entry.value().clone();
        //     let conn_id = conn.id();

        //     let is_valid = conn.open_bi().await.is_ok();

        //     if !is_valid {
        //         dead_conns.push(conn_id);
        //         continue;
        //     }
        //     //we have a conn
        //     live_conn = Some(conn);
        //     break;
        // }

        // // cleanup dead conns
        // for dead_conn in dead_conns {
        //     let _gone = self.connections.remove(&dead_conn);
        // }

        // if let Some(conn) = live_conn {
        //     trace!("{msg_id:?} live connection found to {:?}", self.peer());
        //     Ok(conn)
        // } else {
        //     trace!(
        //         "{msg_id:?} No live connection found to {:?}, creating a new one.",
        //         self.peer()
        //     );
        //     self.create_connection(msg_id).await
        // }
    }

    async fn create_connection(
        &mut self,
        msg_id: MsgId,
    ) -> Result<Arc<Connection>, SendToOneError> {
        debug!("{msg_id:?} create conn attempt to {:?}", self.peer);
        let (conn, incoming_msgs) = self
            .endpoint
            .connect_to(&self.peer.addr())
            .await
            .map_err(SendToOneError::Connection)?;

        trace!(
            "{msg_id:?}: {} to {} (id: {})",
            LogMarker::ConnectionOpened,
            conn.remote_address(),
            conn.id()
        );

        let conn = Arc::new(conn);

        // self.insert(conn.clone());

        self.listener.listen(conn.clone(), incoming_msgs);

        Ok(conn)
    }

    fn insert(&mut self, conn: Arc<Connection>) {
        let id = conn.id();
        debug!("Inserting connection into link store: {id:?}");

        // let _ = self.connections.insert(id.clone(), conn);
        debug!("Connection INSERTED into link store: {id:?}");
    }
}

/// Errors that can be returned from `Comm::send_to_one`.
#[derive(Debug)]
pub(crate) enum SendToOneError {
    ///
    Connection(qp2p::ConnectionError),
    ///
    Send(qp2p::SendError),
    ///
    Recv(qp2p::RecvError),
    /// Remote peer closed the bi-stream we expected a msg on
    RecvClosed(Peer),
}

impl SendToOneError {
    ///
    #[allow(unused)]
    pub(crate) fn is_local_close(&self) -> bool {
        matches!(
            self,
            SendToOneError::Connection(qp2p::ConnectionError::Closed(qp2p::Close::Local))
                | SendToOneError::Send(qp2p::SendError::ConnectionLost(
                    qp2p::ConnectionError::Closed(qp2p::Close::Local)
                ))
        )
    }
}
