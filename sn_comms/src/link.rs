// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use dashmap::DashMap;
use qp2p::{Connection, Endpoint, UsrMsgBytes};
use sn_interface::messaging::MsgId;
use sn_interface::types::{log_markers::LogMarker, Peer};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
type ConnId = String;

const CONN_RETRY_WAIT: Duration = Duration::from_millis(100);
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
    pub(crate) connections: LinkConnections,
}

pub(crate) type LinkConnections = Arc<DashMap<ConnId, Arc<Connection>>>;

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint) -> Self {
        Self {
            peer,
            endpoint,
            connections: Arc::new(DashMap::new()),
        }
    }

    pub(crate) fn peer(&self) -> &Peer {
        &self.peer
    }

    pub(crate) fn remove(&mut self, conn: Arc<Connection>) {
        let conn_id = conn.id();
        debug!("Removing connection from link store: {conn_id}");

        let _ = self.connections.remove(&conn_id);
        debug!("Connection REMOVED from link store: {conn_id}");
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    #[instrument(skip_all)]
    pub(crate) async fn send_with_connection(
        bytes: UsrMsgBytes,
        priority: i32,
        conn: Arc<Connection>,
        connections: LinkConnections,
    ) -> Result<(), SendToOneError> {
        let conn_id = conn.id();
        trace!(
            "We have {} open connections to node {conn_id}.",
            connections.len()
        );

        conn.send_with(bytes, priority).await.map_err(|error| {
            error!(
                "Error sending out from link... We have {} open connections to node {conn_id}.",
                connections.len()
            );
            // clean up failing connections at once, no nead to leak it outside of here
            // next send (e.g. when retrying) will use/create a new connection
            // Timeouts etc should register instantly so we should clean those up fair fast
            let _ = connections.remove(&conn_id);

            debug!("Connection removed from link: {conn_id}");
            // dont close just let the conn timeout incase msgs are coming in...
            // it's removed from out Peer tracking, so won't be used again for sending.
            SendToOneError::Send(error)
        })
    }

    /// Send a message using a bi-di stream and await response
    /// When sending a msg to a peer, if it fails with an existing
    /// cached connection, it will keep retrying till it either:
    /// a. finds another cached connection which it succeeded with,
    /// b. or it cleaned them all up from the cache creating a new connection
    ///    to the peer as last attempt.
    pub(crate) async fn send_on_new_bi_di_stream(
        &mut self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<UsrMsgBytes, SendToOneError> {
        let peer = self.peer;
        trace!(
            "Sending {msg_id:?} via a bi-stream to {peer:?}, we have {} cached connections.",
            self.connections.len()
        );
        let mut attempt = 0;
        loop {
            let conn = self
                .connections
                .iter()
                .next()
                .map(|entry| entry.value().clone());

            let (conn, is_last_attempt) = if let Some(conn) = conn {
                trace!(
                    "Sending {msg_id:?} via bi-di-stream over existing connection {}, attempt #{attempt}.",
                    conn.id()
                );
                (conn, false)
            } else {
                trace!("Sending {msg_id:?} via bi-di-stream over new connection to {peer:?}, attempt #{attempt}.");
                let conn = self.create_connection(msg_id).await?;
                (conn, true)
            };

            attempt += 1;

            let conn_id = conn.id();
            trace!("Connection {conn_id} got to {peer:?} for {msg_id:?}");
            let (mut send_stream, mut recv_stream) = match conn.open_bi().await {
                Ok(bi_stream) => bi_stream,
                Err(err) => {
                    error!("{msg_id:?} Error opening bi-stream over {conn_id}: {err:?}");
                    // remove that broken conn
                    let _conn = self.connections.remove(&conn_id);
                    match is_last_attempt {
                        true => {
                            error!("Last attempt reached for {msg_id:?}, erroring out...");
                            break Err(SendToOneError::Connection(err));
                        }
                        false => {
                            // tiny wait for comms/dashmap to cope with removal
                            sleep(CONN_RETRY_WAIT).await;
                            continue;
                        }
                    }
                }
            };

            let stream_id = send_stream.id();
            trace!("bidi {stream_id} opened for {msg_id:?} to {peer:?}");
            send_stream.set_priority(10);
            if let Err(err) = send_stream.send_user_msg(bytes.clone()).await {
                error!("Error sending bytes for {msg_id:?} over {stream_id}: {err:?}");
                // remove that broken conn
                let _conn = self.connections.remove(&conn_id);
                match is_last_attempt {
                    true => break Err(SendToOneError::Send(err)),
                    false => {
                        // tiny wait for comms/dashmap to cope with removal
                        sleep(CONN_RETRY_WAIT).await;
                        continue;
                    }
                }
            }

            trace!("{msg_id:?} sent on {stream_id} to {peer:?}");

            // unblock + move finish off thread as it's not strictly related to the sending of the msg.
            let stream_id_clone = stream_id.clone();
            let _handle = tokio::spawn(async move {
                // Attempt to gracefully terminate the stream.
                // If this errors it does _not_ mean our message has not been sent
                let result = send_stream.finish().await;
                trace!("{msg_id:?} finished {stream_id_clone} to {peer:?}: {result:?}");
            });

            match recv_stream.next().await {
                Ok(Some(response)) => break Ok(response),
                Ok(None) => {
                    error!(
                        "Stream closed by peer when awaiting response to {msg_id:?} from {peer:?} over {stream_id}."
                    );
                    let _conn = self.connections.remove(&conn_id);

                    if is_last_attempt {
                        break Err(SendToOneError::RecvClosed(peer));
                    }
                    // tiny wait for comms/dashmap to cope with removal
                    sleep(CONN_RETRY_WAIT).await;
                }
                Err(err) => {
                    error!("Error receiving response to {msg_id:?} from {peer:?} over {stream_id}: {err:?}");
                    let _conn = self.connections.remove(&conn_id);
                    if is_last_attempt {
                        break Err(SendToOneError::Recv(err));
                    }

                    // tiny wait for comms/dashmap to cope with removal
                    sleep(CONN_RETRY_WAIT).await;
                }
            }
        }
    }

    // Gets an existing connection or creates a new one to the Link's Peer
    pub(super) async fn get_or_connect(
        &mut self,
        msg_id: MsgId,
    ) -> Result<Arc<Connection>, SendToOneError> {
        let peer = self.peer;
        trace!("{msg_id:?} Grabbing a connection from link store to {peer:?}");

        let conn = self
            .connections
            .iter()
            .next()
            .map(|entry| entry.value().clone());
        if let Some(conn) = conn {
            trace!("{msg_id:?} Connection found to {peer:?}");
            Ok(conn)
        } else {
            trace!("{msg_id:?} No connection found to {peer:?}, creating a new one.");
            self.create_connection(msg_id).await
        }
    }

    async fn create_connection(
        &mut self,
        msg_id: MsgId,
    ) -> Result<Arc<Connection>, SendToOneError> {
        debug!("{msg_id:?} create conn attempt to {:?}", self.peer);
        let (conn, _) = self
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

        self.insert(conn.clone());

        Ok(conn)
    }

    fn insert(&mut self, conn: Arc<Connection>) {
        let conn_id = conn.id();
        debug!("Inserting connection into link store: {conn_id}");

        let _ = self.connections.insert(conn_id.clone(), conn);
        debug!("Connection INSERTED into link store: {conn_id}");
    }

    /// Returns if the link has any connections cached
    pub(crate) fn has_connections(&self) -> bool {
        !self.connections.is_empty()
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
