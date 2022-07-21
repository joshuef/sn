// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::MsgListener;

use sn_interface::types::{log_markers::LogMarker, Peer};

use bytes::Bytes;
use priority_queue::DoublePriorityQueue;
use qp2p::{Endpoint, RetryConfig};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

type Priority = u64;
type ConnId = usize;

// Capacity is needed since we cannot control how many connections
// another node opens to us (however, if they run the same code that
// we do, they would open very few connections).
// 255 is way more than we need and expect, thus gives ample room for
// unforseen bursts, but at the same time puts a sane cap on the max
// number continuously held for an - obviously malfunctioning - peer (i.e. edge case).
const CAPACITY: usize = 255;
const UNUSED_TTL: Duration = Duration::from_secs(120);

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
pub(crate) struct Link {
    peer: Peer,
    endpoint: Endpoint,
    connections: BTreeMap<ConnId, ExpiringConn>,
    queue: DoublePriorityQueue<ConnId, Priority>,
    access_counter: u64,
    listener: MsgListener,
    expiration_check: Instant,
}

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint, listener: MsgListener) -> Self {
        Self {
            peer,
            endpoint,
            connections: BTreeMap::new(),
            queue: DoublePriorityQueue::new(),
            access_counter: 0,
            listener,
            expiration_check: expiration(),
        }
    }

    pub(crate) fn new_with(
        peer: Peer,
        endpoint: Endpoint,
        listener: MsgListener,
        conn: qp2p::Connection,
    ) -> Self {
        let mut instance = Self::new(peer, endpoint, listener);
        instance.insert(conn);
        instance
    }

    pub(crate) fn peer(&self) -> &Peer {
        &self.peer
    }

    pub(crate) fn add(&mut self, conn: qp2p::Connection) {
        self.insert(conn);
    }

    /// Disposes of the link and all underlying resources.
    /// Also any clones of this link that are held, will be cleaned up.
    /// This is due to the fact that we do never leak the `qp2p::Connection` outside of this struct,
    /// since that struct is cloneable and uses Arc internally.
    pub(crate) fn disconnect(&mut self) {
        self.queue.clear();
        for item in self.connections.values() {
            item.conn
                .close(Some("We disconnected from peer.".to_string()));
        }
        self.connections.clear();

    }

    /// Send a message to the peer with default retry configuration.
    ///
    /// The message will be sent on a unidirectional QUIC stream, meaning the application is
    /// responsible for correlating any anticipated responses from incoming streams.
    ///
    /// The priority will be `0` and retry behaviour will be determined by the
    /// [`Config`](crate::Config) that was used to construct the [`Endpoint`] this connection
    /// belongs to. See [`send_with`](Self::send_with) if you want to send a message with specific
    /// configuration.
    #[allow(unused)]
    pub(crate) async fn send(&mut self, msg: Bytes) -> Result<(), SendToOneError> {
        self.send_with(msg, 0, None).await
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    #[instrument(skip_all)]
    pub(crate) async fn send_with(
        &mut self,
        msg: Bytes,
        priority: i32,
        retry_config: Option<&RetryConfig>,
    ) -> Result<(), SendToOneError> {
        let conn = self.get_or_connect().await?;
        trace!(
            "We have {} open connections to node {:?}.",
            self.queue.len(),
            self.peer
        );
        match conn.send_with(msg, priority, retry_config).await {
            Ok(()) => {
                #[cfg(feature = "back-pressure")]
                self.listener.count_msg().await;

                Ok(())
            }
            Err(error) => {
                error!("dropping connection due to error, {error:?}");
                // clean up failing connections at once, no nead to leak it outside of here
                // next send (e.g. when retrying) will use/create a new connection
                let id = &conn.id();
                let conn = self.connections.remove(id);
                let _ = self.queue.remove(id);

                if let Some(conn) = conn {
                    conn.close(Some(format!("{:?}", error)));
                }
                Err(SendToOneError::Send(error))
            }
        }
    }

    async fn get_or_connect(&mut self) -> Result<qp2p::Connection, SendToOneError> {
        // get the most recently used connection
        match self.queue.peek_max().map(|(id, _prio)| *id) {
            None => self.create_connection().await,
            Some(id) => self.read_conn(id).await,
        }
    }

    /// Is this Link currently connected?
    #[allow(unused)]
    pub(crate) fn is_connected(&self) -> bool {
        // get the most recently used connection

        self.queue
            .peek_max()
            .and_then(|(id, _)| self.connections.get(id))
            .map(|conn| !conn.expired())
            .unwrap_or(false)
    }

    async fn read_conn(&mut self, id: usize) -> Result<qp2p::Connection, SendToOneError> {
        match self.connections.get(&id).cloned() {
            Some(item) => {
                self.touch(item.conn.id());
                Ok(item.conn)
            }
            None => self.create_connection().await,
        }
    }

    async fn create_connection(&mut self) -> Result<qp2p::Connection, SendToOneError> {
        let (conn, incoming_msgs) = self
            .endpoint
            .connect_to(&self.peer.addr())
            .await
            .map_err(SendToOneError::Connection)?;

        trace!(
            "{} to {} (id: {})",
            LogMarker::ConnectionOpened,
            conn.remote_address(),
            conn.id()
        );

        self.insert(conn.clone());

        self.listener.listen(conn.clone(), incoming_msgs);

        Ok(conn)
    }

    fn insert(&mut self, conn: qp2p::Connection) {
        let id = conn.id();

        let _ = self.connections.insert(id, ExpiringConn::new(conn));

        let prio = self.priority();
        let _ = self.queue.push(id, prio);
    }

    fn touch(&mut self, id: ConnId) {
        let prio = self.priority();
        let _ = self.queue.change_priority(&id, prio);

        if let Some(conn) = self.connections.get_mut(&id) {
            conn.touch()
        }
    }

    fn priority(&mut self) -> Priority {
        if self.access_counter == u64::MAX {
            // after u64::MAX connections to this peer (very unlikely), we need to update the prios
            let sorted_queue = self.queue.clone().into_sorted_iter();
            // update all prios, starting from zero prio again
            for (new_prio, (id, _old_prio)) in sorted_queue.enumerate() {
                let _ = self.queue.change_priority(&id, new_prio as u64);
            }

            self.access_counter = self.queue.len() as u64;
        }

        self.access_counter = self.access_counter.saturating_add(1);

        self.access_counter
    }

    /// Remove expired connections.
    pub(crate) fn remove_expired(&mut self) {
        if Instant::now() < self.expiration_check {
            return;
        }

        self.expiration_check = expiration();

        let mut expired_ids = vec![];

        // the iter is sorted from lowest to highest
        for (id, _old_prio) in self.queue.clone().into_sorted_iter() {
            if 1 + expired_ids.len() >= self.queue.len() {
                break;
            }

            if let Some(conn) = self.connections.get_mut(&id) {
                if conn.expired() {
                    expired_ids.push(id);
                }
            }
        }

        for id in expired_ids {
            let _ = self.queue.remove(&id);

            if let Some(item) = self.connections.remove(&id) {
                trace!("Connection expired: {}", item.conn.id());
                item.conn.close(Some("Connection expired and so closing.".to_string()));
            }
        }

        self.drop_excess();
    }

    /// Remove connections that exceed capacity, oldest first.
    fn drop_excess(&mut self) {
        if self.queue.len() >= CAPACITY {
            // remove the least recently used connections
            if let Some((evicted_id, _)) = self.queue.pop_min() {
                if let Some(item) = self.connections.remove(&evicted_id) {
                    trace!("Connection evicted: {}", evicted_id);
                    item.conn.close(Some("Connection evicted.".to_string()));
                }
            }
        }
    }
}

/// Errors that can be returned from `Comm::send_to_one`.
#[derive(Debug)]
pub(crate) enum SendToOneError {
    ///
    Connection(qp2p::ConnectionError),
    ///
    Send(qp2p::SendError),
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

#[derive(Clone, Debug)]
struct ExpiringConn {
    conn: qp2p::Connection,
    expiry: Instant,
}

impl ExpiringConn {
    fn new(conn: qp2p::Connection) -> Self {
        ExpiringConn {
            conn,
            expiry: expiration(),
        }
    }

    fn expired(&self) -> bool {
        self.expiry < Instant::now()
    }

    fn touch(&mut self) {
        self.expiry = expiration();
    }
}

fn expiration() -> Instant {
    Instant::now() + UNUSED_TTL
}
