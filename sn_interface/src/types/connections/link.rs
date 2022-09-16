// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Peer;

use crate::types::log_markers::LogMarker;
use qp2p::UsrMsgBytes;

use priority_queue::DoublePriorityQueue;
use qp2p::{ConnectionIncoming as IncomingMsgs, Endpoint, RetryConfig};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
use xor_name::XorName;

type Priority = u64;
type ConnId = String;

// Capacity is needed since we cannot control how many connections
// another node opens to us (however, if they run the same code that
// we do, they would open very few connections).
// 255 is way more than we need and expect, thus gives ample room for
// unforseen bursts, but at the same time puts a sane cap on the max
// number continuously held for an - obviously malfunctioning - peer (i.e. edge case).
const CAPACITY: u8 = u8::MAX;
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
#[derive(Clone, Debug)]
pub struct Link {
    peer: Peer,
    endpoint: Endpoint,
    create_mutex: Arc<Mutex<usize>>,
    connections: Arc<RwLock<BTreeMap<ConnId, ExpiringConn>>>,
    queue: Arc<RwLock<DoublePriorityQueue<ConnId, Priority>>>,
    access_counter: Arc<AtomicU64>,
}

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint) -> Self {
        Self {
            peer,
            endpoint,
            create_mutex: Arc::new(Mutex::new(0)),
            connections: Arc::new(RwLock::new(BTreeMap::new())),
            queue: Arc::new(RwLock::new(DoublePriorityQueue::new())),
            access_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) async fn new_with(id: Peer, endpoint: Endpoint, conn: qp2p::Connection) -> Self {
        let instance = Self::new(id, endpoint);
        instance.insert(conn).await;
        instance
    }

    #[allow(unused)]
    pub(crate) fn name(&self) -> XorName {
        self.peer.name()
    }

    pub(crate) async fn add(&self, conn: qp2p::Connection) {
        self.insert(conn).await;
    }

    /// Disposes of the link and all underlying resources.
    /// Also any clones of this link that are held, will be cleaned up.
    /// This is due to the fact that we do never leak the `qp2p::Connection` outside of this struct,
    /// since that struct is cloneable and uses Arc internally.
    pub async fn disconnect(self) {
        self.queue.write().await.clear();
        let mut guard = self.connections.write().await;
        for (_, item) in guard.iter() {
            item.conn
                .close(Some("We disconnected from peer.".to_string()));
        }
        guard.clear();
    }

    /// Send a message to the peer with default retry configuration.
    ///
    /// The message will be sent on a unidirectional QUIC stream, meaning the application is
    /// responsible for correlating any anticipated responses from incoming streams.
    ///
    /// The priority will be `0` and retry behaviour will be determined by the
    /// [`RetryConfig`] that was used to construct the [`Endpoint`] this connection
    /// belongs to. See [`send_with`](Self::send_with) if you want to send a message with specific
    /// configuration.
    #[allow(unused)]
    pub async fn send<F: Fn(qp2p::Connection, IncomingMsgs)>(
        &self,
        bytes: UsrMsgBytes,
        listen: F,
    ) -> Result<(), SendToOneError> {
        self.send_with(bytes, None, listen).await
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    #[instrument(skip_all)]
    pub async fn send_with<F: Fn(qp2p::Connection, IncomingMsgs)>(
        &self,
        bytes: UsrMsgBytes,
        retry_config: Option<&RetryConfig>,
        listen: F,
    ) -> Result<(), SendToOneError> {
        let default_priority = 10;
        let conn = self.get_or_connect(listen).await?;
        let queue_len = { self.queue.read().await.len() };
        trace!(
            "We have {} open connections to peer {}.",
            queue_len,
            self.peer
        );
        match conn.send_with(bytes, default_priority, retry_config).await {
            Ok(()) => {
                self.remove_expired().await;
                Ok(())
            }
            Err(error) => {
                // clean up failing connections at once, no nead to leak it outside of here
                // next send (e.g. when retrying) will use/create a new connection
                let id = &conn.id();

                // We could write just `self.queue.remove(id)`, but the library warns for `unused_results`.
                {
                    let _ = self.connections.write().await.remove(id);
                    let _ = self.queue.write().await.remove(id);
                }
                conn.close(Some(format!("{:?}", error)));
                Err(SendToOneError::Send(error))
            }
        }
    }

    async fn get_or_connect<F: Fn(qp2p::Connection, IncomingMsgs)>(
        &self,
        listen: F,
    ) -> Result<qp2p::Connection, SendToOneError> {
        // get the most recently used connection
        let res = {
            self.queue
                .read()
                .await
                .peek_max()
                .map(|(id, _prio)| id.clone())
        };

        match res {
            None => {
                // if none found, funnel one caller through at a time
                let _lock = self.create_mutex.lock().await;
                // read again
                // first caller will find none again, but the subsequent callers
                // will access only after the first one finished creating a new connection
                // thus will find a connection here:
                let res = {
                    self.queue
                        .read()
                        .await
                        .peek_max()
                        .map(|(id, _prio)| id.clone())
                };
                if let Some(id) = res {
                    self.read_conn(id, listen).await
                } else {
                    debug!("creating conn");
                    self.create_connection(listen).await
                }
            }
            Some(id) => self.read_conn(id, listen).await,
        }
    }

    /// Is this Link currently connected?
    pub(crate) async fn is_connected(&self) -> bool {
        self.remove_expired().await;
        // get the most recently used connection
        let res = {
            self.queue
                .read()
                .await
                .peek_max()
                .map(|(id, _prio)| id.clone())
        };
        match res {
            None => false,
            Some(id) => match self.connections.read().await.get(&id) {
                Some(conn) => conn.expired().await,
                None => false,
            },
        }
    }

    async fn read_conn<F: Fn(qp2p::Connection, IncomingMsgs)>(
        &self,
        id: ConnId,
        listen: F,
    ) -> Result<qp2p::Connection, SendToOneError> {
        debug!("reading existing conn");

        let res = { self.connections.read().await.get(&id).cloned() };
        match res {
            Some(item) => {
                self.touch(item.conn.id()).await;
                Ok(item.conn)
            }
            None => {
                debug!(
                    "reading existing conn failed... so we're making a new one... to {:?}",
                    self.peer
                );
                self.create_connection(listen).await
            }
        }
    }

    async fn create_connection<F: Fn(qp2p::Connection, IncomingMsgs)>(
        &self,
        listen: F,
    ) -> Result<qp2p::Connection, SendToOneError> {
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

        self.insert(conn.clone()).await;

        listen(conn.clone(), incoming_msgs);

        Ok(conn)
    }

    async fn insert(&self, conn: qp2p::Connection) {
        let id = conn.id();

        {
            let _ = self
                .connections
                .write()
                .await
                .insert(id.clone(), ExpiringConn::new(conn));
        }
        {
            let _ = self.queue.write().await.push(id, self.priority().await);
        }

        self.drop_excess().await;
    }

    async fn touch(&self, id: ConnId) {
        {
            let _ = self
                .queue
                .write()
                .await
                .change_priority(&id, self.priority().await);
        }
        {
            if let Some(conn) = self.connections.read().await.get(&id) {
                conn.touch().await
            }
        }
    }

    async fn priority(&self) -> Priority {
        let prio = self.access_counter.fetch_add(1, Ordering::SeqCst);
        if prio == u64::MAX {
            // after u64::MAX connections to this peer (very unlikely), we need to update the prios
            let mut queue = self.queue.write().await;

            // take a clone of the connections
            let clone = queue.clone();

            // update all prios, starting from zero prio again
            // the iter is sorted from lowest to highest, and the first call after prio == u64::MAX will overflow and give 0.
            for (id, _old_prio) in clone.into_sorted_iter() {
                let _ =
                    queue.change_priority(&id, self.access_counter.fetch_add(1, Ordering::SeqCst));
            }

            // return next prio to the original caller
            self.access_counter.fetch_add(1, Ordering::SeqCst)
        } else {
            prio
        }
    }

    /// Remove expired connections.
    async fn remove_expired(&self) {
        let mut expired_ids = vec![];
        {
            let read_items = self.connections.read().await;
            for (id, conn) in read_items.iter() {
                if conn.expired().await {
                    expired_ids.push(id.clone());
                }
            }
        }

        for id in expired_ids {
            {
                let _ = self.queue.write().await.remove(&id);
            }
            // within braces as to not hold a lock to our data during subsequent call to the cleanup fn
            let removed = { self.connections.write().await.remove(&id) };
            if let Some(item) = removed {
                trace!("Connection expired: {}", item.conn.id());
                item.conn.close(Some("Connection expired.".to_string()));
            }
        }
    }

    /// Remove connections that exceed capacity, oldest first.
    async fn drop_excess(&self) {
        let len = { self.queue.read().await.len() };
        if len >= CAPACITY as usize {
            // remove the least recently used connections
            let popped = { self.queue.write().await.pop_min() };
            if let Some((evicted_id, _)) = popped {
                let removed = { self.connections.write().await.remove(&evicted_id) };
                if let Some(item) = removed {
                    trace!("Connection evicted: {}", evicted_id);
                    item.conn.close(Some("Connection evicted.".to_string()));
                }
            }
        }
    }
}

/// Errors that can be returned from `Comm::send_to_one`.
#[derive(Debug)]
pub enum SendToOneError {
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
            Self::Connection(qp2p::ConnectionError::Closed(qp2p::Close::Local))
                | Self::Send(qp2p::SendError::ConnectionLost(
                    qp2p::ConnectionError::Closed(qp2p::Close::Local)
                ))
        )
    }
}

#[derive(Clone, Debug)]
struct ExpiringConn {
    conn: qp2p::Connection,
    expiry: Arc<RwLock<Instant>>,
}

impl ExpiringConn {
    fn new(conn: qp2p::Connection) -> Self {
        Self {
            conn,
            expiry: Arc::new(RwLock::new(expiration())),
        }
    }

    async fn expired(&self) -> bool {
        *self.expiry.read().await < Instant::now()
    }

    async fn touch(&self) {
        *self.expiry.write().await = expiration();
    }
}

fn expiration() -> Instant {
    Instant::now() + UNUSED_TTL
}
