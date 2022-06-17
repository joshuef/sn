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
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

type Priority = u64;
type ConnId = usize;

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
#[derive(Clone)]
pub(crate) struct Link {
    peer: Peer,
    endpoint: Endpoint,
    create_mutex: Rc<Mutex<usize>>,
    connections: Rc<RefCell<BTreeMap<ConnId, ExpiringConn>>>,
    queue: Rc<RefCell<DoublePriorityQueue<ConnId, Priority>>>,
    access_counter: Rc<AtomicU64>,
    listener: MsgListener,
    expiration_check: Rc<RefCell<Instant>>,
}

impl Link {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint, listener: MsgListener) -> Self {
        Self {
            peer,
            endpoint,
            create_mutex: Rc::new(Mutex::new(0)),
            connections: Rc::new(RefCell::new(BTreeMap::new())),
            queue: Rc::new(RefCell::new(DoublePriorityQueue::new())),
            access_counter: Rc::new(AtomicU64::new(0)),
            listener,
            expiration_check: Rc::new(RefCell::new(expiration())),
        }
    }

    pub(crate) async fn new_with(
        peer: Peer,
        endpoint: Endpoint,
        listener: MsgListener,
        conn: qp2p::Connection,
    ) -> Self {
        let instance = Self::new(peer, endpoint, listener);
        instance.insert(conn).await;
        instance
    }

    #[cfg(test)]
    pub(crate) fn peer(&self) -> &Peer {
        &self.peer
    }

    pub(crate) async fn add(&self, conn: qp2p::Connection) {
        self.insert(conn).await;
    }

    /// Disposes of the link and all underlying resources.
    /// Also any clones of this link that are held, will be cleaned up.
    /// This is due to the fact that we do never leak the qp2p::Connection outside of this struct,
    /// since that struct is cloneable and uses Rc internally.
    pub(crate) async fn disconnect(self) {
        let _ = self.queue.borrow_mut().clear();
        let mut guard = self.connections.borrow_mut();
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
    /// [`Config`](crate::Config) that was used to construct the [`Endpoint`] this connection
    /// belongs to. See [`send_with`](Self::send_with) if you want to send a message with specific
    /// configuration.
    #[allow(unused)]
    pub(crate) async fn send(&self, msg: Bytes) -> Result<(), SendToOneError> {
        self.send_with(msg, 0, None).await
    }

    /// Send a message to the peer using the given configuration.
    ///
    /// See [`send`](Self::send) if you want to send with the default configuration.
    #[instrument(skip_all)]
    pub(crate) async fn send_with(
        &self,
        msg: Bytes,
        priority: i32,
        retry_config: Option<&RetryConfig>,
    ) -> Result<(), SendToOneError> {
        let conn = self.get_or_connect().await?;
        let queue_len = { self.queue.borrow().len() };
        trace!(
            "We have {} open connections to node {:?}.",
            queue_len,
            self.peer
        );
        match conn.send_with(msg, priority, retry_config).await {
            Ok(()) => {
                #[cfg(feature = "back-pressure")]
                self.listener.count_msg().await;

                Ok(())
            }
            Err(error) => {
                // clean up failing connections at once, no nead to leak it outside of here
                // next send (e.g. when retrying) will use/create a new connection
                let id = &conn.id();
                {
                    let _ = self.connections.borrow_mut().remove(id);
                }
                {
                    let _ = self.queue.borrow_mut().remove(id);
                }
                conn.close(Some(format!("{:?}", error)));
                Err(SendToOneError::Send(error))
            }
        }
    }

    async fn get_or_connect(&self) -> Result<qp2p::Connection, SendToOneError> {
        // get the most recently used connection
        let res = { self.queue.borrow().peek_max().map(|(id, _prio)| *id) };
        match res {
            None => {
                // if none found, funnel one caller through at a time
                let _lock = self.create_mutex.lock().await;
                // read again
                // first caller will find none again, but the subsequent callers
                // will access only after the first one finished creating a new connection
                // thus will find a connection here:
                let res = { self.queue.borrow().peek_max().map(|(id, _prio)| *id) };
                if let Some(id) = res {
                    self.read_conn(id).await
                } else {
                    self.create_connection().await
                }
            }
            Some(id) => self.read_conn(id).await,
        }
    }

    /// Is this Link currently connected?
    pub(crate) async fn is_connected(&self) -> bool {
        // get the most recently used connection
        let res = { self.queue.borrow().peek_max().map(|(id, _prio)| *id) };
        match res {
            None => false,
            Some(id) => {
                let expiring = self.connections.borrow().get(&id).cloned();
                match expiring {
                    Some(conn) => !conn.expired().await,
                    None => false,
                }
            }
        }
    }

    async fn read_conn(&self, id: usize) -> Result<qp2p::Connection, SendToOneError> {
        let res = { self.connections.borrow().get(&id).cloned() };
        match res {
            Some(item) => {
                self.touch(item.conn.id()).await;
                Ok(item.conn)
            }
            None => self.create_connection().await,
        }
    }

    async fn create_connection(&self) -> Result<qp2p::Connection, SendToOneError> {
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

        self.listener.listen(conn.clone(), incoming_msgs);

        Ok(conn)
    }

    async fn insert(&self, conn: qp2p::Connection) {
        let id = conn.id();

        {
            let _ = self
                .connections
                .borrow_mut()
                .insert(id, ExpiringConn::new(conn));
        }
        {
            let prio = self.priority().await;
            let _ = self.queue.borrow_mut().push(id, prio);
        }
    }

    async fn touch(&self, id: ConnId) {
        {
            let prio = self.priority().await;
            let _old_prio = self.queue.borrow_mut().change_priority(&id, prio);
        }
        {
            let conns = self.connections.borrow().clone();
            let conn = conns.get(&id);

            if let Some(conn) = conn {
                conn.touch().await
            }
        }
    }

    async fn priority(&self) -> Priority {
        let prio = self.access_counter.fetch_add(1, Ordering::SeqCst);
        if prio == u64::MAX {
            // after u64::MAX connections to this peer (very unlikely), we need to update the prios
            let mut queue = self.queue.borrow_mut();

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
    pub(crate) async fn remove_expired(&self) {
        if Instant::now() > { *self.expiration_check.borrow() } {
            *self.expiration_check.borrow_mut() = expiration();
        } else {
            return;
        }

        let queue = {
            let queue = self.queue.borrow();
            // take a clone of the connections
            queue.clone()
        };

        let mut remaining = queue.len();
        let mut expired_ids = vec![];

        // the iter is sorted from lowest to highest
        for (id, _old_prio) in queue.into_sorted_iter() {
            if remaining <= 1 {
                break;
            }
            let read_items = self.connections.borrow().clone();
            if let Some(conn) = read_items.get(&id) {
                if conn.expired().await {
                    expired_ids.push(id);
                    remaining -= 1;
                }
            }
        }

        for id in expired_ids {
            {
                let _ = self.queue.borrow_mut().remove(&id);
            }
            // within braces as to not hold a lock to our data during subsequent call to the cleanup fn
            let removed = { self.connections.borrow_mut().remove(&id) };
            if let Some(item) = removed {
                trace!("Connection expired: {}", item.conn.id());
                item.conn.close(Some("Connection expired.".to_string()));
            }
        }

        self.drop_excess().await;
    }

    /// Remove connections that exceed capacity, oldest first.
    async fn drop_excess(&self) {
        let len = { self.queue.borrow().len() };
        if len >= CAPACITY as usize {
            // remove the least recently used connections
            let popped = { self.queue.borrow_mut().pop_min() };
            if let Some((evicted_id, _)) = popped {
                let removed = { self.connections.borrow_mut().remove(&evicted_id) };
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
    expiry: Rc<RefCell<Instant>>,
}

impl ExpiringConn {
    fn new(conn: qp2p::Connection) -> Self {
        ExpiringConn {
            conn,
            expiry: Rc::new(RefCell::new(expiration())),
        }
    }

    async fn expired(&self) -> bool {
        *self.expiry.borrow() < Instant::now()
    }

    async fn touch(&self) {
        *self.expiry.borrow_mut() = expiration();
    }
}

fn expiration() -> Instant {
    Instant::now() + UNUSED_TTL
}
