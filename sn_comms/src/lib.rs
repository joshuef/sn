// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Comms for the SAFE Network.
//! All comms with nodes are done though this.

// For quick_error
#![recursion_limit = "256"]
#![doc(
    html_logo_url = "https://github.com/maidsafe/QA/raw/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// Forbid some very bad patterns. Forbid is stronger than `deny`, preventing us from suppressing the
// lint with `#[allow(...)]` et-all.
#![forbid(
    arithmetic_overflow,
    mutable_transmutes,
    no_mangle_const_items,
    unknown_crate_types,
    unsafe_code
)]
// Turn on some additional warnings to encourage good style.
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    clippy::unicode_not_nfc,
    clippy::unwrap_used
)]

#[macro_use]
extern crate tracing;

mod error;
mod link;
mod listener;
mod peer_session;

pub use self::error::{Error, Result};

use self::{
    link::Link,
    listener::{ConnectionEvent, MsgListener},
    peer_session::{PeerSession, SendStatus, SendWatcher},
};

use sn_interface::{
    messaging::{MsgId, WireMsg},
    types::{log_markers::LogMarker, Peer},
};

use qp2p::{Connection, Endpoint, IncomingConnections, SendStream, UsrMsgBytes};

use dashmap::DashMap;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    sync::RwLock,
    task,
};

/// Standard channel size, to allow for large swings in throughput
static STANDARD_CHANNEL_SIZE: usize = 100_000;

/// A msg received on the wire.
#[derive(Debug)]
pub struct MsgFromPeer {
    /// The peer that sent us the msg.
    pub sender: Peer,
    /// The msg that we received.
    pub wire_msg: WireMsg,
    /// An optional stream to return msgs on, if
    /// this msg came on a bidi-stream.
    pub send_stream: Option<SendStream>,
}

/// Communication component of the node to interact with other nodes.
#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct Comm {
    our_endpoint: Endpoint,
    outgoing_sessions: Arc<DashMap<Peer, PeerSession>>,
    members: Arc<RwLock<BTreeSet<Peer>>>,
}

impl Comm {
    /// Creates a new instance of Comm with an endpoint
    /// and starts listening to the incoming messages from other nodes.
    #[tracing::instrument(skip_all)]
    pub async fn new(
        local_addr: SocketAddr,
        config: qp2p::Config,
        incoming_msg_pipe: Sender<MsgFromPeer>,
    ) -> Result<Self> {
        let (our_endpoint, incoming_connections, _) =
            Endpoint::new_peer(local_addr, Default::default(), config).await?;

        let (add_connection, conn_events_recv) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let msg_listener = MsgListener::new(add_connection, incoming_msg_pipe);

        let comm = Comm {
            our_endpoint,
            outgoing_sessions: Arc::new(DashMap::new()),
            members: Arc::new(RwLock::new(BTreeSet::new())),
        };

        let _ = task::spawn(handle_connection_events(comm.clone(), conn_events_recv));

        listen_for_incoming_msgs(msg_listener, incoming_connections);

        Ok(comm)
    }

    /// The socket address of our endpoint.
    pub fn socket_addr(&self) -> SocketAddr {
        self.our_endpoint.public_addr()
    }

    /// Fake function used as replacement for testing only.
    ///
    /// NB: Testing this from an external crate will require referencing
    /// this crate with the feature "test", otherwise this fn will not be called.
    /// <https://github.com/rust-lang/rust/issues/59168#issuecomment-962214945>
    #[cfg(any(test, feature = "test"))]
    pub async fn is_reachable(&self, _peer: &SocketAddr) -> Result<(), Error> {
        Ok(())
    }

    /// Tests whether the peer is reachable.
    ///
    /// NB: If testing comms from an external crate, this fn will be called instead
    /// of the intended test fn above, unless referencing this crate with the feature "test".
    /// <https://github.com/rust-lang/rust/issues/59168#issuecomment-962214945>
    #[cfg(not(any(test, feature = "test")))]
    pub async fn is_reachable(&self, peer: &SocketAddr) -> Result<(), Error> {
        let qp2p_config = qp2p::Config {
            ..Default::default()
        };

        let connectivity_endpoint =
            Endpoint::new_client((self.our_endpoint.local_addr().ip(), 0), qp2p_config)?;

        let result = connectivity_endpoint
            .is_reachable(peer)
            .await
            .map_err(|err| {
                info!("Peer {} is NOT externally reachable: {:?}", peer, err);
                err.into()
            })
            .map(|()| {
                info!("Peer {} is externally reachable.", peer);
            });
        connectivity_endpoint.close();
        result
    }

    /// Closes the endpoint.
    pub fn close_endpoint(&self) {
        self.our_endpoint.close()
    }

    /// Updates cached connections for passed members set only.
    pub async fn update_members(&mut self, members: BTreeSet<Peer>) {
        let new_members = members.clone();
        *self.members.write().await = members;
        self.outgoing_sessions
            .retain(|p, _| new_members.contains(p));
    }

    /// Sends the payload on a new or existing connection,
    /// or on the provided send stream if any.
    #[tracing::instrument(skip(self, bytes))]
    pub async fn send_out_bytes(
        &self,
        peer: Peer,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<()> {
        let watcher = self.send_to_one(peer, msg_id, bytes).await;

        let sessions = self.outgoing_sessions.clone();

        // TODO: Is there an optimium we should actually have.
        // Assuming we dont store clients... can we test for this
        trace!("Sessions known of: {:?}", sessions.len());

        // TODO: we could cache the handles above and check them as part of loop...
        let _handle = tokio::spawn(async move {
            match watcher {
                Ok(watcher) => {
                    let (send_was_successful, should_remove) =
                        Self::is_sent(watcher, msg_id, peer).await?;

                    if send_was_successful {
                        trace!("Msg {msg_id:?} sent to {peer:?}");
                        Ok(())
                    } else {
                        if should_remove {
                            // do cleanup of that peer
                            let perhaps_session = sessions.remove(&peer);
                            if let Some((_peer, session)) = perhaps_session {
                                session.disconnect().await;
                            }
                        }
                        Err(Error::FailedSend(peer))
                    }
                }
                Err(error) => {
                    // there is only one type of error returned: [`Error::InvalidState`]
                    // which should not happen (be reachable) if we only access PeerSession from Comm
                    // The error means we accessed a peer that we disconnected from.
                    // So, this would potentially be a bug!
                    warn!("Accessed a disconnected peer: {peer}. This is potentially a bug!");

                    let _peer = sessions.remove(&peer);
                    error!(
                        "Sending message (msg_id: {msg_id:?}) to {peer} failed \
                        as we have disconnected from the peer. (Error is: {error})",
                    );
                    Err(Error::FailedSend(peer))
                }
            }
        });

        Ok(())
    }

    /// Test helper to send out Msgs in a blocking fashion
    #[cfg(any(test, feature = "test"))]
    pub async fn send_out_bytes_sync(&self, peer: Peer, msg_id: MsgId, bytes: UsrMsgBytes) {
        let watcher = self.send_to_one(peer, msg_id, bytes).await;
        match watcher {
            Ok(watcher) => {
                let (send_was_successful, should_remove) = Self::is_sent(watcher, msg_id, peer)
                    .await
                    .expect("Error in is_sent");

                if send_was_successful {
                    trace!("Msg {msg_id:?} sent to {peer:?}");
                } else if should_remove {
                    // do cleanup of that peer
                    let perhaps_session = self.outgoing_sessions.remove(&peer);
                    if let Some((_peer, session)) = perhaps_session {
                        session.disconnect().await;
                    }
                }
            }
            Err(error) => {
                error!(
                "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed as we have disconnected from the peer. (Error is: {})",
                msg_id,
                peer.addr(),
                peer.name(),
                error,
            );
                let _peer = self.outgoing_sessions.remove(&peer);
            }
        }
    }

    /// Sends the payload on a new bidi-stream and returns
    /// the response.
    #[tracing::instrument(skip(self, bytes))]
    pub async fn send_out_bytes_to_peer_and_return_response(
        &self,
        peer: Peer,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<WireMsg> {
        // TODO: tweak messaging to just allow passthrough
        debug!("trying to get {peer:?} session in order to send: {msg_id:?}");

        let mut peer = self.get_or_create_outgoing_session(&peer).await?;
        debug!("Session of {peer:?} retrieved for {msg_id:?}");
        let adult_response_bytes = peer.send_with_bi_return_response(bytes, msg_id).await?;
        debug!("Peer response from {peer:?} is in for {msg_id:?}");
        WireMsg::from(adult_response_bytes).map_err(|_| Error::InvalidMessage)
    }

    /// Returns (sent success, should remove)
    /// Should remove occurs if max retries reached
    async fn is_sent(mut watcher: SendWatcher, msg_id: MsgId, peer: Peer) -> Result<(bool, bool)> {
        // here we can monitor the sending
        // and we now watch the status of the send
        loop {
            match &mut watcher.await_change().await {
                SendStatus::Sent => {
                    return Ok((true, false));
                }
                SendStatus::Enqueued => {
                    // this block should be unreachable, as Enqueued is the initial state
                    // but let's handle it anyway..
                    continue; // moves on to awaiting a new change
                }
                SendStatus::WatcherDropped => {
                    // the send job is dropped for some reason,
                    // that happens when the peer session dropped
                    // or the msg was sent, meaning the send didn't actually fail,
                    error!(
                        "Sending message (msg_id: {:?}) to {:?} (name {:?}) possibly failed, as monitoring of the send job was aborted",
                        msg_id,
                        peer.addr(),
                        peer.name(),
                    );
                    return Ok((false, false));
                }
                SendStatus::TransientError(error) => {
                    // An individual connection could have been lost when we tried to send. This
                    // could indicate the connection timed out whilst it was held, or some other
                    // transient connection issue. We don't treat this as a failed send, but we
                    // do sleep a little longer here.
                    // Retries are managed by the peer session, where it will open a new
                    // connection.
                    debug!("Transient error when sending to peer {}: {}", peer, error);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue; // moves on to awaiting a new change
                }
                SendStatus::MaxRetriesReached => {
                    error!(
                        "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed, as we've reached maximum retries",
                        msg_id,
                        peer.addr(),
                        peer.name(),
                    );
                    return Ok((false, true));
                }
            }
        }
    }

    /// Get a PeerSession if it already exists, otherwise create and insert
    #[instrument(skip(self))]
    async fn get_or_create_outgoing_session(&self, peer: &Peer) -> Result<PeerSession> {
        if self.members.read().await.contains(peer) {
            debug!("getting or Creating peer session to member: {peer:?}");
            if let Some(entry) = self.outgoing_sessions.get(peer) {
                debug!(" session to: {peer:?} exists");
                return Ok(entry.value().clone());
            }

            debug!("session to: {peer:?} does not exists");
            let link = Link::new(*peer, self.our_endpoint.clone());
            let session = PeerSession::new(link);

            debug!("Peer is a section member, caching session {peer:?}");
            let prev_peer = self.outgoing_sessions.insert(*peer, session.clone());
            debug!(
                "inserted session {peer:?}, prev peer was discarded? {:?}",
                prev_peer.is_some()
            );
            Ok(session)
        } else {
            Err(Error::CreatingConnectionToUnknownNode)
        }
    }

    /// Remove a qp2p:Connection from a peer's session.
    /// Cleans up the session if no more connections exist
    async fn remove_conn(&self, peer: &Peer, conn: Arc<Connection>) {
        debug!(
            "Removing incoming conn to {peer:?} w/ conn_id : {:?}",
            conn.id()
        );

        let mut should_cleanup_session = true;
        if let Some(entry) = self.outgoing_sessions.get(peer) {
            let peer_session = entry.value();
            peer_session.remove(conn).await;
            if peer_session.has_connections() {
                should_cleanup_session = false;
            }
        }

        if should_cleanup_session {
            let _dead_peer = self.outgoing_sessions.remove(peer);
        }
    }

    // Helper to send a message to a single recipient.
    #[instrument(skip(self, bytes))]
    async fn send_to_one(
        &self,
        recipient: Peer,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<SendWatcher> {
        let bytes_len = {
            let (h, d, p) = bytes.clone();
            h.len() + d.len() + p.len()
        };

        trace!("Sending message bytes ({bytes_len} bytes) w/ {msg_id:?} to {recipient:?}");

        let peer = self.get_or_create_outgoing_session(&recipient).await?;
        debug!("Peer session retrieved");
        peer.send_using_session(msg_id, bytes).await
    }
}

#[tracing::instrument(skip_all)]
async fn handle_connection_events(comm: Comm, mut conn_events_recv: Receiver<ConnectionEvent>) {
    while let Some(event) = conn_events_recv.recv().await {
        match event {
            ConnectionEvent::ConnectionClosed { peer, connection } => {
                comm.remove_conn(&peer, connection).await;
            }
        }
    }
}

#[tracing::instrument(skip_all)]
fn listen_for_incoming_msgs(
    msg_listener: MsgListener,
    mut incoming_connections: IncomingConnections,
) {
    let _ = task::spawn(async move {
        while let Some((connection, incoming_msgs)) = incoming_connections.next().await {
            trace!(
                "{}: from {:?} with connection_id {}",
                LogMarker::IncomingConnection,
                connection.remote_address(),
                connection.id()
            );

            msg_listener.listen(Arc::new(connection), incoming_msgs);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use sn_interface::{
        messaging::{
            data::{ClientMsg, DataQuery, DataQueryVariant},
            ClientAuth, Dst, MsgId, MsgKind,
        },
        types::{ChunkAddress, Keypair, Peer},
    };

    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future;
    use qp2p::Config;
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::{net::UdpSocket, sync::mpsc, time};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(local_addr(), Config::default(), tx).await?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        let peer0_msg = new_test_msg(dst(peer0))?;
        let peer1_msg = new_test_msg(dst(peer1))?;

        comm.send_out_bytes(peer0, peer0_msg.msg_id(), peer0_msg.serialize()?)
            .await?;
        comm.send_out_bytes(peer1, peer1_msg.msg_id(), peer1_msg.serialize()?)
            .await?;

        if let Some(bytes) = rx0.recv().await {
            assert_eq!(WireMsg::from(bytes)?, peer0_msg);
        }

        if let Some(bytes) = rx1.recv().await {
            assert_eq!(WireMsg::from(bytes)?, peer1_msg);
        }

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Re-enable this when we've feedback from sends off thread"]
    async fn failed_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            local_addr(),
            Config {
                // This makes this test faster.
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            tx,
        )
        .await?;

        let invalid_peer = get_invalid_peer().await?;
        let invalid_addr = invalid_peer.addr();
        let msg = new_test_msg(dst(invalid_peer))?;
        let result = comm
            .send_out_bytes(invalid_peer, msg.msg_id(), msg.serialize()?)
            .await;

        assert_matches!(result, Err(Error::FailedSend(peer)) => assert_eq!(peer.addr(), invalid_addr));

        Ok(())
    }

    #[tokio::test]
    async fn send_after_reconnect() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let send_comm = Comm::new(local_addr(), Config::default(), tx).await?;

        let (recv_endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let recv_addr = recv_endpoint.public_addr();
        let name = xor_name::rand::random();
        let peer = Peer::new(name, recv_addr);
        let msg0 = new_test_msg(dst(peer))?;

        send_comm
            .send_out_bytes(peer, msg0.msg_id(), msg0.serialize()?)
            .await?;

        let mut msg0_received = false;

        // Receive one message and disconnect from the peer
        {
            if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
                if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                    assert_eq!(WireMsg::from(msg)?, msg0);
                    msg0_received = true;
                }
                // connection dropped here
            }
            assert!(msg0_received);
        }

        let msg1 = new_test_msg(dst(peer))?;
        send_comm
            .send_out_bytes(peer, msg1.msg_id(), msg1.serialize()?)
            .await?;

        let mut msg1_received = false;

        if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
            if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                assert_eq!(WireMsg::from(msg)?, msg1);
                msg1_received = true;
            }
        }

        assert!(msg1_received);

        Ok(())
    }

    #[tokio::test]
    async fn incoming_connection_lost() -> Result<()> {
        let (tx, mut rx0) = mpsc::channel(1);
        let comm0 = Comm::new(local_addr(), Config::default(), tx.clone()).await?;
        let addr0 = comm0.socket_addr();

        let comm1 = Comm::new(local_addr(), Config::default(), tx).await?;

        let peer = Peer::new(xor_name::rand::random(), addr0);
        let msg = new_test_msg(dst(peer))?;
        // Send a message to establish the connection
        comm1
            .send_out_bytes(peer, msg.msg_id(), msg.serialize()?)
            .await?;

        assert_matches!(rx0.recv().await, Some(MsgFromPeer { .. }));

        // Drop `comm1` to cause connection lost.
        drop(comm1);

        assert_matches!(time::timeout(TIMEOUT, rx0.recv()).await, Err(_));

        Ok(())
    }

    fn dst(peer: Peer) -> Dst {
        Dst {
            name: peer.name(),
            section_key: bls::SecretKey::random().public_key(),
        }
    }

    fn new_test_msg(dst: Dst) -> Result<WireMsg> {
        let src_keypair = Keypair::new_ed25519();

        let query = DataQueryVariant::GetChunk(ChunkAddress(xor_name::rand::random()));
        let query = DataQuery {
            node_index: 0,
            variant: query,
        };
        let query = ClientMsg::Query(query);
        let payload = WireMsg::serialize_msg_payload(&query)?;

        let auth = ClientAuth {
            public_key: src_keypair.public_key(),
            signature: src_keypair.sign(&payload),
        };

        Ok(WireMsg::new_msg(
            MsgId::new(),
            payload,
            MsgKind::Client(auth),
            dst,
        ))
    }

    async fn new_peer() -> Result<(Peer, Receiver<UsrMsgBytes>)> {
        let (endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let addr = endpoint.public_addr();

        let (tx, rx) = mpsc::channel(1);

        let _handle = tokio::task::spawn(async move {
            while let Some((_, mut incoming_messages)) = incoming_connections.next().await {
                while let Ok(Some(msg)) = incoming_messages.next().await {
                    let _ = tx.send(msg).await;
                }
            }
        });

        Ok((Peer::new(xor_name::rand::random(), addr), rx))
    }

    async fn get_invalid_peer() -> Result<Peer> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = socket.local_addr()?;

        // Keep the socket alive to keep the address bound, but don't read/write to it so any
        // attempt to connect to it will fail.
        let _handle = tokio::task::spawn(async move {
            debug!("get invalid peer");
            future::pending::<()>().await;
            let _ = socket;
        });

        Ok(Peer::new(xor_name::rand::random(), addr))
    }

    fn local_addr() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 0).into()
    }
}
