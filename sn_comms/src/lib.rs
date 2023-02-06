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
    clippy::unwrap_used,
    clippy::unused_async
)]

#[macro_use]
extern crate tracing;

mod error;
mod listener;
mod peer_session;

pub use self::error::{Error, Result};

use self::{listener::MsgListener, peer_session::PeerSession};

use sn_interface::{
    messaging::{MsgId, WireMsg},
    types::Peer,
};

use qp2p::{Endpoint, SendStream, UsrMsgBytes};

use dashmap::DashMap;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::Sender;

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
    sessions: Arc<DashMap<Peer, PeerSession>>,
}

impl Comm {
    /// Creates a new instance of Comm with an endpoint
    /// and starts listening to the incoming messages from other nodes.
    #[tracing::instrument(skip_all)]
    pub fn new(local_addr: SocketAddr, incoming_msg_pipe: Sender<MsgFromPeer>) -> Result<Self> {
        let (our_endpoint, incoming_connections) = Endpoint::builder().addr(local_addr).server()?;

        let msg_listener = MsgListener::new(incoming_msg_pipe);
        msg_listener.listen_for_incoming_msgs(incoming_connections);

        Ok(Self {
            our_endpoint,
            sessions: Arc::new(DashMap::new()),
        })
    }

    /// The socket address of our endpoint.
    pub fn socket_addr(&self) -> SocketAddr {
        self.our_endpoint.local_addr()
    }

    /// Closes the endpoint.
    pub fn close_endpoint(&self) {
        self.our_endpoint.close()
    }

    /// Sets the available targets to be only those in the passed in set.
    pub fn set_comm_targets(&self, targets: BTreeSet<Peer>) {
        // We only remove sessions by calling this function,
        // No removals are made even if we failed to send using all peer session's connections,
        // as it's our source of truth for known and connectable peers.

        // Drops sessions that not among the targets.
        self.sessions.retain(|p, _| targets.contains(p));

        // Adds new sessions for each new target.
        targets.iter().for_each(|peer| {
            if self.sessions.get(peer).is_none() {
                let session = PeerSession::new(*peer, self.our_endpoint.clone());
                let _ = self.sessions.insert(*peer, session);
            }
        });
    }

    /// Sends the payload on a new or existing connection.
    #[tracing::instrument(skip(self, bytes))]
    pub async fn send_out_bytes(
        &self,
        peer: Peer,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<()> {
        let (h, d, p) = &bytes;
        let bytes_len = h.len() + d.len() + p.len();
        trace!("Sending message bytes ({bytes_len} bytes) w/ {msg_id:?} to {peer:?}");

        let peer_session = self.get_session(&peer)?;
        debug!("Peer session retrieved: {peer:?}");

        let sessions = self.sessions.clone();
        trace!("Sessions known of: {:?}", sessions.len());

        match peer_session.send(msg_id, bytes).await {
            Ok(()) => {
                trace!("Msg {msg_id:?} sent to {peer:?}");
                Ok(())
            }
            Err(error) => {
                error!("Sending message (msg_id: {msg_id:?}) to {peer:?} failed: {error}");
                Err(Error::FailedSend(peer))
            }
        }
    }

    /// Sends the payload on a new bidi-stream and returns the response.
    #[tracing::instrument(skip(self, bytes))]
    pub async fn send_out_bytes_to_peer_and_return_response(
        &self,
        peer: Peer,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<WireMsg> {
        // TODO: tweak messaging to just allow passthrough
        debug!("Trying to get {peer:?} session in order to send: {msg_id:?}");

        let mut session = self.get_session(&peer)?;
        debug!("Session of {peer:?} retrieved for {msg_id:?}");
        let adult_response_bytes = session
            .send_with_bi_return_response(bytes, msg_id)
            .await
            .map_err(|err| {
                error!("Failed sending {msg_id:?} to {peer:?}: {err:?}");
                Error::FailedSend(peer)
            })?;
        debug!("Peer response from {peer:?} is in for {msg_id:?}");
        WireMsg::from(adult_response_bytes).map_err(|_| Error::InvalidMessage)
    }

    /// Get a PeerSession
    #[instrument(skip(self))]
    fn get_session(&self, peer: &Peer) -> Result<PeerSession> {
        debug!("Attempting to get or create peer session to member: {peer:?}");
        if let Some(entry) = self.sessions.get(peer) {
            debug!("Session to {peer:?} exists");
            Ok(entry.value().clone())
        } else {
            debug!("Did not attempt to connect to external peer: {peer:?}");
            Err(Error::CreatingConnectionToUnknownNode(*peer))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sn_interface::{
        messaging::{
            data::{ClientMsg, DataQuery},
            ClientAuth, Dst, MsgId, MsgKind,
        },
        types::{ChunkAddress, Keypair, Peer},
    };

    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future;
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::{
        net::UdpSocket,
        sync::mpsc::{self, Receiver},
        time,
    };

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(local_addr(), tx)?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        // add peers as known members
        comm.set_comm_targets([peer0, peer1].into());

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
    async fn failed_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(local_addr(), tx)?;

        let invalid_peer = get_invalid_peer().await?;
        let invalid_addr = invalid_peer.addr();
        let msg = new_test_msg(dst(invalid_peer))?;
        let result = comm
            .send_out_bytes(invalid_peer, msg.msg_id(), msg.serialize()?)
            .await;

        // the peer is still not set as a known member thus it should have failed
        assert_matches!(result, Err(Error::CreatingConnectionToUnknownNode(peer)) => assert_eq!(peer.addr(), invalid_addr));

        // let's add the peer as a known member and check again
        comm.set_comm_targets([invalid_peer].into());

        let result = comm
            .send_out_bytes(invalid_peer, msg.msg_id(), msg.serialize()?)
            .await;
        assert_matches!(result, Err(Error::FailedSend(peer)) => assert_eq!(peer.addr(), invalid_addr));

        Ok(())
    }

    #[tokio::test]
    async fn send_after_reconnect() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let send_comm = Comm::new(local_addr(), tx)?;

        let (recv_endpoint, mut incoming_connections) =
            Endpoint::builder().addr(local_addr()).server()?;
        let recv_addr = recv_endpoint.local_addr();
        let name = xor_name::rand::random();
        let peer = Peer::new(name, recv_addr);
        let msg0 = new_test_msg(dst(peer))?;

        // add peer as a known member
        send_comm.set_comm_targets([peer].into());

        send_comm
            .send_out_bytes(peer, msg0.msg_id(), msg0.serialize()?)
            .await?;

        let mut msg0_received = false;

        // Receive one message and disconnect from the peer
        {
            if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
                if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                    assert_eq!(WireMsg::from(msg.0)?, msg0);
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
                assert_eq!(WireMsg::from(msg.0)?, msg1);
                msg1_received = true;
            }
        }

        assert!(msg1_received);

        Ok(())
    }

    #[tokio::test]
    async fn incoming_connection_lost() -> Result<()> {
        let (tx, mut rx0) = mpsc::channel(1);
        let comm0 = Comm::new(local_addr(), tx.clone())?;
        let addr0 = comm0.socket_addr();

        let comm1 = Comm::new(local_addr(), tx)?;

        let peer = Peer::new(xor_name::rand::random(), addr0);
        let msg = new_test_msg(dst(peer))?;

        // add peer as a known member
        comm1.set_comm_targets([peer].into());

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

        let query = DataQuery::GetChunk(ChunkAddress(xor_name::rand::random()));
        let query = ClientMsg::Query(query);
        let payload = WireMsg::serialize_msg_payload(&query)?;

        let auth = ClientAuth {
            public_key: src_keypair.public_key(),
            signature: src_keypair.sign(&payload),
        };

        Ok(WireMsg::new_msg(
            MsgId::new(),
            payload,
            MsgKind::Client {
                auth,
                is_spend: false,
                query_index: None,
            },
            dst,
        ))
    }

    async fn new_peer() -> Result<(Peer, Receiver<UsrMsgBytes>)> {
        let (endpoint, mut incoming_connections) =
            Endpoint::builder().addr(local_addr()).server()?;
        let addr = endpoint.local_addr();

        let (tx, rx) = mpsc::channel(1);

        let _handle = tokio::task::spawn(async move {
            while let Some((_, mut incoming_messages)) = incoming_connections.next().await {
                while let Ok(Some(msg)) = incoming_messages.next().await {
                    let _ = tx.send(msg.0).await;
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
