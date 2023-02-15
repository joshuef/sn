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

use self::peer_session::PeerSession;

use sn_interface::{
    messaging::{
        data::{ClientDataResponse as ClientResponse, Error as MsgError},
        Dst, MsgId, MsgKind, WireMsg,
    },
    types::Peer,
};

use qp2p::{Endpoint, SendStream, UsrMsgBytes};

use futures::future::join_all;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task,
};

/// Standard channel size, to allow for large swings in throughput
static STANDARD_CHANNEL_SIZE: usize = 100_000;

/// Events from the comm module.
#[derive(Debug)]
pub enum CommEvent {
    /// A msg was received.
    Msg(MsgFromPeer),
    /// A send error occurred.
    Error {
        /// The sender/recipient that failed.
        peer: Peer,
        /// The failure type.
        error: Error,
    },
}

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
///
/// Any failed sends are tracked via `CommEvent::Error`, which will track issues for any peers
/// in the section (otherwise ignoring failed send to out of section nodes or clients).
#[derive(Clone, Debug)]
pub struct Comm {
    our_endpoint: Endpoint,
    cmd_sender: Sender<CommCmd>,
}

impl Comm {
    /// Creates a new instance of Comm with an endpoint
    /// and starts listening to the incoming messages from other nodes.
    #[tracing::instrument(skip_all)]
    pub fn new(local_addr: SocketAddr) -> Result<(Self, Receiver<CommEvent>)> {
        let (our_endpoint, incoming_conns) = Endpoint::builder()
            .addr(local_addr)
            .max_concurrent_bidi_streams(1)
            .max_concurrent_uni_streams(1)
            // .idle_timeout(70_000)
            .server()?;

        trace!("Creating comms..");
        // comm_events_receiver will be used by upper layer to receive all msgs coming in from the network
        let (comm_events_sender, comm_events_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        let (cmd_sender, cmd_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        // listen for msgs/connections to our endpoint
        listener::listen_for_connections(comm_events_sender.clone(), incoming_conns);

        process_cmds(our_endpoint.clone(), cmd_receiver, comm_events_sender);

        Ok((
            Self {
                our_endpoint,
                cmd_sender,
            },
            comm_events_receiver,
        ))
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
        self.send_cmd(CommCmd::SetTargets(targets))
    }

    /// Sends the payload on a new or existing connection.
    #[tracing::instrument(skip(self, bytes))]
    pub fn send_out_bytes(&self, peer: Peer, msg_id: MsgId, bytes: UsrMsgBytes) {
        self.send_cmd(CommCmd::Send {
            msg_id,
            peer,
            bytes,
        })
    }

    /// Sends the payload on a new bidi-stream and pushes the response onto the comm event channel.
    #[tracing::instrument(skip(self, bytes))]
    pub fn send_and_return_response(&self, peer: Peer, msg_id: MsgId, bytes: UsrMsgBytes) {
        self.send_cmd(CommCmd::SendAndReturnResponse {
            msg_id,
            peer,
            bytes,
        })
    }

    /// Sends the payload on new bidi-stream to noe and sends the response on the dst stream.
    #[tracing::instrument(skip(self, peer_bytes))]
    pub fn send_and_respond_on_stream(
        &self,
        msg_id: MsgId,
        peer_bytes: BTreeMap<Peer, UsrMsgBytes>,
        expected_targets: usize,
        dst_stream: (Dst, SendStream),
    ) {
        self.send_cmd(CommCmd::SendAndRespondOnStream {
            msg_id,
            peer_bytes,
            expected_targets,
            dst_stream,
        })
    }

    fn send_cmd(&self, cmd: CommCmd) {
        let sender = self.cmd_sender.clone();
        let _handle = task::spawn(async move {
            let error_msg = format!("Failed to send {cmd:?} on comm cmd channel ");
            if let Err(error) = sender.send(cmd).await {
                error!("{error_msg} due to {error}.");
            }
        });
    }
}

/// Internal comm cmds.
#[derive(custom_debug::Debug)]
enum CommCmd {
    Send {
        msg_id: MsgId,
        peer: Peer,
        #[debug(skip)]
        bytes: UsrMsgBytes,
    },
    SetTargets(BTreeSet<Peer>),
    SendAndReturnResponse {
        peer: Peer,
        msg_id: MsgId,
        #[debug(skip)]
        bytes: UsrMsgBytes,
    },
    SendAndRespondOnStream {
        msg_id: MsgId,
        #[debug(skip)]
        peer_bytes: BTreeMap<Peer, UsrMsgBytes>,
        expected_targets: usize,
        dst_stream: (Dst, SendStream),
    },
}

fn process_cmds(
    our_endpoint: Endpoint,
    mut cmd_receiver: Receiver<CommCmd>,
    comm_events: Sender<CommEvent>,
) {
    let _handle = task::spawn(async move {
        let mut sessions = BTreeMap::<Peer, PeerSession>::new();
        while let Some(cmd) = cmd_receiver.recv().await {
            trace!("Comms cmd handling: {cmd:?}");
            match cmd {
                // This is the only place that mutates `sessions`.
                CommCmd::SetTargets(targets) => {
                    // Drops sessions that are not among the targets.
                    sessions.retain(|p, _| targets.contains(p));
                    // Adds new sessions for each new target.
                    targets.iter().for_each(|peer| {
                        if sessions.get(peer).is_none() {
                            let session = PeerSession::new(*peer, our_endpoint.clone());
                            let _ = sessions.insert(*peer, session);
                        }
                    });
                }
                CommCmd::Send {
                    msg_id,
                    peer,
                    bytes,
                } => {
                    if let Some(session) = get_session(msg_id, peer, &sessions, comm_events.clone())
                    {
                        send(msg_id, session, bytes, comm_events.clone())
                    }
                }
                CommCmd::SendAndReturnResponse {
                    peer,
                    msg_id,
                    bytes,
                } => {
                    if let Some(session) = get_session(msg_id, peer, &sessions, comm_events.clone())
                    {
                        send_and_return_response(msg_id, session, bytes, comm_events.clone())
                    }
                }
                CommCmd::SendAndRespondOnStream {
                    msg_id,
                    peer_bytes,
                    expected_targets,
                    dst_stream,
                } => {
                    let peer_bytes = peer_bytes
                        .into_iter()
                        .map(|(peer, bytes)| {
                            let session = get_session(msg_id, peer, &sessions, comm_events.clone());
                            (peer, (session, bytes))
                        })
                        .collect();

                    send_and_respond_on_stream(
                        msg_id,
                        peer_bytes,
                        expected_targets,
                        dst_stream,
                        comm_events.clone(),
                    )
                }
            }
        }
    });
}

fn get_session(
    msg_id: MsgId,
    peer: Peer,
    sessions: &BTreeMap<Peer, PeerSession>,
    comm_events: Sender<CommEvent>,
) -> Option<PeerSession> {
    debug!("Trying to get {peer:?} session in order to send: {msg_id:?}");
    match sessions.get(&peer) {
        Some(session) => Some(session.clone()),
        None => {
            error!("Sending message (msg_id: {msg_id:?}) to {peer:?} failed: unknown node.");
            send_error(peer, Error::ConnectingToUnknownNode(msg_id), comm_events);
            None
        }
    }
}

#[tracing::instrument(skip_all)]
fn send(
    msg_id: MsgId,
    mut session: PeerSession,
    bytes: UsrMsgBytes,
    comm_events: Sender<CommEvent>,
) {
    let _handle = task::spawn(async move {
        let (h, d, p) = &bytes;
        let bytes_len = h.len() + d.len() + p.len();
        let peer = session.peer();
        trace!("Sending message bytes ({bytes_len} bytes) w/ {msg_id:?} to {peer:?}");
        match session.send(msg_id, bytes).await {
            Ok(()) => {
                trace!("Msg {msg_id:?} sent to {peer:?}");
            }
            Err(error) => {
                error!("Sending message (msg_id: {msg_id:?}) to {peer:?} failed: {error}");
                send_error(peer, Error::FailedSend(msg_id), comm_events.clone());
            }
        }
    });
}

#[tracing::instrument(skip_all)]
fn send_and_return_response(
    msg_id: MsgId,
    session: PeerSession,
    bytes: UsrMsgBytes,
    comm_events: Sender<CommEvent>,
) {
    let _handle = task::spawn(async move {
        let (h, d, p) = &bytes;
        let bytes_len = h.len() + d.len() + p.len();
        let peer = session.peer();
        trace!("Sending message bytes ({bytes_len} bytes) w/ {msg_id:?} to {peer:?}");

        let node_response_bytes = match session.send_with_bi_return_response(bytes, msg_id).await {
            Ok(response_bytes) => {
                debug!("Peer response from {peer:?} is in for {msg_id:?}");
                response_bytes
            }
            Err(error) => {
                error!("Sending message (msg_id: {msg_id:?}) to {peer:?} failed: {error}");
                send_error(peer, Error::FailedSend(msg_id), comm_events.clone());
                return;
            }
        };
        match WireMsg::from(node_response_bytes) {
            Ok(wire_msg) => {
                listener::msg_received(wire_msg, peer, None, comm_events.clone());
            }
            Err(error) => {
                error!("Failed sending {msg_id:?} to {peer:?}: {error:?}");
                send_error(peer, Error::InvalidMsgReceived(msg_id), comm_events.clone());
            }
        };
    });
}

#[tracing::instrument(skip_all)]
fn send_and_respond_on_stream(
    msg_id: MsgId,
    peer_bytes: BTreeMap<Peer, (Option<PeerSession>, UsrMsgBytes)>,
    expected_targets: usize,
    dst_stream: (Dst, SendStream),
    comm_events: Sender<CommEvent>,
) {
    let _handle = task::spawn(async move {
        let (dst, stream) = dst_stream;

        let tasks = peer_bytes
            .into_iter()
            .map(|pb| (pb, comm_events.clone()))
            .map(|((peer, (session, bytes)), comm_events)| async move {
                let session = match session {
                    Some(session) => session,
                    None => return (peer, Err(Error::ConnectingToUnknownNode(msg_id))),
                };

                let node_response_bytes =
                    match session.send_with_bi_return_response(bytes, msg_id).await {
                        Ok(response_bytes) => response_bytes,
                        Err(error) => {
                            error!("Failed sending {msg_id:?} to {peer:?}: {error:?}");
                            send_error(peer, Error::FailedSend(msg_id), comm_events);
                            return (peer, Err(Error::FailedSend(msg_id)));
                        }
                    };

                debug!("Response from node {peer:?} is in for {msg_id:?}");
                (peer, Ok(node_response_bytes))
            });

        let peer_results: Vec<(Peer, Result<UsrMsgBytes>)> = join_all(tasks).await;

        let succeeded: Vec<_> = peer_results
            .into_iter()
            .filter_map(|(peer, res)| match res {
                Ok(bytes) => Some((peer, bytes)),
                Err(error) => {
                    error!("Failed sending {msg_id:?} to {peer:?}: {error:?}");
                    send_error(peer, Error::FailedSend(msg_id), comm_events.clone());
                    None
                }
            })
            .collect();

        let some_failed = expected_targets > succeeded.len();
        let all_ok_equal = || succeeded.windows(2).all(|w| are_equal(&w[0].1, &w[1].1));

        let response_bytes = if some_failed || !all_ok_equal() {
            match error_response(dst) {
                None => {
                    error!("Could not send the error response to client!");
                    return;
                }
                Some(bytes) => bytes,
            }
        } else {
            match succeeded.last() {
                Some((_, bytes)) => bytes.clone(),
                _ => {
                    error!("Could not send the response to client!");
                    return;
                }
            }
        };

        send_on_stream(msg_id, response_bytes, stream).await;
    });
}

#[tracing::instrument(skip_all)]
fn send_error(peer: Peer, error: Error, comm_events: Sender<CommEvent>) {
    let _handle = task::spawn(async move {
        let error_msg =
            format!("Failed to send error {error} of peer {peer} on comm event channel ");
        if let Err(err) = comm_events.send(CommEvent::Error { peer, error }).await {
            error!("{error_msg} due to {err}.")
        }
    });
}

#[tracing::instrument(skip_all)]
async fn send_on_stream(msg_id: MsgId, bytes: UsrMsgBytes, mut stream: SendStream) {
    match stream.send_user_msg(bytes).await {
        Ok(()) => trace!("Response to {msg_id:?} sent to client."),
        Err(error) => error!("Could not send the response to {msg_id:?} to client due to {error}!"),
    }
}

fn error_response(dst: Dst) -> Option<UsrMsgBytes> {
    let kind = MsgKind::ClientDataResponse(dst.name);
    let response = ClientResponse::NetworkIssue(MsgError::InconsistentStorageNodeResponses);
    let payload = WireMsg::serialize_msg_payload(&response).ok()?;
    let wire_msg = WireMsg::new_msg(MsgId::new(), payload, kind, dst);
    wire_msg.serialize().ok()
}

#[tracing::instrument(skip_all)]
fn are_equal(a: &UsrMsgBytes, b: &UsrMsgBytes) -> bool {
    let (_, _, a_payload) = a;
    let (_, _, b_payload) = b;
    if !are_bytes_equal(a_payload.to_vec(), b_payload.to_vec()) {
        return false;
    }
    true
}

#[tracing::instrument(skip_all)]
fn are_bytes_equal(one: Vec<u8>, other: Vec<u8>) -> bool {
    if one.len() != other.len() {
        return false;
    }
    for (a, b) in one.into_iter().zip(other) {
        if a != b {
            return false;
        }
    }
    true
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
        let (comm, _rx) = Comm::new(local_addr())?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        // add peers as known members
        comm.set_comm_targets([peer0, peer1].into());

        let peer0_msg = new_test_msg(dst(peer0))?;
        let peer1_msg = new_test_msg(dst(peer1))?;

        comm.send_out_bytes(peer0, peer0_msg.msg_id(), peer0_msg.serialize()?);
        comm.send_out_bytes(peer1, peer1_msg.msg_id(), peer1_msg.serialize()?);

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
        let (comm, mut rx) = Comm::new(local_addr())?;

        let invalid_peer = get_invalid_peer().await?;
        let invalid_addr = invalid_peer.addr();
        let msg = new_test_msg(dst(invalid_peer))?;
        comm.send_out_bytes(invalid_peer, msg.msg_id(), msg.serialize()?);

        if let Some(CommEvent::Error { peer, error }) = rx.recv().await {
            // the peer is still not set as a known member thus it should have failed
            assert_matches!(error, Error::ConnectingToUnknownNode(_));
            assert_eq!(peer.addr(), invalid_addr);
        }

        // let's add the peer as a known member and check again
        comm.set_comm_targets([invalid_peer].into());

        comm.send_out_bytes(invalid_peer, msg.msg_id(), msg.serialize()?);

        if let Some(CommEvent::Error { peer, error }) = rx.recv().await {
            assert_matches!(error, Error::FailedSend(_));
            assert_eq!(peer.addr(), invalid_addr);
        }

        Ok(())
    }

    #[tokio::test]
    async fn send_after_reconnect() -> Result<()> {
        let (send_comm, _rx) = Comm::new(local_addr())?;

        let (recv_endpoint, mut incoming_connections) = Endpoint::builder()
            .addr(local_addr())
            .idle_timeout(70_000)
            .server()?;
        let recv_addr = recv_endpoint.local_addr();
        let name = xor_name::rand::random();
        let peer = Peer::new(name, recv_addr);
        let msg0 = new_test_msg(dst(peer))?;

        // add peer as a known member
        send_comm.set_comm_targets([peer].into());

        send_comm.send_out_bytes(peer, msg0.msg_id(), msg0.serialize()?);

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
        send_comm.send_out_bytes(peer, msg1.msg_id(), msg1.serialize()?);

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
        let (comm0, mut rx0) = Comm::new(local_addr())?;
        let addr0 = comm0.socket_addr();

        let (comm1, _rx1) = Comm::new(local_addr())?;

        let peer = Peer::new(xor_name::rand::random(), addr0);
        let msg = new_test_msg(dst(peer))?;

        // add peer as a known member
        comm1.set_comm_targets([peer].into());

        // Send a message to establish the connection
        comm1.send_out_bytes(peer, msg.msg_id(), msg.serialize()?);

        assert_matches!(rx0.recv().await, Some(CommEvent::Msg(MsgFromPeer { .. })));

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
        let (endpoint, mut incoming_connections) = Endpoint::builder()
            .addr(local_addr())
            .idle_timeout(70_000)
            .server()?;
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
