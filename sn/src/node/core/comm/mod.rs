// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod back_pressure;
mod msg_count;

use back_pressure::BackPressure;
use msg_count::MsgCount;

use crate::node::error::{Error, Result};
use sn_interface::messaging::{system::LoadReport, MsgId, WireMsg};
use sn_interface::types::{log_markers::LogMarker, Peer, PeerLinks, SendToOneError};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use qp2p::{ConnectionIncoming, Endpoint};
use std::net::SocketAddr;
use tokio::{sync::mpsc, task};
use tracing::Instrument;

// Communication component of the node to interact with other nodes.
#[derive(Clone)]
pub(crate) struct Comm {
    endpoint: Endpoint,
    event_tx: mpsc::Sender<ConnectionEvent>,
    msg_count: MsgCount,
    back_pressure: BackPressure,
    peer_links: PeerLinks,
}

impl Drop for Comm {
    fn drop(&mut self) {
        // Close all existing connections and stop accepting new ones.
        // FIXME: this may be broken – `Comm` is clone, so this will break any clones?
        self.endpoint.close();
    }
}

impl Comm {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn new(
        local_addr: SocketAddr,
        config: qp2p::Config,
        event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<Self> {
        // Don't bootstrap, just create an endpoint to listen to
        // the incoming messages from other nodes.
        // This also returns the a channel where we can listen for
        // disconnection events.
        let (endpoint, incoming_connections, _) =
            Endpoint::new_peer(local_addr, Default::default(), config).await?;

        let msg_count = MsgCount::new();

        let peer_links = PeerLinks::new(endpoint.clone());

        let _handle = task::spawn(
            handle_incoming_connections(
                incoming_connections,
                event_tx.clone(),
                msg_count.clone(),
                peer_links.clone(),
            )
            .in_current_span(),
        );

        let back_pressure = BackPressure::new();

        Ok(Self {
            endpoint,
            event_tx,
            msg_count,
            back_pressure,
            peer_links,
        })
    }

    #[tracing::instrument(skip(local_addr, config, event_tx))]
    pub(crate) async fn bootstrap(
        local_addr: SocketAddr,
        bootstrap_nodes: &[SocketAddr],
        config: qp2p::Config,
        event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<(Self, SocketAddr)> {
        // Bootstrap to the network returning the connection to a node.
        // We can use the returned channels to listen for incoming messages and disconnection events
        let (endpoint, incoming_connections, bootstrap_node) =
            Endpoint::new_peer(local_addr, bootstrap_nodes, config).await?;

        let (connection, incoming_msgs) = bootstrap_node.ok_or(Error::BootstrapFailed)?;
        let msg_count = MsgCount::new();
        let peer_links = PeerLinks::new(endpoint.clone());

        let _handle = task::spawn(
            handle_incoming_connections(
                incoming_connections,
                event_tx.clone(),
                msg_count.clone(),
                peer_links.clone(),
            )
            .in_current_span(),
        );

        let _ = task::spawn(
            handle_incoming_messages(
                connection.clone(),
                incoming_msgs,
                event_tx.clone(),
                msg_count.clone(),
                peer_links.clone(),
            )
            .in_current_span(),
        );

        Ok((
            Self {
                endpoint,
                event_tx,
                msg_count,
                back_pressure: BackPressure::new(),
                peer_links,
            },
            connection.remote_address(),
        ))
    }

    pub(crate) fn our_connection_info(&self) -> SocketAddr {
        self.endpoint.public_addr()
    }

    /// Disposes of the link to the peer and all underlying resources.
    /// TODO: Use this when new membership is in place, call whenever we drop a member.
    #[allow(unused)]
    pub(crate) async fn disconnect(&self, peer: &Peer) {
        self.peer_links.disconnect(peer.id()).await;
    }

    /// Sends a message to a client. Reuses an existing or creates a connection if none.
    pub(crate) async fn send_to_client(
        &self,
        recipient: &Peer,
        mut wire_msg: WireMsg,
    ) -> Result<(), Error> {
        trace!(
            "Sending msg on existing connection to client {:?}",
            recipient
        );

        let name = recipient.name();
        let addr = recipient.addr();

        wire_msg.set_dst_xorname(name);

        let bytes = wire_msg.serialize()?;
        // TODO: rework priority so this we dont need to deserialise payload to determine priority.
        let priority = wire_msg.into_msg()?.priority();

        let (_, result) = self
            .send_to_one(recipient, wire_msg.msg_id(), priority, bytes)
            .await;

        match result {
            Ok(_) => self.msg_count.increase_outgoing(addr), // count outgoing msgs..
            Err(err) => {
                error!(
                    "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed with {:?}",
                    wire_msg.msg_id(),
                    addr,
                    name,
                    err
                );
                return Err(Error::FailedSend(addr, name));
            }
        }

        Ok(())
    }

    /// Fake function used as replacement for testing only.
    #[cfg(test)]
    pub(crate) async fn is_reachable(&self, _peer: &SocketAddr) -> Result<(), Error> {
        Ok(())
    }

    /// Tests whether the peer is reachable.
    #[cfg(not(test))]
    pub(crate) async fn is_reachable(&self, peer: &SocketAddr) -> Result<(), Error> {
        let qp2p_config = qp2p::Config {
            forward_port: false,
            ..Default::default()
        };

        let connectivity_endpoint =
            Endpoint::new_client((self.endpoint.local_addr().ip(), 0), qp2p_config)?;

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

    /// Sends a message to multiple recipients. Attempts to send to `delivery_group_size`
    /// recipients out of the `recipients` list. If a send fails, attempts to send to the next peer
    /// until `delivery_group_size`  successful sends complete or there are no more recipients to
    /// try.
    ///
    /// Returns an `Error::ConnectionClosed` if the connection is closed locally. Else it returns a
    /// `SendStatus::MinDeliveryGroupSizeReached` or `SendStatus::MinDeliveryGroupSizeFailed` depending
    /// on if the minimum delivery group size is met or not. The failed recipients are sent along
    /// with the status. It returns a `SendStatus::AllRecipients` if message is sent to all the recipients.
    #[allow(clippy::needless_lifetimes)]
    // ^ this is firing a false positive here
    // we need an explicit lifetime for the compiler to see
    // that `recipient` lives long enough in the closure
    #[tracing::instrument(skip(self))]
    pub(crate) async fn send<'r>(
        &self,
        recipients: &'r [Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<SendStatus> {
        let msg_id = wire_msg.msg_id();
        trace!(
            "Sending message (msg_id: {:?}) to {} of {:?}",
            msg_id,
            delivery_group_size,
            recipients
        );

        if recipients.len() < delivery_group_size {
            warn!(
                "Less than delivery_group_size valid recipients - delivery_group_size: {}, recipients: {:?}",
                delivery_group_size,
                recipients,
            );
        }

        let delivery_group_size = delivery_group_size.min(recipients.len());

        if recipients.is_empty() {
            return Err(Error::EmptyRecipientList);
        }

        let msg_bytes = wire_msg.serialize().map_err(Error::Messaging)?;
        let priority = wire_msg.clone().into_msg()?.priority();

        // Run all the sends concurrently (using `FuturesUnordered`). If any of them fails, pick
        // the next recipient and try to send to them. Proceed until the needed number of sends
        // succeeds or if there are no more recipients to pick.
        let mut tasks: FuturesUnordered<_> = recipients[0..delivery_group_size]
            .iter()
            .map(|recipient| self.send_to_one(recipient, msg_id, priority, msg_bytes.clone()))
            .collect();

        let mut next = delivery_group_size;
        let mut successes = 0;
        let mut failed_recipients = vec![];

        while let Some((recipient, result)) = tasks.next().await {
            match result {
                Ok(()) => {
                    successes += 1;
                    // count outgoing msgs..
                    self.msg_count.increase_outgoing(recipient.addr());
                }
                Err(error) if error.is_local_close() => {
                    // The connection was closed by us which means
                    // we are terminating so let's cut this short.
                    return Err(Error::ConnectionClosed);
                }
                Err(SendToOneError::Send(qp2p::SendError::ConnectionLost(_))) => {
                    // We reused an existing connection, but it was lost when we tried to send. This
                    // could indicate the connection timed out whilst it was held, or some other
                    // transient connection issue. We don't treat this as a failed recipient, and
                    // instead push the same recipient again, but force a reconnection.
                    tasks.push(self.send_to_one(recipient, msg_id, priority, msg_bytes.clone()));
                }
                Err(error) => {
                    warn!("during sending, received error {:?}", error);
                    failed_recipients.push(recipient.clone());

                    if next < recipients.len() {
                        tasks.push(self.send_to_one(
                            &recipients[next],
                            msg_id,
                            priority,
                            msg_bytes.clone(),
                        ));
                        next += 1;
                    }
                }
            }
        }

        trace!(
            "Finished sending message {:?} to {}/{} recipients (failed: {:?})",
            wire_msg,
            successes,
            delivery_group_size,
            failed_recipients
        );

        if successes == delivery_group_size {
            if failed_recipients.is_empty() {
                Ok(SendStatus::AllRecipients)
            } else {
                Ok(SendStatus::MinDeliveryGroupSizeReached(failed_recipients))
            }
        } else {
            Ok(SendStatus::MinDeliveryGroupSizeFailed(failed_recipients))
        }
    }

    // Helper to send a message to a single recipient.
    #[allow(clippy::needless_lifetimes)] // clippy is wrong here
    async fn send_to_one<'r>(
        &self,
        recipient: &'r Peer,
        msg_id: MsgId,
        msg_priority: i32,
        msg_bytes: Bytes,
    ) -> (&'r Peer, Result<(), SendToOneError>) {
        trace!(
            "Sending message ({} bytes, msg_id: {:?}) to {}",
            msg_bytes.len(),
            msg_id,
            recipient,
        );

        // TODO: more laid back retries with lower priority, more aggressive with higher
        // NB: This is retries per qp2p::connection!
        let retry_config = self.back_pressure.get(&recipient.addr()).await;

        let link = self.peer_links.get_or_create(&recipient.id()).await;

        let listen = |conn, incoming_msgs| {
            let _ = task::spawn(
                handle_incoming_messages(
                    conn,
                    incoming_msgs,
                    self.event_tx.clone(),
                    self.msg_count.clone(),
                    self.peer_links.clone(),
                )
                .in_current_span(),
            );
        };

        let result = link
            .send_with(msg_bytes, msg_priority, Some(&retry_config), listen)
            .await;

        (recipient, result)
    }

    /// Regulates comms with the specified peer
    /// according to the cpu load report provided by it.
    pub(crate) async fn regulate(&self, peer: SocketAddr, load_report: LoadReport) {
        self.back_pressure.set(peer, load_report).await
    }

    /// Returns cpu load report if being strained.
    pub(crate) async fn check_strain(&self, caller: &SocketAddr) -> Option<LoadReport> {
        self.back_pressure.load_report(caller).await
    }

    pub(crate) fn print_stats(&self) {
        let incoming = self.msg_count.incoming();
        let outgoing = self.msg_count.outgoing();
        info!("*** Incoming msgs: {:?} ***", incoming);
        info!("*** Outgoing msgs: {:?} ***", outgoing);
    }
}

#[derive(Debug)]
pub(crate) enum ConnectionEvent {
    Received {
        sender: Peer,
        wire_msg: WireMsg,
        original_bytes: Bytes,
    },
}

#[tracing::instrument(skip_all)]
async fn handle_incoming_connections(
    mut incoming_connections: qp2p::IncomingConnections,
    event_tx: mpsc::Sender<ConnectionEvent>,
    msg_count: MsgCount,
    peer_links: PeerLinks,
) {
    while let Some((connection, incoming_msgs)) = incoming_connections.next().await {
        trace!(
            "incoming_connection from {:?} with connection_id {:?}",
            connection.remote_address(),
            connection.id()
        );

        let _ = task::spawn(
            handle_incoming_messages(
                connection,
                incoming_msgs,
                event_tx.clone(),
                msg_count.clone(),
                peer_links.clone(),
            )
            .in_current_span(),
        );
    }
}

#[tracing::instrument(skip_all)]
async fn handle_incoming_messages(
    conn: qp2p::Connection,
    mut incoming_msgs: ConnectionIncoming,
    event_tx: mpsc::Sender<ConnectionEvent>,
    msg_count: MsgCount,
    peer_links: PeerLinks,
) {
    let conn_id = conn.id();
    let remote_address = conn.remote_address();
    let mut first = true;

    while let Some(result) = incoming_msgs.next().await.transpose() {
        match result {
            Ok(msg_bytes) => {
                let wire_msg = match WireMsg::from(msg_bytes.clone()) {
                    Ok(wire_msg) => wire_msg,
                    Err(error) => {
                        // TODO: should perhaps rather drop this connection.. as it is a spam vector
                        debug!("Failed to deserialize message: {:?}", error);
                        continue;
                    }
                };

                let src_name = wire_msg.msg_kind().src().name();

                if first {
                    first = false;
                    let node_id = (src_name, remote_address);
                    peer_links.add_incoming(&node_id, conn.clone()).await;
                }

                let _send_res = event_tx
                    .send(ConnectionEvent::Received {
                        sender: Peer::new(src_name, remote_address),
                        wire_msg,
                        original_bytes: msg_bytes,
                    })
                    .await;

                // count incoming msgs..
                msg_count.increase_incoming(remote_address);
            }
            Err(error) => {
                // TODO: should we propagate this?
                warn!("error on connection with {}: {:?}", remote_address, error);
            }
        }
    }

    trace!(%conn_id, %remote_address, "{}", LogMarker::ConnectionClosed);
}

/// Returns the status of the send operation.
#[derive(Debug, Clone)]
pub(crate) enum SendStatus {
    AllRecipients,
    MinDeliveryGroupSizeReached(Vec<Peer>),
    MinDeliveryGroupSizeFailed(Vec<Peer>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future;
    use qp2p::Config;
    use rand::rngs::OsRng;
    use sn_interface::messaging::data::{DataQuery, ServiceMsg};
    use sn_interface::messaging::{DstLocation, MsgId, MsgKind, ServiceAuth};
    use sn_interface::types::{ChunkAddress, Keypair, Peer};
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::{net::UdpSocket, sync::mpsc, time};
    use xor_name::XorName;

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(local_addr(), Config::default(), tx).await?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        let original_message = new_test_msg()?;

        let status = comm
            .send(&[peer0, peer1], 2, original_message.clone())
            .await?;

        assert_matches!(status, SendStatus::AllRecipients);

        if let Some(bytes) = rx0.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message.clone());
        }

        if let Some(bytes) = rx1.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_to_subset() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(local_addr(), Config::default(), tx).await?;

        let (peer0, mut rx0) = new_peer().await?;
        let (peer1, mut rx1) = new_peer().await?;

        let original_message = new_test_msg()?;
        let status = comm
            .send(&[peer0, peer1], 1, original_message.clone())
            .await?;

        assert_matches!(status, SendStatus::AllRecipients);

        if let Some(bytes) = rx0.recv().await {
            assert_eq!(WireMsg::from(bytes)?, original_message);
        }

        assert!(time::timeout(TIMEOUT, rx1.recv())
            .await
            .unwrap_or_default()
            .is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
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

        let status = comm.send(&[invalid_peer], 1, new_test_msg()?).await?;

        assert_matches!(
            &status,
            &SendStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_addr]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_send_after_failed_attempts() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            local_addr(),
            Config {
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            tx,
        )
        .await?;
        let (peer, mut rx) = new_peer().await?;
        let invalid_peer = get_invalid_peer().await?;

        let message = new_test_msg()?;
        let status = comm
            .send(&[invalid_peer.clone(), peer], 1, message.clone())
            .await?;
        assert_matches!(status, SendStatus::MinDeliveryGroupSizeReached(failed_recipients) => {
            assert_eq!(&failed_recipients, &[invalid_peer])
        });

        if let Some(bytes) = rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partially_successful_send() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let comm = Comm::new(
            local_addr(),
            Config {
                idle_timeout: Some(Duration::from_millis(1)),
                ..Config::default()
            },
            tx,
        )
        .await?;
        let (peer, mut rx) = new_peer().await?;
        let invalid_peer = get_invalid_peer().await?;

        let message = new_test_msg()?;
        let status = comm
            .send(&[invalid_peer.clone(), peer], 2, message.clone())
            .await?;

        assert_matches!(
            status,
            SendStatus::MinDeliveryGroupSizeFailed(_) => vec![invalid_peer]
        );

        if let Some(bytes) = rx.recv().await {
            assert_eq!(WireMsg::from(bytes)?, message);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_after_reconnect() -> Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let send_comm = Comm::new(local_addr(), Config::default(), tx).await?;

        let (recv_endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let recv_addr = recv_endpoint.public_addr();
        let name = XorName::random();

        let msg0 = new_test_msg()?;
        let status = send_comm
            .send(&[Peer::new(name, recv_addr)], 1, msg0.clone())
            .await?;
        assert_matches!(status, SendStatus::AllRecipients);

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

        let msg1 = new_test_msg()?;
        let status = send_comm
            .send(&[Peer::new(name, recv_addr)], 1, msg1.clone())
            .await?;
        assert_matches!(status, SendStatus::AllRecipients);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn incoming_connection_lost() -> Result<()> {
        let (tx, mut rx0) = mpsc::channel(1);
        let comm0 = Comm::new(local_addr(), Config::default(), tx).await?;
        let addr0 = comm0.our_connection_info();

        let (tx, _rx) = mpsc::channel(1);
        let comm1 = Comm::new(local_addr(), Config::default(), tx).await?;

        // Send a message to establish the connection
        let status = comm1
            .send(&[Peer::new(XorName::random(), addr0)], 1, new_test_msg()?)
            .await?;
        assert_matches!(status, SendStatus::AllRecipients);

        assert_matches!(rx0.recv().await, Some(ConnectionEvent::Received { .. }));
        // Drop `comm1` to cause connection lost.
        drop(comm1);

        assert_matches!(time::timeout(TIMEOUT, rx0.recv()).await, Err(_));

        Ok(())
    }

    fn new_test_msg() -> Result<WireMsg> {
        let dst_location = DstLocation::Node {
            name: XorName::random(),
            section_pk: bls::SecretKey::random().public_key(),
        };

        let mut rng = OsRng;
        let src_keypair = Keypair::new_ed25519(&mut rng);

        let payload = WireMsg::serialize_msg_payload(&ServiceMsg::Query(DataQuery::GetChunk(
            ChunkAddress(XorName::random()),
        )))?;
        let auth = ServiceAuth {
            public_key: src_keypair.public_key(),
            signature: src_keypair.sign(&payload),
        };

        let wire_msg = WireMsg::new_msg(
            MsgId::new(),
            payload,
            MsgKind::ServiceMsg(auth),
            dst_location,
        )?;

        Ok(wire_msg)
    }

    async fn new_peer() -> Result<(Peer, mpsc::Receiver<Bytes>)> {
        let (endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let addr = endpoint.public_addr();

        let (tx, rx) = mpsc::channel(1);

        let _handle = tokio::spawn(async move {
            while let Some((_, mut incoming_messages)) = incoming_connections.next().await {
                while let Ok(Some(msg)) = incoming_messages.next().await {
                    let _ = tx.send(msg).await;
                }
            }
        });

        Ok((Peer::new(XorName::random(), addr), rx))
    }

    async fn get_invalid_peer() -> Result<Peer> {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = socket.local_addr()?;

        // Keep the socket alive to keep the address bound, but don't read/write to it so any
        // attempt to connect to it will fail.
        let _handle = tokio::spawn(async move {
            debug!("get invalid peer");
            future::pending::<()>().await;
            let _ = socket;
        });

        Ok(Peer::new(XorName::random(), addr))
    }

    fn local_addr() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 0).into()
    }
}
