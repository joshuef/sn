// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(feature = "back-pressure")]
mod back_pressure;

mod link;
mod listener;
mod peer_session;

#[cfg(feature = "back-pressure")]
use self::back_pressure::BackPressure;

use self::{
    link::Link,
    listener::{ListenerEvent, MsgListener},
    peer_session::{PeerSession, SendWatcher},
};

use crate::node::{Error, RateLimits, Result};
use peer_session::SendStatus;

use sn_dysfunction::DysfunctionDetection;
use sn_interface::{messaging::WireMsg, types::Peer};

use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use qp2p::{Endpoint, IncomingConnections};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task,
};

// Communication component of the node to interact with other nodes.
#[derive(Clone)]
pub(crate) struct Comm {
    our_endpoint: Endpoint,
    msg_listener: MsgListener,
    #[cfg(feature = "back-pressure")]
    back_pressure: BackPressure,
    sessions: Arc<DashMap<Peer, PeerSession>>,
}

/// Commands for interacting with Comm.
#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) enum Cmd {
    #[cfg(feature = "back-pressure")]
    /// Set message rate for peer to the desired msgs per second
    Regulate { peer: Peer, msgs_per_s: f64 },
}

impl Comm {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn first_node(
        local_addr: SocketAddr,
        config: qp2p::Config,
        monitoring: RateLimits,
        incoming_msg_pipe: Sender<MsgEvent>,
    ) -> Result<Self> {
        // Doesn't bootstrap, just creates an endpoint to listen to
        // the incoming messages from other nodes.
        let (our_endpoint, incoming_connections, _) =
            Endpoint::new_peer(local_addr, Default::default(), config).await?;

        let (comm, _) = setup_comms(
            our_endpoint,
            incoming_connections,
            monitoring,
            incoming_msg_pipe,
        );

        Ok(comm)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn bootstrap(
        local_addr: SocketAddr,
        bootstrap_nodes: &[SocketAddr],
        config: qp2p::Config,
        monitoring: RateLimits,
        incoming_msg_pipe: Sender<MsgEvent>,
    ) -> Result<(Self, SocketAddr)> {
        debug!("Starting bootstrap process with bootstrap nodes: {bootstrap_nodes:?}");
        // Bootstrap to the network returning the connection to a node.
        let (our_endpoint, incoming_connections, bootstrap_node) =
            Endpoint::new_peer(local_addr, bootstrap_nodes, config).await?;

        let (comm, msg_listener) = setup_comms(
            our_endpoint,
            incoming_connections,
            monitoring,
            incoming_msg_pipe,
        );

        let (connection, incoming_msgs) = bootstrap_node.ok_or(Error::BootstrapFailed)?;
        let remote_address = connection.remote_address();

        msg_listener.listen(connection, incoming_msgs);

        Ok((comm, remote_address))
    }

    pub(crate) async fn handle_cmd(&self, cmd: Cmd) {
        match cmd {
            #[cfg(feature = "back-pressure")]
            Cmd::Regulate { peer, msgs_per_s } => self.regulate(&peer, msgs_per_s).await,
        }
    }

    pub(crate) fn socket_addr(&self) -> SocketAddr {
        self.our_endpoint.public_addr()
    }

    pub(crate) async fn cleanup_peers(
        &self,
        retain_peers: Vec<Peer>,
        mut dysfunction: DysfunctionDetection,
    ) -> Result<()> {
        debug!("Cleanup peers except: {:?}", retain_peers);

        let mut peers_to_cleanup = vec![];
        for entry in self.sessions.iter() {
            let peer = entry.key();
            let session = entry.value();

            session.remove_expired().await;

            let is_connected = session.is_connected();

            if !is_connected {
                if !retain_peers.contains(peer) {
                    peers_to_cleanup.push(*peer);
                }

                dysfunction.track_issue(peer.name(), sn_dysfunction::IssueType::Communication)?;
            }
        }

        // cleanup any and all conns that are not connected
        // TODO: check if we need to remove client conns manually, or if we can assume they're disconnected...
        // Perhaps above a threshold we cleanup non-section conns?
        if !peers_to_cleanup.is_empty() {
            for peer in peers_to_cleanup {
                let perhaps_peer = self.sessions.remove(&peer);

                if let Some((_peer, session)) = perhaps_peer {
                    session.disconnect().await
                };
            }
        } else {
            debug!("No peers to cleanup!!!!!");
        }

        debug!("PeerLink count post-cleanup: {:?}", self.sessions.len());
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

    #[cfg(feature = "back-pressure")]
    /// Returns our caller-specific tolerated msgs per s, if the value has changed significantly.
    pub(crate) async fn tolerated_msgs_per_s(&self) -> Option<f64> {
        let sessions = self.sessions.len();
        self.back_pressure.tolerated_msgs_per_s(sessions).await
    }

    #[cfg(feature = "back-pressure")]
    /// Regulates comms with the specified peer
    /// according to the tolerated msgs per s provided by it.
    pub(crate) async fn regulate(&self, peer: &Peer, msgs_per_s: f64) {
        let session = self.get_or_create(peer).await;
        session.update_send_rate(msgs_per_s).await;
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

        let msg_id = wire_msg.msg_id();

        let priority = wire_msg.priority();

        let (_, result) = self.send_to_one(*recipient, wire_msg, priority).await;

        match result {
            Err(error) => {
                // there is only one type of error returned: [`Error::InvalidState`]
                // which should not happen (be reachable) if we only access PeerSession from Comm
                // The error means we accessed a peer that we disconnected from.
                // So, this would potentially be a bug!
                warn!(
                    "Accessed a disconnected peer: {}. This is potentially a bug!",
                    recipient
                );
                error!(
                    "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed as we have disconnected from the peer. (Error is: {})",
                    msg_id,
                    addr,
                    name,
                    error,
                );
                Err(Error::FailedSend(*recipient))
            }
            Ok(mut watcher) => {
                // here we can monitor the sending
                // and we now watch the status of the send
                loop {
                    match watcher.await_change().await {
                        SendStatus::Sent => {
                            return Ok(()); // all good
                        }
                        SendStatus::Enqueued => {
                            // this block should be unreachable, as Enqueued is the initial state
                            // but let's handle it anyway..
                            continue; // moves on to awaiting a new change
                        }
                        SendStatus::PeerLinkDropped => {
                            // The connection was closed by us which means
                            // we have dropped this peer for some reason
                            error!(
                                "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed, as we have dropped the link to it.",
                                msg_id,
                                addr,
                                name,
                            );
                            return Err(Error::PeerLinkDropped(*recipient));
                        }
                        SendStatus::WatcherDropped => {
                            // the send job is dropped for some reason,
                            // that happens when the peer session dropped
                            // or the msg was sent, meaning the send didn't actually fail,
                            error!(
                                "Sending message (msg_id: {:?}) to {:?} (name {:?}) possibly failed, as monitoring of the send job was aborted",
                                msg_id,
                                addr,
                                name,
                            );
                            return Err(Error::FailedSend(*recipient));
                        }
                        SendStatus::TransientError(error) => {
                            // An individual connection can for example have been lost when we tried to send. This
                            // could indicate the connection timed out whilst it was held, or some other
                            // transient connection issue. We don't treat this as a failed recipient, but we sleep a little longer here.
                            // Retries are managed by the peer session, where it will open a new connection.
                            debug!(
                                "Transient error when sending to peer {}: {}",
                                recipient, error
                            );
                            continue; // moves on to awaiting a new change
                        }
                        SendStatus::MaxRetriesReached => {
                            error!(
                                "Sending message (msg_id: {:?}) to {:?} (name {:?}) failed, as we've reached maximum retries",
                                msg_id,
                                addr,
                                name,
                            );
                            return Err(Error::FailedSend(*recipient));
                        }
                    }
                }
            }
        }
    }

    /// Sends a message to multiple recipients. Attempts to send to `delivery_group_size`
    /// recipients out of the `recipients` list. If a send fails, attempts to send to the next peer
    /// until `delivery_group_size` successful sends complete or there are no more recipients to
    /// try.
    ///
    /// Returns an `Error::EmptyRecipientList` if the recipient list is empty. Else it returns a
    /// `DeliveryStatus::MinDeliveryGroupSizeReached` or `DeliveryStatus::MinDeliveryGroupSizeFailed` depending
    /// on if the minimum delivery group size is met or not. The failed recipients are sent along
    /// with the status. It returns a `DeliveryStatus::AllRecipients` if message is sent to all the recipients.
    #[tracing::instrument(skip(self))]
    pub(crate) async fn send(
        &self,
        recipients: &[Peer],
        wire_msg: WireMsg,
    ) -> Result<DeliveryStatus> {
        // todo: this type of task needs a send job, that we can come back to

        let msg_id = wire_msg.msg_id();
        trace!("Sending message (msg_id: {:?}) to {:?}", msg_id, recipients);

        if recipients.is_empty() {
            return Err(Error::EmptyRecipientList);
        }

        let priority = wire_msg.priority();

        // Run all the sends concurrently (using `FuturesUnordered`). If any of them fails, pick
        // the next recipient and try to send to them. Proceed until the needed number of sends
        // succeeds or if there are no more recipients to pick.
        let mut tasks: FuturesUnordered<_> = recipients
            .iter()
            .map(|recipient| {
                let mut msg = wire_msg.clone();
                msg.set_dst_xorname(recipient.name());
                self.send_to_one(*recipient, msg, priority)
            })
            .collect();

        let delivery_group_size = recipients.len();
        let mut next = delivery_group_size;
        let mut successes = 0;
        let mut failed_recipients = vec![];

        let mut try_next = |error, recipient, tasks: &mut FuturesUnordered<_>| {
            warn!("during sending, received error {:?}", error);
            failed_recipients.push(recipient);

            if next < recipients.len() {
                let mut msg = wire_msg.clone();
                msg.set_dst_xorname(recipients[next].name());
                tasks.push(self.send_to_one(recipients[next], msg, priority));
                next += 1;
            }
        };

        while let Some((recipient, result)) = tasks.next().await {
            match result {
                Err(error) => {
                    // there is only one type of error returned: [`Error::InvalidState`]
                    // which should not happen (be reachable) if we only access PeerSession from Comm
                    // The error means we accessed a peer that we disconnected from.
                    // So, this would potentially be a bug!
                    //
                    // (let's log that bug here, but continue running anyway, as it isn't that critical)
                    warn!(
                        "Accessed a disconnected peer: {}. This is potentially a bug!",
                        recipient
                    );
                    try_next(error, recipient, &mut tasks);
                }
                Ok(mut watcher) => {
                    // we now watch the status of the send for this particular recipient..
                    loop {
                        match watcher.await_change().await {
                            SendStatus::Sent => {
                                successes += 1;
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::Enqueued => {
                                // this block should be unreachable, as Enqueued is the initial state
                                // but let's handle it anyway..
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                continue; // await change on the same recipient again
                            }
                            SendStatus::PeerLinkDropped => {
                                // The connection was closed by us which means
                                // we have dropped this peer for some reason, thus try next
                                try_next(Error::PeerLinkDropped(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::WatcherDropped => {
                                // the send job is dropped for some reason,
                                // that happens when the peer session dropped
                                // or the msg was sent, meaning the send didn't actually fail,
                                // so we would be sending an extra msg here in case of such a glitch
                                info!("Watcher Dropped");
                                try_next(Error::FailedSend(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                            SendStatus::TransientError(error) => {
                                // An individual connection can for example have been lost when we tried to send. This
                                // could indicate the connection timed out whilst it was held, or some other
                                // transient connection issue. We don't treat this as a failed recipient, but we sleep a little longer here.
                                // Retries are managed by the peer session, where it will open a new connection.
                                debug!(
                                    "Transient error when sending to peer {}: {}",
                                    recipient, error
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                continue;
                            }
                            SendStatus::MaxRetriesReached => {
                                info!("Max retries reached");
                                try_next(Error::FailedSend(recipient), recipient, &mut tasks);
                                break; // we now move to checking next recipient send task..
                            }
                        }
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
                Ok(DeliveryStatus::AllRecipients)
            } else {
                Ok(DeliveryStatus::DeliveredToAll(failed_recipients))
            }
        } else {
            // todo: is this really a success case..?
            Ok(DeliveryStatus::FailedToDeliverAll(failed_recipients))
        }
    }

    /// Get a PeerSession if it already exists, otherwise create and insert
    #[instrument(skip(self))]
    async fn get_or_create(&self, peer: &Peer) -> PeerSession {
        if let Some(entry) = self.sessions.get(peer) {
            return entry.value().clone();
        }
        let link = Link::new(*peer, self.our_endpoint.clone(), self.msg_listener.clone());
        let session = PeerSession::new(link);
        let _ = self.sessions.insert(*peer, session.clone());
        session
    }

    /// Any number of incoming qp2p:Connections can be added.
    /// We will eventually converge to the same one in our comms with the peer.
    async fn add_incoming(&self, peer: &Peer, conn: qp2p::Connection) {
        if let Some(entry) = self.sessions.get(peer) {
            // peer already exists
            let peer_session = entry.value();
            // add to it
            peer_session.add(conn).await;
        } else {
            let link = Link::new_with(
                *peer,
                self.our_endpoint.clone(),
                self.msg_listener.clone(),
                conn,
            );
            let session = PeerSession::new(link);
            let _ = self.sessions.insert(*peer, session);
        }
    }

    // Helper to send a message to a single recipient.
    #[instrument(skip(self, wire_msg))]
    async fn send_to_one(
        &self,
        recipient: Peer,
        wire_msg: WireMsg,
        msg_priority: i32,
    ) -> (Peer, Result<SendWatcher>) {
        let msg_id = wire_msg.msg_id();
        let msg_bytes = match wire_msg.serialize() {
            Ok(bytes) => bytes,
            Err(error) => {
                // early return if we cannot serialise msg
                return (recipient, Err(Error::Messaging(error)));
            }
        };

        trace!(
            "Sending message ({} bytes, msg_id: {:?}) to {:?}",
            msg_bytes.len(),
            msg_id,
            recipient,
        );
        let peer = self.get_or_create(&recipient).await;
        let result = peer.send(msg_id, msg_priority, msg_bytes).await;

        (recipient, result)
    }
}

#[tracing::instrument(skip_all)]
fn setup_comms(
    our_endpoint: Endpoint,
    incoming_connections: IncomingConnections,
    monitoring: RateLimits,
    incoming_msg_pipe: Sender<MsgEvent>,
) -> (Comm, MsgListener) {
    let (comm, msg_listener) = setup(our_endpoint, monitoring, incoming_msg_pipe);

    listen_for_incoming_msgs(msg_listener.clone(), incoming_connections);

    (comm, msg_listener)
}

#[tracing::instrument(skip_all)]
fn setup(
    our_endpoint: Endpoint,
    #[cfg(feature = "back-pressure")] monitoring: RateLimits,
    #[cfg(not(feature = "back-pressure"))] _monitoring: RateLimits,
    receive_msg: Sender<MsgEvent>,
) -> (Comm, MsgListener) {
    #[cfg(feature = "back-pressure")]
    let back_pressure = BackPressure::new(monitoring);
    let (add_connection, conn_receiver) = mpsc::channel(100);
    #[cfg(feature = "back-pressure")]
    let (count_msg, msg_counter) = mpsc::channel(1000);
    #[cfg(not(feature = "back-pressure"))]
    let (count_msg, _msg_counter) = mpsc::channel(1000);

    let msg_listener = MsgListener::new(add_connection, receive_msg, count_msg);

    let comm = Comm {
        our_endpoint,
        msg_listener: msg_listener.clone(),
        #[cfg(feature = "back-pressure")]
        back_pressure: back_pressure.clone(),
        sessions: Arc::new(DashMap::new()),
    };

    #[cfg(feature = "back-pressure")]
    let _ = task::spawn_local(async move { count_msgs(back_pressure, msg_counter).await });
    let _ = task::spawn_local(receive_conns(comm.clone(), conn_receiver));

    (comm, msg_listener)
}

#[tracing::instrument(skip_all)]
#[cfg(feature = "back-pressure")]
async fn count_msgs(back_pressure: BackPressure, mut msg_counter: Receiver<()>) {
    debug!("Entered msg counting listener loop.");
    while let Some(()) = msg_counter.recv().await {
        back_pressure.count_msg().await;
    }
    debug!("Exited msg counting listener loop..!");
}

#[tracing::instrument(skip_all)]
async fn receive_conns(comm: Comm, mut conn_receiver: Receiver<ListenerEvent>) {
    while let Some(ListenerEvent::Connected { peer, connection }) = conn_receiver.recv().await {
        comm.add_incoming(&peer, connection).await;
    }
}

#[tracing::instrument(skip_all)]
fn listen_for_incoming_msgs(
    msg_listener: MsgListener,
    mut incoming_connections: IncomingConnections,
) {
    let _ = task::spawn_local(async move {
        while let Some((connection, incoming_msgs)) = incoming_connections.next().await {
            trace!(
                "incoming_connection from {:?} with connection_id {:?}",
                connection.remote_address(),
                connection.id()
            );

            msg_listener.listen(connection, incoming_msgs);
        }
    });
}

impl Drop for Comm {
    fn drop(&mut self) {
        // Close all existing connections and stop accepting new ones.
        // FIXME: this may be broken – `Comm` is clone, so this will break any clones?
        self.our_endpoint.close();
    }
}

#[derive(Debug)]
pub(crate) enum MsgEvent {
    Received {
        sender: Peer,
        wire_msg: WireMsg,
        original_bytes: Bytes,
    },
}

/// Returns the status of the send operation.
#[derive(Debug, Clone)]
pub(crate) enum DeliveryStatus {
    AllRecipients,
    DeliveredToAll(Vec<Peer>),
    FailedToDeliverAll(Vec<Peer>),
}

#[cfg(test)]
mod tests {
    use super::*;

    use sn_interface::{
        messaging::{
            data::{DataQuery, DataQueryVariant, ServiceMsg},
            AuthKind, DstLocation, MsgId, ServiceAuth,
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
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let comm = Comm::first_node(local_addr(), Config::default(), RateLimits::new(), tx)
                    .await?;

                let (peer0, mut rx0) = new_peer().await?;
                let (peer1, mut rx1) = new_peer().await?;

                let original_message = new_test_msg()?;

                let status = comm.send(&[peer0, peer1], original_message.clone()).await?;

                assert_matches!(status, DeliveryStatus::AllRecipients);

                if let Some(bytes) = rx0.recv().await {
                    // the dst location name is updated per sender, so
                    // we need to update that here before we check
                    let mut check_msg = original_message.clone();
                    check_msg.set_dst_xorname(peer0.name());
                    assert_eq!(WireMsg::from(bytes)?, check_msg);
                }

                if let Some(bytes) = rx1.recv().await {
                    // the dst location name is updated per sender, so
                    // we need to update that here before we check
                    let mut check_msg = original_message.clone();
                    check_msg.set_dst_xorname(peer1.name());
                    assert_eq!(WireMsg::from(bytes)?, check_msg);
                }

                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn successful_send_to_subset() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let comm = Comm::first_node(local_addr(), Config::default(), RateLimits::new(), tx)
                    .await?;

                let (peer0, mut rx0) = new_peer().await?;
                let (peer1, mut rx1) = new_peer().await?;

                let original_message = new_test_msg()?;
                let status = comm.send(&[peer0, peer1], original_message.clone()).await?;

                assert_matches!(status, DeliveryStatus::AllRecipients);

                if let Some(bytes) = rx0.recv().await {
                    // the dst location name is updated per sender, so
                    // we need to update that here before we check
                    let mut check_msg = original_message.clone();
                    check_msg.set_dst_xorname(peer0.name());

                    assert_eq!(WireMsg::from(bytes)?, check_msg);
                }

                assert!(time::timeout(TIMEOUT, rx1.recv())
                    .await
                    .unwrap_or_default()
                    .is_none());
                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn failed_send() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let comm = Comm::first_node(
                    local_addr(),
                    Config {
                        // This makes this test faster.
                        idle_timeout: Some(Duration::from_millis(1)),
                        ..Config::default()
                    },
                    RateLimits::new(),
                    tx,
                )
                .await?;
                let invalid_peer = get_invalid_peer().await?;
                let invalid_addr = invalid_peer.addr();

                let status = comm.send(&[invalid_peer], new_test_msg()?).await?;

                assert_matches!(
                    &status,
                    &DeliveryStatus::FailedToDeliverAll(_) => vec![invalid_addr]
                );

                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn successful_send_after_failed_attempts() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let comm = Comm::first_node(
                    local_addr(),
                    Config {
                        idle_timeout: Some(Duration::from_millis(1)),
                        ..Config::default()
                    },
                    RateLimits::new(),
                    tx,
                )
                .await?;
                let (peer, mut rx) = new_peer().await?;
                let invalid_peer = get_invalid_peer().await?;

                let message = new_test_msg()?;
                let status = comm.send(&[invalid_peer, peer], message.clone()).await?;
                assert_matches!(status, DeliveryStatus::DeliveredToAll(failed_recipients) => {
                    assert_eq!(&failed_recipients, &[invalid_peer])
                });

                if let Some(bytes) = rx.recv().await {
                    // the dst location name is updated per sender, so
                    // we need to update that here before we check
                    let mut check_msg = message.clone();
                    check_msg.set_dst_xorname(peer.name());

                    assert_eq!(WireMsg::from(bytes)?, check_msg);
                }

                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn partially_successful_send() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let comm = Comm::first_node(
                    local_addr(),
                    Config {
                        idle_timeout: Some(Duration::from_millis(1)),
                        ..Config::default()
                    },
                    RateLimits::new(),
                    tx,
                )
                .await?;
                let (peer, mut rx) = new_peer().await?;
                let invalid_peer = get_invalid_peer().await?;

                let message = new_test_msg()?;
                let status = comm.send(&[invalid_peer, peer], message.clone()).await?;

                assert_matches!(
                    status,
                    DeliveryStatus::FailedToDeliverAll(_) => vec![invalid_peer]
                );

                if let Some(bytes) = rx.recv().await {
                    // the dst location name is updated per sender, so
                    // we need to update that here before we check
                    let mut check_msg = message.clone();
                    check_msg.set_dst_xorname(peer.name());

                    assert_eq!(WireMsg::from(bytes)?, check_msg);
                }
                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn send_after_reconnect() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, _rx) = mpsc::channel(1);
                let send_comm =
                    Comm::first_node(local_addr(), Config::default(), RateLimits::new(), tx)
                        .await?;

                let (recv_endpoint, mut incoming_connections, _) =
                    Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
                let recv_addr = recv_endpoint.public_addr();
                let name = xor_name::rand::random();

                let msg0 = new_test_msg()?;
                let status = send_comm
                    .send(&[Peer::new(name, recv_addr)], msg0.clone())
                    .await?;
                assert_matches!(status, DeliveryStatus::AllRecipients);

                let mut msg0_received = false;

                // Receive one message and disconnect from the peer
                {
                    if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
                        if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                            // the dst location name is updated per sender, so
                            // we need to update that here before we check
                            let mut check_msg = msg0.clone();
                            check_msg.set_dst_xorname(name);
                            assert_eq!(WireMsg::from(msg)?, check_msg);
                            msg0_received = true;
                        }

                        // connection dropped here
                    }
                    assert!(msg0_received);
                }

                let msg1 = new_test_msg()?;
                let status = send_comm
                    .send(&[Peer::new(name, recv_addr)], msg1.clone())
                    .await?;
                assert_matches!(status, DeliveryStatus::AllRecipients);

                let mut msg1_received = false;

                if let Some((_, mut incoming_msgs)) = incoming_connections.next().await {
                    if let Some(msg) = time::timeout(TIMEOUT, incoming_msgs.next()).await?? {
                        // the dst location name is updated per sender, so
                        // we need to update that here before we check
                        let mut check_msg = msg1.clone();
                        check_msg.set_dst_xorname(name);
                        assert_eq!(WireMsg::from(msg)?, check_msg);

                        msg1_received = true;
                    }
                }

                assert!(msg1_received);
                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn incoming_connection_lost() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let (tx, mut rx0) = mpsc::channel(1);
                let comm0 =
                    Comm::first_node(local_addr(), Config::default(), RateLimits::new(), tx)
                        .await?;
                let addr0 = comm0.socket_addr();

                let (tx, _rx) = mpsc::channel(1);
                let comm1 =
                    Comm::first_node(local_addr(), Config::default(), RateLimits::new(), tx)
                        .await?;

                // Send a message to establish the connection
                let status = comm1
                    .send(
                        &[Peer::new(xor_name::rand::random(), addr0)],
                        new_test_msg()?,
                    )
                    .await?;
                assert_matches!(status, DeliveryStatus::AllRecipients);

                assert_matches!(rx0.recv().await, Some(MsgEvent::Received { .. }));
                // Drop `comm1` to cause connection lost.
                drop(comm1);

                assert_matches!(time::timeout(TIMEOUT, rx0.recv()).await, Err(_));
                Result::<()>::Ok(())
            })
            .await
    }

    fn new_test_msg() -> Result<WireMsg> {
        let dst_location = DstLocation::Node {
            name: xor_name::rand::random(),
            section_pk: bls::SecretKey::random().public_key(),
        };

        let src_keypair = Keypair::new_ed25519();

        let query = DataQueryVariant::GetChunk(ChunkAddress(xor_name::rand::random()));
        let query = DataQuery {
            adult_index: 0,
            variant: query,
        };
        let query = ServiceMsg::Query(query);
        let payload = WireMsg::serialize_msg_payload(&query)?;

        let auth = ServiceAuth {
            public_key: src_keypair.public_key(),
            signature: src_keypair.sign(&payload),
        };

        let wire_msg =
            WireMsg::new_msg(MsgId::new(), payload, AuthKind::Service(auth), dst_location)?;

        Ok(wire_msg)
    }

    async fn new_peer() -> Result<(Peer, Receiver<Bytes>)> {
        let (endpoint, mut incoming_connections, _) =
            Endpoint::new_peer(local_addr(), &[], Config::default()).await?;
        let addr = endpoint.public_addr();

        let (tx, rx) = mpsc::channel(1);

        let _handle = tokio::task::spawn_local(async move {
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
        let _handle = tokio::task::spawn_local(async move {
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
