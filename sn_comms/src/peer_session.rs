// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Result, STANDARD_CHANNEL_SIZE};

use qp2p::{Connection, Endpoint, UsrMsgBytes};

use custom_debug::Debug;
use dashmap::DashMap;
use sn_interface::{
    messaging::MsgId,
    types::{log_markers::LogMarker, Peer},
};
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

type ConnId = String;

/// These retries are how may _new_ connection attempts do we make.
/// If we fail all of these, HandlePeerFailedSend will be triggered
/// for section nodes, which in turn kicks off fault tracking
const MAX_SENDJOB_RETRIES: usize = 3;

const CONN_RETRY_WAIT: Duration = Duration::from_millis(100);

/// A session to a peer in our network.
///
/// Using the session will open a connection if there is none there.
/// The session is a way to keep connections to a peer in one place
/// and use them efficiently; converge to a single one regardless of concurrent
/// comms initiation between the peers, and so on.
/// The session shall be kept around as long as the peer is deemed worth to keep contact with.
#[derive(Clone)]
pub(crate) struct PeerSession {
    peer: Peer,
    endpoint: Endpoint,
    connections: PeerConnections,
    queue: mpsc::Sender<SendJob>,
}

type PeerConnections = Arc<DashMap<ConnId, Arc<Connection>>>;

impl PeerSession {
    pub(crate) fn new(peer: Peer, endpoint: Endpoint) -> Self {
        let (sender, receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        let connections = PeerConnections::default();

        // Spawn the peer session worker, which will stop automatically when
        // the PeerSession is dropped as the channel will be dropped too.
        PeerSessionWorker::new(peer, connections.clone(), endpoint.clone(), sender.clone())
            .run(receiver);

        Self {
            peer,
            endpoint,
            connections,
            queue: sender,
        }
    }

    /// Sends out a UsrMsg on a bidi connection and awaits response bytes.
    /// As such this may be long running if response is returned slowly.
    /// When sending a msg to a peer, if it fails with an existing
    /// cached connection, it will keep retrying till it either:
    /// a. finds another cached connection which it succeeded with,
    /// b. or it cleaned them all up from the cache creating a new connection
    ///    to the peer as last attempt.
    pub(crate) async fn send_with_bi_return_response(
        &mut self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<UsrMsgBytes, PeerSessionError> {
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
                let conn =
                    create_connection(peer, &self.endpoint, self.connections.clone(), msg_id)
                        .await?;
                (conn, true)
            };

            attempt += 1;

            let conn_id = conn.id();
            trace!("Connection {conn_id} got to {peer:?} for {msg_id:?}");
            let (mut send_stream, recv_stream) = match conn.open_bi().await {
                Ok(bi_stream) => bi_stream,
                Err(err) => {
                    error!("{msg_id:?} Error opening bi-stream over {conn_id}: {err:?}");
                    // remove that broken conn
                    let _conn = self.connections.remove(&conn_id);
                    match is_last_attempt {
                        true => {
                            error!("Last attempt reached for {msg_id:?}, erroring out...");
                            break Err(PeerSessionError::Connection(err));
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
            if let Err(err) = send_stream
                .send_user_msg(bytes.clone(), msg_id.as_ref())
                .await
            {
                error!("Error sending bytes for {msg_id:?} over {stream_id}: {err:?}");
                // remove that broken conn
                let _conn = self.connections.remove(&conn_id);
                match is_last_attempt {
                    true => break Err(PeerSessionError::Send(err)),
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

            match recv_stream.read().await {
                Ok(response) => break Ok(response),
                Err(err) => {
                    error!("Error receiving response to {msg_id:?} from {peer:?} over {stream_id}: {err:?}");
                    let _conn = self.connections.remove(&conn_id);
                    if is_last_attempt {
                        break Err(PeerSessionError::Recv(err));
                    }

                    // tiny wait for comms/dashmap to cope with removal
                    sleep(CONN_RETRY_WAIT).await;
                }
            }
        }
    }

    #[instrument(skip(self, bytes))]
    pub(crate) async fn send(
        &self,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
    ) -> Result<(), PeerSessionError> {
        let (sender, mut receiver) = mpsc::channel(1);

        let job = SendJob {
            msg_id,
            bytes,
            connection_retries: 0,
            reporter: sender,
        };

        self.queue.send(job).await.map_err(|err| {
            error!("Failed to enqueue send job for {msg_id:?}: {err:?}");
            PeerSessionError::PeerSessionJobsQueue
        })?;

        trace!("Send job sent to PeerSessionWorker: {msg_id:?}");
        let peer = self.peer;
        match receiver.recv().await {
            Some(Ok(())) => Ok(()),
            Some(Err(err)) => {
                error!("Sending message {msg_id:?} to {peer:?}, possibly failed: {err:?}");
                Err(err)
            }
            None => {
                // the result sharing channel is closed for some unknown reason,
                error!("Sending message {msg_id:?} to {peer:?} possibly failed, as monitoring of the send job was aborted");
                Err(PeerSessionError::UnknownSendJobOutcome)
            }
        }
    }
}

async fn create_connection(
    peer: Peer,
    endpoint: &Endpoint,
    connections: PeerConnections,
    msg_id: MsgId,
) -> Result<Arc<Connection>, PeerSessionError> {
    debug!("{msg_id:?} create conn attempt to {peer:?}");
    let (conn, _) = endpoint
        .connect_to(&peer.addr())
        .await
        .map_err(PeerSessionError::Connection)?;

    trace!(
        "{msg_id:?}: {} to {} (id: {})",
        LogMarker::ConnectionOpened,
        conn.remote_address(),
        conn.id()
    );

    let conn_id = conn.id();
    debug!("Inserting connection into peer session: {conn_id}");

    let conn = Arc::new(conn);
    let _ = connections.insert(conn_id.clone(), conn.clone());
    debug!("Connection INSERTED into peer session: {conn_id}");

    Ok(conn)
}

struct PeerSessionWorker {
    peer: Peer,
    connections: PeerConnections,
    endpoint: Endpoint,
    queue: mpsc::Sender<SendJob>,
}

impl PeerSessionWorker {
    fn new(
        peer: Peer,
        connections: PeerConnections,
        endpoint: Endpoint,
        queue: mpsc::Sender<SendJob>,
    ) -> Self {
        Self {
            peer,
            connections,
            endpoint,
            queue,
        }
    }

    fn run(mut self, mut channel: mpsc::Receiver<SendJob>) {
        let _handle = tokio::task::spawn(async move {
            let peer = self.peer;
            while let Some(job) = channel.recv().await {
                trace!("Processing session {peer:?} send job: {job:?}");
                self.send_over_peer_connection(job).await;
            }

            // close the channel to prevent senders adding more messages.
            channel.close();

            // drain channel to avoid memory leaks.
            while let Some(msg) = channel.recv().await {
                warn!("Draining channel: dropping {:?}", msg);
            }

            warn!("Finished peer session shutdown: {peer:?}");
        });
    }

    async fn send_over_peer_connection(&mut self, mut job: SendJob) {
        let msg_id = job.msg_id;
        trace!("Sending to peer over connection: {msg_id:?}");

        if job.connection_retries > MAX_SENDJOB_RETRIES {
            let error_to_report = PeerSessionError::MaxRetriesReached(MAX_SENDJOB_RETRIES);
            debug!("{error_to_report}: {msg_id:?}");
            if let Err(err) = job.reporter.try_send(Err(error_to_report)) {
                error!("Couldn't report max retries failure for {msg_id:?}: {err:?}");
            }
            return;
        }

        // Keep this connection creation/retrieval as blocking.
        // This avoids us making many many connection attempts to the same node.
        //
        // If a valid connection exists, retrieval is fast.
        //
        // Attempt to get a connection or make one to another node.
        // if there's no successful connection, we requeue the job after a wait
        // incase there's been a delay adding the connection to Comms
        let conn = match self.get_or_connect(msg_id).await {
            Ok(conn) => conn,
            Err(error) => {
                error!("Error when attempting to send {msg_id:?} to peer. Job will be reenqueued for another attempt after a small timeout: {error:?}");

                // only increment connection attempts if our connections set is empty
                // and so we'll be trying to create a fresh connection
                if self.connections.is_empty() {
                    job.connection_retries += 1;
                }

                // we await here in case the connection is fresh and has not yet been added
                sleep(CONN_RETRY_WAIT).await;
                if let Err(e) = self.queue.send(job).await {
                    warn!("Failed to re-enqueue job {msg_id:?} after failed connection retrieval error {e:?}");
                }

                return;
            }
        };

        let connections = self.connections.clone();
        let conns_count = connections.len();
        let peer = self.peer;
        let queue = self.queue.clone();
        let _handle = tokio::spawn(async move {
            let conn_id = conn.id();
            debug!("Connection exists for sendjob: {msg_id:?}, and has conn_id: {conn_id:?}");

            let send_resp =
                Self::send_with_connection(conn, job.bytes.clone(), connections, msg_id).await;

            match send_resp {
                Ok(()) => {
                    if let Err(err) = job.reporter.try_send(Ok(())) {
                        error!("Couldn't report sucessful sent to {peer:?}: {err:?}");
                    }
                }
                Err(err) => {
                    if err.is_local_close() {
                        error!("Peer connection dropped when trying to send {msg_id:?} (we still have {conns_count:?} connections): {err:?}");
                        // we can retry if we've more connections!
                        if conns_count <= 1 {
                            debug!(
                                "No connections left on this session to {peer:?}, terminating session.",
                            );
                            job.connection_retries += 1;
                        }
                    }

                    warn!(
                        "Transient error while attempting to send, re-enqueing job {msg_id:?} {err:?}. Connection id was {:?}",conn_id
                    );

                    // we await here in case the connection is fresh and has not yet been added
                    sleep(CONN_RETRY_WAIT).await;

                    if let Err(e) = queue.send(job).await {
                        warn!("Failed to re-enqueue job {msg_id:?} after transient error {e:?}");
                    }
                }
            }
        });
    }

    // Gets an existing connection or creates a new one
    async fn get_or_connect(&mut self, msg_id: MsgId) -> Result<Arc<Connection>, PeerSessionError> {
        let peer = self.peer;
        trace!("{msg_id:?} Grabbing a connection from peer session to {peer:?}");

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
            create_connection(peer, &self.endpoint, self.connections.clone(), msg_id).await
        }
    }

    /// Send a message to the peer using the given connection.
    #[instrument(skip_all)]
    async fn send_with_connection(
        conn: Arc<Connection>,
        bytes: UsrMsgBytes,
        connections: PeerConnections,
        msg_id: MsgId,
    ) -> Result<(), PeerSessionError> {
        let conn_id = conn.id();
        let conns_count = connections.len();
        trace!("We have {conns_count} open connections to node {conn_id}.");

        conn.send_with(bytes, 0 /* priority */, msg_id.as_ref()).await.map_err(|error| {
            error!(
                "Error sending out msg... We have {conns_count} open connections to node {conn_id}: {error:?}",
            );
            // clean up failing connections at once, no nead to leak it outside of here
            // next send (e.g. when retrying) will use/create a new connection
            // Timeouts etc should register instantly so we should clean those up fair fast
            let _ = connections.remove(&conn_id);

            debug!("Connection removed from session: {conn_id}");
            // dont close just let the conn timeout incase msgs are coming in...
            // it's removed from out Peer tracking, so won't be used again for sending.
            PeerSessionError::Send(error)
        })
    }
}

#[derive(Debug)]
pub(crate) struct SendJob {
    msg_id: MsgId,
    #[debug(skip)]
    bytes: UsrMsgBytes,
    connection_retries: usize, // TAI: Do we need this if we are using QP2P's retry
    reporter: mpsc::Sender<Result<(), PeerSessionError>>,
}

/// Errors that can be returned from `Comm::send_to_one`.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub(super) enum PeerSessionError {
    #[error("Failed to connect: {0:?}")]
    Connection(qp2p::ConnectionError),
    #[error("Failed to send a message: {0:?}")]
    Send(qp2p::SendError),
    #[error("Failed to receive a message: {0:?}")]
    Recv(qp2p::RecvError),
    #[error("Max number of attempts ({0}) to send msg to the peer has been reached")]
    MaxRetriesReached(usize),
    #[error("Status of sending the msg is unknown")]
    UnknownSendJobOutcome,
    #[error("Peer session job sending channel errored")]
    PeerSessionJobsQueue,
}

impl PeerSessionError {
    fn is_local_close(&self) -> bool {
        matches!(
            self,
            PeerSessionError::Connection(qp2p::ConnectionError::Closed(qp2p::Close::Local))
                | PeerSessionError::Send(qp2p::SendError::ConnectionLost(
                    qp2p::ConnectionError::Closed(qp2p::Close::Local)
                ))
        )
    }
}
