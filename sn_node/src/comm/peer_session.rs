// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::fmt::{self, Formatter};
use std::sync::Arc;
use std::time::Duration;

use super::Link;

use crate::node::{Error, Result, STANDARD_CHANNEL_SIZE};

use qp2p::SendStream;
use qp2p::UsrMsgBytes;
use sn_interface::messaging::MsgId;

use custom_debug::Debug;

use tokio::sync::mpsc;
use tokio::sync::Mutex;

// TODO: temporarily disable priority while we transition to channels
// type Priority = i32;

/// These retries are how may _new_ connection attempts do we make.
/// If we fail all of these, HandlePeerFailedSend will be triggered
/// for section nodes, which in turn kicks off Dysfunction tracking
const MAX_SENDJOB_RETRIES: usize = 3;

#[derive(Debug)]
enum SessionCmd {
    Send(SendJob),
    AddConnection(Arc<qp2p::Connection>),
    Terminate,
}

#[derive(Clone)]
pub(crate) struct PeerSession {
    channel: mpsc::Sender<SessionCmd>,
    link: Link,
}

impl PeerSession {
    pub(crate) fn new(link: Link) -> PeerSession {
        let (sender, receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let _ =
            tokio::task::spawn(PeerSessionWorker::new(link.clone(), sender.clone()).run(receiver));

        PeerSession {
            channel: sender,
            link,
        }
    }

    // this must be restricted somehow, we can't allow an unbounded inflow
    // of connections from a peer...
    pub(crate) async fn add(&self, conn: Arc<qp2p::Connection>) {
        let cmd = SessionCmd::AddConnection(conn);
        if let Err(e) = self.channel.send(cmd).await {
            error!("Error while sending AddConnection {e:?}");
        }
    }

    /// Sends out a UsrMsg on a bidi connection and awaits response bytes
    /// As such this may be long running if response is returned slowly.
    /// This manages retries over bidi connection attempts
    pub(crate) async fn send_with_bi_return_response(
        &mut self,
        bytes: UsrMsgBytes,
        msg_id: MsgId,
    ) -> Result<UsrMsgBytes> {
        // TODO: make a real error here
        let mut attempts = 0;
        let mut response = self
            .link
            .send_on_new_bi_di_stream(bytes.clone(), msg_id)
            .await;
        while response.is_err() && attempts <= 2 {
            error!(
                "Error sending {msg_id:?} to {:?} on bi conn: {response:?}, attempt # {attempts}",
                self.link.peer()
            );
            response = self
                .link
                .send_on_new_bi_di_stream(bytes.clone(), msg_id)
                .await;
            attempts += 1;
            // wee sleep
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        response.map_err(|e| {
            error!("Failed sending {msg_id:?} to {:?} {e:?}", self.link.peer());
            Error::CmdSendError
        })
    }

    #[instrument(skip(self, bytes))]
    pub(crate) async fn send_using_session_or_stream(
        &self,
        msg_id: MsgId,
        bytes: UsrMsgBytes,
        send_stream: Option<Arc<Mutex<SendStream>>>,
    ) -> Result<SendWatcher> {
        let (watcher, reporter) = status_watching();

        let job = SendJob {
            msg_id,
            bytes,
            connection_retries: 0,
            reporter,
            send_stream,
        };

        self.channel
            .send(SessionCmd::Send(job))
            .await
            .map_err(|_| Error::PeerSessionChannel)?;

        trace!("Send job sent to PeerSessionWorker: {msg_id:?}");
        Ok(watcher)
    }

    pub(crate) async fn disconnect(self) {
        if let Err(e) = self.channel.send(SessionCmd::Terminate).await {
            error!("Error while sending Terminate command: {e:?}");
        }
    }
}

impl fmt::Debug for PeerSession {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "PeerSession {:?}", self.link.peer())
    }
}

/// After processing each `SessionCmd`, we decide whether to keep going
#[must_use]
enum SessionStatus {
    Ok,
    Terminating,
}

struct PeerSessionWorker {
    queue: mpsc::Sender<SessionCmd>,
    pub(crate) link: Link,
}

impl PeerSessionWorker {
    fn new(link: Link, queue: mpsc::Sender<SessionCmd>) -> Self {
        Self { queue, link }
    }

    async fn run(mut self, mut channel: mpsc::Receiver<SessionCmd>) {
        while let Some(session_cmd) = channel.recv().await {
            let peer = *self.link.peer();
            trace!("Processing session {peer:?} cmd: {session_cmd:?}");

            let status = match session_cmd {
                SessionCmd::Send(job) => {
                    if let Some(send_stream) = job.send_stream {
                        // send response on the stream
                        debug!("Sending on stream via PeerSessionWorker");
                        let _handle = tokio::spawn(async move {
                            let stream_prio = 10;
                            let mut send_stream = send_stream.lock().await;
                            send_stream.set_priority(stream_prio);
                            let stream_id = send_stream.id();
                            if let Err(error) = send_stream.send_user_msg(job.bytes).await {
                                error!(
                                    "Could not send msg {:?} over response {stream_id} to {peer:?}: {error:?}",
                                    job.msg_id
                                );

                                job.reporter.send(SendStatus::TransientError(format!(
                                    "Could not send msg on response {stream_id} to {peer:?} for {:?}", job.msg_id
                                )));
                            } else {
                                // Attempt to gracefully terminate the stream.
                                // If this errors it does _not_ mean our message has not been sent
                                let _ = send_stream.finish().await;

                                job.reporter.send(SendStatus::Sent);
                            }
                        });

                        SessionStatus::Ok
                    } else {
                        //another
                        match self.send_over_peer_connection(job).await {
                            Ok(s) => s,
                            Err(error) => {
                                error!("session error {error:?}");
                                // don't breakout here?
                                // TODO: is this correct?
                                continue;
                            }
                        }
                    }
                }
                SessionCmd::AddConnection(conn) => {
                    self.link.add(conn);
                    SessionStatus::Ok
                }
                SessionCmd::Terminate => SessionStatus::Terminating,
            };

            match status {
                SessionStatus::Terminating => {
                    info!("Terminating connection to {:?}", self.link.peer());
                    break;
                }
                SessionStatus::Ok => (),
            }
        }

        // close the channel to prevent senders adding more messages.
        channel.close();

        // drain channel to avoid memory leaks.
        while let Some(msg) = channel.recv().await {
            info!("Draining channel: dropping {:?}", msg);
        }

        info!("Finished peer session shutdown");
    }

    async fn send_over_peer_connection(&mut self, mut job: SendJob) -> Result<SessionStatus> {
        let id = job.msg_id;
        trace!("Sending to peer over connection: {id:?}");

        if job.connection_retries > MAX_SENDJOB_RETRIES {
            debug!("max retries reached... {id:?}");
            job.reporter.send(SendStatus::MaxRetriesReached);
            return Ok(SessionStatus::Ok);
        }

        let queue = self.queue.clone();
        let the_peer = *self.link.peer();

        let mut link = self.link.clone();

        let _handle = tokio::spawn(async move {
            // Attempt to get a connection or make one to another node.
            // if there's no connection here yet, we requeue the job after a wait
            // incase there's been a delay adding the connection to Comms
            let conn = match link.get_or_connect(id).await {
                Ok(conn) => conn,
                Err(error) => {
                    error!("Error when attempting to send to peer. Job will be reenqueued for another attempt after a small timeout");

                    // only increment connection attempts if our connections set is empty
                    // and so we'll be trying to create a fresh connection
                    job.reporter
                        .send(SendStatus::TransientError(format!("{error:?}")));

                    // we await here in case the connection is fresh and has not yet been added
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    if let Err(e) = queue.send(SessionCmd::Send(job)).await {
                        warn!("Failed to re-enqueue job {id:?} after failed connection retrieval error {e:?}");
                    }

                    return;
                }
            };

            let connection_id = conn.id();
            debug!("Connection exists for sendjob: {id:?}, and has conn_id: {connection_id:?}");

            let send_resp = Link::send_with_connection(job.bytes.clone(), 0, conn).await;

            match send_resp {
                Ok(_) => {
                    job.reporter.send(SendStatus::Sent);
                }
                Err(err) => {
                    if err.is_local_close() {
                        debug!("Peer linked dropped when trying to send {:?}.", id);
                        error!("the error on send :{err:?}");

                        job.connection_retries += 1;
                    }

                    warn!(
                        "Transient error while attempting to send, re-enqueing job {id:?} {err:?}. Connection id was {:?}",connection_id
                    );

                    // we await here in case the connection is fresh and has not yet been added
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                    job.reporter
                        .send(SendStatus::TransientError(format!("{err:?}")));

                    if let Err(e) = queue.send(SessionCmd::Send(job)).await {
                        warn!("Failed to re-enqueue job {id:?} after transient error {e:?}");
                    }

                    // Ok(())
                }
            }
        });

        Ok(SessionStatus::Ok)
    }
}

#[derive(Debug)]
pub(crate) struct SendJob {
    msg_id: MsgId,
    #[debug(skip)]
    bytes: UsrMsgBytes,
    connection_retries: usize, // TAI: Do we need this if we are using QP2P's retry
    reporter: StatusReporting,
    send_stream: Option<Arc<Mutex<SendStream>>>,
}

impl PartialEq for SendJob {
    fn eq(&self, other: &Self) -> bool {
        self.msg_id == other.msg_id
            && self.bytes == other.bytes
            && self.connection_retries == other.connection_retries
    }
}

impl Eq for SendJob {}

impl std::hash::Hash for SendJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.msg_id.hash(state);
        self.bytes.hash(state);
        self.connection_retries.hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SendStatus {
    Enqueued,
    Sent,
    TransientError(String),
    WatcherDropped,
    MaxRetriesReached,
}

pub(crate) struct SendWatcher {
    receiver: tokio::sync::watch::Receiver<SendStatus>,
}

impl SendWatcher {
    /// Reads current status
    #[allow(unused)]
    pub(crate) fn status(&self) -> SendStatus {
        self.receiver.borrow().clone()
    }

    /// Waits until a new status arrives.
    pub(crate) async fn await_change(&mut self) -> SendStatus {
        if self.receiver.changed().await.is_ok() {
            self.receiver.borrow_and_update().clone()
        } else {
            SendStatus::WatcherDropped
        }
    }
}

#[derive(Debug)]
struct StatusReporting {
    sender: tokio::sync::watch::Sender<SendStatus>,
}

impl StatusReporting {
    fn send(&self, status: SendStatus) {
        // todo: ok to drop error here?
        let _ = self.sender.send(status);
    }
}

fn status_watching() -> (SendWatcher, StatusReporting) {
    let (sender, receiver) = tokio::sync::watch::channel(SendStatus::Enqueued);
    (SendWatcher { receiver }, StatusReporting { sender })
}
