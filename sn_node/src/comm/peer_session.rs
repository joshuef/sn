// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Link;

#[cfg(feature = "back-pressure")]
use crate::log_sleep;
use crate::node::Result;

use qp2p::RetryConfig;
use qp2p::UsrMsgBytes;
use sn_interface::messaging::MsgId;

use custom_debug::Debug;

#[cfg(feature = "back-pressure")]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[cfg(feature = "back-pressure")]
use std::time::Duration;
use tokio::sync::mpsc;
#[cfg(feature = "back-pressure")]
use tokio::time::Instant;

/// These retries are how may _new_ connection attempts do we make.
/// If we fail all of these, HandlePeerFailedSend will be triggered
/// for section nodes, which in turn kicks off Dysfunction tracking
const MAX_SENDJOB_RETRIES: usize = 3;

#[cfg(feature = "back-pressure")]
const DEFAULT_DESIRED_RATE: f64 = 10.0; // 10 msgs / s

#[derive(Debug)]
enum SessionCmd {
    Send(SendJob),
    #[cfg(feature = "back-pressure")]
    SetMsgsPerSecond(f64),
    RemoveExpired,
    AddConnection(qp2p::Connection),
    Terminate,
}

#[derive(Clone)]
pub(crate) struct PeerSession {
    channel: mpsc::Sender<SessionCmd>,
}

impl PeerSession {
    pub(crate) fn new(link: Link) -> PeerSession {
        let (sender, receiver) = mpsc::channel(10_000);

        let _ =
            tokio::task::spawn_local(PeerSessionWorker::new(link, sender.clone()).run(receiver));

        PeerSession { channel: sender }
    }

    pub(crate) async fn remove_expired(&self) {
        if let Err(e) = self.channel.send(SessionCmd::RemoveExpired).await {
            error!("Error while sending RemoveExpired cmd {e:?}");
        }
    }

    // this must be restricted somehow, we can't allow an unbounded inflow
    // of connections from a peer...
    pub(crate) async fn add(&self, conn: qp2p::Connection) {
        let cmd = SessionCmd::AddConnection(conn.clone());
        if let Err(e) = self.channel.send(cmd).await {
            error!("Error while sending AddConnection {e:?}");

            // if we have disconnected from a peer, will we allow it to connect to us again anyway..??
            conn.close(Some(
                "We have disconnected from the peer and do not allow incoming connections."
                    .to_string(),
            ));
        }
    }

    #[instrument(skip(self, bytes))]
    pub(crate) async fn send(&self, msg_id: MsgId, bytes: UsrMsgBytes) -> Result<SendWatcher> {
        let (watcher, reporter) = status_watching();

        let job = SendJob {
            msg_id,
            bytes,
            retries: 0,
            reporter,
        };

        if let Err(e) = self.channel.send(SessionCmd::Send(job)).await {
            error!("Error while sending Send command {e:?}");
        }

        trace!("Send job sent: {msg_id:?}");
        Ok(watcher)
    }

    #[cfg(feature = "back-pressure")]
    pub(crate) async fn update_send_rate(&self, rate: f64) {
        if let Err(e) = self.channel.send(SessionCmd::SetMsgsPerSecond(rate)).await {
            error!("Error while sending SetMsgsPerSecond cmd {e:?}");
        }
    }

    pub(crate) async fn disconnect(self) {
        if let Err(e) = self.channel.send(SessionCmd::Terminate).await {
            error!("Error while sending Terminate command: {e:?}");
        }
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
    link: Link,
    #[cfg(feature = "back-pressure")]
    sent: MsgThroughput,
    #[cfg(feature = "back-pressure")]
    attempted: MsgThroughput,
    #[cfg(feature = "back-pressure")]
    peer_desired_rate: f64, // msgs per s
}

impl PeerSessionWorker {
    fn new(link: Link, queue: mpsc::Sender<SessionCmd>) -> Self {
        Self {
            queue,
            link,
            #[cfg(feature = "back-pressure")]
            sent: MsgThroughput::default(),
            #[cfg(feature = "back-pressure")]
            attempted: MsgThroughput::default(),
            #[cfg(feature = "back-pressure")]
            peer_desired_rate: DEFAULT_DESIRED_RATE,
        }
    }

    async fn run(#[allow(unused_mut)] mut self, mut channel: mpsc::Receiver<SessionCmd>) {
        while let Some(session_cmd) = channel.recv().await {
            trace!(
                "Processing session cmd: {:?} to peer {:?}",
                session_cmd,
                self.link.peer(),
            );

            let status = match session_cmd {
                SessionCmd::Send(job) => self.send(job).await,
                #[cfg(feature = "back-pressure")]
                SessionCmd::SetMsgsPerSecond(rate) => {
                    self.peer_desired_rate = rate;
                    SessionStatus::Ok
                }
                SessionCmd::RemoveExpired => {
                    self.link.remove_expired();
                    SessionStatus::Ok
                }
                SessionCmd::AddConnection(conn) => {
                    self.link.add(conn);
                    SessionStatus::Ok
                }
                SessionCmd::Terminate => SessionStatus::Terminating,
            };

            match status {
                SessionStatus::Terminating => {
                    info!("Terminating connection");
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

        // disconnect the link.
        self.link.disconnect();

        info!("Finished peer session shutdown");
    }

    async fn send(&mut self, mut job: SendJob) -> SessionStatus {
        let id = job.msg_id;

        trace!("Performing sendjob: {id:?}");

        if job.retries > MAX_SENDJOB_RETRIES {
            job.reporter.send(SendStatus::MaxRetriesReached);
            return SessionStatus::Ok;
        }

        #[cfg(feature = "back-pressure")]
        let actual_rate = self.attempted.value();
        #[cfg(feature = "back-pressure")]
        if actual_rate > self.peer_desired_rate {
            let diff = actual_rate - self.peer_desired_rate;
            log_sleep!(Duration::from_millis((diff * 10_f64) as u64));
        }
        #[cfg(feature = "back-pressure")]
        self.attempted.increment(); // both on fail and success

        let send_resp = self
            .link
            .send_with(job.bytes.clone(), 0, Some(&RetryConfig::default()))
            .await;

        debug!("==========> SEND WITH DONE for  {id:?} to {:?}", self.link.peer());

        match send_resp {
            Ok(_) => {
                job.reporter.send(SendStatus::Sent);
                #[cfg(feature = "back-pressure")]
                self.sent.increment(); // on success
            }
            Err(err) if err.is_local_close() => {
                info!("Peer linked dropped");
                job.reporter.send(SendStatus::PeerLinkDropped);
                return SessionStatus::Terminating;
            }
            Err(err) => {
                warn!("Transient error while attempting to send, re-enqueing job {id:?} {err:?}");
                job.reporter
                    .send(SendStatus::TransientError(format!("{err:?}")));

                job.retries += 1;

                if let Err(e) = self.queue.send(SessionCmd::Send(job)).await {
                    warn!("Failed to re-enqueue job {id:?} after transient error {e:?}");
                    return SessionStatus::Terminating;
                }
            }
        }

        SessionStatus::Ok
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "back-pressure")]
struct MsgThroughput {
    msgs: Arc<AtomicUsize>,
    since: Instant,
}

#[cfg(feature = "back-pressure")]
impl Default for MsgThroughput {
    fn default() -> Self {
        Self {
            msgs: Arc::new(AtomicUsize::new(0)),
            since: Instant::now(),
        }
    }
}

#[cfg(feature = "back-pressure")]
impl MsgThroughput {
    #[cfg(feature = "back-pressure")]
    fn increment(&self) {
        let _ = self.msgs.fetch_add(1, Ordering::SeqCst);
    }

    // msgs / s
    #[cfg(feature = "back-pressure")]
    fn value(&self) -> f64 {
        let msgs = self.msgs.load(Ordering::SeqCst);
        let seconds = (Instant::now() - self.since).as_secs();
        msgs as f64 / f64::max(1.0, seconds as f64)
    }
}

#[derive(Debug)]
pub(crate) struct SendJob {
    msg_id: MsgId,
    #[debug(skip)]
    bytes: UsrMsgBytes,
    retries: usize, // TAI: Do we need this if we are using QP2P's retry
    reporter: StatusReporting,
}

impl PartialEq for SendJob {
    fn eq(&self, other: &Self) -> bool {
        self.msg_id == other.msg_id && self.bytes == other.bytes && self.retries == other.retries
    }
}

impl Eq for SendJob {}

impl std::hash::Hash for SendJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.msg_id.hash(state);
        self.bytes.hash(state);
        self.retries.hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SendStatus {
    Enqueued,
    Sent,
    PeerLinkDropped,
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
