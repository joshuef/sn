// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Link;

use crate::node::{Error, Result};

use sn_interface::messaging::MsgId;

use bytes::Bytes;
use custom_debug::Debug;
use priority_queue::PriorityQueue;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::time::Instant;

type Priority = i32;

const MAX_RETRIES: usize = 10;
const DEFAULT_DESIRED_RATE: f64 = 10.0; // 10 msgs / s
const SLEEP_TIME: Duration = Duration::from_millis(200);

#[derive(Clone)]
pub(crate) struct PeerSession {
    link: Link,
    msg_queue: Rc<RefCell<PriorityQueue<SendJob, Priority>>>,
    sent: MsgThroughput,
    attempted: MsgThroughput,
    peer_desired_rate: Rc<RefCell<f64>>, // msgs per s
    disconnnected: Rc<RefCell<bool>>,
}

impl PeerSession {
    pub(crate) fn new(link: Link) -> Self {
        let session = Self {
            link,
            msg_queue: Rc::new(RefCell::new(PriorityQueue::new())),
            sent: MsgThroughput::default(),
            attempted: MsgThroughput::default(),
            peer_desired_rate: Rc::new(RefCell::new(DEFAULT_DESIRED_RATE)),
            disconnnected: Rc::new(RefCell::new(false)),
        };

        let session_clone = session.clone();
        let _ = tokio::task::spawn_local(async move { session_clone.keep_sending().await });

        session
    }

    pub(crate) async fn remove_expired(&self) {
        self.link.remove_expired().await
    }

    pub(crate) async fn is_connected(&self) -> bool {
        self.link.is_connected().await
    }

    #[allow(unused)]
    pub(crate) async fn throughput(&self) -> f64 {
        self.sent.value()
    }

    #[allow(unused)]
    pub(crate) async fn success_ratio(&self) -> f64 {
        self.sent.value() / self.attempted.value()
    }

    // this must be restricted somehow, we can't allow an unbounded inflow
    // of connections from a peer...
    pub(crate) async fn add(&self, conn: qp2p::Connection) {
        if self.disconnected().await {
            // if we have disconnected from a peer, will we allow it to connect to us again anyway..??
            conn.close(Some(
                "We have disconnected from the peer and do not allow incoming connections."
                    .to_string(),
            ));
            return;
        }

        self.link.add(conn).await
    }

    #[instrument(skip(self, msg_bytes, msg_priority))]
    pub(crate) async fn send(
        &self,
        msg_id: MsgId,
        msg_priority: i32,
        msg_bytes: Bytes,
    ) -> Result<SendWatcher> {
        if self.disconnected().await {
            // should not happen (be reachable) if we only access PeerSession from Comm
            return Err(Error::InvalidState);
        }

        let (watcher, reporter) = status_watching();

        let job = SendJob {
            msg_id,
            msg_bytes,
            retries: 3,
            reporter,
        };
        let _ = self.msg_queue.borrow_mut().push(job, msg_priority);

        Ok(watcher)
    }

    #[cfg(feature = "back-pressure")]
    pub(crate) async fn update_send_rate(&self, peer_desired_rate: f64) {
        *self.peer_desired_rate.borrow_mut() = peer_desired_rate;
    }

    // consume self
    // NB that clones could still exist, however they would be in the disconnected state
    // if only accessing via session map (as intended)
    pub(crate) async fn disconnect(self) {
        *self.disconnnected.borrow_mut() = true;
        self.msg_queue.borrow_mut().clear();
        self.link.disconnect().await;
    }

    // could be accessed via a clone
    async fn disconnected(&self) -> bool {
        *self.disconnnected.borrow()
    }

    #[instrument(skip_all)]
    async fn keep_sending(&self) {
        loop {
            if self.disconnected().await {
                break;
            }

            let expected_rate = { *self.peer_desired_rate.borrow() };
            let actual_rate = self.attempted.value();
            if actual_rate > expected_rate {
                let diff = actual_rate - expected_rate;
                let diff_ms = Duration::from_millis((diff * 1000_f64) as u64);
                tokio::time::sleep(diff_ms).await;
                continue;
            } else if self.msg_queue.borrow().is_empty() {
                tokio::time::sleep(SLEEP_TIME).await;
                continue;
            }

            #[cfg(test)]
            {
                let queue = self.msg_queue.borrow();
                debug!("Peer {} queue length: {}", self.link.peer(), queue.len());
                for (job, priority) in queue.iter() {
                    debug!("Prio: {}, Job: {:?}", priority, job);
                }
            }

            let queue_res = { self.msg_queue.borrow_mut().pop() };
            if let Some((mut job, prio)) = queue_res {
                if job.retries >= MAX_RETRIES {
                    // break this loop, report error to other nodes
                    // await decision on how to continue

                    // (or send event on a channel (to report error to other nodes), then sleep for a very long time, then try again?)

                    job.reporter
                        .send(SendStatus::MaxRetriesReached(job.retries));

                    break; // this means we will stop all sending to this peer!
                }

                if let Err(err) = self.link.send(job.msg_bytes.clone()).await {
                    job.retries += 1;
                    if err.is_local_close() {
                        job.reporter.send(SendStatus::PeerLinkDropped);
                        break; // this means we will stop all sending to this peer!
                    } else {
                        job.reporter
                            .send(SendStatus::TransientError(format!("{:?}", err)));

                        let _ = self.msg_queue.borrow_mut().push(job, prio);
                    }
                } else {
                    job.reporter.send(SendStatus::Sent);
                    self.sent.increment(); // on success
                }

                self.attempted.increment(); // both on fail and success
            }
        }
    }
}

#[derive(Clone, Debug)]
struct MsgThroughput {
    msgs: Rc<AtomicUsize>,
    since: Instant,
}

impl Default for MsgThroughput {
    fn default() -> Self {
        Self {
            msgs: Rc::new(AtomicUsize::new(0)),
            since: Instant::now(),
        }
    }
}

impl MsgThroughput {
    fn increment(&self) {
        let _ = self.msgs.fetch_add(1, Ordering::SeqCst);
    }

    // msgs / s
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
    msg_bytes: Bytes,
    retries: usize,
    reporter: StatusReporting,
}

impl PartialEq for SendJob {
    fn eq(&self, other: &Self) -> bool {
        self.msg_id == other.msg_id
            && self.msg_bytes == other.msg_bytes
            && self.retries == other.retries
    }
}

impl Eq for SendJob {}

impl std::hash::Hash for SendJob {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.msg_id.hash(state);
        self.msg_bytes.hash(state);
        self.retries.hash(state);
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SendStatus {
    Enqueued,
    Sent,
    PeerLinkDropped,
    TransientError(String),
    MaxRetriesReached(usize),
    WatcherDropped,
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
