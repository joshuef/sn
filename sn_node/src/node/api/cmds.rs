// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::Proposal, XorName};
use sn_interface::network_knowledge::{SectionAuthorityProvider, SectionKeyShare};
use sn_interface::types::Peer;
use sn_interface::{
    messaging::{
        system::{DkgFailureSigSet, KeyedSig, NodeState, SectionAuth, SystemMsg},
        DstLocation, WireMsg,
    },
    types::ReplicatedDataAddress,
};

use bytes::Bytes;
use custom_debug::Debug;
use sn_consensus::Generation;
use std::{
    collections::BTreeSet,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Internal cmds for a node.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub(crate) enum Cmd {
    /// Cleanup node's PeerLinks, removing any unsused, unconnected peers
    CleanupPeerLinks,
    /// Handle `message` from `sender`.
    /// Holding the WireMsg that has been received from the network,
    HandleMsg {
        sender: Peer,
        wire_msg: WireMsg,
        #[debug(skip)]
        // original bytes to avoid reserializing for entropy checks
        original_bytes: Option<Bytes>,
    },
    /// Handle a timeout previously scheduled with `ScheduleTimeout`.
    HandleTimeout(u64),
    /// Handle peer that's been detected as lost.
    HandlePeerLost(Peer),
    /// Handle agreement on a proposal.
    HandleAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle a new Node joining agreement.
    HandleNewNodeOnline(SectionAuth<NodeState>),
    /// Handle a Node leaving agreement.
    HandleNodeLeft(SectionAuth<NodeState>),
    /// Handle agree on elders. This blocks node message processing until complete.
    HandleNewEldersAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle the outcome of a DKG session where we are one of the participants (that is, one of
    /// the proposed new elders).
    HandleDkgOutcome {
        section_auth: SectionAuthorityProvider,
        outcome: SectionKeyShare,
        generation: Generation,
    },
    /// Handle a DKG failure that was observed by a majority of the DKG participants.
    HandleDkgFailure(DkgFailureSigSet),
    /// Send a message to the given `recipients`.
    SendMsg {
        recipients: Vec<Peer>,
        wire_msg: WireMsg,
    },
    /// Send the batch of data messages in a throttled/controlled fashion to the given `recipients`.
    /// chunks addresses are provided, so that we only retrieve the data right before we send it,
    /// hopefully reducing memory impact or data replication
    EnqueueDataForReplication {
        // throttle_duration: Duration,
        recipient: Peer,
        /// Batches of ReplicatedDataAddress to be sent together
        data_batch: Vec<ReplicatedDataAddress>,
        /// How close is the data to the requester's name. 0 indicates it is the closest, and therefore highest prio data
        closeness: usize
    },
    /// Performs serialisation and signing for sending of NodeMsg.
    /// This cmd only send this to other nodes
    SignOutgoingSystemMsg { msg: SystemMsg, dst: DstLocation },
    /// Send a message to `delivery_group_size` peers out of the given `recipients`.
    SendMsgDeliveryGroup {
        recipients: Vec<Peer>,
        delivery_group_size: usize,
        wire_msg: WireMsg,
    },
    /// Schedule a timeout after the given duration. When the timeout expires, a `HandleTimeout`
    /// cmd is raised. The token is used to identify the timeout.
    ScheduleTimeout { duration: Duration, token: u64 },
    /// Proposes peers as offline
    ProposeOffline(BTreeSet<XorName>),
    /// Send a signal to all Elders to
    /// test the connectivity to a specific node
    StartConnectivityTest(XorName),
    /// Test Connectivity
    TestConnectivity(XorName),
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::CleanupPeerLinks => {
                write!(f, "CleanupPeerLinks")
            }
            Cmd::HandleTimeout(_) => write!(f, "HandleTimeout"),
            Cmd::ScheduleTimeout { .. } => write!(f, "ScheduleTimeout"),
            Cmd::HandleMsg { wire_msg, .. } => {
                write!(f, "HandleMsg {:?}", wire_msg.msg_id())
            }
            Cmd::HandlePeerLost(peer) => write!(f, "HandlePeerLost({:?})", peer.name()),
            Cmd::HandleAgreement { .. } => write!(f, "HandleAgreement"),
            Cmd::HandleNewEldersAgreement { .. } => write!(f, "HandleNewEldersAgreement"),
            Cmd::HandleNewNodeOnline(_) => write!(f, "HandleNewNodeOnline"),
            Cmd::HandleNodeLeft(_) => write!(f, "HandleNodeLeft"),
            Cmd::HandleDkgOutcome { .. } => write!(f, "HandleDkgOutcome"),
            Cmd::HandleDkgFailure(_) => write!(f, "HandleDkgFailure"),
            #[cfg(not(test))]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(f, "SendMsg {:?}", wire_msg.msg_id())
            }
            #[cfg(test)]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(
                    f,
                    "SendMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::SignOutgoingSystemMsg { .. } => write!(f, "SignOutgoingSystemMsg"),
            Cmd::EnqueueDataForReplication { .. } => write!(f, "ThrottledSendBatchMsgs"),
            Cmd::SendMsgDeliveryGroup { wire_msg, .. } => {
                write!(f, "SendMsgDeliveryGroup {:?}", wire_msg.msg_id())
            }
            Cmd::ProposeOffline(_) => write!(f, "ProposeOffline"),
            Cmd::StartConnectivityTest(_) => write!(f, "StartConnectivityTest"),
            Cmd::TestConnectivity(_) => write!(f, "TestConnectivity"),
        }
    }
}

/// Generate unique timer token.
pub(crate) fn next_timer_token() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
