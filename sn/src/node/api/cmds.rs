// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::Proposal, XorName};
use sn_interface::messaging::{
    system::{DkgFailureSigSet, KeyedSig, NodeState, SectionAuth, SystemMsg},
    DstLocation, MsgId, NodeMsgAuthority, WireMsg,
};
use sn_interface::network_knowledge::{SectionAuthorityProvider, SectionKeyShare};
use sn_interface::types::Peer;

use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use custom_debug::Debug;
use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Internal cmds for a node.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub(crate) enum Cmd {
    /// Handle `message` from `sender`.
    /// Holding the WireMsg that has been received from the network,
    HandleMsg {
        sender: Peer,
        wire_msg: WireMsg,
        #[debug(skip)]
        // original bytes to avoid reserializing for entropy checks
        original_bytes: Option<Bytes>,
    },
    // TODO: rename this as/when this is all node for clarity
    /// Handle Node, either directly or notify via event listener
    HandleSystemMsg {
        sender: Peer,
        msg_id: MsgId,
        msg: SystemMsg,
        msg_authority: NodeMsgAuthority,
        dst_location: DstLocation,
        #[debug(skip)]
        payload: Bytes,
        #[debug(skip)]
        known_keys: Vec<BlsPublicKey>,
    },
    /// Handle a timeout previously scheduled with `ScheduleTimeout`.
    HandleTimeout(u64),
    /// Handle peer that's been detected as lost.
    HandlePeerLost(Peer),
    /// Handle agreement on a proposal.
    HandleAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle a new Node joining agreement.
    HandleNewNodeOnline(SectionAuth<NodeState>),
    /// Handle agree on elders. This blocks node message processing until complete.
    HandleNewEldersAgreement { proposal: Proposal, sig: KeyedSig },
    /// Handle the outcome of a DKG session where we are one of the participants (that is, one of
    /// the proposed new elders).
    HandleDkgOutcome {
        section_auth: SectionAuthorityProvider,
        outcome: SectionKeyShare,
    },
    /// Handle a DKG failure that was observed by a majority of the DKG participants.
    HandleDkgFailure(DkgFailureSigSet),
    /// Send a message to the given `recipients`.
    SendMsg {
        recipients: Vec<Peer>,
        wire_msg: WireMsg,
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
    /// Test peer's connectivity
    SendAcceptedOnlineShare {
        peer: Peer,
        // Previous name if relocated.
        previous_name: Option<XorName>,
    },
    /// Proposes a peer as offline
    ProposeOffline(XorName),
    /// Send a signal to all Elders to
    /// test the connectivity to a specific node
    StartConnectivityTest(XorName),
    /// Test Connectivity
    TestConnectivity(XorName),
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::HandleTimeout(_) => write!(f, "HandleTimeout"),
            Cmd::ScheduleTimeout { .. } => write!(f, "ScheduleTimeout"),
            Cmd::HandleSystemMsg { msg_id, .. } => {
                write!(f, "HandleSystemMsg {:?}", msg_id)
            }
            Cmd::HandleMsg { wire_msg, .. } => {
                write!(f, "HandleMsg {:?}", wire_msg.msg_id())
            }
            Cmd::HandlePeerLost(peer) => write!(f, "HandlePeerLost({:?})", peer.name()),
            Cmd::HandleAgreement { .. } => write!(f, "HandleAgreement"),
            Cmd::HandleNewEldersAgreement { .. } => write!(f, "HandleNewEldersAgreement"),
            Cmd::HandleNewNodeOnline(_) => write!(f, "HandleNewNodeOnline"),
            Cmd::HandleDkgOutcome { .. } => write!(f, "HandleDkgOutcome"),
            Cmd::HandleDkgFailure(_) => write!(f, "HandleDkgFailure"),
            #[cfg(not(feature = "unstable-wiremsg-debuginfo"))]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(f, "SendMsg {:?}", wire_msg.msg_id())
            }
            #[cfg(feature = "unstable-wiremsg-debuginfo")]
            Cmd::SendMsg { wire_msg, .. } => {
                write!(
                    f,
                    "SendMsg {:?} {:?}",
                    wire_msg.msg_id(),
                    wire_msg.payload_debug
                )
            }
            Cmd::SignOutgoingSystemMsg { .. } => write!(f, "SignOutgoingSystemMsg"),
            Cmd::SendMsgDeliveryGroup { wire_msg, .. } => {
                write!(f, "SendMsgDeliveryGroup {:?}", wire_msg.msg_id())
            }
            Cmd::SendAcceptedOnlineShare { .. } => write!(f, "SendAcceptedOnlineShare"),
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
