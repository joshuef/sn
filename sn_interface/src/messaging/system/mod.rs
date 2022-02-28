// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod agreement;
mod join;
mod join_as_relocated;
mod node_msgs;
mod node_state;
mod signed;

pub use agreement::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, Proposal, SectionAuth};
pub use join::{JoinRejectionReason, JoinRequest, JoinResponse, ResourceProofResponse};
pub use join_as_relocated::{JoinAsRelocatedRequest, JoinAsRelocatedResponse};
pub use node_msgs::{NodeCmd, NodeEvent, NodeQuery, NodeQueryResponse};
pub use node_state::{MembershipState, NodeState, RelocateDetails};
pub use signed::{KeyedSig, SigShare};

/// List of peers of a section
pub type SectionPeers = BTreeSet<SectionAuth<NodeState>>;

use crate::messaging::{EndUser, MsgId, SectionAuthorityProvider};
use bls_dkg::key_gen::message::Message as DkgMessage;
use bytes::Bytes;
use secured_linked_list::SecuredLinkedList;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
use xor_name::{Prefix, XorName};

use super::authority::SectionAuth as SectionAuthProof;
use super::AuthorityProof;

#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
#[allow(clippy::large_enum_variant)]
/// Message sent over the among nodes
pub enum SystemMsg {
    /// Message sent to a peer when a message with outdated section
    /// information was received, attaching the bounced message so
    /// the peer can resend it with up to date destination information.
    AntiEntropyRetry {
        /// Current `SectionAuthorityProvider` of the sender's section.
        section_auth: SectionAuthorityProvider,
        /// Sender's section signature over the `SectionAuthorityProvider`.
        section_signed: KeyedSig,
        /// Sender's section chain truncated from the dst section key found in the `bounced_msg`.
        proof_chain: SecuredLinkedList,
        /// Message bounced due to outdated destination section information.
        #[debug(skip)]
        bounced_msg: Bytes,
    },
    /// Message sent to a peer when a message needs to be sent to a different
    /// and/or closest section, attaching the bounced message so the peer can
    /// resend it to the correct section with up to date destination information.
    AntiEntropyRedirect {
        /// Current `SectionAuthorityProvider` of a closest section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider` of the closest
        /// section the bounced message shall be resent to.
        section_signed: KeyedSig,
        /// Section chain (from genesis key) for the closest section.
        section_chain: SecuredLinkedList,
        /// Message bounced that shall be resent by the peer.
        #[debug(skip)]
        bounced_msg: Bytes,
    },
    /// Message to update a section when they bounced a message as untrusted back at us.
    /// That section must be behind our current knowledge.
    AntiEntropyUpdate {
        /// Current `SectionAuthorityProvider` of our section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider` of our
        /// section the bounced message shall be resent to.
        section_signed: KeyedSig,
        /// Our section chain truncated from the triggering msg's dst section_key (or genesis key for full proof)
        proof_chain: SecuredLinkedList,
        /// Section members
        members: SectionPeers,
    },
    /// Probes the network by sending a message to a random dst triggering an AE flow.
    AntiEntropyProbe(XorName),
    /// Sent when a msg-consuming node is surpassing certain thresholds for
    /// cpu load. It tells msg-producing nodes to back off a bit, proportional
    /// to the node's cpu load, as given by the included `LoadReport`.
    BackPressure(LoadReport),
    /// Send from a section to the node to be immediately relocated.
    Relocate(SectionAuth<NodeState>),
    /// Sent from a bootstrapping peer to the section requesting to join as a new member
    JoinRequest(Box<JoinRequest>),
    /// Response to a `JoinRequest`
    JoinResponse(Box<JoinResponse>),
    /// Sent from a peer to the section requesting to join as relocated from another section
    JoinAsRelocatedRequest(Box<JoinAsRelocatedRequest>),
    /// Response to a `JoinAsRelocatedRequest`
    JoinAsRelocatedResponse(Box<JoinAsRelocatedResponse>),
    /// Sent to the new elder candidates to start the DKG process.
    DkgStart {
        /// The identifier of the DKG session to start.
        session_id: DkgSessionId,
        /// The section prefix. It matches all the members' names.
        prefix: Prefix,
        /// The section's complete set of elders as a map from their name to their socket address.
        elders: BTreeMap<XorName, SocketAddr>,
    },
    /// Message sent when a DKG session has not started
    DkgSessionUnknown {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// DKG message that came in
        message: DkgMessage,
    },
    /// DKG session info along with section authority
    DkgSessionInfo {
        /// The identifier of the DKG session to start.
        session_id: DkgSessionId,
        /// The section prefix. It matches all the members' names.
        prefix: Prefix,
        /// The section's complete set of elders as a map from their name to their socket address.
        elders: BTreeMap<XorName, SocketAddr>,
        /// Section authority for the DKG start message
        section_auth: AuthorityProof<SectionAuthProof>,
        /// Messages processed in the session so far
        message_cache: Vec<DkgMessage>,
        /// The original DKG message
        message: DkgMessage,
    },
    /// Message exchanged for DKG process.
    DkgMessage {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The DKG message.
        message: DkgMessage,
    },
    /// Message signalling that the node is not ready for the
    /// DKG message yet
    DkgNotReady {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The sent DKG message.
        message: DkgMessage,
    },
    /// Message containing a history of received DKG messages so other nodes can catch-up
    DkgRetry {
        /// History of messages received at the sender's end
        message_history: Vec<DkgMessage>,
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The originally sent DKG message.
        message: DkgMessage,
    },
    /// Broadcast to the other DKG participants when a DKG failure is observed.
    DkgFailureObservation {
        /// The DKG key
        session_id: DkgSessionId,
        /// Signature over the failure
        sig: DkgFailureSig,
        /// Nodes that failed to participate
        failed_participants: BTreeSet<XorName>,
    },
    /// Sent to the current elders by the DKG participants when at least majority of them observe
    /// a DKG failure.
    DkgFailureAgreement(DkgFailureSigSet),
    /// Message containing a single `Proposal` to be aggregated in the proposal aggregator.
    Propose {
        /// The content of the proposal
        proposal: Proposal,
        // TODO: try to remove this in favor of the msg header MsgKind sig share we already have
        /// BLS signature share
        sig_share: SigShare,
    },
    /// Message that notifies a section to test
    /// the connectivity to a node
    StartConnectivityTest(XorName),
    /// Cmds are orders to perform some operation, only sent internally in the network.
    NodeCmd(NodeCmd),
    /// Queries is a read-only operation.
    NodeQuery(NodeQuery),
    /// Events are facts about something that happened on a node.
    NodeEvent(NodeEvent),
    /// The response to a query, containing the query result.
    NodeQueryResponse {
        /// QueryResponse.
        response: NodeQueryResponse,
        /// ID of causing query.
        correlation_id: MsgId,
        /// TEMP: Add user here as part of return flow. Remove this as we have chunk routing etc
        user: EndUser,
    },
    /// The returned error, from any msg handling on recipient node.
    NodeMsgError {
        /// The error.
        // TODO: return node::Error instead
        error: crate::messaging::data::Error,
        /// ID of causing cmd.
        correlation_id: MsgId,
    },
}

/// Load report to be sent over the wire.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoadReport {
    /// CPU load short term (~1 min).
    pub short_term: CpuLoad,
    /// CPU load mid term (~5 min).
    pub mid_term: CpuLoad,
    /// CPU load long term (~15 min).
    pub long_term: CpuLoad,
}


impl LoadReport {
    pub fn is_good(&self) -> bool {
        self.mid_term.low && self.long_term.low && !self.short_term.critical
    }

    pub fn is_ok(&self) -> bool {
        !self.is_good() && !self.mid_term.high && !self.long_term.moderate
    }

    pub fn is_bad(&self) -> bool {
        self.short_term.critical
            || self.mid_term.very_high
            || self.mid_term.critical
            || self.long_term.high
            || self.long_term.very_high
            || self.long_term.critical
    }
}


/// An evaluation of measured cpu load during a period.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CpuLoad {
    /// This is considered to be well below sustainable levels.
    pub low: bool,
    /// This is considered to be OK.
    pub moderate: bool,
    /// This is not a sustainable level.
    pub high: bool,
    /// This is not a sustainable level.
    pub very_high: bool,
    /// This is not a sustainable level.
    pub critical: bool,
}
