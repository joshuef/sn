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
mod msg_authority;
mod node_msgs;
mod node_state;
mod op_id;
mod signed;
use bls::PublicKey as BlsPublicKey;

pub use agreement::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, Proposal, SectionAuth};
pub use join::{JoinRejectionReason, JoinRequest, JoinResponse, ResourceProof};
pub use join_as_relocated::{JoinAsRelocatedRequest, JoinAsRelocatedResponse};
pub use msg_authority::NodeMsgAuthorityUtils;
pub use node_msgs::{NodeCmd, NodeEvent, NodeQuery, NodeQueryResponse};
pub use node_state::{MembershipState, NodeState, RelocateDetails};
pub use op_id::OperationId;
pub use signed::{KeyedSig, SigShare};

use super::{authority::SectionAuth as SectionAuthProof, AuthorityProof};
use qp2p::UsrMsgBytes;

use crate::messaging::SectionAuthorityProvider;
use crate::network_knowledge::{SapCandidate, SectionsDAG};

use sn_consensus::{Generation, SignedVote};

use bls_dkg::key_gen::message::Message as DkgMessage;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use xor_name::XorName;

/// List of peers of a section
pub type SectionPeers = BTreeSet<SectionAuth<NodeState>>;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum AntiEntropyKind {
    /// This AE message is sent to a peer when a message with outdated section
    /// information was received, attaching the bounced message so
    /// the peer can resend it with up to date destination information.
    Retry { bounced_msg: UsrMsgBytes },
    /// This AE message is sent to a peer when a message needs to be sent to a
    /// different and/or closest section, attaching the bounced message so the peer
    /// can resend it to the correct section with up to date destination information.
    Redirect { bounced_msg: UsrMsgBytes },
    /// This AE message is sent to update a peer when we notice they are behind
    Update { members: SectionPeers },
}

#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
#[allow(clippy::large_enum_variant, clippy::derive_partial_eq_without_eq)]
/// Message sent over the among nodes
pub enum SystemMsg {
    AntiEntropy {
        /// Current `SectionAuthorityProvider` of our section.
        section_auth: SectionAuthorityProvider,
        /// Section signature over the `SectionAuthorityProvider` of our
        /// section the bounced message shall be resent to.
        section_signed: KeyedSig,
        /// Our section chain truncated from the triggering msg's dst section_key (or genesis key for full proof)
        partial_dag: SectionsDAG,
        /// The kind of anti-entropy response.
        kind: AntiEntropyKind,
    },
    /// Probes the network by sending a message to a random or chosen dst triggering an AE flow.
    /// Sends the current section key of target section which we know
    /// This expects a response, even if we're up to date.
    AntiEntropyProbe(BlsPublicKey),
    /// Send from a section to the node to be immediately relocated.
    Relocate(SectionAuth<NodeState>),
    /// Membership Votes, in order they should be processed in.
    MembershipVotes(Vec<SignedVote<NodeState>>),
    /// Membership Anti-Entropy request
    MembershipAE(Generation),
    /// Sent from a bootstrapping peer to the section requesting to join as a new member
    JoinRequest(JoinRequest),
    /// Response to a `JoinRequest`
    JoinResponse(Box<JoinResponse>),
    /// Sent from a peer to the section requesting to join as relocated from another section
    JoinAsRelocatedRequest(Box<JoinAsRelocatedRequest>),
    /// Response to a `JoinAsRelocatedRequest`
    JoinAsRelocatedResponse(Box<JoinAsRelocatedResponse>),
    /// Sent to the new elder candidates to start the DKG process.
    DkgStart(DkgSessionId),
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
    /// Section handover consensus vote message
    HandoverVotes(Vec<SignedVote<SapCandidate>>),
    /// Handover Anti-Entropy request
    HandoverAE(Generation),
    /// Message containing a single `Proposal` to be aggregated in the proposal aggregator.
    Propose {
        /// The content of the proposal
        proposal: Proposal,
        // TODO: try to remove this in favor of the msg header MsgKind sig share we already have
        /// BLS signature share
        sig_share: SigShare,
    },
    /// Events are facts about something that happened on a node.
    NodeEvent(NodeEvent),
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// Cmds are orders to perform some operation, only sent internally in the network.
    NodeCmd(NodeCmd),
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// Queries is a read-only operation.
    NodeQuery(NodeQuery),
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// The response to a query, containing the query result.
    NodeQueryResponse {
        /// QueryResponse.
        response: NodeQueryResponse,
        /// ID of the requested operation.
        operation_id: OperationId,
    },
}

impl SystemMsg {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        use super::msg_type::{
            ANTIENTROPY_MSG_PRIORITY, DKG_MSG_PRIORITY, JOIN_RELOCATE_MSG_PRIORITY,
            JOIN_RESPONSE_PRIORITY, MEMBERSHIP_PRIORITY, NODE_DATA_MSG_PRIORITY,
        };
        match self {
            // DKG messages
            Self::DkgStart { .. }
            | Self::DkgSessionUnknown { .. }
            | Self::DkgSessionInfo { .. }
            | Self::DkgNotReady { .. }
            | Self::DkgRetry { .. }
            | Self::DkgMessage { .. }
            | Self::DkgFailureObservation { .. }
            | Self::DkgFailureAgreement(_) => DKG_MSG_PRIORITY,

            // Inter-node comms for AE updates
            Self::AntiEntropy { .. } | Self::AntiEntropyProbe(_) => ANTIENTROPY_MSG_PRIORITY,

            // Join responses
            Self::JoinResponse(_) | Self::JoinAsRelocatedResponse(_) => JOIN_RESPONSE_PRIORITY,

            Self::Propose { .. }
            | Self::MembershipVotes(_)
            | Self::MembershipAE(_)
            | Self::HandoverAE(_)
            | Self::HandoverVotes(_) => MEMBERSHIP_PRIORITY,

            // Inter-node comms for joining, relocating etc.
            Self::Relocate(_) | Self::JoinRequest(_) | Self::JoinAsRelocatedRequest(_) => {
                JOIN_RELOCATE_MSG_PRIORITY
            }

            #[cfg(any(feature = "chunks", feature = "registers"))]
            // Inter-node comms related to processing client requests
            Self::NodeCmd(_)
            | Self::NodeEvent(NodeEvent::CouldNotStoreData { .. })
            | Self::NodeQuery(_)
            | Self::NodeQueryResponse { .. } => NODE_DATA_MSG_PRIORITY,
        }
    }

    pub fn statemap_states(&self) -> crate::statemap::State {
        use crate::statemap::State;
        match self {
            Self::AntiEntropy { .. } => State::AntiEntropy,
            Self::AntiEntropyProbe { .. } => State::AntiEntropy,
            Self::Relocate(_) => State::Relocate,
            Self::MembershipAE(_) => State::Membership,
            Self::MembershipVotes(_) => State::Membership,
            Self::JoinRequest(_) => State::Join,
            Self::JoinResponse(_) => State::Join,
            Self::JoinAsRelocatedRequest(_) => State::Join,
            Self::JoinAsRelocatedResponse(_) => State::Join,
            Self::DkgStart(_) => State::Dkg,
            Self::DkgSessionUnknown { .. } => State::Dkg,
            Self::DkgSessionInfo { .. } => State::Dkg,
            Self::DkgMessage { .. } => State::Dkg,
            Self::DkgNotReady { .. } => State::Dkg,
            Self::DkgRetry { .. } => State::Dkg,
            Self::DkgFailureObservation { .. } => State::Dkg,
            Self::DkgFailureAgreement(_) => State::Dkg,
            Self::HandoverVotes(_) => State::Handover,
            Self::HandoverAE(_) => State::Handover,
            Self::Propose { .. } => State::Propose,
            Self::NodeEvent(_) => State::Node,
            Self::NodeCmd(_) => State::Node,
            Self::NodeQuery(_) => State::Node,
            Self::NodeQueryResponse { .. } => State::Node,
        }
    }
}

impl Display for SystemMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AntiEntropy { .. } => write!(f, "SystemMsg::AntiEntropy"),
            Self::AntiEntropyProbe { .. } => write!(f, "SystemMsg::AntiEntropyProbe"),
            Self::Relocate { .. } => write!(f, "SystemMsg::Relocate"),
            Self::MembershipVotes { .. } => write!(f, "SystemMsg::MembershipVotes"),
            Self::MembershipAE { .. } => write!(f, "SystemMsg::MembershipAE"),
            Self::JoinRequest { .. } => write!(f, "SystemMsg::JoinRequest"),
            Self::JoinResponse { .. } => write!(f, "SystemMsg::JoinResponse"),
            Self::JoinAsRelocatedRequest { .. } => {
                write!(f, "SystemMsg::JoinAsRelocatedRequest")
            }
            Self::JoinAsRelocatedResponse { .. } => {
                write!(f, "SystemMsg::JoinAsRelocatedResponse")
            }
            Self::DkgStart { .. } => write!(f, "SystemMsg::DkgStart"),
            Self::DkgSessionUnknown { .. } => write!(f, "SystemMsg::DkgSessionUnknown"),
            Self::DkgSessionInfo { .. } => write!(f, "SystemMsg::DkgSessionInfo"),
            Self::DkgMessage { .. } => write!(f, "SystemMsg::DkgMessage"),
            Self::DkgNotReady { .. } => write!(f, "SystemMsg::DkgNotReady"),
            Self::DkgRetry { .. } => write!(f, "SystemMsg::DkgRetry"),
            Self::DkgFailureObservation { .. } => {
                write!(f, "SystemMsg::DkgFailureObservation")
            }
            Self::DkgFailureAgreement { .. } => write!(f, "SystemMsg::DkgFailureAgreement"),
            Self::HandoverVotes { .. } => write!(f, "SystemMsg::HandoverVotes"),
            Self::HandoverAE { .. } => write!(f, "SystemMsg::HandoverAE"),
            Self::Propose { .. } => write!(f, "SystemMsg::Propose"),
            Self::NodeEvent { .. } => write!(f, "SystemMsg::NodeEvent"),
            #[cfg(any(feature = "chunks", feature = "registers"))]
            Self::NodeCmd { .. } => write!(f, "SystemMsg::NodeCmd"),
            #[cfg(any(feature = "chunks", feature = "registers"))]
            Self::NodeQuery { .. } => write!(f, "SystemMsg::NodeQuery"),
            #[cfg(any(feature = "chunks", feature = "registers"))]
            Self::NodeQueryResponse { .. } => write!(f, "SystemMsg::NodeQueryResponse"),
        }
    }
}
