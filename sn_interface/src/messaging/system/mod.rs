// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod dkg;
mod join;
mod join_as_relocated;
mod node_msgs;
mod op_id;
mod proposal;
mod section_sig;

use super::{data::CmdResponse, MsgId};

use crate::messaging::AuthorityProof;
use crate::network_knowledge::{NodeState, SapCandidate, SectionTreeUpdate};

pub use dkg::DkgSessionId;
pub use join::{JoinRejectionReason, JoinRequest, JoinResponse};
pub use join_as_relocated::{JoinAsRelocatedRequest, JoinAsRelocatedResponse};
pub use node_msgs::{NodeDataCmd, NodeDataQuery, NodeEvent, NodeQueryResponse};
pub use op_id::OperationId;
pub use proposal::Proposal;
pub use section_sig::{SectionSig, SectionSigShare, SectionSigned};

use bls::PublicKey as BlsPublicKey;
use ed25519::Signature;
use qp2p::UsrMsgBytes;
use serde::{Deserialize, Serialize};
use sn_consensus::{Generation, SignedVote};
use sn_sdkg::DkgSignedVote;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use xor_name::XorName;

/// List of peers of a section
pub type SectionPeers = BTreeSet<SectionSigned<NodeState>>;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, custom_debug::Debug)]
pub enum AntiEntropyKind {
    /// This AE message is sent to a peer when a message with outdated section
    /// information was received, attaching the bounced message so
    /// the peer can resend it with up to date destination information.
    Retry {
        #[debug(skip)]
        bounced_msg: UsrMsgBytes,
    },
    /// This AE message is sent to a peer when a message needs to be sent to a
    /// different and/or closest section, attaching the bounced message so the peer
    /// can resend it to the correct section with up to date destination information.
    Redirect {
        #[debug(skip)]
        bounced_msg: UsrMsgBytes,
    },
    /// This AE message is sent to update a peer when we notice they are behind
    Update { members: SectionPeers },
}

#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
#[allow(clippy::large_enum_variant, clippy::derive_partial_eq_without_eq)]
/// Message sent over the among nodes
pub enum NodeMsg {
    AntiEntropy {
        /// The update to our NetworkKnowledge containing the current `SectionAuthorityProvider`
        /// and the section chain truncated from the triggering msg's dst section_key or genesis_key
        /// if the the dst section_key is not a direct ancestor to our section_key
        section_tree_update: SectionTreeUpdate,
        /// The kind of anti-entropy response.
        kind: AntiEntropyKind,
    },
    /// Probes the network by sending a message to a random or chosen dst triggering an AE flow.
    /// Sends the current section key of target section which we know
    /// This expects a response, even if we're up to date.
    AntiEntropyProbe(BlsPublicKey),
    /// Send from a section to the node to be immediately relocated.
    Relocate(SectionSigned<NodeState>),
    /// Membership Votes, in order they should be processed in.
    MembershipVotes(Vec<SignedVote<NodeState>>),
    /// Membership Anti-Entropy request
    MembershipAE(Generation),
    /// Sent from a bootstrapping peer to the section requesting to join as a new member
    JoinRequest(JoinRequest),
    /// Response to a `JoinRequest`
    JoinResponse(JoinResponse),
    /// Sent from a peer to the section requesting to join as relocated from another section
    JoinAsRelocatedRequest(Box<JoinAsRelocatedRequest>),
    /// Response to a `JoinAsRelocatedRequest`
    JoinAsRelocatedResponse(Box<JoinAsRelocatedResponse>),
    /// Sent to the new elder candidates to start the DKG process, along with a sig of the DkgSessionId
    DkgStart(DkgSessionId, SectionSigShare),
    /// Sent when DKG is triggered to other participant
    DkgEphemeralPubKey {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// Section authority for the DkgSessionId (if you missed the DkgStart you can trust this)
        section_auth: AuthorityProof<SectionSig>,
        /// The ephemeral bls key chosen by candidate
        pub_key: BlsPublicKey,
        /// The ed25519 signature of the candidate
        sig: Signature,
    },
    /// Votes exchanged for DKG process.
    DkgVotes {
        /// The identifier of the DKG session this message is for.
        session_id: DkgSessionId,
        /// The ephemeral bls public keys used for this Dkg round
        pub_keys: BTreeMap<XorName, (BlsPublicKey, Signature)>,
        /// The DKG message.
        votes: Vec<DkgSignedVote>,
    },
    /// Dkg Anti-Entropy request when receiving votes that are ahead of our knowledge
    DkgAE(DkgSessionId),
    /// Section handover consensus vote message
    HandoverVotes(Vec<SignedVote<SapCandidate>>),
    /// Handover Anti-Entropy request
    HandoverAE(Generation),
    /// Message containing a single `Proposal` to be aggregated in the proposal aggregator.
    Propose {
        /// The content of the proposal
        proposal: Proposal,
        /// BLS signature share of an Elder
        sig_share: SectionSigShare,
        /// BLS signature share of an Elder if Proposal::NewSectionsAgreement
        optional_sig_share: Option<SectionSigShare>,
    },
    /// Node events are Node to Elder events about something that happened on a Node.
    NodeEvent(NodeEvent),
    /// Data cmds are orders to perform some data operation, only sent internally in the network.
    NodeDataCmd(NodeDataCmd),
    /// Data queries is a read-only operation.
    NodeDataQuery(NodeDataQuery),
}

/// Messages sent from adults to the elders in response to client queries or commands
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeDataResponse {
    /// The response to a query, containing the query result.
    QueryResponse {
        /// The result of the query.
        response: NodeQueryResponse,
        /// ID of the requested operation.
        operation_id: OperationId,
    },
    /// The response will be sent back to the client when the handling on the
    /// receiving Elder has been finished.
    CmdResponse {
        /// The result of the command.
        response: CmdResponse,
        /// ID of causing ClientMsg::Cmd.
        correlation_id: MsgId,
    },
}

impl NodeDataResponse {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        super::msg_type::NODE_DATA_MSG_PRIORITY
    }
}

impl Display for NodeDataResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryResponse { response, .. } => {
                write!(f, "NodeDataResponse::QueryResponse({response:?})")
            }
            Self::CmdResponse { response, .. } => {
                write!(f, "NodeDataResponse::CmdResponse({response:?})")
            }
        }
    }
}

impl NodeMsg {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        use super::msg_type::{
            ANTIENTROPY_MSG_PRIORITY, DATA_REPLICATION_MSG_PRIORITY, DKG_MSG_PRIORITY,
            JOIN_RELOCATE_MSG_PRIORITY, JOIN_RESPONSE_PRIORITY, MEMBERSHIP_PRIORITY,
            NODE_DATA_MSG_PRIORITY,
        };
        match self {
            // DKG messages
            Self::DkgStart { .. }
            | Self::DkgEphemeralPubKey { .. }
            | Self::DkgVotes { .. }
            | Self::DkgAE { .. } => DKG_MSG_PRIORITY,

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

            Self::NodeEvent(_) => DATA_REPLICATION_MSG_PRIORITY,

            // Inter-node comms related to processing client data requests
            Self::NodeDataCmd(_) | Self::NodeDataQuery(_) => NODE_DATA_MSG_PRIORITY,
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
            Self::DkgStart { .. } => State::Dkg,
            Self::DkgEphemeralPubKey { .. } => State::Dkg,
            Self::DkgVotes { .. } => State::Dkg,
            Self::DkgAE { .. } => State::Dkg,
            Self::HandoverVotes(_) => State::Handover,
            Self::HandoverAE(_) => State::Handover,
            Self::Propose { .. } => State::Propose,
            Self::NodeEvent(_) => State::Node,
            Self::NodeDataCmd(_) => State::Node,
            Self::NodeDataQuery(_) => State::Node,
        }
    }
}

impl Display for NodeMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AntiEntropy { .. } => write!(f, "NodeMsg::AntiEntropy"),
            Self::AntiEntropyProbe { .. } => write!(f, "NodeMsg::AntiEntropyProbe"),
            Self::Relocate { .. } => write!(f, "NodeMsg::Relocate"),
            Self::MembershipVotes { .. } => write!(f, "NodeMsg::MembershipVotes"),
            Self::MembershipAE { .. } => write!(f, "NodeMsg::MembershipAE"),
            Self::JoinRequest { .. } => write!(f, "NodeMsg::JoinRequest"),
            Self::JoinResponse { .. } => write!(f, "NodeMsg::JoinResponse"),
            Self::JoinAsRelocatedRequest { .. } => {
                write!(f, "NodeMsg::JoinAsRelocatedRequest")
            }
            Self::JoinAsRelocatedResponse { .. } => {
                write!(f, "NodeMsg::JoinAsRelocatedResponse")
            }
            Self::DkgStart { .. } => write!(f, "NodeMsg::DkgStart"),
            Self::DkgEphemeralPubKey { .. } => write!(f, "NodeMsg::DkgEphemeralPubKey"),
            Self::DkgVotes { .. } => write!(f, "NodeMsg::DkgVotes"),
            Self::DkgAE { .. } => write!(f, "NodeMsg::DkgAE"),
            Self::HandoverVotes { .. } => write!(f, "NodeMsg::HandoverVotes"),
            Self::HandoverAE { .. } => write!(f, "NodeMsg::HandoverAE"),
            Self::Propose { .. } => write!(f, "NodeMsg::Propose"),
            Self::NodeEvent { .. } => write!(f, "NodeMsg::NodeEvent"),
            Self::NodeDataCmd { .. } => write!(f, "NodeMsg::NodeCmd"),
            Self::NodeDataQuery { .. } => write!(f, "NodeMsg::NodeQuery"),
        }
    }
}
