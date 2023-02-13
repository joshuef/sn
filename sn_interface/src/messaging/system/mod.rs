// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod dkg;
mod join;
mod node_msgs;
mod section_sig;

use crate::messaging::AuthorityProof;
use crate::network_knowledge::{
    NodeState, RelocationProof, RelocationTrigger, SapCandidate, SectionTreeUpdate,
};
use crate::SectionAuthorityProvider;

pub use dkg::DkgSessionId;
pub use join::{JoinRejectReason, JoinRequest, JoinResponse};
pub use node_msgs::{NodeDataCmd, NodeEvent, NodeQueryResponse};
pub use section_sig::{SectionSig, SectionSigShare, SectionSigned};

use bls::PublicKey as BlsPublicKey;
use ed25519::Signature;
use qp2p::UsrMsgBytes;
use serde::{Deserialize, Serialize};
use sn_consensus::{Generation, SignedVote};
use sn_sdkg::DkgSignedVote;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
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

/// A vote about the state of the section
/// This can be a result of seeing a node go offline or deciding wether we want to accept new nodes
/// Anything where we need section authority before action can be taken
/// Section State Proposals are sent by elders to elders
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SectionStateVote {
    /// Vote to remove a node from our section
    NodeIsOffline(NodeState),
    /// Vote to change whether new nodes are allowed to join our section
    JoinsAllowed(bool),
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
    /// Sent to the relocating node to begin the relocation process. The `RelocationTrigger` is used
    /// by the relocating nodes to request the Section for the relocation membership change.
    BeginRelocating(RelocationTrigger),
    /// Sent by the relocating node to the Section to start the relocation membership change process.
    RelocationRequest {
        relocation_node: XorName,
        relocation_trigger: RelocationTrigger,
    },
    /// Sent from a section to the node to be immediately relocated.
    Relocate(SectionSigned<NodeState>),
    /// Membership Votes, in order they should be processed in.
    MembershipVotes(Vec<SignedVote<NodeState>>),
    /// Membership Anti-Entropy request
    MembershipAE(Generation),
    /// Try to join a section in the network.
    TryJoin(Option<RelocationProof>),
    /// Response to a join request.
    JoinResponse(JoinResponse),
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
    /// After DKG, elder candidates request handover with this to the current elders
    /// by submitting their new SAP along with their sig share
    /// The elders can then aggregate it and confirm the SAP is valid before accepting it for Handover
    RequestHandover {
        /// SAP generated by the candidate's finished DKG session
        sap: SectionAuthorityProvider,
        /// BLS sig share of the candidate over the SAP
        sig_share: SectionSigShare,
    },
    /// Section handover consensus vote message
    HandoverVotes(Vec<SignedVote<SapCandidate>>),
    /// Handover Anti-Entropy request
    HandoverAE(Generation),
    /// After Handover consensus, the elders inform the new elder candidates that they are promoted
    /// The candidates can aggregate the sig_share to obtain SectionSigned proof that they are promoted
    SectionHandoverPromotion {
        /// The promoted SAP (signed by themselves)
        sap: SectionSigned<SectionAuthorityProvider>,
        /// BLS signature share of an Elder over the sap's pubkey
        sig_share: SectionSigShare,
    },
    /// After Handover consensus, the elders inform the new elder candidates that they are promoted
    /// The candidates can aggregate the sig_shares to obtain SectionSigned proof that they are promoted
    SectionSplitPromotion {
        /// The promoted SAP of section 0 (signed by themselves)
        sap0: SectionSigned<SectionAuthorityProvider>,
        /// BLS signature share of an Elder over the sap0's pubkey
        sig_share0: SectionSigShare,
        /// The promoted SAP of section 1 (signed by themselves)
        sap1: SectionSigned<SectionAuthorityProvider>,
        /// BLS signature share of an Elder over the sap1's pubkey
        sig_share1: SectionSigShare,
    },
    /// Elders votes among themselves regarding SectionStates of joins_allowed and voting nodes off
    /// Once fully aggregated in the SectionStateVote aggregator, the proposal is accepted
    ProposeSectionState {
        proposal: SectionStateVote,
        /// BLS signature share of an Elder over the vote
        sig_share: SectionSigShare,
    },
    /// Node events are Node to Elder events about something that happened on a Node.
    NodeEvent(NodeEvent),
    /// Data cmds are orders to perform some data operation, only sent internally in the network.
    NodeDataCmd(NodeDataCmd),
}

impl NodeMsg {
    pub fn is_join(&self) -> bool {
        // we could also differentiate, say if it's a relocation
        matches!(self, NodeMsg::TryJoin(_))
    }
    pub fn is_ae(&self) -> bool {
        matches!(self, NodeMsg::AntiEntropy { .. })
    }
}

impl NodeMsg {
    pub fn statemap_states(&self) -> crate::statemap::State {
        use crate::statemap::State;
        match self {
            Self::AntiEntropy { .. } => State::AntiEntropy,
            Self::AntiEntropyProbe { .. } => State::AntiEntropy,
            Self::Relocate(_) => State::Relocate,
            Self::BeginRelocating(_) => State::Relocate,
            Self::RelocationRequest { .. } => State::Relocate,
            Self::MembershipAE(_) => State::Membership,
            Self::MembershipVotes(_) => State::Membership,
            Self::TryJoin(_) => State::Join,
            Self::JoinResponse(_) => State::Join,
            Self::DkgStart { .. } => State::Dkg,
            Self::DkgEphemeralPubKey { .. } => State::Dkg,
            Self::DkgVotes { .. } => State::Dkg,
            Self::DkgAE { .. } => State::Dkg,
            Self::RequestHandover { .. } => State::Dkg,
            Self::HandoverVotes(_) => State::Handover,
            Self::HandoverAE(_) => State::Handover,
            Self::SectionHandoverPromotion { .. } => State::Handover,
            Self::SectionSplitPromotion { .. } => State::Handover,
            Self::ProposeSectionState { .. } => State::Propose,
            Self::NodeEvent(_) => State::Node,
            Self::NodeDataCmd(_) => State::Node,
        }
    }
}

impl Display for NodeMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AntiEntropy { .. } => write!(f, "NodeMsg::AntiEntropy"),
            Self::AntiEntropyProbe { .. } => write!(f, "NodeMsg::AntiEntropyProbe"),
            Self::Relocate { .. } => write!(f, "NodeMsg::Relocate"),
            Self::BeginRelocating { .. } => write!(f, "NodeMsg::BeginRelocating"),
            Self::RelocationRequest { .. } => write!(f, "NodeMsg::RequestRelocation"),
            Self::MembershipVotes { .. } => write!(f, "NodeMsg::MembershipVotes"),
            Self::MembershipAE { .. } => write!(f, "NodeMsg::MembershipAE"),
            Self::TryJoin(_) => write!(f, "NodeMsg::TryJoin"),
            Self::JoinResponse { .. } => write!(f, "NodeMsg::JoinResponse"),
            Self::DkgStart { .. } => write!(f, "NodeMsg::DkgStart"),
            Self::DkgEphemeralPubKey { .. } => write!(f, "NodeMsg::DkgEphemeralPubKey"),
            Self::DkgVotes { .. } => write!(f, "NodeMsg::DkgVotes"),
            Self::DkgAE { .. } => write!(f, "NodeMsg::DkgAE"),
            Self::RequestHandover { .. } => write!(f, "NodeMsg::RequestHandover"),
            Self::HandoverVotes { .. } => write!(f, "NodeMsg::HandoverVotes"),
            Self::HandoverAE { .. } => write!(f, "NodeMsg::HandoverAE"),
            Self::SectionHandoverPromotion { .. } => write!(f, "NodeMsg::SectionHandoverPromotion"),
            Self::SectionSplitPromotion { .. } => write!(f, "NodeMsg::SectionSplitPromotion"),
            Self::ProposeSectionState { .. } => write!(f, "NodeMsg::ProposeSectionState"),
            Self::NodeEvent { .. } => write!(f, "NodeMsg::NodeEvent"),
            Self::NodeDataCmd { .. } => write!(f, "NodeMsg::NodeCmd"),
        }
    }
}
