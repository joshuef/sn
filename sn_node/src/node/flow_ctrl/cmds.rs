// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, messaging::Peers, SectionStateVote, XorName};

use qp2p::SendStream;
use sn_consensus::Decision;
use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{
        data::{ClientDataResponse, ClientMsg},
        system::{NodeDataResponse, NodeMsg, SectionSig, SectionSigned},
        AuthorityProof, ClientAuth, MsgId, WireMsg,
    },
    network_knowledge::{NodeState, SectionAuthorityProvider, SectionKeyShare, SectionsDAG},
    types::{DataAddress, Peer},
};

use custom_debug::Debug;
use std::{collections::BTreeSet, fmt, time::SystemTime};

/// A struct for the job of controlling the flow
/// of a [`Cmd`] in the system.
///
/// An id is assigned to it, its parent id (if any),
/// a priority by which it is ordered in the queue
/// among other pending cmd jobs, and the time the
/// job was instantiated.
#[derive(Debug)]
pub(crate) struct CmdJob {
    id: usize,
    parent_id: Option<usize>,
    cmd: Cmd,
    created_at: SystemTime,
}

/// Commands for a node.
///
/// Cmds are used to connect different modules, allowing
/// for a better modularization of the code base.
/// Modelling a call like this also allows for throttling
/// and prioritization, which is not something e.g. tokio tasks allow.
/// In other words, it enables enhanced flow control.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum Cmd {
    TryJoinNetwork,
    /// Validate `wire_msg` from `sender`.
    /// Holding the WireMsg that has been received from the network,
    HandleMsg {
        origin: Peer,
        wire_msg: WireMsg,
        send_stream: Option<SendStream>,
    },
    /// Allows joining of new nodes.
    SetJoinsAllowed(bool),
    /// Allows joining of new nodes until the section splits.
    SetJoinsAllowedUntilSplit(bool),
    /// Add an issue to the tracking of a node's faults
    TrackNodeIssue {
        name: XorName,
        issue: IssueType,
    },
    UpdateNetworkAndHandleValidClientMsg {
        proof_chain: SectionsDAG,
        signed_sap: SectionSigned<SectionAuthorityProvider>,
        msg_id: MsgId,
        msg: ClientMsg,
        origin: Peer,
        send_stream: SendStream,
        /// Requester's authority over this message
        auth: AuthorityProof<ClientAuth>,
    },
    /// Handle peer that's been detected as lost.
    HandleFailedSendToNode {
        peer: Peer,
        msg_id: MsgId,
    },
    /// Handle agreement on a proposal.
    HandleSectionDecisionAgreement {
        proposal: SectionStateVote,
        sig: SectionSig,
    },
    /// Handle a membership decision.
    HandleMembershipDecision(Decision<NodeState>),
    /// Handle agree on elders. This blocks node message processing until complete.
    HandleNewEldersAgreement {
        new_elders: SectionSigned<SectionAuthorityProvider>,
        sig: SectionSig,
    },
    /// Handle agree on new sections. This blocks node message processing until complete.
    HandleNewSectionsAgreement {
        sap1: SectionSigned<SectionAuthorityProvider>,
        sig1: SectionSig,
        sap2: SectionSigned<SectionAuthorityProvider>,
        sig2: SectionSig,
    },
    /// Handle the outcome of a DKG session where we are one of the participants (that is, one of
    /// the proposed new elders).
    HandleDkgOutcome {
        section_auth: SectionAuthorityProvider,
        outcome: SectionKeyShare,
    },
    /// Send the batch of data messages in a throttled/controlled fashion to the given `recipients`.
    /// chunks addresses are provided, so that we only retrieve the data right before we send it,
    /// hopefully reducing memory impact or data replication
    EnqueueDataForReplication {
        // throttle_duration: Duration,
        recipient: Peer,
        /// Batches of DataAddress to be sent together
        data_batch: Vec<DataAddress>,
    },
    /// Performs serialisation and signing and sends the msg.
    SendMsg {
        msg: NodeMsg,
        msg_id: MsgId,
        recipients: Peers,
        #[debug(skip)]
        context: NodeContext,
    },
    /// Performs serialisation and signing and sends the msg over a bidi connection
    /// and then enqueues any response returned
    SendMsgEnqueueAnyResponse {
        msg: NodeMsg,
        msg_id: MsgId,
        recipients: BTreeSet<Peer>,
        #[debug(skip)]
        context: NodeContext,
    },
    /// Performs serialisation and sends the response NodeMsg to the peer over the given stream.
    SendNodeMsgResponse {
        msg: NodeMsg,
        msg_id: MsgId,
        recipient: Peer,
        send_stream: SendStream,
        #[debug(skip)]
        context: NodeContext,
    },
    /// Performs serialisation and sends the msg to the client over the given stream.
    SendClientResponse {
        msg: ClientDataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        #[debug(skip)]
        context: NodeContext,
        source_client: Peer,
    },
    /// Performs serialisation and sends the response NodeDataResponse msg to the peer
    /// over the given stream.
    SendNodeDataResponse {
        msg: NodeDataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        #[debug(skip)]
        context: NodeContext,
        requesting_peer: Peer,
    },
    /// Performs serialisation and sends the msg to the peer node over a new bi-stream,
    /// awaiting for a response which is forwarded to the client.
    SendMsgAwaitResponseAndRespondToClient {
        msg_id: MsgId,
        msg: NodeMsg,
        #[debug(skip)]
        context: NodeContext,
        targets: BTreeSet<Peer>,
        client_stream: SendStream,
        source_client: Peer,
    },
    /// Performs serialisation and signing and sends the msg after reading NodeContext
    /// from the node
    ///
    /// Currently only used during Join process where Node is not readily available
    /// DO NOT USE ELSEWHERE
    SendLockingJoinMsg {
        msg: NodeMsg,
        msg_id: MsgId,
        recipients: Peers,
    },
    /// Proposes peers as offline
    ProposeVoteNodesOffline(BTreeSet<XorName>),
}

impl Cmd {
    pub(crate) fn send_msg(msg: NodeMsg, recipients: Peers, context: NodeContext) -> Self {
        let msg_id = MsgId::new();
        debug!("Sending msg {msg_id:?} to {recipients:?}: {msg:?}");
        Cmd::SendMsg {
            msg,
            msg_id,
            recipients,
            context,
        }
    }

    /// Special wrapper to trigger SendLockingJoinMsg as NodeContext is unavailable
    /// during the join process
    pub(crate) fn send_join_msg(msg: NodeMsg, recipients: Peers) -> Self {
        let msg_id = MsgId::new();
        debug!("Sending locking join msg {msg_id:?} {msg:?}");
        Cmd::SendLockingJoinMsg {
            msg,
            msg_id,
            recipients,
        }
    }

    pub(crate) fn statemap_state(&self) -> sn_interface::statemap::State {
        use sn_interface::statemap::State;
        match self {
            Cmd::SendMsg { .. }
            | Cmd::SendMsgEnqueueAnyResponse { .. }
            | Cmd::SendNodeMsgResponse { .. }
            | Cmd::SendClientResponse { .. }
            | Cmd::SendNodeDataResponse { .. }
            | Cmd::SendMsgAwaitResponseAndRespondToClient { .. } => State::Comms,
            Cmd::SendLockingJoinMsg { .. } => State::Comms,
            Cmd::HandleFailedSendToNode { .. } => State::Comms,
            Cmd::HandleMsg { .. } => State::HandleMsg,
            Cmd::UpdateNetworkAndHandleValidClientMsg { .. } => State::ClientMsg,
            Cmd::TrackNodeIssue { .. } => State::FaultDetection,
            Cmd::HandleSectionDecisionAgreement { .. } => State::Agreement,
            Cmd::HandleMembershipDecision(_) => State::Membership,
            Cmd::ProposeVoteNodesOffline(_) => State::Membership,
            Cmd::HandleNewEldersAgreement { .. } => State::Handover,
            Cmd::HandleNewSectionsAgreement { .. } => State::Handover,
            Cmd::HandleDkgOutcome { .. } => State::Dkg,
            Cmd::EnqueueDataForReplication { .. } => State::Replication,
            Cmd::SetJoinsAllowed { .. } => State::Data,
            Cmd::SetJoinsAllowedUntilSplit { .. } => State::Data,
            Cmd::TryJoinNetwork => State::Join,
        }
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::HandleMsg { wire_msg, .. } => {
                write!(f, "HandleMsg {:?}", wire_msg.msg_id())
            }
            Cmd::UpdateNetworkAndHandleValidClientMsg { msg_id, msg, .. } => {
                write!(f, "UpdateAndHandleValidClientMsg {:?}: {:?}", msg_id, msg)
            }
            Cmd::HandleFailedSendToNode { peer, msg_id } => {
                write!(f, "HandlePeerFailedSend({:?}, {:?})", peer.name(), msg_id)
            }
            Cmd::HandleSectionDecisionAgreement { .. } => {
                write!(f, "HandleSectionDecisionAgreement")
            }
            Cmd::HandleNewEldersAgreement { .. } => write!(f, "HandleNewEldersAgreement"),
            Cmd::HandleNewSectionsAgreement { .. } => write!(f, "HandleNewSectionsAgreement"),
            Cmd::HandleMembershipDecision(_) => write!(f, "HandleMembershipDecision"),
            Cmd::HandleDkgOutcome { .. } => write!(f, "HandleDkgOutcome"),
            Cmd::SendMsg { .. } => write!(f, "SendMsg"),
            Cmd::SendMsgEnqueueAnyResponse { .. } => write!(f, "SendMsgEnqueueAnyResponse"),
            Cmd::SendNodeMsgResponse { .. } => write!(f, "SendNodeMsgResponse"),
            Cmd::SendClientResponse { .. } => write!(f, "SendClientResponse"),
            Cmd::SendNodeDataResponse { .. } => write!(f, "SendNodeDataResponse"),
            Cmd::SendMsgAwaitResponseAndRespondToClient { .. } => {
                write!(f, "SendMsgAwaitResponseAndRespondToClient")
            }
            Cmd::SendLockingJoinMsg { .. } => write!(f, "SendLockingJoinMsg"),
            Cmd::EnqueueDataForReplication { .. } => write!(f, "EnqueueDataForReplication"),
            Cmd::TrackNodeIssue { name, issue } => {
                write!(f, "TrackNodeIssue {:?}, {:?}", name, issue)
            }
            Cmd::ProposeVoteNodesOffline(_) => write!(f, "ProposeOffline"),
            Cmd::SetJoinsAllowed { .. } => write!(f, "SetJoinsAllowed"),
            Cmd::SetJoinsAllowedUntilSplit { .. } => write!(f, "SetJoinsAllowedUntilSplit"),
            Cmd::TryJoinNetwork => write!(f, "TryJoinNetwork"),
        }
    }
}
