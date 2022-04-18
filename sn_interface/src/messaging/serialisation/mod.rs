// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod wire_msg;
mod wire_msg_header;

use xor_name::XorName;

// highest priority, since we must sort out membership first of all
pub(crate) const DKG_MSG_PRIORITY: i32 = 10;
// very high prio, since we must have correct contact details to the network
pub(crate) const ANTIENTROPY_MSG_PRIORITY: i32 = 8;
// high prio as recipient can't do anything until they've joined. Needs to be lower than DKG (or else no split)
pub(crate) const JOIN_RESPONSE_PRIORITY: i32 = 6;
// our joining to the network
pub(crate) const JOIN_RELOCATE_MSG_PRIORITY: i32 = 4;
// reporting dysfunction is somewhat critical, so not super low
pub(crate) const DYSFUNCTION_MSG_PRIORITY: i32 = 2;
#[cfg(feature = "back-pressure")]
// reporting backpressure isn't time critical, so fairly low
pub(crate) const BACKPRESSURE_MSG_PRIORITY: i32 = 0;
// not maintaining network structure, so can wait
pub(crate) const NODE_DATA_MSG_PRIORITY: i32 = -6;
#[cfg(feature = "service-msgs")]
// has payment throttle, but is not critical for network function
pub(crate) const SERVICE_CMD_PRIORITY: i32 = -8;
#[cfg(feature = "service-msgs")]
// has no throttle and is sent by clients, lowest prio
pub(crate) const SERVICE_QUERY_PRIORITY: i32 = -10;

use crate::types::PublicKey;

pub use self::wire_msg::WireMsg;
use super::{
    data::ServiceMsg,
    system::{NodeEvent, SystemMsg},
    AuthorityProof, BlsShareAuth, DstLocation, MsgId, NodeAuth, SectionAuth, ServiceAuth,
};

/// Type of message.
/// Note this is part of this crate's public API but this enum is
/// never serialised or even part of the message that is sent over the wire.
#[derive(PartialEq, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum MsgType {
    #[cfg(feature = "service-msgs")]
    /// Service message
    Service {
        /// Message ID
        msg_id: MsgId,
        /// Requester's authority over this message
        auth: AuthorityProof<ServiceAuth>,
        /// Message destination location
        dst_location: DstLocation,
        /// the message
        msg: ServiceMsg,
    },
    /// System message
    System {
        /// Message ID
        msg_id: MsgId,
        /// Node authority over this message
        msg_authority: NodeMsgAuthority,
        /// Message destination location
        dst_location: DstLocation,
        /// the message
        msg: SystemMsg,
    },
}

impl MsgType {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        match self {
            // DKG messages
            MsgType::System {
                msg:
                    SystemMsg::DkgStart { .. }
                    | SystemMsg::DkgSessionUnknown { .. }
                    | SystemMsg::DkgSessionInfo { .. }
                    | SystemMsg::DkgNotReady { .. }
                    | SystemMsg::DkgRetry { .. }
                    | SystemMsg::DkgMessage { .. }
                    | SystemMsg::DkgFailureObservation { .. }
                    | SystemMsg::DkgFailureAgreement(_),
                ..
            } => DKG_MSG_PRIORITY,

            // Inter-node comms for AE updates
            MsgType::System {
                msg:
                    SystemMsg::AntiEntropyRetry { .. }
                    | SystemMsg::AntiEntropyRedirect { .. }
                    | SystemMsg::AntiEntropyUpdate { .. }
                    | SystemMsg::AntiEntropyProbe(_),
                ..
            } => ANTIENTROPY_MSG_PRIORITY,

            // Join responses
            MsgType::System {
                msg: SystemMsg::JoinResponse(_) | SystemMsg::JoinAsRelocatedResponse(_),
                ..
            } => JOIN_RESPONSE_PRIORITY,

            // Inter-node comms for joining, relocating etc.
            MsgType::System {
                msg:
                    SystemMsg::Relocate(_)
                    | SystemMsg::JoinRequest(_)
                    | SystemMsg::JoinAsRelocatedRequest(_)
                    | SystemMsg::Propose { .. }
                    | SystemMsg::StartConnectivityTest(_)
                    | SystemMsg::MembershipVote(_)
                    | SystemMsg::MembershipAE(_),
                ..
            } => JOIN_RELOCATE_MSG_PRIORITY,

            // Inter-node comms for dysfunction detection
            MsgType::System {
                msg: SystemMsg::NodeEvent(NodeEvent::SuspiciousNodesDetected(_)),
                ..
            } => DYSFUNCTION_MSG_PRIORITY,

            #[cfg(feature = "back-pressure")]
            // Inter-node comms for backpressure
            MsgType::System {
                msg: SystemMsg::BackPressure(_),
                ..
            } => BACKPRESSURE_MSG_PRIORITY,

            // Inter-node comms related to processing client requests
            MsgType::System {
                msg:


                    SystemMsg::NodeMsgError { .. },
                ..
            } => NODE_DATA_MSG_PRIORITY,
            // Inter-node comms related to processing client requests
            #[cfg(feature = "service-msgs")]
            MsgType::System {
                msg:

                    SystemMsg::NodeCmd(_)
                    | SystemMsg::NodeEvent(NodeEvent::CouldNotStoreData { .. })
                    | SystemMsg::NodeQuery(_)
                    | SystemMsg::NodeQueryResponse { .. }
                    | SystemMsg::NodeMsgError { .. },
                ..
            } => NODE_DATA_MSG_PRIORITY,

            // Client <-> node service comms
            #[cfg(feature = "service-msgs")]
            MsgType::Service {
                msg: ServiceMsg::Cmd(_),
                ..
            } => SERVICE_CMD_PRIORITY,
            #[cfg(feature = "service-msgs")]
            MsgType::Service { .. } => SERVICE_QUERY_PRIORITY,
        }
    }
}

/// Authority of a NodeMsg.
/// Src of message and authority to send it. Authority is validated by the signature.
#[derive(PartialEq, Debug, Clone)]
pub enum NodeMsgAuthority {
    /// Authority of a single peer.
    Node(AuthorityProof<NodeAuth>),
    /// Authority of a single peer that uses it's BLS Keyshare to sign the message.
    BlsShare(AuthorityProof<BlsShareAuth>),
    /// Authority of a whole section.
    Section(AuthorityProof<SectionAuth>),
}

impl NodeMsgAuthority {
    /// Returns the XorName of the authority used for the auth signing
    pub fn get_auth_xorname(&self) -> XorName {
        match self.clone() {
            NodeMsgAuthority::BlsShare(auth_proof) => {
                let auth = auth_proof.into_inner();
                auth.src_name
            }
            NodeMsgAuthority::Node(auth_proof) => {
                let auth = auth_proof.into_inner();
                let pk = auth.node_ed_pk;

                XorName::from(PublicKey::from(pk))
            }
            NodeMsgAuthority::Section(auth_proof) => {
                let auth = auth_proof.into_inner();
                auth.src_name
            }
        }
    }
}
