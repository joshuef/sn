// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::Dst;

use super::{
    data::ServiceMsg, system::SystemMsg, AuthorityProof, MsgId, NodeMsgAuthority, ServiceAuth,
};
use std::fmt::{Display, Formatter};

// highest priority, since we must sort out membership first of all
pub(crate) const DKG_MSG_PRIORITY: i32 = 8;
// very high prio, since we must have correct contact details to the network
pub(crate) const ANTIENTROPY_MSG_PRIORITY: i32 = 6;
// high prio as recipient can't do anything until they've joined. Needs to be lower than DKG (or else no split)
pub(crate) const JOIN_RESPONSE_PRIORITY: i32 = 4;
// Membership changes
pub(crate) const MEMBERSHIP_PRIORITY: i32 = 4;
// our joining to the network
pub(crate) const JOIN_RELOCATE_MSG_PRIORITY: i32 = 2;
#[cfg(feature = "back-pressure")]
// reporting backpressure isn't time critical, so fairly low
pub(crate) const BACKPRESSURE_MSG_PRIORITY: i32 = 0;
// not maintaining network structure, so can wait
pub(crate) const NODE_DATA_MSG_PRIORITY: i32 = -6;
#[cfg(any(feature = "chunks", feature = "registers"))]
// has payment throttle, but is not critical for network function
pub(crate) const SERVICE_CMD_PRIORITY: i32 = -8;
#[cfg(any(feature = "chunks", feature = "registers"))]
// has no throttle and is sent by clients, lowest prio
pub(crate) const SERVICE_QUERY_PRIORITY: i32 = -10;

/// Type of message.
/// Note this is part of this crate's public API but this enum is
/// never serialised or even part of the message that is sent over the wire.
#[derive(PartialEq, Eq, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum MsgType {
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// Service message for client<->node comms.
    Service {
        /// Message ID
        msg_id: MsgId,
        /// Requester's authority over this message
        auth: AuthorityProof<ServiceAuth>,
        /// Message dst
        dst: Dst,
        /// the message
        msg: ServiceMsg,
    },
    /// System message for node<->node comms.
    System {
        /// Message ID
        msg_id: MsgId,
        /// Node authority over this message
        msg_authority: NodeMsgAuthority,
        /// Message dst
        dst: Dst,
        /// the message
        msg: SystemMsg,
    },
}

impl MsgType {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        match self {
            // node <-> node system comms
            MsgType::System { msg, .. } => msg.priority(),
            // client <-> node service comms
            #[cfg(any(feature = "chunks", feature = "registers"))]
            MsgType::Service { msg, .. } => msg.priority(),
        }
    }
}

impl Display for MsgType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MsgType::System { msg, .. } => write!(f, "MsgType::System({})", msg),
            MsgType::Service { msg, .. } => write!(f, "MsgType::Service({})", msg),
        }
    }
}
