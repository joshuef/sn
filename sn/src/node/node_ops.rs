// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    data::{ServiceError, ServiceMsg},
    system::SystemMsg,
    DstLocation, MessageId,
};
use crate::routing::Prefix;
use crate::types::PublicKey;
use std::collections::BTreeSet;
use xor_name::XorName;

/// Internal messages are what is passed along
/// within a node, between the entry point and
/// exit point of remote messages.
/// In other words, when communication from another
/// participant at the network arrives, it is mapped
/// to an internal message, that can
/// then be passed along to its proper processing module
/// at the node. At a node module, the result of such a call
/// is also an internal message.
/// Finally, an internal message might be destined for messaging
/// module, by which it leaves the process boundary of this node
/// and is sent on the wire to some other dst(s) on the network.

/// Vec of NodeDuty
pub(super) type NodeDuties = Vec<NodeDuty>;

/// Common duties run by all nodes.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum NodeDuty {
    Genesis,
    EldersChanged {
        /// Our section prefix.
        our_prefix: Prefix,
        /// The new Elders.
        new_elders: BTreeSet<XorName>,
        /// Oldie or newbie?
        newbie: bool,
    },
    AdultsChanged {
        /// Remaining Adults in our section.
        remaining: BTreeSet<XorName>,
        /// New Adults in our section.
        added: BTreeSet<XorName>,
        /// Removed Adults in our section.
        removed: BTreeSet<XorName>,
    },
    SectionSplit {
        /// Our section prefix.
        our_prefix: Prefix,
        /// our section public key
        our_key: PublicKey,
        /// oldie or newbie?
        newbie: bool,
    },
    /// When demoted, node levels down
    LevelDown,
    /// Sets joining allowed to true or false.
    SetNodeJoinsAllowed(bool),
    /// Send a message to the specified dst.
    Send(OutgoingMsg),
    /// Send a lazy error as a result of a specific message.
    /// The aim here is for the sender to respond with any missing state
    SendError(OutgoingLazyError),
    /// Send the same request to each individual node.
    SendToNodes {
        msg_id: MessageId,
        msg: SystemMsg,
        targets: BTreeSet<XorName>,
        aggregation: bool,
    },
    NoOp,
}

impl From<NodeDuty> for NodeDuties {
    fn from(duty: NodeDuty) -> Self {
        if matches!(duty, NodeDuty::NoOp) {
            vec![]
        } else {
            vec![duty]
        }
    }
}

// --------------- Messaging ---------------

#[derive(Debug, Clone)]
pub struct OutgoingMsg {
    pub msg: MsgType,
    pub dst: DstLocation,
    pub aggregation: bool,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum MsgType {
    Node(SystemMsg),
    Client(ServiceMsg),
}

#[derive(Debug, Clone)]
pub struct OutgoingLazyError {
    pub msg: ServiceError,
    pub dst: DstLocation,
}
