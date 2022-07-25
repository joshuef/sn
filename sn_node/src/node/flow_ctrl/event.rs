// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::cmds::CmdJob;

use sn_interface::messaging::{
    data::ServiceMsg, system::SystemMsg, AuthorityProof, Dst, EndUser, MsgId, NodeAuth, ServiceAuth,
};

use bls::PublicKey as BlsPublicKey;
use chrono::{DateTime, Utc};
use ed25519_dalek::Keypair;
use std::{collections::BTreeSet, sync::Arc, time::SystemTime};
use xor_name::{Prefix, XorName};

/// Node-internal events raised via its event sender.
///
/// These have previously connected to separate (legacy) layers of the
/// code base (routing and node), which today has been refactored away.
/// They remained in tests though, as those were tightly dependent on
/// the events, and were not refactored.
/// Recently the event has been taken into use by the domain logic in one place,
/// but primarily the aim is now to make use of it for a more structured logging
/// and as a read-node_api to a UI.
#[allow(clippy::large_enum_variant)]
#[derive(custom_debug::Debug)]
pub enum Event {
    ///
    Data(DataEvent),
    ///
    Messaging(MessagingEvent),
    ///
    Membership(MembershipEvent),
    ///
    CmdProcessing(CmdProcessEvent),
}

/// Informing on data related changes.
///
/// Currently not used.
#[derive(Debug)]
pub enum DataEvent {}

/// Informing on incoming msgs.
///
/// Currently only used in tests.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum MessagingEvent {
    /// Received a msg from another Node.
    SystemMsgReceived {
        /// The msg ID
        msg_id: MsgId,
        /// Msg authority
        auth: AuthorityProof<NodeAuth>,
        /// Dst of the msg
        dst: Dst,
        /// The msg.
        msg: Box<SystemMsg>,
    },
    /// Received a msg from a client.
    ServiceMsgReceived {
        /// The msg ID
        msg_id: MsgId,
        /// The content of the msg.
        msg: Box<ServiceMsg>,
        /// Msg authority
        auth: AuthorityProof<ServiceAuth>,
        /// The end user that sent the msg.
        /// Its xorname is derived from the client public key,
        /// and the socket_id maps against the actual socketaddr
        user: EndUser,
        /// Dst of the msg
        dst: Dst,
    },
}

/// Informing on the processing of an individual cmd.
#[derive(custom_debug::Debug)]
pub enum CmdProcessEvent {
    ///
    Started {
        ///
        job: CmdJob,
        ///
        time: SystemTime,
    },
    ///
    Finished {
        ///
        job: CmdJob,
        ///
        time: SystemTime,
    },
    ///
    Failed {
        ///
        job: CmdJob,
        ///
        time: SystemTime,
        ///
        error: String,
    },
}

/// Informing on membership related changes.¨
///
/// Still mostly used in tests.
#[derive(custom_debug::Debug)]
pub enum MembershipEvent {
    /// Join occured during section churn and new elders missed it,
    /// therefore the node is not a section member anymore, it needs to rejoin the network.
    ChurnJoinMissError,
    /// A new peer joined our section.
    MemberJoined {
        /// Name of the node
        name: XorName,
        /// Previous name before relocation or `None` if it is a new node.
        previous_name: Option<XorName>,
        /// Age of the node
        age: u8,
    },
    /// A node left our section.
    MemberLeft {
        /// Name of the node
        name: XorName,
        /// Age of the node
        age: u8,
    },
    /// The set of elders in our section has changed.
    EldersChanged {
        /// The Elders of our section.
        elders: Elders,
        /// Promoted, demoted or no change?
        self_status_change: NodeElderChange,
    },
    /// Notify the current list of adult nodes, in case of churning.
    AdultsChanged {
        /// Remaining Adults in our section.
        remaining: BTreeSet<XorName>,
        /// New Adults in our section.
        added: BTreeSet<XorName>,
        /// Removed Adults in our section.
        removed: BTreeSet<XorName>,
    },
    /// Our section has split.
    SectionSplit {
        /// The Elders of our section.
        elders: Elders,
        /// Promoted, demoted or no change?
        self_status_change: NodeElderChange,
    },
    /// This node has started relocating to other section. Will be followed by
    /// `Relocated` when the node finishes joining the destination section.
    RelocationStarted {
        /// Previous name before relocation
        previous_name: XorName,
    },
    /// This node has completed relocation to other section.
    Relocated {
        /// Old name before the relocation.
        previous_name: XorName,
        /// New keypair to be used after relocation.
        #[debug(skip)]
        new_keypair: Arc<Keypair>,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(e) => write!(f, "{:?}", e),
            Self::Messaging(e) => write!(f, "{:?}", e),
            Self::Membership(e) => write!(f, "{:?}", e),
            Self::CmdProcessing(e) => write!(f, "{}", e),
        }
    }
}

impl std::fmt::Display for CmdProcessEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started { job, time } => {
                let cmd = job.cmd();
                let queued_for = time
                    .duration_since(job.created_at())
                    .unwrap_or_default()
                    .as_millis();
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Started id: {}, parent: {:?} prio: {}, queued for {} ms. Cmd: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.parent_id(),
                    job.priority(),
                    queued_for,
                    cmd,
                )
            }
            Self::Finished { job, time } => {
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Finished id: {}, prio: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.priority(),
                )
            }
            Self::Failed {
                job,
                // retry,
                time,
                error,
            } => {
                let time: DateTime<Utc> = (*time).into();
                write!(
                    f,
                    "{}: Failed id: {}, prio: {}, due to: {}",
                    time.to_rfc3339(),
                    job.id(),
                    job.priority(),
                    // retry,
                    error,
                )
            }
        }
    }
}

/// A flag in `EldersChanged` event, indicating
/// whether the node got promoted, demoted or did not change.
#[derive(Debug, Clone, Copy)]
pub enum NodeElderChange {
    /// The node was promoted to Elder.
    Promoted,
    /// The node was demoted to Adult.
    Demoted,
    /// There was no change to the node.
    None,
}

/// Bound name of elders and `section_key`, `section_prefix` info together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elders {
    /// The prefix of the section.
    pub prefix: Prefix,
    /// The BLS public key of a section.
    pub key: BlsPublicKey,
    /// Remaining Elders in our section.
    pub remaining: BTreeSet<XorName>,
    /// New Elders in our section.
    pub added: BTreeSet<XorName>,
    /// Removed Elders in our section.
    pub removed: BTreeSet<XorName>,
}
