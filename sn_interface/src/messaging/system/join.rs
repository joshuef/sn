// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::NodeState;
use serde::{Deserialize, Serialize};
use sn_consensus::Decision;
use std::{fmt, net::SocketAddr};

/// Response to a request to join a section
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub struct JoinRequest {
    /// The public key of the section to join.
    pub section_key: bls::PublicKey,
}

impl JoinRequest {
    pub fn section_key(&self) -> bls::PublicKey {
        self.section_key
    }

    pub fn set_section_key(&mut self, section_key: bls::PublicKey) {
        self.section_key = section_key;
    }
}

/// Response to a request to join a section
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JoinResponse {
    /// Message sent to joining peer containing the current node's
    /// state as a member of the section.
    Approved(Decision<NodeState>),
    /// Join was rejected
    Rejected(JoinRejectReason),
}

/// Reason of a join request being rejected
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JoinRejectReason {
    /// No new peers are currently accepted for joining
    JoinsDisallowed,
    /// The requesting node is not externally reachable
    NodeNotReachable(SocketAddr),
}

impl fmt::Display for JoinRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
