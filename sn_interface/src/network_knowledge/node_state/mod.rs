// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod relocation;

pub use relocation::{
    ChurnId, RelocationInfo, RelocationProof, RelocationState, RelocationTrigger,
};

use crate::network_knowledge::{section_has_room_for_node, Error, Result};
use crate::types::NodeId;

use serde::{Deserialize, Serialize};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Formatter},
    net::SocketAddr,
};
use xor_name::{Prefix, XorName};

#[derive(custom_debug::Debug)]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
/// Node's current section membership state
pub enum MembershipState {
    /// Node is active member of the section.
    Joined,
    /// Node went offline.
    Left,
    #[debug(skip)]
    /// Node was relocated to a different section.
    Relocated(RelocationTrigger),
}

/// Information about a member of our section.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NodeState {
    node_id: NodeId,
    /// Current membership state of the node.
    state: MembershipState,
    /// To avoid sybil attack via relocation, a relocated node's original name will be recorded.
    previous_name: Option<XorName>,
}

impl Debug for NodeState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut f = f.debug_tuple("NodeState");
        let f = f
            .field(&self.name())
            .field(&self.addr());
            // .field(&self.state());

        let f = if let Some(prev_name) = self.previous_name() {
            f.field(&format!("prev_name: {prev_name:?}"))
        } else {
            f
        };
        f.finish()
    }
}

impl NodeState {
    // Creates a `NodeState` in the `Joined` state.
    pub fn joined(node_id: NodeId, previous_name: Option<XorName>) -> Self {
        Self {
            node_id,
            state: MembershipState::Joined,
            previous_name,
        }
    }

    // Creates a `NodeState` in the `Left` state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn left(node_id: NodeId, previous_name: Option<XorName>) -> Self {
        Self {
            node_id,
            state: MembershipState::Left,
            previous_name,
        }
    }

    // Creates a `NodeState` in the `Relocated` state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn relocated(
        node_id: NodeId,
        previous_name: Option<XorName>,
        relocation_trigger: RelocationTrigger,
    ) -> Self {
        Self {
            node_id,
            state: MembershipState::Relocated(relocation_trigger),
            previous_name,
        }
    }

    pub fn validate_node_state(
        &self,
        prefix: &Prefix,
        current_joined_members: &BTreeMap<XorName, Self>,
        archived: &BTreeSet<XorName>,
    ) -> Result<()> {
        let name = self.name();
        info!("Validating node state for {name} - {:?}", self.state);

        if !prefix.matches(&name) {
            warn!("Membership - rejecting node {name}, name doesn't match our prefix {prefix:?}");
            return Err(Error::WrongSection);
        }

        match self.state {
            MembershipState::Joined => {
                if current_joined_members.contains_key(&name) {
                    warn!("Rejecting join from existing member {name}");
                    Err(Error::ExistingMemberNameConflict)
                } else if !section_has_room_for_node(
                    name,
                    prefix,
                    current_joined_members.keys().copied(),
                ) {
                    warn!("Rejecting join since we are at capacity");
                    Err(Error::TryJoinLater)
                } else if let Some(existing_node) = current_joined_members
                    .values()
                    .find(|n| n.node_id().addr() == self.node_id().addr())
                {
                    warn!("Rejecting join since we have an existing node with this address: {existing_node:?}");
                    Err(Error::ExistingMemberSocketAddrConflict)
                } else if archived.contains(&name) {
                    Err(Error::ArchivedNodeRejoined)
                } else {
                    Ok(())
                }
            }
            MembershipState::Relocated(_) => {
                // A node relocation is always OK
                Ok(())
            }
            MembershipState::Left => {
                if !current_joined_members.contains_key(&name) {
                    warn!("Rejecting leave from non-existing member");
                    Err(Error::NotAMember)
                } else {
                    Ok(())
                }
            }
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn name(&self) -> XorName {
        self.node_id.name()
    }

    pub fn addr(&self) -> SocketAddr {
        self.node_id.addr()
    }

    pub fn state(&self) -> MembershipState {
        self.state.clone()
    }

    pub fn previous_name(&self) -> Option<XorName> {
        self.previous_name
    }

    pub fn age(&self) -> u8 {
        self.node_id.age()
    }

    // Returns true if the state is a Relocated node
    pub fn is_relocated(&self) -> bool {
        matches!(self.state, MembershipState::Relocated(_))
    }

    pub fn leave(self) -> Result<Self, Error> {
        // Do not allow switching to `Left` when already relocated,
        assert_eq!(self.state, MembershipState::Joined);

        Ok(Self {
            state: MembershipState::Left,
            ..self
        })
    }

    // Convert this info into one with the state changed to `Relocated`.
    pub fn relocate(self, relocation_trigger: RelocationTrigger) -> Self {
        Self {
            state: MembershipState::Relocated(relocation_trigger),
            ..self
        }
    }
}
