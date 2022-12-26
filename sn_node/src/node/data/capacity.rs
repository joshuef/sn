// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::XorName;
use sn_interface::messaging::data::StorageThreshold;
use std::collections::{BTreeMap, BTreeSet};

/// A util for sharing the info on data capacity among the
/// chunk storing nodes in the section.
#[derive(Default)]
pub(crate) struct Capacity {
    full_nodes: BTreeMap<XorName, StorageThreshold>,
}

impl Capacity {
    /// Full chunk storing nodes in the section (considered full when at >= `MIN_LEVEL_WHEN_FULL`).
    pub(crate) fn full_nodes(&self) -> BTreeSet<XorName> {
        self.full_nodes.iter().map(|(name, _)| *name).collect()
    }

    /// Returns whether the node was set.
    pub(crate) fn set_node_full(&mut self, node: XorName) -> bool {
        self.full_nodes
            .insert(node, StorageThreshold::new())
            .is_none()
    }

    // Clears list of reportedly full nodes
    pub(crate) fn clear_full_nodes(&mut self) {
        self.full_nodes.clear()
    }

    /// Registered holders not present in provided list of members
    /// will be removed from `full_nodes` and no longer tracked.
    pub(crate) fn retain_members_only(&mut self, members: &BTreeSet<XorName>) {
        self.full_nodes.retain(|name, _| members.contains(name))
    }
}
