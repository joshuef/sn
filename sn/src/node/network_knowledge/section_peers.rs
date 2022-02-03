// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::system::{MembershipState, SectionAuth};
use crate::node::network_knowledge::{NodeState, SectionAuthorityProvider};
use crate::peer::Peer;
use dashmap::{mapref::entry::Entry, DashMap};
use itertools::Itertools;
use secured_linked_list::SecuredLinkedList;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};
use xor_name::{Prefix, XorName};

// Number of Elder churn events before a Left/Relocated member
// can be removed from the section members archive.
const ELDER_CHURN_EVENTS_TO_PRUNE_ARCHIVE: usize = 5;

/// Container for storing information about (current and archived) members of our section.
#[derive(Clone, Default, Debug)]
pub(super) struct SectionPeers {
    members: Arc<DashMap<XorName, SectionAuth<NodeState>>>,
    archive: Arc<DashMap<XorName, SectionAuth<NodeState>>>,
}

impl SectionPeers {
    /// Returns set of current members, i.e. those with state == `Joined`.
    pub(super) fn members(&self) -> BTreeSet<SectionAuth<NodeState>> {
        self.members
            .iter()
            .map(|entry| {
                let (_, state) = entry.pair();
                state.clone()
            })
            .collect()
    }

    /// Returns the number of current members.
    pub(super) fn num_of_members(&self) -> usize {
        self.members.len()
    }

    /// Get the `NodeState` for the member with the given name.
    pub(super) fn get(&self, name: &XorName) -> Option<NodeState> {
        self.members.get(name).map(|state| state.value.clone())
    }

    /// Returns whether the given peer is currently a member of our section.
    pub(super) fn is_member(&self, name: &XorName) -> bool {
        self.members.get(name).is_some()
    }

    /// Returns whether the given peer is already relocated to our section.
    pub(super) fn is_relocated_to_our_section(&self, name: &XorName) -> bool {
        let is_previous_name_of_member = self.members.iter().any(|entry| {
            let (_, state) = entry.pair();
            state.previous_name() == Some(*name)
        });

        is_previous_name_of_member
            || self.archive.iter().any(|entry| {
                let (_, state) = entry.pair();
                state.previous_name() == Some(*name)
            })
    }

    /// Get section signed `NodeState` for the member with the given name.
    pub(super) fn is_either_member_or_archived(
        &self,
        name: &XorName,
    ) -> Option<SectionAuth<NodeState>> {
        if let Some(member) = self.members.get(name).map(|state| state.value().clone()) {
            Some(member)
        } else {
            self.archive.get(name).map(|state| state.value().clone())
        }
    }

    /// Returns the nodes that should be candidates to become the next elders, sorted by names.
    pub(super) fn elder_candidates(
        &self,
        elder_size: usize,
        current_elders: &SectionAuthorityProvider,
        excluded_names: &BTreeSet<XorName>,
        prefix: Option<&Prefix>,
    ) -> Vec<Peer> {
        self.members
            .iter()
            .filter(|entry| {
                let (name, _) = entry.pair();
                prefix.map_or_else(|| true, |p| p.matches(name)) && !excluded_names.contains(name)
            })
            .map(|entry| {
                let (_, node_state) = entry.pair();
                node_state.clone()
            })
            .sorted_by(|lhs, rhs| cmp_elder_candidates(lhs, rhs, current_elders))
            .take(elder_size)
            .map(|node_state| node_state.peer().clone())
            .collect()
    }

    /// Update a member of our section.
    /// Returns whether anything actually changed.
    /// To maintain commutativity, the only allowed transitions are:
    /// - Joined -> Joined if the new age is greater than the old age
    /// - Joined -> Left
    /// - Joined -> Relocated
    /// - Relocated <--> Left (should not happen, but needed for consistency)
    pub(super) fn update(&self, new_state: SectionAuth<NodeState>) -> bool {
        let node_name = new_state.name();
        match (self.members.entry(new_state.name()), new_state.state()) {
            (Entry::Vacant(entry), MembershipState::Joined) => {
                // unless it was already archived, insert it as current member
                if self.archive.get(&node_name).is_none() {
                    let _prev = entry.insert(new_state);
                    true
                } else {
                    false
                }
            }
            (Entry::Vacant(_), MembershipState::Left | MembershipState::Relocated(_)) => {
                // insert it in our archive regardless it was there with another state
                let _prev = self.archive.insert(node_name, new_state);
                true
            }
            (Entry::Occupied(mut entry), MembershipState::Joined)
                if new_state.age() > entry.get().age() =>
            {
                let _prev = entry.insert(new_state);
                true
            }
            (Entry::Occupied(_), MembershipState::Joined) => false,
            (Entry::Occupied(entry), MembershipState::Left | MembershipState::Relocated(_)) => {
                //  remove it from our current members, and insert it into our archive
                let _prev = entry.remove_entry();
                let _prev = self.archive.insert(node_name, new_state);
                true
            }
        }
    }

    /// Remove all members whose name does not match `prefix`.
    pub(super) fn retain(&self, prefix: &Prefix) {
        self.members.retain(|name, _| prefix.matches(name))
    }

    /// Merge connections into of our current members
    pub(super) async fn merge_connections(&self, sources: &BTreeMap<SocketAddr, &Peer>) {
        for entry in self.members.iter() {
            let (_, node) = entry.pair();
            if let Some(source) = sources.get(&node.addr()) {
                node.peer().merge_connection(source).await;
            }
        }
    }

    // Remove any member which Left, or was Relocated, more
    // than ELDER_CHURN_EVENTS_TO_PRUNE_ARCHIVE section keys ago.
    pub(super) async fn prune_members_archive(&self, section_chain: &SecuredLinkedList) {
        let last_section_keys = section_chain.truncate(ELDER_CHURN_EVENTS_TO_PRUNE_ARCHIVE);
        self.archive
            .retain(|_, node_state| last_section_keys.has_key(&node_state.sig.public_key))
    }
}

// Compare candidates for the next elders. The one comparing `Less` wins.
fn cmp_elder_candidates(
    lhs: &SectionAuth<NodeState>,
    rhs: &SectionAuth<NodeState>,
    current_elders: &SectionAuthorityProvider,
) -> Ordering {
    // Older nodes are preferred. In case of a tie, prefer current elders. If still a tie, break
    // it comparing by the signed signatures because it's impossible for a node to predict its
    // signature and therefore game its chances of promotion.
    rhs.age()
        .cmp(&lhs.age())
        .then_with(|| {
            let lhs_is_elder = current_elders.contains_elder(&lhs.name());
            let rhs_is_elder = current_elders.contains_elder(&rhs.name());

            match (lhs_is_elder, rhs_is_elder) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                _ => Ordering::Equal,
            }
        })
        .then_with(|| lhs.sig.signature.cmp(&rhs.sig.signature))
}
