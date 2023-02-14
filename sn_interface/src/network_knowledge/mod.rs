// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod errors;
mod node_info;
mod section_peers;
mod section_tree;
mod sections_dag;

pub mod node_state;
pub mod section_authority_provider;
pub mod section_keys;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
#[cfg(any(test, feature = "test-utils"))]
pub use section_tree::test_utils as test_utils_st;

pub use self::{
    errors::{Error, Result},
    node_info::MyNodeInfo,
    node_state::{MembershipState, NodeState, RelocationDst, RelocationInfo, RelocationProof},
    section_authority_provider::{SapCandidate, SectionAuthUtils, SectionAuthorityProvider},
    section_keys::{SectionKeyShare, SectionKeysProvider},
    section_tree::{SectionTree, SectionTreeUpdate},
    sections_dag::SectionsDAG,
};

use crate::{
    messaging::{
        system::{NodeMsg, SectionPeers as SectionPeersMsg, SectionSig, SectionSigned},
        Dst,
    },
    types::Peer,
};

use bls::PublicKey as BlsPublicKey;
use section_peers::SectionPeers;
use serde::Serialize;
use std::{collections::BTreeSet, iter, net::SocketAddr};
use xor_name::{Prefix, XorName};

/// The secret key for the genesis DBC.
///
/// This key is public for auditing purposes. Hard coding its value means all nodes will be able to
/// validate it.
pub const GENESIS_DBC_SK: &str = "0c5152498fc5b2f9ed691ef875f2c16f1f950910391f7ba1df63e9f0ce4b2780";

/// The minimum age a node becomes an adult node.
pub const MIN_ADULT_AGE: u8 = 1;

const SN_ELDER_COUNT: &str = "SN_ELDER_COUNT";
/// Number of elders per section.
pub const DEFAULT_ELDER_COUNT: usize = 7;

/// Get the expected elder count for our network.
/// Defaults to `DEFAULT_ELDER_COUNT`, but can be overridden by the env var `SN_ELDER_COUNT`.
pub fn elder_count() -> usize {
    // if we have an env var for this, lets override
    match std::env::var(SN_ELDER_COUNT) {
        Ok(count) => {
            match count.parse() {
                Ok(count) => {
                    warn!("ELDER_COUNT count set from env var SN_ELDER_COUNT: {SN_ELDER_COUNT:?}={count}");
                    count
                }
                Err(error) => {
                    warn!("There was an error parsing {SN_ELDER_COUNT:?} env var. DEFAULT_ELDER_COUNT={DEFAULT_ELDER_COUNT} will be used: {error:?}");
                    DEFAULT_ELDER_COUNT
                }
            }
        }
        Err(_) => DEFAULT_ELDER_COUNT,
    }
}

/// Recommended section size.
/// The section will keep adding nodes when requested by the upper layers, until it can split.
/// A split happens if both post-split sections would have at least this number of nodes.
pub fn recommended_section_size() -> usize {
    2 * elder_count()
}

/// `SuperMajority` of a given group (i.e. > 2/3)
#[inline]
pub const fn supermajority(group_size: usize) -> usize {
    threshold(group_size) + 1
}

/// `Threshold` of a given group
#[inline]
pub const fn threshold(group_size: usize) -> usize {
    group_size * 2 / 3
}

pub fn partition_by_prefix(
    prefix: &Prefix,
    nodes: impl IntoIterator<Item = XorName>,
) -> Option<(BTreeSet<XorName>, BTreeSet<XorName>)> {
    let decision_index: u8 = if let Ok(idx) = prefix.bit_count().try_into() {
        idx
    } else {
        return None;
    };

    let (one, zero) = nodes
        .into_iter()
        .filter(|name| prefix.matches(name))
        .partition(|name| name.bit(decision_index));

    Some((zero, one))
}

pub fn section_has_room_for_node(
    joining_node: XorName,
    prefix: &Prefix,
    members: impl IntoIterator<Item = XorName>,
) -> bool {
    // We multiply by two to allow a buffer for when nodes are joining sequentially.
    let split_section_size_cap = recommended_section_size() * 2;

    match partition_by_prefix(prefix, members) {
        Some((zeros, ones)) => {
            let n_zeros = zeros.len();
            let n_ones = ones.len();
            info!("Section {prefix:?} would split into {n_zeros} zero and {n_ones} one nodes");
            match joining_node.bit(prefix.bit_count() as u8) {
                // joining node would be part of the `ones` child section
                true => n_ones < split_section_size_cap,

                // joining node would be part of the `zeros` child section
                false => n_zeros < split_section_size_cap,
            }
        }
        None => false,
    }
}

/// Container for storing information about the network, including our own section.
#[derive(Clone, Debug)]
pub struct NetworkKnowledge {
    /// Signed Section Authority Provider
    signed_sap: SectionSigned<SectionAuthorityProvider>,
    /// Members of our section
    section_peers: SectionPeers,
    /// The network section tree, i.e. a map from prefix to SAPs plus all sections keys
    section_tree: SectionTree,
}

impl NetworkKnowledge {
    /// Creates a new `NetworkKnowledge` instance. The prefix is used to choose the
    /// `signed_sap` from the provided SectionTree
    pub fn new(prefix: Prefix, section_tree: SectionTree) -> Result<Self, Error> {
        let signed_sap = section_tree
            .get_signed(&prefix)
            .cloned()
            .ok_or(Error::NoMatchingSection)?;
        Ok(Self {
            signed_sap,
            section_peers: SectionPeers::default(),
            section_tree,
        })
    }

    /// Creates `NetworkKnowledge` for the first node in the network
    pub fn first_node(
        peer: Peer,
        genesis_sk_set: bls::SecretKeySet,
    ) -> Result<(Self, SectionKeyShare)> {
        let public_key_set = genesis_sk_set.public_keys();
        let secret_key_index = 0u8;
        let secret_key_share = genesis_sk_set.secret_key_share(secret_key_index as u64);

        let section_tree = {
            let genesis_signed_sap =
                create_first_section_authority_provider(&public_key_set, &secret_key_share, peer)?;
            SectionTree::new(genesis_signed_sap)?
        };
        let mut network_knowledge = Self::new(Prefix::default(), section_tree)?;

        for peer in network_knowledge.signed_sap.elders() {
            let node_state = NodeState::joined(*peer, None);
            let sig = create_first_sig(&public_key_set, &secret_key_share, &node_state)?;
            let _changed = network_knowledge.section_peers.update(SectionSigned {
                value: node_state,
                sig,
            });
        }

        let section_key_share = SectionKeyShare {
            public_key_set,
            index: 0,
            secret_key_share,
        };

        Ok((network_knowledge, section_key_share))
    }

    /// Switches to use the provided section
    /// as the section where our node belongs.
    pub fn switch_section(
        &mut self,
        dst_sap: SectionSigned<SectionAuthorityProvider>,
    ) -> Result<()> {
        if self.section_tree().get(&dst_sap.prefix()).is_none() {
            return Err(Error::NoMatchingSection);
        }
        self.signed_sap = dst_sap;
        Ok(())
    }

    /// Update our network knowledge with the provided `SectionTreeUpdate`
    pub fn update_knowledge_if_valid(
        &mut self,
        section_tree_update: SectionTreeUpdate,
        updated_members: Option<SectionPeersMsg>,
        our_name: &XorName,
    ) -> Result<bool> {
        trace!("Attempting to update network knoweldge");
        let mut there_was_an_update = false;
        let signed_sap = section_tree_update.signed_sap.clone();
        let sap_prefix = signed_sap.prefix();

        // If the update is for a different prefix, we just update the section_tree; else we should
        // update the section_tree and signed_sap together. Or else they might go out of sync and
        // querying section_tree using signed_sap will result in undesirable effect
        match self
            .section_tree
            .update_the_section_tree(section_tree_update)
        {
            Ok(true) => {
                there_was_an_update = true;
                info!("Updated network section tree with SAP for {:?}", sap_prefix);
                // update the signed_sap only if the prefix matches
                if sap_prefix.matches(our_name) {
                    let our_prev_prefix = self.prefix();
                    // Remove any peer which doesn't belong to our new section's prefix
                    self.section_peers.retain(&sap_prefix);
                    info!("Updated our section's SAP ({our_prev_prefix:?} to {sap_prefix:?}) with new one: {:?}", signed_sap.value);

                    let proof_chain = self
                        .section_tree
                        .get_sections_dag()
                        .partial_dag(self.genesis_key(), &signed_sap.section_key())?;

                    // Prune list of archived members
                    self.section_peers
                        .prune_members_archive(&proof_chain, &signed_sap.section_key())?;

                    // Switch to new SAP
                    self.signed_sap = signed_sap;
                }
            }
            Ok(false) => {
                warn!("Anti-Entropy: discarded SAP for {sap_prefix:?} since it's the same as the one in our records: {:?}", signed_sap.value);
            }
            Err(err) => {
                warn!("Anti-Entropy: discarded SAP for {sap_prefix:?} since we failed to update section tree with: {err:?}");
                return Err(err);
            }
        }

        // Update members if changes were provided
        if let Some(members) = updated_members {
            if !members.is_empty() && self.merge_members(members)? {
                there_was_an_update = true;
                let prefix = self.prefix();
                info!(
                    "Updated our section's members ({:?}): {:?}",
                    prefix, self.section_peers
                );
            }
        }

        Ok(there_was_an_update)
    }

    // Return the network genesis key
    pub fn genesis_key(&self) -> &bls::PublicKey {
        self.section_tree.genesis_key()
    }

    /// Prefix of our section.
    pub fn prefix(&self) -> Prefix {
        self.signed_sap.prefix()
    }

    /// All other prefixes we know of.
    pub fn prefixes(&self) -> impl Iterator<Item = &Prefix> {
        self.section_tree.prefixes()
    }

    // Returns reference to network section tree
    pub fn section_tree(&self) -> &SectionTree {
        &self.section_tree
    }

    // Returns mutable reference to network section tree
    pub fn section_tree_mut(&mut self) -> &mut SectionTree {
        &mut self.section_tree
    }

    /// Return current section key
    pub fn section_key(&self) -> bls::PublicKey {
        self.signed_sap.section_key()
    }

    /// Return a copy of current SAP
    pub fn section_auth(&self) -> SectionAuthorityProvider {
        self.signed_sap.value.clone()
    }

    // Returns the SAP for the prefix that matches name
    pub fn section_auth_by_name(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.section_tree
            .get_signed_by_name(name)
            .map(|sap| sap.value)
    }

    // Returns the SAP for the prefix
    pub fn section_auth_by_prefix(&self, prefix: &Prefix) -> Result<SectionAuthorityProvider> {
        self.section_tree
            .get_signed_by_prefix(prefix)
            .map(|sap| sap.value)
    }

    /// Return a copy of current SAP with corresponding section authority
    pub fn signed_sap(&self) -> SectionSigned<SectionAuthorityProvider> {
        self.signed_sap.clone()
    }

    // Get SAP of a known section with the given prefix, along with its proof chain
    pub fn closest_signed_sap(
        &self,
        name: &XorName,
    ) -> Option<SectionSigned<SectionAuthorityProvider>> {
        let closest_sap = self
            .section_tree
            .closest(name, Some(&self.prefix()))
            // In case the only prefix is ours, we fallback to it.
            .unwrap_or(self.section_tree.get_signed(&self.prefix())?);
        Some(closest_sap.clone())
    }

    // Get SAP of a known section with the given prefix, along with its proof chain
    pub fn closest_signed_sap_with_chain(
        &self,
        name: &XorName,
    ) -> Option<(SectionSigned<SectionAuthorityProvider>, SectionsDAG)> {
        let closest_sap = self.closest_signed_sap(name)?;

        if let Ok(section_chain) = self
            .section_tree
            .get_sections_dag()
            .partial_dag(self.genesis_key(), &closest_sap.value.section_key())
        {
            return Some((closest_sap, section_chain));
        }

        None
    }

    /// Generate a proof chain from the provided key to our current section key
    pub fn get_proof_chain_to_current_section(
        &self,
        from_key: &BlsPublicKey,
    ) -> Result<SectionsDAG> {
        let our_section_key = self.signed_sap.section_key();
        let proof_chain = self
            .section_tree
            .get_sections_dag()
            .partial_dag(from_key, &our_section_key)?;

        Ok(proof_chain)
    }

    /// Generate a proof chain from the genesis key to our current section key
    pub fn section_chain(&self) -> SectionsDAG {
        self.get_proof_chain_to_current_section(self.genesis_key())
            // Cannot fails since the section key in `NetworkKnowledge::signed_sap` is always
            // present in the `SectionTree`
            .unwrap_or_else(|_| SectionsDAG::new(*self.genesis_key()))
    }

    /// Return the number of keys in our section chain
    pub fn section_chain_len(&self) -> u64 {
        self.section_chain().keys().count() as u64
    }

    /// Return weather current section chain has the provided key
    pub fn has_chain_key(&self, key: &bls::PublicKey) -> bool {
        self.section_chain().has_key(key)
    }

    /// Verify the given public key corresponds to any (current/old) section known to us
    pub fn verify_section_key_is_known(&self, section_key: &BlsPublicKey) -> bool {
        self.section_tree.get_sections_dag().has_key(section_key)
    }

    /// Return the set of known keys
    pub fn known_keys(&self) -> BTreeSet<bls::PublicKey> {
        self.section_tree.get_sections_dag().keys().collect()
    }

    /// Try to merge this `NetworkKnowledge` members with `peers`.
    /// Checks if we're already up to date before attempting to verify and merge members
    pub fn merge_members(&mut self, peers: BTreeSet<SectionSigned<NodeState>>) -> Result<bool> {
        let mut there_was_an_update = false;
        let our_current_members = self.section_peers.members();

        for node_state in &peers {
            if our_current_members.contains(node_state) {
                // we already know of this one, so nothing to do here.
                continue;
            }
            trace!(
                "Attempting to update section members. Name: {:?}, new state: {:?}",
                node_state.name(),
                node_state.state()
            );
            if !node_state.verify(&self.section_chain()) {
                error!(
                    "Can't update section member, name: {:?}, new state: {:?}",
                    node_state.name(),
                    node_state.state()
                );
            } else if self.section_peers.update(node_state.clone()) {
                there_was_an_update = true;
            }
        }

        self.section_peers.retain(&self.prefix());

        Ok(there_was_an_update)
    }

    /// Update the member. Returns whether it actually updated it.
    pub fn update_member(&mut self, node_state: SectionSigned<NodeState>) -> bool {
        let node_name = node_state.name();
        trace!(
            "Updating section member state, name: {node_name:?}, new state: {:?}",
            node_state.state()
        );

        // let's check the node state is properly signed by one of the keys in our chain
        if !node_state.verify(&self.section_chain()) {
            error!(
                "Can't update section member, name: {node_name:?}, new state: {:?}",
                node_state.state()
            );
            return false;
        }

        let updated = self.section_peers.update(node_state);
        trace!("Section member state, name: {node_name:?}, updated: {updated}");

        updated
    }

    /// Returns the members of our section
    pub fn members(&self) -> BTreeSet<Peer> {
        self.elders().into_iter().chain(self.adults()).collect()
    }

    /// Returns the elders of our section
    pub fn elders(&self) -> BTreeSet<Peer> {
        self.section_auth().elders_set()
    }

    /// Returns live adults from our section.
    pub fn adults(&self) -> BTreeSet<Peer> {
        let mut live_adults = BTreeSet::new();
        for node_state in self.section_peers.members() {
            if !self.is_elder(&node_state.name()) {
                let _ = live_adults.insert(*node_state.peer());
            }
        }
        live_adults
    }

    /// Return whether the name provided belongs to an Elder, by checking if
    /// it is one of the current section's SAP member,
    pub fn is_elder(&self, name: &XorName) -> bool {
        self.signed_sap.contains_elder(name)
    }

    /// Return whether the name provided belongs to an Adult, by checking if
    /// it is one of the current section's SAP member,
    pub fn is_adult(&self, name: &XorName) -> bool {
        self.adults().iter().any(|a| a.name() == *name)
    }

    /// Generate dst for a given XorName with correct section_key
    pub fn generate_dst(&self, recipient: &XorName) -> Result<Dst> {
        Ok(Dst {
            name: *recipient,
            section_key: self.section_auth_by_name(recipient)?.section_key(),
        })
    }

    /// Returns members that are joined.
    pub fn section_members(&self) -> BTreeSet<NodeState> {
        self.section_peers
            .members()
            .into_iter()
            .map(|state| state.value)
            .collect()
    }

    /// Returns the list of members that have left our section
    pub fn section_signed_archived_members(&self) -> BTreeSet<SectionSigned<NodeState>> {
        self.section_peers.archived_members()
    }

    /// Returns current list of section signed members.
    pub fn section_signed_members(&self) -> BTreeSet<SectionSigned<NodeState>> {
        self.section_peers.members()
    }

    /// Returns current section size, i.e. number of peers in the section.
    pub fn section_size(&self) -> usize {
        self.section_peers.num_of_members()
    }

    /// Get info for the member with the given name.
    pub fn get_section_member(&self, name: &XorName) -> Option<NodeState> {
        self.section_peers.get(name)
    }

    /// Get info for the member with the given name either from current members list,
    /// or from the archive of left/relocated members
    pub fn is_either_member_or_archived(&self, name: &XorName) -> Option<SectionSigned<NodeState>> {
        self.section_peers.is_either_member_or_archived(name)
    }

    /// Get info for the member with the given name.
    pub fn is_section_member(&self, name: &XorName) -> bool {
        self.section_peers.is_member(name)
    }

    pub fn find_member_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        self.section_peers
            .members()
            .into_iter()
            .find(|info| info.addr() == *addr)
            .map(|info| *info.peer())
    }

    pub fn anti_entropy_probe(&self) -> NodeMsg {
        NodeMsg::AntiEntropyProbe(self.section_key())
    }
}

// Create `SectionAuthorityProvider` for the first node.
fn create_first_section_authority_provider(
    pk_set: &bls::PublicKeySet,
    sk_share: &bls::SecretKeyShare,
    peer: Peer,
) -> Result<SectionSigned<SectionAuthorityProvider>> {
    let section_auth = SectionAuthorityProvider::new(
        iter::once(peer),
        Prefix::default(),
        [NodeState::joined(peer, None)],
        pk_set.clone(),
        0,
    );
    let sig = create_first_sig(pk_set, sk_share, &section_auth)?;
    Ok(SectionSigned::new(section_auth, sig))
}

fn create_first_sig<T: Serialize>(
    pk_set: &bls::PublicKeySet,
    sk_share: &bls::SecretKeyShare,
    payload: &T,
) -> Result<SectionSig> {
    let bytes = bincode::serialize(payload).map_err(|_| Error::InvalidPayload)?;
    let signature_share = sk_share.sign(bytes);
    let signature = pk_set
        .combine_signatures(iter::once((0, &signature_share)))
        .map_err(|_| Error::InvalidSignatureShare)?;

    Ok(SectionSig {
        public_key: pk_set.public_key(),
        signature,
    })
}

#[cfg(test)]
mod tests {
    use super::{supermajority, NetworkKnowledge};
    use crate::{
        test_utils::{gen_addr, prefix, TestKeys, TestSapBuilder, TestSectionTree},
        types::Peer,
    };

    use bls::SecretKeySet;
    use eyre::Result;
    use proptest::prelude::*;
    use rand::thread_rng;
    use xor_name::XorName;

    #[test]
    fn supermajority_of_small_group() {
        assert_eq!(supermajority(0), 1);
        assert_eq!(supermajority(1), 1);
        assert_eq!(supermajority(2), 2);
        assert_eq!(supermajority(3), 3);
        assert_eq!(supermajority(4), 3);
        assert_eq!(supermajority(5), 4);
        assert_eq!(supermajority(6), 5);
        assert_eq!(supermajority(7), 5);
        assert_eq!(supermajority(8), 6);
        assert_eq!(supermajority(9), 7);
    }

    proptest! {
        #[test]
        fn proptest_supermajority(a in 0usize..10000) {
            let n = 3 * a;
            assert_eq!(supermajority(n),     2 * a + 1);
            assert_eq!(supermajority(n + 1), 2 * a + 1);
            assert_eq!(supermajority(n + 2), 2 * a + 2);
        }
    }

    #[test]
    fn signed_sap_field_should_not_be_changed_if_the_update_is_for_a_different_prefix() -> Result<()>
    {
        let mut rng = thread_rng();
        let sk_gen = SecretKeySet::random(0, &mut rng);
        let peer = Peer::new(XorName::random(&mut rng), gen_addr());
        let (mut knowledge, _) = NetworkKnowledge::first_node(peer, sk_gen.clone())?;

        // section 1
        let (sap1, sk_1, ..) = TestSapBuilder::new(prefix("1")).elder_count(0).build();
        let sap1 = TestKeys::get_section_signed(&sk_1.secret_key(), sap1);
        let our_node_name_prefix_1 = sap1.prefix().name();
        let proof_chain = knowledge.section_chain();
        let section_tree_update =
            TestSectionTree::get_section_tree_update(&sap1, &proof_chain, &sk_gen.secret_key());
        assert!(knowledge.update_knowledge_if_valid(
            section_tree_update,
            None,
            &our_node_name_prefix_1
        )?);
        assert_eq!(knowledge.signed_sap, sap1);

        // section with different prefix (0) and our node name doesn't match
        let (sap0, sk_0, ..) = TestSapBuilder::new(prefix("0")).elder_count(0).build();
        let sap0 = TestKeys::get_section_signed(&sk_0.secret_key(), sap0);
        let section_tree_update =
            TestSectionTree::get_section_tree_update(&sap0, &proof_chain, &sk_gen.secret_key());
        // our node is still in prefix1
        assert!(knowledge.update_knowledge_if_valid(
            section_tree_update,
            None,
            &our_node_name_prefix_1
        )?);
        // sap should not be updated
        assert_eq!(knowledge.signed_sap, sap1);

        Ok(())
    }
}
