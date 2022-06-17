// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod errors;
mod node_info;
pub mod node_state;
pub mod prefix_map;
pub mod section_authority_provider;
pub mod section_keys;
mod section_peers;
pub mod utils;

#[cfg(any(test, feature = "test-utils"))]
pub use self::section_authority_provider::test_utils;

pub use self::section_keys::{SectionKeyShare, SectionKeysProvider};

pub use node_info::NodeInfo;
pub use node_state::NodeState;
pub use section_authority_provider::{SapCandidate, SectionAuthUtils, SectionAuthorityProvider};

use crate::messaging::{
    system::{
        KeyedSig, NodeMsgAuthorityUtils, SectionAuth, SectionPeers as SectionPeersMsg, SystemMsg,
    },
    NodeMsgAuthority,
};
use prefix_map::NetworkPrefixMap;
// use crate::node::{dkg::SectionAuthUtils, recommended_section_size};
use crate::types::Peer;
pub use errors::{Error, Result};

use bls::PublicKey as BlsPublicKey;
use section_peers::SectionPeers;
use secured_linked_list::SecuredLinkedList;
use serde::Serialize;
use std::{cell::RefCell, collections::BTreeSet, iter, net::SocketAddr, rc::Rc};
use xor_name::{Prefix, XorName};

/// The minimum age a node becomes an adult node.
pub const MIN_ADULT_AGE: u8 = 5;

/// During the first section, nodes can start at a range of age to avoid too many nodes having the
/// same time get relocated at the same time.
/// Defines the lower bound of this range.
pub const FIRST_SECTION_MIN_AGE: u8 = MIN_ADULT_AGE + 1;
/// Defines the higher bound of this range.
pub const FIRST_SECTION_MAX_AGE: u8 = 100;

const SN_ELDER_COUNT: &str = "SN_ELDER_COUNT";
/// Number of elders per section.
pub const DEFAULT_ELDER_COUNT: usize = 7;

/// Get the expected elder count for our network.
/// Defaults to DEFAULT_ELDER_COUNT, but can be overridden by the env var SN_ELDER_COUNT.
pub fn elder_count() -> usize {
    // if we have an env var for this, lets override
    match std::env::var(SN_ELDER_COUNT) {
        Ok(count) => match count.parse() {
            Ok(count) => {
                warn!(
                    "ELDER_COUNT count set from env var SN_ELDER_COUNT: {:?}",
                    SN_ELDER_COUNT
                );
                count
            }
            Err(error) => {
                warn!("There was an error parsing {:?} env var. DEFAULT_ELDER_COUNT will be used: {:?}", SN_ELDER_COUNT, error);
                DEFAULT_ELDER_COUNT
            }
        },
        Err(_) => DEFAULT_ELDER_COUNT,
    }
}

/// Recommended section size.
/// The section will keep adding nodes when requested by the upper layers, until it can split.
/// A split happens if both post-split sections would have at least this number of nodes.
pub fn recommended_section_size() -> usize {
    2 * crate::network_knowledge::elder_count()
}

/// SuperMajority of a given group (i.e. > 2/3)
#[inline]
pub const fn supermajority(group_size: usize) -> usize {
    1 + group_size * 2 / 3
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
    /// Network genesis key
    genesis_key: BlsPublicKey,
    /// Current section chain of our own section, starting from genesis key
    chain: Rc<RefCell<SecuredLinkedList>>,
    /// Signed Section Authority Provider
    signed_sap: Rc<RefCell<SectionAuth<SectionAuthorityProvider>>>,
    /// Members of our section
    section_peers: SectionPeers,
    /// The network prefix map, i.e. a map from prefix to SAPs
    prefix_map: NetworkPrefixMap,
    /// A DAG containing all section chains of the whole network that we are aware of
    all_sections_chains: Rc<RefCell<SecuredLinkedList>>,
}

impl NetworkKnowledge {
    /// Creates a minimal `NetworkKnowledge` initially containing only info about our elders
    /// (`SAP`).
    ///
    /// Returns error if the `signed_sap` is not verifiable with the `chain`.
    pub fn new(
        genesis_key: bls::PublicKey,
        chain: SecuredLinkedList,
        signed_sap: SectionAuth<SectionAuthorityProvider>,
        passed_prefix_map: Option<NetworkPrefixMap>,
    ) -> Result<Self, Error> {
        // Let's check the section chain's genesis key matches ours.
        if genesis_key != *chain.root_key() {
            return Err(Error::UntrustedProofChain(format!(
                "genesis key doesn't match first key in proof chain: {:?}",
                chain.root_key()
            )));
        }

        // Check the SAP's key is the last key of the section chain
        if signed_sap.sig.public_key != *chain.last_key() {
            error!("can't create section: SAP signed with incorrect key");
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "section key doesn't match last key in proof chain: {:?}",
                signed_sap.value
            )));
        }

        // Check if SAP signature is valid
        if !signed_sap.self_verify() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "invalid signature: {:?}",
                signed_sap.value
            )));
        }

        // Check if SAP's section key matches SAP signature's key
        if signed_sap.sig.public_key != signed_sap.section_key() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "section key doesn't match signature's key: {:?}",
                signed_sap.value
            )));
        }

        // Make sure the section chain can be trusted, i.e. check that
        // each key is signed by its parent/predecesor key.
        if !chain.self_verify() {
            return Err(Error::UntrustedProofChain(format!(
                "invalid chain: {:?}",
                chain
            )));
        }

        // Check if the genesis key in the provided prefix_map matches ours.
        // If no prefix map was provided, start afresh.
        let prefix_map = match passed_prefix_map {
            Some(prefix_map) => {
                if prefix_map.genesis_key() != genesis_key {
                    return Err(Error::InvalidGenesisKey(prefix_map.genesis_key()));
                } else {
                    prefix_map
                }
            }
            None => NetworkPrefixMap::new(genesis_key),
        };

        // At this point we know the prefix map corresponds to the correct genesis key,
        // let's make sure the prefix map contains also our own prefix and SAP,
        if let Err(err) = prefix_map.update(signed_sap.clone(), &chain) {
            debug!("Failed to update NetworkPrefixMap with SAP {:?} and chain {:?} upon creating new NetworkKnowledge intance: {:?}", signed_sap, chain, err);
        }

        Ok(Self {
            genesis_key,
            chain: Rc::new(RefCell::new(chain.clone())),
            signed_sap: Rc::new(RefCell::new(signed_sap)),
            section_peers: SectionPeers::default(),
            prefix_map,
            all_sections_chains: Rc::new(RefCell::new(chain)),
        })
    }

    /// update all section info for our new section
    pub async fn relocated_to(&self, new_network_nowledge: Self) -> Result<()> {
        debug!("Node was relocated to {:?}", new_network_nowledge);

        let chain_to_update_to = new_network_nowledge.section_chain().await;
        *self.chain.borrow_mut() = chain_to_update_to;

        *self.signed_sap.borrow_mut() = new_network_nowledge.signed_sap();

        let _updated = self
            .merge_members(new_network_nowledge.section_signed_members().await)
            .await?;

        Ok(())
    }

    /// Creates `NetworkKnowledge` for the first node in the network
    pub async fn first_node(
        peer: Peer,
        genesis_sk_set: bls::SecretKeySet,
    ) -> Result<(NetworkKnowledge, SectionKeyShare)> {
        let public_key_set = genesis_sk_set.public_keys();
        let secret_key_index = 0u8;
        let secret_key_share = genesis_sk_set.secret_key_share(secret_key_index as u64);
        let genesis_key = public_key_set.public_key();

        let section_auth =
            create_first_section_authority_provider(&public_key_set, &secret_key_share, peer)?;

        let network_knowledge = NetworkKnowledge::new(
            genesis_key,
            SecuredLinkedList::new(genesis_key),
            section_auth,
            None,
        )?;

        let sap = network_knowledge.signed_sap.borrow().clone();

        for peer in sap.elders() {
            let node_state = NodeState::joined(*peer, None);
            let sig = create_first_sig(&public_key_set, &secret_key_share, &node_state)?;
            let _changed = network_knowledge.section_peers.update(SectionAuth {
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

    /// If we already have the signed SAP and section chain for the provided key and prefix
    /// we make them the current SAP and section chain, and if so, this returns 'true'.
    /// Note this function assumes we already have the key share for the provided section key.
    pub async fn try_update_current_sap(&self, section_key: BlsPublicKey, prefix: &Prefix) -> bool {
        // Let's try to find the signed SAP corresponding to the provided prefix and section key
        match self.prefix_map.get_signed(prefix) {
            Some(signed_sap) if signed_sap.value.section_key() == section_key => {
                let proof = self
                    .all_sections_chains
                    .borrow()
                    .get_proof_chain(&self.genesis_key, &section_key);
                // We have the signed SAP for the provided prefix and section key,
                // we should be able to update our current SAP and section chain
                match proof {
                    Ok(section_chain) => {
                        // Remove any peer which doesn't belong to our new section's prefix
                        self.section_peers.retain(prefix);
                        // Prune list of archived members
                        self.section_peers
                            .prune_members_archive(&section_chain)
                            .await;

                        // Let's then update our current SAP and section chain
                        let our_prev_prefix = self.prefix().await;
                        *self.signed_sap.borrow_mut() = signed_sap.clone();
                        *self.chain.borrow_mut() = section_chain;

                        info!(
                            "Switched our section's SAP ({:?} to {:?}) with new one: {:?}",
                            our_prev_prefix, prefix, signed_sap
                        );

                        true
                    }
                    Err(err) => {
                        trace!(
                            "We couldn't find section chain for {:?} and section key {:?}: {:?}",
                            prefix,
                            section_key,
                            err
                        );
                        false
                    }
                }
            }
            Some(_) | None => {
                trace!(
                    "We yet don't have the signed SAP for {:?} and section key {:?}",
                    prefix,
                    section_key
                );
                false
            }
        }
    }

    /// Given a NodeMsg can we trust it (including verifying contents of an AE message)
    pub fn verify_node_msg_can_be_trusted(
        msg_authority: NodeMsgAuthority,
        msg: SystemMsg,
        known_keys: &[BlsPublicKey],
    ) -> bool {
        if !msg_authority.verify_src_section_key_is_known(known_keys) {
            // In case the incoming message itself is trying to update our knowledge,
            // it shall be allowed.
            if let SystemMsg::AntiEntropyUpdate {
                ref proof_chain, ..
            } = msg
            {
                // The attached chain shall contains a key known to us
                if !proof_chain.check_trust(known_keys) {
                    return false;
                } else {
                    trace!(
                        "Allows AntiEntropyUpdate msg({:?}) ahead of our knowledge",
                        msg,
                    );
                }
            } else {
                return false;
            }
        }
        true
    }

    /// Update our network knowledge if the provided SAP is valid and can be verified
    /// with the provided proof chain.
    /// If the 'update_sap' flag is set to 'true', the provided SAP and chain will be
    /// set as our current.
    pub async fn update_knowledge_if_valid(
        &self,
        signed_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: &SecuredLinkedList,
        updated_members: Option<SectionPeersMsg>,
        our_name: &XorName,
        section_keys_provider: &SectionKeysProvider,
    ) -> Result<bool> {
        let mut there_was_an_update = false;
        let provided_sap = signed_sap.value.clone();

        // Update the network prefix map
        match self.prefix_map.verify_with_chain_and_update(
            signed_sap.clone(),
            proof_chain,
            &self.section_chain().await,
        ) {
            Ok(true) => {
                there_was_an_update = true;
                debug!(
                    "Updated network prefix map with SAP for {:?}",
                    provided_sap.prefix()
                );

                // Join the proof chain to our DAG since it's a new SAP
                // thus it shall extend some branch/chain.
                self.all_sections_chains
                    .borrow_mut()
                    .join(proof_chain.clone())?;

                // and if we are... do we have the key share needed to perform elder duties
                let mut we_have_a_share_of_this_key = false;

                // lets find out if we should be an elder after the change
                let we_are_an_adult = !self.is_elder(our_name).await;

                // check we should not be _becoming_ an elder
                let we_should_become_an_elder = if we_are_an_adult {
                    provided_sap.contains_elder(our_name)
                } else {
                    true
                };

                if !we_are_an_adult || we_should_become_an_elder {
                    we_have_a_share_of_this_key = section_keys_provider
                        .key_share(&signed_sap.section_key())
                        .await
                        .is_ok();
                }

                trace!(
                    "we_are_an_adult: {we_are_an_adult},we_have_a_share_of_this_key: {we_have_a_share_of_this_key}"
                );

                // if we're an adult, we accept the validated sap
                // if we have a keyshare, we're an eder and we shoud continue with this validated sap
                // if we are an elder candidate, only switch to the new sap when have the key share
                let switch_to_new_sap =
                    we_have_a_share_of_this_key || (we_are_an_adult && !we_should_become_an_elder);

                trace!(
                    "update_knowledge_if_valid: will switch_to_new_sap {:?}",
                    switch_to_new_sap
                );

                // if we're not an adult, but we don't have a key share...
                // something is wrong
                if !we_are_an_adult && !we_have_a_share_of_this_key {
                    error!("We should be an elder, but we're missing the keyshare!");
                }

                // We try to update our SAP and own chain only if we were flagged to,
                // otherwise this update could be due to an AE message and we still don't have
                // the key share for the new SAP, making this node unable to sign section messages
                // and possibly being kicked out of the group of Elders.
                if switch_to_new_sap && provided_sap.prefix().matches(our_name) {
                    let our_prev_prefix = self.prefix().await;
                    // Remove any peer which doesn't belong to our new section's prefix
                    self.section_peers.retain(&provided_sap.prefix());
                    info!(
                        "Updated our section's SAP ({:?} to {:?}) with new one: {:?}",
                        our_prev_prefix,
                        provided_sap.prefix(),
                        provided_sap
                    );

                    let section_chain = self
                        .all_sections_chains
                        .borrow()
                        .get_proof_chain(&self.genesis_key, &provided_sap.section_key())?;

                    // Prune list of archived members
                    self.section_peers
                        .prune_members_archive(&section_chain)
                        .await;

                    // Switch to new SAP and chain.
                    *self.signed_sap.borrow_mut() = signed_sap.clone();
                    *self.chain.borrow_mut() = section_chain;
                }
            }
            Ok(false) => {
                debug!(
                    "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
                    provided_sap.prefix(), provided_sap
                );
            }
            Err(err) => {
                debug!(
                    "Anti-Entropy: discarded SAP for {:?} since we failed to update prefix map with: {:?}",
                    provided_sap.prefix(), err
                );
            }
        }

        // Update members if changes were provided
        if let Some(members) = updated_members {
            let peers = members
                .into_iter()
                .map(|member| member.into_authed_state())
                .collect();

            if self.merge_members(peers).await? {
                let prefix = self.prefix().await;
                info!(
                    "Updated our section's members ({:?}): {:?}",
                    prefix, self.section_peers
                );
            }
        }

        Ok(there_was_an_update)
    }

    // Returns reference to network prefix map
    pub fn prefix_map(&self) -> &NetworkPrefixMap {
        &self.prefix_map
    }

    // Returns the section authority provider for the prefix that matches name.
    pub fn section_by_name(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.prefix_map.section_by_name(name)
    }
    // Returns the signed sap
    pub fn signed_sap(&self) -> SectionAuth<SectionAuthorityProvider> {
        self.signed_sap.borrow().clone()
    }

    // Get SectionAuthorityProvider of a known section with the given prefix,
    // along with its section chain.
    pub async fn get_closest_or_opposite_signed_sap(
        &self,
        name: &XorName,
    ) -> Option<(SectionAuth<SectionAuthorityProvider>, SecuredLinkedList)> {
        let closest_sap = self
            .prefix_map
            .closest_or_opposite(name, Some(&self.prefix().await));

        if let Some(signed_sap) = closest_sap {
            if let Ok(proof_chain) = self
                .all_sections_chains
                .borrow()
                .get_proof_chain(&self.genesis_key, &signed_sap.value.section_key())
            {
                return Some((signed_sap, proof_chain));
            }
        }

        None
    }

    // Return the network genesis key
    pub fn genesis_key(&self) -> &bls::PublicKey {
        &self.genesis_key
    }

    // Try to merge this `NetworkKnowledge` members with `peers`.
    pub async fn merge_members(&self, peers: BTreeSet<SectionAuth<NodeState>>) -> Result<bool> {
        let mut there_was_an_update = false;
        let chain = self.section_chain().await;

        for node_state in peers.iter() {
            trace!(
                "Updating section members. Name: {:?}, new state: {:?}",
                node_state.name(),
                node_state.state()
            );
            if !node_state.verify(&chain) {
                error!(
                    "Can't update section member, name: {:?}, new state: {:?}",
                    node_state.name(),
                    node_state.state()
                );
            } else if self.section_peers.update(node_state.clone()) {
                there_was_an_update = true;
            }
        }

        // drop the borrow
        drop(chain);

        self.section_peers.retain(&self.prefix().await);

        Ok(there_was_an_update)
    }

    /// Update the member. Returns whether it actually updated it.
    pub async fn update_member(&self, node_state: SectionAuth<NodeState>) -> bool {
        let node_name = node_state.name();
        trace!(
            "Updating section member state, name: {:?}, new state: {:?}",
            node_name,
            node_state.state()
        );
        let the_chain = self.section_chain().await;
        // let's check the node state is properly signed by one of the keys in our chain
        if !node_state.verify(&the_chain) {
            error!(
                "Can't update section member, name: {:?}, new state: {:?}",
                node_name,
                node_state.state()
            );
            return false;
        }
        let updated = self.section_peers.update(node_state);
        trace!(
            "Section member state, name: {:?}, updated: {}",
            node_name,
            updated
        );
        updated
    }

    /// Return a copy of our section chain
    pub async fn section_chain(&self) -> SecuredLinkedList {
        self.chain.borrow().clone()
    }

    /// Generate a proof chain from the provided key to our current section key
    pub async fn get_proof_chain_to_current(
        &self,
        from_key: &BlsPublicKey,
    ) -> Result<SecuredLinkedList> {
        let our_section_key = self.signed_sap.borrow().section_key();
        let proof_chain = self
            .chain
            .borrow()
            .get_proof_chain(from_key, &our_section_key)?;

        Ok(proof_chain)
    }

    /// Return current section key
    pub async fn section_key(&self) -> bls::PublicKey {
        self.signed_sap.borrow().section_key()
    }

    /// Return current section chain length
    pub async fn chain_len(&self) -> u64 {
        self.chain.borrow().main_branch_len() as u64
    }

    /// Return weather current section chain has the provided key
    pub async fn has_chain_key(&self, key: &bls::PublicKey) -> bool {
        self.chain.borrow().has_key(key)
    }

    /// Return a copy of current SAP
    pub async fn authority_provider(&self) -> SectionAuthorityProvider {
        self.signed_sap.borrow().value.clone()
    }

    /// Return a copy of current SAP with corresponding section authority
    pub async fn section_signed_authority_provider(&self) -> SectionAuth<SectionAuthorityProvider> {
        self.signed_sap.borrow().clone()
    }

    /// Prefix of our section.
    pub async fn prefix(&self) -> Prefix {
        self.signed_sap.borrow().prefix()
    }

    /// Returns the elders of our section
    pub async fn elders(&self) -> Vec<Peer> {
        self.authority_provider().await.elders_vec()
    }

    /// Return whether the name provided belongs to an Elder, by checking if
    /// it is one of the current section's SAP member,
    pub async fn is_elder(&self, name: &XorName) -> bool {
        self.signed_sap.borrow().contains_elder(name)
    }

    /// Returns members that are joined.
    pub async fn section_members(&self) -> BTreeSet<NodeState> {
        self.section_peers
            .members()
            .into_iter()
            .map(|state| state.value)
            .collect()
    }

    /// Returns current list of section signed members.
    pub async fn section_signed_members(&self) -> BTreeSet<SectionAuth<NodeState>> {
        self.section_peers.members()
    }

    /// Returns current section size, i.e. number of peers in the section.
    pub async fn section_size(&self) -> usize {
        self.section_peers.num_of_members()
    }

    /// Returns live adults from our section.
    pub async fn adults(&self) -> Vec<Peer> {
        let mut live_adults = vec![];
        for node_state in self.section_peers.members() {
            if !self.is_elder(&node_state.name()).await {
                live_adults.push(*node_state.peer())
            }
        }
        live_adults
    }

    /// Get info for the member with the given name.
    pub async fn get_section_member(&self, name: &XorName) -> Option<NodeState> {
        self.section_peers.get(name)
    }

    /// Get info for the member with the given name either from current members list,
    /// or from the archive of left/relocated members
    pub async fn is_either_member_or_archived(
        &self,
        name: &XorName,
    ) -> Option<SectionAuth<NodeState>> {
        self.section_peers.is_either_member_or_archived(name)
    }

    /// Get info for the member with the given name.
    pub async fn is_section_member(&self, name: &XorName) -> bool {
        self.section_peers.is_member(name)
    }

    pub async fn find_member_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        self.section_peers
            .members()
            .into_iter()
            .find(|info| info.addr() == *addr)
            .map(|info| *info.peer())
    }
}

// Create `SectionAuthorityProvider` for the first node.
fn create_first_section_authority_provider(
    pk_set: &bls::PublicKeySet,
    sk_share: &bls::SecretKeyShare,
    peer: Peer,
) -> Result<SectionAuth<SectionAuthorityProvider>> {
    let section_auth = SectionAuthorityProvider::new(
        iter::once(peer),
        Prefix::default(),
        [NodeState::joined(peer, None)],
        pk_set.clone(),
        0,
    );
    let sig = create_first_sig(pk_set, sk_share, &section_auth)?;
    Ok(SectionAuth::new(section_auth, sig))
}

fn create_first_sig<T: Serialize>(
    pk_set: &bls::PublicKeySet,
    sk_share: &bls::SecretKeyShare,
    payload: &T,
) -> Result<KeyedSig> {
    let bytes = bincode::serialize(payload).map_err(|_| Error::InvalidPayload)?;
    let signature_share = sk_share.sign(&bytes);
    let signature = pk_set
        .combine_signatures(iter::once((0, &signature_share)))
        .map_err(|_| Error::InvalidSignatureShare)?;

    Ok(KeyedSig {
        public_key: pk_set.public_key(),
        signature,
    })
}

#[cfg(test)]
mod tests {
    use super::supermajority;
    use proptest::prelude::*;

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
}
