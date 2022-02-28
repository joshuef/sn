// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Container that acts as a map whose keys are section Prefixes, and values
//! are the Section Authority Provider of the section.
//!
//! It automatically prunes redundant entries. That is, when the prefix of an entry is fully
//! covered by other prefixes, that entry is removed. For example, when there is entry with
//! prefix (00) and we insert entries with (000) and (001), the (00) prefix becomes fully
//! covered and is automatically removed.
//!

mod stats;

use self::stats::NetworkStats;

use crate::messaging::system::SectionAuth;
use crate::network_knowledge::{Error, Result, SectionAuthUtils, SectionAuthorityProvider};

use bls::PublicKey as BlsPublicKey;
use dashmap::{self, mapref::multiple::RefMulti, DashMap};
use secured_linked_list::SecuredLinkedList;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::iter::{self, Iterator};
use std::sync::Arc;
use xor_name::{Prefix, XorName};

/// Container for storing information about other sections in the network.
#[derive(Debug, Clone, Serialize)]
pub struct NetworkPrefixMap {
    /// Map of sections prefixes to their latest signed section authority providers.
    sections: Arc<DashMap<Prefix, SectionAuth<SectionAuthorityProvider>>>,
    /// The network's genesis public key
    genesis_pk: BlsPublicKey,
}

impl NetworkPrefixMap {
    /// Create an empty container
    pub fn new(genesis_pk: BlsPublicKey) -> Self {
        Self {
            sections: Arc::new(DashMap::new()),
            genesis_pk,
        }
    }

    /// Returns the genesis key of the Network
    pub fn genesis_key(&self) -> BlsPublicKey {
        self.genesis_pk
    }

    /// Inserts new entry into the map. Replaces previous entry at the same prefix.
    /// Removes those ancestors of the inserted prefix that are now fully covered by their
    /// descendants.
    /// Does not insert anything if any descendant of the prefix of `entry` is already present in
    /// the map.
    /// Returns a boolean indicating whether anything changed.
    //
    // This is not a public API since we shall not allow any insert/update without a
    // proof chain, users shall call either `update` or `verify_with_chain_and_update` API.
    fn insert(&self, sap: SectionAuth<SectionAuthorityProvider>) -> bool {
        let prefix = sap.prefix();
        // Don't insert if any descendant is already present in the map.
        if self.descendants(&prefix).next().is_some() {
            return false;
        }

        let _prev = self.sections.insert(prefix, sap);

        let parent_prefix = prefix.popped();
        self.prune(parent_prefix);
        true
    }

    /// Returns the known section that is closest to the given name,
    /// regardless of whether `name` belongs in that section or not.
    /// If provided, it excludes any section matching the passed prefix.
    fn closest(
        &self,
        name: &XorName,
        exclude: Option<&Prefix>,
    ) -> Option<SectionAuth<SectionAuthorityProvider>> {
        self.sections
            .iter()
            .filter(|e| {
                if let Some(prefix) = exclude {
                    e.key() != prefix
                } else {
                    true
                }
            })
            .min_by(|lhs, rhs| lhs.key().cmp_distance(rhs.key(), name))
            .map(|e| e.value().clone())
    }

    /// Returns the known section that is closest to the given name,
    /// regardless of whether `name` belongs in that section or not.
    /// If there are no close matches in remote sections, return a SAP from an opposite prefix.
    /// If provided, it excludes any section matching the passed prefix.
    pub fn closest_or_opposite(
        &self,
        name: &XorName,
        exclude: Option<&Prefix>,
    ) -> Option<SectionAuth<SectionAuthorityProvider>> {
        self.closest(name, exclude).or_else(|| {
            self.sections
                .iter()
                .filter(|e| e.key().matches(&name.with_bit(0, !name.bit(0))))
                .max_by_key(|e| e.key().bit_count())
                .map(|entry| entry.value().clone())
        })
    }

    /// Returns all known sections SAP.
    pub fn all(&self) -> Vec<SectionAuthorityProvider> {
        self.sections
            .iter()
            .map(|e| e.value().value.clone())
            .collect()
    }

    /// Get `SectionAuthorityProvider` of a known section with the given prefix.
    #[allow(unused)]
    pub fn get(&self, prefix: &Prefix) -> Option<SectionAuthorityProvider> {
        self.sections
            .get(prefix)
            .map(|entry| entry.value().value.clone())
    }

    /// Get signed `SectionAuthorityProvider` of a known section with the given prefix.
    pub fn get_signed(
        &self,
        prefix: &Prefix,
    ) -> Option<SectionAuth<SectionAuthorityProvider>> {
        self.sections.get(prefix).map(|entry| entry.value().clone())
    }

    /// Update our knowledge of a remote section's SAP only
    /// if it's verifiable with the provided proof chain and the
    /// currently known SAP we are aware of for the Prefix.
    pub fn update(
        &self,
        signed_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: &SecuredLinkedList,
    ) -> Result<bool> {
        let prefix = signed_sap.prefix();
        trace!("Attempting to update prefixmap for {:?}", prefix);
        let section_key = match self.section_by_prefix(&prefix) {
            Ok(sap) => sap.section_key(),
            Err(_) => {
                trace!("No key found for prefix: {:?}", prefix);
                self.genesis_pk
            }
        };

        let res = self.verify_with_chain_and_update(
            signed_sap,
            proof_chain,
            &SecuredLinkedList::new(section_key),
        );

        for section in self.sections.iter() {
            let prefix = section.key();
            trace!("Known prefix after update: {:?}", prefix);
        }

        res
    }

    /// Update our knowledge of a remote section's SAP only
    /// if it's verifiable with the provided proof chain and section chain.
    /// Returns true if an udpate was made
    pub fn verify_with_chain_and_update(
        &self,
        signed_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: &SecuredLinkedList,
        section_chain: &SecuredLinkedList,
    ) -> Result<bool> {
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

        // We currently don't keep the complete chain of remote sections,
        // **but** the SAPs of remote sections we keep were already verified by us
        // as trusted before we store them in our local records.
        // Thus, we just need to check our knowledge of the remote section's key
        // is part of the proof chain received.
        match self.sections.get(&signed_sap.prefix()) {
            Some(entry) if entry.value() == &signed_sap => {
                // It's the same SAP we are already aware of
                return Ok(false);
            }
            Some(entry) => {
                // We are then aware of the prefix, let's just verify the new SAP can
                // be trusted based on the SAP we aware of and the proof chain provided.
                if !proof_chain.has_key(&entry.value().section_key()) {
                    // This case may happen when both the sender and receiver is about to using
                    // a new SAP. The AE-Update was sent before sender switching to use new SAP,
                    // hence it only contains proof_chain covering the old SAP.
                    // When the update arrives after the receiver got switched to use new SAP,
                    // this error will be complained.
                    // As an outdated node will got updated via AE triggered by other messages,
                    // there is no need to bounce back here (assuming the sender is outdated) to
                    // avoid potential looping.
                    return Err(Error::UntrustedProofChain(format!(
                        "provided proof_chain doesn't cover the SAP's key we currently know: {:?}",
                        entry.value().value
                    )));
                }
            }
            None => {
                // We are not aware of the prefix, let's then verify it can be
                // trusted based on our own section chain and the provided proof chain.
                if !proof_chain.check_trust(section_chain.keys()) {
                    return Err(Error::UntrustedProofChain(format!(
                        "none of the keys were found on our section chain: {:?}",
                        signed_sap.value
                    )));
                }
            }
        }

        // Make sure the proof chain can be trusted,
        // i.e. check each key is signed by its parent/predecesor key.
        if !proof_chain.self_verify() {
            return Err(Error::UntrustedProofChain(format!(
                "invalid proof chain: {:?}",
                proof_chain
            )));
        }

        // Check the SAP's key is the last key of the proof chain
        if proof_chain.last_key() != &signed_sap.section_key() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "section key ({:?}, from prefix {:?}) isn't in the last key in the proof chain provided. (Which ends with ({:?}))",
                signed_sap.section_key(),
                signed_sap.prefix(),
                proof_chain.last_key()
            )));
        }

        // We can now update our knowledge of the remote section's SAP.
        // Note: we don't expect the same SAP to be found in our records
        // for the prefix since we've already checked that above.
        let changed = self.insert(signed_sap);

        for section in self.sections.iter() {
            let prefix = section.key();
            trace!("Known prefix: {:?}", prefix);
        }

        Ok(changed)
    }

    /// Returns the known section public keys.
    pub fn section_keys(&self) -> Vec<bls::PublicKey> {
        self.sections
            .iter()
            .map(|e| e.value().section_key())
            .collect()
    }

    /// Number of SAPs we know about.
    pub fn len(&self) -> usize {
        self.sections.len()
    }

    /// Returns the section authority provider for the prefix that matches `name`.
    pub fn section_by_name(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.sections
            .iter()
            .filter(|e| e.key().matches(name))
            .max_by_key(|e| e.key().bit_count())
            .ok_or(Error::NoMatchingSection)
            .map(|entry| entry.value().value.clone())
    }

    /// Get the section that matches `prefix`. In case of multiple matches, returns the
    /// one with the longest prefix.
    pub fn section_by_prefix(&self, prefix: &Prefix) -> Result<SectionAuthorityProvider> {
        self.section_by_name(&prefix.name())
    }

    /// Get total number of known sections
    pub fn known_sections_count(&self) -> usize {
        self.sections.len()
    }

    /// Returns network statistics.
    pub fn network_stats(&self, our: &SectionAuthorityProvider) -> NetworkStats {
        // Let's compute an estimate of the total number of elders in the network
        // from the size of our routing table.
        let section_prefixes = self.sections.iter().map(|e| *e.key());
        let known_prefixes: Vec<_> = section_prefixes.chain(iter::once(our.prefix())).collect();

        let total_elders_exact = Prefix::default().is_covered_by(&known_prefixes);

        // Estimated fraction of the network that we have in our RT.
        // Computed as the sum of 1 / 2^(prefix.bit_count) for all known section prefixes.
        let network_fraction: f64 = known_prefixes
            .iter()
            .map(|p| 1.0 / (p.bit_count() as f64).exp2())
            .sum();

        let network_elders_count: usize =
            self.sections.iter().map(|e| e.value().elder_count()).sum();
        let total = network_elders_count as f64 / network_fraction;

        // `total_elders_exact` indicates whether `total_elders` is
        // an exact number or an estimate.
        NetworkStats {
            known_elders: network_elders_count as u64,
            total_elders: total.ceil() as u64,
            total_elders_exact,
        }
    }

    // Returns an iterator over all entries whose prefixes
    // are descendants (extensions) of `prefix`.
    fn descendants<'a>(
        &'a self,
        prefix: &'a Prefix,
    ) -> impl Iterator<Item = RefMulti<'a, Prefix, SectionAuth<SectionAuthorityProvider>>> + 'a
    {
        self.sections
            .iter()
            .filter(move |e| e.key().is_extension_of(prefix))
    }

    /// Remove `prefix` and any of its ancestors if they are covered by their descendants.
    /// For example, if `(00)` and `(01)` are both in the map, we can remove `(0)` and `()`.
    fn prune(&self, mut prefix: Prefix) {
        // TODO: can this be optimized?
        loop {
            let is_covered = {
                let descendants: Vec<_> = self.descendants(&prefix).collect();
                let descendant_prefixes: Vec<&Prefix> =
                    descendants.iter().map(|item| item.key()).collect();
                prefix.is_covered_by(descendant_prefixes)
            };
            if is_covered {
                let _prev = self.sections.remove(&prefix);
            }

            if prefix.is_empty() {
                break;
            } else {
                prefix = prefix.popped();
            }
        }
    }
}

impl<'de> Deserialize<'de> for NetworkPrefixMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // This is easier than hand-writing an impl, not sure if there's a downside.
        // Uses the same name in case it shows up in errors messages etc.
        #[derive(Deserialize)]
        struct NetworkPrefixMap {
            sections: DashMap<Prefix, SectionAuth<crate::messaging::SectionAuthorityProvider>>,
            genesis_pk: BlsPublicKey,
        }

        let helper = NetworkPrefixMap::deserialize(deserializer)?;
        let sections = helper
            .sections
            .into_iter()
            .map(|(k, v)| (k, v.into_authed_state()))
            .collect();

        Ok(Self {
            sections: Arc::new(sections),
            genesis_pk: helper.genesis_pk,
        })
    }
}

impl Ord for NetworkPrefixMap {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len().cmp(&other.len())
    }
}

impl PartialOrd for NetworkPrefixMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for NetworkPrefixMap {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
    }
}

impl Eq for NetworkPrefixMap {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{gen_section_authority_provider, section_signed};
    use eyre::{eyre, Context, Result};
    use rand::Rng;

    #[test]
    fn insert_existing_prefix() -> Result<()> {
        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let sap0 = gen_section_auth(p0)?;
        let new_sap0 = gen_section_auth(p0)?;
        assert_ne!(sap0, new_sap0);

        assert!(map.insert(sap0));
        assert!(map.insert(new_sap0.clone()));
        assert_eq!(map.get(&p0), Some(new_sap0.value));

        Ok(())
    }

    #[test]
    fn insert_direct_descendants_of_existing_prefix() -> Result<()> {
        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p00 = prefix("00")?;
        let p01 = prefix("01")?;

        let sap0 = gen_section_auth(p0)?;
        assert!(map.insert(sap0.clone()));

        // Insert the first sibling. Parent remain in the map.
        let sap00 = gen_section_auth(p00)?;
        assert!(map.insert(sap00.clone()));

        assert_eq!(map.get(&p00), Some(sap00.value.clone()));
        assert_eq!(map.get(&p01), None);
        assert_eq!(map.get(&p0), Some(sap0.value));

        // Insert the other sibling. Parent is removed because it is now fully covered by its
        // descendants.
        let sap3 = gen_section_auth(p01)?;
        assert!(map.insert(sap3.clone()));

        assert_eq!(map.get(&p00), Some(sap00.value));
        assert_eq!(map.get(&p01), Some(sap3.value));
        assert_eq!(map.get(&p0), None);

        Ok(())
    }

    #[test]
    fn return_opposite_prefix_if_none_matching() -> Result<()> {
        let mut rng = rand::thread_rng();

        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p1 = prefix("1")?;

        let sap0 = gen_section_auth(p0)?;

        let _changed = map.insert(sap0.clone());

        // There are no matching prefixes, so return Err.
        assert!(map.section_by_name(&p1.substituted_in(rng.gen())).is_err(),);

        // There are no matching prefixes, so return an opposite prefix.
        assert_eq!(
            map.closest_or_opposite(&p1.substituted_in(rng.gen()), None)
                .ok_or(Error::NoMatchingSection)?,
            sap0
        );

        let _changed = map.insert(sap0.clone());
        assert_eq!(
            map.closest_or_opposite(&p1.substituted_in(rng.gen()), None)
                .ok_or(Error::NoMatchingSection)?,
            sap0
        );

        Ok(())
    }

    #[test]
    fn insert_indirect_descendants_of_existing_prefix() -> Result<()> {
        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p000 = prefix("000")?;
        let p001 = prefix("001")?;
        let p00 = prefix("00")?;
        let p01 = prefix("01")?;

        let sap0 = gen_section_auth(p0)?;
        let sap01 = gen_section_auth(p01)?;
        let sap000 = gen_section_auth(p000)?;
        let sap001 = gen_section_auth(p001)?;

        assert!(map.insert(sap0.clone()));

        assert!(map.insert(sap000.clone()));
        assert_eq!(map.get(&p000), Some(sap000.value.clone()));
        assert_eq!(map.get(&p001), None);
        assert_eq!(map.get(&p00), None);
        assert_eq!(map.get(&p01), None);
        assert_eq!(map.get(&p0), Some(sap0.value.clone()));

        assert!(map.insert(sap001.clone()));
        assert_eq!(map.get(&p000), Some(sap000.value.clone()));
        assert_eq!(map.get(&p001), Some(sap001.value.clone()));
        assert_eq!(map.get(&p00), None);
        assert_eq!(map.get(&p01), None);
        assert_eq!(map.get(&p0), Some(sap0.value));

        assert!(map.insert(sap01.clone()));
        assert_eq!(map.get(&p000), Some(sap000.value));
        assert_eq!(map.get(&p001), Some(sap001.value));
        assert_eq!(map.get(&p00), None);
        assert_eq!(map.get(&p01), Some(sap01.value));
        // (0) is now fully covered and so was removed
        assert_eq!(map.get(&p0), None);

        Ok(())
    }

    #[test]
    fn insert_ancestor_of_existing_prefix() -> Result<()> {
        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p00 = prefix("00")?;

        let sap0 = gen_section_auth(p0)?;
        let sap00 = gen_section_auth(p00)?;
        let _changed = map.insert(sap00.clone());

        assert!(!map.insert(sap0));
        assert_eq!(map.get(&p0), None);
        assert_eq!(map.get(&p00), Some(sap00.value));

        Ok(())
    }

    #[test]
    fn get_matching() -> Result<()> {
        let mut rng = rand::thread_rng();

        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p1 = prefix("1")?;
        let p10 = prefix("10")?;

        let sap0 = gen_section_auth(p0)?;
        let sap1 = gen_section_auth(p1)?;
        let sap10 = gen_section_auth(p10)?;

        let _changed = map.insert(sap0.clone());
        let _changed = map.insert(sap1.clone());
        let _changed = map.insert(sap10.clone());

        assert_eq!(
            map.section_by_name(&p0.substituted_in(rng.gen()))?,
            sap0.value
        );

        assert_eq!(
            map.section_by_name(&prefix("11")?.substituted_in(rng.gen()))?,
            sap1.value
        );

        assert_eq!(
            map.section_by_name(&p10.substituted_in(rng.gen()))?,
            sap10.value
        );

        Ok(())
    }

    #[test]
    fn get_matching_prefix() -> Result<()> {
        let (map, _, _) = new_network_prefix_map();
        let p0 = prefix("0")?;
        let p1 = prefix("1")?;
        let p10 = prefix("10")?;

        let sap0 = gen_section_auth(p0)?;
        let sap1 = gen_section_auth(p1)?;
        let sap10 = gen_section_auth(p10)?;

        let _changed = map.insert(sap0.clone());
        let _changed = map.insert(sap1.clone());
        let _changed = map.insert(sap10.clone());

        assert_eq!(map.section_by_prefix(&p0)?, sap0.value);

        assert_eq!(map.section_by_prefix(&prefix("11")?)?, sap1.value);

        assert_eq!(map.section_by_prefix(&p10)?, sap10.value);

        assert_eq!(map.section_by_prefix(&prefix("101")?)?, sap10.value);

        Ok(())
    }

    #[test]
    fn closest() -> Result<()> {
        // Create map containing sections (00), (01) and (10)
        let (map, genesis_sk, genesis_pk) = new_network_prefix_map();
        let chain = SecuredLinkedList::new(genesis_pk);
        let p01 = prefix("01")?;
        let p10 = prefix("10")?;
        let p11 = prefix("11")?;

        let mut chain01 = chain.clone();
        let section_auth_01 = gen_section_auth(p01)?;
        let pk01 = section_auth_01.section_key();
        let sig01 = bincode::serialize(&pk01).map(|bytes| genesis_sk.sign(&bytes))?;
        chain01.insert(&genesis_pk, pk01, sig01)?;
        let _updated = map.verify_with_chain_and_update(section_auth_01, &chain01, &chain);

        let mut chain10 = chain.clone();
        let section_auth_10 = gen_section_auth(p10)?;
        let pk10 = section_auth_10.section_key();
        let sig10 = bincode::serialize(&pk10).map(|bytes| genesis_sk.sign(&bytes))?;
        chain10.insert(&genesis_pk, pk10, sig10)?;
        let _updated = map.verify_with_chain_and_update(section_auth_10, &chain10, &chain);

        let mut rng = rand::thread_rng();
        let n01 = p01.substituted_in(rng.gen());
        let n10 = p10.substituted_in(rng.gen());
        let n11 = p11.substituted_in(rng.gen());

        assert_eq!(map.closest(&n01, None).map(|sap| sap.prefix()), Some(p01));
        assert_eq!(map.closest(&n10, None).map(|sap| sap.prefix()), Some(p10));
        assert_eq!(map.closest(&n11, None).map(|sap| sap.prefix()), Some(p10));

        Ok(())
    }

    // Test helpers

    fn prefix(s: &str) -> Result<Prefix> {
        s.parse()
            .map_err(|err| eyre!("failed to parse Prefix '{}': {}", s, err))
    }

    fn gen_section_auth(prefix: Prefix) -> Result<SectionAuth<SectionAuthorityProvider>> {
        let (section_auth, _, secret_key_set) = gen_section_authority_provider(prefix, 5);
        section_signed(secret_key_set.secret_key(), section_auth)
            .context(format!("Failed to generate SAP for prefix {:?}", prefix))
    }

    fn new_network_prefix_map() -> (NetworkPrefixMap, bls::SecretKey, BlsPublicKey) {
        let genesis_sk = bls::SecretKey::random();
        let genesis_pk = genesis_sk.public_key();

        let map = NetworkPrefixMap::new(genesis_pk);

        (map, genesis_sk, genesis_pk)
    }
}
