// Copyright 2023 MaidSafe.net limited.
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

use crate::messaging::system::SectionSigned;
use crate::network_knowledge::{
    Error, Result, SectionAuthUtils, SectionAuthorityProvider, SectionsDAG,
};

use bls::PublicKey as BlsPublicKey;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    io::Write,
    iter::{self, Iterator},
    path::Path,
};
use tempfile::NamedTempFile;
use tokio::{fs, io::AsyncReadExt};
use xor_name::{Prefix, XorName};

/// Container for storing information about other sections in the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectionTree {
    /// Map of sections prefixes to their latest signed section authority providers.
    sections: BTreeMap<Prefix, SectionSigned<SectionAuthorityProvider>>,
    /// A DAG containing all section chains of the whole network that we are aware of
    sections_dag: SectionsDAG,
}

impl SectionTree {
    /// Create a `SectionTree` with just the genesis section
    pub fn new(genesis_sap: SectionSigned<SectionAuthorityProvider>) -> Result<Self> {
        if genesis_sap.prefix() != Prefix::default() {
            return Err(Error::NonGenesisSap);
        }

        let genesis_pk = genesis_sap.public_key_set().public_key();
        Ok(Self {
            sections: BTreeMap::from([(Prefix::default(), genesis_sap)]),
            sections_dag: SectionsDAG::new(genesis_pk),
        })
    }

    /// Create a new SectionTree deserialised from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|err| Error::Deserialisation(err.to_string()))
    }

    /// Create a new SectionTree deserialised from a file
    pub async fn from_disk(path: &Path) -> Result<Self> {
        let mut section_tree_file = fs::File::open(path).await.map_err(|err| {
            Error::FileHandling(format!(
                "Error opening SectionTree file from {}: {:?}",
                path.display(),
                err
            ))
        })?;

        let mut section_tree_content = vec![];
        let _ = section_tree_file
            .read_to_end(&mut section_tree_content)
            .await
            .map_err(|err| {
                Error::FileHandling(format!(
                    "Error reading SectionTree from {}: {:?}",
                    path.display(),
                    err
                ))
            })?;

        Self::from_bytes(&section_tree_content)
    }

    /// Returns the genesis key of the Network
    pub fn genesis_key(&self) -> &BlsPublicKey {
        self.sections_dag.genesis_key()
    }

    pub fn get_sections_dag(&self) -> &SectionsDAG {
        &self.sections_dag
    }

    pub fn prefixes(&self) -> impl Iterator<Item = &Prefix> {
        self.sections.keys()
    }

    /// Returns the known section that is closest to the given name,
    /// regardless of whether `name` belongs in that section or not.
    /// If provided, it excludes any section matching the passed prefix.
    pub fn closest(
        &self,
        name: &XorName,
        exclude: Option<&Prefix>,
    ) -> Option<&SectionSigned<SectionAuthorityProvider>> {
        self.sections
            .iter()
            .filter(|&(prefix, _)| Some(prefix) != exclude)
            .min_by(|&(prefix_lhs, _), &(prefix_rhs, _)| prefix_lhs.cmp_distance(prefix_rhs, name))
            .map(|(_, sap)| sap)
    }

    /// Returns all known sections SAP.
    pub fn all(&self) -> impl Iterator<Item = &SectionAuthorityProvider> {
        self.sections.values().map(|sap| &sap.value)
    }

    /// Get `SectionAuthorityProvider` of a known section with the given prefix.
    #[allow(unused)]
    pub fn get(&self, prefix: &Prefix) -> Option<SectionAuthorityProvider> {
        self.get_signed(prefix).map(|sap| sap.value.clone())
    }

    /// Get signed `SectionAuthorityProvider` of a known section with the given prefix.
    pub fn get_signed(&self, prefix: &Prefix) -> Option<&SectionSigned<SectionAuthorityProvider>> {
        self.sections.get(prefix)
    }

    /// Get signed `SectionAuthorityProvider` of a known section with the given section key.
    pub fn get_signed_by_key(
        &self,
        section_key: &bls::PublicKey,
    ) -> Option<&SectionSigned<SectionAuthorityProvider>> {
        self.sections
            .values()
            .find(|&signed_sap| signed_sap.public_key_set().public_key() == *section_key)
    }

    /// Returns the section authority provider for the prefix that matches `name`.
    /// In case there is no prefix matches the `name`, we shall return the one with longest
    /// common bits. i.e. for the name of `00xxx`, if we have `01` and `1`, then we shall return
    /// with `01`.
    pub fn get_signed_by_name(
        &self,
        name: &XorName,
    ) -> Result<SectionSigned<SectionAuthorityProvider>> {
        self.sections
            .iter()
            .max_by_key(|&(prefix, _)| prefix.common_prefix(name))
            .ok_or(Error::NoMatchingSection)
            .map(|(_, sap)| sap.clone())
    }

    /// Get the section that matches `prefix`. In case of multiple matches, returns the
    /// one with the longest prefix.
    pub fn get_signed_by_prefix(
        &self,
        prefix: &Prefix,
    ) -> Result<SectionSigned<SectionAuthorityProvider>> {
        self.get_signed_by_name(&prefix.name())
    }

    /// Returns the known section public keys.
    pub fn section_keys(&self) -> Vec<bls::PublicKey> {
        self.sections
            .values()
            .map(|sap| sap.section_key())
            .collect()
    }

    /// Number of SAPs we know about.
    pub fn len(&self) -> usize {
        self.sections.len()
    }

    /// Is the section tree empty?
    pub fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }

    /// Get total number of known sections
    pub fn known_sections_count(&self) -> usize {
        self.sections.len()
    }

    pub fn generate_section_tree_update(&self, prefix: &Prefix) -> Result<SectionTreeUpdate> {
        let signed_sap = self
            .sections
            .get(prefix)
            .ok_or(Error::NoMatchingSection)?
            .clone();

        let proof_chain = self
            .sections_dag
            .partial_dag(self.sections_dag.genesis_key(), &signed_sap.section_key())?;

        Ok(SectionTreeUpdate {
            signed_sap,
            proof_chain,
        })
    }

    /// Update our `SectionTree` if the provided update can be verified
    /// Returns true if an update was made
    pub fn update_the_section_tree(
        &mut self,
        section_tree_update: SectionTreeUpdate,
    ) -> Result<bool> {
        let signed_sap = section_tree_update.signed_sap;
        let proof_chain = section_tree_update.proof_chain;

        if self.sections_dag.has_key(&signed_sap.value.section_key()) {
            info!(
                "Dropping SectionTree update as already have the incoming SAP {:?}",
                signed_sap.value
            );
            return Ok(false);
        }

        let incoming_prefix = &signed_sap.prefix();
        if let Some(sap) = self.get_signed(incoming_prefix) {
            let current_sap_elder_count = sap.elder_count();
            let proposed_sap_elder_count = signed_sap.elder_count();
            if proposed_sap_elder_count < current_sap_elder_count {
                warn!("Proposed SAP elder count is LESS than current...\
                proposed: {proposed_sap_elder_count:?}, current: {current_sap_elder_count:?} (proposed is: {signed_sap:?})");
            }

            // Verify the new SAP with the SAP we know and the proof chain provided.
            if !proof_chain.has_key(&sap.section_key()) {
                // This case may happen when both the sender and receiver is about to using
                // a new SAP. The AE-Update was sent before sender switching to use new SAP,
                // hence it only contains proof_chain covering the old SAP.
                // When the update arrives after the receiver got switched to use new SAP,
                // this error will be complained.
                // As an outdated node will got updated via AE triggered by other messages,
                // there is no need to bounce back here (assuming the sender is outdated) to
                // avoid potential looping.
                return Err(Error::SAPKeyNotCoveredByProofChain(format!(
                    "{proof_chain:?}, {:?}",
                    sap.value
                )));
            }
        } else {
            warn!("Could not find related section to {incoming_prefix:?} in order to validate SAP's section is not shrinking");
            // We are not aware of the prefix, let's then verify it can be
            // trusted based on our own sections_dag and the provided proof chain.
            if !proof_chain.check_trust(self.sections_dag.keys()) {
                return Err(Error::UntrustedProofChain(format!(
                    "None of the keys were found on our section chain: {:?}",
                    signed_sap.value
                )));
            }
        }

        // Check if SAP signature is valid
        if !signed_sap.self_verify() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "Invalid signature: {:?}",
                signed_sap.value
            )));
        }
        // Check if SAP's section key matches SAP signature's key
        if signed_sap.sig.public_key != signed_sap.section_key() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "Section key does not match signature's key: {:?}",
                signed_sap.value
            )));
        }
        // SAP's key should be the last key of the proof chain
        if proof_chain.last_key()? != signed_sap.section_key() {
            return Err(Error::UntrustedProofChain(format!(
                "Provided section key ({:?}, from prefix {:?}) is not the last key of the proof chain",
                signed_sap.section_key(),
                signed_sap.prefix(),
            )));
        }

        // We can now update our knowledge of the remote section's SAP.
        // Note: we don't expect the same SAP to be found in our records
        // for the prefix since we've already checked that above.
        if self.insert(signed_sap) {
            // update our sections_dag with the proof chain. Cannot be an error, since in cases
            // where we have outdated SAP (aware of prefix)/ not aware of the prefix, we have the
            // proof chain's genesis key in our sections_dag.
            self.sections_dag.merge(proof_chain)?;
            for (prefix, section_key) in &self.sections {
                debug!("Known prefix, section_key after update: {prefix:?} = {section_key:?}");
            }
            debug!("updated sections_dag: {:?}", self.sections_dag);
            Ok(true)
        } else {
            debug!("SectionTree not updated");
            Ok(false)
        }
    }

    /// For testing purpose, we may need to populate a `section_tree` without a proof chain.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn insert_without_chain(&mut self, sap: SectionSigned<SectionAuthorityProvider>) -> bool {
        self.insert(sap)
    }

    /// Serialise and write it to disk on the provided file path
    pub async fn write_to_disk(&self, path: &Path) -> Result<()> {
        trace!("Writing section tree to disk at {}", path.display());
        let parent_path = if let Some(parent_path) = path.parent() {
            fs::create_dir_all(parent_path).await.map_err(|err| {
                Error::DirectoryHandling(format!(
                    "Could not create '{}' parent directory path: {}",
                    path.display(),
                    err,
                ))
            })?;
            parent_path
        } else {
            Path::new(".")
        };

        let mut temp_file = NamedTempFile::new_in(parent_path).map_err(|e| {
            Error::FileHandling(format!(
                "Error creating tempfile at {}: {:?}",
                parent_path.display(),
                e
            ))
        })?;

        let serialized =
            serde_json::to_vec(self).map_err(|e| Error::Serialisation(e.to_string()))?;

        temp_file.write_all(serialized.as_slice()).map_err(|e| {
            Error::FileHandling(format!(
                "Error writing tempfile at {}: {:?}",
                temp_file.path().display(),
                e
            ))
        })?;

        fs::rename(temp_file.path(), &path).await.map_err(|e| {
            Error::FileHandling(format!(
                "Error renaming tempfile from {} to {}: {:?}",
                temp_file.path().display(),
                path.display(),
                e
            ))
        })?;

        trace!("Wrote SectionTree to disk: {}", path.display());

        Ok(())
    }

    /// Returns network statistics.
    pub fn network_stats(&self, our: &SectionAuthorityProvider) -> NetworkStats {
        // Let's compute an estimate of the total number of elders in the network
        // from the size of our routing table.
        let section_prefixes = self.sections.keys().copied();
        let known_prefixes: Vec<_> = section_prefixes.chain(iter::once(our.prefix())).collect();

        let total_elders_exact = Prefix::default().is_covered_by(&known_prefixes);

        // Estimated fraction of the network that we have in our RT.
        // Computed as the sum of 1 / 2^(prefix.bit_count) for all known section prefixes.
        let network_fraction: f64 = known_prefixes
            .iter()
            .map(|p| 1.0 / (p.bit_count() as f64).exp2())
            .sum();

        let network_elders_count: usize = self.sections.values().map(|sap| sap.elder_count()).sum();
        let total = network_elders_count as f64 / network_fraction;

        // `total_elders_exact` indicates whether `total_elders` is
        // an exact number or an estimate.
        NetworkStats {
            known_elders: network_elders_count as u64,
            total_elders: total.ceil() as u64,
            total_elders_exact,
        }
    }

    /// Remove `prefix` and any of its ancestors.
    fn prune(&mut self, mut prefix: Prefix) {
        loop {
            let _prev = self.sections.remove(&prefix);

            if prefix.is_empty() {
                break;
            } else {
                prefix = prefix.popped();
            }
        }
    }

    /// Inserts new entry into the map. Replaces previous entry at the same prefix.
    /// Removes those ancestors of the inserted prefix.
    /// Does not insert anything if any descendant of the prefix of `entry` is already present in
    /// the map.
    /// Returns a boolean indicating whether anything changed.
    //
    // This is not a public API since we shall not allow any insert/update without a
    // proof chain, users shall call the `update` API.
    fn insert(&mut self, sap: SectionSigned<SectionAuthorityProvider>) -> bool {
        let prefix = sap.prefix();
        // Don't insert if any descendant is already present in the map.
        if let Some(extension_p) = self.sections.keys().find(|p| p.is_extension_of(&prefix)) {
            info!("Dropping update since we have a prefix '{extension_p}' that is an extension of '{prefix}'");
            return false;
        }

        let _prev = self.sections.insert(prefix, sap);

        if prefix.is_empty() {
            return true;
        }
        let parent_prefix = prefix.popped();

        self.prune(parent_prefix);
        true
    }
}

impl Ord for SectionTree {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len().cmp(&other.len())
    }
}

impl PartialOrd for SectionTree {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SectionTree {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
    }
}

impl Eq for SectionTree {}

/// The update to our `NetworkKnowledge` containing the section's `SectionAuthorityProvider` signed
/// by the section and the proof chain to validate the it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SectionTreeUpdate {
    pub signed_sap: SectionSigned<SectionAuthorityProvider>,
    pub proof_chain: SectionsDAG,
}

impl SectionTreeUpdate {
    pub fn new(
        signed_sap: SectionSigned<SectionAuthorityProvider>,
        proof_chain: SectionsDAG,
    ) -> Self {
        Self {
            signed_sap,
            proof_chain,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils {

    use super::{SectionTree, SectionTreeUpdate, SectionsDAG};
    use crate::{
        messaging::system::SectionSigned,
        test_utils::{TestKeys, TestSapBuilder},
        SectionAuthorityProvider,
    };
    use xor_name::Prefix;

    /// `SectionTree` related utils for testing
    pub struct TestSectionTree {}

    impl TestSectionTree {
        /// Get a random `SectionTree`
        pub fn random_tree() -> (SectionTree, bls::SecretKey) {
            let (gen_sap, gen_sk_set, ..) = TestSapBuilder::new(Prefix::default()).build();
            let gen_sap = TestKeys::get_section_signed(&gen_sk_set.secret_key(), gen_sap);
            let tree = SectionTree::new(gen_sap).expect("SAP belongs to the genesis prefix");

            (tree, gen_sk_set.secret_key())
        }

        /// Generate a `SectionTreeUpdate` where the SAP's section key is appended to the proof chain
        pub fn get_section_tree_update(
            sap: &SectionSigned<SectionAuthorityProvider>,
            proof_chain: &SectionsDAG,
            parent_sk: &bls::SecretKey,
        ) -> SectionTreeUpdate {
            let signed_key = TestKeys::get_section_signed(parent_sk, sap.section_key());
            let mut proof_chain = proof_chain.clone();
            proof_chain
                .verify_and_insert(
                    &parent_sk.public_key(),
                    signed_key.value,
                    signed_key.sig.signature,
                )
                .expect("Failed to insert into proof_chain");
            SectionTreeUpdate::new(sap.clone(), proof_chain)
        }

        /// Generate a proof chain from the provided `genesis_key` followed by all keys provided in `other_keys`
        pub fn gen_proof_chain(
            genesis_key: &bls::SecretKey,
            other_keys: &Vec<bls::SecretKey>,
        ) -> SectionsDAG {
            let mut proof_chain = SectionsDAG::new(genesis_key.public_key());
            let mut parent = genesis_key.clone();
            for key in other_keys {
                let sig = parent.sign(key.public_key().to_bytes());
                proof_chain
                    .verify_and_insert(&parent.public_key(), key.public_key(), sig)
                    .expect("Failed to insert into proof_chain");
                parent = key.clone();
            }
            proof_chain
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        network_knowledge::sections_dag::tests::arb_sections_dag_and_proof_chains,
        test_utils::{assert_lists, prefix, TestKeys, TestSapBuilder, TestSectionTree},
    };
    use eyre::Result;
    use proptest::{prelude::ProptestConfig, prop_assert_eq, proptest};

    #[test]
    fn insert_existing_prefix() {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let (sap0, _) = random_signed_sap(p0);
        let (new_sap0, _) = random_signed_sap(p0);
        assert_ne!(sap0, new_sap0);

        assert!(tree.insert(sap0));
        assert!(tree.insert(new_sap0.clone()));
        assert_eq!(tree.get(&p0), Some(new_sap0.value));
    }

    #[test]
    fn insert_direct_descendants_of_existing_prefix() {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let p00 = prefix("00");
        let p01 = prefix("01");

        let (sap0, _) = random_signed_sap(p0);
        assert!(tree.insert(sap0));

        // Insert the first sibling. Parent get pruned in the map.
        let (sap00, _) = random_signed_sap(p00);
        assert!(tree.insert(sap00.clone()));

        assert_eq!(tree.get(&p00), Some(sap00.value.clone()));
        assert_eq!(tree.get(&p01), None);
        assert_eq!(tree.get(&p0), None);

        // Insert the other sibling.
        let (sap3, _) = random_signed_sap(p01);
        assert!(tree.insert(sap3.clone()));

        assert_eq!(tree.get(&p00), Some(sap00.value));
        assert_eq!(tree.get(&p01), Some(sap3.value));
        assert_eq!(tree.get(&p0), None);
    }

    #[test]
    fn return_opposite_prefix_if_none_matching() -> Result<()> {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let p1 = prefix("1");

        let (sap0, _) = random_signed_sap(p0);

        let _changed = tree.insert(sap0.clone());

        assert_eq!(
            tree.get_signed_by_name(&p1.substituted_in(xor_name::rand::random()))?,
            sap0
        );

        // There are no matching prefixes, so return an opposite prefix.
        assert_eq!(
            tree.closest(&p1.substituted_in(xor_name::rand::random()), None)
                .ok_or(Error::NoMatchingSection)?,
            &sap0
        );

        let _changed = tree.insert(sap0.clone());
        assert_eq!(
            tree.closest(&p1.substituted_in(xor_name::rand::random()), None)
                .ok_or(Error::NoMatchingSection)?,
            &sap0
        );

        Ok(())
    }

    #[test]
    fn insert_indirect_descendants_of_existing_prefix() {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let p000 = prefix("000");
        let p001 = prefix("001");
        let p00 = prefix("00");
        let p01 = prefix("01");

        let (sap0, _) = random_signed_sap(p0);
        let (sap01, _) = random_signed_sap(p01);
        let (sap000, _) = random_signed_sap(p000);
        let (sap001, _) = random_signed_sap(p001);

        assert!(tree.insert(sap0));

        assert!(tree.insert(sap000.clone()));
        assert_eq!(tree.get(&p000), Some(sap000.value.clone()));
        assert_eq!(tree.get(&p001), None);
        assert_eq!(tree.get(&p00), None);
        assert_eq!(tree.get(&p01), None);
        assert_eq!(tree.get(&p0), None);

        assert!(tree.insert(sap001.clone()));
        assert_eq!(tree.get(&p000), Some(sap000.value.clone()));
        assert_eq!(tree.get(&p001), Some(sap001.value.clone()));
        assert_eq!(tree.get(&p00), None);
        assert_eq!(tree.get(&p01), None);
        assert_eq!(tree.get(&p0), None);

        assert!(tree.insert(sap01.clone()));
        assert_eq!(tree.get(&p000), Some(sap000.value));
        assert_eq!(tree.get(&p001), Some(sap001.value));
        assert_eq!(tree.get(&p00), None);
        assert_eq!(tree.get(&p01), Some(sap01.value));
        assert_eq!(tree.get(&p0), None);
    }

    #[test]
    fn insert_ancestor_of_existing_prefix() {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let p00 = prefix("00");

        let (sap0, _) = random_signed_sap(p0);
        let (sap00, _) = random_signed_sap(p00);
        let _changed = tree.insert(sap00.clone());

        assert!(!tree.insert(sap0));
        assert_eq!(tree.get(&p0), None);
        assert_eq!(tree.get(&p00), Some(sap00.value));
    }

    #[test]
    fn get_matching() -> Result<()> {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p = prefix("");
        let p0 = prefix("0");
        let p1 = prefix("1");
        let p10 = prefix("10");

        let (sap, _) = random_signed_sap(p);
        let (sap0, _) = random_signed_sap(p0);
        let (sap1, _) = random_signed_sap(p1);
        let (sap10, _) = random_signed_sap(p10);

        let _changed = tree.insert(sap.clone());

        assert_eq!(
            tree.get_signed_by_name(&p0.substituted_in(xor_name::rand::random()))?,
            sap
        );

        let _changed = tree.insert(sap0.clone());

        assert_eq!(
            tree.get_signed_by_name(&p1.substituted_in(xor_name::rand::random()))?,
            sap0
        );

        let _changed = tree.insert(sap1);
        let _changed = tree.insert(sap10.clone());

        assert_eq!(
            tree.get_signed_by_name(&p0.substituted_in(xor_name::rand::random()))?,
            sap0
        );

        // sap1 get pruned once sap10 inserted.
        assert_eq!(
            tree.get_signed_by_name(&prefix("11").substituted_in(xor_name::rand::random()))?,
            sap10
        );

        assert_eq!(
            tree.get_signed_by_name(&p10.substituted_in(xor_name::rand::random()))?,
            sap10
        );

        Ok(())
    }

    #[test]
    fn get_matching_prefix() -> Result<()> {
        let (mut tree, _) = TestSectionTree::random_tree();
        let p0 = prefix("0");
        let p1 = prefix("1");
        let p10 = prefix("10");

        let (sap0, _) = random_signed_sap(p0);
        let (sap1, _) = random_signed_sap(p1);
        let (sap10, _) = random_signed_sap(p10);

        let _changed = tree.insert(sap0.clone());
        let _changed = tree.insert(sap1);
        let _changed = tree.insert(sap10.clone());

        assert_eq!(tree.get_signed_by_prefix(&p0)?, sap0);

        // sap1 get pruned once sap10 inserted.
        assert_eq!(tree.get_signed_by_prefix(&prefix("11"))?, sap10);

        assert_eq!(tree.get_signed_by_prefix(&p10)?, sap10);

        assert_eq!(tree.get_signed_by_prefix(&prefix("101"))?, sap10);

        Ok(())
    }

    #[test]
    fn closest() -> Result<()> {
        // Create map containing sections (00), (01) and (10)
        let (mut tree, genesis_sk) = TestSectionTree::random_tree();
        let dag = SectionsDAG::new(genesis_sk.public_key());
        let p01 = prefix("01");
        let p10 = prefix("10");
        let p11 = prefix("11");

        let (sap01, _) = random_signed_sap(p01);
        let section_tree_update =
            TestSectionTree::get_section_tree_update(&sap01, &dag, &genesis_sk);
        assert!(tree.update_the_section_tree(section_tree_update)?);

        let (sap10, _) = random_signed_sap(p10);
        let section_tree_update =
            TestSectionTree::get_section_tree_update(&sap10, &dag, &genesis_sk);
        assert!(tree.update_the_section_tree(section_tree_update)?);

        let n01 = p01.substituted_in(xor_name::rand::random());
        let n10 = p10.substituted_in(xor_name::rand::random());
        let n11 = p11.substituted_in(xor_name::rand::random());

        assert_eq!(tree.closest(&n01, None).map(|sap| sap.prefix()), Some(p01));
        assert_eq!(tree.closest(&n10, None).map(|sap| sap.prefix()), Some(p10));
        assert_eq!(tree.closest(&n11, None).map(|sap| sap.prefix()), Some(p10));

        Ok(())
    }

    #[test]
    fn proof_chain_should_contain_a_single_branch_during_update() -> Result<()> {
        let (mut tree, genesis_sk) = TestSectionTree::random_tree();

        let (sap0, _) = random_signed_sap(prefix("0"));
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap0, tree.get_sections_dag(), &genesis_sk);
        assert!(tree.update_the_section_tree(tree_update)?);

        let (sap1, _) = random_signed_sap(prefix("1"));
        // instead of constructing a proof_chain from gen -> 1; we also include the branch '0'
        // which will result in an error while updating the SectionTree
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap1, tree.get_sections_dag(), &genesis_sk);
        assert!(matches!(
            tree.update_the_section_tree(tree_update),
            Err(Error::MultipleBranchError)
        ));

        Ok(())
    }

    #[test]
    fn updating_with_same_sap_should_return_false() -> Result<()> {
        let (mut tree, genesis_sk) = TestSectionTree::random_tree();

        // node updated with sap0
        let (sap0, _) = random_signed_sap(prefix("0"));
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap0, tree.get_sections_dag(), &genesis_sk);
        let tree_update_same = tree_update.clone();
        assert!(tree.update_the_section_tree(tree_update)?);

        // node tries to call update with the same information
        assert!(!tree.update_the_section_tree(tree_update_same)?);

        Ok(())
    }

    #[test]
    fn sap_with_same_parent_and_prefix_should_result_in_error_during_update() -> Result<()> {
        let (mut tree, genesis_sk) = TestSectionTree::random_tree();

        // node updated with sap0
        let (sap0, _) = random_signed_sap(prefix("0"));
        let proof_chain = tree.get_sections_dag().clone();
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap0, &proof_chain, &genesis_sk);
        assert!(tree.update_the_section_tree(tree_update)?);

        // node tries to update with sap signed by same parent for the same prefix
        let (sap0_same, _) = random_signed_sap(prefix("0"));
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap0_same, &proof_chain, &genesis_sk);
        assert!(matches!(
            tree.update_the_section_tree(tree_update),
            Err(Error::SAPKeyNotCoveredByProofChain(_))
        ));

        Ok(())
    }

    #[test]
    fn outdated_sap_result_in_no_update() -> Result<()> {
        let (mut tree, genesis_sk) = TestSectionTree::random_tree();

        // node updated with sap0
        let (sap0, sk0) = random_signed_sap(prefix("0"));
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap0, tree.get_sections_dag(), &genesis_sk);
        let tree_update_outdated = tree_update.clone();
        assert!(tree.update_the_section_tree(tree_update)?);

        // node updated with sap1 with same prefix
        let (sap1, _) = random_signed_sap(prefix("0"));
        let tree_update =
            TestSectionTree::get_section_tree_update(&sap1, tree.get_sections_dag(), &sk0);
        assert!(tree.update_the_section_tree(tree_update)?);

        // node receives an outdated AE update for sap0
        assert!(matches!(
            tree.update_the_section_tree(tree_update_outdated),
            Ok(false)
        ));

        Ok(())
    }

    // Proptest which updates the `SectionTree` using randomized length/order of proof_chain. Error cases, no update cases
    // are ignored, i.e., each update results in a new SAP being added. At the end of each update verify that the
    // leaves of `SectionTree::sections_dag` are the keys of all the `SectionTree::sections` (SAPs). After all the
    // updates, verify that we got back the original `SectionsDAG`
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 100, .. ProptestConfig::default()
        })]
        #[test]
        #[allow(clippy::unwrap_used)]
        fn proptest_section_tree_fields_should_stay_in_sync((genesis_sap, main_dag, list_of_proof_chains) in arb_sections_dag_and_proof_chains(100, true)) {
            let mut section_tree = SectionTree::new(genesis_sap).expect("SAP belongs to the genesis prefix");
            for (proof_chain, sap) in &list_of_proof_chains {
                let tree_update = SectionTreeUpdate::new(sap.clone(), proof_chain.clone());
                assert!(section_tree.update_the_section_tree(tree_update)?);
                // The `sections` are supposed to hold the SAP of the `sections_dag`'s leaves. Verify it
                assert_lists(
                    section_tree.sections.values().map(|sap| sap.section_key()),
                    section_tree.sections_dag.leaf_keys()
                );
            }
            assert_lists(
                section_tree.sections.values().map(|sap| sap.section_key()),
                section_tree.sections_dag.leaf_keys()
            );
            // Finally, verify that we got the main_dag back
            prop_assert_eq!(main_dag, section_tree.sections_dag);
        }
    }

    /// Test helper
    fn random_signed_sap(
        prefix: Prefix,
    ) -> (SectionSigned<SectionAuthorityProvider>, bls::SecretKey) {
        let (sap, sk, ..) = TestSapBuilder::new(prefix)
            .elder_count(0)
            .adult_count(5)
            .build();
        let sap = TestKeys::get_section_signed(&sk.secret_key(), sap);
        (sap, sk.secret_key())
    }
}
