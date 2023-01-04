// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![allow(dead_code)]

use super::errors::{Error, Result};

use crdts::{
    merkle_reg::{Hash, MerkleReg, Node, Sha3Hash},
    CmRDT,
};
use serde::{de::Error as DeserializationError, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Formatter},
    iter, mem,
};
use tiny_keccak::{Hasher, Sha3};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
struct SectionInfo {
    key: bls::PublicKey,
    sig: bls::Signature,
}

impl Debug for SectionInfo {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let bytes: Vec<u8> = self
            .key
            .to_bytes()
            .into_iter()
            .chain(self.sig.to_bytes().into_iter())
            .collect();
        let hex = hex::encode(bytes);
        let hex: String = hex.chars().into_iter().take(10).collect();
        write!(formatter, "SectionInfo({})", hex)
    }
}

impl Sha3Hash for SectionInfo {
    fn hash(&self, hasher: &mut Sha3) {
        hasher.update(&self.key.to_bytes());
        hasher.update(&self.sig.to_bytes());
    }
}

/// A Merkle DAG of BLS keys where every key is signed by its parent key, except the genesis one.
#[derive(Clone, PartialEq, Eq)]
pub struct SectionsDAG {
    genesis_key: bls::PublicKey,
    dag: MerkleReg<SectionInfo>,
    dag_root: BTreeSet<bls::PublicKey>,
    hashes: BTreeMap<bls::PublicKey, Hash>,
}

#[derive(Deserialize, Serialize)]
struct Intermediate {
    genesis_key: bls::PublicKey,
    sections: Vec<(bls::PublicKey, SectionInfo)>,
}

impl Serialize for SectionsDAG {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sections: Vec<(bls::PublicKey, SectionInfo)> = Vec::new();
        let mut already_visited: BTreeSet<bls::PublicKey> = BTreeSet::new();

        for leaf in self.non_genesis_leaf_nodes() {
            let mut node = leaf;
            let mut ancestors: Vec<(bls::PublicKey, SectionInfo)> = Vec::new();
            loop {
                // skip to the next leaf since we have visited the current node and its ancestors
                if already_visited.contains(&node.value.key) {
                    break;
                }
                let parent = match self.parent_node(node.hash()) {
                    Some(parent) => parent,
                    None => {
                        ancestors.push((self.genesis_key, node.value.clone()));
                        break;
                    }
                };
                ancestors.push((parent.value.key, node.value.clone()));
                node = parent
            }

            ancestors.into_iter().rev().for_each(|section| {
                already_visited.insert(section.1.key);
                sections.push(section);
            })
        }
        let inter = Intermediate {
            genesis_key: self.genesis_key,
            sections,
        };
        inter.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SectionsDAG {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inter: Intermediate = Deserialize::deserialize(deserializer)?;
        let mut dag = SectionsDAG::new(inter.genesis_key);
        for (parent, info) in inter.sections {
            dag.verify_and_insert(&parent, info.key, info.sig)
                .map_err(D::Error::custom)?;
        }
        Ok(dag)
    }
}

impl Debug for SectionsDAG {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let keys: Vec<_> = self.keys().collect();
        let key_positions: BTreeMap<&bls::PublicKey, usize> = keys
            .iter()
            .enumerate()
            .map(|(idx, key)| (key, idx))
            .collect();
        let mut parent_indices: Vec<usize> = Vec::new();
        for key in keys.iter() {
            match self.get_parent_key(key).map_err(|_| fmt::Error)? {
                None => parent_indices.push(usize::MAX),
                Some(parent_key) => {
                    let idx = key_positions.get(&parent_key).ok_or(fmt::Error)?;
                    parent_indices.push(*idx)
                }
            }
        }
        write!(
            formatter,
            "{:?}",
            keys.into_iter()
                .zip(parent_indices.into_iter())
                .collect::<Vec<_>>()
        )
    }
}

impl SectionsDAG {
    /// Creates a new DAG containing only the `genesis_key`
    pub fn new(genesis_key: bls::PublicKey) -> Self {
        Self {
            genesis_key,
            dag: MerkleReg::new(),
            dag_root: BTreeSet::new(),
            hashes: BTreeMap::new(),
        }
    }

    /// Insert new key into the DAG. `parent_key` must exist in the DAG and must validate
    /// `signature`, otherwise error is returned.
    pub fn verify_and_insert(
        &mut self,
        parent_key: &bls::PublicKey,
        key: bls::PublicKey,
        signature: bls::Signature,
    ) -> Result<()> {
        if !self.verify_sig(parent_key, &key, &signature) {
            return Err(Error::InvalidSignature);
        }

        self.insert_trusted_key(parent_key, key, signature)
    }

    /// Insert new key into the DAG. Does not verify the keys in the dag. This is expected to be called
    /// for partial dag creation only (where the source is already verified + trusted)
    fn insert_trusted_key(
        &mut self,
        parent_key: &bls::PublicKey,
        key: bls::PublicKey,
        signature: bls::Signature,
    ) -> Result<()> {
        let parent = if *parent_key == self.genesis_key {
            self.dag_root.insert(key);
            BTreeSet::new()
        } else {
            let hash = self
                .hashes
                .get(parent_key)
                .ok_or(Error::KeyNotFound(*parent_key))?;
            BTreeSet::from([*hash])
        };

        let info = SectionInfo {
            key,
            sig: signature,
        };
        // dag's children arg = parent
        let child = self.dag.write(info, parent);
        self.hashes.insert(key, child.hash());
        self.dag.apply(child);

        Ok(())
    }

    /// Insert new key into the DAG. `parent_key` must exist in the DAG and must validate
    /// `signature`, otherwise error is returned.
    fn insert_node(&mut self, parent_key: &bls::PublicKey, node: Node<SectionInfo>) -> Result<()> {
        self.verify_and_insert(parent_key, node.value.key, node.value.sig)
    }

    /// Get a partial `SectionsDAG` from the `from` key to the `to` key
    /// Returns `Error::KeyNotFound` if the `to` key is not present in the DAG;
    /// Returns `Error::InvalidBranch` if the `from` key is not found or is not a direct ancestor of the `to` key
    pub fn partial_dag(&self, from: &bls::PublicKey, to: &bls::PublicKey) -> Result<Self> {
        // start from the "to" key (bottom of the tree) and traverse to the root
        let mut crdt_ops: Vec<Node<SectionInfo>> = Vec::new();

        // Return the singleton DAG when from == to == genesis
        // This is a special case because the genesis entry does not have a `Node` so `get_node(to)` will fail
        if to == &self.genesis_key {
            if from != &self.genesis_key {
                return Err(Error::InvalidBranch);
            } else {
                return Ok(Self::new(self.genesis_key));
            }
        }

        // if "to" key is genesis, returns error
        let mut node = self.get_node(to)?;
        loop {
            if node.value.key == *from {
                // we have reached the end
                break;
            }
            // we don't insert the `from` node
            crdt_ops.push(node.clone());
            match self.parent_node(node.hash()) {
                Some(parent) => node = parent,
                // reached root of the dag
                None => {
                    // after reaching dag_root, return the partial_dag if from == genesis
                    if *from == self.genesis_key {
                        break;
                    }
                    // from key was apparently not a direct ancestor
                    return Err(Error::InvalidBranch);
                }
            }
        }
        // create the SectionsDAG and apply the CRDT ops
        let mut dag = Self::new(*from);
        let mut parent = *from;
        for node in crdt_ops.into_iter().rev() {
            let key = node.value.key;
            dag.insert_trusted_key(&parent, node.value.key, node.value.sig)?;
            parent = key;
        }
        Ok(dag)
    }

    /// Get a partial `SectionsDAG` with a single branch which contains the given `key`,
    /// from the genesis to the last key of any of its children branches.
    /// It also returns the last key of the (single) branch the returned DAG contains.
    /// Returns `Error::KeyNotFound` if the `key` key is not present in the DAG;
    pub fn single_branch_dag_for_key(
        &self,
        key: &bls::PublicKey,
    ) -> Result<(Self, bls::PublicKey)> {
        let mut last_key = if &self.genesis_key == key {
            match self.dag_root.iter().next() {
                Some(child_key) => *child_key,
                None => return Ok((Self::new(*key), *key)),
            }
        } else {
            *key
        };

        let mut partial_dag = self.partial_dag(&self.genesis_key, &last_key)?;
        while let Some(child_node) = self
            .get_hash(&last_key)
            .map(|hash| self.child_nodes(hash))?
            // TODO: allow to select which branch ??
            .get(0)
        {
            partial_dag.verify_and_insert(
                &last_key,
                child_node.value.key,
                child_node.value.sig.clone(),
            )?;
            last_key = child_node.value.key;
        }

        Ok((partial_dag, last_key))
    }

    /// Update our current `SectionsDAG` with the keys from another `SectionsDAG`
    /// Returns `Error::InvalidSignature` if the provided DAG fails signature verification
    /// Returns `Error::KeyNotFound` if the genesis_key of either of the DAGs is not present in the
    /// other
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        if !other.self_verify() {
            return Err(Error::UntrustedProofChain(format!(
                "Proof chain failed self verification: {other:?}",
            )));
        }
        // find which DAG is the parent
        if !self.has_key(other.genesis_key()) {
            if other.has_key(self.genesis_key()) {
                mem::swap(self, &mut other);
            } else {
                return Err(Error::KeyNotFound(self.genesis_key));
            }
        }
        // insert all the nodes from `other`
        let mut need_to_visit: Vec<(bls::PublicKey, Node<SectionInfo>)> = Vec::new();
        for key in &other.dag_root {
            need_to_visit.push((*other.genesis_key(), other.get_node(key)?));
        }
        while let Some((parent_key, current_node)) = need_to_visit.pop() {
            need_to_visit.extend(
                other
                    .child_nodes(current_node.hash())
                    .into_iter()
                    .map(|child_node| (current_node.value.key, child_node)),
            );
            self.insert_node(&parent_key, current_node)?;
        }

        Ok(())
    }

    /// Returns the genesis key
    pub fn genesis_key(&self) -> &bls::PublicKey {
        &self.genesis_key
    }

    /// Returns the list of the leaf keys
    pub fn leaf_keys(&self) -> BTreeSet<bls::PublicKey> {
        // if leaf_sections is empty, genesis is the only leaf
        let leaf_sections = self.non_genesis_leaf_sections();
        if leaf_sections.is_empty() {
            BTreeSet::from([self.genesis_key])
        } else {
            leaf_sections.iter().map(|info| info.key).collect()
        }
    }

    /// Iterator over all the keys in the `SectionsDAG`
    pub fn keys(&self) -> impl Iterator<Item = bls::PublicKey> + '_ {
        iter::once(self.genesis_key).chain(self.dag.all_nodes().map(|node| node.value.key))
    }

    /// Returns whether `key` is present in this `SectionsDAG`.
    pub fn has_key(&self, key: &bls::PublicKey) -> bool {
        key == &self.genesis_key || self.hashes.contains_key(key)
    }

    /// Verify every BLS key in the DAG is proven (signed) by its parent key,
    /// except the genesis key
    pub fn self_verify(&self) -> bool {
        let mut verified_keys = BTreeSet::from([self.genesis_key]);
        // if we contain only genesis, non_genesis_leaf_sections returns empty list; hence return
        // true since no verification is required
        self.non_genesis_leaf_sections().iter().all(|section| {
            let mut key = section.key;
            loop {
                let parent = match self.get_parent_key(&key) {
                    // cannot be None as section.key can never be genesis
                    Ok(Some(parent)) => parent,
                    _ => break false,
                };

                let sig = match self.get_node(&key) {
                    Ok(node) => node.value.sig,
                    Err(_) => {
                        error!(
                            "SectionsDAG::self_verify() unreachable. Can be error only if hashes go out of sync"
                        );
                        break false;
                    }
                };

                if !self.verify_sig(&parent, &key, &sig) {
                    break false;
                }
                verified_keys.insert(key);
                // we stop when parent is verified; meaning all ancestors are verified.
                if verified_keys.contains(&parent) {
                    break true;
                }
                // go up the chain and keep verifying its parent
                key = parent;
            }
        })
    }

    /// Returns `true` if the `genesis_key` is present in the list of `trusted_keys`
    pub fn check_trust<I>(&self, trusted_keys: I) -> bool
    where
        I: IntoIterator<Item = bls::PublicKey>,
    {
        trusted_keys.into_iter().any(|k| k == self.genesis_key)
    }

    /// Returns the parent of the provided key. None is returned if we're provided the `genesis_key`
    /// Can return `Error::KeyNotFound`
    pub fn get_parent_key(&self, key: &bls::PublicKey) -> Result<Option<bls::PublicKey>> {
        if *key == self.genesis_key {
            Ok(None)
        } else {
            self.get_hash(key).map(|hash| {
                self.parent_node(hash)
                    .map(|node| node.value.key)
                    // parent_node is None if we're provided the dag_root; hence parent = genesis key
                    .or(Some(self.genesis_key))
            })
        }
    }

    /// Returns the ancestors of the provided key. List is empty if we're provided the `genesis_key`
    /// Can return `Error::KeyNotFound`
    pub fn get_ancestors(&self, key: &bls::PublicKey) -> Result<Vec<bls::PublicKey>> {
        let mut ancestors = Vec::new();
        let mut key = *key;
        // loop until we reach key = genesis_key, then get_parent_key = None
        while let Some(parent) = self.get_parent_key(&key)? {
            ancestors.push(parent);
            key = parent;
        }
        Ok(ancestors)
    }

    /// Returns the immediate children of a given section key. List is empty if the section
    /// is a leaf. Can return `Error::KeyNotFound`
    pub fn get_child_keys(&self, key: &bls::PublicKey) -> Result<Vec<bls::PublicKey>> {
        if *key == self.genesis_key {
            Ok(self.dag_root.iter().cloned().collect())
        } else {
            self.get_hash(key).map(|hash| {
                self.child_nodes(hash)
                    .iter()
                    .map(|node| node.value.key)
                    .collect()
            })
        }
    }

    /// Returns the last key if the DAG contains a single branch
    /// Else returns `Error::MultipleBranchError`
    pub fn last_key(&self) -> Result<bls::PublicKey> {
        let last_key = self.leaf_keys().into_iter().collect::<Vec<_>>();
        if last_key.len() != 1 {
            return Err(Error::MultipleBranchError);
        }
        Ok(last_key[0])
    }

    /// Returns the list of the leaf sections. A section is considered a leaf if it has not
    /// gone through any churn or split. Empty list if we only hold the genesis.
    fn non_genesis_leaf_sections(&self) -> Vec<SectionInfo> {
        self.dag.read().values().cloned().collect()
    }

    /// Returns the list of the leaf nodes. A section is considered a leaf if it has not
    /// gone through any churn or split. Empty list if we only hold the genesis.
    fn non_genesis_leaf_nodes(&self) -> Vec<Node<SectionInfo>> {
        self.dag.read().nodes().cloned().collect()
    }

    /// Returns the parent node of the given hash; There can be only one parent;
    /// If hash of dag_root is provided, parent = genesis, hence parent_node is absent
    fn parent_node(&self, hash: Hash) -> Option<Node<SectionInfo>> {
        // .children() means the actual parent; max 1 parent
        self.dag.children(hash).nodes().next().cloned()
    }

    // Returns the child node of the given hash; Since a section can only split into 2 section,
    // children can be max 2. List is empty if the provided Hash belongs to a leaf section.
    fn child_nodes(&self, hash: Hash) -> Vec<Node<SectionInfo>> {
        // .parent() means the actual children
        self.dag.parents(hash).nodes().cloned().collect()
    }

    fn get_node(&self, key: &bls::PublicKey) -> Result<Node<SectionInfo>> {
        let hash = self.get_hash(key)?;
        match self.dag.node(hash) {
            Some(node) => Ok(node.clone()),
            None => {
                error!("SectionsDAG::get_node() unreachable. Can be error only if hashes go out of sync");
                Err(Error::KeyNotFound(*key))
            }
        }
    }

    fn get_hash(&self, key: &bls::PublicKey) -> Result<Hash> {
        let hash = *self.hashes.get(key).ok_or(Error::KeyNotFound(*key))?;
        Ok(hash)
    }

    fn verify_sig(
        &self,
        parent_key: &bls::PublicKey,
        section_key: &bls::PublicKey,
        sig: &bls::Signature,
    ) -> bool {
        bincode::serialize(section_key)
            .map(|bytes| parent_key.verify(sig, bytes))
            .unwrap_or(false)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{Error, SectionInfo, SectionsDAG};
    use crate::{
        messaging::system::SectionSigned,
        test_utils::{assert_lists, prefix, TestKeys, TestSapBuilder},
        SectionAuthorityProvider,
    };
    use crdts::CmRDT;
    use eyre::{eyre, Result};
    use proptest::prelude::{any, proptest, ProptestConfig, Strategy};
    use rand::{rngs::StdRng, thread_rng, Rng, RngCore, SeedableRng};
    use std::collections::{BTreeMap, BTreeSet};
    use xor_name::Prefix;

    #[test]
    fn test_partial_dag_for_genesis_key() {
        let (_, pk) = gen_keypair();
        let dag = SectionsDAG::new(pk);

        let partial_dag = dag
            .partial_dag(&pk, &pk)
            .expect("Should have succeeded in creating a partial dag");

        assert_eq!(Vec::from_iter(partial_dag.keys()), vec![pk]);

        let (_, pk_other) = gen_keypair();

        assert!(matches!(
            dag.partial_dag(&pk_other, &pk),
            Err(Error::InvalidBranch)
        ));
    }

    #[test]
    fn insert_last() -> Result<()> {
        let (mut last_sk, pk) = gen_keypair();
        let mut dag = SectionsDAG::new(pk);
        let mut expected_keys = vec![pk];

        for _ in 0..10 {
            let last_pk = &expected_keys[expected_keys.len() - 1];
            let (sk, info) = gen_signed_keypair(&last_sk);

            dag.verify_and_insert(last_pk, info.key, info.sig)?;

            expected_keys.push(info.key);
            last_sk = sk;
        }
        assert_lists(dag.keys(), expected_keys);
        Ok(())
    }

    #[test]
    fn insert_fork() -> Result<()> {
        // We use a DAG with two branches, a and b:
        //  gen -> pk_a1 -> pk_a2
        //     |
        //     +-> pk_b
        //
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);
        let (_, info_b) = gen_signed_keypair(&sk_gen);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        dag.verify_and_insert(&pk_gen, info_b.key, info_b.sig)?;

        assert_lists(dag.keys(), [pk_gen, info_a1.key, info_a2.key, info_b.key]);

        assert_lists(
            dag.partial_dag(&pk_gen, &info_a2.key)?.keys(),
            [pk_gen, info_a1.key, info_a2.key],
        );
        assert_lists(
            dag.partial_dag(&pk_gen, &info_b.key)?.keys(),
            [pk_gen, info_b.key],
        );

        assert!(dag.partial_dag(&info_a2.key, &pk_gen).is_err());
        assert!(dag.partial_dag(&info_a1.key, &info_b.key).is_err());
        assert!(dag.self_verify());
        Ok(())
    }

    #[test]
    fn get_chain_from_genesis_to_last() -> Result<()> {
        // We use a DAG with three branches, a, b and c:
        //  gen -> pk_a1 -> pk_a2
        //     |
        //     +-> pk_b1 -> pk_b2 -> pk_b3
        //                       |
        //                       +-> pk_c
        //
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);
        let (sk_b1, info_b1) = gen_signed_keypair(&sk_gen);
        let (sk_b2, info_b2) = gen_signed_keypair(&sk_b1);
        let (_, info_b3) = gen_signed_keypair(&sk_b2);
        let (_, info_c) = gen_signed_keypair(&sk_b2);

        // create DAG with genesis key
        let mut dag = SectionsDAG::new(pk_gen);

        // branch from genesis now is a partial DAG with just [genesis key]
        let (partial_gen, last_key_gen) = dag.single_branch_dag_for_key(&pk_gen)?;
        assert!(partial_gen.self_verify());
        assert_eq!(last_key_gen, pk_gen);
        assert_lists(partial_gen.keys(), [pk_gen]);

        // let's insert only a1 into the DAG for now
        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;

        // branch from genesis or a1 is a partial DAG with [genesis key, a1]
        let (partial_gen, last_key_gen) = dag.single_branch_dag_for_key(&pk_gen)?;
        let (partial_a1, last_key_a1) = dag.single_branch_dag_for_key(&info_a1.key)?;
        assert!(partial_gen.self_verify());
        assert!(partial_a1.self_verify());
        assert_eq!(last_key_gen, info_a1.key);
        assert_eq!(last_key_a1, info_a1.key);
        assert_lists(partial_gen.keys(), [pk_gen, info_a1.key]);
        assert_lists(partial_a1.keys(), partial_gen.keys());

        // let's now insert a2 into the DAG
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;

        // branch from genesis, a1 or a2 is a partial DAG with [genesis key, a1, a2]
        let (partial_gen, last_key_gen) = dag.single_branch_dag_for_key(&pk_gen)?;
        let (partial_a1, last_key_a1) = dag.single_branch_dag_for_key(&info_a1.key)?;
        let (partial_a2, last_key_a2) = dag.single_branch_dag_for_key(&info_a2.key)?;
        assert!(partial_gen.self_verify());
        assert!(partial_a1.self_verify());
        assert!(partial_a2.self_verify());
        assert_eq!(last_key_gen, info_a2.key);
        assert_eq!(last_key_a1, info_a2.key);
        assert_eq!(last_key_a2, info_a2.key);
        assert_lists(partial_gen.keys(), [pk_gen, info_a1.key, info_a2.key]);
        assert_lists(partial_a1.keys(), partial_gen.keys());
        assert_lists(partial_a2.keys(), partial_gen.keys());

        // let's now insert the other two branches (b and c) into the DAG
        dag.verify_and_insert(&pk_gen, info_b1.key, info_b1.sig)?;
        dag.verify_and_insert(&info_b1.key, info_b2.key, info_b2.sig)?;
        dag.verify_and_insert(&info_b2.key, info_b3.key, info_b3.sig)?;
        dag.verify_and_insert(&info_b2.key, info_c.key, info_c.sig)?;
        assert!(dag.self_verify());
        assert_lists(
            dag.keys(),
            [
                pk_gen,
                info_a1.key,
                info_a2.key,
                info_b1.key,
                info_b2.key,
                info_b3.key,
                info_c.key,
            ],
        );

        let (partial_gen, last_key_gen) = dag.single_branch_dag_for_key(&pk_gen)?;
        let (partial_a1, last_key_a1) = dag.single_branch_dag_for_key(&info_a1.key)?;
        let (partial_a2, last_key_a2) = dag.single_branch_dag_for_key(&info_a2.key)?;
        let (partial_b1, last_key_b1) = dag.single_branch_dag_for_key(&info_b1.key)?;
        let (partial_b2, last_key_b2) = dag.single_branch_dag_for_key(&info_b2.key)?;
        let (partial_b3, last_key_b3) = dag.single_branch_dag_for_key(&info_b3.key)?;
        let (partial_c, last_key_c) = dag.single_branch_dag_for_key(&info_c.key)?;
        assert!(partial_gen.self_verify());
        assert!(partial_a1.self_verify());
        assert!(partial_a2.self_verify());
        assert!(partial_b1.self_verify());
        assert!(partial_b2.self_verify());
        assert!(partial_b3.self_verify());
        assert!(partial_c.self_verify());

        // branch from a1 or a2 has a2 as the last key
        assert_eq!(last_key_a1, info_a2.key);
        assert_eq!(last_key_a2, info_a2.key);

        // branch from b1 or b2 has either b3 or c as the last key
        assert!((last_key_b1 == info_b3.key) ^ (last_key_b1 == info_c.key));
        assert!((last_key_b2 == info_b3.key) ^ (last_key_b2 == info_c.key));

        // branch from b3 has b3 as the last key
        assert_eq!(last_key_b3, info_b3.key);

        // branch from c has c as the last key
        assert_eq!(last_key_c, info_c.key);

        // branch from genesis has either a2, b3 or c as the last key
        assert!(
            (last_key_gen == info_a2.key)
                ^ (last_key_gen == info_b3.key)
                ^ (last_key_gen == info_c.key)
        );

        // branch from a1 or a2 is a partial DAG with [genesis key, a1, a2]
        assert_lists(partial_a1.keys(), [pk_gen, info_a1.key, info_a2.key]);
        assert_lists(partial_a1.keys(), partial_a2.keys());

        // branch from b3 is a partial DAG with [genesis key, b1, b2, b3]
        assert_lists(
            partial_b3.keys(),
            [pk_gen, info_b1.key, info_b2.key, info_b3.key],
        );

        // branch from c is a partial DAG with [genesis key, b1, b2, c]
        assert_lists(
            partial_c.keys(),
            [pk_gen, info_b1.key, info_b2.key, info_c.key],
        );

        // branch from b1 is a partial DAG with either:
        // - [genesis key, b1, b2, b3]
        // - or [genesis key, b1, b2, c]
        assert_eq!(
            [&partial_b3, &partial_c]
                .into_iter()
                .filter(|b| b == &&partial_b1)
                .count(),
            1
        );

        // branch from b2 is a partial DAG with either:
        // - [genesis key, b1, b2, b3]
        // - or [genesis key, b1, b2, c]
        assert_eq!(
            [&partial_b3, &partial_c]
                .into_iter()
                .filter(|b| b == &&partial_b2)
                .count(),
            1
        );

        // branch from genesis key is a partial DAG with either:
        // - [genesis key, a1, a2]
        // - or [genesis key, b1, b2, b3]
        // - or [genesis key, b1, b2, c]
        assert_eq!(
            [partial_a2, partial_b3, partial_c]
                .into_iter()
                .filter(|b| b == &partial_gen)
                .count(),
            1
        );

        // trying to get branch from a random/non-existing key returns `KeyNotFound` error
        let (_, random_pk) = gen_keypair();
        matches!(
            dag.single_branch_dag_for_key(&random_pk).err(),
            Some(Error::KeyNotFound(key)) if key == random_pk
        );

        Ok(())
    }

    #[test]
    fn insert_duplicate_key() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (_, info_a) = gen_signed_keypair(&sk_gen);

        let mut dag = SectionsDAG::new(pk_gen);
        assert!(dag
            .verify_and_insert(&pk_gen, info_a.key, info_a.sig.clone())
            .is_ok());
        assert!(dag
            .verify_and_insert(&pk_gen, info_a.key, info_a.sig)
            .is_ok());
        assert_lists(dag.keys(), [info_a.key, pk_gen]);

        Ok(())
    }

    #[test]
    fn invalid_signature() {
        let (_, pk_gen) = gen_keypair();
        let bad_sk_gen = bls::SecretKey::random();
        let (_, info_a) = gen_signed_keypair(&bad_sk_gen);

        let mut dag = SectionsDAG::new(pk_gen);
        assert!(dag
            .verify_and_insert(&pk_gen, info_a.key, info_a.sig)
            .is_err());
    }

    #[test]
    fn wrong_parent_child_order() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);

        let mut dag = SectionsDAG::new(pk_gen);
        // inserting child before parent
        assert!(dag
            .verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig.clone())
            .is_err());
        assert_lists(dag.keys(), [pk_gen]);

        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        assert_lists(dag.keys(), [pk_gen, info_a1.key, info_a2.key]);

        Ok(())
    }

    #[test]
    fn merge() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (sk_a2, info_a2) = gen_signed_keypair(&sk_a1);
        let (_, info_a3) = gen_signed_keypair(&sk_a2);

        // main_dag: 0->1->2->3
        let mut main_dag = SectionsDAG::new(pk_gen);
        main_dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        let mut dag_01 = main_dag.clone();
        let mut dag_01_err = main_dag.clone();
        main_dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        let mut dag_012 = main_dag.clone();
        main_dag.verify_and_insert(&info_a2.key, info_a3.key, info_a3.sig)?;
        let mut dag_0123 = main_dag.clone();

        // main_dag: 0->1->2->3
        // partial_dag: 0
        // out: 0->1->2->3
        let partial_dag = SectionsDAG::new(pk_gen);
        main_dag.merge(partial_dag)?;
        assert!(main_dag.self_verify());
        assert_eq!(main_dag, dag_0123);

        // dag: 0->1->2
        // partial_dag: 2->3
        // out: 0->1->2->3
        let partial_dag = main_dag.partial_dag(&info_a2.key, &info_a3.key)?;
        dag_012.merge(partial_dag)?;
        assert!(dag_012.self_verify());
        assert_eq!(dag_012, main_dag);

        // dag: 0->1
        // partial_dag: 1->2->3
        // out: 0->1->2->3
        let partial_dag = main_dag.partial_dag(&info_a1.key, &info_a3.key)?;
        dag_01.merge(partial_dag)?;
        assert!(dag_01.self_verify());
        assert_eq!(dag_01, main_dag);

        // dag: 0->1->2->3
        // partial_dag: 0->1->2->3
        // out: 0->1->2->3
        let partial_dag = main_dag.partial_dag(&pk_gen, &info_a3.key)?;
        dag_0123.merge(partial_dag)?;
        assert!(dag_0123.self_verify());
        assert_eq!(dag_0123, main_dag);

        // dag: 0->1
        // partial_dag: 2->3
        // out: Error
        let partial_dag = main_dag.partial_dag(&info_a2.key, &info_a3.key)?;
        assert!(dag_01_err.merge(partial_dag).is_err());
        assert_lists(dag_01_err.keys(), [pk_gen, info_a1.key]);

        Ok(())
    }

    #[test]
    fn merge_with_branches() -> Result<()> {
        // create a main DAG
        //  gen -> pk_a1 -> pk_a2
        //     |
        //     +-> pk_b1 -> pk_b2
        //              |
        //              +-> pk_c
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);
        let (sk_b1, info_b1) = gen_signed_keypair(&sk_gen);
        // pk_b1->pk_b2
        let (_, info_b2) = gen_signed_keypair(&sk_b1);
        // pk_b1->pk_c
        let (_, info_c) = gen_signed_keypair(&sk_b1);

        let mut main_dag = SectionsDAG::new(pk_gen);
        // gen->pk_a1->pk_a2
        main_dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        main_dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        // gen->pk_b1->pk_b2
        main_dag.verify_and_insert(&pk_gen, info_b1.key, info_b1.sig)?;
        main_dag.verify_and_insert(&info_b1.key, info_b2.key, info_b2.sig)?;
        // pk_b1->pk_c
        main_dag.verify_and_insert(&info_b1.key, info_c.key, info_c.sig)?;

        let mut dag = SectionsDAG::new(pk_gen);
        // merge from gen till pk_c
        let partial_dag = main_dag.partial_dag(&pk_gen, &info_c.key)?;
        dag.merge(partial_dag)?;
        // merge from gen till pk_a2
        let partial_dag = main_dag.partial_dag(&pk_gen, &info_a2.key)?;
        dag.merge(partial_dag)?;
        // merge from gen till pk_b2
        let partial_dag = main_dag.partial_dag(&pk_gen, &info_b2.key)?;
        dag.merge(partial_dag)?;

        assert!(main_dag.self_verify());
        assert!(dag.self_verify());
        assert_eq!(main_dag, dag);
        Ok(())
    }

    #[test]
    fn merge_fork() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (_, info_a) = gen_signed_keypair(&sk_gen);
        let (_, info_b) = gen_signed_keypair(&sk_gen);

        // dag_a: gen->pk_a
        // dag_b: gen->pk_b
        // dag_a + dag_b: gen -> pk_a
        //                   |
        //                   +-> pk_b
        let mut dag_a = SectionsDAG::new(pk_gen);
        dag_a.verify_and_insert(&pk_gen, info_a.key, info_a.sig)?;
        let mut dag_b = SectionsDAG::new(pk_gen);
        dag_b.verify_and_insert(&pk_gen, info_b.key, info_b.sig)?;

        let dag_from_a = dag_a.partial_dag(&pk_gen, &info_a.key)?;
        let dag_from_b = dag_b.partial_dag(&pk_gen, &info_b.key)?;

        dag_a.merge(dag_from_b)?;
        dag_b.merge(dag_from_a)?;

        assert!(dag_a.self_verify());
        assert!(dag_b.self_verify());
        assert_eq!(dag_a, dag_b);
        Ok(())
    }

    #[test]
    fn self_verify_invalid_sigs() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk1, info1) = gen_signed_keypair(&sk_gen);
        let (_, info2) = gen_signed_keypair(&sk1);
        let (sk3, mut info3) = gen_signed_keypair(&sk1);
        let (_, info4) = gen_signed_keypair(&sk3);

        // make a DAG with a fork but an invalid signature in section 3
        // gen->1->2
        //    |
        //    +->3->4
        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info1.key, info1.sig.clone())?;
        dag.verify_and_insert(&info1.key, info2.key, info2.sig)?;
        // insert section3 manually with corrupted sig
        info3.sig = info1.sig; // use a random section's sig
        let hash1 = dag.get_hash(&info1.key)?;
        let node3_parent = BTreeSet::from([hash1]);
        let node3 = dag.dag.write(info3.clone(), node3_parent);
        dag.hashes.insert(info3.key, node3.hash());
        dag.dag.apply(node3);
        // continue inserting section 4
        dag.verify_and_insert(&info3.key, info4.key, info4.sig)?;

        assert!(!dag.self_verify());
        Ok(())
    }

    #[test]
    fn verify_parent_during_split() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (_, info_a) = gen_signed_keypair(&sk_gen);
        let (_, info_b) = gen_signed_keypair(&sk_gen);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a.key, info_a.sig)?;
        dag.verify_and_insert(&pk_gen, info_b.key, info_b.sig)?;

        assert_eq!(
            dag.get_parent_key(&info_a.key)?,
            dag.get_parent_key(&info_b.key)?
        );
        Ok(())
    }

    #[test]
    fn verify_parents_during_churn() -> Result<()> {
        // gen -> pk_a1 -> pk_a2
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;

        assert!(matches!(dag.get_parent_key(&info_a2.key)?, Some(key) if key == info_a1.key));
        assert!(matches!(dag.get_parent_key(&info_a1.key)?, Some(key) if key == pk_gen));
        assert!(matches!(dag.get_parent_key(&pk_gen)?, None));
        Ok(())
    }

    #[test]
    fn verify_children() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (_, info_a) = gen_signed_keypair(&sk_gen);
        let (sk_b1, info_b1) = gen_signed_keypair(&sk_gen);
        let (_, info_b2) = gen_signed_keypair(&sk_b1);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a.key, info_a.sig)?;
        dag.verify_and_insert(&pk_gen, info_b1.key, info_b1.sig)?;
        dag.verify_and_insert(&info_b1.key, info_b2.key, info_b2.sig)?;

        assert_lists(dag.get_child_keys(&pk_gen)?, [info_a.key, info_b1.key]);
        assert_lists(dag.get_child_keys(&info_b1.key)?, [info_b2.key]);
        assert!(dag.get_child_keys(&info_a.key)?.is_empty());
        Ok(())
    }

    #[test]
    fn verify_leaves() -> Result<()> {
        let (sk_gen, pk_gen) = gen_keypair();
        let (_, info_a) = gen_signed_keypair(&sk_gen);
        let (sk_b1, info_b1) = gen_signed_keypair(&sk_gen);
        let (_, info_b2) = gen_signed_keypair(&sk_b1);

        let mut sections_dag = SectionsDAG::new(pk_gen);
        sections_dag.verify_and_insert(&pk_gen, info_a.key, info_a.sig)?;
        sections_dag.verify_and_insert(&pk_gen, info_b1.key, info_b1.sig)?;
        sections_dag.verify_and_insert(&info_b1.key, info_b2.key, info_b2.sig)?;

        assert_lists(sections_dag.leaf_keys(), [info_a.key, info_b2.key]);
        Ok(())
    }

    #[test]
    fn verify_ancestors() -> Result<()> {
        // gen -> pk_a1 -> pk_a2 -> pk_a3
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (sk_a2, info_a2) = gen_signed_keypair(&sk_a1);
        let (_, info_a3) = gen_signed_keypair(&sk_a2);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        dag.verify_and_insert(&info_a2.key, info_a3.key, info_a3.sig)?;

        assert_lists(
            dag.get_ancestors(&info_a3.key)?,
            [pk_gen, info_a1.key, info_a2.key],
        );
        assert_lists(dag.get_ancestors(&info_a2.key)?, [pk_gen, info_a1.key]);
        assert_lists(dag.get_ancestors(&info_a1.key)?, [pk_gen]);
        assert_lists(dag.get_ancestors(&pk_gen)?, []);

        Ok(())
    }

    // Proptest to make sure that the using `merge` with various combinations of partial dags will give
    // back the original `SectionsDAG`
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 100, .. ProptestConfig::default()
        })]
        #[test]
        #[allow(clippy::unwrap_used)]
        fn proptest_merge_sections_dag((_, main_dag, list_of_partial_dags) in arb_sections_dag_and_proof_chains(100, false)) {
                let mut dag = SectionsDAG::new(main_dag.genesis_key);
                for (partial_dag, _last_key_sap) in list_of_partial_dags {
                    dag.merge(partial_dag).unwrap();
                }
                assert_eq!(dag, main_dag);
        }
    }

    #[test]
    fn verify_ser_de() -> Result<()> {
        //  gen -> pk_a1 -> pk_a2
        //     |
        //     +-> pk_b1 -> pk_b2
        //              |
        //              +-> pk_c
        let (sk_gen, pk_gen) = gen_keypair();
        let (sk_a1, info_a1) = gen_signed_keypair(&sk_gen);
        let (_, info_a2) = gen_signed_keypair(&sk_a1);
        let (sk_b1, info_b1) = gen_signed_keypair(&sk_gen);
        let (_, info_b2) = gen_signed_keypair(&sk_b1);
        let (_, info_c) = gen_signed_keypair(&sk_b1);

        let mut dag = SectionsDAG::new(pk_gen);
        dag.verify_and_insert(&pk_gen, info_a1.key, info_a1.sig)?;
        dag.verify_and_insert(&info_a1.key, info_a2.key, info_a2.sig)?;
        dag.verify_and_insert(&pk_gen, info_b1.key, info_b1.sig)?;
        dag.verify_and_insert(&info_b1.key, info_b2.key, info_b2.sig)?;
        dag.verify_and_insert(&info_b1.key, info_c.key, info_c.sig)?;

        let dag_string = serde_json::to_string(&dag)?;
        let dag_from_string = serde_json::from_str::<SectionsDAG>(dag_string.as_str())?;
        assert_eq!(dag, dag_from_string);

        Ok(())
    }

    // Test helpers
    fn gen_keypair() -> (bls::SecretKey, bls::PublicKey) {
        let sk_set = bls::SecretKeySet::random(0, &mut thread_rng());
        let sk = sk_set.secret_key();
        (sk.clone(), sk.public_key())
    }

    fn gen_signed_keypair(parent_sk: &bls::SecretKey) -> (bls::SecretKey, SectionInfo) {
        let (sk, pk) = gen_keypair();
        let sig = TestKeys::sign(parent_sk, &pk);
        let info = SectionInfo { key: pk, sig };
        (sk, info)
    }

    // Generate an arbitrary sized `SectionsDAG` and a list of partial_dags which inserted in
    // that order gives back the main_dag
    // new_information_only: if false, the partial_dags can end with keys which has been previously
    // inserted, providing no new information. Useful in `SectionTree` proptest where we want new_information_only.
    #[allow(clippy::unwrap_used)]
    pub(crate) fn arb_sections_dag_and_proof_chains(
        max_sections: usize,
        new_information_only: bool,
    ) -> impl Strategy<
        Value = (
            SectionSigned<SectionAuthorityProvider>,
            SectionsDAG,
            Vec<(SectionsDAG, SectionSigned<SectionAuthorityProvider>)>,
        ),
    > {
        (any::<u64>(), 2..=max_sections).prop_map(move |(seed, size)| {
            // size is [2, max_sections] size of 0,1 will give us back only the genesis_key and
            // hence we cannot obtain the partial dag
            let (main_dag, map) = gen_random_sections_dag(Some(seed), size).unwrap();
            let mut rng = StdRng::seed_from_u64(seed);

            let mut leaves: Vec<_> = main_dag.leaf_keys().into_iter().collect();
            let mut inserted_keys = BTreeSet::from([main_dag.genesis_key]);

            let mut list_of_part_dags = Vec::new();
            while !leaves.is_empty() {
                // get partial dag to a random leaf
                let random_leaf = *leaves
                    .get(rng.gen::<usize>() % leaves.len())
                    .expect("leaves cannot be empty");

                let mut ancestors = main_dag.get_ancestors(&random_leaf).unwrap();
                ancestors.reverse();
                // a simple chain of keys from genesis_key, random_leaf
                ancestors.push(random_leaf);

                // find the position of the first node which we have not inserted; cannot be 0
                let first_unique_node_idx = ancestors
                    .iter()
                    .position(|key| !inserted_keys.contains(key))
                    .expect("The leaf should not have been inserted, hence cannot be None");

                let rand_from_idx = rng.gen_range(0..first_unique_node_idx);
                let rand_to_idx = {
                    // Consider the `SectionTree` is aware of the following section keys A->B->C
                    // Now, the valid `SectionsDag` which can be inserted in the tree can be
                    // [A/B/C to D/E/F] i.e., B->C->D is valid, C->D is valid, C is also valid
                    // Where as A->B is invalid since we are providing old information to the tree
                    let start = if new_information_only {
                        // cannot be 0; hence `to` cannot be genesis
                        first_unique_node_idx
                    } else {
                        // make sure `to` cannot be genesis since `merge` will throw error
                        if rand_from_idx == 0 {
                            rand_from_idx + 1
                        } else {
                            rand_from_idx
                        }
                    };
                    rng.gen_range(start..ancestors.len())
                };
                let rand_from = ancestors[rand_from_idx];
                let rand_to = ancestors[rand_to_idx];
                // `merge` with just 1 key (i.e., empty dag) does not throw an error
                let partial_dag = main_dag.partial_dag(&rand_from, &rand_to).unwrap();

                // if the "to" key is a leaf, remove it from leaves
                if let Some(index) = leaves.iter().position(|key| *key == rand_to) {
                    leaves.swap_remove(index);
                }
                // update the inserted list
                inserted_keys.extend(partial_dag.keys());

                let last_key_sap = map.get(&rand_to).unwrap();
                list_of_part_dags.push((partial_dag, last_key_sap.clone()));
            }
            let gen_sap = map.get(main_dag.genesis_key()).unwrap().clone();
            (gen_sap, main_dag, list_of_part_dags)
        })
    }

    // Generate a random `SectionsDAG` and the SAP for each of the section key
    fn gen_random_sections_dag(
        seed: Option<u64>,
        n_sections: usize,
    ) -> Result<(
        SectionsDAG,
        BTreeMap<bls::PublicKey, SectionSigned<SectionAuthorityProvider>>,
    )> {
        let mut rng = match seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        let (sap_gen, sk_gen, ..) = TestSapBuilder::new(Prefix::default())
            .elder_count(0)
            .build_rng(&mut rng);
        let sk_gen = sk_gen.secret_key();
        let sap_gen = TestKeys::get_section_signed(&sk_gen, sap_gen);
        let pk_gen = sap_gen.public_key_set().public_key();

        let mut dag = SectionsDAG::new(pk_gen);
        let mut sections_map = BTreeMap::from([(pk_gen, (sk_gen.clone(), sap_gen))]);
        let mut count = 1;

        if n_sections <= 1 {
            // filter out sk
            let map = sections_map
                .iter()
                .map(|(key, (_, sap))| (*key, sap.clone()))
                .collect();
            return Ok((dag, map));
        };
        // insert a new section
        fn insert<R: RngCore>(
            prefix: Prefix,
            parent_sk: &bls::SecretKey,
            rng: &mut R,
            sections_map: &mut BTreeMap<
                bls::PublicKey,
                (bls::SecretKey, SectionSigned<SectionAuthorityProvider>),
            >,
            dag: &mut SectionsDAG,
        ) -> Result<()> {
            let (sap, sk_set, ..) = TestSapBuilder::new(prefix).elder_count(0).build_rng(rng);
            let sap = TestKeys::get_section_signed(&sk_set.secret_key(), sap);
            let key = sap.public_key_set().public_key();
            let sig = TestKeys::sign(parent_sk, &key);
            dag.verify_and_insert(&parent_sk.public_key(), sap.section_key(), sig)?;
            sections_map.insert(sap.section_key(), (sk_set.secret_key(), sap));
            Ok(())
        }

        // insert prefix 0,1
        insert(prefix("0"), &sk_gen, &mut rng, &mut sections_map, &mut dag)?;
        insert(prefix("1"), &sk_gen, &mut rng, &mut sections_map, &mut dag)?;

        while count < n_sections {
            let leaves: Vec<_> = dag.leaf_keys().into_iter().collect();
            let pk_leaf = leaves
                .get(rng.gen::<usize>() % leaves.len())
                .ok_or_else(|| eyre!("Leaves cannot be empty"))?;
            let (sk_leaf, sap_leaf) = sections_map
                .get(pk_leaf)
                .cloned()
                .ok_or_else(|| eyre!("leaf should be present"))?;

            if rng.gen_range(0..2) % 2 == 0 {
                // Split, insert two sections; increment the prefix
                let pref = prefix(format!("{:b}0", sap_leaf.prefix()).as_str());
                insert(pref, &sk_leaf, &mut rng, &mut sections_map, &mut dag)?;
                let pref = prefix(format!("{:b}1", sap_leaf.prefix()).as_str());
                insert(pref, &sk_leaf, &mut rng, &mut sections_map, &mut dag)?;
                count += 2;
            } else {
                // Churn; same prefix
                insert(
                    sap_leaf.prefix(),
                    &sk_leaf,
                    &mut rng,
                    &mut sections_map,
                    &mut dag,
                )?;
                count += 1;
            }
        }
        let map = sections_map
            .into_iter()
            .map(|(key, (_, sap))| (key, sap))
            .collect();
        Ok((dag, map))
    }
}
