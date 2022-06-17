// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Relocation related types and utilities.

use sn_interface::{
    elder_count,
    messaging::system::RelocateDetails,
    network_knowledge::{recommended_section_size, NetworkKnowledge, NodeState},
    types::{keys::ed25519, Peer},
};

use ed25519_dalek::{Signature, Verifier};
use std::{
    cmp::min,
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
};
use xor_name::XorName;

// Unique identifier for a churn event, which is used to select nodes to relocate.
pub(crate) struct ChurnId(pub(crate) Vec<u8>);

impl Display for ChurnId {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(
            fmt,
            "Churn-{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2]
        )
    }
}

/// Find all nodes to relocate after a churn event and generate the relocation details for them.
pub(super) fn find_nodes_to_relocate(
    network_knowledge: &NetworkKnowledge,
    churn_id: &ChurnId,
    excluded: BTreeSet<XorName>,
) -> Vec<(NodeState, RelocateDetails)> {
    // Find the peers that pass the relocation check and take only the oldest ones to avoid
    // relocating too many nodes at the same time.
    // Capped by criteria that cannot relocate too many node at once.
    let joined_nodes = network_knowledge.section_members();

    if joined_nodes.len() < recommended_section_size() {
        return vec![];
    }

    let max_reloctions = elder_count() / 2;
    let allowed_relocations = min(
        joined_nodes.len() - recommended_section_size(),
        max_reloctions,
    );

    // Find the peers that pass the relocation check
    let mut candidates: Vec<_> = joined_nodes
            .into_iter()
            .filter(|info| check(info.age(), churn_id))
            // the newly joined node shall not be relocated immediately
            .filter(|info| !excluded.contains(&info.name()))
            .collect();
    // To avoid a node to manipulate its name to gain priority of always being first in XorName,
    // here we sort the nodes by its distance to the churn_id.
    let target_name = XorName::from_content(&churn_id.0);
    candidates.sort_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()));

    let max_age = if let Some(age) = candidates.iter().map(|info| info.age()).max() {
        age
    } else {
        return vec![];
    };

    let mut relocating_nodes = vec![];
    for node_state in candidates {
        if node_state.age() == max_age {
            let dst = dst(&node_state.name(), churn_id);
            let age = node_state.age().saturating_add(1);
            let relocate_details =
                RelocateDetails::with_age(network_knowledge, node_state.peer(), dst, age);
            relocating_nodes.push((node_state, relocate_details));
        }
    }

    relocating_nodes
        .into_iter()
        .take(allowed_relocations)
        .collect()
}

/// Details of a relocation: which node to relocate, where to relocate it to
/// and what age it should get once relocated.
pub(super) trait RelocateDetailsUtils {
    fn with_age(
        network_knowledge: &NetworkKnowledge,
        peer: &Peer,
        dst: XorName,
        age: u8,
    ) -> RelocateDetails;

    fn verify_identity(&self, new_name: &XorName, new_name_sig: &Signature) -> bool;
}

impl RelocateDetailsUtils for RelocateDetails {
    fn with_age(
        network_knowledge: &NetworkKnowledge,
        peer: &Peer,
        dst: XorName,
        age: u8,
    ) -> RelocateDetails {
        let genesis_key = *network_knowledge.genesis_key();

        let dst_section_key = network_knowledge
            .section_by_name(&dst)
            .map_or_else(|_| genesis_key, |section_auth| section_auth.section_key());

        RelocateDetails {
            previous_name: peer.name(),
            dst,
            dst_section_key,
            age,
        }
    }

    fn verify_identity(&self, new_name: &XorName, new_name_sig: &Signature) -> bool {
        let pub_key = if let Ok(pub_key) = ed25519::pub_key(&self.previous_name) {
            pub_key
        } else {
            return false;
        };

        pub_key.verify(&new_name.0, new_name_sig).is_ok()
    }
}

// Relocation check - returns whether a member with the given age is a candidate for relocation on
// a churn event with the given churn id.
pub(crate) fn check(age: u8, churn_id: &ChurnId) -> bool {
    // Evaluate the formula: `signature % 2^age == 0` Which is the same as checking the signature
    // has at least `age` trailing zero bits.
    trailing_zeros(&churn_id.0) >= age as u32
}

// Compute the destination for the node with `relocating_name` to be relocated to.
fn dst(relocating_name: &XorName, churn_id: &ChurnId) -> XorName {
    XorName::from_content_parts(&[&relocating_name.0, &churn_id.0])
}

// Returns the number of trailing zero bits of the bytes slice.
fn trailing_zeros(bytes: &[u8]) -> u32 {
    let mut output = 0;

    for &byte in bytes.iter().rev() {
        if byte == 0 {
            output += 8;
        } else {
            output += byte.trailing_zeros();
            break;
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    use sn_interface::{
        elder_count,
        network_knowledge::{test_utils::section_signed, SectionAuthorityProvider, MIN_ADULT_AGE},
        types::SecretKeySet,
    };

    use eyre::Result;
    use itertools::Itertools;
    use proptest::{collection::SizeRange, prelude::*};
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use secured_linked_list::SecuredLinkedList;
    use std::net::SocketAddr;
    use xor_name::{Prefix, XOR_NAME_LEN};

    #[test]
    fn byte_slice_trailing_zeros() {
        assert_eq!(trailing_zeros(&[0]), 8);
        assert_eq!(trailing_zeros(&[1]), 0);
        assert_eq!(trailing_zeros(&[2]), 1);
        assert_eq!(trailing_zeros(&[4]), 2);
        assert_eq!(trailing_zeros(&[8]), 3);
        assert_eq!(trailing_zeros(&[0, 0]), 16);
        assert_eq!(trailing_zeros(&[1, 0]), 8);
        assert_eq!(trailing_zeros(&[2, 0]), 9);
    }

    const MAX_AGE: u8 = MIN_ADULT_AGE + 3;

    proptest! {
        #[test]
        fn proptest_actions(
            peers in arbitrary_unique_peers(2..(recommended_section_size() + elder_count()), MIN_ADULT_AGE..MAX_AGE),
            signature_trailing_zeros in 0..MAX_AGE)
        {
            proptest_actions_impl(peers, signature_trailing_zeros).unwrap()
        }
    }

    fn proptest_actions_impl(peers: Vec<Peer>, signature_trailing_zeros: u8) -> Result<()> {
        let sk_set = SecretKeySet::random();
        let sk = sk_set.secret_key();
        let genesis_pk = sk.public_key();

        // Create `Section` with `peers` as its members and set the `elder_count()` oldest peers as
        // the elders.
        let section_auth = SectionAuthorityProvider::new(
            peers
                .iter()
                .sorted_by_key(|peer| peer.age())
                .rev()
                .take(elder_count())
                .cloned(),
            Prefix::default(),
            peers.iter().map(|p| NodeState::joined(*p, None)),
            sk_set.public_keys(),
            0,
        );
        let section_auth = section_signed(sk, section_auth)?;

        let network_knowledge = NetworkKnowledge::new(
            genesis_pk,
            SecuredLinkedList::new(genesis_pk),
            section_auth,
            None,
        )?;

        for peer in &peers {
            let info = NodeState::joined(*peer, None);
            let info = section_signed(sk, info)?;

            let res = network_knowledge.update_member(info);
            assert!(res);
        }

        // Simulate a churn event whose signature has the given number of trailing zeros.
        let churn_id = ChurnId(
            signature_with_trailing_zeros(signature_trailing_zeros as u32)
                .to_bytes()
                .to_vec(),
        );

        let relocations =
            find_nodes_to_relocate(&network_knowledge, &churn_id, BTreeSet::default());

        let relocations: Vec<_> = relocations
            .into_iter()
            .map(|(_, details)| details)
            .collect();

        let allowed_relocations = if peers.len() > recommended_section_size() {
            min(elder_count() / 2, peers.len() - recommended_section_size())
        } else {
            0
        };

        // Only the oldest matching peers should be relocated.
        let expected_relocated_age = peers
            .iter()
            .map(Peer::age)
            .filter(|age| *age <= signature_trailing_zeros)
            .max();

        let mut expected_relocated_peers: Vec<_> = peers
            .iter()
            .filter(|peer| Some(peer.age()) == expected_relocated_age)
            .collect();
        let target_name = XorName::from_content(&churn_id.0);
        expected_relocated_peers
            .sort_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()));
        let expected_relocated_peers: Vec<_> = expected_relocated_peers
            .iter()
            .take(allowed_relocations)
            .collect();

        assert_eq!(expected_relocated_peers.len(), relocations.len());

        // Verify the relocate action is correct depending on whether the peer is elder or not.
        // NOTE: `zip` works here, as both collections are sorted by the same criteria.
        for (peer, details) in expected_relocated_peers.into_iter().zip(relocations) {
            assert_eq!(peer.name(), details.previous_name);
        }

        Ok(())
    }

    // Fetch a `bls::Signature` with the given number of trailing zeros. The signature is generated
    // from an unspecified random data using an unspecified random `SecretKey`. That is OK because
    // the relocation algorithm doesn't care about whether the signature is valid. It only
    // cares about its number of trailing zeros.
    fn signature_with_trailing_zeros(trailing_zeros_count: u32) -> bls::Signature {
        use std::{cell::RefCell, collections::HashMap};

        // Cache the signatures to avoid expensive re-computation.
        thread_local! {
            static CACHE: RefCell<HashMap<u32, bls::Signature>> = RefCell::new(HashMap::new());
        }

        CACHE.with(|cache| {
            cache
                .borrow_mut()
                .entry(trailing_zeros_count)
                .or_insert_with(|| gen_signature_with_trailing_zeros(trailing_zeros_count))
                .clone()
        })
    }

    fn gen_signature_with_trailing_zeros(trailing_zeros_count: u32) -> bls::Signature {
        let mut rng = SmallRng::seed_from_u64(0);
        let sk: bls::SecretKey = rng.gen();

        loop {
            let data: u64 = rng.gen();
            let signature = sk.sign(&data.to_be_bytes());

            if trailing_zeros(&signature.to_bytes()) == trailing_zeros_count {
                return signature;
            }
        }
    }

    // Generate Vec<Peer> where no two peers have the same name.
    fn arbitrary_unique_peers(
        count: impl Into<SizeRange>,
        age: impl Strategy<Value = u8>,
    ) -> impl Strategy<Value = Vec<Peer>> {
        proptest::collection::btree_map(arbitrary_bytes(), (any::<SocketAddr>(), age), count)
            .prop_map(|peers| {
                peers
                    .into_iter()
                    .map(|(mut bytes, (addr, age))| {
                        bytes[XOR_NAME_LEN - 1] = age;
                        let name = XorName(bytes);
                        Peer::new(name, addr)
                    })
                    .collect()
            })
    }

    fn arbitrary_bytes() -> impl Strategy<Value = [u8; XOR_NAME_LEN]> {
        any::<[u8; XOR_NAME_LEN]>()
    }
}
