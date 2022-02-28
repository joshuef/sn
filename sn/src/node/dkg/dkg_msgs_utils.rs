// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::ed25519::{self, Digest256, Keypair, Verifier};
use sn_interface::messaging::system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId};
use sn_interface::network_knowledge::{supermajority, ElderCandidates};
use std::collections::BTreeSet;
use tiny_keccak::{Hasher, Sha3};
use xor_name::XorName;

pub(crate) trait DkgSessionIdUtils {
    fn new(elder_candidates: &ElderCandidates, generation: u64) -> Self;
}

impl DkgSessionIdUtils for DkgSessionId {
    fn new(elder_candidates: &ElderCandidates, generation: u64) -> Self {
        // Calculate the hash without involving serialization to avoid having to return `Result`.
        let mut hasher = Sha3::v256();
        let mut hash = Digest256::default();

        for peer in elder_candidates.names() {
            hasher.update(&peer.0);
        }

        hasher.update(&elder_candidates.prefix().name().0);
        hasher.update(&elder_candidates.prefix().bit_count().to_le_bytes());
        hasher.finalize(&mut hash);

        Self { hash, generation }
    }
}

pub(crate) trait DkgFailureSigUtils {
    fn new(
        keypair: &Keypair,
        failed_participants: &BTreeSet<XorName>,
        dkg_key: &DkgSessionId,
    ) -> Self;

    fn verify(&self, dkg_key: &DkgSessionId, failed_participants: &BTreeSet<XorName>) -> bool;
}

impl DkgFailureSigUtils for DkgFailureSig {
    fn new(
        keypair: &Keypair,
        failed_participants: &BTreeSet<XorName>,
        dkg_key: &DkgSessionId,
    ) -> Self {
        DkgFailureSig {
            public_key: keypair.public,
            signature: ed25519::sign(&hashed_failure(dkg_key, failed_participants), keypair),
        }
    }

    fn verify(&self, dkg_key: &DkgSessionId, failed_participants: &BTreeSet<XorName>) -> bool {
        let hash = hashed_failure(dkg_key, failed_participants);
        self.public_key.verify(&hash, &self.signature).is_ok()
    }
}

pub(crate) trait DkgFailureSigSetUtils {
    fn insert(&mut self, sig: DkgFailureSig, failed_participants: &BTreeSet<XorName>) -> bool;

    fn has_agreement(&self, elder_candidates: &ElderCandidates) -> bool;

    fn verify(&self, elder_candidates: &ElderCandidates, generation: u64) -> bool;
}

impl DkgFailureSigSetUtils for DkgFailureSigSet {
    // Insert a signature into this set. The signature is assumed valid. Returns `true` if the signature was
    // not already present in the set and `false` otherwise.
    fn insert(&mut self, sig: DkgFailureSig, failed_participants: &BTreeSet<XorName>) -> bool {
        if self.failed_participants.is_empty() {
            self.failed_participants = failed_participants.clone();
        }
        if self
            .sigs
            .iter()
            .all(|existing_sig| existing_sig.public_key != sig.public_key)
        {
            self.sigs.push(sig);
            true
        } else {
            false
        }
    }

    // Check whether we have enough signatures to reach agreement on the failure. The contained signatures
    // are assumed valid.
    fn has_agreement(&self, elder_candidates: &ElderCandidates) -> bool {
        has_failure_agreement(elder_candidates.len(), self.sigs.len())
    }

    fn verify(&self, elder_candidates: &ElderCandidates, generation: u64) -> bool {
        let hash = hashed_failure(
            &DkgSessionId::new(elder_candidates, generation),
            &self.failed_participants,
        );
        let votes = self
            .sigs
            .iter()
            .filter(|sig| {
                elder_candidates.contains(&ed25519::name(&sig.public_key))
                    && sig.public_key.verify(&hash, &sig.signature).is_ok()
            })
            .count();

        has_failure_agreement(elder_candidates.len(), votes)
    }
}

// Check whether we have enough signeds to reach agreement on the failure. We only need
// `N - supermajority(N) + 1` signeds, because that already makes a supermajority agreement on a
// successful outcome impossible.
fn has_failure_agreement(num_participants: usize, num_votes: usize) -> bool {
    num_votes > num_participants - supermajority(num_participants)
}

// Create a value whose signature serves as the signed that a failure of a DKG session with the given
// `dkg_key` was observed.
fn hashed_failure(dkg_key: &DkgSessionId, failed_participants: &BTreeSet<XorName>) -> Digest256 {
    let mut hasher = Sha3::v256();
    let mut hash = Digest256::default();
    hasher.update(&dkg_key.hash);
    hasher.update(&dkg_key.generation.to_le_bytes());
    for name in failed_participants.iter() {
        hasher.update(&name.0);
    }
    hasher.update(b"failure");
    hasher.finalize(&mut hash);
    hash
}
