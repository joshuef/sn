// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::ElderCandidates;

use crate::messaging::{
    system::{KeyedSig, SectionAuth}, SectionAuthorityProvider as SectionAuthorityProviderMsg,
};
use xor_name::{Prefix, XorName};
use crate::types::Peer;

use bls::{PublicKey, PublicKeySet};
use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};
// pub use sn_interface::messaging::system::{KeyedSig, SigShare};

// use super::{verify_sig, KeyedSig};
// use sn_interface::messaging::system::SectionAuth;
use secured_linked_list::SecuredLinkedList;
use serde::Serialize;

///
pub trait SectionAuthUtils<T: Serialize> {
    ///
    fn new(value: T, sig: KeyedSig) -> Self;

    ///
    fn verify(&self, section_chain: &SecuredLinkedList) -> bool;

    ///
    fn self_verify(&self) -> bool;
}

impl<T: Serialize> SectionAuthUtils<T> for SectionAuth<T> {
    fn new(value: T, sig: KeyedSig) -> Self {
        Self { value, sig }
    }

    fn verify(&self, section_chain: &SecuredLinkedList) -> bool {
        section_chain.has_key(&self.sig.public_key) && self.self_verify()
    }

    fn self_verify(&self) -> bool {
        // verify_sig(&self.sig, &self.value)
        bincode::serialize(&self.value)
            .map(|bytes| self.sig.verify(&bytes))
            .unwrap_or(false)
    }
}



/// Details of section authority.
///
/// A new `SectionAuthorityProvider` is created whenever the elders change, due to an elder being
/// added or removed, or the section splitting or merging.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SectionAuthorityProvider {
    prefix: Prefix,
    public_key_set: PublicKeySet,
    elders: BTreeSet<Peer>,
}

impl Display for SectionAuthorityProvider {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let elders_info: Vec<_> = self
            .elders
            .iter()
            .map(|peer| (peer.addr(), peer.name()))
            .collect();
        write!(
            f,
            "Sap {:?}  elder len:{} contains: {{{:?}}})",
            self.prefix,
            self.elders.len(),
            elders_info,
        )
    }
}

impl serde::Serialize for SectionAuthorityProvider {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as `SectionAuthorityProviderMsg`
        self.to_msg().serialize(serializer)
    }
}

impl SectionAuthorityProvider {
    /// Creates a new `SectionAuthorityProvider` with the given members, prefix and public keyset.
    pub fn new<I>(elders: I, prefix: Prefix, pk_set: PublicKeySet) -> Self
    where
        I: IntoIterator<Item = Peer>,
    {
        Self {
            prefix,
            public_key_set: pk_set,
            elders: elders.into_iter().collect(),
        }
    }

    /// Creates a new `SectionAuthorityProvider` from ElderCandidates and public keyset.
    pub fn from_elder_candidates(
        elder_candidates: ElderCandidates,
        pk_set: PublicKeySet,
    ) -> SectionAuthorityProvider {
        SectionAuthorityProvider {
            prefix: elder_candidates.prefix(),
            public_key_set: pk_set,
            elders: elder_candidates.elders().cloned().collect(),
        }
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn elders(&self) -> impl Iterator<Item = &Peer> + '_ {
        self.elders.iter()
    }

    /// A convenience function since we often use SAP elders as recipients.
    pub fn elders_vec(&self) -> Vec<Peer> {
        self.elders.iter().cloned().collect()
    }

    /// Returns `ElderCandidates`, which doesn't have key related infos.
    pub fn elder_candidates(&self) -> ElderCandidates {
        ElderCandidates::new(self.prefix, self.elders.iter().cloned())
    }

    /// Returns the number of elders in the section.
    pub fn elder_count(&self) -> usize {
        self.elders.len()
    }

    /// Returns a map of name to socket_addr.
    pub fn contains_elder(&self, name: &XorName) -> bool {
        self.elders.iter().any(|elder| &elder.name() == name)
    }

    /// Returns the elder `Peer` with the given `name`.
    pub fn get_elder(&self, name: &XorName) -> Option<&Peer> {
        self.elders.iter().find(|elder| elder.name() == *name)
    }

    /// Returns the set of elder names.
    pub fn names(&self) -> BTreeSet<XorName> {
        self.elders.iter().map(Peer::name).collect()
    }

    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.elders.iter().map(Peer::addr).collect()
    }

    /// Key of the section.
    pub fn section_key(&self) -> PublicKey {
        self.public_key_set.public_key()
    }

    // We prefer this over `From<...>` to make it easier to read the conversion.
    pub fn to_msg(&self) -> SectionAuthorityProviderMsg {
        SectionAuthorityProviderMsg {
            prefix: self.prefix,
            public_key_set: self.public_key_set.clone(),
            elders: self
                .elders
                .iter()
                .map(|elder| (elder.name(), elder.addr()))
                .collect(),
        }
    }
}

impl SectionAuth<SectionAuthorityProvider> {
    pub fn into_authed_msg(self) -> SectionAuth<SectionAuthorityProviderMsg> {
        SectionAuth {
            value: self.value.to_msg(),
            sig: self.sig,
        }
    }
}

impl SectionAuthorityProviderMsg {
    pub fn into_state(self) -> SectionAuthorityProvider {
        SectionAuthorityProvider {
            prefix: self.prefix,
            public_key_set: self.public_key_set,
            elders: self
                .elders
                .into_iter()
                .map(|(name, value)| Peer::new(name, value))
                .collect(),
        }
    }
}

impl SectionAuth<SectionAuthorityProviderMsg> {
    pub fn into_authed_state(self) -> SectionAuth<SectionAuthorityProvider> {
        SectionAuth {
            value: self.value.into_state(),
            sig: self.sig,
        }
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use safe_network::node::api::tests::SecretKeySet;
    use safe_network::node::{ed25519, NodeInfo, MIN_ADULT_AGE};
    use itertools::Itertools;
    use std::{cell::Cell, net::SocketAddr};
    use xor_name::Prefix;

    // Generate unique SocketAddr for testing purposes
    pub fn gen_addr() -> SocketAddr {
        thread_local! {
            static NEXT_PORT: Cell<u16> = Cell::new(1000);
        }

        let port = NEXT_PORT.with(|cell| cell.replace(cell.get().wrapping_add(1)));

        ([192, 0, 2, 0], port).into()
    }

    // Create `count` Nodes sorted by their names.
    // The `age_diff` flag is used to trigger nodes being generated with different age pattern.
    // The test of `handle_agreement_on_online_of_elder_candidate` requires most nodes to be with
    // age of MIN_AGE + 2 and one node with age of MIN_ADULT_AGE.
    pub fn gen_sorted_nodes(prefix: &Prefix, count: usize, age_diff: bool) -> Vec<NodeInfo> {
        (0..count)
            .map(|index| {
                let age = if age_diff && index < count - 1 {
                    MIN_ADULT_AGE + 1
                } else {
                    MIN_ADULT_AGE
                };
                NodeInfo::new(
                    ed25519::gen_keypair(&prefix.range_inclusive(), age),
                    gen_addr(),
                )
            })
            .sorted_by_key(|node| node.name())
            .collect()
    }

    // Generate random `SectionAuthorityProvider` for testing purposes.
    pub fn gen_section_authority_provider(
        prefix: Prefix,
        count: usize,
    ) -> (SectionAuthorityProvider, Vec<NodeInfo>, SecretKeySet) {
        let nodes = gen_sorted_nodes(&prefix, count, false);
        let elders = nodes.iter().map(NodeInfo::peer);

        let secret_key_set = SecretKeySet::random();
        let section_auth = SectionAuthorityProvider::from_elder_candidates(
            ElderCandidates::new(prefix, elders),
            secret_key_set.public_keys(),
        );

        (section_auth, nodes, secret_key_set)
    }
}
