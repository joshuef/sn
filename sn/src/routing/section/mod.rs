// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(super) mod node_state;
pub(crate) mod section_authority_provider;
pub(super) mod section_keys;
mod section_peers;

#[cfg(test)]
pub(crate) use self::section_authority_provider::test_utils;

pub(super) use self::section_keys::{SectionKeyShare, SectionKeysProvider};

use crate::messaging::{
    system::{ElderCandidates, KeyedSig, NodeState, Peer, SectionAuth, SectionPeers},
    SectionAuthorityProvider,
};
use crate::routing::{
    dkg::SectionAuthUtils,
    error::{Error, Result},
    log_markers::LogMarker,
    peer::PeerUtils,
    ELDER_SIZE, RECOMMENDED_SECTION_SIZE,
};
use bls::PublicKey as BlsPublicKey;
pub(crate) use node_state::NodeStateUtils;
pub(crate) use section_authority_provider::ElderCandidatesUtils;
use section_authority_provider::SectionAuthorityProviderUtils;
use secured_linked_list::SecuredLinkedList;
use serde::Serialize;
use std::{collections::BTreeSet, convert::TryInto, iter, net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use xor_name::{Prefix, XorName};

/// Container for storing information about a section.
#[derive(Clone, Debug)]
/// All information about a section
pub(crate) struct Section {
    /// Network genesis key
    genesis_key: BlsPublicKey,
    /// The secured linked list of previous section keys, starting from genesis key
    pub(crate) chain: Arc<RwLock<SecuredLinkedList>>,
    /// Signed section authority
    section_auth: Arc<RwLock<SectionAuth<SectionAuthorityProvider>>>,
    /// Members of the section
    section_peers: SectionPeers,
}

impl Section {
    /// Creates a minimal `Section` initially containing only info about our elders
    /// (`section_auth`).
    ///
    /// Returns error if `section_auth` is not verifiable with the `chain`.
    pub(super) fn new(
        genesis_key: bls::PublicKey,
        chain: SecuredLinkedList,
        section_auth: SectionAuth<SectionAuthorityProvider>,
    ) -> Result<Self, Error> {
        if genesis_key != *chain.root_key() {
            return Err(Error::UntrustedProofChain(format!(
                "genesis key doesn't match first key in proof chain: {:?}",
                chain.root_key()
            )));
        }

        if section_auth.sig.public_key != *chain.last_key() {
            error!("can't create section: section_auth signed with incorrect key");
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "section key doesn't match last key in proof chain: {:?}",
                section_auth.value
            )));
        }

        // Check if SAP signature is valid
        if !section_auth.self_verify() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "invalid signature: {:?}",
                section_auth.value
            )));
        }

        // Check if SAP's section key matches SAP signature's key
        if section_auth.sig.public_key != section_auth.value.public_key_set.public_key() {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "section key doesn't match signature'ssss key: {:?}",
                section_auth.value
            )));
        }

        // Make sure the proof chain can be trusted,
        // i.e. check each key is signed by its parent/predecesor key.
        if !chain.self_verify() {
            return Err(Error::UntrustedProofChain(format!(
                "invalid chain: {:?}",
                chain
            )));
        }

        Ok(Self {
            genesis_key,
            chain: Arc::new(RwLock::new(chain)),
            section_auth: Arc::new(RwLock::new(section_auth)),
            section_peers: SectionPeers::default(),
        })
    }

    /// update all section info for our new section
    pub(super) async fn relocated_to(&self, new_section: Self) -> Result<()> {
        let mut chain = self.chain.write().await;
        *chain = new_section.chain().await;
        // don't hold write lock
        drop(chain);

        let mut section_auth = self.section_auth.write().await;
        *section_auth = new_section.section_auth.read().await.clone();
        // don't hold write lock
        drop(section_auth);

        self.merge_members(new_section.members().clone()).await?;

        Ok(())
    }

    /// Creates `Section` for the first node in the network
    pub(super) async fn first_node(
        peer: Peer,
        genesis_sk_set: bls::SecretKeySet,
    ) -> Result<(Section, SectionKeyShare)> {
        let public_key_set = genesis_sk_set.public_keys();
        let secret_key_share = genesis_sk_set.secret_key_share(0);

        let section_auth =
            create_first_section_authority_provider(&public_key_set, &secret_key_share, peer)?;

        let section = Section::new(
            section_auth.sig.public_key,
            SecuredLinkedList::new(section_auth.sig.public_key),
            section_auth,
        )?;

        for peer in section.section_auth.read().await.value.peers() {
            let node_state = NodeState::joined(peer, None);
            let sig = create_first_sig(&public_key_set, &secret_key_share, &node_state)?;
            let _changed = section.section_peers.update(SectionAuth {
                value: node_state,
                sig,
            });
        }

        let section_key_share = SectionKeyShare {
            public_key_set,
            index: 0,
            secret_key_share,
        };

        Ok((section, section_key_share))
    }

    pub(super) fn genesis_key(&self) -> &bls::PublicKey {
        &self.genesis_key
    }

    /// Try to merge this `Section` members with `other`. .
    pub(super) async fn merge_members(&self, peers: SectionPeers) -> Result<()> {
        for refmulti in peers.members.iter() {
            let info = refmulti.value().clone();
            let _changed = self.update_member(info).await;
        }

        self.section_peers
            .retain(&self.section_auth.read().await.value.prefix());

        Ok(())
    }

    /// Try to merge this `Section` with `other`. Returns `InvalidMessage` if `other` is invalid or
    /// its chain is not compatible with the chain of `self`.
    pub(super) async fn merge_chain(
        &self,
        other: &SectionAuth<SectionAuthorityProvider>,
        proof_chain: SecuredLinkedList,
    ) -> Result<()> {
        // We've been AE validated here.
        self.chain.write().await.merge(proof_chain)?;

        if &other.sig.public_key == self.chain.read().await.last_key() {
            *self.section_auth.write().await = other.clone();
        }
        Ok(())
    }

    /// Update the `SectionAuthorityProvider` of our section.
    pub(super) async fn update_elders(
        &self,
        new_section_auth: SectionAuth<SectionAuthorityProvider>,
        new_key_sig: KeyedSig,
    ) -> bool {
        let (our_prefix, section_key) = {
            let signed_sap = self.section_auth.read().await;
            (signed_sap.value.prefix, signed_sap.value.section_key())
        };

        if new_section_auth.value.prefix() != our_prefix
            && !new_section_auth.value.prefix().is_extension_of(&our_prefix)
        {
            return false;
        }

        if !new_section_auth.self_verify() {
            return false;
        }

        let new_section_key = new_section_auth.sig.public_key;
        let signed_by_key = new_key_sig.public_key;

        // Check if it's a SAP signed by current section key
        // TODO: this shall be checked by SecuredLinkedList automatically
        if section_key != signed_by_key {
            return false;
        }

        if let Err(error) =
            self.chain
                .write()
                .await
                .insert(&signed_by_key, new_section_key, new_key_sig.signature)
        {
            error!(
                "failed to insert key {:?} (signed with {:?}) into the section chain: {:?}",
                new_section_key, signed_by_key, error,
            );
            return false;
        }

        // Update our section's SAP
        let new_prefix = new_section_auth.value.prefix();
        *self.section_auth.write().await = new_section_auth;

        self.section_peers.retain(&new_prefix);

        true
    }

    /// Update the member. Returns whether it actually changed anything.
    pub(super) async fn update_member(&self, node_state: SectionAuth<NodeState>) -> bool {
        if !node_state.verify(&*self.chain.read().await) {
            error!("can't merge member {:?}", node_state.value);
            return false;
        }

        self.section_peers.update(node_state)
    }

    /// Return a copy of the section chain
    pub(super) async fn chain(&self) -> SecuredLinkedList {
        self.chain.read().await.clone()
    }

    /// Return current section key
    pub(super) async fn section_key(&self) -> bls::PublicKey {
        *self.chain.read().await.last_key()
    }

    /// Return current section chain length
    pub(crate) async fn main_chain_branch_len(&self) -> u64 {
        self.chain.read().await.main_branch_len() as u64
    }

    /// Return weather current section chain has the provided key
    pub(crate) async fn has_chain_key(&self, key: &bls::PublicKey) -> bool {
        self.chain.read().await.has_key(key)
    }

    /// Return a copy of current SAP
    pub(super) async fn authority_provider(&self) -> SectionAuthorityProvider {
        self.section_auth.read().await.value.clone()
    }

    /// Return a copy of current SAP with corresponding section authority
    pub(super) async fn section_signed_authority_provider(
        &self,
    ) -> SectionAuth<SectionAuthorityProvider> {
        self.section_auth.read().await.clone()
    }

    /// Return weather we are an Elder, by checking if we are one of
    /// the current section's SAP member,
    pub(super) async fn is_elder(&self, name: &XorName) -> bool {
        self.section_auth.read().await.value.contains_elder(name)
    }

    /// Generate a new section info(s) based on the current set of members,
    /// excluding any member matching a name in the provided `excluded_names` set.
    /// Returns a set of candidate SectionAuthorityProviders.
    pub(super) async fn promote_and_demote_elders(
        &self,
        our_name: &XorName,
        excluded_names: &BTreeSet<XorName>,
    ) -> Vec<ElderCandidates> {
        if let Some((our_elder_candidates, other_elder_candidates)) =
            self.try_split(our_name, excluded_names).await
        {
            return vec![our_elder_candidates, other_elder_candidates];
        }

        // Candidates for elders out of all the nodes in the section, even out of the
        // relocating nodes if there would not be enough instead.
        let sap = self.authority_provider().await;
        let expected_peers = self
            .section_peers
            .elder_candidates(ELDER_SIZE, &sap, excluded_names);

        let expected_names: BTreeSet<_> = expected_peers.iter().map(Peer::name).cloned().collect();
        let current_names: BTreeSet<_> = sap.names();

        if expected_names == current_names {
            vec![]
        } else if expected_names.len() < crate::routing::supermajority(current_names.len()) {
            warn!("ignore attempt to reduce the number of elders too much");
            vec![]
        } else {
            let elder_candidates = ElderCandidates::new(expected_peers, sap.prefix());
            vec![elder_candidates]
        }
    }

    /// Prefix of our section.
    pub(super) async fn prefix(&self) -> Prefix {
        self.section_auth.read().await.value.prefix
    }

    /// Return the list of our section's members
    pub(super) fn members(&self) -> &SectionPeers {
        &self.section_peers
    }

    /// Returns members that are either joined or are left but still elders.
    pub(super) async fn active_members(&self) -> Vec<Peer> {
        let mut active_members = vec![];
        let nodes = self.section_peers.all_members();
        for peer in nodes {
            if self.section_peers.is_joined(peer.name()) || self.is_elder(peer.name()).await {
                active_members.push(peer);
            }
        }

        active_members
    }

    /// Returns adults from our section.
    pub(super) async fn adults(&self) -> Vec<Peer> {
        let mut adults = vec![];
        let nodes = self.section_peers.mature();
        for peer in nodes {
            if !self.is_elder(peer.name()).await {
                adults.push(peer);
            }
        }

        adults
    }

    /// Returns live adults from our section.
    pub(super) async fn live_adults(&self) -> Vec<Peer> {
        let mut live_adults = vec![];

        for node_state in self.section_peers.joined() {
            if !self.is_elder(node_state.peer.name()).await {
                live_adults.push(node_state.peer)
            }
        }
        live_adults
    }

    pub(super) fn find_joined_member_by_addr(&self, addr: &SocketAddr) -> Option<Peer> {
        self.section_peers
            .joined()
            .into_iter()
            .find(|info| info.peer.addr() == addr)
            .map(|info| info.peer)
    }

    // Tries to split our section.
    // If we have enough mature nodes for both subsections, returns the SectionAuthorityProviders
    // of the two subsections. Otherwise returns `None`.
    pub(super) async fn try_split(
        &self,
        our_name: &XorName,
        excluded_names: &BTreeSet<XorName>,
    ) -> Option<(ElderCandidates, ElderCandidates)> {
        trace!("{}", LogMarker::SplitAttempt);
        if self.authority_provider().await.elders().len() < ELDER_SIZE {
            trace!("No attempt to split as our section does not have enough elders.");
            return None;
        }

        let next_bit_index = if let Ok(index) = self.prefix().await.bit_count().try_into() {
            index
        } else {
            debug!("at longest prefix");
            // Already at the longest prefix, can't split further.
            return None;
        };

        let next_bit = our_name.bit(next_bit_index);

        let (our_new_size, sibling_new_size) = self
            .section_peers
            .mature()
            .iter()
            .filter(|peer| !excluded_names.contains(peer.name()))
            .map(|peer| peer.name().bit(next_bit_index) == next_bit)
            .fold((0, 0), |(ours, siblings), is_our_prefix| {
                if is_our_prefix {
                    (ours + 1, siblings)
                } else {
                    (ours, siblings + 1)
                }
            });

        debug!(
            ">>>> our size {:?}, theirs {:?}",
            our_new_size, sibling_new_size
        );

        // If none of the two new sections would contain enough entries, return `None`.
        if our_new_size < RECOMMENDED_SECTION_SIZE || sibling_new_size < RECOMMENDED_SECTION_SIZE {
            debug!(">>>> returning here TOO SMALLLLLLLLL hmmmmmm");
            return None;
        }

        let our_prefix = self.prefix().await.pushed(next_bit);
        let other_prefix = self.prefix().await.pushed(!next_bit);

        let our_elders = self.section_peers.elder_candidates_matching_prefix(
            &our_prefix,
            ELDER_SIZE,
            &self.authority_provider().await,
            excluded_names,
        );
        let other_elders = self.section_peers.elder_candidates_matching_prefix(
            &other_prefix,
            ELDER_SIZE,
            &self.authority_provider().await,
            excluded_names,
        );

        let our_elder_candidates = ElderCandidates::new(our_elders, our_prefix);
        let other_elder_candidates = ElderCandidates::new(other_elders, other_prefix);

        debug!(">>>>> end of split attempt");
        Some((our_elder_candidates, other_elder_candidates))
    }
}

// Create `SectionAuthorityProvider` for the first node.
fn create_first_section_authority_provider(
    pk_set: &bls::PublicKeySet,
    sk_share: &bls::SecretKeyShare,
    mut peer: Peer,
) -> Result<SectionAuth<SectionAuthorityProvider>> {
    peer.set_reachable(true);
    let section_auth =
        SectionAuthorityProvider::new(iter::once(peer), Prefix::default(), pk_set.clone());
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
