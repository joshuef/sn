// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::dbcs::DbcReason;
use crate::network_knowledge::{Error, Result, SectionAuthorityProvider};
use crate::types::log_markers::LogMarker;

use itertools::Itertools;
use sn_dbc::{
    Commitment, DbcTransaction, Hash, IndexedSignatureShare, SpentProofContent, SpentProofShare,
};
use uluru::LRUCache;

const KEY_CACHE_SIZE: usize = 50;

/// All the key material needed to sign or combine signature for our section key.
#[derive(custom_debug::Debug, Clone)]
pub struct SectionKeyShare {
    /// Public key set to verify threshold signatures and combine shares.
    pub public_key_set: bls::PublicKeySet,
    /// Index of the owner of this key share within the set of all section elders.
    pub index: usize,
    /// Secret Key share.
    #[debug(skip)]
    pub secret_key_share: bls::SecretKeyShare,
}

/// Struct that holds the current section keys and helps with new key generation.
/// Implementation of super simple cache, for no more than a handfull of items.
#[derive(Debug, Clone)]
pub struct SectionKeysProvider {
    /// A cache for current and previous section BLS keys.
    cache: Box<LRUCache<SectionKeyShare, KEY_CACHE_SIZE>>,
}

impl PartialEq for SectionKeyShare {
    fn eq(&self, other: &Self) -> bool {
        self.public_key_set == other.public_key_set
            && self.index == other.index
            && self.secret_key_share == other.secret_key_share
    }
}

impl PartialEq for SectionKeysProvider {
    fn eq(&self, other: &Self) -> bool {
        self.cache.iter().cloned().collect_vec() == other.cache.iter().cloned().collect_vec()
    }
}
impl SectionKeysProvider {
    /// Constructor.
    pub fn new(current: Option<SectionKeyShare>) -> Self {
        let mut section_keys_provider = Self {
            cache: Box::default(),
        };

        if let Some(share) = current {
            section_keys_provider.insert(share);
        }
        section_keys_provider
    }

    /// Resets the cache
    pub fn wipe(&mut self) {
        self.cache.clear();
    }

    /// Returns the most recently added key.
    pub fn key_share(&self, public_key: &bls::PublicKey) -> Result<SectionKeyShare> {
        match self
            .cache
            .iter()
            .find(|share| public_key == &share.public_key_set.public_key())
        {
            Some(key_share) => Ok(key_share.clone()),
            None => Err(Error::MissingSecretKeyShare(*public_key)),
        }
    }

    /// Uses the secret key from cache, corresponding to
    /// the provided public key.
    pub fn sign_with(
        &self,
        data: &[u8],
        public_key: &bls::PublicKey,
    ) -> Result<(usize, bls::SignatureShare)> {
        let key_share = self.key_share(public_key)?;

        Ok((key_share.index, key_share.secret_key_share.sign(data)))
    }

    /// Returns true if no key share exists.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Adds a new key to the cache, and removes the oldest
    /// key if cache size is exceeded.
    pub fn insert(&mut self, share: SectionKeyShare) {
        let public_key = share.public_key_set.public_key();
        if let Some(evicted) = self.cache.insert(share) {
            trace!("evicted old key share from cache: {:?}", evicted);
        }
        let cache_len = self.cache.len();
        trace!(
            "{} in cache (total {cache_len}): {public_key:?}",
            LogMarker::NewKeyShareStored,
        );
    }
}

/// Builds a spent proof share based on the given inputs.
///
/// This code is shared between `safenode` and `sn_client`. The node needs it both for validating a
/// spend request and for test setup, and the client needs it for test setup. This is not a perfect
/// location for it, but since it makes use of the section key provider I thought it could sit
/// alongside it.
pub fn build_spent_proof_share(
    public_key: &bls::PublicKey,
    tx: &DbcTransaction,
    reason: DbcReason,
    sap: &SectionAuthorityProvider,
    skp: &SectionKeysProvider,
    public_commitment: Commitment,
) -> Result<SpentProofShare> {
    let content = SpentProofContent {
        public_key: *public_key,
        transaction_hash: Hash::from(tx.hash()),
        reason: reason.into(),
        public_commitment,
    };
    let (index, sig_share) = skp.sign_with(content.hash().as_ref(), &sap.section_key())?;
    Ok(SpentProofShare {
        content,
        spentbook_pks: sap.public_key_set(),
        spentbook_sig_share: IndexedSignatureShare::new(index as u64, sig_share),
    })
}
