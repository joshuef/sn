// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::network_knowledge::{Error, Result};
use crate::types::log_markers::LogMarker;

use std::sync::Arc;
use tokio::sync::RwLock;
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
    cache: Arc<RwLock<LRUCache<SectionKeyShare, KEY_CACHE_SIZE>>>,
}

impl SectionKeysProvider {
    /// Constructor.
    pub async fn new(current: Option<SectionKeyShare>) -> Self {
        let section_keys_provider = Self {
            cache: Arc::new(RwLock::new(LRUCache::default())),
        };

        if let Some(share) = current {
            section_keys_provider.insert(share).await;
        }
        section_keys_provider
    }

    /// Resets the cache
    pub async fn wipe(&self) {
        self.cache.write().await.clear();
    }

    /// Returns the most recently added key.
    pub async fn key_share(&self, public_key: &bls::PublicKey) -> Result<SectionKeyShare> {
        match self
            .cache
            .write()
            .await
            .find(|share| public_key == &share.public_key_set.public_key())
        {
            Some(key_share) => Ok(key_share.clone()),
            None => Err(Error::MissingSecretKeyShare(*public_key)),
        }
    }

    /// Uses the secret key from cache, corresponding to
    /// the provided public key.
    pub async fn sign_with(
        &self,
        data: &[u8],
        public_key: &bls::PublicKey,
    ) -> Result<(usize, bls::SignatureShare)> {
        let key_share = self.key_share(public_key).await?;

        Ok((key_share.index, key_share.secret_key_share.sign(data)))
    }

    /// Returns true if no key share exists.
    pub async fn is_empty(&self) -> bool {
        self.cache.read().await.is_empty()
    }

    /// Adds a new key to the cache, and removes the oldest
    /// key if cache size is exceeded.
    pub async fn insert(&self, share: SectionKeyShare) {
        let public_key = share.public_key_set.public_key();
        if let Some(evicted) = self.cache.write().await.insert(share) {
            trace!("evicted old key share from cache: {:?}", evicted);
        }
        let cache_len = self.cache.read().await.len();
        trace!(
            "{} in cache (total {}): {:?}",
            LogMarker::NewKeyShareStored,
            cache_len,
            public_key
        );
    }
}
