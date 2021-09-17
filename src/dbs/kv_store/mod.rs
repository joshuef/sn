// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! A simple, persistent, disk-based key-value store.

mod kv;
#[cfg(test)]
mod tests;

pub(super) mod to_db_key;
pub(super) mod used_space;

pub(crate) use kv::{Key, Value};

use super::{
    deserialise, Subdir,
    {encoding::serialise, Error, Result},
};
use serde::de::DeserializeOwned;
use sled::Db;
use std::{marker::PhantomData, path::Path};
use to_db_key::ToDbKey;
use tracing::info;
use used_space::UsedSpace;

const DB_DIR: &str = "db";

/// `KvStore` is a store of keys and values into a Sled db, while maintaining a maximum disk
/// usage to restrict storage.
#[derive(Clone, Debug)]
pub(crate) struct KvStore<K, V> {
    // tracks space used.
    used_space: UsedSpace,
    db: Db,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K: Key, V: Value> KvStore<K, V>
where
    Self: Subdir,
{
    /// Creates a new `KvStore` at location `root/STORE_DIR/<type>`.
    ///
    /// If the location specified already exists, the previous KvStore there is opened, otherwise
    /// the required folder structure is created.
    ///
    /// Used space of the dir is tracked.
    pub(crate) fn new<P: AsRef<Path>>(root: P, used_space: UsedSpace) -> Result<Self> {
        let dir = root.as_ref().join(DB_DIR).join(Self::subdir());

        // used_space.add_dir(&dir);

        let db = sled::open(&dir).map_err(Error::from)?;

        Ok(KvStore {
            used_space,
            db,
            _k: PhantomData,
            _v: PhantomData,
        })
    }
}

impl<K: Key, V: Value + Send + Sync> KvStore<K, V> {
    ///
    pub(crate) async fn total_used_space(&self) -> u64 {
        self.used_space.total().await
    }

    /// Tests if a value has been previously stored under `key`.
    pub(crate) fn has(&self, key: &V::Key) -> Result<bool> {
        let key = key.to_db_key()?;
        self.db.contains_key(key).map_err(Error::from)
    }

    /// Deletes the value stored under `key`.
    ///
    /// If the data doesn't exist, it does nothing and returns `Ok`.  In the case of an IO error, it
    /// returns `Error::Io`.
    pub(crate) fn delete(&self, key: &V::Key) -> Result<()> {
        let key = key.to_db_key()?;
        self.db.remove(key).map_err(Error::from).map(|_| ())
    }

    /// Stores a new value.
    ///
    /// If there is not enough storage space available, returns `Error::NotEnoughSpace`.  In case of
    /// an IO error, it returns `Error::Io`.
    ///
    /// If a value with the same id already exists, it will be overwritten.
    pub(crate) async fn store(&self, value: &V) -> Result<()> {
        debug!("Writing value to KV store");

        let key = value.key().to_db_key()?;
        let exists = self.db.contains_key(key.clone()).map_err(Error::from)?;

        trace!(">> Does this entry exist; {:?}: {:?}", key, exists);

        let serialised_value = serialise(value)?.to_vec();
        // FIXME: We're not considering overwriting here. So the space isn't necessarily all 'consumed'
        // if we overwrite same value eg, or it's only 5 bytes longer/shorter etc
        let consumed_space = serialised_value.len() as u64;

        info!("consumed space: {:?}", consumed_space);
        if !self.used_space.can_consume(consumed_space).await {
            return Err(Error::NotEnoughSpace);
        }

        let res = self.db.insert(key, serialised_value);

        match res {
            Ok(_) => {
                info!("Writing value succeeded!");
                Ok(())
            }
            Err(e) => {
                info!("Writing value failed!");
                Err(Error::Sled(e))
            }
        }
    }

    /// Stores a batch of values.
    ///
    /// If there is not enough storage space available, returns `Error::NotEnoughSpace`.  In case of
    /// an IO error, it returns `Error::Io`.
    ///
    /// If a value with the same id already exists, it will be overwritten.
    #[allow(unused)] // this will soon be used
    pub(crate) async fn store_batch(&self, values: &[V]) -> Result<()> {
        info!("Writing batch");
        use rayon::prelude::*;
        type KvPair = (String, Vec<u8>);
        type KvPairResults = Vec<Result<KvPair>>;

        let mut batch = sled::Batch::default();
        let (ok, err_results): (KvPairResults, KvPairResults) = values
            .par_iter()
            .map(|value| {
                let serialised_value = serialise(value)?.to_vec();
                let key = value.key().to_db_key()?;
                Ok((key, serialised_value))
            })
            .partition(|r| r.is_ok());

        if !err_results.is_empty() {
            for e in err_results {
                error!("{:?}", e);
            }

            return Err(Error::SledBatching);
        }

        let consumed_space = ok
            .into_iter()
            .flatten()
            .map(|(key, value)| {
                let consumed_space = value.len() as u64;
                batch.insert(key.as_bytes(), value);
                consumed_space
            })
            .sum();

        if !self.used_space.can_consume(consumed_space).await {
            return Err(Error::NotEnoughSpace);
        }

        let res = self.db.apply_batch(batch);

        match res {
            Ok(_) => {
                info!("Writing batch succeeded!");
                Ok(())
            }
            Err(e) => {
                warn!("Writing batch failed!");
                Err(Error::Sled(e))
            }
        }
    }

    /// Returns a value previously stored under `key`.
    ///
    /// If the value can't be accessed, it returns `Error::NoSuchData`.
    pub(crate) fn get(&self, key: &V::Key) -> Result<V> {
        let db_key = key.to_db_key()?;
        let res = self
            .db
            .get(db_key.clone())
            .map_err(|_| Error::KeyNotFound(db_key.clone()))?;

        if let Some(data) = res {
            let value: V = deserialise(&data)?;
            // Check it's the requested value.
            if value.key() == key {
                return Ok(value);
            }
        }

        Err(Error::KeyNotFound(db_key))
    }

    /// Used space to max capacity ratio.
    pub(crate) async fn used_space_ratio(&self) -> f64 {
        let used = self.total_used_space().await;
        let max_capacity = self.used_space.max_capacity();
        let used_space_ratio = used as f64 / max_capacity as f64;
        info!("Used space: {:?}", used);
        info!("Max capacity: {:?}", max_capacity);
        info!("Used space ratio: {:?}", used_space_ratio);
        used_space_ratio
    }

    /// Lists all keys of currently stored data.
    #[cfg_attr(not(test), allow(unused))]
    pub(crate) fn keys(&self) -> Result<Vec<V::Key>> {
        let keys = self
            .db
            .iter()
            .flatten()
            .map(|(key, _)| key)
            .map(convert)
            .flatten()
            .collect();
        Ok(keys)
    }
}

pub(crate) fn convert<T: DeserializeOwned>(key: sled::IVec) -> Result<T> {
    let db_key = &String::from_utf8(key.to_vec()).map_err(|_| Error::CouldNotConvertDbKey)?;
    to_db_key::from_db_key(db_key)
}
