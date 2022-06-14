// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{deserialise, serialise, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use sled::{Db, Tree};
use std::{fmt::Debug, marker::PhantomData};
use xor_name::XorName;

/// Disk storage for events and similar.
#[derive(Clone, Debug)]
pub(crate) struct EventStore<TEvent: Debug + Serialize + DeserializeOwned> {
    tree: Tree,
    _phantom: PhantomData<TEvent>,
}

impl<'a, TEvent: Debug + Serialize + DeserializeOwned> EventStore<TEvent>
where
    TEvent: 'a,
{
    /// Create a new event store
    pub(crate) fn new(id: &XorName, db: Db) -> Result<Self> {
        debug!("Creating new event store for {id}");
        let tree = match db.open_tree(id) {
            Ok(x) => x,
            Err(error) => {
                println!("sled db error: {:?}", error);
                return Err(Error::from(error));
            }
        };

        Ok(Self {
            tree,
            _phantom: PhantomData::default(),
        })
    }

    /// Get all events stored in db
    pub(crate) fn get_all(&self) -> Result<Vec<TEvent>> {
        let iter = self.tree.iter();

        let mut events = vec![];
        for (_, res) in iter.enumerate() {
            let (key, val) = res?;
            let db_key = String::from_utf8(key.to_vec())
                .map_err(|_| Error::CouldNotParseDbKey(key.to_vec()))?;

            let value: TEvent = deserialise(&val)?;
            events.push((db_key, value))
        }

        events.sort_by(|(key_a, _), (key_b, _)| key_a.partial_cmp(key_b).unwrap());

        let events: Vec<TEvent> = events.into_iter().map(|(_, val)| val).collect();

        Ok(events)
    }

    /// append a new entry
    pub(crate) fn append(&self, event: TEvent) -> Result<()> {
        let key = &self.tree.len().to_string();
        if self.tree.get(key)?.is_some() {
            return Err(Error::DataExists);
        }

        let event = serialise(&event)?;
        let _old_entry = self.tree.insert(key, event).map_err(Error::Sled)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::EventStore;
    use crate::node::{Error, Result};
    use sn_interface::types::Token;
    use std::path::Path;
    use tempfile::tempdir;
    use xor_name::XorName;

    #[tokio::test]
    async fn history() -> Result<()> {
        let id: XorName = xor_name::rand::random();
        let tmp_dir = tempdir()?;
        let db_dir = tmp_dir.path().join(Path::new(&"Token"));
        let db = sled::open(db_dir).map_err(|error| {
            trace!("Sled Error: {:?}", error);
            Error::Sled(error)
        })?;
        let store = EventStore::<Token>::new(&id, db)?;

        store.append(Token::from_nano(10))?;

        let events = store.get_all()?;
        assert_eq!(events.len(), 1);

        match events.get(0) {
            Some(token) => assert_eq!(token.as_nano(), 10),
            None => unreachable!(),
        }

        Ok(())
    }
}
