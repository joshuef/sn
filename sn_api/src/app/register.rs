// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub use sn_interface::types::register::{Entry, EntryHash};

use crate::safeurl::{ContentType, SafeUrl, XorUrl};
use crate::{Error, Result, Safe};

use log::debug;
use rand::Rng;
use sn_interface::types::{
    register::{
        Policy, PrivatePermissions, PrivatePolicy, PublicPermissions, PublicPolicy, User,
    },
    DataAddress, Error as SafeNdError, RegisterAddress, Scope,
};
use safe_network::{
    client::Error as ClientError,
};
use std::collections::{BTreeMap, BTreeSet};
use tracing::info;
use xor_name::XorName;

impl Safe {
    // === Register data operations ===
    /// Create a Register on the network
    pub async fn register_create(
        &self,
        name: Option<XorName>,
        tag: u64,
        private: bool,
        content_type: ContentType,
    ) -> Result<XorUrl> {
        debug!(
            "Storing {} Register data with tag type: {}, xorname: {:?}, dry_run: {}",
            if private { "Private" } else { "Public" },
            tag,
            name,
            self.dry_run_mode
        );

        let xorname = name.unwrap_or_else(rand::random);
        info!("Xorname for new Register storage: {:?}", &xorname);

        let scope = if private {
            Scope::Private
        } else {
            Scope::Public
        };

        let xorurl = SafeUrl::encode_register(xorname, tag, scope, content_type, self.xorurl_base)?;

        // return early if dry_run_mode
        if self.dry_run_mode {
            return Ok(xorurl);
        }

        // The Register's owner will be the client's public key
        let client = self.get_safe_client()?;
        let my_pk = User::Key(client.public_key());

        // Store the Register on the network
        let policy = if private {
            private_policy(my_pk)
        } else {
            public_policy(my_pk)
        };

        let (_, op_batch) = client
            .create_register(xorname, tag, policy)
            .await
            .map_err(|e| {
                Error::NetDataError(format!(
                    "Failed to prepare store Private Register operation: {:?}",
                    e
                ))
            })?;

        client.publish_register_ops(op_batch).await?;

        Ok(xorurl)
    }

    /// Read value from a Register on the network
    pub async fn register_read(&self, url: &str) -> Result<BTreeSet<(EntryHash, Entry)>> {
        debug!("Getting Public Register data from: {:?}", url);
        let safeurl = self.parse_and_resolve_url(url).await?;

        self.register_fetch_entries(&safeurl).await
    }

    /// Read value from a Register on the network by its hash
    pub async fn register_read_entry(&self, url: &str, hash: EntryHash) -> Result<Entry> {
        debug!("Getting Public Register data from: {:?}", url);
        let safeurl = self.parse_and_resolve_url(url).await?;

        self.register_fetch_entry(&safeurl, hash).await
    }

    /// Fetch a Register from a SafeUrl without performing any type of URL resolution
    /// Supports version hashes:
    /// e.g. safe://mysafeurl?v=ce56a3504c8f27bfeb13bdf9051c2e91409230ea
    pub(crate) async fn register_fetch_entries(
        &self,
        url: &SafeUrl,
    ) -> Result<BTreeSet<(EntryHash, Entry)>> {
        debug!("Fetching Register entries from {}", url);
        let result = match url.content_version() {
            Some(v) => {
                let hash = v.entry_hash();
                debug!("Take entry with version hash: {:?}", hash);
                self.register_fetch_entry(url, hash)
                    .await
                    .map(|entry| vec![(hash, entry)].into_iter().collect())
            }
            None => {
                debug!("No version so take latest entry from Register at: {}", url);
                let address = self.get_register_address(url)?;
                let client = self.get_safe_client()?;
                client.read_register(address).await.map_err(|err| {
                    if let ClientError::NetworkDataError(SafeNdError::NoSuchEntry) = err {
                        Error::EmptyContent(format!("Empty Register found at {:?}", address))
                    } else {
                        Error::NetDataError(format!(
                            "Failed to read latest value from Register data: {:?}",
                            err
                        ))
                    }
                })
            }
        };

        match result {
            Ok(data) => {
                debug!("Register retrieved from {}...", url);
                Ok(data)
            }
            Err(Error::EmptyContent(_)) => Err(Error::EmptyContent(format!(
                "Register found at \"{}\" was empty",
                url
            ))),
            Err(Error::ContentNotFound(_)) => Err(Error::ContentNotFound(format!(
                "No Register found at \"{}\"",
                url
            ))),
            other_err => other_err,
        }
    }

    /// Fetch a Register from a SafeUrl without performing any type of URL resolution
    pub(crate) async fn register_fetch_entry(
        &self,
        url: &SafeUrl,
        hash: EntryHash,
    ) -> Result<Entry> {
        // TODO: allow to specify the hash with the SafeUrl as well: safeurl.content_hash(),
        // e.g. safe://mysafeurl#ce56a3504c8f27bfeb13bdf9051c2e91409230ea
        let address = self.get_register_address(url)?;
        let client = self.get_safe_client()?;
        client
            .get_register_entry(address, hash)
            .await
            .map_err(|err| {
                if let ClientError::ErrorMsg {
                    source: sn_interface::messaging::data::Error::NoSuchEntry,
                    ..
                } = err
                {
                    Error::HashNotFound(hash)
                } else {
                    Error::NetDataError(format!(
                        "Failed to retrieve entry with hash '{}' from Register data: {:?}",
                        hex::encode(hash.0),
                        err
                    ))
                }
            })
    }

    /// Write value to a Register on the network
    pub async fn register_write(
        &self,
        url: &str,
        entry: Entry,
        parents: BTreeSet<EntryHash>,
    ) -> Result<EntryHash> {
        let reg_url = self.parse_and_resolve_url(url).await?;
        let address = self.get_register_address(&reg_url)?;
        if self.dry_run_mode {
            return Ok(EntryHash(rand::thread_rng().gen::<[u8; 32]>()));
        }

        let client = self.get_safe_client()?;
        let (entry_hash, op_batch) = client.write_to_register(address, entry, parents).await?;

        client.publish_register_ops(op_batch).await?;

        Ok(entry_hash)
    }

    pub(crate) fn get_register_address(&self, url: &SafeUrl) -> Result<RegisterAddress> {
        let address = match url.address() {
            DataAddress::Register(reg_address) => reg_address,
            other => {
                return Err(Error::ContentError(format!(
                    "The url {} has an {:?} address. \
                    To fetch register entries, this url must refer to a register.",
                    url, other
                )))
            }
        };
        Ok(address)
    }
}

fn private_policy(owner: User) -> Policy {
    let mut permissions = BTreeMap::new();
    let _ = permissions.insert(owner, PrivatePermissions::new(true, true));
    Policy::Private(PrivatePolicy { owner, permissions })
}

fn public_policy(owner: User) -> Policy {
    let mut permissions = BTreeMap::new();
    let _ = permissions.insert(owner, PublicPermissions::new(true));
    Policy::Public(PublicPolicy { owner, permissions })
}

#[cfg(test)]
mod tests {
    use crate::{app::test_helpers::new_safe_instance, ContentType};
    use anyhow::Result;

    #[tokio::test]
    async fn test_register_create() -> Result<()> {
        let safe = new_safe_instance().await?;

        let xorurl = safe
            .register_create(None, 25_000, false, ContentType::Raw)
            .await?;
        let xorurl_priv = safe
            .register_create(None, 25_000, true, ContentType::Raw)
            .await?;

        let received_data = safe.register_read(&xorurl).await?;
        let received_data_priv = safe.register_read(&xorurl_priv).await?;

        assert!(received_data.is_empty());
        assert!(received_data_priv.is_empty());

        let initial_data = "initial data bytes".as_bytes().to_vec();
        let hash = safe
            .register_write(&xorurl, initial_data.clone(), Default::default())
            .await?;
        let hash_priv = safe
            .register_write(&xorurl_priv, initial_data.clone(), Default::default())
            .await?;

        let received_entry = safe.register_read_entry(&xorurl, hash).await?;
        let received_entry_priv = safe.register_read_entry(&xorurl_priv, hash_priv).await?;

        assert_eq!(received_entry, initial_data.clone());
        assert_eq!(received_entry_priv, initial_data);

        Ok(())
    }
}
