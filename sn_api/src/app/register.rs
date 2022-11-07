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

use sn_client::Error as ClientError;
use sn_interface::{
    messaging::data::Error as ErrorMsg,
    types::{
        register::{Permissions, Policy, User},
        DataAddress, Error as SafeNdError, RegisterAddress,
    },
};

use log::debug;
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet};
use tracing::info;
use xor_name::XorName;

impl Safe {
    // === Register data operations ===
    /// Create a Register on the network
    pub async fn register_create(
        &mut self,
        name: Option<XorName>,
        tag: u64,
        content_type: ContentType,
    ) -> Result<XorUrl> {
        debug!(
            "Storing Register data with tag type: {}, xorname: {:?}, dry_run: {}",
            tag, name, self.dry_run_mode
        );

        let xorname = name.unwrap_or_else(xor_name::rand::random);
        info!("Xorname for new Register storage: {:?}", &xorname);

        let xorurl = SafeUrl::from_register(xorname, tag, content_type)?.encode(self.xorurl_base);

        // return early if dry_run_mode
        if self.dry_run_mode {
            return Ok(xorurl);
        }

        // The Register's owner will be the client's public key
        let mut client = self.get_safe_client()?;
        let owner = User::Key(client.public_key());

        // Store the Register on the network
        let (_, op_batch) = client
            .create_register(xorname, tag, policy(owner))
            .await
            .map_err(|e| {
                Error::NetDataError(format!(
                    "Failed to prepare store Register operation: {:?}",
                    e
                ))
            })?;

        client.publish_register_ops(op_batch).await?;

        Ok(xorurl)
    }

    /// Read value from a Register on the network
    pub async fn register_read(&mut self, url: &str) -> Result<BTreeSet<(EntryHash, Entry)>> {
        debug!("Getting Register data from: {:?}", url);
        let safeurl = self.parse_and_resolve_url(url).await?;

        self.register_fetch_entries(&safeurl).await
    }

    /// Read value from a Register on the network by its hash
    pub async fn register_read_entry(&mut self, url: &str, hash: EntryHash) -> Result<Entry> {
        debug!("Getting Register data from: {:?}", url);
        let safeurl = self.parse_and_resolve_url(url).await?;

        self.register_fetch_entry(&safeurl, hash).await
    }

    /// Fetch a Register from a `SafeUrl` without performing any type of URL resolution
    /// Supports version hashes:
    /// e.g. safe://mysafeurl?v=ce56a3504c8f27bfeb13bdf9051c2e91409230ea
    pub(crate) async fn register_fetch_entries(
        &mut self,
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
                let mut client = self.get_safe_client()?;
                match client.read_register(address).await {
                    Ok(entry) => Ok(entry),
                    Err(ClientError::NetworkDataError(SafeNdError::NoSuchEntry(_))) => Err(
                        Error::EmptyContent(format!("Empty Register found at \"{}\"", url)),
                    ),
                    Err(ClientError::ErrorMsg {
                        source: ErrorMsg::AccessDenied(_),
                        ..
                    }) => Err(Error::AccessDenied(format!(
                        "Couldn't read entry from Register found at \"{}\"",
                        url
                    ))),
                    Err(err) => Err(Error::NetDataError(format!(
                        "Failed to read latest value from Register data: {:?}",
                        err
                    ))),
                }
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

    /// Fetch a Register from a `SafeUrl` without performing any type of URL resolution
    pub(crate) async fn register_fetch_entry(
        &mut self,
        url: &SafeUrl,
        hash: EntryHash,
    ) -> Result<Entry> {
        // TODO: allow to specify the hash with the SafeUrl as well: safeurl.content_hash(),
        // e.g. safe://mysafeurl#ce56a3504c8f27bfeb13bdf9051c2e91409230ea
        let address = self.get_register_address(url)?;
        let mut client = self.get_safe_client()?;
        client
            .get_register_entry(address, hash)
            .await
            .map_err(|err| {
                if let ClientError::ErrorMsg {
                    source: sn_interface::messaging::data::Error::NoSuchEntry(_),
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
        &mut self,
        url: &str,
        entry: Entry,
        parents: BTreeSet<EntryHash>,
    ) -> Result<EntryHash> {
        let reg_url = self.parse_and_resolve_url(url).await?;
        let address = self.get_register_address(&reg_url)?;
        if self.dry_run_mode {
            return Ok(EntryHash(rand::thread_rng().gen::<[u8; 32]>()));
        }

        let mut client = self.get_safe_client()?;
        let (entry_hash, op_batch) = match client
            .write_to_local_register(address, entry, parents)
            .await
        {
            Ok(data) => data,
            Err(
                ClientError::NetworkDataError(SafeNdError::AccessDenied(_))
                | ClientError::ErrorMsg {
                    source: ErrorMsg::AccessDenied(_),
                    ..
                },
            ) => {
                return Err(Error::AccessDenied(format!(
                    "Couldn't write data on Register found at \"{}\"",
                    url
                )));
            }
            Err(err) => {
                return Err(Error::NetDataError(format!(
                    "Failed to write data on Register: {:?}",
                    err
                )));
            }
        };

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

fn policy(owner: User) -> Policy {
    let mut permissions = BTreeMap::new();
    let _ = permissions.insert(owner, Permissions::new(true));
    Policy { owner, permissions }
}

#[cfg(test)]
mod tests {
    use crate::{app::test_helpers::new_safe_instance, ContentType, Error};
    use anyhow::{bail, Result};

    #[tokio::test]
    async fn test_register_create() -> Result<()> {
        let mut safe = new_safe_instance().await?;

        let xorurl = safe.register_create(None, 25_000, ContentType::Raw).await?;

        let received_data = safe.register_read(&xorurl).await?;

        assert!(received_data.is_empty());

        let initial_data = "initial data bytes".as_bytes().to_vec();
        let hash = safe
            .register_write(&xorurl, initial_data.clone(), Default::default())
            .await?;

        let received_entry = safe.register_read_entry(&xorurl, hash).await?;

        assert_eq!(received_entry, initial_data.clone());

        Ok(())
    }

    #[tokio::test]
    async fn test_register_owner_permissions() -> Result<()> {
        let mut safe = new_safe_instance().await?;

        let xorname = xor_name::rand::random();

        let xorurl = safe
            .register_create(Some(xorname), 25_000, ContentType::Raw)
            .await?;

        let received_data = safe.register_read(&xorurl).await?;

        assert!(received_data.is_empty());

        // now we check that trying to write to the same Registers with different owner shall fail
        let mut safe = new_safe_instance().await?;

        match safe
            .register_write(&xorurl, b"dummy-pub-data".to_vec(), Default::default())
            .await
        {
            Err(Error::AccessDenied(msg)) => {
                assert_eq!(
                    msg,
                    format!("Couldn't write data on Register found at \"{}\"", xorurl)
                );
            }
            Err(err) => bail!("Error returned is not the expected: {:?}", err),
            Ok(_) => bail!("Creation of Register succeeded unexpectedly".to_string()),
        }

        // now check that trying to read the same Registers with different owner
        let _ = safe.register_read(&xorurl).await?;
        Ok(())
    }
}
