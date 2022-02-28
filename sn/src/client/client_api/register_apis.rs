// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;

use crate::client::Error;
use sn_interface::messaging::data::{
    CreateRegister, DataCmd, DataQuery, DeleteRegister, EditRegister, QueryResponse, RegisterCmd,
    RegisterQuery, SignedRegisterCreate, SignedRegisterDelete, SignedRegisterEdit,
};
use sn_interface::types::{
    register::{Entry, EntryHash, Permissions, Policy, Register, User},
    RegisterAddress as Address,
};

use std::collections::BTreeSet;
use xor_name::XorName;

/// Register Write Ahead Log
///
/// Batches up register write operation before publishing them up to the network, in order.
/// Can also be used as a way to implement dry runs:
/// nothing is uploaded to the network as long as the wal is not published.
/// Batches can be republished without duplication risks thanks to the CRDT nature of registers.
pub type RegisterWriteAheadLog = Vec<DataCmd>;

impl Client {
    //----------------------
    // Write Operations
    //---------------------

    /// Publish all register mutation operations in a WAL to the network
    /// Incrementing the WAL index as successful writes are sent out. Stops at the first error.
    /// Starts publishing from the index when called again with the same WAL.
    #[instrument(skip(self), level = "debug")]
    pub async fn publish_register_ops(&self, wal: RegisterWriteAheadLog) -> Result<(), Error> {
        for cmd in wal.iter() {
            self.send_cmd(cmd.clone()).await?;
        }
        Ok(())
    }

    /// Creates a Register which can then be written to.
    ///
    /// Returns a write ahead log (WAL) of register operations, note that the changes are not uploaded to the
    /// network until the WAL is published with `publish_register_ops`
    ///
    /// A tag must be supplied.
    /// A xorname must be supplied, this can be random or deterministic as per your apps needs.
    #[instrument(skip(self), level = "debug")]
    pub async fn create_register(
        &self,
        name: XorName,
        tag: u64,
        policy: Policy,
    ) -> Result<(Address, RegisterWriteAheadLog), Error> {
        let address = if matches!(policy, Policy::Public(_)) {
            Address::Public { name, tag }
        } else {
            Address::Private { name, tag }
        };

        let op = CreateRegister::Empty {
            name,
            tag,
            size: u16::MAX, // TODO: use argument
            policy,
        };
        let signature = self.keypair.sign(&bincode::serialize(&op)?);

        let cmd = DataCmd::Register(RegisterCmd::Create {
            cmd: SignedRegisterCreate {
                op,
                auth: sn_interface::messaging::ServiceAuth {
                    public_key: self.keypair.public_key(),
                    signature,
                },
            },
            section_auth: section_auth(), // obtained after presenting a valid payment to the network
        });

        Ok((address, vec![cmd]))
    }

    /// Delete Register
    ///
    /// Returns a write ahead log (WAL) of register operations, note that the changes are not uploaded to the
    /// network until the WAL is published with `publish_register_ops`
    ///
    /// You're only able to delete a PrivateRegister. Public data can not be removed from the network.
    #[instrument(skip(self), level = "debug")]
    pub async fn delete_register(&self, address: Address) -> Result<RegisterWriteAheadLog, Error> {
        let op = DeleteRegister(address);
        let signature = self.keypair.sign(&bincode::serialize(&op)?);

        let update = SignedRegisterDelete {
            op,
            auth: sn_interface::messaging::ServiceAuth {
                public_key: self.keypair.public_key(),
                signature,
            },
        };

        let cmd = DataCmd::Register(RegisterCmd::Delete(update));

        let batch = vec![cmd];

        Ok(batch)
    }

    /// Write to Register
    ///
    /// Returns a write ahead log (WAL) of register operations, note that the changes are not uploaded to the
    /// network until the WAL is published with `publish_register_ops`
    ///
    /// Public or private isn't important for writing, though the data you write will
    /// be Public or Private according to the type of the targeted Register.
    #[instrument(skip(self, children), level = "debug")]
    pub async fn write_to_register(
        &self,
        address: Address,
        entry: Entry,
        children: BTreeSet<EntryHash>,
    ) -> Result<(EntryHash, RegisterWriteAheadLog), Error> {
        // First we fetch it so we can get the causality info,
        // either from local CRDT replica or from the network if not found
        debug!("Writing to register at {:?}", address);
        let mut register = self.get_register(address).await?;

        // We can now write the entry to the Register
        let (hash, op) = register.write(entry, children)?;
        let op = EditRegister { address, edit: op };

        let signature = self.keypair.sign(&bincode::serialize(&op)?);

        let edit = SignedRegisterEdit {
            op,
            auth: sn_interface::messaging::ServiceAuth {
                public_key: self.keypair.public_key(),
                signature,
            },
        };

        // Finally we package the mutation for the network's replicas (it's now ready to be sent)
        let cmd = DataCmd::Register(RegisterCmd::Edit(edit));
        let batch = vec![cmd];
        Ok((hash, batch))
    }

    //----------------------
    // Get Register
    //---------------------

    /// Get the entire Register from the Network
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register(&self, address: Address) -> Result<Register, Error> {
        // Let's fetch the Register from the network
        let query = DataQuery::Register(RegisterQuery::Get(address));
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::GetRegister((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }

    /// Get the latest entry (or entries if branching)
    #[instrument(skip(self), level = "debug")]
    pub async fn read_register(
        &self,
        address: Address,
    ) -> Result<BTreeSet<(EntryHash, Entry)>, Error> {
        let query = DataQuery::Register(RegisterQuery::Read(address));
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::ReadRegister((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }

    /// Get an entry from a Register on the Network by its hash
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_entry(
        &self,
        address: Address,
        hash: EntryHash,
    ) -> Result<Entry, Error> {
        let query = DataQuery::Register(RegisterQuery::GetEntry { address, hash });
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::GetRegisterEntry((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }

    //----------------------
    // Ownership
    //---------------------

    /// Get the owner of a Register.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_owner(&self, address: Address) -> Result<User, Error> {
        let query = DataQuery::Register(RegisterQuery::GetOwner(address));
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::GetRegisterOwner((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }

    //----------------------
    // Permissions
    //---------------------

    /// Get the set of Permissions in a Register for a specific user.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_permissions_for_user(
        &self,
        address: Address,
        user: User,
    ) -> Result<Permissions, Error> {
        let query = DataQuery::Register(RegisterQuery::GetUserPermissions { address, user });
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::GetRegisterUserPermissions((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }

    /// Get the Policy of a Register.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_policy(&self, address: Address) -> Result<Policy, Error> {
        let query = DataQuery::Register(RegisterQuery::GetPolicy(address));
        let query_result = self.send_query(query).await?;
        match query_result.response {
            QueryResponse::GetRegisterPolicy((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            _ => Err(Error::ReceivedUnexpectedEvent),
        }
    }
}

// temp dummy
fn section_auth() -> sn_interface::messaging::SectionAuth {
    use sn_interface::messaging::system::KeyedSig;

    let sk = bls::SecretKey::random();
    let public_key = sk.public_key();
    let data = "hello".to_string();
    let signature = sk.sign(&data);
    let sig = KeyedSig {
        public_key,
        signature,
    };
    sn_interface::messaging::SectionAuth {
        src_name: sn_interface::types::PublicKey::Bls(public_key).into(),
        sig,
    }
}

#[cfg(test)]
mod tests {
    use crate::client::{
        utils::test_utils::{
            create_test_client, create_test_client_with, gen_ed_keypair, init_test_logger,
            run_w_backoff_delayed,
        },
        Error,
    };
    use crate::retry_loop_for_pattern;
    use eyre::{bail, eyre, Result};
    use rand::Rng;
    use sn_interface::messaging::data::Error as ErrorMsg;
    use sn_interface::types::{
        log_markers::LogMarker,
        register::{
            Action, EntryHash, Permissions, Policy, PrivatePolicy, PublicPermissions, PublicPolicy,
            User,
        },
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Instant,
    };
    use tokio::time::Duration;
    use tracing::Instrument;
    use xor_name::XorName;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_batching() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_basics").entered();

        let client = create_test_client().await?;
        let one_sec = tokio::time::Duration::from_secs(1);
        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // create a Private Register
        let (address, mut batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;

        // create a Public Register
        let (address2, mut batch2) = client
            .create_register(name, tag, public_policy(owner))
            .await?;

        // batch them up
        batch.append(&mut batch2);

        // publish that batch to the network
        client.publish_register_ops(batch).await?;
        tokio::time::sleep(one_sec).await;

        // check they're both there
        let priv_register = client.get_register(address).await?;
        assert!(priv_register.is_private());
        assert_eq!(*priv_register.name(), name);
        assert_eq!(priv_register.tag(), tag);
        assert_eq!(priv_register.size(), 0);
        assert_eq!(priv_register.owner(), owner);

        let pub_register = client.get_register(address2).await?;
        assert!(pub_register.is_public());
        assert_eq!(*pub_register.name(), name);
        assert_eq!(pub_register.tag(), tag);
        assert_eq!(pub_register.size(), 0);
        assert_eq!(pub_register.owner(), owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Testnet network_assert_ tests should be excluded from normal tests runs, they need to be run in sequence to ensure validity of checks"]
    async fn register_network_assert_expected_log_counts() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("register_network_assert").entered();

        let mut the_logs = crate::testnet_grep::NetworkLogState::new()?;

        let network_assert_delay: u64 = std::env::var("NETWORK_ASSERT_DELAY")
            .unwrap_or_else(|_| "3".to_string())
            .parse()?;

        let client = create_test_client().await?;

        let delay = tokio::time::Duration::from_secs(network_assert_delay);
        debug!("Running network asserts with delay of {:?}", delay);

        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Private Register
        let (_address, batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        // small delay to ensure logs have written
        tokio::time::sleep(delay).await;

        // All elders should have been written to
        the_logs.assert_count(LogMarker::RegisterWrite, 7).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "too heavy for CI"]
    async fn measure_upload_times() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__measure_upload_times").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, public_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let mut total = 0;
        let value_1 = random_register_entry();

        for i in 0..1000_usize {
            let now = Instant::now();

            // write to the register
            let _value1_hash = run_w_backoff_delayed(
                || async {
                    let (hash, batch) = client
                        .write_to_register(address, value_1.clone(), BTreeSet::new())
                        .await?;
                    client.publish_register_ops(batch).await?;
                    Ok(hash)
                },
                10,
                1,
            )
            .await?;

            let elapsed = now.elapsed().as_millis();
            total += elapsed;
            println!("Iter # {}, elapsed: {}", i, elapsed);
        }

        println!("Total elapsed: {}", total);

        Ok(())
    }

    /**** Register data tests ****/

    #[tokio::test(flavor = "multi_thread")]
    async fn register_basics() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_basics").entered();

        let client = create_test_client().await?;
        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Private Register
        let (address, batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        let register = client.get_register(address).await?;

        assert!(register.is_private());
        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert_eq!(register.size(), 0);
        assert_eq!(register.owner(), owner);

        // store a Public Register
        let (address, batch) = client
            .create_register(name, tag, public_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        tokio::time::sleep(delay).await;
        let register = client.get_register(address).await?;

        assert!(register.is_public());
        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert_eq!(register.size(), 0);
        assert_eq!(register.owner(), owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_private_permissions() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_private_permissions").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        let register = client.get_register(address).await?;

        assert_eq!(register.size(), 0);

        tokio::time::sleep(delay).await;
        let permissions = client
            .get_register_permissions_for_user(address, owner)
            .instrument(tracing::info_span!("first get perms for owner"))
            .await?;

        match permissions {
            Permissions::Private(user_perms) => {
                assert!(user_perms.is_allowed(Action::Read));
                assert!(user_perms.is_allowed(Action::Write));
            }
            Permissions::Public(_) => bail!(
                "Incorrect user permissions were returned: {:?}",
                permissions
            ),
        }

        let other_user = User::Key(gen_ed_keypair().public_key());

        match client
            .get_register_permissions_for_user(address, other_user)
            .instrument(tracing::info_span!("get other user perms"))
            .await
        {
            Ok(_) => bail!("Should not be able to retrieve an entry for a random user"),
            Err(Error::ErrorMsg {
                source: ErrorMsg::NoSuchEntry,
                ..
            }) => Ok(()),
            Err(err) => Err(eyre!(
                "Unexpected error returned when retrieving non-existing Register user permission: {:?}", err,
            )),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_public_permissions() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_public_permissions").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, public_none_policy(owner)) // trying to set write perms to false for the owner (will not be reflected as long as the user is the owner, as an owner will have full authority)
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        // keep retrying until ok
        let permissions = client
            .get_register_permissions_for_user(address, owner)
            .instrument(tracing::info_span!("get owner perms"))
            .await?;

        match permissions {
            // above the owner perms were set to more restrictive than full, we test below that
            // write&read perms for the owner will always be true, as an owner will always have full authority (even if perms were explicitly set some other way)
            Permissions::Public(user_perms) => {
                assert_eq!(Some(true), user_perms.is_allowed(Action::Read));
                assert_eq!(Some(true), user_perms.is_allowed(Action::Write));
            }
            Permissions::Private(_) => {
                return Err(eyre!("Unexpectedly obtained incorrect user permissions",));
            }
        }

        let other_user = User::Key(gen_ed_keypair().public_key());

        match client
            .get_register_permissions_for_user(address, other_user)
            .instrument(tracing::info_span!("get other user perms"))
            .await
        {
            Ok(_) => bail!("Should not be able to retrieve an entry for a random user"),
            Err(Error::ErrorMsg {
                source: ErrorMsg::NoSuchEntry,
                ..
            }) => Ok(()),
            Err(err) => Err(eyre!(
                "Unexpected error returned when retrieving non-existing Register user permission: {:?}", err,
            )),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_write() -> Result<()> {
        init_test_logger();
        let start_span = tracing::info_span!("test__register_write_start").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, public_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let value_1 = random_register_entry();

        // write to the register
        let value1_hash = run_w_backoff_delayed(
            || async {
                let (hash, batch) = client
                    .write_to_register(address, value_1.clone(), BTreeSet::new())
                    .await?;
                client.publish_register_ops(batch).await?;
                Ok(hash)
            },
            10,
            1,
        )
        .await?;

        // now check last entry
        let hashes = retry_loop_for_pattern!(client.read_register(address), Ok(hashes) if !hashes.is_empty())?;

        assert_eq!(1, hashes.len());
        let current = hashes.iter().next();
        assert_eq!(current, Some(&(value1_hash, value_1.clone())));

        let value_2 = random_register_entry();

        drop(start_span);
        let _second_span = tracing::info_span!("test__register_write__second_write").entered();
        // write to the register
        let value2_hash = run_w_backoff_delayed(
            || async {
                let (hash, batch) = client
                    .write_to_register(address, value_2.clone(), BTreeSet::new())
                    .await?;
                client.publish_register_ops(batch).await?;
                Ok(hash)
            },
            10,
            1,
        )
        .await?;

        // and then lets check all entries are returned
        // NB: these will not be ordered according to insertion order, but according to the hashes of the values.
        let hashes =
            retry_loop_for_pattern!(client.read_register(address), Ok(hashes) if hashes.len() > 1)?;

        assert_eq!(2, hashes.len());

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;
        // get_register_entry
        let retrieved_value_1 = client
            .get_register_entry(address, value1_hash)
            .instrument(tracing::info_span!("get_value_1"))
            .await?;
        assert_eq!(retrieved_value_1, value_1);

        tokio::time::sleep(delay).await;

        let retrieved_value_2 = client
            .get_register_entry(address, value2_hash)
            .instrument(tracing::info_span!("get_value_2"))
            .await?;
        assert_eq!(retrieved_value_2, value_2);

        // Requesting an entry which doesn't exist returns an error
        match client
            .get_register_entry(address, EntryHash(rand::thread_rng().gen::<[u8; 32]>()))
            .instrument(tracing::info_span!("final get"))
            .await
        {
            Err(Error::ErrorMsg {
                source: ErrorMsg::NoSuchEntry,
                ..
            }) => Ok(()),
            Err(err) => Err(eyre!(
                "Unexpected error returned when retrieving a non-existing Register entry: {:?}",
                err,
            )),
            Ok(_data) => Err(eyre!(
                "Unexpectedly retrieved a register entry with a random hash!",
            )),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_owner() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_owner").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        // Assert that the data is stored.
        let current_owner = client.get_register_owner(address).await?;

        assert_eq!(owner, current_owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_can_delete_private() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_can_delete_private").entered();

        let mut client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, private_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        let register = client.get_register(address).await?;

        assert!(register.is_private());

        let batch2 = client.delete_register(address).await?;
        client.publish_register_ops(batch2).await?;

        client.query_timeout = Duration::from_secs(5); // override with a short timeout
        let mut res = client.get_register(address).await;
        while res.is_ok() {
            // attempt to delete register again (perhaps a message was dropped)
            let batch3 = client.delete_register(address).await?;
            client.publish_register_ops(batch3).await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            res = client.get_register(address).await;
        }

        match res {
            Err(Error::NoResponse) => Ok(()),
            Err(err) => Err(eyre!(
                "Unexpected error returned when deleting a non-existing Private Register: {:?}",
                err
            )),
            Ok(_data) => Err(eyre!("Unexpectedly retrieved a deleted Private Register!",)),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_cannot_delete_public() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__register_cannot_delete_public").entered();

        let client = create_test_client().await?;

        let name = XorName(rand::random());
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Public Register
        let (address, batch) = client
            .create_register(name, tag, public_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        let register = client.get_register(address).await?;
        assert!(register.is_public());

        let batch2 = client.delete_register(address).await?;
        match client.publish_register_ops(batch2).await {
            Err(Error::ErrorCmd {
                source: ErrorMsg::InvalidOperation(_),
                ..
            }) => {}
            Err(err) => bail!(
                "Unexpected error returned when attempting to delete a Public Register: {:?}",
                err
            ),
            Ok(()) => {}
        }

        // Check that our data still exists.
        let register = client.get_register(address).await?;
        assert!(register.is_public());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_checks_register_test() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("ae_checks_register_test").entered();
        let client = create_test_client_with(None, None, false).await?;

        let name = XorName::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Public Register
        let (address, batch) = client
            .create_register(name, tag, public_policy(owner))
            .await?;
        client.publish_register_ops(batch).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let register = client.get_register(address).await?;
        assert!(register.is_public());

        Ok(())
    }

    fn random_register_entry() -> Vec<u8> {
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        random_bytes.to_vec()
    }

    fn private_policy(owner: User) -> Policy {
        let permissions = BTreeMap::new();
        Policy::Private(PrivatePolicy { owner, permissions })
    }

    fn public_policy(owner: User) -> Policy {
        let permissions = BTreeMap::new();
        Policy::Public(PublicPolicy { owner, permissions })
    }

    fn public_none_policy(owner: User) -> Policy {
        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(owner, PublicPermissions::new(None));
        Policy::Public(PublicPolicy { owner, permissions })
    }
}
