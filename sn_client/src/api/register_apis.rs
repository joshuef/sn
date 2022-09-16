// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;

use crate::Error;

use sn_interface::{
    messaging::data::{
        CreateRegister, DataCmd, DataQueryVariant, EditRegister, QueryResponse, RegisterCmd,
        RegisterQuery, SignedRegisterCreate, SignedRegisterEdit,
    },
    types::{
        register::{Action, Entry, EntryHash, Permissions, Policy, Register, User},
        RegisterAddress as Address,
    },
};

use std::collections::BTreeSet;
use xor_name::XorName;

/// Register Write Ahead Log
///
/// Batches up register write operation before publishing them up to the network, in order.
/// Can also be used as a way to implement dry runs:
/// nothing is uploaded to the network as long as the WAL is not published.
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
        for cmd in &wal {
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
        let address = Address { name, tag };

        let op = CreateRegister { name, tag, policy };
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

        debug!("Creating Register: {:?}", cmd);

        Ok((address, vec![cmd]))
    }

    /// Write to Register
    ///
    /// Returns a write ahead log (WAL) of register operations, note that the changes are not uploaded to the
    /// network until the WAL is published with `publish_register_ops`
    #[instrument(skip(self, children), level = "debug")]
    pub async fn write_to_local_register(
        &self,
        address: Address,
        entry: Entry,
        children: BTreeSet<EntryHash>,
    ) -> Result<(EntryHash, RegisterWriteAheadLog), Error> {
        // First we fetch it so we can get the causality info,
        // either from local CRDT replica or from the network if not found
        debug!("Writing to register at {:?}", address);
        let mut register = self.get_register(address).await?;

        // Let's check the policy/permissions to make sure this operation is allowed,
        // otherwise it will fail when the operation is applied on the network replica.
        let public_key = self.keypair.public_key();
        register.check_permissions(Action::Write, Some(User::Key(public_key)))?;

        // We can now write the entry to the Register
        let (hash, op) = register.write(entry, children)?;
        let op = EditRegister { address, edit: op };

        let signature = self.keypair.sign(&bincode::serialize(&op)?);

        let edit = SignedRegisterEdit {
            op,
            auth: sn_interface::messaging::ServiceAuth {
                public_key,
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
        let query = DataQueryVariant::Register(RegisterQuery::Get(address));
        let query_result = self.send_query(query.clone()).await?;

        debug!("get_register result is; {query_result:?}");
        match query_result.response {
            QueryResponse::GetRegister((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
        }
    }

    /// Get the latest entry (or entries if branching)
    #[instrument(skip(self), level = "debug")]
    pub async fn read_register(
        &self,
        address: Address,
    ) -> Result<BTreeSet<(EntryHash, Entry)>, Error> {
        let query = DataQueryVariant::Register(RegisterQuery::Read(address));
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::ReadRegister((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
        }
    }

    /// Get an entry from a Register on the Network by its hash
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_entry(
        &self,
        address: Address,
        hash: EntryHash,
    ) -> Result<Entry, Error> {
        let query = DataQueryVariant::Register(RegisterQuery::GetEntry { address, hash });
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::GetRegisterEntry((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
        }
    }

    //----------------------
    // Ownership
    //---------------------

    /// Get the owner of a Register.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_owner(&self, address: Address) -> Result<User, Error> {
        let query = DataQueryVariant::Register(RegisterQuery::GetOwner(address));
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::GetRegisterOwner((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
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
        let query = DataQueryVariant::Register(RegisterQuery::GetUserPermissions { address, user });
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::GetRegisterUserPermissions((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
        }
    }

    /// Get the Policy of a Register.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_register_policy(&self, address: Address) -> Result<Policy, Error> {
        let query = DataQueryVariant::Register(RegisterQuery::GetPolicy(address));
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::GetRegisterPolicy((res, op_id)) => {
                res.map_err(|err| Error::ErrorMsg { source: err, op_id })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
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
    use crate::{
        retry_loop_for_pattern,
        utils::test_utils::{create_test_client, create_test_client_with, init_logger},
        Error,
    };

    use sn_interface::{
        messaging::data::Error as ErrorMsg,
        types::{
            log_markers::LogMarker,
            register::{Action, EntryHash, Permissions, Policy, User},
            Keypair,
        },
    };

    use eyre::{bail, eyre, Result};
    use rand::Rng;
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Instant,
    };
    use tracing::Instrument;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_batching() -> Result<()> {
        init_logger();
        let _outer_span = tracing::info_span!("test__register_basics").entered();

        let client = create_test_client().await?;
        let one_sec = tokio::time::Duration::from_secs(1);
        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // create a Register
        let (address, mut batch) = client.create_register(name, tag, policy(owner)).await?;

        // create a second Register
        let (address2, mut batch2) = client.create_register(name, tag, policy(owner)).await?;

        // batch them up
        batch.append(&mut batch2);

        // publish that batch to the network
        client.publish_register_ops(batch).await?;
        tokio::time::sleep(one_sec).await;

        // check they're both there
        let register1 = client.get_register(address).await?;
        assert_eq!(*register1.name(), name);
        assert_eq!(register1.tag(), tag);
        assert_eq!(register1.size(), 0);
        assert_eq!(register1.owner(), owner);

        let register2 = client.get_register(address2).await?;
        assert_eq!(*register2.name(), name);
        assert_eq!(register2.tag(), tag);
        assert_eq!(register2.size(), 0);
        assert_eq!(register2.owner(), owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Testnet network_assert_ tests should be excluded from normal tests runs, they need to be run in sequence to ensure validity of checks"]
    async fn register_network_assert_expected_log_counts() -> Result<()> {
        init_logger();
        let _outer_span = tracing::info_span!("register_network_assert").entered();

        let mut the_logs = crate::testnet_grep::NetworkLogState::new()?;

        let network_assert_delay: u64 = std::env::var("NETWORK_ASSERT_DELAY")
            .unwrap_or_else(|_| "3".to_string())
            .parse()?;

        let client = create_test_client().await?;

        let delay = tokio::time::Duration::from_secs(network_assert_delay);
        debug!("Running network asserts with delay of {:?}", delay);

        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Register
        let (_address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        // small delay to ensure logs have written
        tokio::time::sleep(delay).await;

        // All elders should have been written to
        the_logs.assert_count(LogMarker::RegisterWrite, 7)?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "too heavy for CI"]
    async fn measure_upload_times() -> Result<()> {
        init_logger();
        let _outer_span = tracing::info_span!("test__measure_upload_times").entered();

        let client = create_test_client().await?;

        let name = xor_name::rand::random();
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        let mut total = 0;
        let value_1 = random_register_entry();

        for i in 0..1000_usize {
            let now = Instant::now();

            let (_value1_hash, batch) = client
                .write_to_local_register(address, value_1.clone(), BTreeSet::new())
                .await?;
            client.publish_register_ops(batch).await?;

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
        init_logger();
        let _outer_span = tracing::info_span!("test__register_basics").entered();

        let client = create_test_client().await?;
        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Register
        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        let register = client.get_register(address).await?;

        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert_eq!(register.size(), 0);
        assert_eq!(register.owner(), owner);

        // store a second Register
        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        tokio::time::sleep(delay).await;
        let register = client.get_register(address).await?;

        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert_eq!(register.size(), 0);
        assert_eq!(register.owner(), owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn register_permissions() -> Result<()> {
        init_logger();
        let _outer_span = tracing::info_span!("test__register_permissions").entered();

        let client = create_test_client().await?;

        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        let (address, batch) = client
            .create_register(name, tag, none_policy(owner)) // trying to set write perms to false for the owner (will not be reflected as long as the user is the owner, as an owner will have full authority)
            .await?;
        client.publish_register_ops(batch).await?;

        let delay = tokio::time::Duration::from_secs(1);
        tokio::time::sleep(delay).await;

        // keep retrying until ok
        let permissions = client
            .get_register_permissions_for_user(address, owner)
            .instrument(tracing::info_span!("get owner perms"))
            .await?;

        assert_eq!(Some(true), permissions.is_allowed(Action::Read));
        assert_eq!(Some(true), permissions.is_allowed(Action::Write));

        let other_user = User::Key(Keypair::new_ed25519().public_key());

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
        init_logger();
        let start_span = tracing::info_span!("test__register_write_start").entered();

        let client = create_test_client().await?;

        let name = xor_name::rand::random();
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        let value_1 = random_register_entry();

        let (value1_hash, batch) = client
            .write_to_local_register(address, value_1.clone(), BTreeSet::new())
            .await?;
        client.publish_register_ops(batch).await?;

        // now check last entry
        let hashes = retry_loop_for_pattern!(client.read_register(address), Ok(hashes) if !hashes.is_empty())?;

        assert_eq!(1, hashes.len());
        let current = hashes.iter().next();
        assert_eq!(current, Some(&(value1_hash, value_1.clone())));

        let value_2 = random_register_entry();

        drop(start_span);
        let _second_span = tracing::info_span!("test__register_write__second_write").entered();

        // write to the register
        let (value2_hash, batch) = client
            .write_to_local_register(address, value_2.clone(), BTreeSet::new())
            .await?;

        // we get an op to publish
        assert!(batch.len() == 1);

        client.publish_register_ops(batch).await?;

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
        init_logger();
        let _outer_span = tracing::info_span!("test__register_owner").entered();

        let client = create_test_client().await?;

        let name = xor_name::rand::random();
        let tag = 10;
        let owner = User::Key(client.public_key());

        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;

        // Assert that the data is stored.
        let current_owner = client.get_register_owner(address).await?;

        assert_eq!(owner, current_owner);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_checks_register_test() -> Result<()> {
        init_logger();
        let _outer_span = tracing::info_span!("ae_checks_register_test").entered();
        let client = create_test_client_with(None, None, None).await?;

        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());

        // store a Register
        let (address, batch) = client.create_register(name, tag, policy(owner)).await?;
        client.publish_register_ops(batch).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let _register = client.get_register(address).await?;

        Ok(())
    }

    fn random_register_entry() -> Vec<u8> {
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        random_bytes.to_vec()
    }

    fn policy(owner: User) -> Policy {
        let permissions = BTreeMap::new();
        Policy { owner, permissions }
    }

    fn none_policy(owner: User) -> Policy {
        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(owner, Permissions::new(None));
        Policy { owner, permissions }
    }
}
