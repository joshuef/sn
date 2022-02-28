// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::dbs::{
    convert_to_error_msg, Error, EventStore, LruCache, Result, UsedSpace, SLED_FLUSH_TIME_MS,
};
use sn_interface::messaging::{
    data::{
        CreateRegister, DeleteRegister, EditRegister, ExtendRegister, OperationId, RegisterCmd,
        RegisterQuery, RegisterStoreExport, ReplicatedRegisterLog, SignedRegisterCreate,
        SignedRegisterDelete, SignedRegisterEdit, SignedRegisterExtend,
    },
    system::NodeQueryResponse,
    SectionAuth, VerifyAuthority,
};
use sn_interface::types::{
    register::{Action, EntryHash, Register, User},
    DataAddress, RegisterAddress as Address,
};

use bincode::serialize;
use rayon::prelude::*;
use sled::Db;
use std::{
    fmt::{self, Display, Formatter},
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::info;
#[cfg(test)]
use xor_name::Prefix;
use xor_name::{XorName, XOR_NAME_LEN};

const REG_DB_NAME: &str = "register";
const KEY_DB_NAME: &str = "addresses";
const CACHE_SIZE: u16 = 100;

type RegOpStore = EventStore<RegisterCmd>;
type Cache = LruCache<CacheEntry>;

/// Operations over the data type Register.
// TODO: dont expose this
#[derive(Clone, Debug)]
pub(crate) struct RegisterStorage {
    key_db: Db,
    reg_db: Db,
    cache: Cache,
    used_space: UsedSpace,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    state: Arc<RwLock<Register>>,
    store: RegOpStore,
    section_auth: SectionAuth,
}

impl RegisterStorage {
    /// Create new RegisterStorage
    pub(crate) fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        let create_path = |name: &str| path.join("db").join(name);
        let create_db = |db_dir| {
            sled::Config::default()
                .path(&db_dir)
                .flush_every_ms(SLED_FLUSH_TIME_MS)
                .open()
                .map_err(Error::from)
        };

        Ok(Self {
            used_space,
            cache: Cache::new(CACHE_SIZE),
            key_db: create_db(&create_path(KEY_DB_NAME))?,
            reg_db: create_db(&create_path(REG_DB_NAME))?,
        })
    }

    /// --- Node Synching ---
    /// These are node internal functions, not to be exposed to users.

    pub(crate) async fn remove_register(&self, address: &Address) -> Result<()> {
        trace!("Removing register, {:?}", address);
        self.drop_register_key(address.id()?).await
    }

    pub(crate) async fn keys(&self) -> Result<Vec<Address>> {
        type KeyResults = Vec<Result<XorName>>;
        let mut the_data = vec![];

        // parse keys in parallel
        let (ok, err): (KeyResults, KeyResults) = self
            .key_db
            .export()
            .into_iter()
            .flat_map(|(_, _, pairs)| pairs)
            .par_bridge()
            .map(|pair| {
                let src_key = &pair[0];
                // we expect xornames as keys
                if src_key.len() != XOR_NAME_LEN {
                    return Err(Error::CouldNotParseDbKey(src_key.to_vec()));
                }
                let mut dst_key: [u8; 32] = Default::default();
                dst_key.copy_from_slice(src_key);

                Ok(XorName(dst_key))
            })
            .partition(|r| r.is_ok());

        if !err.is_empty() {
            for e in err {
                error!("{:?}", e);
            }
            return Err(Error::CouldNotConvertDbKey);
        }

        // TODO: make this concurrent
        for key in ok.iter().flatten() {
            match self.try_load_cache_entry(key).await {
                Ok(entry) => {
                    the_data.push(*entry.state.read().await.address());
                }
                Err(Error::KeyNotFound(_)) => return Err(Error::InvalidStore),
                Err(e) => return Err(e),
            }
        }

        Ok(the_data)
    }

    /// Used for replication of data to new Adults.
    pub(crate) async fn get_register_replica(
        &self,
        address: &Address,
    ) -> Result<ReplicatedRegisterLog> {
        let key = address.id()?;
        let entry = match self.try_load_cache_entry(&key).await {
            Ok(entry) => entry,
            Err(Error::KeyNotFound(_key)) => {
                return Err(Error::NoSuchData(DataAddress::Register(*address)))
            }
            Err(e) => return Err(e),
        };

        self.create_replica(key, entry)
    }

    fn create_replica(
        &self,
        key: XorName,
        entry: Arc<CacheEntry>,
    ) -> Result<ReplicatedRegisterLog> {
        let mut address = None;
        let op_log = entry
            .store
            .get_all()?
            .into_iter()
            .filter_map(|stored_cmd| {
                // only spread signed data
                match stored_cmd.clone() {
                    RegisterCmd::Create { cmd, .. } => {
                        // TODO 1: in higher layers we must verify that the section_auth is from a proper section..!
                        // TODO 2: Enable this check once we have section signature over the container key.
                        // if section_auth.verify_authority(key).is_err() {
                        //     warn!("Invalid section auth on register container: {}", key);
                        //     return None;
                        // }
                        address = Some(cmd.dst_address());
                    }
                    RegisterCmd::Edit(SignedRegisterEdit { op, auth }) => {
                        let verification = auth.verify_authority(serialize(&op).ok()?);
                        if verification.is_err() {
                            error!(
                                "Invalid signature found for a cmd stored in db: {:?}",
                                stored_cmd
                            );
                            return None;
                        }
                    }
                    RegisterCmd::Delete(SignedRegisterDelete { op, auth }) => {
                        let verification = auth.verify_authority(serialize(&op).ok()?);
                        if verification.is_err() {
                            error!(
                                "Invalid signature found for cmd stored in db: {:?}",
                                stored_cmd
                            );
                            return None;
                        }
                    }
                    RegisterCmd::Extend { section_auth, .. } => {
                        // TODO: in higher layers we must verify that the section_auth is from a proper section..!
                        if section_auth.verify_authority(key).is_err() {
                            warn!("Invalid section auth on register container: {}", key);
                            return None;
                        }
                    }
                };
                Some(stored_cmd)
            })
            .collect();

        Ok(ReplicatedRegisterLog {
            address: address.ok_or(Error::InvalidStore)?,
            section_auth: entry.section_auth.clone(),
            op_log,
        })
    }

    /// Used for replication of data to new Adults.
    #[cfg(test)]
    pub(crate) async fn get_data_of(&self, prefix: Prefix) -> Result<RegisterStoreExport> {
        type KeyResults = Vec<Result<XorName>>;

        // parse keys in parallel
        let (ok, err): (KeyResults, KeyResults) = self
            .key_db
            .export()
            .into_iter()
            .flat_map(|(_, _, pairs)| pairs)
            .par_bridge()
            .map(|pair| {
                let src_key = &pair[0];
                // we expect xornames as keys
                if src_key.len() != XOR_NAME_LEN {
                    return Err(Error::CouldNotParseDbKey(src_key.to_vec()));
                }
                let mut dst_key: [u8; 32] = Default::default();
                dst_key.copy_from_slice(src_key);

                Ok(XorName(dst_key))
            })
            .partition(|r| r.is_ok());

        if !err.is_empty() {
            for e in err {
                error!("{:?}", e);
            }
            return Err(Error::CouldNotConvertDbKey);
        }

        let mut the_data = vec![];

        // TODO: make this concurrent
        for key in ok.into_iter().flatten() {
            match self.try_load_cache_entry(&key).await {
                Ok(entry) => {
                    let read_only = entry.state.read().await;
                    if prefix.matches(read_only.name()) {
                        the_data.push(self.create_replica(key, entry.clone())?);
                    }
                }
                Err(Error::KeyNotFound(_)) => return Err(Error::InvalidStore),
                Err(e) => return Err(e),
            }
        }

        Ok(RegisterStoreExport(the_data))
    }

    /// On receiving data from Elders when promoted.
    pub(crate) async fn update(&self, store_data: RegisterStoreExport) -> Result<()> {
        debug!("Updating Register store");

        let RegisterStoreExport(registers) = store_data;

        // nested loops, slow..
        for data in registers {
            let key = data.address.id()?;
            for replicated_cmd in data.op_log {
                if replicated_cmd.dst_address() != data.address {
                    warn!(
                        "Corrupt ReplicatedRegisterLog, op log contains foreign ops: {}",
                        key
                    );
                    continue;
                }
                let _ = self.apply(replicated_cmd).await?;
            }
        }

        Ok(())
    }

    /// --- Writing ---

    pub(crate) async fn write(&self, cmd: RegisterCmd) -> Result<()> {
        // rough estimate ignoring the extra space used by sled
        let required_space = std::mem::size_of::<RegisterCmd>();
        if !self.used_space.can_add(required_space) {
            return Err(Error::NotEnoughSpace);
        }
        self.apply(cmd).await
    }

    async fn apply(&self, cmd: RegisterCmd) -> Result<()> {
        // rough estimate ignoring the extra space used by sled
        let required_space = std::mem::size_of::<RegisterCmd>();

        let address = cmd.dst_address();
        let key = address.id()?;

        use RegisterCmd::*;
        match cmd.clone() {
            Create {
                cmd: SignedRegisterCreate { op, auth },
                ..
            } => {
                // TODO 1: in higher layers we must verify that the section_auth is from a proper section..!
                // TODO 2: Enable this check once we have section signature over the container key.
                // let public_key = section_auth.sig.public_key;
                // let _ = section_auth.verify_authority(key).or(Err(Error::InvalidSignature(PublicKey::Bls(public_key))))?;

                let public_key = auth.public_key;
                let _ = auth
                    .verify_authority(serialize(&op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                let old_value = None::<Vec<u8>>;
                let new_value = Some(vec![]); // inserts empty value

                // init store first, to allow append to happen asap after key insert
                // could be races, but edge case for later todos.
                let store = self.get_or_create_store(&key)?;

                // only inserts if no value existed - which is denoted by passing in `None` as old_value
                match self.key_db.compare_and_swap(key, old_value, new_value)? {
                    Ok(()) => trace!("Creating new register"),
                    Err(sled::CompareAndSwapError { .. }) => return Err(Error::DataExists),
                }

                // insert the op to the event log
                let _ = store.append(cmd)?;
                self.used_space.increase(required_space);

                Ok(())
            }
            Edit(SignedRegisterEdit { op, auth }) => {
                let public_key = auth.public_key;
                let _ = auth
                    .verify_authority(serialize(&op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                let EditRegister { edit, .. } = op;

                let entry = self.try_load_cache_entry(&key).await?;

                info!("Editing Register");
                entry
                    .state
                    .read()
                    .await
                    .check_permissions(Action::Write, Some(User::Key(public_key)))?;
                let result = entry
                    .state
                    .write()
                    .await
                    .apply_op(edit)
                    .map_err(Error::NetworkData);

                if result.is_ok() {
                    entry.store.append(cmd)?;
                    self.used_space.increase(required_space);
                    trace!("Editing Register success!");
                } else {
                    trace!("Editing Register failed!");
                }

                result
            }
            Delete(SignedRegisterDelete { op, auth }) => {
                let DeleteRegister(address) = &op;
                if address.is_public() {
                    return Err(Error::CannotDeletePublicData(DataAddress::Register(
                        *address,
                    )));
                }
                let public_key = auth.public_key;
                let _ = auth
                    .verify_authority(serialize(&op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                match self.try_load_cache_entry(&key).await {
                    Err(Error::KeyNotFound(_)) => {
                        trace!("Register was already deleted, or never existed..");
                        Ok(())
                    }
                    Ok(entry) => {
                        let read_only = entry.state.read().await;
                        // TODO - Register::check_permission() doesn't support Delete yet in safe-nd
                        // register.check_permission(action, Some(public_key))?;
                        if User::Key(public_key) != read_only.owner() {
                            Err(Error::InvalidOwner(public_key))
                        } else {
                            info!("Deleting Register");
                            self.drop_register_key(key).await?;
                            Ok(())
                        }
                    }
                    _ => Err(Error::InvalidStore),
                }
            }
            Extend {
                cmd: SignedRegisterExtend { op, auth },
                ..
            } => {
                // TODO 1: in higher layers we must verify that the section_auth is from a proper section..!
                // TODO 2: Enable this check once we have section signature over this.
                // let public_key = section_auth.sig.public_key;
                // let _ = section_auth.verify_authority(key).or(Err(Error::InvalidSignature(PublicKey::Bls(public_key))))?;

                let public_key = auth.public_key;
                let _ = auth
                    .verify_authority(serialize(&op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                let ExtendRegister { extend_with, .. } = op;

                let entry = self.try_load_cache_entry(&key).await?;
                entry.store.append(cmd)?;

                let mut write = entry.state.write().await;
                let prev = write.cap();
                write.increment_cap(extend_with);

                info!(
                    "Extended Register size from {} to {}",
                    prev,
                    prev + extend_with,
                );

                self.used_space.increase(required_space);
                Ok(())
            }
        }
    }

    /// --- Reading ---

    pub(crate) async fn read(&self, read: &RegisterQuery, requester: User) -> NodeQueryResponse {
        trace!("Reading register {:?}", read.dst_address());
        let operation_id = match read.operation_id() {
            Ok(id) => id,
            Err(_e) => {
                return NodeQueryResponse::FailedToCreateOperationId;
            }
        };
        trace!("Operation of register read: {:?}", operation_id);
        use RegisterQuery::*;
        match read {
            Get(address) => self.get(*address, requester, operation_id).await,
            Read(address) => self.read_register(*address, requester, operation_id).await,
            GetOwner(address) => self.get_owner(*address, requester, operation_id).await,
            GetEntry { address, hash } => {
                self.get_entry(*address, *hash, requester, operation_id)
                    .await
            }
            GetPolicy(address) => self.get_policy(*address, requester, operation_id).await,
            GetUserPermissions { address, user } => {
                self.get_user_permissions(*address, *user, requester, operation_id)
                    .await
            }
        }
    }

    /// Get `Register` from the store and check permissions.
    async fn get_register(
        &self,
        address: &Address,
        action: Action,
        requester: User,
    ) -> Result<Register> {
        let entry = match self.try_load_cache_entry(&address.id()?).await {
            Ok(entry) => entry,
            Err(Error::KeyNotFound(_key)) => {
                return Err(Error::NoSuchData(DataAddress::Register(*address)))
            }
            Err(e) => return Err(e),
        };

        let read_only = entry.state.read().await;
        read_only
            .check_permissions(action, Some(requester))
            .map_err(Error::from)?;

        Ok(read_only.clone())
    }

    /// Get entire Register.
    async fn get(
        &self,
        address: Address,
        requester: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(register) => Ok(register),
            Err(error) => Err(convert_to_error_msg(error)),
        };

        NodeQueryResponse::GetRegister((result, operation_id))
    }

    async fn read_register(
        &self,
        address: Address,
        requester: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(register) => Ok(register.read()),
            Err(error) => Err(error),
        };

        NodeQueryResponse::ReadRegister((result.map_err(convert_to_error_msg), operation_id))
    }

    async fn get_owner(
        &self,
        address: Address,
        requester: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(res) => Ok(res.owner()),
            Err(error) => Err(convert_to_error_msg(error)),
        };

        NodeQueryResponse::GetRegisterOwner((result, operation_id))
    }

    async fn get_entry(
        &self,
        address: Address,
        hash: EntryHash,
        requester: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester)
            .await
            .and_then(|register| register.get(hash).map(|c| c.clone()).map_err(Error::from))
        {
            Ok(res) => Ok(res),
            Err(error) => Err(convert_to_error_msg(error)),
        };

        NodeQueryResponse::GetRegisterEntry((result, operation_id))
    }

    async fn get_user_permissions(
        &self,
        address: Address,
        user: User,
        requester: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester)
            .await
            .and_then(|register| register.permissions(user).map_err(Error::from))
        {
            Ok(res) => Ok(res),
            Err(error) => Err(convert_to_error_msg(error)),
        };

        NodeQueryResponse::GetRegisterUserPermissions((result, operation_id))
    }

    async fn get_policy(
        &self,
        address: Address,
        requester_pk: User,
        operation_id: OperationId,
    ) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester_pk)
            .await
            .map(|register| register.policy().clone())
        {
            Ok(res) => Ok(res),
            Err(error) => Err(convert_to_error_msg(error)),
        };

        NodeQueryResponse::GetRegisterPolicy((result, operation_id))
    }

    /// ========================================================================
    /// =========================== Helpers ====================================
    /// ========================================================================

    // get or create a register op store
    fn get_or_create_store(&self, id: &XorName) -> Result<RegOpStore> {
        RegOpStore::new(id, self.reg_db.clone()).map_err(Error::from)
    }

    // helper that drops the sled tree for a given register
    // decreases the used space by a rough estimate of the size before deletion
    // as with addition this estimate ignores the extra space used by sled
    // (that estimate can fall victim to a race condition if someone writes to a register that is being deleted)
    async fn drop_register_key(&self, key: XorName) -> Result<()> {
        let regcmd_size = std::mem::size_of::<RegisterCmd>();
        let reg_tree = self.reg_db.open_tree(key)?;
        let len = reg_tree.len();
        let key_used_space = len * regcmd_size;

        let _removed = self.key_db.remove(key)?;
        let _removed = self.reg_db.drop_tree(key)?;

        self.cache.remove(&key).await;
        self.used_space.decrease(key_used_space);

        Ok(())
    }

    // gets entry from the cache, or populates cache from disk if expired
    async fn try_load_cache_entry(&self, key: &XorName) -> Result<Arc<CacheEntry>> {
        let entry = self.cache.get(key).await;

        // return early on cache hit
        if let Some(entry) = entry {
            return Ok(entry);
        }

        // read from disk
        let store = self.get_or_create_store(key)?;
        let mut hydrated_register = None;
        // apply all ops
        use RegisterCmd::*;
        for stored_cmd in store.get_all()? {
            match stored_cmd {
                // first op would be create
                Create {
                    cmd: SignedRegisterCreate { op, .. },
                    section_auth,
                } => {
                    hydrated_register = match op {
                        CreateRegister::Empty {
                            name,
                            tag,
                            size,
                            policy,
                        } => Some((Register::new(name, tag, policy, size), section_auth)),
                        CreateRegister::Populated(instance) => {
                            if instance.size() > (u16::MAX as u64) {
                                // this would mean the instance has been modified on disk outside of the software
                                warn!("Data corruption! Encountered stored register with {} entries, wich is larger than max size of {}", instance.size(), u16::MAX);
                            }
                            Some((instance, section_auth))
                        }
                    };
                }
                Edit(SignedRegisterEdit {
                    op: EditRegister { edit, .. },
                    ..
                }) => {
                    if let Some((reg, _)) = &mut hydrated_register {
                        reg.apply_op(edit).map_err(Error::NetworkData)?
                    }
                }
                Delete(SignedRegisterDelete { .. }) => {
                    // should not be reachable, since we don't append these ops
                    return Err(Error::KeyNotFound(key.to_string()));
                }
                Extend {
                    cmd:
                        SignedRegisterExtend {
                            op: ExtendRegister { extend_with, .. },
                            ..
                        },
                    ..
                } => {
                    if let Some((reg, _)) = &mut hydrated_register {
                        reg.increment_cap(extend_with);
                    }
                }
            }
        }

        match hydrated_register {
            None => Err(Error::KeyNotFound(key.to_string())), // nothing found on disk
            Some((reg, section_auth)) => {
                let entry = Arc::new(CacheEntry {
                    state: Arc::new(RwLock::new(reg)),
                    store,
                    section_auth,
                });
                // populate cache
                self.cache.insert(key, entry.clone()).await;
                Ok(entry)
            }
        }
    }
}

impl Display for RegisterStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "RegisterStorage")
    }
}

#[cfg(test)]
mod test {
    use super::RegisterStorage;

    use crate::node::{Error, Result};
    use crate::UsedSpace;
    use crate::{
        messaging::{
            data::{CreateRegister, RegisterCmd, RegisterQuery, SignedRegisterCreate},
            system::NodeQueryResponse,
            ServiceAuth,
        },
        types::register::{Policy, PublicPolicy},
    };
    use sn_interface::messaging::SectionAuth;
    use sn_interface::types::register::{EntryHash, PrivatePolicy};
    use sn_interface::types::DataAddress;
    use sn_interface::types::{register::User, Keypair};

    use rand::rngs::OsRng;
    use rand::Rng;
    use tempfile::tempdir;
    use xor_name::{Prefix, XorName};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_write() -> Result<()> {
        register_write(create_private_register).await?;
        register_write(create_public_register).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_delete() -> Result<()> {
        register_delete(create_private_register).await?;
        register_delete(create_public_register).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_export() -> Result<()> {
        register_export(create_private_register).await?;
        register_export(create_public_register).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_non_existing_entry() -> Result<()> {
        register_non_existing_entry(create_private_register).await?;
        register_non_existing_entry(create_public_register).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_non_existing_permissions() -> Result<()> {
        register_non_existing_permissions(create_private_register).await?;
        register_non_existing_permissions(create_public_register).await?;

        Ok(())
    }

    async fn register_write<F>(create_register: F) -> Result<()>
    where
        F: Fn() -> Result<(RegisterCmd, User)>,
    {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority) = create_register()?;
        let _ = store.write(cmd.clone()).await?;

        // get register

        let address = cmd.dst_address();
        let res = store.read(&RegisterQuery::Get(address), authority).await;
        match res {
            NodeQueryResponse::GetRegister((Ok(reg), _)) => {
                assert_eq!(reg.address(), &address, "Should have same address!");
                assert_eq!(reg.owner(), authority, "Should have same owner!");
            }
            e => panic!("Could not read! {:?}", e),
        }

        // try to create the register again
        // (should fail)

        let res = store.write(cmd.clone()).await;

        assert_eq!(
            res.err().unwrap().to_string(),
            Error::DataExists.to_string(),
            "Should not be able to create twice!"
        );

        Ok(())
    }

    async fn register_delete<F>(create_register: F) -> Result<()>
    where
        F: Fn() -> Result<(RegisterCmd, User)>,
    {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority) = create_register()?;
        let _ = store.write(cmd.clone()).await?;

        // try remove it
        let _ = store.drop_register_key(cmd.dst_address().id()?).await?;

        // should not get the removed register
        let res = store
            .read(&RegisterQuery::Get(cmd.dst_address()), authority)
            .await;

        use sn_interface::messaging::data::Error as MsgError;

        match res {
            NodeQueryResponse::GetRegister((Ok(_), _)) => panic!("Was not removed!"),
            NodeQueryResponse::GetRegister((Err(MsgError::DataNotFound(addr)), _)) => {
                assert_eq!(addr, DataAddress::Register(cmd.dst_address()))
            }
            e => panic!("Unexpected response! {:?}", e),
        }

        Ok(())
    }

    async fn register_export<F>(create_register: F) -> Result<()>
    where
        F: Fn() -> Result<(RegisterCmd, User)>,
    {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority) = create_register()?;
        let _ = store.write(cmd.clone()).await?;

        // export db
        // get all data in db
        let prefix = Prefix::new(0, cmd.name());
        let for_update = store.get_data_of(prefix).await?;

        // create new db and update it with the data from first db
        let new_store = new_store()?;

        let _ = new_store.update(for_update).await?;
        let address = cmd.dst_address();
        // assert the same tests hold as for the first db

        // should fail to write same register again, also on this new store
        let res = new_store.write(cmd).await;

        assert_eq!(
            res.err().unwrap().to_string(),
            Error::DataExists.to_string(),
            "Should not be able to create twice!"
        );

        // should be able to read the same value from this new store also
        let res = new_store
            .read(&RegisterQuery::Get(address), authority)
            .await;

        match res {
            NodeQueryResponse::GetRegister((Ok(reg), _)) => {
                assert_eq!(reg.address(), &address, "Should have same address!");
                assert_eq!(reg.owner(), authority, "Should have same owner!");
            }
            e => panic!("Could not read! {:?}", e),
        }

        Ok(())
    }

    async fn register_non_existing_entry<F>(create_register: F) -> Result<()>
    where
        F: Fn() -> Result<(RegisterCmd, User)>,
    {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority) = create_register()?;
        let _ = store.write(cmd.clone()).await?;

        let hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());

        // try get permissions of random user
        let address = cmd.dst_address();
        let res = store
            .read(&RegisterQuery::GetEntry { address, hash }, authority)
            .await;
        match res {
            NodeQueryResponse::GetRegisterEntry((Err(e), _)) => {
                assert_eq!(e, sn_interface::messaging::data::Error::NoSuchEntry)
            }
            NodeQueryResponse::GetRegisterEntry((Ok(entry), _)) => {
                panic!("Should not exist any entry for random hash! {:?}", entry)
            }
            e => panic!("Could not read! {:?}", e),
        }

        Ok(())
    }

    async fn register_non_existing_permissions<F>(create_register: F) -> Result<()>
    where
        F: Fn() -> Result<(RegisterCmd, User)>,
    {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority) = create_register()?;
        let _ = store.write(cmd.clone()).await?;

        let (user, _) = random_user();

        // try get permissions of random user
        let address = cmd.dst_address();
        let res = store
            .read(
                &RegisterQuery::GetUserPermissions { address, user },
                authority,
            )
            .await;
        match res {
            NodeQueryResponse::GetRegisterUserPermissions((Err(e), _)) => {
                assert_eq!(e, sn_interface::messaging::data::Error::NoSuchEntry)
            }
            NodeQueryResponse::GetRegisterUserPermissions((Ok(perms), _)) => panic!(
                "Should not exist any permissions for random user! {:?}",
                perms
            ),
            e => panic!("Could not read! {:?}", e),
        }

        Ok(())
    }

    fn new_store() -> Result<RegisterStorage> {
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path();
        let used_space = UsedSpace::new(usize::MAX);
        let store = RegisterStorage::new(path, used_space)?;
        Ok(store)
    }

    fn random_user() -> (User, Keypair) {
        let mut rng = OsRng;
        let keypair = Keypair::new_ed25519(&mut rng);
        let authority = User::Key(keypair.public_key());
        (authority, keypair)
    }

    fn create_private_register() -> Result<(RegisterCmd, User)> {
        let (authority, keypair) = random_user();
        let policy = Policy::Private(PrivatePolicy {
            owner: authority,
            permissions: Default::default(),
        });
        Ok((create_reg_w_policy(policy, keypair)?, authority))
    }

    fn create_public_register() -> Result<(RegisterCmd, User)> {
        let (authority, keypair) = random_user();
        let policy = Policy::Public(PublicPolicy {
            owner: authority,
            permissions: Default::default(),
        });
        Ok((create_reg_w_policy(policy, keypair)?, authority))
    }

    fn create_reg_w_policy(policy: Policy, keypair: Keypair) -> Result<RegisterCmd> {
        let op = CreateRegister::Empty {
            name: XorName::random(),
            tag: 1,
            size: u16::MAX,
            policy,
        };
        let signature = keypair.sign(&bincode::serialize(&op)?);

        let auth = ServiceAuth {
            public_key: keypair.public_key(),
            signature,
        };

        Ok(RegisterCmd::Create {
            cmd: SignedRegisterCreate { op, auth },
            section_auth: section_auth(),
        })
    }

    fn section_auth() -> SectionAuth {
        use sn_interface::messaging::system::KeyedSig;

        let sk = bls::SecretKey::random();
        let public_key = sk.public_key();
        let data = "hello".to_string();
        let signature = sk.sign(&data);
        let sig = KeyedSig {
            public_key,
            signature,
        };
        SectionAuth {
            src_name: sn_interface::types::PublicKey::Bls(public_key).into(),
            sig,
        }
    }
}
