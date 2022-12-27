// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    register_store::{RegisterStore, StoredRegister},
    Error, Result,
};

use sn_interface::{
    messaging::{
        data::{
            CreateRegister, EditRegister, RegisterCmd, RegisterQuery, SignedRegisterCreate,
            SignedRegisterEdit,
        },
        system::NodeQueryResponse,
        ClientAuth, SectionSig, VerifyAuthority,
    },
    types::{
        register::{Action, EntryHash, Permissions, Policy, Register, User},
        DataAddress, Keypair, PublicKey, RegisterAddress, ReplicatedRegisterLog,
        SPENTBOOK_TYPE_TAG,
    },
};

use crate::UsedSpace;
use bincode::serialize;
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    path::Path,
};
use tracing::info;
use xor_name::XorName;

const REGISTER_STORE_DIR_NAME: &str = "register";

/// Operations over the Register data type and its storage.
#[derive(Debug, Clone)]
pub(super) struct RegisterStorage {
    file_store: RegisterStore,
}

impl RegisterStorage {
    /// Create new `RegisterStorage`
    pub(super) fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        let file_store = RegisterStore::new(path.join(REGISTER_STORE_DIR_NAME), used_space)?;
        Ok(Self { file_store })
    }

    #[allow(dead_code)]
    pub(super) async fn remove_register(&self, address: &RegisterAddress) -> Result<()> {
        trace!("Removing register, {:?}", address);
        self.file_store.delete_data(address).await
    }

    pub(super) async fn addrs(&self) -> Vec<RegisterAddress> {
        self.file_store.list_all_reg_addrs().await
    }

    /// Used for replication of data to new Adults.
    pub(super) async fn get_register_replica(
        &self,
        address: &RegisterAddress,
    ) -> Result<ReplicatedRegisterLog> {
        let stored_reg = self.try_load_stored_register(address).await?;
        // Build the replicated register log assuming ops stored are all valid and correctly
        // signed since we performed such validations before storing them.
        Ok(ReplicatedRegisterLog {
            address: *address,
            op_log: stored_reg.op_log,
        })
    }

    /// Update our Register's replica on receiving data from other nodes.
    pub(super) async fn update(&self, data: &ReplicatedRegisterLog) -> Result<()> {
        debug!("Updating Register store: {:?}", data.address);
        let mut stored_reg = self.try_load_stored_register(&data.address).await?;

        let mut log_to_write = Vec::new();
        for replicated_cmd in &data.op_log {
            if let Err(err) = self
                .try_to_apply_cmd_against_register_state(replicated_cmd, &mut stored_reg)
                .await
            {
                warn!(
                    "Discarding ReplicatedRegisterLog cmd {:?}: {:?}",
                    replicated_cmd, err
                );
            } else {
                log_to_write.push(replicated_cmd.clone());
            }
        }

        // Write the new cmds all to disk
        self.file_store
            .write_log_to_disk(&log_to_write, &stored_reg.op_log_path)
            .await
    }

    /// --- Writing ---

    pub(super) async fn write(&self, cmd: &RegisterCmd) -> Result<()> {
        info!("Writing register cmd: {:?}", cmd);
        // Let's first try to load and reconstruct the replica of targetted Register
        // we have in local storage, to then try to apply the new command onto it.
        let mut stored_reg = self.try_load_stored_register(&cmd.dst_address()).await?;

        self.try_to_apply_cmd_against_register_state(cmd, &mut stored_reg)
            .await?;

        // Everything went fine, let's write the single cmd to disk
        self.file_store
            .write_log_to_disk(&vec![cmd.clone()], &stored_reg.op_log_path)
            .await
    }

    /// --- Reading ---

    pub(super) async fn read(&self, read: &RegisterQuery, requester: User) -> NodeQueryResponse {
        trace!("Reading register: {:?}", read.dst_address());
        use RegisterQuery::*;
        match read {
            Get(address) => self.get(*address, requester).await,
            Read(address) => self.read_register(*address, requester).await,
            GetOwner(address) => self.get_owner(*address, requester).await,
            GetEntry { address, hash } => self.get_entry(*address, *hash, requester).await,
            GetPolicy(address) => self.get_policy(*address, requester).await,
            GetUserPermissions { address, user } => {
                self.get_user_permissions(*address, *user, requester).await
            }
        }
    }

    /// Get `Register` from the store and check permissions.
    async fn get_register(
        &self,
        address: &RegisterAddress,
        action: Action,
        requester: User,
    ) -> Result<Register> {
        let stored_reg = self.try_load_stored_register(address).await?;
        if let Some(register) = stored_reg.state {
            register
                .check_permissions(action, Some(requester))
                .map_err(Error::from)?;

            Ok(register)
        } else {
            Err(Error::RegisterNotFound(*address))
        }
    }

    /// Get entire Register.
    async fn get(&self, address: RegisterAddress, requester: User) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(register) => Ok(register),
            Err(error) => {
                error!("Error reading register from disk {error:?}");
                Err(error.into())
            }
        };

        NodeQueryResponse::GetRegister(result)
    }

    async fn read_register(&self, address: RegisterAddress, requester: User) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(register) => Ok(register.read()),
            Err(error) => Err(error),
        };

        NodeQueryResponse::ReadRegister(result.map_err(|error| error.into()))
    }

    async fn get_owner(&self, address: RegisterAddress, requester: User) -> NodeQueryResponse {
        let result = match self.get_register(&address, Action::Read, requester).await {
            Ok(res) => Ok(res.owner()),
            Err(error) => Err(error.into()),
        };

        NodeQueryResponse::GetRegisterOwner(result)
    }

    async fn get_entry(
        &self,
        address: RegisterAddress,
        hash: EntryHash,
        requester: User,
    ) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester)
            .await
            .and_then(|register| register.get(hash).map(|c| c.clone()).map_err(Error::from))
        {
            Ok(res) => Ok(res),
            Err(error) => Err(error.into()),
        };

        NodeQueryResponse::GetRegisterEntry(result)
    }

    async fn get_user_permissions(
        &self,
        address: RegisterAddress,
        user: User,
        requester: User,
    ) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester)
            .await
            .and_then(|register| register.permissions(user).map_err(Error::from))
        {
            Ok(res) => Ok(res),
            Err(error) => Err(error.into()),
        };

        NodeQueryResponse::GetRegisterUserPermissions(result)
    }

    async fn get_policy(&self, address: RegisterAddress, requester_pk: User) -> NodeQueryResponse {
        let result = match self
            .get_register(&address, Action::Read, requester_pk)
            .await
            .map(|register| register.policy().clone())
        {
            Ok(res) => Ok(res),
            Err(error) => Err(error.into()),
        };

        NodeQueryResponse::GetRegisterPolicy(result)
    }

    // ========================================================================
    // =========================== Helpers ====================================
    // ========================================================================

    // Temporary helper function which makes sure there exists a Register for the spentbook,
    // this shouldn't be required once we have a Spentbook data type.
    pub(super) async fn write_spentbook_register(
        &self,
        cmd: &RegisterCmd,
        section_pk: PublicKey,
        node_keypair: Keypair,
    ) -> Result<()> {
        let address = cmd.dst_address();
        trace!("Creating new spentbook register: {:?}", address);

        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(User::Anyone, Permissions::new(true));
        let owner = User::Key(section_pk);
        let policy = Policy { owner, permissions };

        let create_cmd =
            create_reg_w_policy(*address.name(), SPENTBOOK_TYPE_TAG, policy, &node_keypair)?;

        self.update(&ReplicatedRegisterLog {
            address,
            op_log: [create_cmd, cmd.clone()].to_vec(),
        })
        .await
    }

    // Private helper which does all verification and tries to apply given cmd to given Register
    // state. It accumulates the cmd, if valid, into the log so further calls can be made with
    // the same state and log, as used by the `update` function.
    // Note the cmd is always pushed to the log even if it's a duplicated cmd.
    async fn try_to_apply_cmd_against_register_state(
        &self,
        cmd: &RegisterCmd,
        stored_reg: &mut StoredRegister,
    ) -> Result<()> {
        // If we have the target Register, try to apply the cmd, otherwise let's keep
        // the cmd in the log anyway, whenever we receive the 'Register create' cmd
        // it can be reconstructed from all cmds we hold in the log. If this is a 'Register create'
        // cmd let's verify it's valid before accepting it, however 'Edits cmds' cannot be
        // verified untill we have the `Register create` cmd.
        match (stored_reg.state.as_mut(), cmd) {
            (Some(ref mut register), cmd) => self.apply(cmd, register).await?,
            (None, RegisterCmd::Create { cmd, .. }) => {
                // the target Register is not in our store or we don't have the 'Register create',
                // let's verify the create cmd we received is valid and try to apply stored cmds we may have.
                let SignedRegisterCreate { op, auth } = cmd;
                let public_key = auth.public_key;
                let _ = auth
                    .clone()
                    .verify_authority(serialize(op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                trace!("Creating new register: {:?}", cmd.dst_address());
                // let's do a final check, let's try to apply all cmds to it,
                // those which are new cmds were not validated yet, so let's do it now.
                let mut register =
                    Register::new(*op.policy.owner(), op.name, op.tag, op.policy.clone());

                for cmd in &stored_reg.op_log {
                    self.apply(cmd, &mut register).await?;
                }

                stored_reg.state = Some(register);
            }
            (None, _edit_cmd) => { /* we cannot validate it right now, but we'll store it */ }
        }

        stored_reg.op_log.push(cmd.clone());
        Ok(())
    }

    // Try to apply the provided cmd to the register state, performing all op validations
    async fn apply(&self, cmd: &RegisterCmd, register: &mut Register) -> Result<()> {
        let addr = cmd.dst_address();
        if &addr != register.address() {
            return Err(Error::RegisterAddrMismatch {
                cmd_dst_addr: addr,
                reg_addr: *register.address(),
            });
        }

        match cmd {
            RegisterCmd::Create { .. } => Err(Error::DataExists(DataAddress::Register(addr))),
            RegisterCmd::Edit(SignedRegisterEdit { op, auth }) => {
                let public_key = auth.public_key;
                let _ = auth
                    .clone()
                    .verify_authority(serialize(op)?)
                    .or(Err(Error::InvalidSignature(public_key)))?;

                info!("Editing Register: {:?}", addr);
                register.check_permissions(Action::Write, Some(User::Key(public_key)))?;
                let result = register
                    .apply_op(op.edit.clone())
                    .map_err(Error::NetworkData);

                match result {
                    Ok(()) => {
                        trace!("Editing Register success: {:?}", addr);
                        Ok(())
                    }
                    Err(err) => {
                        trace!("Editing Register failed {:?}: {:?}", addr, err);
                        Err(err)
                    }
                }
            }
        }
    }

    // Gets stored register log from disk, trying to reconstruct the Register
    // Note this doesn't perform any cmd sig/perms validation, it's only used when the log
    // is read from disk which has already been validated before storing it.
    async fn try_load_stored_register(&self, addr: &RegisterAddress) -> Result<StoredRegister> {
        let mut stored_reg = self.file_store.open_reg_log_from_disk(addr).await?;
        // if we have the Register creation cmd, apply all ops to reconstruct the Register
        if let Some(register) = &mut stored_reg.state {
            for cmd in &stored_reg.op_log {
                if let RegisterCmd::Edit(SignedRegisterEdit { op, .. }) = cmd {
                    let EditRegister { edit, .. } = op;
                    register
                        .apply_op(edit.clone())
                        .map_err(Error::NetworkData)?;
                }
            }
        }

        Ok(stored_reg)
    }
}

impl Display for RegisterStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "RegisterStorage")
    }
}

// Helper functions temporarily used for spentbook logic, but also used for tests.
// This shouldn't be required outside of tests once we have a Spentbook data type.
fn create_reg_w_policy(
    name: XorName,
    tag: u64,
    policy: Policy,
    node_keypair: &Keypair,
) -> Result<RegisterCmd> {
    let op = CreateRegister { name, tag, policy };
    let signature = node_keypair.sign(&serialize(&op)?);

    let auth = ClientAuth {
        public_key: node_keypair.public_key(),
        signature,
    };

    Ok(RegisterCmd::Create {
        cmd: SignedRegisterCreate { op, auth },
        section_sig: section_sig(),
    })
}

fn section_sig() -> SectionSig {
    let sk = bls::SecretKey::random();
    let public_key = sk.public_key();
    let data = "TODO-spentbook".to_string();
    let signature = sk.sign(data);
    SectionSig {
        public_key,
        signature,
    }
}

#[cfg(test)]
mod test {
    use super::{create_reg_w_policy, Error, RegisterStorage, UsedSpace};
    use sn_interface::{
        messaging::{
            data::{EditRegister, RegisterCmd, RegisterQuery, SignedRegisterEdit},
            system::NodeQueryResponse,
            ClientAuth,
        },
        types::{
            register::{EntryHash, Policy, Register, User},
            DataAddress, Keypair,
        },
    };

    use bincode::serialize;
    use eyre::{bail, eyre, Result};
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::BTreeSet;
    use tempfile::tempdir;
    use xor_name::XorName;

    #[tokio::test]
    async fn test_register_try_load_stored() -> Result<()> {
        let store = new_store()?;

        let (cmd_create, _, keypair, name, policy) = create_register()?;
        let addr = cmd_create.dst_address();
        let log_path = store.file_store.address_to_filepath(&addr)?;
        let mut register = Register::new(*policy.owner(), name, 0, policy);

        let stored_reg = store.try_load_stored_register(&addr).await?;
        // it should *not* contain the create cmd
        assert!(stored_reg.state.is_none());
        assert!(stored_reg.op_log.is_empty());
        assert_eq!(stored_reg.op_log_path, log_path);

        store.write(&cmd_create).await?;
        let stored_reg = store.try_load_stored_register(&addr).await?;
        // it should contain the create cmd
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log, vec![cmd_create.clone()]);
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.map(|reg| reg.size()), Some(0));

        // let's now edit the register
        let cmd_edit = edit_register(&mut register, &keypair)?;
        store.write(&cmd_edit).await?;

        let stored_reg = store.try_load_stored_register(&addr).await?;
        // it should contain the create and edit cmds
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log.len(), 2);
        assert!(
            stored_reg
                .op_log
                .iter()
                .all(|op| [&cmd_create, &cmd_edit].contains(&op)),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.map(|reg| reg.size()), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_register_try_load_stored_inverted_cmds_order() -> Result<()> {
        let store = new_store()?;

        let (cmd_create, _, keypair, name, policy) = create_register()?;
        let addr = cmd_create.dst_address();
        let log_path = store.file_store.address_to_filepath(&addr)?;
        let mut register = Register::new(*policy.owner(), name, 0, policy);

        // let's first store an edit cmd for the register
        let cmd_edit = edit_register(&mut register, &keypair)?;
        store.write(&cmd_edit).await?;

        let stored_reg = store.try_load_stored_register(&addr).await?;
        // it should contain the edit cmd only
        assert_eq!(stored_reg.state, None);
        assert_eq!(stored_reg.op_log, vec![cmd_edit.clone()]);
        assert_eq!(stored_reg.op_log_path, log_path);

        // and now store the create cmd for the register
        store.write(&cmd_create).await?;

        let stored_reg = store.try_load_stored_register(&addr).await?;
        // it should contain the create and edit cmds
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log.len(), 2);
        assert!(
            stored_reg
                .op_log
                .iter()
                .all(|op| [&cmd_create, &cmd_edit].contains(&op)),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.map(|reg| reg.size()), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_register_apply_cmd_against_state() -> Result<()> {
        let store = new_store()?;

        let (cmd_create, _, keypair, name, policy) = create_register()?;
        let addr = cmd_create.dst_address();
        let log_path = store.file_store.address_to_filepath(&addr)?;
        let mut register = Register::new(*policy.owner(), name, 0, policy);
        let mut stored_reg = store.try_load_stored_register(&addr).await?;

        // apply the create cmd
        store
            .try_to_apply_cmd_against_register_state(&cmd_create, &mut stored_reg)
            .await?;
        // it should contain the create cmd
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log, vec![cmd_create.clone()]);
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.as_ref().map(|reg| reg.size()), Some(0));

        // apply the create cmd again should fail with DataExists
        match store
            .try_to_apply_cmd_against_register_state(&cmd_create, &mut stored_reg)
            .await
        {
            Ok(()) => bail!("An error should occur for this test case"),
            Err(Error::DataExists(DataAddress::Register(reported_addr))) => {
                assert_eq!(addr, reported_addr)
            }
            Err(err) => bail!("A Error::DataExists variant was expected: {:?}", err),
        }

        // let's now apply an edit cmd
        let cmd_edit = edit_register(&mut register, &keypair)?;
        store
            .try_to_apply_cmd_against_register_state(&cmd_edit, &mut stored_reg)
            .await?;
        // it should contain the create and edit cmds
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log.len(), 2);
        assert!(
            stored_reg
                .op_log
                .iter()
                .all(|op| [&cmd_create, &cmd_edit].contains(&op)),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.as_ref().map(|reg| reg.size()), Some(1));

        // applying the edit cmd again shouldn't fail or alter the register content,
        // although the log will contain the edit cmd duplicated
        store
            .try_to_apply_cmd_against_register_state(&cmd_edit, &mut stored_reg)
            .await?;
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log.len(), 3);
        assert!(
            stored_reg
                .op_log
                .iter()
                .all(|op| [&cmd_create, &cmd_edit].contains(&op)),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.map(|reg| reg.size()), Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_register_apply_cmd_against_state_inverted_cmds_order() -> Result<()> {
        let store = new_store()?;

        let (cmd_create, _, keypair, name, policy) = create_register()?;
        let addr = cmd_create.dst_address();
        let log_path = store.file_store.address_to_filepath(&addr)?;
        let mut register = Register::new(*policy.owner(), name, 0, policy);
        let mut stored_reg = store.try_load_stored_register(&addr).await?;

        // apply an edit cmd first
        let cmd_edit = edit_register(&mut register, &keypair)?;
        store
            .try_to_apply_cmd_against_register_state(&cmd_edit, &mut stored_reg)
            .await?;
        // it should contain the edit cmd
        assert_eq!(stored_reg.state, None);
        assert_eq!(stored_reg.op_log, vec![cmd_edit.clone()]);
        assert_eq!(stored_reg.op_log_path, log_path);

        // applying the edit cmd again shouldn't fail,
        // although the log will contain the edit cmd duplicated
        store
            .try_to_apply_cmd_against_register_state(&cmd_edit, &mut stored_reg)
            .await?;
        assert_eq!(stored_reg.state, None);
        assert_eq!(stored_reg.op_log.len(), 2);
        assert!(
            stored_reg.op_log.iter().all(|op| op == &cmd_edit),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);

        // let's apply the create cmd now
        store
            .try_to_apply_cmd_against_register_state(&cmd_create, &mut stored_reg)
            .await?;
        // it should contain the create and edit cmds
        assert_eq!(stored_reg.state.as_ref(), Some(&register));
        assert_eq!(stored_reg.op_log.len(), 3);
        assert!(
            stored_reg
                .op_log
                .iter()
                .all(|op| [&cmd_create, &cmd_edit].contains(&op)),
            "Op log doesn't match"
        );
        assert_eq!(stored_reg.op_log_path, log_path);
        assert_eq!(stored_reg.state.as_ref().map(|reg| reg.size()), Some(1));

        // apply the create cmd again should fail with DataExists
        match store
            .try_to_apply_cmd_against_register_state(&cmd_create, &mut stored_reg)
            .await
        {
            Ok(()) => bail!("An error should occur for this test case"),
            Err(Error::DataExists(DataAddress::Register(reported_addr))) => {
                assert_eq!(addr, reported_addr)
            }
            Err(err) => bail!("A Error::DataExists variant was expected: {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_register_write() -> Result<()> {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd, authority, _, _, _) = create_register()?;
        store.write(&cmd).await?;

        // get register
        let address = cmd.dst_address();
        match store.read(&RegisterQuery::Get(address), authority).await {
            NodeQueryResponse::GetRegister(Ok(reg)) => {
                assert_eq!(reg.address(), &address, "Should have same address!");
                assert_eq!(reg.owner(), authority, "Should have same owner!");
            }
            e => bail!("Could not read register! {:?}", e),
        }

        match store.write(&cmd).await {
            Ok(()) => Err(eyre!("An error should occur for this test case")),
            Err(error @ Error::DataExists(_)) => {
                assert_eq!(
                    error.to_string(),
                    format!(
                        "Data already exists at this node: {:?}",
                        DataAddress::Register(address)
                    )
                );
                Ok(())
            }
            Err(err) => Err(eyre!("A Error::DataExists variant was expected: {:?}", err)),
        }
    }

    #[tokio::test]
    async fn test_register_export() -> Result<()> {
        // setup store
        let store = new_store()?;

        let (cmd_create, authority, keypair, name, policy) = create_register()?;
        let addr = cmd_create.dst_address();
        let mut register = Register::new(*policy.owner(), name, 0, policy);

        // store the register along with a few edit ops
        store.write(&cmd_create).await?;
        for _ in 0..10 {
            let cmd_edit = edit_register(&mut register, &keypair)?;
            store.write(&cmd_edit).await?;
        }

        // should fail to write same register again
        match store.write(&cmd_create).await {
            Ok(()) => bail!("An error should occur for this test case"),
            Err(error @ Error::DataExists(_)) => assert_eq!(
                error.to_string(),
                format!(
                    "Data already exists at this node: {:?}",
                    DataAddress::Register(addr)
                )
            ),
            Err(err) => bail!("A Error::DataExists variant was expected: {:?}", err),
        }

        // export Registers, get all data we held in storage
        let all_addrs = store.addrs().await;

        // create new store and update it with the data from first store
        let new_store = new_store()?;
        for addr in all_addrs {
            let replica = store.get_register_replica(&addr).await?;
            new_store.update(&replica).await?;
        }

        // assert the same tests hold as for the first store
        // should fail to write same register again, also on this new store
        match new_store.write(&cmd_create).await {
            Ok(()) => bail!("An error should occur for this test case"),
            Err(error @ Error::DataExists(_)) => assert_eq!(
                error.to_string(),
                format!(
                    "Data already exists at this node: {:?}",
                    DataAddress::Register(addr)
                )
            ),
            Err(err) => bail!("A Error::DataExists variant was expected: {:?}", err),
        }

        // should be able to read the same value from this new store also
        let res = new_store.read(&RegisterQuery::Get(addr), authority).await;

        match res {
            NodeQueryResponse::GetRegister(Ok(reg)) => {
                assert_eq!(reg.address(), &addr, "Should have same address!");
                assert_eq!(reg.owner(), authority, "Should have same owner!");
            }
            e => panic!("Could not read! {:?}", e),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_register_non_existing_entry() -> Result<()> {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd_create, authority, _, _, _) = create_register()?;
        store.write(&cmd_create).await?;

        let hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());

        // try get permissions of random user
        let address = cmd_create.dst_address();
        let res = store
            .read(&RegisterQuery::GetEntry { address, hash }, authority)
            .await;
        match res {
            NodeQueryResponse::GetRegisterEntry(Err(e)) => {
                assert_eq!(e, sn_interface::messaging::data::Error::NoSuchEntry(hash))
            }
            NodeQueryResponse::GetRegisterEntry(Ok(entry)) => {
                panic!("Should not exist any entry for random hash! {:?}", entry)
            }
            e => panic!("Could not read! {:?}", e),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_register_non_existing_permissions() -> Result<()> {
        // setup store
        let store = new_store()?;

        // create register
        let (cmd_create, authority, _, _, _) = create_register()?;
        store.write(&cmd_create).await?;

        let (user, _) = random_user();

        // try get permissions of random user
        let address = cmd_create.dst_address();
        let res = store
            .read(
                &RegisterQuery::GetUserPermissions { address, user },
                authority,
            )
            .await;
        match res {
            NodeQueryResponse::GetRegisterUserPermissions(Err(e)) => {
                assert_eq!(e, sn_interface::messaging::data::Error::NoSuchUser(user))
            }
            NodeQueryResponse::GetRegisterUserPermissions(Ok(perms)) => panic!(
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
        let used_space = UsedSpace::new(usize::MAX, usize::MAX);
        let store = RegisterStorage::new(path, used_space)?;
        Ok(store)
    }

    fn random_user() -> (User, Keypair) {
        let keypair = Keypair::new_ed25519();
        let authority = User::Key(keypair.public_key());
        (authority, keypair)
    }

    fn create_register() -> Result<(RegisterCmd, User, Keypair, XorName, Policy)> {
        let (authority, keypair) = random_user();
        let policy = Policy {
            owner: authority,
            permissions: Default::default(),
        };
        let xorname = xor_name::rand::random();
        let cmd = create_reg_w_policy(xorname, 0, policy.clone(), &keypair)?;

        Ok((cmd, authority, keypair, xorname, policy))
    }

    fn edit_register(register: &mut Register, keypair: &Keypair) -> Result<RegisterCmd> {
        let data = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .collect();
        let (_, edit) = register.write(data, BTreeSet::default())?;
        let op = EditRegister {
            address: *register.address(),
            edit,
        };
        let signature = keypair.sign(&serialize(&op)?);

        Ok(RegisterCmd::Edit(SignedRegisterEdit {
            op,
            auth: ClientAuth {
                public_key: keypair.public_key(),
                signature,
            },
        }))
    }
}
