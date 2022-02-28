// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod metadata;
mod policy;
mod reg_crdt;

pub use metadata::{Action, Entry};
pub use policy::{
    Permissions, Policy, PrivatePermissions, PrivatePolicy, PublicPermissions, PublicPolicy, User,
};
pub use reg_crdt::EntryHash;

use super::{Error, Result};
use crate::{types::RegisterAddress as Address, types::Scope};
use reg_crdt::{CrdtOperation, RegisterCrdt};
use self_encryption::MIN_ENCRYPTABLE_BYTES;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hash,
};
use xor_name::XorName;

/// Arbitrary maximum size of a register entry.
const MAX_REG_ENTRY_SIZE: usize = MIN_ENCRYPTABLE_BYTES / 3; // 1024 bytes

/// Register mutation operation to apply to Register.
pub type RegisterOp<T> = CrdtOperation<T>;

/// Object storing the Register
#[derive(Clone, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct Register {
    authority: User,
    crdt: RegisterCrdt,
    policy: Policy,
    cap: u16,
}

impl Register {
    ///
    pub fn new(name: XorName, tag: u64, policy: Policy, cap: u16) -> Self {
        let address = if matches!(policy, Policy::Public(_)) {
            Address::Public { name, tag }
        } else {
            Address::Private { name, tag }
        };
        Self {
            authority: *policy.owner(),
            crdt: RegisterCrdt::new(address),
            policy,
            cap,
        }
    }

    /// Construct a new Public Register.
    /// The 'authority' is assumed to be the PK which the messages were and will be
    /// signed with.
    /// If a policy is not provided, a default policy will be set where
    /// the 'authority' is the owner along with an empty users permissions set.
    pub fn new_public(
        authority: User,
        name: XorName,
        tag: u64,
        policy: Option<PublicPolicy>,
        cap: u16,
    ) -> Self {
        let policy = policy.unwrap_or(PublicPolicy {
            owner: authority,
            permissions: BTreeMap::new(),
        });

        Self {
            authority,
            crdt: RegisterCrdt::new(Address::Public { name, tag }),
            policy: policy.into(),
            cap,
        }
    }

    /// Construct a new Private Register.
    /// The 'authority' is assumed to be the PK which the messages were and will be
    /// signed with.
    /// If a policy is not provided, a default policy will be set where
    /// the 'authority' is the owner along with an empty users permissions set.
    pub fn new_private(
        authority: User,
        name: XorName,
        tag: u64,
        policy: Option<PrivatePolicy>,
        cap: u16,
    ) -> Self {
        let policy = policy.unwrap_or(PrivatePolicy {
            owner: authority,
            permissions: BTreeMap::new(),
        });

        Self {
            authority,
            crdt: RegisterCrdt::new(Address::Private { name, tag }),
            policy: policy.into(),
            cap,
        }
    }

    /// Return `true` if public.
    pub fn is_public(&self) -> bool {
        self.address().is_public()
    }

    /// Return `true` if private.
    pub fn is_private(&self) -> bool {
        self.address().is_private()
    }

    /// Return the address.
    pub fn address(&self) -> &Address {
        self.crdt.address()
    }

    /// Return the scope.
    pub fn scope(&self) -> Scope {
        self.address().scope()
    }

    /// Return the name.
    pub fn name(&self) -> &XorName {
        self.address().name()
    }

    /// Return the tag.
    pub fn tag(&self) -> u64 {
        self.address().tag()
    }

    /// Return the owner of the data.
    pub fn owner(&self) -> User {
        *self.policy.owner()
    }

    /// Return the PK which the messages are expected to be signed with by this replica.
    pub fn replica_authority(&self) -> User {
        self.authority
    }

    /// Return the max number of items that can be held in the register.
    pub fn cap(&self) -> u16 {
        self.cap
    }

    /// Return the number of items held in the register
    pub fn size(&self) -> u64 {
        self.crdt.size()
    }

    /// Return true if the register is empty.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Return a value corresponding to the provided 'hash', if present.
    pub fn get(&self, hash: EntryHash) -> Result<&Entry> {
        self.crdt.get(hash).ok_or(Error::NoSuchEntry)
    }

    /// Read the last entry, or entries when there are branches, if the register is not empty.
    pub fn read(&self) -> BTreeSet<(EntryHash, Entry)> {
        self.crdt.read()
    }

    /// Return user permissions, if applicable.
    pub fn permissions(&self, user: User) -> Result<Permissions> {
        self.policy.permissions(user).ok_or(Error::NoSuchEntry)
    }

    /// Return the policy.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Increment the size cap of the register, returning the previous value.
    pub fn increment_cap(&mut self, add: u16) {
        self.cap += add;
    }

    /// Write an entry to the Register, returning the generated unsigned
    /// CRDT operation so the caller can sign and broadcast it to other replicas,
    /// along with the hash of the entry just written.
    pub fn write(
        &mut self,
        entry: Entry,
        children: BTreeSet<EntryHash>,
    ) -> Result<(EntryHash, RegisterOp<Entry>)> {
        let size = entry.len();
        if size > MAX_REG_ENTRY_SIZE {
            return Err(Error::EntryTooBig(size, MAX_REG_ENTRY_SIZE));
        }
        if self.crdt.size() >= self.cap() as u64 {
            return Err(Error::TooManyEntries(self.crdt.size() as usize));
        }

        self.crdt.write(entry, children, self.authority)
    }

    /// Apply a signed data CRDT operation.
    pub fn apply_op(&mut self, op: RegisterOp<Entry>) -> Result<()> {
        self.crdt.apply_op(op)
    }

    /// Helper to check permissions for given `action`
    /// for the given requester's public key.
    ///
    /// Returns:
    /// `Ok(())` if the permissions are valid,
    /// `Err::AccessDenied` if the action is not allowed.
    pub fn check_permissions(&self, action: Action, requester: Option<User>) -> Result<()> {
        let requester = requester.unwrap_or(self.authority);
        self.policy.is_action_allowed(requester, action)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        register::{
            Entry, EntryHash, Permissions, PrivatePermissions, PrivatePolicy, PublicPermissions,
            PublicPolicy, Register, RegisterOp, User,
        },
        utils, Error, Keypair, Result,
    };
    use crate::{types::RegisterAddress as Address, types::Scope};
    use proptest::prelude::*;
    use rand::{rngs::OsRng, seq::SliceRandom, thread_rng};
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };
    use xor_name::XorName;

    #[test]
    fn register_create_public() {
        let name = XorName::random();
        let tag = 43_000;
        let (authority_keypair, register) = &gen_pub_reg_replicas(None, name, tag, None, 1)[0];

        assert_eq!(register.scope(), Scope::Public);
        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert!(register.is_public());
        assert!(!register.is_private());

        let authority = User::Key(authority_keypair.public_key());
        assert_eq!(register.owner(), authority);
        assert_eq!(register.replica_authority(), authority);

        let address = Address::new(name, Scope::Public, tag);
        assert_eq!(*register.address(), address);
    }

    #[test]
    fn register_create_private() {
        let name = XorName::random();
        let tag = 43_000;
        let (authority_keypair, register) = &gen_priv_reg_replicas(None, name, tag, None, 1)[0];

        assert_eq!(register.scope(), Scope::Private);
        assert_eq!(*register.name(), name);
        assert_eq!(register.tag(), tag);
        assert!(!register.is_public());
        assert!(register.is_private());

        let authority = User::Key(authority_keypair.public_key());
        assert_eq!(register.owner(), authority);
        assert_eq!(register.replica_authority(), authority);

        let address = Address::new(name, Scope::Private, tag);
        assert_eq!(*register.address(), address);
    }

    #[test]
    fn register_concurrent_write_ops() -> Result<()> {
        let authority_keypair1 = Keypair::new_ed25519(&mut OsRng);
        let authority1 = User::Key(authority_keypair1.public_key());
        let authority_keypair2 = Keypair::new_ed25519(&mut OsRng);
        let authority2 = User::Key(authority_keypair2.public_key());

        let name: XorName = rand::random();
        let tag = 43_000u64;
        let cap = u16::MAX;

        // We'll have 'authority1' as the owner in both replicas and
        // grant permissions for Write to 'authority2' in both replicas too
        let mut perms = BTreeMap::default();
        let user_perms = PublicPermissions::new(true);
        let _prev = perms.insert(authority2, user_perms);

        // Instantiate the same Register on two replicas with the two diff authorities
        let mut replica1 = Register::new_public(
            authority1,
            name,
            tag,
            Some(PublicPolicy {
                owner: authority1,
                permissions: perms.clone(),
            }),
            cap,
        );
        let mut replica2 = Register::new_public(
            authority2,
            name,
            tag,
            Some(PublicPolicy {
                owner: authority1,
                permissions: perms,
            }),
            cap,
        );

        // And let's write an item to replica1 with autority1
        let item1 = random_register_entry();
        let (_, op1) = replica1.write(item1, BTreeSet::new())?;
        let signed_write_op1 = sign_register_op(op1, &authority_keypair1)?;

        // Let's assert current state on both replicas
        assert_eq!(replica1.size(), 1);
        assert_eq!(replica2.size(), 0);

        // Concurrently write another item with authority2 on replica2
        let item2 = random_register_entry();
        let (_, op2) = replica2.write(item2, BTreeSet::new())?;
        let signed_write_op2 = sign_register_op(op2, &authority_keypair2)?;

        // Item should be writeed on replica2
        assert_eq!(replica2.size(), 1);

        // Write operations are now broadcasted and applied to both replicas
        replica1.apply_op(signed_write_op2)?;
        replica2.apply_op(signed_write_op1)?;

        // Let's assert data convergence on both replicas
        verify_data_convergence(vec![replica1, replica2], 2)?;

        Ok(())
    }

    #[test]
    fn register_get_by_hash() -> eyre::Result<()> {
        let (_, register) = &mut create_public_reg_replicas(1)[0];

        let entry1 = random_register_entry();
        let entry2 = random_register_entry();
        let entry3 = random_register_entry();

        let (entry1_hash, _) = register.write(entry1.clone(), BTreeSet::new())?;

        // this creates a fork since entry1 is not set as child of entry2
        let (entry2_hash, _) = register.write(entry2.clone(), BTreeSet::new())?;

        // we'll write entry2 but having the entry1 and entry2 as children,
        // i.e. solving the fork created by them
        let children = vec![entry1_hash, entry2_hash].into_iter().collect();

        let (entry3_hash, _) = register.write(entry3.clone(), children)?;

        assert_eq!(register.size(), 3);

        let first_entry = register.get(entry1_hash)?;
        assert_eq!(first_entry, &entry1);

        let second_entry = register.get(entry2_hash)?;
        assert_eq!(second_entry, &entry2);

        let third_entry = register.get(entry3_hash)?;
        assert_eq!(third_entry, &entry3);

        let non_existing_hash = EntryHash::default();
        let entry_not_found = register.get(non_existing_hash);
        assert_eq!(entry_not_found, Err(Error::NoSuchEntry));

        Ok(())
    }

    #[test]
    fn register_query_public_policy() -> eyre::Result<()> {
        let name = XorName::random();
        let tag = 43_666;

        // one replica will allow write ops to anyone
        let authority_keypair1 = Keypair::new_ed25519(&mut OsRng);
        let owner1 = User::Key(authority_keypair1.public_key());
        let mut perms1 = BTreeMap::default();
        let _prev = perms1.insert(User::Anyone, PublicPermissions::new(true));
        let replica1 = create_public_reg_replica_with(
            name,
            tag,
            Some(authority_keypair1),
            Some(PublicPolicy {
                owner: owner1,
                permissions: perms1,
            }),
        );

        // the other replica will allow write ops to 'owner1' and 'authority2' only
        let authority_keypair2 = Keypair::new_ed25519(&mut OsRng);
        let authority2 = User::Key(authority_keypair2.public_key());
        let mut perms2 = BTreeMap::default();
        let _prev = perms2.insert(owner1, PublicPermissions::new(true));
        let replica2 = create_public_reg_replica_with(
            name,
            tag,
            Some(authority_keypair2),
            Some(PublicPolicy {
                owner: authority2,
                permissions: perms2,
            }),
        );

        assert_eq!(replica1.owner(), owner1);
        assert_eq!(replica1.replica_authority(), owner1);
        assert_eq!(
            replica1.policy().permissions(User::Anyone),
            Some(Permissions::Public(PublicPermissions::new(true))),
        );
        assert_eq!(
            replica1.permissions(User::Anyone)?,
            Permissions::Public(PublicPermissions::new(true)),
        );

        assert_eq!(replica2.owner(), authority2);
        assert_eq!(replica2.replica_authority(), authority2);
        assert_eq!(
            replica2.policy().permissions(owner1),
            Some(Permissions::Public(PublicPermissions::new(true))),
        );
        assert_eq!(
            replica2.permissions(owner1)?,
            Permissions::Public(PublicPermissions::new(true)),
        );

        let random_keypair = Keypair::new_ed25519(&mut OsRng);
        let random_user = User::Key(random_keypair.public_key());
        assert_eq!(replica2.permissions(random_user), Err(Error::NoSuchEntry),);

        Ok(())
    }

    #[test]
    fn register_query_private_policy() -> eyre::Result<()> {
        let name = XorName::random();
        let tag = 43_666;

        let authority_keypair1 = Keypair::new_ed25519(&mut OsRng);
        let authority1 = User::Key(authority_keypair1.public_key());
        let authority_keypair2 = Keypair::new_ed25519(&mut OsRng);
        let authority2 = User::Key(authority_keypair2.public_key());

        let mut perms1 = BTreeMap::default();
        let user_perms1 = PrivatePermissions::new(/*read*/ true, /*write*/ false); // trying to set write perms to false for the owner (will not be reflected as long as the user is the owner, as an owner will have full authority)
        let _prev = perms1.insert(authority1, user_perms1);

        let mut perms2 = BTreeMap::default();
        let user_perms2 = PrivatePermissions::new(/*read*/ true, /*write*/ true);
        let _prev = perms2.insert(authority2, user_perms2);
        let user_perms2 = PrivatePermissions::new(/*read*/ false, /*write*/ true);
        let _prev = perms2.insert(authority1, user_perms2);

        let replica1 = create_private_reg_replica_with(
            name,
            tag,
            Some(authority_keypair1),
            Some(PrivatePolicy {
                owner: authority1,
                permissions: perms1,
            }),
        );

        let replica2 = create_private_reg_replica_with(
            name,
            tag,
            Some(authority_keypair2),
            Some(PrivatePolicy {
                owner: authority2,
                permissions: perms2,
            }),
        );

        assert_eq!(replica1.owner(), authority1);
        assert_eq!(replica1.replica_authority(), authority1);
        // above the owner perms were set to more restrictive than full, we test below that
        // write&read perms for the owner will always be true, as an owner will always have full authority (even if perms were explicitly set some other way)
        assert_eq!(
            replica1.policy().permissions(authority1),
            Some(Permissions::Private(PrivatePermissions::new(true, true))),
        );
        assert_eq!(
            replica1.permissions(authority1)?,
            Permissions::Private(PrivatePermissions::new(true, true)),
        );

        assert_eq!(replica2.owner(), authority2);
        assert_eq!(replica2.replica_authority(), authority2);
        assert_eq!(
            replica2.policy().permissions(authority2),
            Some(Permissions::Private(PrivatePermissions::new(true, true))),
        );
        assert_eq!(
            replica2.permissions(authority2)?,
            Permissions::Private(PrivatePermissions::new(true, true)),
        );
        assert_eq!(
            replica2.permissions(authority1)?,
            Permissions::Private(PrivatePermissions::new(false, true)),
        );

        let random_keypair = Keypair::new_ed25519(&mut OsRng);
        let random_user = User::Key(random_keypair.public_key());
        assert_eq!(replica2.permissions(random_user), Err(Error::NoSuchEntry),);

        Ok(())
    }

    // Helpers for tests

    fn sign_register_op(mut op: RegisterOp<Entry>, keypair: &Keypair) -> Result<RegisterOp<Entry>> {
        let bytes = utils::serialise(&op.crdt_op)?;
        let signature = keypair.sign(&bytes);
        op.signature = Some(signature);
        Ok(op)
    }

    fn gen_pub_reg_replicas(
        authority_keypair: Option<Keypair>,
        name: XorName,
        tag: u64,
        policy: Option<PublicPolicy>,
        count: usize,
    ) -> Vec<(Keypair, Register)> {
        let replicas: Vec<(Keypair, Register)> = (0..count)
            .map(|_| {
                let authority_keypair = authority_keypair
                    .clone()
                    .unwrap_or_else(|| Keypair::new_ed25519(&mut OsRng));
                let authority = User::Key(authority_keypair.public_key());
                let register = Register::new_public(authority, name, tag, policy.clone(), u16::MAX);
                (authority_keypair, register)
            })
            .collect();

        assert_eq!(replicas.len(), count);
        replicas
    }

    fn gen_priv_reg_replicas(
        authority_keypair: Option<Keypair>,
        name: XorName,
        tag: u64,
        policy: Option<PrivatePolicy>,
        count: usize,
    ) -> Vec<(Keypair, Register)> {
        let replicas: Vec<(Keypair, Register)> = (0..count)
            .map(|_| {
                let authority_keypair = authority_keypair
                    .clone()
                    .unwrap_or_else(|| Keypair::new_ed25519(&mut OsRng));
                let authority = User::Key(authority_keypair.public_key());
                let register =
                    Register::new_private(authority, name, tag, policy.clone(), u16::MAX);
                (authority_keypair, register)
            })
            .collect();

        assert_eq!(replicas.len(), count);
        replicas
    }

    fn create_public_reg_replicas(count: usize) -> Vec<(Keypair, Register)> {
        let name = XorName::random();
        let tag = 43_000;

        gen_pub_reg_replicas(None, name, tag, None, count)
    }

    fn create_public_reg_replica_with(
        name: XorName,
        tag: u64,
        authority_keypair: Option<Keypair>,
        policy: Option<PublicPolicy>,
    ) -> Register {
        let replicas = gen_pub_reg_replicas(authority_keypair, name, tag, policy, 1);
        replicas[0].1.clone()
    }

    fn create_private_reg_replica_with(
        name: XorName,
        tag: u64,
        authority_keypair: Option<Keypair>,
        policy: Option<PrivatePolicy>,
    ) -> Register {
        let replicas = gen_priv_reg_replicas(authority_keypair, name, tag, policy, 1);
        replicas[0].1.clone()
    }

    // verify data convergence on a set of replicas and with the expected length
    fn verify_data_convergence(replicas: Vec<Register>, expected_size: u64) -> Result<()> {
        // verify all replicas have the same and expected size
        for r in &replicas {
            assert_eq!(r.size(), expected_size);
        }

        // now verify that the items are the same in all replicas
        let r0 = &replicas[0];
        for r in replicas.iter() {
            assert_eq!(r.crdt, r0.crdt);
        }

        Ok(())
    }

    // Generate a vec of Register replicas of some length, with corresponding vec of keypairs for signing, and the overall owner of the register
    fn generate_replicas(
        max_quantity: usize,
    ) -> impl Strategy<Value = Result<(Vec<Register>, Arc<Keypair>)>> {
        let xorname = XorName::random();
        let tag = 45_000u64;

        let owner_keypair = Arc::new(Keypair::new_ed25519(&mut OsRng));
        let owner = User::Key(owner_keypair.public_key());
        let policy = PublicPolicy {
            owner,
            permissions: BTreeMap::default(),
        };

        (1..max_quantity + 1).prop_map(move |quantity| {
            let mut replicas = Vec::with_capacity(quantity);
            for _ in 0..quantity {
                let replica =
                    Register::new_public(owner, xorname, tag, Some(policy.clone()), u16::MAX);

                replicas.push(replica);
            }

            Ok((replicas, owner_keypair.clone()))
        })
    }

    // Generate a Register entry
    fn generate_reg_entry() -> impl Strategy<Value = Vec<u8>> {
        "\\PC*".prop_map(|s| s.into_bytes())
    }

    // Generate a vec of Register entries
    fn generate_dataset(max_quantity: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop::collection::vec(generate_reg_entry(), 1..max_quantity + 1)
    }

    // Generates a vec of Register entries each with a value suggesting
    // the delivery chance of the op that gets created with the entry
    fn generate_dataset_and_probability(
        max_quantity: usize,
    ) -> impl Strategy<Value = Vec<(Vec<u8>, u8)>> {
        prop::collection::vec((generate_reg_entry(), any::<u8>()), 1..max_quantity + 1)
    }

    proptest! {
        #[test]
        fn proptest_reg_doesnt_crash_with_random_data(
            _data in generate_reg_entry()
        ) {
            // Instantiate the same Register on two replicas
            let name = XorName::random();
            let tag = 45_000u64;
            let owner_keypair = Keypair::new_ed25519(&mut OsRng);
            let policy = PublicPolicy {
                owner: User::Key(owner_keypair.public_key()),
                permissions: BTreeMap::default(),
            };

            let mut replicas = gen_pub_reg_replicas(
                Some(owner_keypair.clone()),
                name,
                tag,
                Some(policy),
                2);
            let (_, mut replica1) = replicas.remove(0);
            let (_, mut replica2) = replicas.remove(0);

            // Write an item on replicas
            let (_, op) = replica1.write(random_register_entry(), BTreeSet::new())?;
            let write_op = sign_register_op(op, &owner_keypair)?;
            replica2.apply_op(write_op)?;

            verify_data_convergence(vec![replica1, replica2], 1)?;
        }

        #[test]
        fn proptest_reg_converge_with_many_random_data(
            dataset in generate_dataset(1000)
        ) {
            // Instantiate the same Register on two replicas
            let name = XorName::random();
            let tag = 43_000u64;
            let owner_keypair = Keypair::new_ed25519(&mut OsRng);
            let policy = PublicPolicy {
                owner: User::Key(owner_keypair.public_key()),
                permissions: BTreeMap::default(),
            };

            // Instantiate the same Register on two replicas
            let mut replicas = gen_pub_reg_replicas(
                Some(owner_keypair.clone()),
                name,
                tag,
                Some(policy),
                2);
            let (_, mut replica1) = replicas.remove(0);
            let (_, mut replica2) = replicas.remove(0);

            let dataset_length = dataset.len() as u64;

            // insert our data at replicas
            let mut children = BTreeSet::new();
            for _data in dataset {
                // Write an item on replica1
                let (hash, op) = replica1.write(random_register_entry(), children.clone())?;
                let write_op = sign_register_op(op, &owner_keypair)?;
                // now apply that op to replica 2
                replica2.apply_op(write_op)?;
                children = vec![hash].into_iter().collect();
            }

            verify_data_convergence(vec![replica1, replica2], dataset_length)?;
        }

        #[test]
        fn proptest_reg_converge_with_many_random_data_random_entry_children(
            dataset in generate_dataset(1000)
        ) {
            // Instantiate the same Register on two replicas
            let name = XorName::random();
            let tag = 43_000u64;
            let owner_keypair = Keypair::new_ed25519(&mut OsRng);
            let policy = PublicPolicy {
                owner: User::Key(owner_keypair.public_key()),
                permissions: BTreeMap::default(),
            };

            // Instantiate the same Register on two replicas
            let mut replicas = gen_pub_reg_replicas(
                Some(owner_keypair.clone()),
                name,
                tag,
                Some(policy),
                2);
            let (_, mut replica1) = replicas.remove(0);
            let (_, mut replica2) = replicas.remove(0);

            let dataset_length = dataset.len() as u64;

            // insert our data at replicas
            let mut list_of_hashes = Vec::new();
            let mut rng = thread_rng();
            for _data in dataset {
                // choose a random set of children
                let num_of_children: usize = rng.gen();
                let children: BTreeSet<_> = list_of_hashes.choose_multiple(&mut OsRng, num_of_children).cloned().collect();

                // Write an item on replica1 using the randomly generated set of children
                let (hash, op) = replica1.write(random_register_entry(), children)?;
                let write_op = sign_register_op(op, &owner_keypair)?;

                // now apply that op to replica 2
                replica2.apply_op(write_op)?;
                list_of_hashes.push(hash);
            }

            verify_data_convergence(vec![replica1, replica2], dataset_length)?;
        }

        #[test]
        fn proptest_reg_converge_with_many_random_data_across_arbitrary_number_of_replicas(
            dataset in generate_dataset(500),
            res in generate_replicas(50)
        ) {
            let (mut replicas, owner_keypair) = res?;
            let dataset_length = dataset.len() as u64;

            // insert our data at replicas
            let mut children = BTreeSet::new();
            for _data in dataset {
                // first generate an op from one replica...
                let (hash, op)= replicas[0].write(random_register_entry(), children)?;
                let signed_op = sign_register_op(op, &owner_keypair)?;

                // then apply this to all replicas
                for replica in &mut replicas {
                    replica.apply_op(signed_op.clone())?;
                }
                children = vec![hash].into_iter().collect();
            }

            verify_data_convergence(replicas, dataset_length)?;

        }

        #[test]
        fn proptest_converge_with_shuffled_op_set_across_arbitrary_number_of_replicas(
            dataset in generate_dataset(100),
            res in generate_replicas(500)
        ) {
            let (mut replicas, owner_keypair) = res?;
            let dataset_length = dataset.len() as u64;

            // generate an ops set from one replica
            let mut ops = vec![];

            let mut children = BTreeSet::new();
            for _data in dataset {
                let (hash, op) = replicas[0].write(random_register_entry(), children)?;
                let signed_op = sign_register_op(op, &owner_keypair)?;
                ops.push(signed_op);
                children = vec![hash].into_iter().collect();
            }

            // now we randomly shuffle ops and apply at each replica
            for replica in &mut replicas {
                let mut ops = ops.clone();
                ops.shuffle(&mut OsRng);

                for op in ops {
                    replica.apply_op(op)?;
                }
            }

            verify_data_convergence(replicas, dataset_length)?;
        }

        #[test]
        fn proptest_converge_with_shuffled_ops_from_many_replicas_across_arbitrary_number_of_replicas(
            dataset in generate_dataset(1000),
            res in generate_replicas(7)
        ) {
            let (mut replicas, owner_keypair) = res?;
            let dataset_length = dataset.len() as u64;

            // generate an ops set using random replica for each data
            let mut ops = vec![];
            let mut children = BTreeSet::new();
            for _data in dataset {
                if let Some(replica) = replicas.choose_mut(&mut OsRng)
                {
                    let (hash, op) = replica.write(random_register_entry(), children)?;
                    let signed_op = sign_register_op(op, &owner_keypair)?;
                    ops.push(signed_op);
                    children = vec![hash].into_iter().collect();
                }
            }

            let opslen = ops.len() as u64;
            prop_assert_eq!(dataset_length, opslen);

            // now we randomly shuffle ops and apply at each replica
            for replica in &mut replicas {
                let mut ops = ops.clone();
                ops.shuffle(&mut OsRng);

                for op in ops {
                    replica.apply_op(op)?;
                }
            }

            verify_data_convergence(replicas, dataset_length)?;
        }

        #[test]
        fn proptest_dropped_data_can_be_reapplied_and_we_converge(
            dataset in generate_dataset_and_probability(1000),
        ) {
            // Instantiate the same Register on two replicas
            let name = XorName::random();
            let tag = 43_000u64;
            let owner_keypair = Keypair::new_ed25519(&mut OsRng);
            let policy = PublicPolicy {
                owner: User::Key(owner_keypair.public_key()),
                permissions: BTreeMap::default(),
            };

            // Instantiate the same Register on two replicas
            let mut replicas = gen_pub_reg_replicas(
                Some(owner_keypair.clone()),
                name,
                tag,
                Some(policy),
                2);
            let (_, mut replica1) = replicas.remove(0);
            let (_, mut replica2) = replicas.remove(0);

            let dataset_length = dataset.len() as u64;

            let mut ops = vec![];
            let mut children = BTreeSet::new();
            for (_data, delivery_chance) in dataset {
                let (hash, op)= replica1.write(random_register_entry(), children)?;
                let signed_op = sign_register_op(op, &owner_keypair)?;

                ops.push((signed_op, delivery_chance));
                children = vec![hash].into_iter().collect();
            }

            for (op, delivery_chance) in ops.clone() {
                if delivery_chance < u8::MAX / 3 {
                    replica2.apply_op(op)?;
                }
            }

            // here we statistically should have dropped some messages
            if dataset_length > 50 {
                assert_ne!(replica2.size(), replica1.size());
            }

            // reapply all ops
            for (op, _) in ops {
                replica2.apply_op(op)?;
            }

            // now we converge
            verify_data_convergence(vec![replica1, replica2], dataset_length)?;
        }

        #[test]
        fn proptest_converge_with_shuffled_ops_from_many_while_dropping_some_at_random(
            dataset in generate_dataset_and_probability(1000),
            res in generate_replicas(7),
        ) {
            let (mut replicas, owner_keypair) = res?;
            let dataset_length = dataset.len() as u64;

            // generate an ops set using random replica for each data
            let mut ops = vec![];
            let mut children = BTreeSet::new();
            for (_data, delivery_chance) in dataset {
                // a random index within the replicas range
                let index: usize = OsRng.gen_range(0, replicas.len());
                let replica = &mut replicas[index];

                let (hash, op)=replica.write(random_register_entry(), children)?;
                let signed_op = sign_register_op(op, &owner_keypair)?;
                ops.push((signed_op, delivery_chance));
                children = vec![hash].into_iter().collect();
            }

            let opslen = ops.len() as u64;
            prop_assert_eq!(dataset_length, opslen);

            // now we randomly shuffle ops and apply at each replica
            for replica in &mut replicas {
                let mut ops = ops.clone();
                ops.shuffle(&mut OsRng);

                for (op, delivery_chance) in ops.clone() {
                    if delivery_chance > u8::MAX / 3 {
                        replica.apply_op(op)?;
                    }
                }

                // reapply all ops, simulating lazy messaging filling in the gaps
                for (op, _) in ops {
                    replica.apply_op(op)?;
                }
            }

            verify_data_convergence(replicas, dataset_length)?;
        }

        #[test]
        fn proptest_converge_with_shuffled_ops_including_bad_ops_which_error_and_are_not_applied(
            dataset in generate_dataset(10),
            bogus_dataset in generate_dataset(10), // should be same number as dataset
            gen_replicas_result in generate_replicas(10),

        ) {
            let (mut replicas, owner_keypair) = gen_replicas_result?;
            let dataset_length = dataset.len();
            let bogus_dataset_length = bogus_dataset.len();
            let number_replicas = replicas.len();

            // generate the real ops set using random replica for each data
            let mut ops = vec![];
            let mut children = BTreeSet::new();
            for _data in dataset {
                if let Some(replica) = replicas.choose_mut(&mut OsRng)
                {
                    let (hash, op)=replica.write(random_register_entry(), children)?;
                    let signed_op = sign_register_op(op, &owner_keypair)?;
                    ops.push(signed_op);
                    children = vec![hash].into_iter().collect();
                }
            }

            // set up a replica that has nothing to do with the rest, random xor... different owner...
            let xorname = XorName::random();
            let tag = 45_000u64;
            let cap = u16::MAX;
            let random_owner_keypair = Keypair::new_ed25519(&mut OsRng);
            let mut bogus_replica = Register::new_public(User::Key(random_owner_keypair.public_key()), xorname, tag, None, cap);

            // add bogus ops from bogus replica + bogus data
            let mut children = BTreeSet::new();
            for _data in bogus_dataset {
                let (hash, op)=bogus_replica.write(random_register_entry(), children)?;
                let bogus_op = sign_register_op(op, &random_owner_keypair)?;
                bogus_replica.apply_op(bogus_op.clone())?;
                ops.push(bogus_op);
                children = vec![hash].into_iter().collect();
            }

            let opslen = ops.len();
            prop_assert_eq!(dataset_length + bogus_dataset_length, opslen);

            let mut err_count = vec![];
            // now we randomly shuffle ops and apply at each replica
            for replica in &mut replicas {
                let mut ops = ops.clone();
                ops.shuffle(&mut OsRng);

                for op in ops {
                    match replica.apply_op(op) {
                        Ok(_) => {},
                        // record all errors to check this matches bogus data
                        Err(error) => {err_count.push(error)},
                    }
                }
            }

            // check we get an error per bogus datum per replica
            assert_eq!(err_count.len(), bogus_dataset_length * number_replicas);

            verify_data_convergence(replicas, dataset_length as u64)?;
        }
    }

    fn random_register_entry() -> Vec<u8> {
        let random_bytes = thread_rng().gen::<[u8; 32]>();
        random_bytes.to_vec()
    }
}
