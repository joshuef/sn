// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::{
    messaging::data::{CreateRegister, EditRegister, SignedRegisterCreate, SignedRegisterEdit},
    test_utils::TestKeys,
    types::{
        register::{Policy, Register, User},
        Chunk, Keypair, PublicKey, RegisterCmd, ReplicatedData, SectionSig,
    },
};
use sn_node::{
    node::{cfg::config_handler::Config, DataStorage},
    UsedSpace,
};

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion};
use eyre::{Result, WrapErr};
use rand::{distributions::Alphanumeric, rngs::OsRng, thread_rng, Rng};
use rayon::current_num_threads;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use tempfile::tempdir;
use tokio::runtime::Runtime;

// sample size is _NOT_ the number of times the command is run...
// https://bheisler.github.io/criterion.rs/book/analysis.html#measurement
const SAMPLE_SIZE: usize = 10;

/// Generates a random vector using provided `length`.
fn random_vector(length: usize) -> Vec<u8> {
    use rayon::prelude::*;
    let threads = current_num_threads();

    if threads > length {
        let mut rng = OsRng;
        return ::std::iter::repeat(())
            .map(|()| rng.gen::<u8>())
            .take(length)
            .collect();
    }

    let per_thread = length / threads;
    let remainder = length % threads;

    let mut bytes: Vec<u8> = (0..threads)
        .par_bridge()
        .map(|_| vec![0u8; per_thread])
        .map(|mut bytes| {
            let bytes = bytes.as_mut_slice();
            rand::thread_rng().fill(bytes);
            bytes.to_owned()
        })
        .flatten()
        .collect();

    bytes.extend(vec![0u8; remainder]);

    bytes
}

/// During bench testing, each iteration need 4000 different data inputs.
/// And it will be around 110 iterations in total to be undertaken.
/// Given the changing bytes will give a range of:
///     range = 2^(8*bytes_changing)
/// The non-collision success rate will be:
///     (1-1/range)*(1-2/range)*...*(1-num_of_different_data/range)
/// Which, when having 4 changing bytes,
/// for 4000 different data inputs, the rate will be 99.8%
/// but after 10 bench iterations, drops to 83%,
/// then being alost 0% after 110 iterations.
/// Hence, choosing 8 bytes need to keeps changing.
fn grows_vec_to_bytes(seed: &[u8]) -> Bytes {
    let mut bytes = BytesMut::from(seed);
    let mut rng = OsRng;
    bytes.iter_mut().take(8).for_each(|b| *b = rng.gen::<u8>());

    bytes.freeze()
}

fn main() -> Result<()> {
    let mut criterion = custom_criterion();

    bench_data_storage_register_edits(&mut criterion)?;
    bench_data_storage_writes(&mut criterion)?;
    bench_data_storage_reads(&mut criterion)?;

    Ok(())
}

fn bench_data_storage_writes(c: &mut Criterion) -> Result<()> {
    let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
    let keypair = Keypair::new_ed25519();

    let mut group = c.benchmark_group("write-sampling");

    let runtime = Runtime::new().unwrap();
    pub const NONSENSE_CHUNK_SIZE: usize = 1024; // data size should not be important for keys() tests

    let size_ranges = [100, 1_000, 4_000];

    for size in &size_ranges {
        let data_set: Vec<_> = (0..*size)
            .map(|_| create_random_register_replicated_data())
            .collect();
        group.bench_with_input(
            BenchmarkId::new("register_writes", size),
            &(size, &data_set),
            |b, (size, data_set)| {
                let storage = get_new_data_store()
                    .context("Could not create a temp data store")
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    for i in 0..**size {
                        let _ = storage
                            .clone()
                            .store(&data_set[i], pk, keypair.clone())
                            .await;
                    }
                })
            },
        );
    }

    for size in &size_ranges {
        let seed = random_vector(NONSENSE_CHUNK_SIZE);
        group.bench_with_input(
            BenchmarkId::new("chunk writes", size),
            &(size, &seed),
            |b, (size, seed)| {
                let storage = get_new_data_store()
                    .context("Could not create a temp data store")
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    for _ in 0..**size {
                        let random_data =
                            ReplicatedData::Chunk(Chunk::new(grows_vec_to_bytes(seed)));
                        storage
                            .clone()
                            .store(&random_data, pk, keypair.clone())
                            .await
                            .expect("failed to write chunk {i}");
                    }
                })
            },
        );
    }

    Ok(())
}

fn bench_data_storage_register_edits(c: &mut Criterion) -> Result<()> {
    let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
    let mut group = c.benchmark_group("register-edit-sampling");

    let runtime = Runtime::new().unwrap();

    let size_ranges = [1000];

    for size in &size_ranges {
        let (keypair, op) = create_random_register_register_op();
        let address = op.address();
        // the actual register we'll be editing
        let mut register = Register::new(
            op.owner(),
            *address.name(),
            address.tag(),
            op.policy.clone(),
        );

        let data_set: Vec<_> = (0..*size)
            .map(|_| create_random_register_replicated_data_edit(&keypair, &mut register))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("register_edits", size),
            &(size, &data_set),
            |b, (size, data_set)| {
                let storage = get_new_data_store()
                    .context("Could not create a temp data store")
                    .unwrap();
                let signature =
                    keypair.sign(&bincode::serialize(&op.clone()).expect("could not serialize op"));

                let reg_cmd = RegisterCmd::Create {
                    cmd: SignedRegisterCreate {
                        op: op.clone(),
                        auth: sn_interface::messaging::ClientAuth {
                            public_key: keypair.public_key(),
                            signature,
                        },
                    },
                    section_sig: section_sig(), // obtained after presenting a valid payment to the network
                };

                let first_write = ReplicatedData::RegisterWrite(reg_cmd);
                runtime
                    .block_on(storage.store(&first_write, pk, keypair.clone()))
                    .expect("Could not store initial register");

                b.to_async(&runtime).iter(|| async {
                    for i in 0..**size {
                        storage
                            .clone()
                            .store(&data_set[i], pk, keypair.clone())
                            .await
                            .expect("failed to write data storage edit");
                    }
                })
            },
        );
    }

    Ok(())
}

fn bench_data_storage_reads(c: &mut Criterion) -> Result<()> {
    let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
    let keypair = Keypair::new_ed25519();

    let mut group = c.benchmark_group("read-sampling");

    let runtime = Runtime::new().unwrap();
    pub const NONSENSE_CHUNK_SIZE: usize = 1024; // data size should not be important for keys() tests

    let size_ranges = [100, 1_000, 4_000];

    for size in &size_ranges {
        group.bench_with_input(BenchmarkId::new("register_keys", size), size, |b, &size| {
            let storage = get_new_data_store()
                .context("Could not create a temp data store")
                .unwrap();

            for _ in 0..size {
                let random_data = create_random_register_replicated_data();

                if let Err(error) = runtime
                    .block_on(storage.clone().store(&random_data, pk, keypair.clone()))
                    .context("could not store register")
                {
                    panic!("Error storing register {random_data:?}: {error:?}");
                }
            }

            b.iter(|| {
                let _keys = runtime.block_on(storage.data_addrs());
            })
        });
    }

    for size in &size_ranges {
        group.bench_with_input(BenchmarkId::new("chunk keys", size), size, |b, &size| {
            let storage = get_new_data_store()
                .context("Could not create a temp data store")
                .unwrap();

            for _ in 0..size {
                let file = sn_interface::types::utils::random_bytes(NONSENSE_CHUNK_SIZE);
                let random_data = ReplicatedData::Chunk(Chunk::new(file));
                if let Err(error) = runtime
                    .block_on(storage.store(&random_data, pk, keypair.clone()))
                    .context("could not store chunk")
                {
                    panic!("Error storing chunk {error:?}");
                };
            }

            b.iter(|| {
                let _keys = runtime.block_on(storage.data_addrs());
            })
        });
    }

    Ok(())
}

fn section_sig() -> SectionSig {
    let sk = bls::SecretKey::random();
    TestKeys::get_section_sig_bytes(&sk, "hello".as_bytes())
}

fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(SAMPLE_SIZE)
}

fn public_policy(owner: User) -> Policy {
    let permissions = BTreeMap::new();
    Policy { owner, permissions }
}

pub fn create_random_register_register_op() -> (Keypair, CreateRegister) {
    let keypair = Keypair::new_ed25519();

    let name = xor_name::rand::random();
    let tag = 15000;
    let owner = User::Key(keypair.public_key());
    let policy = public_policy(owner);

    (keypair, CreateRegister { name, tag, policy })
}

/// create random edits to an existing register
pub fn create_random_register_replicated_data_edit(
    keypair: &Keypair,
    register: &mut Register,
) -> ReplicatedData {
    let random_content = rand::thread_rng().gen::<[u8; 32]>().to_vec();

    let (_hash, edit) = register
        .write(random_content, BTreeSet::default())
        .expect("could not write edit to register");

    let edit_register_op = EditRegister {
        address: *register.address(),
        edit,
    };
    let signature =
        keypair.sign(&bincode::serialize(&edit_register_op).expect("could not serialize op"));

    let auth = sn_interface::messaging::ClientAuth {
        public_key: keypair.public_key(),
        signature,
    };

    let reg_cmd = RegisterCmd::Edit(SignedRegisterEdit {
        op: edit_register_op,
        auth,
    });

    ReplicatedData::RegisterWrite(reg_cmd)
}

pub fn create_random_register_replicated_data() -> ReplicatedData {
    let (keypair, op) = create_random_register_register_op();
    let signature = keypair.sign(&bincode::serialize(&op).expect("could not serialize op"));
    let reg_cmd = RegisterCmd::Create {
        cmd: SignedRegisterCreate {
            op,
            auth: sn_interface::messaging::ClientAuth {
                public_key: keypair.public_key(),
                signature,
            },
        },
        section_sig: section_sig(), // obtained after presenting a valid payment to the network
    };

    ReplicatedData::RegisterWrite(reg_cmd)
}

fn get_new_data_store() -> Result<DataStorage> {
    let random_filename: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();

    let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
    let storage_dir = Path::new(root_dir.path()).join(random_filename);

    let config = Config::default();
    let max_capacity = config.max_capacity();
    let min_capacity = config.min_capacity();

    let used_space = UsedSpace::new(max_capacity, min_capacity);
    let store = DataStorage::new(&storage_dir, used_space)?;

    Ok(store)
}
