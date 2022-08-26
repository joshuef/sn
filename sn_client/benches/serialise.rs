// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::{
    messaging::{
        data::{CreateRegister, ServiceMsg, SignedRegisterCreate}
        , WireMsg,
    },
    types::{
        register::{Policy, User},
        Chunk, Keypair, PublicKey, RegisterCmd, ReplicatedData,
    },
};
// use sn_node::{
//     node::{cfg::config_handler::Config, DataStorage},
//     UsedSpace,
// };

use sn_client::Client;

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion};
use eyre::{Result, WrapErr};
use rand::{distributions::Alphanumeric, rngs::OsRng, thread_rng, Rng};
use rayon::current_num_threads;
use std::{collections::BTreeMap, path::Path};
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

/// We only testing with 4000 different data inputs.
/// Making the first 4 bytes random shall be enough.
fn grows_vec_to_bytes(seed: &[u8]) -> Bytes {
    let mut bytes = BytesMut::from(seed);
    let mut rng = OsRng;
    bytes[0] = rng.gen::<u8>();
    bytes[1] = rng.gen::<u8>();
    bytes[2] = rng.gen::<u8>();
    bytes[3] = rng.gen::<u8>();

    Bytes::from(bytes)
}

fn main() -> Result<()> {
    let mut criterion = custom_criterion();

    bench_wire_msg_to_many(&mut criterion)?;
    // bench_data_storage_reads(&mut criterion)?;

    Ok(())
}

fn bench_wire_msg_to_many(c: &mut Criterion) -> Result<()> {
    let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
    let keypair = Keypair::new_ed25519();

    let mut group = c.benchmark_group("write-sampling");

    let runtime = Runtime::new().unwrap();
    pub const NONSENSE_CHUNK_SIZE: usize = 1024; // data size should not be important for keys() tests

    let size_ranges = [100];

    // for size in size_ranges.iter() {
        // let data_set: Vec<_> = (0..*size)
        //     .map(|_| create_random_register_replicated_data())
        //     .collect();


        let client = futures::executor::block_on(async {
            Client::builder()
            // .keypair(optional_keypair)
            // .dbc_owner(dbc_owner)
            // .query_timeout(timeout)
            // .cmd_timeout(timeout)
            .build()
            .await
        })?;
        // let one_sec = tokio::time::Duration::from_secs(1);
        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());
        let policy = public_policy(owner);

        let (address, mut batch) = futures::executor::block_on(client.create_register(name, tag, policy))?;

        let size = 100;
        // group.bench_function(
        group.bench_with_input(
            BenchmarkId::new("msg_serialize_to_many", size),
            &(size, &batch),
            |b, (size, batch)| {
                // let storage = get_new_data_store()
                //     .context("Could not create a temp data store")
                //     .unwrap();
                b.to_async(&runtime).iter(|| async {
                    // let wire_msg =
                    for cmd in *batch {

                        let msg = ServiceMsg::Cmd(cmd.clone());
                        if let Err(err) = WireMsg::serialize_msg_payload(&msg) {
                            panic!("error serialising payload")
                        };


                        // Ok::<(), Box<dyn std::error::Error>>(())
                        // let _ = storage
                        //     .clone()
                        //     .store(&data_set[i], pk, keypair.clone())
                        //     .await;

                    }
                })
            },
        );
    // }

    // for size in size_ranges.iter() {
    //     let seed = random_vector(NONSENSE_CHUNK_SIZE);
    //     group.bench_with_input(
    //         BenchmarkId::new("chunk writes", size),
    //         &(size, &seed),
    //         |b, (size, seed)| {
    //             let storage = get_new_data_store()
    //                 .context("Could not create a temp data store")
    //                 .unwrap();
    //             b.to_async(&runtime).iter(|| async {
    //                 for _ in 0..**size {
    //                     let random_data =
    //                         ReplicatedData::Chunk(Chunk::new(grows_vec_to_bytes(seed)));
    //                     storage
    //                         .clone()
    //                         .store(&random_data, pk, keypair.clone())
    //                         .await
    //                         .expect("failed to write chunk {i}");
    //                 }
    //             })
    //         },
    //     );
    // }

    Ok(())
}


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

fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(SAMPLE_SIZE)
}

fn public_policy(owner: User) -> Policy {
    let permissions = BTreeMap::new();
    Policy { owner, permissions }
}

pub fn create_random_register_replicated_data() -> ReplicatedData {
    let keypair = Keypair::new_ed25519();

    let name = xor_name::rand::random();
    let tag = 15000;
    let owner = User::Key(keypair.public_key());
    let policy = public_policy(owner);

    let op = CreateRegister { name, tag, policy };
    let signature = keypair.sign(&bincode::serialize(&op).expect("could not serialize op"));
    let reg_cmd = RegisterCmd::Create {
        cmd: SignedRegisterCreate {
            op,
            auth: sn_interface::messaging::ServiceAuth {
                public_key: keypair.public_key(),
                signature,
            },
        },
        section_auth: section_auth(), // obtained after presenting a valid payment to the network
    };


    ReplicatedData::RegisterWrite(reg_cmd)
}

// fn get_new_data_store() -> Result<DataStorage> {
//     let random_filename: String = thread_rng()
//         .sample_iter(&Alphanumeric)
//         .take(7)
//         .map(char::from)
//         .collect();

//     let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
//     let storage_dir = Path::new(root_dir.path()).join(random_filename);
//     let config = Config::default();
//     let max_capacity = config.max_capacity();

//     let used_space = UsedSpace::new(max_capacity);
//     let store = DataStorage::new(&storage_dir, used_space)?;

//     Ok(store)
// }
