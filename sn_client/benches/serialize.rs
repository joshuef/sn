// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use eyre::Result;
use rand::{rngs::OsRng, Rng};
use rayon::current_num_threads;
use sn_client::{Client, Error};
use tokio::runtime::Runtime;
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
use std::collections::BTreeMap;

fn public_policy(owner: User) -> Policy {
    let permissions = BTreeMap::new();
    Policy { owner, permissions }
}

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

/// Grows a seed vector into a Bytes with specified length.
fn grows_vec_to_bytes(seed: &[u8], length: usize) -> Bytes {
    let mut seed = BytesMut::from(seed);
    let mut rng = OsRng;
    seed[0] = rng.gen::<u8>();
    let iterations = length / seed.len();
    let remainder = length % seed.len();

    let mut bytes = BytesMut::new();

    for _ in 0..iterations {
        bytes.extend(seed.clone());
    }

    bytes.extend(vec![0u8; remainder]);

    Bytes::from(bytes)
}

async fn create_client() -> Result<Client, Error> {
    let client = Client::builder().build().await?;

    Ok(client)
}

/// This bench requires a network already set up
async fn upload_and_read_bytes(client: &Client, bytes: Bytes) -> Result<(), Error> {
    let address = client.upload(bytes.clone()).await?;

    // let's make sure the public chunk is stored
    let received_bytes = client.read_bytes(address).await?;

    assert_eq!(received_bytes, bytes);

    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");
    // measure over around 9s in general
    // group.measurement_time(std::time::Duration::from_secs(90));
    // group.sampling_mode(SamplingMode::Flat);
    let runtime = Runtime::new().unwrap();
    // group.sample_size(10);

    let client = match runtime.block_on(create_client()) {
        Ok(client) => client,
        Err(err) => {
            println!("Failed to create client with {:?}", err);
            return;
        }
    };
    let seed = random_vector(1024);

    // upload and read
    group.bench_with_input(
        "serial",
        &(&seed, &client),
        |b, (seed, client)| {
            b.to_async(&runtime).iter(|| async {
                // let bytes = grows_vec_to_bytes(seed, 3072);

                let name = xor_name::rand::random();
                let tag = 15000;
                let owner = User::Key(client.public_key());
                let policy = public_policy(owner);

                let (address, mut batch) = futures::executor::block_on(client.create_register(name, tag, policy))?;
                for cmd in batch {

                    let msg = ServiceMsg::Cmd(cmd.clone());
                    if let Err(err) = WireMsg::serialize_msg_payload(&msg) {
                        panic!("error serialising payload");
                    };


                    // let _ = storage
                    //     .clone()
                    //     .store(&data_set[i], pk, keypair.clone())
                    //     .await;

                }

                Ok::<(), Box<dyn std::error::Error>>(())
                // match upload_and_read_bytes(client, bytes).await {
                //     Ok(_) => {}
                //     Err(error) => println!("3072b upload and read bench failed with {:?}", error),
                // }
            });
        },
    );
    // group.bench_with_input(
    //     "upload and read 1mb",
    //     &(&seed, &client),
    //     |b, (seed, client)| {
    //         b.to_async(&runtime).iter(|| async {
    //             let bytes = grows_vec_to_bytes(seed, 1024 * 1024);
    //             match upload_and_read_bytes(client, bytes).await {
    //                 Ok(_) => {}
    //                 Err(error) => println!("1mb upload and read bench failed with {:?}", error),
    //             }
    //         });
    //     },
    // );
    // group.bench_with_input(
    //     "upload and read 10mb",
    //     &(&seed, &client),
    //     |b, (seed, client)| {
    //         b.to_async(&runtime).iter(|| async {
    //             let bytes = grows_vec_to_bytes(seed, 1024 * 1024 * 10);
    //             match upload_and_read_bytes(client, bytes).await {
    //                 Ok(_) => {}
    //                 Err(error) => println!("10mb upload and read bench failed with {:?}", error),
    //             }
    //         });
    //     },
    // );

    // // only upload
    // group.bench_with_input("upload 3072b", &(&seed, &client), |b, (seed, client)| {
    //     b.to_async(&runtime).iter(|| async {
    //         let bytes = grows_vec_to_bytes(seed, 3072);
    //         match client.upload(bytes).await {
    //             Ok(_) => {}
    //             Err(error) => println!("3072b upload bench failed with {:?}", error),
    //         }
    //     });
    // });
    // group.bench_with_input("upload 1mb", &(&seed, &client), |b, (seed, client)| {
    //     b.to_async(&runtime).iter(|| async {
    //         let bytes = grows_vec_to_bytes(seed, 1024 * 1024);
    //         match client.upload(bytes).await {
    //             Ok(_) => {}
    //             Err(error) => println!("1mb upload bench failed with {:?}", error),
    //         }
    //     });
    // });
    // group.bench_with_input("upload 10mb", &(&seed, &client), |b, (seed, client)| {
    //     b.to_async(&runtime).iter(|| async {
    //         let bytes = grows_vec_to_bytes(seed, 1024 * 1024 * 10);
    //         match client.upload(bytes).await {
    //             Ok(_) => {}
    //             Err(error) => println!("10mb upload bench failed with {:?}", error),
    //         }
    //     });
    // });
    group.finish()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
