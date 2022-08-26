// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion::{BatchSize, BenchmarkId, Throughput};

use eyre::Result;
use rand::{rngs::OsRng, Rng};
use rayon::current_num_threads;
use sn_client::{Client, Error};
use tokio::runtime::Runtime;
use sn_interface::{
    messaging::{ MsgId,
        AuthKind, Dst, ServiceAuth,
        data::{CreateRegister, ServiceMsg, SignedRegisterCreate, DataCmd}
        , WireMsg,
    },
    types::{
        register::{Policy, User},
        Chunk, Keypair, PublicKey, RegisterCmd, ReplicatedData,
    },
};
use xor_name::XorName;
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
    let runtime = Runtime::new().unwrap();

    let client = match runtime.block_on(create_client()) {
        Ok(client) => client,
        Err(err) => {
            println!("Failed to create client with {:?}", err);
            return;
        }
    };

    let msg = runtime.block_on(async{
        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(client.public_key());
        let policy = public_policy(owner);

        let (address, mut batch) = match client.create_register(name, tag, policy).await {
            Ok(x) => x,
            Err(error) => panic!("error creating register {error:?}")
        };

        let client_pk = client.public_key();

        // batch[0].clone()
        // let payload = ServiceMsg::Cmd(batch[0].clone());

        let msg_id = MsgId::new();

        let payload = {
            let msg = ServiceMsg::Cmd(batch[0].clone());
            WireMsg::serialize_msg_payload(&msg)?
        };

        let auth = ServiceAuth {
            public_key: client_pk,
            signature : client.sign(&payload)
        };


        // let elders_len = elders.len();

        // debug!(
        //     "Sending cmd w/id {msg_id:?}, from {}, to {elders_len} Elders w/ dst: {dst_address:?}",
        //     endpoint.public_addr(),
        // );

        // let signature = client.sign(&serialised_cmd);

        // Dst at thiss point is nonsense
        // it has no bearing on serialisation speed
        let dst = Dst {
            name: xor_name::rand::random(),
            section_key: bls::SecretKey::random().public_key(),
        };

        let auth = AuthKind::Service(auth);

        #[allow(unused_mut)]
        let mut wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);

        wire_msg

    }) ;


    let seed = random_vector(1024);

    group.throughput(Throughput::Bytes(std::mem::size_of_val(&msg) as u64));

    // upload and read
    group.bench_with_input(
        "serial",
        &(&msg, &client),
        |b, (msg, client)| {
            b.to_async(&runtime).iter(|| async {


                    if let Err(err) = msg.serialize() {
                        panic!("error serialising payload");
                    };


                Ok::<(), Box<dyn std::error::Error>>(())

            });
        },
    );

    group.finish()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
