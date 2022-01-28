// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use criterion::{criterion_group, criterion_main, Criterion};
use eyre::Result;
use safe_network::types::Scope;
use safe_network::{
    client::{
        utils::test_utils::{read_network_conn_info, run_w_backoff_delayed},
        Client, ClientConfig, Error,
    },
    types::utils::random_bytes,
};
use tokio::runtime::Runtime;

/// This bench requires a network already set up
async fn upload_bytes(size: usize) -> Result<(), Error> {
    let (genesis_key, bootstrap_nodes) = read_network_conn_info().unwrap();
    let bytes = random_bytes(size);
    let config = ClientConfig::new(None, None, genesis_key, None, None, None, None).await;
    let client = Client::new(config, bootstrap_nodes, None).await?;
    let address = client.upload(bytes.clone(), Scope::Public).await?;

    // the larger the file, the longer we have to wait before we start querying
    let delay = usize::max(1, size / 2_000_000);

    // let's make sure the public chunk is stored
    let received_bytes = run_w_backoff_delayed(
        || async { Ok(client.read_bytes(address).await?) },
        10,
        delay,
    )
    .await?;

    assert_eq!(received_bytes, bytes);

    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("upload-sampling");

    let runtime = Runtime::new().unwrap();
    group.sample_size(10);
    group.bench_function("upload 3072b", |b| {
        b.to_async(&runtime).iter(|| async {
            match upload_bytes(3072).await {
                Ok(_) => {}
                Err(error) => println!("bench failed with {:?}", error),
            }
        });
    });
    group.bench_function("upload 1mb", |b| {
        b.to_async(&runtime).iter(|| async {
            match upload_bytes(1024 * 1024).await {
                Ok(_) => {}
                Err(error) => println!("bench failed with {:?}", error),
            }
        });
    });
    group.finish()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
