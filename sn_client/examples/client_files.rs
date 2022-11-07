// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use eyre::Result;
use sn_client::Client;
use sn_interface::types::utils::random_bytes;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut client = Client::builder().build().await?;

    let pk = client.public_key();
    println!("Client Public Key: {}", pk);

    let bytes = random_bytes(self_encryption::MIN_ENCRYPTABLE_BYTES);
    println!("Storing {} bytes..", bytes.len());

    let address = client.upload(bytes).await?;
    println!("Bytes stored at address: {:?}", address);

    let delay = 5;
    println!("Reading bytes from the network in {} secs...", delay);
    sleep(Duration::from_secs(delay)).await;

    println!("...reading bytes from the network now...");
    let _bytes = client.read_bytes(address).await?;
    println!("Bytes read from {:?}", address);

    println!();

    Ok(())
}
