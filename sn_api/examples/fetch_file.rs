// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bytes::Buf;
use color_eyre::{eyre::eyre, Result};
use sn_api::{resolver::SafeData, Safe};
use std::{env::args, net::SocketAddr};

// To be executed passing Safe network contact address and file Safe URL, e.g.:
// $ cargo run --release --example fetch_file 127.0.0.1:12000 safe://hy8oyeyqhd1e8keggcjyb9zjyje1m7ihod1pyru6h5y6jkmmihdnym4ngdf
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Skip executable name form args
    let mut args_received = args();
    args_received.next();

    // Read the network contact socket address from first arg passed
    let network_contact = args_received
        .next()
        .ok_or_else(|| eyre!("No Safe network contact socket address provided"))?;
    let network_addr: SocketAddr = network_contact
        .parse()
        .map_err(|err| eyre!("Invalid Safe network contact socket address: {}", err))?;
    println!("Safe network to be contacted at {}", network_addr);

    // Read URL from second argument passed
    let url = args_received
        .next()
        .ok_or_else(|| eyre!("No Safe URL provided as argument"))?;
    println!("Fetching file from Safe with URL: {}", url);

    // The Safe instance is what will give us access to the network API.
    let mut safe = Safe::connected(None, None, None, None).await?;

    println!("Connected to Safe!");

    // Now we can simply fetch the file using `fetch` API,
    // it will return not only the content of the file
    // but its metadata too, so we can distinguish what has
    // been fetched from the provided Safe-URL.
    match safe.fetch(&url, None).await {
        Ok(SafeData::PublicFile { data, .. }) => {
            let data = String::from_utf8(data.chunk().to_vec())?;
            println!("File content retrieved:\n{}", data);
        }
        Ok(other) => println!("Failed to retrieve file, instead obtained: {:?}", other),
        Err(err) => println!("Failed to retrieve file: {:?}", err),
    }

    Ok(())
}
