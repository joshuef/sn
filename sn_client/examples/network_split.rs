// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! `sn_node` provides the interface to Safe routing.  The resulting executable is the node
//! for the Safe network.

use sn_client::Client;
use sn_interface::types::utils::random_bytes;
use sn_launch_tool::Launch;

use clap::Parser;
use dirs_next::home_dir;
use eyre::{eyre, Context, Result};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
};
use tiny_keccak::{Hasher, Sha3};
use tokio::fs::create_dir_all;
use tokio::time::{sleep, Duration};
use tracing::{debug, info};
use xor_name::XorName;

#[cfg(not(target_os = "windows"))]
const SAFE_NODE_EXECUTABLE: &str = "sn_node";

#[cfg(target_os = "windows")]
const SAFE_NODE_EXECUTABLE: &str = "sn_node.exe";

const NODES_DIR: &str = "local-test-network";
const INTERVAL: &str = "5000"; // milliseconds
const RUST_LOG: &str = "RUST_LOG";
const ADDITIONAL_NODES_TO_SPLIT: u64 = 15;

#[tokio::main]
async fn main() -> Result<()> {
    // First lets build the network and testnet launcher, to ensure we're on the latest version
    let args: Vec<&str> = vec!["build", "--release", "--features=test-utils"];

    println!("Building current sn_node");
    let _child = Command::new("cargo")
        .args(args.clone())
        // .env("RUST_LOG", "debug")
        // .env("RUST_BACKTRACE", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .map_err(|err| {
            eyre!(
                "Failed to run build command with args '{:?}': {}",
                args,
                err
            )
        })?;

    println!("sn_node bins built successfully");

    run_split().await?;

    Ok(())
}

fn get_node_bin_path(node_path: Option<PathBuf>) -> Result<PathBuf> {
    match node_path {
        Some(p) => Ok(p),
        None => {
            let mut home_dirs =
                home_dir().ok_or_else(|| eyre!("Failed to obtain user's home path"))?;

            home_dirs.push(".safe");
            home_dirs.push("node");
            Ok(home_dirs)
        }
    }
}

/// Uses `sn_launch_tool` to create a local network of nodes
pub async fn run_split() -> Result<()> {
    info!("Starting local network");
    let node_path = Some(PathBuf::from("./target/release"));
    let node_path = get_node_bin_path(node_path)?;

    let arg_node_path = node_path.join(SAFE_NODE_EXECUTABLE).display().to_string();
    debug!("Running node from {}", arg_node_path);

    let base_log_dir = get_node_bin_path(None)?;
    let node_log_dir = base_log_dir.join(NODES_DIR);
    if !node_log_dir.exists() {
        debug!("Creating '{}' folder", node_log_dir.display());
        create_dir_all(node_log_dir.clone()).await.map_err(|err| {
            eyre!(
                "Couldn't create target path to store nodes' generated data: {}",
                err
            )
        })?;
    }
    let arg_node_log_dir = node_log_dir.display().to_string();
    info!("Storing nodes' generated data at {}", arg_node_log_dir);

    // Let's create an args array to pass to the network launcher tool
    let mut sn_launch_tool_args = vec![
        "sn_launch_tool",
        "--node-path",
        &arg_node_path,
        "--nodes-dir",
        &arg_node_log_dir,
        "--interval",
        INTERVAL,
        "--local",
    ];

    // If RUST_LOG was set we pass it down to the launch tool
    // so it's set for each of the nodes logs as well.
    let rust_log = std::env::var(RUST_LOG).unwrap_or_else(|_| "".to_string());
    if !rust_log.is_empty() {
        sn_launch_tool_args.push("--rust-log");
        sn_launch_tool_args.push(&rust_log);
    }

    let interval_as_int = &INTERVAL
        .parse::<u64>()
        .context("Error parsing Interval argument")?;

    debug!("Running testnet with args: {:?}", sn_launch_tool_args);

    // We can now call the tool with the args
    info!("Launching local Safe network...");
    Launch::from_iter_safe(&sn_launch_tool_args)
        .map_err(|error| eyre!(error))
        .and_then(|launch| launch.run())
        .wrap_err("Error starting the testnet")?;

    // leave a longer interval with more nodes to allow for splits if using split amounts
    let interval_duration = Duration::from_millis(2 * interval_as_int);
    sleep(interval_duration).await;
    println!("Done sleeping....");

    let mut all_data_put = vec![];

    let files_to_put: i32 = 40;
    for _i in 0..files_to_put {
        let (address, hash) = upload_data().await?;
        all_data_put.push((address, hash));
    }

    // ======================
    // Now we add more nodes
    // ======================

    let additional_node_count_str = &ADDITIONAL_NODES_TO_SPLIT.to_string();

    sn_launch_tool_args.push("--add");
    sn_launch_tool_args.push("-n");
    sn_launch_tool_args.push(additional_node_count_str);
    debug!("Adding testnet nodes with args: {:?}", sn_launch_tool_args);

    // We can now call the tool with the args
    info!("Adding nodes to the local Safe network...");
    Launch::from_iter_safe(&sn_launch_tool_args)
        .map_err(|error| eyre!(error))
        .and_then(|launch| launch.run())
        .wrap_err("Error adding nodes to the testnet")?;

    // leave a longer interval with more nodes to allow for splits if using split amounts
    let interval_duration = Duration::from_millis(interval_as_int * ADDITIONAL_NODES_TO_SPLIT / 10);
    sleep(interval_duration).await;

    let mut client = Client::builder().build().await?;

    for (address, hash) in all_data_put {
        println!("...reading bytes at address {:?} ...", address);
        let mut bytes = client.read_bytes(address).await;

        let mut attempts = 0;
        while bytes.is_err() && attempts < 10 {
            attempts += 1;
            // do some retries to ensure we're not just timing out by chance
            sleep(Duration::from_secs(attempts)).await;
            bytes = client.read_bytes(address).await;
        }

        let bytes = bytes?;
        println!("Bytes read from {:?}:", address);

        let mut hasher = Sha3::v256();
        let mut output = [0; 32];
        hasher.update(&bytes);
        hasher.finalize(&mut output);

        assert_eq!(output, hash);
    }

    println!("All okay");

    Ok(())
}

async fn upload_data() -> Result<(XorName, [u8; 32])> {
    let mut client = Client::builder().build().await?;

    let bytes = random_bytes(1024 * 1024 * 10);

    let mut hasher = Sha3::v256();
    let mut output = [0; 32];
    hasher.update(&bytes);
    hasher.finalize(&mut output);

    println!("Storing bytes w/ hash {:?}", output);

    let address = client.upload(bytes).await?;
    println!("Bytes stored at address: {:?}", address);

    let delay = 2;
    println!("Reading bytes from the network in {} secs...", delay);
    sleep(Duration::from_secs(delay)).await;

    println!("...reading bytes from the network now...");
    let mut bytes = client.read_bytes(address).await;

    let mut attempts = 0;
    while bytes.is_err() && attempts < 10 {
        attempts += 1;
        // do some retries to ensure we're not just timing out by chance
        sleep(Duration::from_secs(attempts)).await;
        bytes = client.read_bytes(address).await;
    }

    let _bytes = bytes?;

    println!("Bytes successfully read from {:?}:", address);

    Ok((address, output))
}
