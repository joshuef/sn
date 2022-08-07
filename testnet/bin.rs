// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! sn_node provides the interface to Safe routing.  The resulting executable is the node
//! for the Safe network.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/maidsafe/QA/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// For explanation of lint checks, run `rustc -W help`.
#![forbid(unsafe_code)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results
)]

use clap::Parser;
use dirs_next::home_dir;
use eyre::{eyre, Result, WrapErr as _};
use sn_launch_tool::Launch;
#[cfg(not(target_os = "windows"))]
use std::process::{Command, Stdio};
use std::{io, path::PathBuf};
use tokio::fs::{create_dir_all, remove_dir_all};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_os = "windows"))]
const SAFE_NODE_EXECUTABLE: &str = "sn_node";

#[cfg(target_os = "windows")]
const SAFE_NODE_EXECUTABLE: &str = "sn_node.exe";

const BASE_TRACING_DIRECTIVES: &str = "testnet=info,sn_launch_tool=debug";
const NODES_DIR: &str = "local-test-network";
const DEFAULT_INTERVAL: &str = "100";
const DEFAULT_NODE_COUNT: u32 = 30;

#[derive(Debug, clap::StructOpt)]
#[clap(name = "testnet", version)]
struct Cmd {
    /// All nodes will be joining existing testnet, none will be started as a genesis node.
    #[clap(long = "add", value_parser)]
    add_nodes_to_existing_network: bool,

    /// Interval in milliseconds between launching each of the nodes.
    #[clap(long = "interval", default_value = DEFAULT_INTERVAL)]
    interval: u64,

    /// Format logs as JSON.
    #[clap(long)]
    json_logs: bool,

    /// Use flamegraph setup.
    /// NB. This requires cargo flamegraph to be installed
    /// NB. This runs nodes as `sudo`, so any files created will
    /// have to be handled as such (ie, `sudo rm -rf ~/.safe/node/local-test-network`)
    #[clap(long)]
    flame: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    init_tracing()?;

    let path = std::path::Path::new("nodes");
    remove_dir_all(&path)
        .await
        .or_else(|error| match error.kind() {
            io::ErrorKind::NotFound => Ok(()),
            _ => Err(error),
        })
        .wrap_err("Failed to remove existing nodes directory")?;
    create_dir_all(&path)
        .await
        .wrap_err("Cannot create nodes directory")?;

    // TODO:: Remove this conditional compilation once the issue on Windows
    // got resoved within the new version of Rust.
    #[cfg(not(target_os = "windows"))]
    // For Windows guys, rember to use
    // `cargo build --release --bins`
    // before executing the testnet.exe.
    {
        let cmd_args = Cmd::from_args();
        let mut build_args = vec!["build", "--release"];

        // Keep features consistent to avoid recompiling when possible
        if cfg!(feature = "chaos") {
            println!("*** Building testnet with CHAOS enabled. Watch out. ***");
            build_args.push("--features");
            build_args.push("chaos");
        }

        // Keep features consistent to avoid recompiling when possible
        if cfg!(feature = "traceroute") {
            println!("*** Building testnet with TRACEROUTE enabled. Watch out for traces. ***");
            build_args.push("--features");
            build_args.push("traceroute");
        }

        if cfg!(feature = "unstable-wiremsg-debuginfo") {
            build_args.push("--features");
            build_args.push("unstable-wiremsg-debuginfo");
        }

        info!("Building current sn_node");
        debug!("Building current sn_node with args: {:?}", build_args);

        if !cmd_args.flame {
            Command::new("cargo")
                .args(build_args.clone())
                .current_dir("sn_node")
                // .env("RUST_LOG", "debug")
                // .env("RUST_BACKTRACE", "1")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output()
                .map_err(Into::into)
                .and_then(|result| {
                    result
                        .status
                        .success()
                        .then(|| ())
                        .ok_or_else(|| eyre!("Command exited with error"))
                })
                .wrap_err_with(|| {
                    format!("Failed to run build command with args: {:?}", build_args)
                })?;
            info!("sn_node built successfully");
        }
    }

    run_network().await?;

    Ok(())
}

/// Uses SNLT to create a local network of nodes
pub async fn run_network() -> Result<()> {
    let args = Cmd::from_args();
    let adding_nodes = args.add_nodes_to_existing_network;

    info!("Starting local network");
    let node_path = Some(PathBuf::from("./target/release"));
    let node_path = get_node_bin_path(node_path)?;

    let arg_node_path = node_path.join(SAFE_NODE_EXECUTABLE).display().to_string();
    debug!("Running node from {}", arg_node_path);

    let base_log_dir = get_node_bin_path(None)?;
    let node_log_dir = base_log_dir.join(NODES_DIR);
    if !node_log_dir.exists() {
        debug!("Creating '{}' folder", node_log_dir.display());
        create_dir_all(node_log_dir.clone())
            .await
            .wrap_err("Couldn't create target path to store nodes' generated data")?;
    }
    let arg_node_log_dir = node_log_dir.display().to_string();
    info!("Storing nodes' generated data at {}", arg_node_log_dir);

    let node_count_str =
        std::env::var("NODE_COUNT").unwrap_or_else(|_| DEFAULT_NODE_COUNT.to_string());

    // Let's create an args array to pass to the network launcher tool
    let interval_str = args.interval.to_string();

    let mut sn_launch_tool_args = vec![
        "sn_launch_tool",
        "--node-path",
        &arg_node_path,
        "--nodes-dir",
        &arg_node_log_dir,
        "--num-nodes",
        &node_count_str,
        "--interval",
        &interval_str,
        "--local",
    ];

    if adding_nodes {
        sn_launch_tool_args.push("--add")
    }

    if args.json_logs {
        sn_launch_tool_args.push("--json-logs");
    }

    if args.flame {
        sn_launch_tool_args.push("--flame");
    }

    // If RUST_LOG was set we pass it down to the launch tool
    // so it's set for each of the nodes logs as well.
    let rust_log =
        std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "sn_node=info".to_string());
    if !rust_log.is_empty() {
        sn_launch_tool_args.push("--rust-log");
        sn_launch_tool_args.push(&rust_log);
    }

    debug!(
        "Running network launch tool with args: {:?}",
        sn_launch_tool_args
    );

    // We can now call the tool with the args
    info!("Launching local Safe network...");
    Launch::from_iter_safe(&sn_launch_tool_args)?.run()?;

    Ok(())
}

fn init_tracing() -> Result<()> {
    let mut filter = EnvFilter::try_new(BASE_TRACING_DIRECTIVES)
        .wrap_err("BUG: hard-coded tracing directives are invalid")?;

    let extra_directives = std::env::var(EnvFilter::DEFAULT_ENV)
        .map_or_else(
            |error| match error {
                std::env::VarError::NotPresent => Ok(None),
                std::env::VarError::NotUnicode(_) => Err(eyre!(error)),
            },
            |filter| Ok(Some(EnvFilter::try_new(filter)?)),
        )
        .wrap_err_with(|| format!("Invalid value for {}", EnvFilter::DEFAULT_ENV))?;

    if let Some(extra_directives) = extra_directives {
        for directive in extra_directives.to_string().split(',') {
            filter = filter.add_directive(
                directive
                    .parse()
                    .expect("BUG: invalid directive in parsed EnvFilter"),
            );
        }
    }

    tracing_subscriber::fmt().with_env_filter(filter).init();

    Ok(())
}

fn get_node_bin_path(node_path: Option<PathBuf>) -> Result<PathBuf> {
    node_path.ok_or(()).or_else(|()| {
        let mut bin_path = home_dir().ok_or_else(|| eyre!("Failed to obtain user's home path"))?;
        bin_path.push(".safe/node");
        Ok(bin_path)
    })
}
