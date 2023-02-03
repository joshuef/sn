// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! sn_node provides the interface to Safe routing.  The resulting executable is the node
//! for the Safe network.
// boop
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
    unused_results,
    clippy::unwrap_used
)]

use sn_node::node::{start_new_node, Config, Error as NodeError, RejoinReason};

use clap::{CommandFactory, Parser};
use clap_complete::{generate, Shell};
use color_eyre::{Section, SectionExt};
use eyre::{eyre, ErrReport, Result};
use self_update::{cargo_crate_version, Status};
use std::{io::Write, process::exit};
use tokio::runtime::Runtime;
use tokio::time::Duration;
use tracing::{self, error, info, warn};

const JOIN_RETRY_TIME_SEC: u64 = 25;
const JOIN_DISALLOWED_RETRY_TIME_SEC: u64 = 60;

mod log;

fn main() -> Result<()> {
    color_eyre::install()?;

    let mut config = futures::executor::block_on(Config::new())?;

    #[cfg(not(feature = "otlp"))]
    let _log_guard = log::init_node_logging(&config)?;
    #[cfg(feature = "otlp")]
    let (_rt, _guard) = {
        // init logging in a separate runtime if we are sending traces to an opentelemetry server
        let rt = Runtime::new()?;
        let guard = rt.block_on(async { log::init_node_logging(&config) })?;
        (rt, guard)
    };

    {
        use parking_lot::deadlock;
        use std::thread;

        // Create a background thread which checks for deadlocks every 10s
        let _handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(3));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            println!("{} deadlocks detected", deadlocks.len());
            warn!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                println!("Deadlock #{i}");
                warn!("Deadlock #{i}");
                for t in threads {
                    println!("Thread Id {:#?}", t.thread_id());
                    warn!("Thread Id {:#?}", t.thread_id());
                    println!("{:#?}", t.backtrace());
                    warn!("{:#?}", t.backtrace());
                }
            }
        });
    }

    loop {
        println!("Node started");
        create_runtime_and_node(&config)?;

        // if we've had an issue, lets put the brakes on any crazy looping here
        std::thread::sleep(Duration::from_secs(1));

        // pull config again in case it has been updated meanwhile
        config = futures::executor::block_on(Config::new())?;
    }
}

/// Create a tokio runtime per `start_node` attempt.
/// This ensures any spawned tasks are closed before this would
/// be run again.
fn create_runtime_and_node(config: &Config) -> Result<()> {
    if let Some(c) = &config.completions() {
        let shell = c.parse().map_err(|err: String| eyre!(err))?;
        let buf = gen_completions_for_shell(shell, Config::command()).map_err(|err| eyre!(err))?;
        std::io::stdout().write_all(&buf)?;

        return Ok(());
    }

    if config.update() || config.update_only() {
        match update() {
            Ok(status) => {
                if let Status::Updated { .. } = status {
                    println!("Node has been updated. Please restart.");
                    exit(0);
                }
            }
            Err(e) => println!("Updating node failed: {e:?}"),
        }

        if config.update_only() {
            exit(0);
        }
    }

    let our_pid = std::process::id();
    let join_timeout = Duration::from_secs(JOIN_RETRY_TIME_SEC);
    let mut join_retry_sec = JOIN_DISALLOWED_RETRY_TIME_SEC;
    let log_path = if let Some(path) = config.log_dir() {
        format!("{}", path.display())
    } else {
        "unknown".to_string()
    };

    loop {
        // make a fresh runtime
        let rt = Runtime::new()?;

        let message = format!(
            "Running {} v{}. PID: {our_pid}",
            Config::clap().get_name(),
            env!("CARGO_PKG_VERSION")
        );

        info!("\n{}\n{}", message, "=".repeat(message.len()));

        let outcome = rt.block_on(async {
            info!("Initial node config: {config:?}");
            Ok::<_, ErrReport>(start_new_node(config, join_timeout).await)
        })?;

        match outcome {
            Ok((_node, mut rejoin_network_rx)) => {
                let join_future = async {
                    // Simulate failed node starts, and ensure that
                    #[cfg(feature = "chaos")]
                    {
                        use rand::Rng;
                        let mut rng = rand::thread_rng();
                        let x: f64 = rng.gen_range(0.0..1.0);

                        if !config.is_first() && x > 0.6 {
                            println!(
                               "\n =========== [Chaos] (PID: {our_pid}): Startup chaos crash w/ x of: {x}. ============== \n",
                           );

                            // tiny sleep so testnet doesn't detect a faulty node and exit
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            warn!("[Chaos] (PID: {our_pid}): ChaoticStartupCrash");
                            return Err(NodeError::ChaoticStartupCrash);
                        }
                    }

                    // this keeps node running
                    if let Some(reason) = rejoin_network_rx.recv().await {
                        error!("{reason:?}");
                        return Err(NodeError::RejoinRequired(reason));
                    }
                    Ok(())
                };

                if let Err(error) = rt.block_on(join_future) {
                    // actively shut down the runtime
                    rt.shutdown_timeout(Duration::from_secs(2));

                    // Here are both recoverable errors, (where anything we can just
                    // restart to handle, we do so here), and irrecoverable ones, where we do not restart.
                    use RejoinReason::*;
                    match error {
                        NodeError::RejoinRequired(RemovedFromSection) => {
                            continue;
                        }
                        NodeError::RejoinRequired(JoinsDisallowed) => {
                            let message = format!(
                                "The network is not accepting nodes right now. \
                                Retrying after {JOIN_DISALLOWED_RETRY_TIME_SEC} seconds."
                            );
                            println!("{message} Node log path: {log_path}");
                            info!("{message}");
                            join_retry_sec = JOIN_DISALLOWED_RETRY_TIME_SEC;
                            continue;
                        }
                        NodeError::RejoinRequired(NodeNotReachable(addr)) => {
                            let err = Err(NodeError::RejoinRequired(NodeNotReachable(addr))).suggestion(
                                "Unfortunately we are unable to establish a connection to your machine through its \
                                public IP address. This might involve forwarding ports on your router."
                                    .header("Please ensure your node is externally reachable")
                            );
                            println!("{err:?}");
                            error!("{err:?}");
                            return err;
                        }
                        #[cfg(feature = "chaos")]
                        NodeError::ChaoticStartupCrash => {
                            continue;
                        }
                        _ => {
                            error!("Irrecoverable error, closing node program: {error:?}");
                            return Err(error).map_err(ErrReport::msg);
                        }
                    }
                }
            }
            Err(error) => {
                let err = Err(error)
                    .suggestion(format!("Cannot start node. Node log path: {log_path}").header(
                    "If this is the first node on the network pass the local address to be used using --first",
                ));
                error!("{err:?}");
                // actively shut down the runtime
                rt.shutdown_timeout(Duration::from_secs(2));
                return err;
            }
        }

        // actively shut down the runtime
        rt.shutdown_timeout(Duration::from_secs(2));
        // The sleep shall only need to be carried out when being asked to join later?
        // For the case of a timed_out, a retry can be carried out immediately?
        // OTOH: A timeout might indicate heavy load or some sync problems. Probably wise to stay a bit loose on the gas pedal then.
        std::thread::sleep(Duration::from_secs(join_retry_sec));
    }
}

fn update() -> Result<Status, Box<dyn (::std::error::Error)>> {
    info!("Checking for updates...");
    let target = self_update::get_target();

    let releases = self_update::backends::github::ReleaseList::configure()
        .repo_owner("maidsafe")
        .repo_name("safe_network")
        .with_target(target)
        .build()?
        .fetch()?;

    if releases.is_empty() {
        println!("Current version is '{}'", cargo_crate_version!());
        println!("No releases are available for updates");
        return Ok(Status::UpToDate(
            "No releases are available for updates".to_string(),
        ));
    }

    tracing::debug!("Target for update is {}", target);
    tracing::debug!("Found releases: {:#?}\n", releases);
    let bin_name = if target.contains("pc-windows") {
        "sn_node.exe"
    } else {
        "sn_node"
    };
    let status = self_update::backends::github::Update::configure()
        .repo_owner("maidsafe")
        .repo_name("safe_network")
        .target(target)
        .bin_name(bin_name)
        .show_download_progress(true)
        .no_confirm(true)
        .current_version(cargo_crate_version!())
        .build()?
        .update()?;
    println!("Update status: '{}'!", status.version());
    Ok(status)
}

fn gen_completions_for_shell(shell: Shell, mut cmd: clap::Command) -> Result<Vec<u8>, String> {
    // Get exe path
    let exe_path =
        std::env::current_exe().map_err(|err| format!("Can't get the exec path: {err}"))?;

    // get filename without preceding path as std::ffi::OsStr (C string)
    let exec_name_ffi = match exe_path.file_name() {
        Some(v) => v,
        None => {
            return Err(format!(
                "Can't extract file_name of executable from path {}",
                exe_path.display()
            ))
        }
    };

    // Convert OsStr to string.  Can fail if OsStr contains any invalid unicode.
    let exec_name = match exec_name_ffi.to_str() {
        Some(v) => v.to_string(),
        None => {
            return Err(format!(
                "Can't decode unicode in executable name '{exec_name_ffi:?}'",
            ))
        }
    };

    // Generates shell completions for <shell> and prints to stdout
    let mut buf: Vec<u8> = vec![];
    generate(shell, &mut cmd, exec_name, &mut buf);

    Ok(buf)
}
