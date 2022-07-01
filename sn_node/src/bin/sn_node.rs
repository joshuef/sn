// Copyright 2022 MaidSafe.net limited.
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
    unused_results
)]

#[cfg(not(feature = "tokio-console"))]
use sn_interface::LogFormatter;
use sn_node::node::{
    add_connection_info, set_connection_info, Config, Error as NodeError, Event, MembershipEvent,
    NodeApi,
};

use color_eyre::{Section, SectionExt};
#[cfg(not(feature = "tokio-console"))]
use eyre::Error;
use eyre::{eyre, Context, ErrReport, Result};
use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use self_update::{cargo_crate_version, Status};
use std::{fmt::Debug, io, io::Write, path::Path, process::exit};
use structopt::{clap, StructOpt};
use tokio::time::{sleep, Duration};
use tracing::{self, debug, error, info, trace, warn};

#[cfg(not(feature = "tokio-console"))]
use tracing_appender::non_blocking::WorkerGuard;

#[cfg(not(feature = "tokio-console"))]
use tracing_subscriber::filter::EnvFilter;

#[cfg(not(feature = "tokio-console"))]
const MODULE_NAME: &str = "sn_node";
const JOIN_TIMEOUT_SEC: u64 = 30;
const BOOTSTRAP_RETRY_TIME_SEC: u64 = 15;

fn main() -> Result<()> {
    color_eyre::install()?;
    #[cfg(feature = "tokio-console")]
    console_subscriber::init();

    // first, let's grab the config. We do this outwith of the node, so we can init logging
    // with the config, and so it can persists across node restarts
    let config_rt = tokio::runtime::Runtime::new()?;
    let config = config_rt.block_on(Config::new())?;
    // shut down this runtime, we do not need it anymore
    config_rt.shutdown_timeout(Duration::from_secs(1));

    trace!("Initial node config: {config:?}");

    #[cfg(not(feature = "tokio-console"))]
    let _guard = init_node_logging(config).map_err(Error::from)?;

    loop {
        create_runtime_and_node()?;
    }
}

/// Create a tokio runtime per `run_node` instance.
///
fn create_runtime_and_node() -> Result<()> {
    info!("Node runtime started");

    // start a new runtime for a node.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("sn_node")
        // 16mb here for windows stack size, which was being exceeded previously
        .thread_stack_size(16 * 1024 * 1024)
        .build()?;

    let _res = rt.block_on(async move {
        // pull config again in case it has been updated meanwhile
        let config = Config::new().await?;

        let local = tokio::task::LocalSet::new();

        local
            .run_until(async move {
                // we want logging to persist
                // loops ready to catch any ChurnJoinMiss
                match run_node(config).await {
                    Ok(_) => {
                        info!("Node has finished running, no runtime errors were reported");
                    }
                    Err(error) => {
                        warn!("Node instance finished with an error: {error:?}");
                    }
                };
            })
            .await;

        Result::<(), NodeError>::Ok(())
    });

    info!("Shutting down node runtime");

    // doesn't really matter the outcome here.
    rt.shutdown_timeout(Duration::from_secs(2));
    debug!("Node runtime should be shutdown now");
    Ok(())
}

/// Inits node logging, returning the global node guard if required.
/// This guard should be held for the life of the program.
///
/// Logging should be instantiated only once.
#[cfg(not(feature = "tokio-console"))]
fn init_node_logging(config: Config) -> Result<Option<WorkerGuard>> {
    // ==============
    // Set up logging
    // ==============

    let mut _optional_guard: Option<WorkerGuard> = None;

    let filter = match EnvFilter::try_from_env("RUST_LOG") {
        Ok(filter) => filter,
        // If we have an error (ie RUST_LOG not set or otherwise), we check the verbosity flags
        Err(_) => {
            // we manually determine level filter instead of using tracing EnvFilter.
            let level_filter = config.verbose();
            let module_filter = format!("{}={}", MODULE_NAME, level_filter)
                .parse()
                .wrap_err("BUG: invalid module filter constructed")?;
            EnvFilter::from_default_env().add_directive(module_filter)
        }
    };

    _optional_guard = if let Some(log_dir) = config.log_dir() {
        println!("Starting logging to directory: {:?}", log_dir);

        let mut content_limit = ContentLimit::BytesSurpassed(config.logs_max_bytes);
        if config.logs_max_lines > 0 {
            content_limit = ContentLimit::Lines(config.logs_max_lines);
        }

        // FileRotate crate changed `0 means for all` to `0 means only original`
        // Here set the retained value to be same as uncompressed in case of 0.
        let logs_retained = if config.logs_retained == 0 {
            config.logs_uncompressed
        } else {
            config.logs_retained
        };
        let file_appender = FileRotateAppender::make_rotate_appender(
            log_dir,
            "sn_node.log",
            AppendCount::new(logs_retained),
            content_limit,
            Compression::OnRotate(config.logs_uncompressed),
        );

        // configure how tracing non-blocking works: https://tracing.rs/tracing_appender/non_blocking/struct.nonblockingbuilder#method.default
        let non_blocking_builder = tracing_appender::non_blocking::NonBlockingBuilder::default();

        let (non_blocking, guard) = non_blocking_builder
                  // lose lines and keep perf, or exert backpressure?
                  .lossy(false)
                  // optionally change buffered lines limit
                  // .buffered_lines_limit(buffered_lines_limit)
                  .finish(file_appender);

        let builder = tracing_subscriber::fmt()
              // eg : RUST_LOG=my_crate=info,my_crate::my_mod=debug,[my_span]=trace
                  .with_env_filter(filter)
                  .with_thread_names(true)
                  .with_ansi(false)
                  .with_writer(non_blocking);

        if config.json_logs {
            builder.json().init();
        } else {
            builder.event_format(LogFormatter::default()).init();
        }

        Some(guard)
    } else {
        println!("Starting logging to stdout");

        tracing_subscriber::fmt()
            .with_thread_names(true)
            .with_ansi(false)
            .with_env_filter(filter)
            .with_target(false)
            .event_format(LogFormatter::default())
            .init();

        None
    };

    Ok(_optional_guard)
}

/// `FileRotateAppender` is a tracing_appender with extra logrotate features:
///  - most recent logfile name re-used to support following (e.g. 'tail -f=logfile')
///  - numbered rotation (logfile.1, logfile.2 etc)
///  - limit logfile by size, lines or time
///  - limit maximum number of logfiles
///  - optional compression of rotated logfiles
//
// The above functionality is provided using crate file_rotation
pub struct FileRotateAppender {
    writer: FileRotate<AppendCount>,
}

impl FileRotateAppender {
    /// Create default `FileRotateAppender`
    pub fn new(directory: impl AsRef<Path>, file_name_prefix: impl AsRef<Path>) -> Self {
        let log_directory = directory.as_ref().to_str().unwrap();
        let log_filename_prefix = file_name_prefix.as_ref().to_str().unwrap();
        let path = Path::new(&log_directory).join(&log_filename_prefix);
        let writer = FileRotate::new(
            &Path::new(&path),
            AppendCount::new(9),
            ContentLimit::Bytes(10 * 1024 * 1024),
            Compression::OnRotate(1),
        );

        Self { writer }
    }

    /// Create `FileRotateAppender` using parameters
    pub fn make_rotate_appender(
        directory: impl AsRef<Path>,
        file_name_prefix: impl AsRef<Path>,
        num_logs: AppendCount,
        max_log_size: ContentLimit,
        compression: Compression,
    ) -> Self {
        let log_directory = directory.as_ref().to_str().unwrap();
        let log_filename_prefix = file_name_prefix.as_ref().to_str().unwrap();
        let path = Path::new(&log_directory).join(&log_filename_prefix);
        let writer = FileRotate::new(&Path::new(&path), num_logs, max_log_size, compression);

        Self { writer }
    }
}

impl Write for FileRotateAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

use std::fmt;

impl Debug for FileRotateAppender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileRotateAppender").finish()
    }
}

async fn run_node(config: Config) -> Result<()> {
    if let Some(c) = &config.completions() {
        let shell = c.parse().map_err(|err: String| eyre!(err))?;
        let buf = gen_completions_for_shell(shell).map_err(|err| eyre!(err))?;
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
            Err(e) => error!("Updating node failed: {:?}", e),
        }

        if config.update_only() {
            exit(0);
        }
    }

    let message = format!(
        "Running {} v{}",
        Config::clap().get_name(),
        env!("CARGO_PKG_VERSION")
    );
    info!("\n{}\n{}", message, "=".repeat(message.len()));

    let our_pid = std::process::id();

    let log = format!("The network is not accepting nodes right now. Retrying after {BOOTSTRAP_RETRY_TIME_SEC} seconds");

    let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
    let bootstrap_retry_duration = Duration::from_secs(BOOTSTRAP_RETRY_TIME_SEC);

    let (node, mut event_stream) = loop {
        match NodeApi::new(&config, join_timeout).await {
            Ok(result) => break result,
            Err(NodeError::CannotConnectEndpoint(qp2p::EndpointError::Upnp(error))) => {
                return Err(error).suggestion(
                    "You can disable port forwarding by supplying --skip-auto-port-forwarding. Without port\n\
                    forwarding, your machine must be publicly reachable by the given\n\
                    --public-addr. If your machine is not publicly reachable, you may have to\n\
                    adjust your router settings to either:\n\
                    \n\
                    - Resolve the error (e.g. by enabling UPnP).\n\
                    - Manually configure port forwarding, such that your machine is publicly \
                      reachable, and supplying that address with --public-addr."
                        .header("Disable port forwarding or change your router settings"),
                );
            }
            Err(NodeError::TryJoinLater) => {
                println!("{}", log);
                info!("{}", log);
            }
            Err(NodeError::NodeNotReachable(addr)) => {
                let err_msg = format!(
                    "Unfortunately we are unable to establish a connection to your machine ({}) either through a \
                    public IP address, or via IGD on your router. Please ensure that IGD is enabled on your router - \
                    if it is and you are still unable to add your node to the testnet, then skip adding a node for this \
                    testnet iteration. You can still use the testnet as a client, uploading and downloading content, etc. \
                    https://safenetforum.org/",
                    addr
                );
                println!("{}", err_msg);
                error!("{}", err_msg);
                exit(1);
            }
            Err(NodeError::JoinTimeout) => {
                let message = format!("(PID: {our_pid}): Encountered a timeout while trying to join the network. Retrying after {BOOTSTRAP_RETRY_TIME_SEC} seconds.");
                println!("{}", &message);
                error!("{}", &message);
            }
            Err(e) => {
                let log_path = if let Some(path) = config.log_dir() {
                    format!("{}", path.display())
                } else {
                    "unknown".to_string()
                };

                return Err(e).wrap_err(format!(
                    "Cannot start node (log path: {}). If this is the first node on the network pass the local \
                    address to be used using --first", log_path)
                );
            }
        }
        sleep(bootstrap_retry_duration).await;
    };

    let our_conn_info = node.our_connection_info().await;

    if config.is_first() {
        let genesis_key = node.genesis_key().await;
        set_connection_info(genesis_key, our_conn_info)
            .await
            .unwrap_or_else(|err| {
                error!("Unable to write our connection info to disk: {:?}", err);
            });
    } else {
        add_connection_info(our_conn_info)
            .await
            .unwrap_or_else(|err| {
                error!("Unable to add our connection info to disk: {:?}", err);
            });
    }

    // Simulate failed node starts, and ensure that
    #[cfg(feature = "chaos")]
    {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let x: f64 = rng.gen_range(0.0..1.0);

        if !config.is_first() && x > 0.6 {
            println!(
                "\n =========== [Chaos] (PID: {our_pid}): Startup chaos crash w/ x of: {} ============== \n",
                x
            );

            // tiny sleep so testnet doesn't detect a fauly node and exit
            sleep(Duration::from_secs(1)).await;
            warn!("[Chaos] (PID: {our_pid}): ChaoticStartupCrash");
            return Err(NodeError::ChaoticStartupCrash).map_err(ErrReport::msg);
        }
    }

    // This just keeps the node going as long as routing goes
    while let Some(event) = event_stream.next().await {
        trace!("Node event! {}", event);
        if let Event::Membership(MembershipEvent::ChurnJoinMissError) = event {
            return Err(NodeError::ChurnJoinMiss).map_err(ErrReport::msg);
        }
    }

    Ok(())
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

fn gen_completions_for_shell(shell: clap::Shell) -> Result<Vec<u8>, String> {
    // Get exe path
    let exe_path =
        std::env::current_exe().map_err(|err| format!("Can't get the exec path: {}", err))?;

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
                "Can't decode unicode in executable name '{:?}'",
                exec_name_ffi
            ))
        }
    };

    // Generates shell completions for <shell> and prints to stdout
    let mut buf: Vec<u8> = vec![];
    Config::clap().gen_completions_to(exec_name, shell, &mut buf);

    Ok(buf)
}
