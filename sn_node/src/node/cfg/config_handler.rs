// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Error, NetworkConfig, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    io::{self},
    net::SocketAddr,
    path::PathBuf,
};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    time::Duration,
};
use tracing::{debug, error, warn, Level};

pub(crate) const DEFAULT_MIN_CAPACITY: usize = 50 * 1024 * 1024 * 1024; // 1gb
pub(crate) const DEFAULT_MAX_CAPACITY: usize = 2 * DEFAULT_MIN_CAPACITY;

const CONFIG_FILE: &str = "node.config";
const DEFAULT_ROOT_DIR_NAME: &str = "root_dir";

// In the absence of any (QUIC) keep-alive messages, connections will be closed
// if they remain idle for at least this duration.
const DEFAULT_IDLE_TIMEOUT_MSEC: u64 = 70_000;

/// Node configuration
#[derive(Default, Clone, Debug, Serialize, Deserialize, clap::StructOpt)]
#[clap(rename_all = "kebab-case", bin_name = "sn_node", version)]
#[clap(global_settings = &[clap::AppSettings::ColoredHelp])]
pub struct Config {
    /// The address to be credited when this node farms SafeCoin.
    /// A hex formatted BLS public key.
    #[clap(short, long, parse(try_from_str))]
    pub wallet_id: Option<String>,
    /// Root directory for dbs and cached state. If not set, it defaults to "root_dir"
    /// within the sn_node project data directory, located at:
    /// Linux: $HOME/.safe/node/root_dir
    /// Windows: {FOLDERID_Profile}/.safe/node/root_dir
    /// MacOS: $HOME/.safe/node/root_dir
    #[clap(short, long, parse(from_os_str))]
    pub root_dir: Option<PathBuf>,
    /// Verbose output. `-v` is equivalent to logging with `warn`, `-vv` to `info`, `-vvv` to
    /// `debug`, `-vvvv` to `trace`. This flag overrides RUST_LOG.
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: u8,
    /// dump shell completions for: [bash, fish, zsh, powershell, elvish]
    #[clap(long)]
    pub completions: Option<String>,
    /// Send logs to a file within the specified directory
    #[clap(long)]
    pub log_dir: Option<PathBuf>,
    /// Number of rotated log files to keep (0 to keep all)
    #[clap(long, default_value = "0")]
    pub logs_retained: usize,
    /// Maximum bytes per log file
    #[clap(long, default_value = "10485760")]
    pub logs_max_bytes: usize,
    /// Maximum lines per log file (overrides logs_max_bytes)
    #[clap(long, default_value = "0")]
    pub logs_max_lines: usize,
    /// Number of rotated files left not compressed
    #[clap(long, default_value = "100")] // 100*10mb files by default
    pub logs_uncompressed: usize,
    /// Attempt to self-update?
    #[clap(long)]
    pub update: bool,
    /// Attempt to self-update without starting the node process
    #[clap(long)]
    pub update_only: bool,
    /// Outputs logs in json format for easier processing
    #[clap(short, long)]
    pub json_logs: bool,
    /// print node resourse usage to stdout
    #[clap(long)]
    pub resource_logs: bool,
    /// Delete all data from a previous node running on the same PC
    #[clap(long)]
    pub clear_data: bool,
    /// Whether the node is the first on the network.
    ///
    /// When set, you must specify either `--local-addr` or `--public-addr` to ensure the correct
    /// connection info is stored.
    #[clap(long)]
    pub first: bool,
    /// File with initial network contacts to bootstrap to if this node is not the first on
    /// the network. This argument and the `--first` flag are mutually exclusive.
    ///
    /// This shall be set to the file path where a valid `SectionTree` can be read from.
    #[clap(short, long)]
    pub network_contacts_file: Option<PathBuf>,
    /// Local address to be used for the node.
    ///
    /// When unspecified, the node will listen on `0.0.0.0` with a random unused port. If you're
    /// running a local-only network, you should set this to `127.0.0.1:0` to prevent any external
    /// traffic from reaching the node (but note that the node will also be unable to connect to
    /// non-local nodes).
    #[clap(long)]
    pub local_addr: Option<SocketAddr>,
    /// External address of the node, to use when writing connection info.
    ///
    /// If unspecified, it will be queried from a peer; if there are no peers, the `local-addr` will
    /// be used, if specified.
    #[clap(long, parse(try_from_str = parse_public_addr))]
    pub public_addr: Option<SocketAddr>,
    /// DEPRECATED (to be removed)
    /// This flag can be used to skip automated port forwarding using IGD. This is used when running
    /// a network on a LAN or when a node is connected to the internet directly, without a router,
    /// e.g. Digital Ocean droplets.
    #[clap(long)]
    pub skip_auto_port_forwarding: bool,
    /// This is the maximum message size we'll allow the peer to send to us. Any bigger message and
    /// we'll error out probably shutting down the connection to the peer. If none supplied we'll
    /// default to the documented constant.
    #[clap(long)]
    pub max_msg_size_allowed: Option<u32>,
    /// If we hear nothing from the peer in the given interval we declare it offline to us. If none
    /// supplied we'll default to the documented constant.
    ///
    /// The interval is in milliseconds. A value of 0 disables this feature.
    #[clap(long)]
    pub idle_timeout_msec: Option<u64>,
    /// Interval to send keep-alives if we are idling so that the peer does not disconnect from us
    /// declaring us offline. If none is supplied we'll default to the documented constant.
    ///
    /// The interval is in milliseconds. A value of 0 disables this feature.
    #[clap(long)]
    pub keep_alive_interval_msec: Option<u32>,
    /// Duration of a UPnP port mapping.
    #[clap(long)]
    pub upnp_lease_duration: Option<u32>,
}

impl Config {
    /// Returns a new `Config` instance.  Tries to read from the default node config file location,
    /// and overrides values with any equivalent cmd line args.
    pub async fn new() -> Result<Self, Error> {
        let mut config = Config::default();

        let cmd_line_args = Config::parse();
        cmd_line_args.validate()?;

        config.merge(cmd_line_args);

        config.clear_data_from_disk().await.unwrap_or_else(|_| {
            error!("Error deleting data file from disk");
        });

        info!("Node config to be used: {:?}", config);
        Ok(config)
    }

    /// Validate configuration that came from the cmd line.
    ///
    /// `StructOpt` doesn't support validation that crosses multiple field values.
    fn validate(&self) -> Result<(), Error> {
        if !(self.first ^ self.network_contacts_file.is_some()) {
            return Err(Error::Configuration(
                "Either the --first or --network-contacts-file argument is required, and they \
                are mutually exclusive. Please run the command again and use one or the other, \
                but not both, of these arguments."
                    .to_string(),
            ));
        }

        if let Some(local_addr) = self.local_addr {
            if local_addr.ip().is_loopback() && self.public_addr.is_some() {
                return Err(Error::Configuration(
                    "Cannot specify --public-addr when --local-addr uses a loopback IP. \
                    When local-addr uses a loopback IP, the node will never be reachable publicly. \
                    You can drop public-addr if this is a local-only node, or change local-addr to \
                    a public or unspecified IP."
                        .to_string(),
                ));
            }
        }

        let local_ip_unspecified = self
            .local_addr
            .map(|addr| addr.ip().is_unspecified())
            .unwrap_or(true);
        if local_ip_unspecified && self.first && self.public_addr.is_none() {
            return Err(Error::Configuration(
                "Must specify public address for --first node. \
                The first node cannot query its public address from peers, so one must be \
                specifed. This can be specified with --public-addr, or by setting a concrete IP \
                for --local-addr."
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Overwrites the current config with the provided values from another config
    fn merge(&mut self, config: Config) {
        if let Some(wallet_id) = config.wallet_id() {
            self.wallet_id = Some(wallet_id.clone());
        }

        if config.root_dir.is_some() {
            self.root_dir = config.root_dir.clone();
        }

        self.json_logs = config.json_logs;
        self.resource_logs = config.resource_logs;

        if config.verbose > 0 {
            self.verbose = config.verbose;
        }

        if config.completions.is_some() {
            self.completions = config.completions.clone();
        }

        if config.log_dir.is_some() {
            self.log_dir = config.log_dir.clone();
        }

        self.logs_retained = config.logs_retained();
        self.logs_max_bytes = config.logs_max_bytes();
        self.logs_max_lines = config.logs_max_lines();
        self.logs_uncompressed = config.logs_uncompressed();

        self.update = config.update || self.update;
        self.update_only = config.update_only || self.update_only;
        self.clear_data = config.clear_data || self.clear_data;
        self.first = config.first || self.first;

        if config.network_contacts_file.is_some() {
            self.network_contacts_file = config.network_contacts_file;
        }

        if config.local_addr.is_some() {
            self.local_addr = config.local_addr;
        }

        if config.public_addr.is_some() {
            self.public_addr = config.public_addr;
        }

        if config.max_msg_size_allowed.is_some() {
            self.max_msg_size_allowed = config.max_msg_size_allowed;
        }

        match config.idle_timeout_msec {
            Some(t) => self.idle_timeout_msec = Some(t),
            None => self.idle_timeout_msec = Some(DEFAULT_IDLE_TIMEOUT_MSEC),
        }

        if config.keep_alive_interval_msec.is_some() {
            self.keep_alive_interval_msec = config.keep_alive_interval_msec;
        }
    }

    /// The address to be credited when this node farms `SafeCoin`.
    pub fn wallet_id(&self) -> Option<&String> {
        self.wallet_id.as_ref()
    }

    /// Is this the first node in a section?
    pub fn is_first(&self) -> bool {
        self.first
    }

    /// Network contacts to bootstrap to if this is not the first node in a network
    pub fn network_contacts_file(&self) -> Option<PathBuf> {
        self.network_contacts_file.clone()
    }

    /// The minimum capacity in bytes required by the network, to avoid the risk of being kicked out.
    pub fn min_capacity(&self) -> usize {
        DEFAULT_MIN_CAPACITY
    }

    /// Upper limit in bytes for allowed network storage on this node.
    pub fn max_capacity(&self) -> usize {
        DEFAULT_MAX_CAPACITY
    }

    /// Root directory for dbs and cached state. If not set, it defaults to
    /// `DEFAULT_ROOT_DIR_NAME` within the project's data directory (see `Config::root_dir` for the
    /// directories on each platform).
    pub fn root_dir(&self) -> Result<PathBuf> {
        Ok(match &self.root_dir {
            Some(root_dir) => root_dir.clone(),
            None => project_dirs()?.join(DEFAULT_ROOT_DIR_NAME),
        })
    }

    /// Set the root directory for dbs and cached state.
    pub fn set_root_dir<P: Into<PathBuf>>(&mut self, path: P) {
        self.root_dir = Some(path.into())
    }

    /// Set the directory to write the logs.
    pub fn set_log_dir<P: Into<PathBuf>>(&mut self, path: P) {
        self.log_dir = Some(path.into())
    }

    /// Get the log level.
    pub fn verbose(&self) -> Level {
        match self.verbose {
            0 => Level::ERROR,
            1 => Level::WARN,
            2 => Level::INFO,
            3 => Level::DEBUG,
            _ => Level::TRACE,
        }
    }

    /// Local address to be used for the node.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
            .unwrap_or_else(|| SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0)))
    }

    /// Network configuration options.
    pub fn network_config(&self) -> NetworkConfig {
        let mut network_config = NetworkConfig::default();
        if let Some(public_addr) = self.public_addr {
            network_config.external_port = Some(public_addr.port());
            network_config.external_ip = Some(public_addr.ip());
        }

        if let Some(t) = self.idle_timeout_msec {
            network_config.idle_timeout = Some(Duration::from_millis(t));
        }

        if let Some(t) = self.keep_alive_interval_msec {
            network_config.keep_alive_interval = Some(Duration::from_millis(t.into()));
        }

        network_config
    }

    /// Get the completions option
    pub fn completions(&self) -> &Option<String> {
        &self.completions
    }

    /// Directory where to write log file/s if specified
    pub fn log_dir(&self) -> &Option<PathBuf> {
        &self.log_dir
    }

    /// Number of rotated log files retained
    pub fn logs_retained(&self) -> usize {
        self.logs_retained
    }

    /// Max lines per logfile
    pub fn logs_max_lines(&self) -> usize {
        self.logs_max_lines
    }

    /// Max bytes per logfile
    pub fn logs_max_bytes(&self) -> usize {
        self.logs_max_bytes
    }

    /// Number of rotated logs left not compressed
    pub fn logs_uncompressed(&self) -> usize {
        self.logs_uncompressed
    }

    /// Attempt to self-update?
    pub fn update(&self) -> bool {
        self.update
    }

    /// Attempt to self-update without starting the node process
    pub fn update_only(&self) -> bool {
        self.update_only
    }

    // Clear data from of a previous node running on the same PC
    async fn clear_data_from_disk(&self) -> Result<()> {
        if self.clear_data {
            let path = project_dirs()?.join(self.root_dir()?);
            if path.exists() {
                fs::remove_dir_all(&path).await?;
            }
        }
        Ok(())
    }

    /// Reads the default node config file.
    #[allow(unused)]
    async fn read_from_file() -> Result<Option<Config>> {
        let path = project_dirs()?.join(CONFIG_FILE);

        match fs::read(path.clone()).await {
            Ok(content) => {
                debug!("Reading settings from {}", path.display());

                serde_json::from_slice(&content).map_err(|err| {
                    warn!(
                        "Could not parse content of config file '{:?}': {:?}",
                        path, err
                    );
                    err.into()
                })
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    debug!("No config file available at {:?}", path);
                    Ok(None)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    /// Writes the config file to disk
    pub async fn write_to_disk(&self) -> Result<()> {
        let project_dirs = project_dirs()?;
        fs::create_dir_all(project_dirs.clone()).await?;

        let path = project_dirs.join(CONFIG_FILE);
        let mut file = File::create(&path).await?;
        let serialized = serde_json::to_string_pretty(self)?;
        file.write_all(serialized.as_bytes()).await?;
        file.sync_all().await?;

        Ok(())
    }
}

fn parse_public_addr(public_addr: &str) -> Result<SocketAddr, String> {
    let public_addr: SocketAddr = public_addr.parse().map_err(|err| format!("{}", err))?;

    if public_addr.ip().is_unspecified() {
        return Err("Cannot use unspecified IP for public address. \
            You can drop this option to query the public IP from a peer instead."
            .to_string());
    }
    if public_addr.ip().is_loopback() {
        return Err("Cannot use loopback IP for public address. \
            You can drop this option for a local-only network."
            .to_string());
    }
    if public_addr.port() == 0 {
        return Err("Cannot use unspecified port for public address. \
            You must specify the concrete port on which the node will be reachable."
            .to_string());
    }

    Ok(public_addr)
}

fn project_dirs() -> Result<PathBuf> {
    let mut home_dir = dirs_next::home_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Home directory not found"))?;

    home_dir.push(".safe");
    home_dir.push("node");

    Ok(home_dir)
}

#[test]
fn smoke() -> Result<()> {
    // NOTE: IF this value is being changed due to a change in the config,
    // the change in config also be handled in Config::merge()
    // and in examples/config_handling.rs
    let expected_size = 51;

    assert_eq!(bincode::serialize(&Config::default())?.len(), expected_size);
    Ok(())
}
