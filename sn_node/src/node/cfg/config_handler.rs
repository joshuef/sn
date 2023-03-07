// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Error, Result};
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
};
use tracing::{error, Level};

pub(crate) const DEFAULT_MIN_CAPACITY: usize = 50 * 1024 * 1024; // 50mb
pub(crate) const DEFAULT_MAX_CAPACITY: usize = 2 * DEFAULT_MIN_CAPACITY;

const CONFIG_FILE: &str = "node.config";
const DEFAULT_ROOT_DIR_NAME: &str = "root_dir";

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
    /// `debug`, `-vvvv` to `trace`. This is overridden by the `RUST_LOG` environment variable.
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
    /// Update sn_node to the latest available version
    #[clap(long)]
    pub update: bool,
    /// Do not prompt to confirm the update
    #[clap(short = 'y', long = "no-confirm")]
    pub no_confirm: bool,
    /// Outputs logs in json format for easier processing
    #[clap(short, long)]
    pub json_logs: bool,
    /// print node resourse usage to stdout
    #[clap(long)]
    pub resource_logs: bool,
    /// Delete all data from a previous node running on the same PC
    #[clap(long)]
    pub clear_data: bool,
    /// Make this the node the first on the network. This requires an address where it's
    /// reachable for by other nodes. This address will be put in the network contacts file.
    ///
    /// If the port is `0`, then the will be equal to whatever port we will locally bind to.
    #[clap(long)]
    pub first: Option<SocketAddr>,
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
}

impl Config {
    /// Returns a new `Config` instance.  Tries to read from the default node config file location,
    /// and overrides values with any equivalent cmd line args.
    pub fn new() -> Result<Self, Error> {
        let mut config = Config::default();
        let cmd_line_args = Config::parse();
        config.merge(cmd_line_args);
        if let Err(err) = config.clear_data_from_disk() {
            error!("Error clearing the data from disk: {err:?}");
        }
        info!("Node config to be used: {:?}", config);
        Ok(config)
    }

    /// Validate configuration that came from the cmd line.
    ///
    /// `StructOpt` doesn't support validation that crosses multiple field values.
    pub fn validate(&self) -> Result<(), Error> {
        if !(self.first.is_some() ^ self.network_contacts_file.is_some()) {
            return Err(Error::Configuration(
                "Either the --first or --network-contacts-file argument is required, and they \
                are mutually exclusive. Please run the command again and use one or the other, \
                but not both, of these arguments."
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
        self.no_confirm = config.no_confirm || self.no_confirm;
        self.clear_data = config.clear_data || self.clear_data;
        if config.first.is_some() {
            self.first = config.first;
        }

        if config.network_contacts_file.is_some() {
            self.network_contacts_file = config.network_contacts_file;
        }

        if config.local_addr.is_some() {
            self.local_addr = config.local_addr;
        }
    }

    /// The address to be credited when this node farms `SafeCoin`.
    pub fn wallet_id(&self) -> Option<&String> {
        self.wallet_id.as_ref()
    }

    /// Is this the first node in a section?
    pub fn first(&self) -> Option<SocketAddr> {
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

    /// Do not prompt for confirmation
    pub fn no_confirm(&self) -> bool {
        self.no_confirm
    }

    // Clear data from of a previous node running on the same PC, if the config flag is set to do so.
    fn clear_data_from_disk(&self) -> Result<()> {
        // Only clear the data if we are configured to do so.
        if !self.clear_data {
            return Ok(());
        }

        let path = project_dirs()?.join(self.root_dir()?);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }

        Ok(())
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
    let expected_size = 45;

    assert_eq!(bincode::serialize(&Config::default())?.len(), expected_size);
    Ok(())
}
