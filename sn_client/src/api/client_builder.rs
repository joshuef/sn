//! A builder to instantiate a [`Client`]
//!
//! # Example
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), sn_client::Error> {
//! use sn_client::api::Client;
//! use xor_name::XorName;
//!
//! let client = Client::builder().build().await?;
//! let _bytes = client.read_bytes(XorName::from_content("example".as_bytes())).await?;
//!
//! # Ok(())
//! # }
//! ```
use crate::{sessions::Session, Client, Error, DEFAULT_NETWORK_CONTACTS_FILE_NAME};

use sn_dbc::Owner;
use sn_interface::{network_knowledge::SectionTree, types::Keypair};
use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

/// Environment variable used to convert into [`ClientBuilder::query_timeout`] (seconds)
pub const ENV_QUERY_TIMEOUT: &str = "SN_QUERY_TIMEOUT";
/// Environment variable used to convert into [`ClientBuilder::max_backoff_interval`] (seconds)
pub const ENV_MAX_BACKOFF_INTERVAL: &str = "SN_MAX_BACKOFF_INTERVAL";
/// Environment variable used to convert into [`ClientBuilder::cmd_timeout`] (seconds)
pub const ENV_CMD_TIMEOUT: &str = "SN_CMD_TIMEOUT";

/// Bind by default to all network interfaces on a OS assigned port
pub const DEFAULT_LOCAL_ADDR: (Ipv4Addr, u16) = (Ipv4Addr::UNSPECIFIED, 0);
/// Default timeout to use before timing out queries and commands
pub const DEFAULT_QUERY_CMD_TIMEOUT: Duration = Duration::from_secs(15);
/// Max amount of time for an operation backoff (time between attempts). In Seconds.
pub const DEFAULT_MAX_QUERY_CMD_BACKOFF_INTERVAL: Duration = Duration::from_secs(3);

/// Build a [`crate::Client`]
#[derive(Debug, Default)]
pub struct ClientBuilder {
    keypair: Option<Keypair>,
    dbc_owner: Option<Owner>,
    local_addr: Option<SocketAddr>,
    query_timeout: Option<Duration>,
    max_backoff_interval: Option<Duration>,
    cmd_timeout: Option<Duration>,
    network_contacts: Option<SectionTree>,
}

impl ClientBuilder {
    /// Instantiate a builder with default parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the keypair associated with the queries sent from this client.
    pub fn keypair(mut self, kp: impl Into<Option<Keypair>>) -> Self {
        self.keypair = kp.into();
        self
    }

    /// Set the DBC owner associated with this client.
    pub fn dbc_owner(mut self, owner: impl Into<Option<Owner>>) -> Self {
        self.dbc_owner = owner.into();
        self
    }

    /// Local address to bind client endpoint to
    pub fn local_addr(mut self, addr: impl Into<Option<SocketAddr>>) -> Self {
        self.local_addr = addr.into();
        self
    }

    /// Time to wait for responses to queries before giving up and returning an error
    pub fn query_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.query_timeout = timeout.into();
        self
    }

    /// Max backoff time between operation retries
    pub fn max_backoff_interval(
        mut self,
        max_backoff_interval: impl Into<Option<Duration>>,
    ) -> Self {
        self.max_backoff_interval = max_backoff_interval.into();
        self
    }

    /// Time to wait for cmds to not error before giving up and returning an error
    pub fn cmd_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.cmd_timeout = timeout.into();
        self
    }

    /// SectionTree used to bootstrap the client on the network
    pub fn network_contacts(mut self, pm: impl Into<Option<SectionTree>>) -> Self {
        self.network_contacts = pm.into();
        self
    }

    /// Read options from environment variables:
    /// - [`Self::query_timeout()`] from [`ENV_QUERY_TIMEOUT`]
    /// - [`Self::max_backoff_interval()`] from [`ENV_MAX_BACKOFF_INTERVAL`]
    /// - [`Self::cmd_timeout()`] from [`ENV_CMD_TIMEOUT`]
    pub fn from_env(mut self) -> Self {
        if let Ok(Some(v)) = env_parse(ENV_QUERY_TIMEOUT) {
            self.query_timeout = Some(Duration::from_secs(v));
        }
        if let Ok(Some(v)) = env_parse(ENV_MAX_BACKOFF_INTERVAL) {
            self.max_backoff_interval = Some(Duration::from_secs(v));
        }
        if let Ok(Some(v)) = env_parse(ENV_CMD_TIMEOUT) {
            self.cmd_timeout = Some(Duration::from_secs(v));
        }

        self
    }

    /// Instantiate the [`Client`] using the parameters passed to this builder.
    ///
    /// In case parameters have not been passed to this builder, defaults will be used:
    /// - `[Self::keypair]` and `[Self::dbc_owner]` are randomly generated
    /// - `[Self::query_timeout`] and `[Self::cmd_timeout]` default to [`DEFAULT_QUERY_CMD_TIMEOUT`]
    /// - `[Self::max_backoff_interval`] defaults to [`DEFAULT_MAX_QUERY_CMD_BACKOFF_INTERVAL`]
    /// - Network contacts file will be read from a standard location
    pub async fn build(self) -> Result<Client, Error> {
        let max_backoff_interval = self
            .max_backoff_interval
            .unwrap_or(DEFAULT_MAX_QUERY_CMD_BACKOFF_INTERVAL);
        let query_timeout = self.query_timeout.unwrap_or(DEFAULT_QUERY_CMD_TIMEOUT);
        let cmd_timeout = self.cmd_timeout.unwrap_or(DEFAULT_QUERY_CMD_TIMEOUT);

        let network_contacts = match self.network_contacts {
            Some(pm) => pm,
            None => {
                let network_contacts_dir = default_network_contacts_path()?;
                SectionTree::from_disk(&network_contacts_dir)
                    .await
                    .map_err(|err| Error::NetworkContacts(err.to_string()))?
            }
        };

        let session = Session::new(
            self.local_addr
                .unwrap_or_else(|| SocketAddr::from(DEFAULT_LOCAL_ADDR)),
            network_contacts,
        )?;

        let keypair = self.keypair.unwrap_or_else(Keypair::new_ed25519);
        let dbc_owner = self
            .dbc_owner
            .unwrap_or_else(|| Owner::from_random_secret_key(&mut rand::thread_rng()));

        let client = Client {
            keypair,
            dbc_owner,
            session,
            query_timeout,
            max_backoff_interval,
            cmd_timeout,
            chunks_cache: Arc::new(RwLock::new(Default::default())),
        };
        client.connect().await?;

        Ok(client)
    }
}

/// Parse environment variable. Returns `Ok(None)` if environment variable isn't set.
fn env_parse<F: FromStr>(s: &str) -> Result<Option<F>, F::Err> {
    match std::env::var(s) {
        Ok(v) => F::from_str(&v).map(|v| Some(v)),
        Err(_) => Ok(None),
    }
}

fn default_network_contacts_path() -> Result<PathBuf, Error> {
    // Use `$HOME/.safe/network_contacts` directory
    let path = dirs_next::home_dir()
        .ok_or_else(|| {
            crate::Error::NetworkContacts("Could not read user's home directory".to_string())
        })?
        .join(".safe")
        .join("network_contacts")
        .join(DEFAULT_NETWORK_CONTACTS_FILE_NAME);

    Ok(path)
}

#[cfg(test)]
mod tests {}
