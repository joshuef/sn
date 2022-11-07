// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// --------------------------------------------------------------------
// ------ The following is what's meant to be the public API -------

pub mod files;
pub mod keys;
pub mod multimap;
pub mod nrs;
pub mod register;
pub mod resolver;
pub mod wallet;

pub use crate::safeurl::*;
pub use consts::DEFAULT_XORURL_BASE;
pub use sn_client::DEFAULT_NETWORK_CONTACTS_FILE_NAME;
pub use sn_interface::network_knowledge::SectionTree;
pub use xor_name::{XorName, XOR_NAME_LEN};

// --------------------------------------------------------------------

mod auth;
mod consts;
mod helpers;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;

use super::{common, constants, Error, Result};

use sn_client::Client;
use sn_dbc::Owner;
use sn_interface::types::Keypair;

use std::time::Duration;
use tracing::debug;

const APP_NOT_CONNECTED: &str = "Application is not connected to the network";

#[derive(Clone)]
pub struct Safe {
    client: Option<Client>,
    pub xorurl_base: XorUrlBase,
    pub dry_run_mode: bool,
}

impl Safe {
    /// Create a Safe instance without connecting to the SAFE Network
    pub fn dry_runner(xorurl_base: Option<XorUrlBase>) -> Self {
        Self {
            client: None,
            xorurl_base: xorurl_base.unwrap_or(DEFAULT_XORURL_BASE),
            dry_run_mode: true,
        }
    }

    /// Create a Safe instance connected to the SAFE Network
    pub async fn connected(
        keypair: Option<Keypair>,
        xorurl_base: Option<XorUrlBase>,
        timeout: Option<Duration>,
        dbc_owner: Option<Owner>,
    ) -> Result<Self> {
        let mut safe = Self {
            client: None,
            xorurl_base: xorurl_base.unwrap_or(DEFAULT_XORURL_BASE),
            dry_run_mode: false,
        };

        safe.connect(keypair, timeout, dbc_owner).await?;

        Ok(safe)
    }

    /// Connect to the SAFE Network
    pub async fn connect(
        &mut self,
        keypair: Option<Keypair>,
        timeout: Option<Duration>,
        dbc_owner: Option<Owner>,
    ) -> Result<()> {
        debug!("Connecting to SAFE Network...");
        debug!("Client to be instantiated with specific pk?: {:?}", keypair);

        let mut b = Client::builder()
            .from_env() // Load parameters from environment variables.
            .keypair(keypair)
            .dbc_owner(dbc_owner);

        // Override timeout
        if let Some(timeout) = timeout {
            b = b.query_timeout(timeout).cmd_timeout(timeout);
        }

        self.client = Some(b.build().await.map_err(|err| {
            Error::ConnectionError(format!("Failed to connect to the SAFE Network: {:?}", err))
        })?);

        debug!("Successfully connected to the Network!!!");

        Ok(())
    }

    /// Returns true if we already have a connection with the network
    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// SectionTree used to bootstrap on the network.
    ///
    /// This is updated by as Anti-Entropy/update messages are received from the network.
    /// Any user of this API is responsible for caching it so it can use it for any new `Safe`
    /// instance, preventing it from learning all this information from the network all over again.
    pub async fn section_tree(&mut self) -> Result<SectionTree> {
        let section_tree = self.get_safe_client()?.section_tree().await;
        Ok(section_tree)
    }

    // Private helper to obtain the Client instance
    pub(crate) fn get_safe_client(&mut self) -> Result<Client> {
        match self.client.clone() {
            Some(client) => Ok(client),
            None => Err(Error::ConnectionError(APP_NOT_CONNECTED.to_string())),
        }
    }
}
