// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Implementation of the "Node" node for the SAFE Network.

// Configuration
mod cfg;
// Node public API
mod api;

mod core;
mod dkg;
// mod ed25519;
mod error;
mod logging;
pub(crate) mod membership;
mod messages;

use sn_interface::types::Peer;

pub use self::{
    api::{
        event::{Elders, Event, MessageReceived, NodeElderChange},
        event_stream::EventStream,
        NodeApi,
    },
    cfg::config_handler::{add_connection_info, set_connection_info, Config},
    error::{Error, Result},
};
pub use qp2p::{Config as NetworkConfig, SendStream};
pub use sn_interface::network_knowledge::{
    FIRST_SECTION_MAX_AGE, FIRST_SECTION_MIN_AGE, MIN_ADULT_AGE,
};
pub use xor_name::{Prefix, XorName, XOR_NAME_LEN}; // TODO remove pub on API update

#[cfg(feature = "service-msgs")]
pub(crate) use self::core::MIN_LEVEL_WHEN_FULL;
#[cfg(feature = "test-utils")]
pub use test_utils::*;

#[cfg(any(test, feature = "test-utils"))]
mod test_utils {
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    const TEST_MAX_CAPACITY: usize = 1024 * 1024;

    /// Create a register store for routing examples
    pub fn create_test_max_capacity_and_root_storage() -> eyre::Result<(usize, PathBuf)> {
        let random_filename: String = thread_rng().sample_iter(&Alphanumeric).take(15).collect();

        let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
        let storage_dir = Path::new(root_dir.path()).join(random_filename);

        Ok((TEST_MAX_CAPACITY, storage_dir))
    }
}
