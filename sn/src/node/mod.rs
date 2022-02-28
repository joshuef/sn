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
mod ed25519;
mod error;
mod logging;
mod messages;

use sn_interface::types::{Peer, PublicKey};

use ed25519_dalek::Keypair;
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
    sync::Arc,
};

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

#[cfg(any(test, feature = "test-utils"))]
pub use test_utils::*;

#[cfg(test)]
pub(crate) use dkg::test_utils::section_signed;
#[cfg(test)]
pub(crate) use sn_interface::network_knowledge::test_utils::gen_section_authority_provider;

pub(crate) use self::core::MIN_LEVEL_WHEN_FULL;


/// Information and state of our node
#[derive(Clone, custom_debug::Debug)]
pub(crate) struct NodeInfo {
    // Keep the secret key in Arc to allow Clone while also preventing multiple copies to exist in
    // memory which might be insecure.
    #[debug(skip)]
    pub(crate) keypair: Arc<Keypair>,
    pub(crate) addr: SocketAddr,
}

impl NodeInfo {
    pub(crate) fn new(keypair: Keypair, addr: SocketAddr) -> Self {
        Self {
            keypair: Arc::new(keypair),
            addr,
        }
    }

    pub(crate) fn peer(&self) -> Peer {
        Peer::new(self.name(), self.addr)
    }

    pub(crate) fn name(&self) -> XorName {
        XorName::from(PublicKey::from(self.keypair.public))
    }

    // Last byte of the name represents the age.
    pub(crate) fn age(&self) -> u8 {
        self.name()[XOR_NAME_LEN - 1]
    }
}

impl Display for NodeInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

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

#[cfg(test)]
mod tests {
    use super::supermajority;
    use proptest::prelude::*;

    #[test]
    fn supermajority_of_small_group() {
        assert_eq!(supermajority(0), 1);
        assert_eq!(supermajority(1), 1);
        assert_eq!(supermajority(2), 2);
        assert_eq!(supermajority(3), 3);
        assert_eq!(supermajority(4), 3);
        assert_eq!(supermajority(5), 4);
        assert_eq!(supermajority(6), 5);
        assert_eq!(supermajority(7), 5);
        assert_eq!(supermajority(8), 6);
        assert_eq!(supermajority(9), 7);
    }

    proptest! {
        #[test]
        fn proptest_supermajority(a in 0usize..10000) {
            let n = 3 * a;
            assert_eq!(supermajority(n),     2 * a + 1);
            assert_eq!(supermajority(n + 1), 2 * a + 1);
            assert_eq!(supermajority(n + 2), 2 * a + 2);
        }
    }
}
