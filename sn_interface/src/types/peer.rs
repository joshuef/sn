// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    net::SocketAddr,
};
use xor_name::{XorName, XOR_NAME_LEN};

const PRIMARY_NODE_AGE: u8 = 7;

/// A Peer with name, derived from its `PublicKey`, and an address.
#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Peer {
    name: XorName,
    addr: SocketAddr,
}

impl Peer {
    pub fn new(name: XorName, addr: SocketAddr) -> Self {
        Self { name, addr }
    }

    pub fn name(&self) -> XorName {
        self.name
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the age.
    pub fn age(&self) -> u8 {
        self.name[XOR_NAME_LEN - 1]
    }

    /// Returns if this node can be considered primary storage
    /// (pruimary storage being anything beyond the low age nodes which are much more
    /// likely to be churning, and so only perform backup operations)
    pub fn is_primary_node(&self) -> bool {
        self.age() >= PRIMARY_NODE_AGE
    }

    pub fn id(&self) -> (XorName, SocketAddr) {
        (self.name, self.addr)
    }

    pub fn from(addr: SocketAddr, public_key: ed25519_dalek::PublicKey) -> Peer {
        Peer {
            addr,
            name: XorName::from(super::PublicKey::from(public_key)),
        }
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{} at {}", self.name(), self.addr(),)
    }
}

impl Eq for Peer {}

impl Hash for Peer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.addr().hash(state);
    }
}

impl Ord for Peer {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name()
            .cmp(&other.name())
            .then_with(|| self.addr().cmp(&other.addr()))
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.addr() == other.addr()
    }
}

impl PartialEq<&Self> for Peer {
    fn eq(&self, other: &&Self) -> bool {
        self.name() == other.name() && self.addr() == other.addr()
    }
}

impl PartialOrd for Peer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
