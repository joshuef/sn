// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Formatter};
use tiny_keccak::{Hasher, Sha3};

/// Id of an operation. Node to node query/response should return the same id for simple
/// nodes tracking purposes.
#[derive(Deserialize, Serialize, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct OperationId(pub [u8; 32]);

impl Display for OperationId {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(
            fmt,
            "OpId-{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2]
        )
    }
}

impl Debug for OperationId {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl OperationId {
    /// Creates an operation id by hashing the provided bytes
    pub fn from(bytes: &Bytes) -> Self {
        let mut hasher = Sha3::v256();
        let mut output = [0; 32];
        hasher.update(bytes);
        hasher.finalize(&mut output);

        Self(output)
    }

    /// Creates a random operation id
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        Self(rng.gen())
    }
}
