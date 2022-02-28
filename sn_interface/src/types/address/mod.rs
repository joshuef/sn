// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod bytes;
mod register;
mod safe_key;

#[allow(unreachable_pub)]
pub use self::bytes::BytesAddress;
#[allow(unreachable_pub)]
pub use register::RegisterAddress;
#[allow(unreachable_pub)]
pub use safe_key::SafeKeyAddress;

use super::{utils, Result};
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// We also encode the data scope - i.e. accessibility on the SAFE Network.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub enum Scope {
    #[allow(missing_docs)]
    Public = 0x00,
    #[allow(missing_docs)]
    Private = 0x01,
}

/// An address of data on the network
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub enum DataAddress {
    ///
    SafeKey(SafeKeyAddress),
    ///
    Bytes(BytesAddress),
    ///
    Register(RegisterAddress),
}

impl DataAddress {
    /// The xorname.
    pub fn name(&self) -> &XorName {
        match self {
            Self::SafeKey(address) => address.name(),
            Self::Bytes(address) => address.name(),
            Self::Register(address) => address.name(),
        }
    }

    /// The address scope
    pub fn scope(&self) -> Scope {
        if self.is_public() {
            Scope::Public
        } else {
            Scope::Private
        }
    }

    /// Returns true if public.
    pub fn is_public(self) -> bool {
        match self {
            Self::SafeKey(address) => address.is_public(),
            Self::Bytes(address) => address.is_public(),
            Self::Register(address) => address.is_public(),
        }
    }

    /// Returns true if private.
    pub fn is_private(self) -> bool {
        !self.is_public()
    }

    /// Returns the Address serialised and encoded in z-base-32.
    pub fn encode_to_zbase32(&self) -> Result<String> {
        utils::encode(&self)
    }

    /// Creates from z-base-32 encoded string.
    pub fn decode_from_zbase32<T: AsRef<str>>(encoded: T) -> Result<Self> {
        utils::decode(encoded)
    }

    ///
    pub fn register(name: XorName, scope: Scope, tag: u64) -> DataAddress {
        DataAddress::Register(RegisterAddress::new(name, scope, tag))
    }

    ///
    pub fn bytes(name: XorName, scope: Scope) -> DataAddress {
        DataAddress::Bytes(BytesAddress::new(name, scope))
    }

    ///
    pub fn safe_key(name: XorName, scope: Scope) -> DataAddress {
        DataAddress::SafeKey(SafeKeyAddress::new(name, scope))
    }
}

/// An address of data on the network
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub enum ReplicatedDataAddress {
    ///
    Chunk(ChunkAddress),
    ///
    Register(RegisterAddress),
}

impl ReplicatedDataAddress {
    /// The xorname.
    pub fn name(&self) -> &XorName {
        match self {
            Self::Chunk(address) => address.name(),
            Self::Register(address) => address.name(),
        }
    }

    ///
    pub fn register(name: XorName, scope: Scope, tag: u64) -> ReplicatedDataAddress {
        ReplicatedDataAddress::Register(RegisterAddress::new(name, scope, tag))
    }

    ///
    pub fn chunk(name: XorName) -> ReplicatedDataAddress {
        ReplicatedDataAddress::Chunk(ChunkAddress(name))
    }
}

/// Address of a Chunk.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct ChunkAddress(pub XorName);

impl ChunkAddress {
    /// Returns the name.
    pub fn name(&self) -> &XorName {
        &self.0
    }

    /// Returns the Address serialised and encoded in z-base-32.
    pub fn encode_to_zbase32(&self) -> Result<String> {
        utils::encode(&self)
    }

    /// Creates from z-base-32 encoded string.
    pub fn decode_from_zbase32<T: AsRef<str>>(encoded: T) -> Result<Self> {
        utils::decode(encoded)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{BytesAddress, DataAddress, Result};
    use xor_name::XorName;

    #[test]
    fn zbase32_encode_decode_chunk_address() -> Result<()> {
        let name = XorName::random();
        let address = DataAddress::Bytes(BytesAddress::Public(name));
        let encoded = address.encode_to_zbase32()?;
        let decoded = DataAddress::decode_from_zbase32(&encoded)?;
        assert_eq!(address, decoded);
        Ok(())
    }
}
