// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::RegisterCmd;
use crate::{
    messaging::SectionAuth,
    types::{Error, RegisterAddress as Address, Result},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use xor_name::XorName;

/// Metadata (register and chunk holders) replication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataExchange {
    /// Adult storage levels.
    pub adult_levels: BTreeMap<XorName, StorageLevel>,
}

/// Data to be exchanged between Register stores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterStoreExport(pub Vec<ReplicatedRegisterLog>);

/// Register data exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedRegisterLog {
    ///
    pub address: Address,
    /// section sig over address.id()
    /// This is a duplicated entry as it should exist in first cmd
    pub section_auth: SectionAuth,
    ///
    pub op_log: Vec<RegisterCmd>,
}

/// The degree to which storage has been used.
/// Expressed in values between 0-10, where each unit represents 10-percentage points.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageLevel(u8);

///
impl StorageLevel {
    /// The maximum level (100%).
    pub const MAX: u8 = 10;

    /// Creates a new instance with level 0.
    pub fn zero() -> Self {
        Self(0)
    }

    /// Creates a new instance with provided level.
    /// Returns an OutOfRange error if the value is above StorageLevel::MAX.
    pub fn from(value: u8) -> Result<Self> {
        if value > Self::MAX {
            Err(Error::OutOfRange)
        } else {
            Ok(Self(value))
        }
    }

    /// The next level.
    /// Returns an OutOfRange error if called on an instance with StorageLevel::MAX value.
    pub fn next(&self) -> Result<StorageLevel> {
        StorageLevel::from(self.0 + 1)
    }

    /// The previous level.
    /// Returns an OutOfRange error if called on a StorageLevel with value 0.
    pub fn previous(&self) -> Result<StorageLevel> {
        if self.0 == 0 {
            Err(Error::OutOfRange)
        } else {
            StorageLevel::from(self.0 - 1)
        }
    }

    /// The current value.
    pub fn value(&self) -> u8 {
        self.0
    }
}
