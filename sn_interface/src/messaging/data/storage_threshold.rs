// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use serde::{Deserialize, Serialize};

/// The threshold degree of storage used, before needing to split.
/// Expressed in values between 0-100, where each unit represents a percentage point.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageThreshold(u8);

///
impl StorageThreshold {
    /// The threshold level (90%).
    pub const THRESHOLD: u8 = 90;

    /// Creates a new instance.
    pub fn new() -> Self {
        Self(Self::THRESHOLD)
    }

    /// The value.
    pub fn value(&self) -> u8 {
        self.0
    }
}

impl Default for StorageThreshold {
    fn default() -> Self {
        Self::new()
    }
}
