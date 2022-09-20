// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::types::{register::User, DataAddress};
use serde::{Deserialize, Serialize};
use std::result;
use thiserror::Error;
use xor_name::Prefix;

/// A specialised `Result` type.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Errors that can occur when interactive with client messaging APIs.
#[derive(Error, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Error {
    /// Access denied for user
    #[error("Access denied for user: {0:?}")]
    AccessDenied(User),
    /// Requested data not found
    #[error("Requested data not found: {0:?}")]
    DataNotFound(DataAddress),
    /// Failed to write file, likely due to a system Io error
    #[error("Failed to write file")]
    FailedToWriteFile,
    /// Insufficient Adults found to store data
    #[error("Failed to store data. Insufficient replication count at section {prefix:?}. Expected {expected}, found {found}.")]
    InsufficientAdults {
        /// The prefix of the section.
        prefix: Prefix,
        /// Expected number of Adults for minimum replication.
        expected: u8,
        /// Actual number of Adults found to hold the data.
        found: u8,
    },
    /// Provided data already exists on the network
    #[error("Data provided already exists: {0:?}")]
    DataExists(DataAddress),
    /// Entry could not be found on the data
    #[error("Requested entry not found")]
    NoSuchEntry,
    /// Invalid Operation such as a POST on ImmutableData
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    /// Failed to verify a spent proof since it's signed by unknown section key
    #[error("Spent proof was signed with unknown section key: {0:?}")]
    SpentProofUnknownSectionKey(bls::PublicKey),
}
