// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::types::register::User;
use crate::types::DataAddress;
use crate::types::PublicKey;
use serde::{Deserialize, Serialize};
use std::result;
use thiserror::Error;
use xor_name::{Prefix, XorName};

/// A specialised `Result` type.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Errors that can occur when interactive with client messaging APIs.
#[derive(Error, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// Access denied for user
    #[error("Access denied for user: {0:?}")]
    AccessDenied(User),
    /// Requested data not found
    #[error("Requested chunk not found: {0:?}")]
    ChunkNotFound(XorName),
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
    #[error("Data provided already exists")]
    DataExists,
    /// Entry could not be found on the data
    #[error("Requested entry not found")]
    NoSuchEntry,
    /// Key does not exist
    #[error("Key does not exist")]
    NoSuchKey,
    /// The list of owner keys is invalid
    #[error("Invalid owner key: {0}")]
    InvalidOwner(PublicKey),
    /// Invalid Operation such as a POST on ImmutableData
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    /// There was an error forming the OperationId
    #[error("Operation id could not be derived.")]
    NoOperationId,
    /// Node failed to delete the requested data for some reason.
    #[error("Failed to delete requested data")]
    FailedToDelete,
    /// Error is not valid for operation id generation. This should not absolve a pending (and thus far unfulfilled) operation
    #[error(
        "Could not generation operation id for chunk retrieval. Error was not 'DataNotFound'."
    )]
    InvalidQueryResponseErrorForOperationId,
    /// Destination is either outdated or incorrect
    #[error("Destination is either outdated or wrong")]
    WrongDestination,
}
