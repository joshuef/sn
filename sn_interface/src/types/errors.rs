// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{register::User, RegisterAddress};

#[cfg(any(feature = "chunks", feature = "registers"))]
use crate::messaging::data::Error as ErrorMsg;

use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
    result,
};
use thiserror::Error;

/// A specialised `Result` type for types crate.
pub type Result<T> = result::Result<T, Error>;

/// Error debug struct
struct ErrorDebug<'a, T>(&'a Result<T>);

impl<'a, T> Debug for ErrorDebug<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Err(error) = self.0 {
            write!(f, "{:?}", error)
        } else {
            write!(f, "Success")
        }
    }
}

/// Main error type for the crate.
#[derive(Error, Debug, Clone, PartialEq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// Access denied for user
    #[error("Access denied for user: {0:?}")]
    AccessDenied(User),
    /// Serialization error
    #[error("Serialisation error: {0}")]
    Serialisation(String),
    /// Error creating File at given path
    #[error("File Creation error: {0}")]
    FileHandling(String),
    /// Error creating Directory  at given path
    #[error("Directory Creation error: {0}")]
    DirectoryHandling(String),
    /// Entry already exists. Contains the current entry Key.
    #[error("Entry already exists {0}")]
    EntryExists(u8),
    /// Entry is too big to fit inside a register
    #[error("Entry is too big to fit inside a register: {0}, max: {1}")]
    EntryTooBig(usize, usize),
    /// Cannot add another entry since the register entry cap has been reached.
    #[error("Cannot add another entry since the register entry cap has been reached: {0}")]
    TooManyEntries(usize),
    /// Supplied actions are not valid
    #[error("Some entry actions are not valid")]
    InvalidEntryActions(BTreeMap<Vec<u8>, Error>),
    /// Entry could not be found on the data
    #[error("Requested entry not found")]
    NoSuchEntry,
    /// Key does not exist
    #[error("Key does not exist")]
    NoSuchKey,
    /// Owner is not valid
    #[error("Owner is not a PublicKeySet")]
    InvalidOwnerNotPublicKeySet,
    /// No Policy has been set to the data
    #[error("No policy has been set for this data")]
    PolicyNotSet,
    /// Invalid version for performing a given mutating operation. Contains the
    /// current data version.
    #[error("Invalid version provided: {0}")]
    InvalidSuccessor(u64),
    /// Invalid mutating operation as it causality dependency is currently not satisfied
    #[error("Operation is not causally ready. Ensure you have the full history of operations.")]
    OpNotCausallyReady,
    /// Invalid Operation such as a POST on ImmutableData
    #[error("Invalid operation")]
    InvalidOperation,
    /// Mismatch between key type and signature type.
    #[error("Sign key and signature type do not match")]
    SigningKeyTypeMismatch,
    /// Failed signature validation.
    #[error("Invalid signature")]
    InvalidSignature,
    /// While parsing, precision would be lost.
    #[error("Lost precision on the number of coins during parsing")]
    LossOfPrecision,
    /// The amount would exceed the maximum value for `Token` (u64::MAX).
    #[error("The token amount would exceed the maximum value (u64::MAX)")]
    ExcessiveValue,
    /// Failed to parse a string.
    #[error("Failed to parse: {0}")]
    FailedToParse(String),
    /// Inexistent recipient balance.
    // TODO: this should not be possible
    #[error("No such recipient key balance")]
    NoSuchRecipient,
    #[error("No matching Section")]
    NoMatchingSection,
    /// Expected data size exceeded.
    #[error("Size of the structure exceeds the limit")]
    ExceededSize,
    /// Number out of expected range.
    #[error("The provided number is out of the expected range")]
    OutOfRange,
    /// The operation has not been signed by an actor PK and so cannot be validated.
    #[error("CRDT operation missing actor signature")]
    CrdtMissingOpSignature,
    /// The data for a given policy could not be located, so CRDT operations cannot be applied.
    #[error("CRDT data is in an unexpected and/or inconsistent state. No data found for current policy.")]
    CrdtUnexpectedState,
    /// The CRDT operation cannot be applied as it targets a different content address.
    #[error("The CRDT operation cannot be applied as it targets a different content address.")]
    CrdtWrongAddress(RegisterAddress),
    #[error("Section authority provider cannot be trusted: {0}")]
    UntrustedSectionAuthProvider(String),
    #[error("Proof chain cannot be trusted: {0}")]
    UntrustedProofChain(String),
}

pub fn convert_bincode_error(err: bincode::Error) -> Error {
    Error::Serialisation(err.as_ref().to_string())
}

/// Convert type errors to messaging::Errors for sending scross the network
#[cfg(any(feature = "chunks", feature = "registers"))]
pub fn convert_dt_error_to_error_msg(error: Error) -> ErrorMsg {
    match error {
        Error::InvalidOperation => {
            ErrorMsg::InvalidOperation("DtError::InvalidOperation".to_string())
        }
        Error::NoSuchEntry => ErrorMsg::NoSuchEntry,
        Error::AccessDenied(pk) => ErrorMsg::AccessDenied(pk),
        other => ErrorMsg::InvalidOperation(format!("DtError: {:?}", other)),
    }
}
