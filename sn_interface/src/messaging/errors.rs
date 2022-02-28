// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::result;
use thiserror::Error;

/// A specialised `Result` type for messaging APIs.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Errors that can occur when interacting with messaging APIs.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred when serializing a message.
    // In reality, serialisation itself should be infallible, so this should only occur when
    // serialising involves I/O (e.g. serialising to `io::Write`). As such, we might want to replace
    // this with an `io::Error` variant, if it's ever applicable.
    #[error("Failed to serialize message: {0}")]
    Serialisation(String),

    /// Failed to parse a message.
    ///
    /// The `String` will be a human-readable indication of the problem with the message.
    #[error("Failed to parse: {0}")]
    FailedToParse(String),

    /// Signature verification failed.
    ///
    /// Wherever possible, signatures are verified when deserialising messages. This error is
    /// returned when deserialisation succeeded but the signature was invalid.
    #[error("Invalid signature")]
    InvalidSignature,

    /// Message read was built with an unsupported version.
    #[error("Unsupported messaging protocol version: {0}")]
    UnsupportedVersion(u16),
}
