// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::dbs;
use crate::messaging::{data::Error as ErrorMessage, MessageId};
use crate::routing::Prefix;
use crate::types::{convert_dt_error_to_error_message, DataAddress, PublicKey};
use std::io;
use thiserror::Error;
use xor_name::XorName;

/// Specialisation of `std::Result` for Node.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
#[non_exhaustive]
/// Node error variants.
pub enum Error {
    /// Attempted to perform an operation meant only for Adults when we are not one.
    #[error("Attempted Adult operation when not an Adult")]
    NotAnAdult,
    /// Attempted to perform an operation meant only for Elders when we are not one.
    #[error("Attempted Elder operation when not an Elder")]
    NotAnElder,
    /// Not enough in the section to perform Chunk operation
    #[error("Not enough Adults available in Section({0:?}) to perform operation")]
    NoAdults(Prefix),
    /// Database error.
    #[error("Database error:: {0}")]
    Database(#[from] dbs::Error),
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing")]
    NoSectionPublicKey,
    /// Not Section PublicKeySet.
    #[error("Not section public key set returned from routing")]
    NoSectionPublicKeySet,
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing for xorname {0}")]
    NoSectionPublicKeyKnown(XorName),
    /// Key, Value pair not found.
    #[error("No such data: {0:?}")]
    NoSuchData(DataAddress),
    /// Creating temp directory failed.
    #[error("Could not create temp store: {0}")]
    TempDirCreationFailed(String),
    /// Chunk already exists for this node
    #[error("Data already exists at this node")]
    DataExists,
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// JSON serialisation error.
    #[error("JSON serialisation error:: {0}")]
    JsonSerialisation(#[from] serde_json::Error),
    /// Bincode error.
    #[error("Bincode error:: {0}")]
    Bincode(#[from] bincode::Error),
    /// Network service message error.
    #[error("Network service message error:: {0}")]
    ServiceMsg(#[from] crate::messaging::data::Error),
    /// Network service error.
    #[error("Service error:: {0:?}")]
    ServiceError(crate::messaging::data::ServiceError),
    /// Network message error.
    #[error("Network message error:: {0}")]
    Message(#[from] crate::messaging::Error),
    /// Network data error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] crate::types::Error),
    /// Routing error.
    #[error("Routing error:: {0}")]
    Routing(#[from] crate::routing::Error),
    /// Message is invalid.
    #[error("Message with id: '{0:?}' is invalid. {1}")]
    InvalidMessage(MessageId, String),
    /// Data owner provided is invalid.
    #[error("Provided PublicKey is not a valid owner. Provided PublicKey: {0}")]
    InvalidOwner(PublicKey),
    /// Operation is invalid, eg signing validation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    /// No mapping to sn_messages::Error could be found. Either we need a new error there, or we need to handle or convert this error before sending it as a message
    #[error("No mapping to sn_messages error is set up for this NodeError {0}")]
    NoErrorMapping(String),
    /// Logic error.
    #[error("Logic error: {0}")]
    Logic(String),
    /// Timeout when trying to join the network
    #[error("Timeout when trying to join the network")]
    JoinTimeout,
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),
    /// Sled error.
    #[error("Sled error:: {0}")]
    Sled(#[from] sled::Error),
}
