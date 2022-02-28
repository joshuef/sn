// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::messaging::data::Error as ErrorMsg;
use sn_interface::types::{convert_dt_error_to_error_msg, DataAddress, PublicKey};
use std::io;
use thiserror::Error;
use xor_name::XorName;

/// Specialisation of `std::Result` for dbs.
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
#[non_exhaustive]
/// Node error variants.
pub enum Error {
    /// Db key conversion failed
    #[error("Could not convert the Db key")]
    CouldNotConvertDbKey,
    /// Db key conversion failed
    #[error("Could not decode the Db key: {0:?}")]
    CouldNotDecodeDbKey(String),
    /// Not enough space to store the value.
    #[error("Not enough space")]
    NotEnoughSpace,
    /// Key not found.
    #[error("Key not found: {0:?}")]
    KeyNotFound(String),
    /// Key, Value pair not found.
    #[error("No value found for key: {0:?}")]
    NoSuchValue(String),
    /// Data id not found.
    #[error("Data id not found: {0:?}")]
    DataIdNotFound(DataAddress),
    /// Cannot delete public data
    #[error("Cannot delete public data {0:?}")]
    CannotDeletePublicData(DataAddress),
    /// Data not found.
    #[error("No such data: {0:?}")]
    NoSuchData(DataAddress),
    /// Chunk not found.
    #[error("Chunk not found: {0:?}")]
    ChunkNotFound(XorName),
    /// Chunk already exists for this node
    #[error("Data already exists at this node")]
    DataExists,
    /// Data owner provided is invalid.
    #[error("Provided PublicKey is not a valid owner. Provided PublicKey: {0}")]
    InvalidOwner(PublicKey),
    /// Invalid store found
    #[error("A KV store was loaded, but found to be invalid")]
    InvalidStore,
    /// Data owner provided is invalid.
    #[error("Provided PublicKey could not validate signature {0:?}")]
    InvalidSignature(PublicKey),
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialize(String),
    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialize(String),
    /// Creating temp directory failed.
    #[error("Could not create temp store: {0}")]
    TempDirCreationFailed(String),
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Bincode error.
    #[error("Bincode error:: {0}")]
    Bincode(#[from] bincode::Error),
    ///Db key parse error.
    #[error("Could not parse key:: {0:?}")]
    CouldNotParseDbKey(Vec<u8>),
    ///Operation Id could not be generated
    #[error("Operation Id could not be generated")]
    NoOperationId,
    /// Sled error.
    #[error("Sled error:: {0}")]
    Sled(#[from] sled::Error),
    /// There were Error(s) while batching for Sled operations.
    #[error("Errors found when batching for Sled")]
    SledBatching,
    /// Invalid filename
    #[error("Invalid chunk filename")]
    InvalidFilename,
    /// NetworkData error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] sn_interface::types::Error),
    /// No filename found
    #[error("Path contains no file name")]
    NoFilename,
}

/// Convert db error to messaging error message for sending over the network.
pub(crate) fn convert_to_error_msg(error: Error) -> ErrorMsg {
    match error {
        Error::NotEnoughSpace => ErrorMsg::FailedToWriteFile,
        Error::DataIdNotFound(address) => ErrorMsg::DataNotFound(address),
        Error::NoSuchData(address) => ErrorMsg::DataNotFound(address),
        Error::ChunkNotFound(xorname) => ErrorMsg::ChunkNotFound(xorname),
        Error::TempDirCreationFailed(_) => ErrorMsg::FailedToWriteFile,
        Error::DataExists => ErrorMsg::DataExists,
        Error::NetworkData(error) => convert_dt_error_to_error_msg(error),
        other => ErrorMsg::InvalidOperation(format!("Failed to perform operation: {:?}", other)),
    }
}
