// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::messaging::data::Error as ErrorMsg;
use sn_interface::types::convert_dt_error_to_error_msg;
use std::io;
use thiserror::Error;
use xor_name::XorName;

/// Specialisation of `std::Result` for dbs.
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
#[non_exhaustive]
/// Chunk Store error variants.
pub enum Error {
    /// Chunk not found.
    #[error("Chunk not found: {0:?}")]
    ChunkNotFound(XorName),
    /// Invalid filename
    #[error("Invalid chunk filename")]
    InvalidFilename,
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// NetworkData error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] use sn_interface::types::Error),
    /// No filename found
    #[error("Path contains no file name")]
    NoFilename,
    /// Not enough space to store the Chunk.
    #[error("Not enough space")]
    NotEnoughSpace,
}
