// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{register::RegisterQuery, spentbook::SpentbookQuery, Error, QueryResponse};
use crate::types::{ChunkAddress, DataAddress, SpentbookAddress};
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// A query for requesting data at a particular node.
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub struct DataQuery {
    /// The actual query, e.g. retrieving a Chunk or Register
    pub variant: DataQueryVariant,
    /// nth closest node (XOR distance) to query for data
    pub node_index: usize,
}

/// Data queries - retrieving data and inspecting their structure.
///
/// See the [`types`] module documentation for more details of the types supported by the Safe
/// Network, and their semantics.
///
/// [`types`]: crate::types
#[allow(clippy::large_enum_variant)]
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub enum DataQueryVariant {
    /// Retrieve a [`Chunk`] at the given address.
    ///
    /// This should eventually lead to a [`GetChunk`] response.
    ///
    /// [`Chunk`]:  crate::types::Chunk
    /// [`GetChunk`]: QueryResponse::GetChunk
    GetChunk(ChunkAddress),
    /// [`Register`] read operation.
    ///
    /// [`Register`]: crate::types::register::Register
    Register(RegisterQuery),
    /// Spentbook read operation.
    Spentbook(SpentbookQuery),
}

impl DataQueryVariant {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn to_error_response(&self, error: Error) -> QueryResponse {
        use DataQueryVariant::*;
        match self {
            GetChunk(_) => QueryResponse::GetChunk(Err(error)),
            Register(q) => q.to_error_response(error),
            Spentbook(q) => q.to_error_response(error),
        }
    }

    /// Returns the xorname of the data destination for `request`.
    pub fn dst_name(&self) -> XorName {
        use DataQueryVariant::*;
        match self {
            GetChunk(address) => *address.name(),
            Register(q) => q.dst_name(),
            Spentbook(q) => q.dst_name(),
        }
    }

    /// Returns the address of the data
    pub fn address(&self) -> DataAddress {
        match self {
            Self::GetChunk(address) => DataAddress::Bytes(*address),
            Self::Register(read) => DataAddress::Register(read.dst_address()),
            Self::Spentbook(read) => {
                DataAddress::Spentbook(SpentbookAddress::new(*read.dst_address().name()))
            }
        }
    }
}
