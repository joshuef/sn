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

/// A query for requesting (meta)data at a particular adult.
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub struct DataQuery {
    /// The actual query, e.g. retrieving a Chunk or Register
    pub variant: DataQueryVariant,
    /// nth closest adult (XOR distance) to query for data
    pub adult_index: usize,
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
    #[cfg(feature = "chunks")]
    /// Retrieve a [`Chunk`] at the given address.
    ///
    /// This should eventually lead to a [`GetChunk`] response.
    ///
    /// [`Chunk`]:  crate::types::Chunk
    /// [`GetChunk`]: QueryResponse::GetChunk
    GetChunk(ChunkAddress),
    #[cfg(feature = "registers")]
    /// [`Register`] read operation.
    ///
    /// [`Register`]: crate::types::register::Register
    Register(RegisterQuery),
    #[cfg(feature = "spentbook")]
    /// Spentbook read operation.
    Spentbook(SpentbookQuery),
}

impl DataQueryVariant {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> QueryResponse {
        use DataQueryVariant::*;
        match self {
            #[cfg(feature = "chunks")]
            GetChunk(_) => QueryResponse::GetChunk(Err(error)),
            #[cfg(feature = "registers")]
            Register(q) => q.error(error),
            #[cfg(feature = "spentbook")]
            Spentbook(q) => q.error(error),
        }
    }

    /// Returns the xorname of the data destination for `request`.
    pub fn dst_name(&self) -> XorName {
        use DataQueryVariant::*;
        match self {
            #[cfg(feature = "chunks")]
            GetChunk(address) => *address.name(),
            #[cfg(feature = "registers")]
            Register(q) => q.dst_name(),
            #[cfg(feature = "spentbook")]
            Spentbook(q) => q.dst_name(),
        }
    }

    /// Returns the address of the data
    pub fn address(&self) -> DataAddress {
        match self {
            #[cfg(feature = "chunks")]
            Self::GetChunk(address) => DataAddress::Bytes(*address),
            #[cfg(feature = "registers")]
            Self::Register(read) => DataAddress::Register(read.dst_address()),
            #[cfg(feature = "spentbook")]
            Self::Spentbook(read) => {
                DataAddress::Spentbook(SpentbookAddress::new(*read.dst_address().name()))
            }
        }
    }
}
