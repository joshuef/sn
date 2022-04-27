// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    data::{
        MetadataExchange, OperationId, QueryResponse, Result, StorageLevel,
    },
    EndUser, MsgId, ServiceAuth,
};

#[cfg(any(feature="chunks", feature="registers"))]
use crate::messaging::data::{DataCmd, DataQuery};

#[cfg(feature = "registers")]
use crate::types::register::{Entry, EntryHash, Permissions, Policy, Register, User};

use crate::types::PublicKey;
#[cfg(any(feature="chunks", feature="registers"))]
use crate::types::{
    ReplicatedData, ReplicatedDataAddress,
};


#[cfg(feature = "chunks")]
use crate::types::Chunk;

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use xor_name::XorName;

/// cmd message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeCmd {
    /// Metadata is handled by Elders
    Metadata {
        /// The contained cmd
        cmd: DataCmd,
        /// Requester pk and signature
        auth: ServiceAuth,
        /// Message source
        origin: EndUser,
    },
    /// Notify Elders on nearing max capacity
    RecordStorageLevel {
        /// Node Id
        node_id: PublicKey,
        /// Section to which the message needs to be sent to. (NB: this is the section of the node id).
        section: XorName,
        /// The storage level reported by the node.
        level: StorageLevel,
    },
    #[cfg(any(feature="chunks", feature="registers"))]
    /// Tells an Adult to store a replica of the data
    ReplicateData(Vec<ReplicatedData>),
    #[cfg(any(feature="chunks", feature="registers"))]
    /// Tells an Adult to fetch and replicate data from the sender
    SendReplicateDataAddress(Vec<ReplicatedDataAddress>),
    #[cfg(any(feature="chunks", feature="registers"))]
    /// Fetch the given replicated data we are missing
    FetchReplicateData(Vec<ReplicatedDataAddress>),
    /// Sent to all promoted nodes (also sibling if any) after
    /// a completed transition to a new constellation.
    ReceiveMetadata {
        /// Metadata
        metadata: MetadataExchange,
    },
}

/// Event message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// Sent by a full Adult, and tells the Elders to store a chunk at some other Adult in the section
    CouldNotStoreData {
        /// Node Id
        node_id: PublicKey,
        /// The data that the Adult couldn't store
        data: ReplicatedData,
        /// Whether store failed due to full
        full: bool,
    },
    /// Inform Adults of a possible suspect node
    SuspiciousNodesDetected(BTreeSet<XorName>),
}

/// Query originating at a node
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum NodeQuery {
    /// Metadata is handled by Elders
    Metadata {
        /// The actual query message
        query: DataQuery,
        /// Client signature
        auth: ServiceAuth,
        /// The user that has initiated this query
        origin: EndUser,
    },
    /// Data is handled by Adults
    Data {
        /// The query
        query: DataQuery,
        /// Client signature
        auth: ServiceAuth,
        /// The user that has initiated this query
        origin: EndUser,
        /// The correlation id that recorded in Elders for this query
        correlation_id: MsgId,
    },
}

/// Responses to queries from Elders to Adults.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeQueryResponse {
    #[cfg(feature = "chunks")]
    //
    // ===== Chunk =====
    //
    /// Response to [`ChunkRead::Get`].
    GetChunk(Result<Chunk>),
    #[cfg(feature = "registers")]
    //
    // ===== Register Data =====
    //
    /// Response to [`RegisterQuery::Get`].
    GetRegister((Result<Register>, OperationId)),
    #[cfg(feature = "registers")]
    /// Response to [`RegisterQuery::GetOwner`].
    GetRegisterOwner((Result<User>, OperationId)),
    #[cfg(feature = "registers")]
    /// Response to [`RegisterQuery::GetEntry`].
    GetRegisterEntry((Result<Entry>, OperationId)),
    #[cfg(feature = "registers")]
    /// Response to [`RegisterQuery::GetPolicy`].
    GetRegisterPolicy((Result<Policy>, OperationId)),
    #[cfg(feature = "registers")]
    /// Response to [`RegisterQuery::Read`].
    ReadRegister((Result<BTreeSet<(EntryHash, Entry)>>, OperationId)),
    #[cfg(feature = "registers")]
    /// Response to [`RegisterQuery::GetUserPermissions`].
    GetRegisterUserPermissions((Result<Permissions>, OperationId)),
    //
    // ===== Other =====
    //
    /// Failed to create id generation
    FailedToCreateOperationId,
}

impl NodeQueryResponse {
    pub fn convert(self) -> QueryResponse {
        use NodeQueryResponse::*;
        match self {
            #[cfg(feature = "chunks")]
            GetChunk(res) => QueryResponse::GetChunk(res),
            #[cfg(feature = "registers")]
            GetRegister(res) => QueryResponse::GetRegister(res),
            #[cfg(feature = "registers")]
            GetRegisterEntry(res) => QueryResponse::GetRegisterEntry(res),
            #[cfg(feature = "registers")]
            GetRegisterOwner(res) => QueryResponse::GetRegisterOwner(res),
            #[cfg(feature = "registers")]
            ReadRegister(res) => QueryResponse::ReadRegister(res),
            #[cfg(feature = "registers")]
            GetRegisterPolicy(res) => QueryResponse::GetRegisterPolicy(res),
            #[cfg(feature = "registers")]
            GetRegisterUserPermissions(res) => QueryResponse::GetRegisterUserPermissions(res),
            FailedToCreateOperationId => QueryResponse::FailedToCreateOperationId,
        }
    }

    pub fn operation_id(&self) -> Result<OperationId> {
        self.clone().convert().operation_id()
    }
}
