// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::OperationId;
use crate::messaging::{
    data::{DataQueryVariant, QueryResponse, StorageThreshold},
    ClientAuth,
};
use crate::types::{DataAddress, PublicKey, ReplicatedData};

use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// cmd message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeDataCmd {
    /// Tells an Adult to store a data
    ReplicateOneData(ReplicatedData),
    /// Tells an Adult to store a replica of some data set
    ReplicateDataBatch(Vec<ReplicatedData>),
    /// Tells an Adult to fetch and replicate data from the sender
    SendAnyMissingRelevantData(Vec<DataAddress>),
}

/// Event message sent among nodes
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    /// Sent by a full Adult, and tells the Elders to store a chunk at some other Adult in the section
    CouldNotStoreData {
        /// Node Id
        node_id: PublicKey,
        /// The data that the Adult couldn't store
        data: ReplicatedData,
        /// Whether store failed due to full
        full: bool,
    },
    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// Notify Elders on nearing max capacity
    StorageThresholdReached {
        /// Node Id
        node_id: PublicKey,
        /// Section to which the message needs to be sent to. (NB: this is the section of the node id).
        section: XorName,
        /// The storage level reported by the node.
        level: StorageThreshold,
    },
}

/// Query originating at a node
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub struct NodeDataQuery {
    /// Data is handled by Adults
    /// The query
    pub query: DataQueryVariant,
    /// Client signature
    pub auth: ClientAuth,
    /// The operation id that recorded in Elders for this query
    pub operation_id: OperationId,
}

/// Responses to queries sent from Elders to Adults.
/// We define it as an alias to `QueryResponse` type, but we keep it as
/// a separate system message type for more clarity in logs and messaging tracking/debugging.
pub type NodeQueryResponse = QueryResponse;
