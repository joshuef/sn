// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{MyNode, Result};

use sn_interface::messaging::{
    data::ClientDataResponse,
    system::{NodeDataResponse, NodeMsg},
    MsgKind, WireMsg,
};

use bytes::Bytes;
use xor_name::XorName;

impl MyNode {
    /// Serialize a message for a Client
    pub(crate) fn serialize_client_msg_response(
        our_node_name: XorName,
        msg: &ClientDataResponse,
    ) -> Result<(MsgKind, Bytes)> {
        let payload = WireMsg::serialize_msg_payload(msg)?;
        let kind = MsgKind::ClientDataResponse(our_node_name);
        Ok((kind, payload))
    }

    /// Serialize a message for a Node
    pub(crate) fn serialize_node_msg(
        our_node_name: XorName,
        msg: &NodeMsg,
    ) -> Result<(MsgKind, Bytes)> {
        let payload = WireMsg::serialize_msg_payload(msg)?;
        let kind = MsgKind::Node {
            name: our_node_name,
            is_join: matches!(msg, NodeMsg::TryJoin),
        };
        Ok((kind, payload))
    }

    /// Serialize a message for a Node
    pub(crate) fn serialize_node_data_response(
        our_node_name: XorName,
        msg: &NodeDataResponse,
    ) -> Result<(MsgKind, Bytes)> {
        let payload = WireMsg::serialize_msg_payload(msg)?;
        let kind = MsgKind::NodeDataResponse(our_node_name);
        Ok((kind, payload))
    }
}
