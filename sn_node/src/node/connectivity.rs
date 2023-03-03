// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, MyNode, Result};

use sn_fault_detection::IssueType;
use sn_interface::types::Peer;

use std::collections::BTreeSet;
use xor_name::XorName;

impl MyNode {
    /// Handle error in communication with peer.
    pub(crate) fn handle_comms_error(context: NodeContext, peer: Peer, error: sn_comms::Error) {
        use sn_comms::Error::*;
        match error {
            ConnectingToUnknownNode(msg_id) => {
                trace!(
                    "Tried to send msg {msg_id:?} to unknown peer {}. No connection made.",
                    peer
                );
            }
            CannotConnectEndpoint(_err) => {
                trace!("Cannot connect to endpoint: {}", peer);
            }
            AddressNotReachable(_err) => {
                trace!("Address not reachable: {}", peer);
            }
            FailedSend(msg_id) => {
                trace!("Could not send {msg_id:?}, lost known peer: {}", peer);
            }
            InvalidMsgReceived(msg_id) => {
                trace!("Invalid msg {msg_id:?} received from {}.", peer);
            }
        }
        // Track comms issue if this is a peer we know and care about
        if context.network_knowledge.is_section_member(&peer.name()) {
            context.track_node_issue(peer.name(), IssueType::Communication);
        }
    }

    pub(crate) fn cast_offline_proposals(&mut self, names: &BTreeSet<XorName>) -> Result<Vec<Cmd>> {
        // Don't send the `Offline` proposal to the peer being lost as that send would fail,
        // triggering a chain of further `Offline` proposals.
        let elders: Vec<_> = self
            .network_knowledge
            .section_auth()
            .elders()
            .filter(|peer| !names.contains(&peer.name()))
            .cloned()
            .collect();
        let mut result: Vec<Cmd> = Vec::new();
        for name in names.iter() {
            if let Some(info) = self.network_knowledge.get_section_member(name) {
                let info = info.leave()?;
                if let Ok(cmds) = self.send_node_off_proposal(elders.clone(), info) {
                    result.extend(cmds);
                }
            }
        }
        Ok(result)
    }
}
