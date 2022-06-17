// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node};

use sn_interface::{
    messaging::system::{JoinResponse, SectionAuth, SystemMsg},
    network_knowledge::NodeState,
    types::log_markers::LogMarker,
};

impl Node {
    // Send `NodeApproval` to a joining node which makes it a section member
    pub(crate) fn send_node_approval(&self, node_state: SectionAuth<NodeState>) -> Vec<Cmd> {
        let peer = *node_state.peer();
        let prefix = self.network_knowledge.prefix();
        info!("Our section with {:?} has approved peer {}.", prefix, peer,);

        let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Approval {
            genesis_key: *self.network_knowledge.genesis_key(),
            section_auth: self
                .network_knowledge
                .section_signed_authority_provider()
                .into_authed_msg(),
            node_state: node_state.into_authed_msg(),
            section_chain: self.network_knowledge.section_chain(),
        }));

        let dst_section_pk = self.network_knowledge.section_key();
        trace!("{}", LogMarker::SendNodeApproval);
        match self.send_direct_msg(peer, node_msg, dst_section_pk) {
            Ok(cmd) => vec![cmd],
            Err(err) => {
                error!("Failed to send join approval to node {}: {:?}", peer, err);
                vec![]
            }
        }
    }
}
