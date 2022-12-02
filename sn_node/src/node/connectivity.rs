// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::cmds::Cmd, MyNode, Proposal, Result};
use sn_dysfunction::IssueType;
use std::{collections::BTreeSet, net::SocketAddr};
use xor_name::XorName;

impl MyNode {
    /// Track comms issue if this is a peer we know and care about
    pub(crate) fn handle_failed_send(&self, addr: &SocketAddr) {
        let name = if let Some(peer) = self.network_knowledge.find_member_by_addr(addr) {
            debug!("Lost known peer {}", peer);
            peer.name()
        } else {
            trace!("Lost unknown peer {}", addr);
            return;
        };

        self.log_node_issue(name, IssueType::Communication);
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
                if let Ok(cmds) =
                    self.send_proposal(elders.clone(), Proposal::VoteNodeOffline(info))
                {
                    result.extend(cmds);
                }
            }
        }
        Ok(result)
    }
}
