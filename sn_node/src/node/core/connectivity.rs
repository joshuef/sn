// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{Node, Proposal},
    Result,
};
use std::{collections::BTreeSet, net::SocketAddr};
use xor_name::XorName;

impl Node {
    pub(crate) async fn handle_peer_lost(&self, addr: &SocketAddr) -> Result<Vec<Cmd>> {
        let name = if let Some(peer) = self.network_knowledge.find_member_by_addr(addr) {
            debug!("Lost known peer {}", peer);
            peer.name()
        } else {
            trace!("Lost unknown peer {}", addr);
            return Ok(vec![]);
        };

        if self.is_not_elder() {
            // Adults cannot complain about connectivity.
            return Ok(vec![]);
        }

        self.log_comm_issue(name).await?;
        let cmds = vec![Cmd::StartConnectivityTest(name)];
        Ok(cmds)
    }

    pub(crate) fn cast_offline_proposals(&self, names: &BTreeSet<XorName>) -> Result<Vec<Cmd>> {
        // Don't send the `Offline` proposal to the peer being lost as that send would fail,
        // triggering a chain of further `Offline` proposals.
        let elders: Vec<_> = self
            .network_knowledge
            .authority_provider()
            .elders()
            .filter(|peer| !names.contains(&peer.name()))
            .cloned()
            .collect();
        let mut result: Vec<Cmd> = Vec::new();
        for name in names.iter() {
            if let Some(info) = self.network_knowledge.get_section_member(name) {
                let info = info.leave()?;
                if let Ok(cmds) = self.send_proposal(elders.clone(), Proposal::Offline(info)) {
                    result.extend(cmds);
                }
            }
        }
        Ok(result)
    }
}
