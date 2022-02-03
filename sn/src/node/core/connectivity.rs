// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::command::Command,
    core::{Core, Proposal},
    Result,
};
use std::{collections::BTreeSet, iter, net::SocketAddr};
use xor_name::XorName;

impl Core {
    pub(crate) async fn handle_peer_lost(&self, addr: &SocketAddr) -> Result<Vec<Command>> {
        let name = if let Some(peer) = self.network_knowledge.find_member_by_addr(addr).await {
            debug!("Lost known peer {}", peer);
            peer.name()
        } else {
            trace!("Lost unknown peer {}", addr);
            return Ok(vec![]);
        };

        if self.is_not_elder().await {
            // Adults cannot complain about connectivity.
            return Ok(vec![]);
        }

        let mut commands = self.propose_offline(name).await?;
        commands.push(Command::StartConnectivityTest(name));
        Ok(commands)
    }

    pub(crate) async fn propose_offline(&self, name: XorName) -> Result<Vec<Command>> {
        self.cast_offline_proposals(&iter::once(name).collect())
            .await
    }

    pub(crate) async fn cast_offline_proposals(
        &self,
        names: &BTreeSet<XorName>,
    ) -> Result<Vec<Command>> {
        // Don't send the `Offline` proposal to the peer being lost as that send would fail,
        // triggering a chain of further `Offline` proposals.
        let elders: Vec<_> = self
            .network_knowledge
            .authority_provider()
            .await
            .elders()
            .filter(|peer| !names.contains(&peer.name()))
            .cloned()
            .collect();
        let mut result: Vec<Command> = Vec::new();
        for name in names.iter() {
            if let Some(info) = self.network_knowledge.get_section_member(name).await {
                let info = info.leave()?;
                if let Ok(commands) = self
                    .send_proposal(elders.clone(), Proposal::Offline(info))
                    .await
                {
                    result.extend(commands);
                }
            }
        }
        Ok(result)
    }
}
