// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{
        bootstrap::JoiningAsRelocated,
        relocation::{find_nodes_to_relocate, ChurnId, RelocateDetailsUtils},
        Node, Proposal,
    },
    Event, Result,
};

use sn_interface::{
    elder_count,
    messaging::system::{MembershipState, NodeState as NodeStateMsg, RelocateDetails, SectionAuth},
    network_knowledge::NodeState,
    types::log_markers::LogMarker,
};

use std::collections::BTreeSet;
use xor_name::XorName;

// Relocation
impl Node {
    pub(crate) fn relocate_peers(
        &self,
        churn_id: ChurnId,
        excluded: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        // Do not carry out relocations in the first section
        // TODO: consider avoiding relocations in first 16 sections instead.
        if self.network_knowledge.prefix().is_empty() {
            return Ok(vec![]);
        }

        // Do not carry out relocation when there is not enough elder nodes.
        if self.network_knowledge.authority_provider().elder_count() < elder_count() {
            return Ok(vec![]);
        }

        let mut cmds = vec![];
        for (node_state, relocate_details) in
            find_nodes_to_relocate(&self.network_knowledge, &churn_id, excluded)
        {
            debug!(
                "Relocating {:?} to {} (on churn of {})",
                node_state.peer(),
                relocate_details.dst,
                churn_id
            );

            cmds.extend(self.propose(Proposal::Offline(node_state.relocate(relocate_details)))?);
        }

        Ok(cmds)
    }

    pub(crate) async fn relocate_rejoining_peer(
        &self,
        node_state: NodeState,
        age: u8,
    ) -> Result<Vec<Cmd>> {
        let peer = node_state.peer();
        let relocate_details =
            RelocateDetails::with_age(&self.network_knowledge, peer, peer.name(), age);

        trace!(
            "Relocating {:?} to {} with age {} due to rejoin",
            peer,
            relocate_details.dst,
            relocate_details.age
        );

        self.propose(Proposal::Offline(node_state.relocate(relocate_details)))
    }

    pub(crate) async fn handle_relocate(
        &self,
        relocate_proof: SectionAuth<NodeStateMsg>,
    ) -> Result<Option<Cmd>> {
        let (dst_xorname, dst_section_key, new_age) =
            if let MembershipState::Relocated(ref relocate_details) = relocate_proof.value.state {
                (
                    relocate_details.dst,
                    relocate_details.dst_section_key,
                    relocate_details.age,
                )
            } else {
                debug!(
                    "Ignoring Relocate msg containing invalid NodeState: {:?}",
                    relocate_proof.state
                );
                return Ok(None);
            };

        let node = self.info.borrow().clone();
        if dst_xorname != node.name() {
            // This `Relocate` message is not for us - it's most likely a duplicate of a previous
            // message that we already handled.
            return Ok(None);
        }

        debug!(
            "Received Relocate message to join the section at {}",
            dst_xorname
        );

        let we_have_relocate_state = { self.relocate_state.borrow().is_some() };
        if we_have_relocate_state {
            trace!("Ignore Relocate - relocation already in progress");
            return Ok(None);
        } else {
            trace!("{}", LogMarker::RelocateStart);
            self.send_event(Event::RelocationStarted {
                previous_name: node.name(),
            })
            .await;
        }

        // Create a new instance of JoiningAsRelocated to start the relocation
        // flow. This same instance will handle responses till relocation is complete.
        let genesis_key = *self.network_knowledge.genesis_key();

        let bootstrap_addrs = if let Ok(sap) = self.network_knowledge.section_by_name(&dst_xorname)
        {
            sap.addresses()
        } else {
            self.network_knowledge.authority_provider().addresses()
        };
        let (joining_as_relocated, cmd) = JoiningAsRelocated::start(
            node,
            genesis_key,
            relocate_proof,
            bootstrap_addrs,
            dst_xorname,
            dst_section_key,
            new_age,
        )?;

        *self.relocate_state.borrow_mut() = Some(Box::new(joining_as_relocated));

        Ok(Some(cmd))
    }
}
