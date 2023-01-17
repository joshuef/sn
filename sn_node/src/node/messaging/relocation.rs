// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::cmds::Cmd,
    relocated::JoiningAsRelocated,
    relocation::{find_nodes_to_relocate, ChurnId},
    MyNode, Result, SectionStateVote,
};

use sn_interface::{
    elder_count,
    messaging::system::SectionSigned,
    network_knowledge::{MembershipState, NodeState},
    types::log_markers::LogMarker,
};

use std::collections::BTreeSet;
use xor_name::XorName;

// Relocation
impl MyNode {
    pub(crate) fn relocate_peers(
        &mut self,
        churn_id: ChurnId,
        excluded: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        // Do not carry out relocations in the first section
        // TODO: consider avoiding relocations in first 16 sections instead.
        if self.network_knowledge.prefix().is_empty() {
            return Ok(vec![]);
        }
        debug!("Try to find relocate peers, excluded {excluded:?}");
        // Do not carry out relocation when there is not enough elder nodes.
        if self.network_knowledge.section_auth().elder_count() < elder_count() {
            debug!(
                "Not enough elders current {:?} vs. expected {:?}",
                self.network_knowledge.section_auth().elder_count(),
                elder_count()
            );
            return Ok(vec![]);
        }

        let mut cmds = vec![];
        for (node_state, relocate_details) in
            find_nodes_to_relocate(&self.network_knowledge, &churn_id, excluded)
        {
            debug!(
                "Relocating {:?} to {} (on churn of {churn_id})",
                node_state.peer(),
                relocate_details.dst,
            );

            cmds.extend(self.propose_section_state(SectionStateVote::NodeIsOffline(
                node_state.relocate(relocate_details),
            ))?);
        }

        Ok(cmds)
    }

    pub(crate) fn handle_relocate(
        &mut self,
        relocate_proof: SectionSigned<NodeState>,
    ) -> Result<Option<Cmd>> {
        trace!("Handle relocate {:?}", relocate_proof);
        let (dst_xorname, dst_section_key, new_age) =
            if let MembershipState::Relocated(ref relocate_details) = relocate_proof.state() {
                (
                    relocate_details.dst,
                    relocate_details.dst_section_key,
                    relocate_details.age,
                )
            } else {
                debug!(
                    "Ignoring Relocate msg containing invalid NodeState: {:?}",
                    relocate_proof.state()
                );
                return Ok(None);
            };

        let node = self.info();
        // `relocate_details.dst` is not the name of the relocated node,
        // but the target section for the peer to be relocated to.
        // TODO: having an additional field within reolocate_details for that info?
        // if dst_xorname != node.name() {
        //     // This `Relocate` message is not for us - it's most likely a duplicate of a previous
        //     // message that we already handled.
        //     return Ok(None);
        // }

        debug!("Received Relocate message to join the section at {dst_xorname}");

        if self.relocate_state.is_some() {
            trace!("Ignore Relocate - relocation already in progress");
            return Ok(None);
        }

        trace!("Relocation has started. previous_name: {:?}", node.name());
        trace!("{}", LogMarker::RelocateStart);

        // Create a new instance of JoiningAsRelocated to start the relocation
        // flow. This same instance will handle responses till relocation is complete.
        let bootstrap_addrs =
            if let Ok(sap) = self.network_knowledge.section_auth_by_name(&dst_xorname) {
                sap.addresses()
            } else {
                self.network_knowledge.section_auth().addresses()
            };
        let (joining_as_relocated, cmd) = JoiningAsRelocated::start(
            node,
            relocate_proof,
            bootstrap_addrs,
            dst_xorname,
            dst_section_key,
            new_age,
        )?;

        self.relocate_state = Some(Box::new(joining_as_relocated));

        Ok(Some(cmd))
    }
}
