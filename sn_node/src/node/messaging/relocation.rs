// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::cmds::Cmd,
    relocation::{find_nodes_to_relocate, ChurnId},
    MyNode, Result,
};

use sn_interface::{
    elder_count,
    messaging::system::SectionSigned,
    network_knowledge::{node_state::RelocationInfo, MembershipState, NodeState, RelocationProof},
    types::{keys::ed25519, log_markers::LogMarker},
};

use std::collections::BTreeSet;
use xor_name::XorName;

// Relocation
impl MyNode {
    pub(crate) fn try_relocate_peers(
        &mut self,
        churn_id: ChurnId,
        excluded: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
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
        for (node_state, relocation_dst) in
            find_nodes_to_relocate(&self.network_knowledge, &churn_id, excluded)
        {
            debug!(
                "Relocating {:?} to {} (on churn of {churn_id})",
                node_state.peer(),
                relocation_dst.name(),
            );

            cmds.extend(self.propose_membership_change(node_state.relocate(relocation_dst)));
        }

        Ok(cmds)
    }

    pub(crate) fn relocate(
        &mut self,
        signed_relocation: SectionSigned<NodeState>,
    ) -> Result<Option<Cmd>> {
        // should be unreachable, but a sanity check
        let serialized = bincode::serialize(&signed_relocation.value)?;
        if !signed_relocation.sig.verify(&serialized) {
            warn!("Relocate: Could not verify section signature of our relocation");
            return Err(super::Error::InvalidSignature);
        }
        if self.name() != signed_relocation.peer().name() {
            // not for us, drop it
            warn!("Relocate: The received section signed relocation is not for us.");
            return Ok(None);
        }

        let dst_section =
            if let MembershipState::Relocated(relocation_dst) = signed_relocation.state() {
                *relocation_dst.name()
            } else {
                debug!(
                    "Relocate: Ignoring msg containing invalid NodeState: {:?}",
                    signed_relocation.state()
                );
                return Ok(None);
            };

        trace!("{}", LogMarker::RelocateStart);
        debug!("Relocate: Received decision to relocate to other section at {dst_section}");

        let original_info = self.info();

        let dst_sap = self
            .network_knowledge
            .closest_signed_sap(&dst_section)
            .ok_or(super::Error::NoMatchingSection)?;
        let new_keypair = ed25519::gen_keypair(
            &dst_sap.prefix().range_inclusive(),
            original_info.age().saturating_add(1),
        );
        let new_name = ed25519::name(&new_keypair.public);

        let info = RelocationInfo::new(signed_relocation, new_name);
        let serialized_info = bincode::serialize(&info)?;
        // we verify that this new name was actually created by the old name
        let node_sig = ed25519::sign(&serialized_info, &original_info.keypair);
        let new_prefix = dst_sap.prefix();

        // we switch to the new section
        self.switch_section(dst_sap, new_keypair)?;

        info!(
            "Relocation of us as {}: switched section to {new_prefix:?} with new name {new_name}. Now trying to join..",
            original_info.name(),
        );

        let proof = RelocationProof::new(info, node_sig, original_info.keypair.public);
        // we cache the proof so that we can retry if the join times out
        self.relocation_proof = Some(proof.clone());

        Ok(MyNode::try_join_section(self.context(), Some(proof)))
    }
}
