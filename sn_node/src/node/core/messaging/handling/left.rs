// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{relocation::ChurnId, Node, Result},
};

use sn_interface::{
    messaging::system::{KeyedSig, SectionAuth},
    network_knowledge::NodeState,
    types::log_markers::LogMarker,
};

use std::collections::BTreeSet;

impl Node {
    pub(crate) async fn handle_node_left(
        &self,
        node_state: NodeState,
        sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        // return Ok(cmds);
        let signature = sig.signature.clone();

        let signed_node_state = SectionAuth {
            value: node_state.clone(),
            sig,
        };

        if !self
            .network_knowledge
            .update_member(signed_node_state.clone())
        {
            info!(
                "{}: {} at {}",
                LogMarker::IgnoredNodeAsOffline,
                node_state.name(),
                node_state.addr()
            );
            return Ok(cmds);
        }

        info!(
            "{}: {} at {}",
            LogMarker::AcceptedNodeAsOffline,
            node_state.name(),
            node_state.addr()
        );

        // If this is an Offline agreement where the new node state is Relocated,
        // we then need to send the Relocate msg to the peer attaching the signed NodeState
        // containing the relocation details.
        if node_state.is_relocated() {
            cmds.push(self.send_relocate(*node_state.peer(), signed_node_state)?);
        }

        let churn_id = ChurnId(signature.to_bytes().to_vec());
        cmds.extend(self.relocate_peers(churn_id, BTreeSet::default())?);

        let result = self
            .promote_and_demote_elders_except(&BTreeSet::default())
            .await?;
        if result.is_empty() {
            // Send AE-Update to Adults of our section
            let our_adults = self.network_knowledge.adults();
            let our_prefix = self.network_knowledge.prefix();
            let our_section_pk = self.network_knowledge.section_key();
            cmds.extend(
                self.send_ae_update_to_nodes(our_adults, &our_prefix, our_section_pk)
                    .await,
            );
        }

        cmds.extend(result);

        self.liveness_retain_only(
            self.network_knowledge
                .adults()
                .iter()
                .map(|peer| peer.name())
                .collect(),
        )
        .await?;
        *self.joins_allowed.borrow_mut() = true;

        Ok(cmds)
    }
}
