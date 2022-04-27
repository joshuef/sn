use std::collections::BTreeSet;

use sn_interface::{
    messaging::system::{KeyedSig, SectionAuth},
    network_knowledge::NodeState,
    types::log_markers::LogMarker,
};

use crate::node::{
    api::cmds::Cmd,
    core::{relocation::ChurnId, Node, Result},
};

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
            .await
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
            cmds.push(
                self.send_relocate(*node_state.peer(), signed_node_state)
                    .await?,
            );
        }

        let churn_id = ChurnId(signature.to_bytes().to_vec());
        cmds.extend(self.relocate_peers(churn_id, BTreeSet::default()).await?);

        let result = self
            .promote_and_demote_elders_except(&BTreeSet::default())
            .await?;
        if result.is_empty() {
            // Send AE-Update to Adults of our section
            let our_adults = self.network_knowledge.adults().await;
            let our_prefix = self.network_knowledge.prefix().await;
            let our_section_pk = self.network_knowledge.section_key().await;
            cmds.extend(
                self.send_ae_update_to_nodes(our_adults, &our_prefix, our_section_pk)
                    .await,
            );
        }

        cmds.extend(result);

        let members: BTreeSet<_> = self
            .network_knowledge
            .adults()
            .await
            .iter()
            .map(|peer| peer.name())
            .collect();

        #[cfg(any(feature = "chunks", feature = "registers"))]
        self.data_records_retain_only(members.clone()).await?;

        let _ = self.dysfunction_tracking.retain_members_only(members).await;

        *self.joins_allowed.write().await = true;

        Ok(cmds)
    }
}
