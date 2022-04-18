// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{Node, StateSnapshot},
    Result,
};
use sn_interface::messaging::system::{NodeCmd, SystemMsg};
use sn_interface::types::{log_markers::LogMarker, Peer};

use bls::PublicKey as BlsPublicKey;
use xor_name::Prefix;

impl Node {
    /// Send `AntiEntropyUpdate` message to all nodes in our own section.
    pub(crate) async fn send_ae_update_to_our_section(&self) -> Vec<Cmd> {
        let our_name = self.info.read().await.name();
        let nodes: Vec<_> = self
            .network_knowledge
            .section_members()
            .await
            .into_iter()
            .filter(|info| info.name() != our_name)
            .map(|info| *info.peer())
            .collect();

        if nodes.is_empty() {
            warn!("No peers of our section found in our network knowledge to send AE-Update");
            return vec![];
        }

        // The previous PK which is likely what adults know
        let previous_pk = *self.section_chain().await.prev_key();

        let our_prefix = self.network_knowledge.prefix().await;

        self.send_ae_update_to_nodes(nodes, &our_prefix, previous_pk)
            .await
    }

    /// Send `AntiEntropyUpdate` message to the specified nodes.
    pub(crate) async fn send_ae_update_to_nodes(
        &self,
        recipients: Vec<Peer>,
        prefix: &Prefix,
        section_pk: BlsPublicKey,
    ) -> Vec<Cmd> {
        let node_msg = match self.generate_ae_update_msg(section_pk).await {
            Ok(node_msg) => node_msg,
            Err(err) => {
                warn!("Failed to generate AE-Update msg to send: {:?}", err);
                return vec![];
            }
        };

        let our_section_key = self.network_knowledge.section_key().await;
        match self
            .send_direct_msg_to_nodes(recipients.clone(), node_msg, prefix.name(), our_section_key)
            .await
        {
            Ok(cmd) => vec![cmd],
            Err(err) => {
                error!(
                    "Failed to send AE update to ({:?}) {:?}: {:?}",
                    prefix, recipients, err
                );
                vec![]
            }
        }
    }

    #[cfg(feature = "service-msgs")]
    /// Send `MetadataExchange` packet to the specified nodes
    pub(crate) async fn send_metadata_updates_to_nodes(
        &self,
        recipients: Vec<Peer>,
        prefix: &Prefix,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        let metadata = self.get_metadata_of(prefix).await;
        let data_update_msg = SystemMsg::NodeCmd(NodeCmd::ReceiveMetadata { metadata });

        match self
            .send_direct_msg_to_nodes(
                recipients.clone(),
                data_update_msg,
                prefix.name(),
                section_pk,
            )
            .await
        {
            Ok(cmd) => Ok(vec![cmd]),
            Err(err) => {
                error!(
                    "Failed to send data updates to: {:?} with {:?}",
                    recipients, err
                );
                Ok(vec![])
            }
        }
    }

    #[instrument(skip_all)]
    /// Send AntiEntropyUpdate message to the nodes in our sibling section.
    pub(crate) async fn send_updates_to_sibling_section(
        &self,
        our_prev_state: &StateSnapshot,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::AeSendUpdateToSiblings);
        let sibling_prefix = self.network_knowledge.prefix().await.sibling();
        if let Some(sibling_sap) = self
            .network_knowledge
            .prefix_map()
            .get_signed(&sibling_prefix)
        {
            let promoted_sibling_elders: Vec<_> = sibling_sap
                .elders()
                .filter(|peer| !our_prev_state.elders.contains(&peer.name()))
                .cloned()
                .collect();

            if promoted_sibling_elders.is_empty() {
                debug!("No promoted siblings found in our network knowledge to send AE-Update");
                return Ok(vec![]);
            }

            // Using previous_key as dst_section_key as newly promoted
            // sibling Elders shall still in the state of pre-split.
            let previous_section_key = our_prev_state.section_key;
            let sibling_prefix = sibling_sap.prefix();

            let mut cmds = vec![];

            #[cfg(feature = "service-msgs")]
            cmds.extend(
                self.send_metadata_updates_to_nodes(
                    promoted_sibling_elders.clone(),
                    &sibling_prefix,
                    previous_section_key,
                )
                .await?,
            );

            // Also send AE update to sibling section's new Elders
            cmds.extend(
                self.send_ae_update_to_nodes(
                    promoted_sibling_elders,
                    &sibling_prefix,
                    previous_section_key,
                )
                .await,
            );

            Ok(cmds)
        } else {
            error!("Failed to get sibling SAP during split.");
            Ok(vec![])
        }
    }

    // Private helper to generate AntiEntropyUpdate message to update
    // a peer abot our SAP, with proof_chain and members list.
    async fn generate_ae_update_msg(&self, dst_section_key: BlsPublicKey) -> Result<SystemMsg> {
        let signed_sap = self
            .network_knowledge
            .section_signed_authority_provider()
            .await;

        let proof_chain = if let Ok(chain) = self
            .network_knowledge
            .get_proof_chain_to_current(&dst_section_key)
            .await
        {
            chain
        } else {
            // error getting chain from key, so let's send the whole chain from genesis
            self.network_knowledge.section_chain().await
        };

        let members = self
            .network_knowledge
            .section_signed_members()
            .await
            .iter()
            .map(|state| state.clone().into_authed_msg())
            .collect();

        Ok(SystemMsg::AntiEntropyUpdate {
            section_auth: signed_sap.value.to_msg(),
            section_signed: signed_sap.sig,
            proof_chain,
            members,
        })
    }
}
