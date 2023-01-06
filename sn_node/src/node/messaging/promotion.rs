// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::flow_ctrl::cmds::Cmd;
use crate::node::{MyNode, Result};

use sn_interface::{
    messaging::system::{SectionSig, SectionSigShare, SectionSigned},
    messaging::MsgId,
    network_knowledge::{SectionAuthorityProvider, SectionTreeUpdate},
    types::log_markers::LogMarker,
    types::Peer,
};

impl MyNode {
    pub(crate) fn handle_handover_promotion(
        &mut self,
        msg_id: MsgId,
        sap: SectionSigned<SectionAuthorityProvider>,
        sig_share: SectionSigShare,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        trace!("Handling handover promotion message {msg_id:?} by {sender:?} with sap: {sap:?}");
        let our_prefix = self.network_knowledge.prefix();
        let sig_share_pk = &sig_share.public_key_set.public_key();

        // Proposal from other sections shall be ignored.
        if !our_prefix.matches(&sender.name()) {
            trace!("Ignore promotion message {msg_id:?} from other section");
            return Ok(vec![]);
        }
        // Let's now verify the section key in the msg authority is trusted
        // based on our current knowledge of the network and sections chains.
        if !self.network_knowledge.has_chain_key(sig_share_pk) {
            warn!("Ignore promotion message {msg_id:?} with untrusted sig share");
            return Ok(vec![]);
        }

        // try aggregate
        let serialize_err = |e| {
            error!(
                "Failed to serialize pubkey while handling handover promotion message {msg_id:?}"
            );
            e
        };
        let serialised_pk = bincode::serialize(&sap.sig.public_key).map_err(serialize_err)?;
        match self
            .elder_promotion_aggregator
            .try_aggregate(&serialised_pk, sig_share)
        {
            Ok(Some(sig)) => {
                trace!("Promotion message {msg_id:?} successfully aggregated");
                Ok(vec![Cmd::HandleNewEldersAgreement {
                    new_elders: sap,
                    sig,
                }])
            }
            Ok(None) => {
                trace!("Promotion message {msg_id:?} acknowledged, waiting for more...");
                Ok(vec![])
            }
            Err(err) => {
                error!("Failed to aggregate promotion message {msg_id:?} from {sender}: {err:?}");
                Ok(vec![])
            }
        }
    }

    pub(crate) fn handle_section_split_promotion(
        &mut self,
        msg_id: MsgId,
        sap1: SectionSigned<SectionAuthorityProvider>,
        sig_share1: SectionSigShare,
        sap2: SectionSigned<SectionAuthorityProvider>,
        sig_share2: SectionSigShare,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        trace!("Handling section split promotion message {msg_id:?} by {sender:?} with saps: {sap1:?} {sap2:?}");
        let our_prefix = self.network_knowledge.prefix();
        let sig_share_pk1 = &sig_share1.public_key_set.public_key();
        let sig_share_pk2 = &sig_share2.public_key_set.public_key();

        // Proposal from other sections shall be ignored.
        if !our_prefix.matches(&sender.name()) {
            trace!("Ignore promotion message {msg_id:?} from other section sent by {sender:?} when our prefix is {our_prefix:?}");
            return Ok(vec![]);
        }
        // Let's now verify the section key in the msg authority is trusted
        // based on our current knowledge of the network and sections chains.
        if !self.network_knowledge.has_chain_key(sig_share_pk1)
            || !self.network_knowledge.has_chain_key(sig_share_pk2)
        {
            warn!("Ignore promotion message {msg_id:?} with untrusted sig share");
            return Ok(vec![]);
        }

        // try aggregate
        let serialize_err = |e| {
            error!("Failed to serialize pubkey while handling split promotion message {msg_id:?}");
            e
        };
        let serialised_pk1 = bincode::serialize(&sap1.sig.public_key).map_err(serialize_err)?;
        let serialised_pk2 = bincode::serialize(&sap2.sig.public_key).map_err(serialize_err)?;
        let res1 = self
            .elder_promotion_aggregator
            .try_aggregate(&serialised_pk1, sig_share1);
        let res2 = self
            .elder_promotion_aggregator
            .try_aggregate(&serialised_pk2, sig_share2);

        match (res1, res2) {
            (Ok(Some(sig1)), Ok(Some(sig2))) => {
                trace!("Promotion message {msg_id:?} successfully aggregated");
                Ok(vec![Cmd::HandleNewSectionsAgreement {
                    sap1,
                    sig1,
                    sap2,
                    sig2,
                }])
            }
            (Ok(None), Ok(None)) => {
                trace!("Promotion message {msg_id:?} acknowledged, waiting for more...");
                Ok(vec![])
            }
            (_, Err(err)) | (Err(err), _) => {
                error!("Failed to aggregate promotion message {msg_id:?} from {sender}: {err:?}");
                Ok(vec![])
            }
            _ => {
                warn!("Unexpected aggregation result aggregate promotion message {msg_id:?}: one sig is aggregated while the other is not. This should not happen.");
                Ok(vec![])
            }
        }
    }

    #[instrument(skip(self), level = "trace")]
    pub(crate) async fn handle_new_sections_agreement(
        &mut self,
        sap1: SectionSigned<SectionAuthorityProvider>,
        sig1: SectionSig,
        sap2: SectionSigned<SectionAuthorityProvider>,
        sig2: SectionSig,
    ) -> Result<Vec<Cmd>> {
        if sap1.members().any(|m| m.name() == self.name()) {
            self.update_us(sap1, sig1, sap2, sig2).await
        } else if sap2.members().any(|m| m.name() == self.name()) {
            self.update_us(sap2, sig2, sap1, sig1).await
        } else {
            // Should not be possible..
            error!(
                "Error handling sections agreement, we are not a member in either section {}, {}",
                sap1.prefix(),
                sap2.prefix()
            );
            Ok(vec![])
        }
    }

    async fn update_us(
        &mut self,
        our_sap: SectionSigned<SectionAuthorityProvider>,
        sig_over_us: SectionSig,
        their_sap: SectionSigned<SectionAuthorityProvider>,
        sig_over_them: SectionSig,
    ) -> Result<Vec<Cmd>> {
        trace!("{}", LogMarker::HandlingNewSectionsAgreement);
        let context = self.context();

        // First we snapshot the section chain
        let mut parent_section_chain = self.section_chain();
        let parent_key = parent_section_chain.last_key()?;

        // Then we apply the add of our own new section
        let cmds = self
            .handle_new_elders_agreement(our_sap, sig_over_us)
            .await?;

        // Finally we update our network knowledge with our sibling section SAP.
        // We use the parent proof chain to connect our current chain to sibling SAP.
        parent_section_chain.verify_and_insert(
            &parent_key,
            their_sap.section_key(),
            sig_over_them.signature,
        )?;
        let their_prefix = their_sap.prefix();
        let section_tree_update = SectionTreeUpdate::new(their_sap, parent_section_chain);
        let updated = self.network_knowledge.update_knowledge_if_valid(
            section_tree_update,
            None,
            &context.name,
        )?;
        if updated {
            info!("Updated our network knowledge for {:?}", their_prefix);
            info!("Writing updated knowledge to disk");
            MyNode::write_section_tree(&context);

            let current_members = self.network_knowledge.members();
            self.comm.retain_only_peers(current_members);
        }
        Ok(cmds)
    }

    #[instrument(skip(self), level = "trace")]
    pub(crate) async fn handle_new_elders_agreement(
        &mut self,
        signed_sap: SectionSigned<SectionAuthorityProvider>,
        section_sig: SectionSig,
    ) -> Result<Vec<Cmd>> {
        trace!("{}", LogMarker::HandlingNewEldersAgreement);
        let context = self.context();
        let mut section_chain = self.section_chain();
        let last_key = section_chain.last_key()?;

        let prefix = signed_sap.prefix();
        trace!("{}: for {:?}", LogMarker::NewSignedSap, prefix);

        info!("New SAP agreed for:{}", *signed_sap);

        // Let's update our network knowledge, including our
        // section SAP and chain if the new SAP's prefix matches our name
        // We need to generate the proof chain to connect our current chain to new SAP.
        section_chain.verify_and_insert(
            &last_key,
            signed_sap.section_key(),
            section_sig.signature,
        )?;
        let update = SectionTreeUpdate::new(signed_sap, section_chain);
        let name = self.context().name;
        let updated = self
            .network_knowledge
            .update_knowledge_if_valid(update, None, &name)?;

        if updated {
            info!("Updated our network knowledge for {:?}", prefix);
            info!("Writing updated knowledge to disk");
            MyNode::write_section_tree(&context);

            info!(
                "Prefixes we know about: {:?}",
                self.network_knowledge.section_tree()
            );

            let cmds = self.update_on_elder_change(&context).await;
            let current_members = self.network_knowledge.members();
            self.comm.retain_only_peers(current_members);

            cmds
        } else {
            Ok(vec![])
        }
    }
}
