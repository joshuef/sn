// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{relocation::ChurnId, Node, Proposal},
    Event, Result,
};
use sn_interface::messaging::system::{KeyedSig, MembershipState, SectionAuth};
use sn_interface::network_knowledge::SectionAuthUtils;
use sn_interface::network_knowledge::{NodeState, SectionAuthorityProvider, MIN_ADULT_AGE};
use sn_interface::types::log_markers::LogMarker;

use std::{cmp, collections::BTreeSet};

// Agreement
impl Node {
    #[instrument(skip(self), level = "trace")]
    pub(crate) async fn handle_general_agreements(
        &self,
        proposal: Proposal,
        sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        debug!("handle agreement on {:?}", proposal);
        match proposal {
            Proposal::Offline(node_state) => self.handle_offline_agreement(node_state, sig).await,
            Proposal::SectionInfo(section_auth) => {
                self.handle_section_info_agreement(section_auth, sig).await
            }
            Proposal::NewElders(_) => {
                error!("Elders agreement should be handled in a separate blocking fashion");
                Ok(vec![])
            }
            Proposal::JoinsAllowed(joins_allowed) => {
                *self.joins_allowed.write().await = joins_allowed;
                Ok(vec![])
            }
        }
    }

    pub(crate) async fn handle_online_agreement(
        &self,
        new_info: NodeState,
        sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::AgreementOfOnline);
        let mut cmds = vec![];
        if let Some(old_info) = self
            .network_knowledge
            .is_either_member_or_archived(&new_info.name())
            .await
        {
            // This node is rejoin with same name.
            if old_info.state() != MembershipState::Left {
                debug!(
                    "Ignoring Online node {} - {:?} not Left.",
                    new_info.name(),
                    old_info.state(),
                );

                return Ok(cmds);
            }

            // We would approve and relocate it only if half its age is at least MIN_ADULT_AGE
            let new_age = cmp::max(MIN_ADULT_AGE - 1, old_info.age() / 2);
            if new_age >= MIN_ADULT_AGE {
                // TODO: consider handling the relocation inside the bootstrap phase, to avoid
                // having to send this `NodeApproval`.
                cmds.extend(self.send_node_approval(old_info.clone()).await);

                cmds.extend(
                    self.relocate_rejoining_peer(old_info.value, new_age)
                        .await?,
                );

                return Ok(cmds);
            }
        }

        let new_info = SectionAuth {
            value: new_info,
            sig,
        };

        if !self.network_knowledge.update_member(new_info.clone()).await {
            info!("ignore Online: {} at {}", new_info.name(), new_info.addr());
            return Ok(vec![]);
        }

        self.add_new_adult_to_trackers(new_info.name()).await;

        info!("handle Online: {} at {}", new_info.name(), new_info.addr());

        // still used for testing
        self.send_event(Event::MemberJoined {
            name: new_info.name(),
            previous_name: new_info.previous_name(),
            age: new_info.age(),
        })
        .await;

        self.log_section_stats().await;

        // Do not disable node joins in first section.
        let our_prefix = self.network_knowledge.prefix().await;
        if !our_prefix.is_empty() {
            // ..otherwise, switch off joins_allowed on a node joining.
            // TODO: fix racing issues here? https://github.com/maidsafe/safe_network/issues/890
            *self.joins_allowed.write().await = false;
        }

        let churn_id = ChurnId(new_info.sig.signature.to_bytes().to_vec());
        let excluded_from_relocation = vec![new_info.name()].into_iter().collect();
        cmds.extend(
            self.relocate_peers(churn_id, excluded_from_relocation)
                .await?,
        );

        let result = self.promote_and_demote_elders().await?;
        if result.is_empty() {
            // Send AE-Update to Adults of our section
            let our_adults = self.network_knowledge.adults().await;
            let our_section_pk = self.network_knowledge.section_key().await;
            cmds.extend(
                self.send_ae_update_to_nodes(our_adults, &our_prefix, our_section_pk)
                    .await,
            );
        }

        cmds.extend(result);
        cmds.extend(self.send_node_approval(new_info).await);

        info!("cmds in queue for Accepting node {:?}", cmds);

        self.print_network_stats().await;

        Ok(cmds)
    }

    #[instrument(skip(self))]
    async fn handle_offline_agreement(
        &self,
        node_state: NodeState,
        sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
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
                "ignore Offline: {} at {}",
                node_state.name(),
                node_state.addr()
            );
            return Ok(cmds);
        }

        info!(
            "handle Offline: {} at {}",
            node_state.name(),
            node_state.addr()
        );

        // If this is an Offline agreement where the new node state is Relocated,
        // we then need to send the Relocate msg to the peer attaching the signed NodeState
        // containing the relocation details.
        if node_state.is_relocated() {
            cmds.push(
                self.send_relocate(node_state.peer().clone(), signed_node_state)
                    .await?,
            );
        }

        let churn_id = ChurnId(signature.to_bytes().to_vec());
        cmds.extend(self.relocate_peers(churn_id, BTreeSet::default()).await?);

        let result = self.promote_and_demote_elders().await?;
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

        self.liveness_retain_only(
            self.network_knowledge
                .adults()
                .await
                .iter()
                .map(|peer| peer.name())
                .collect(),
        )
        .await?;
        *self.joins_allowed.write().await = true;

        Ok(cmds)
    }

    #[instrument(skip(self), level = "trace")]
    async fn handle_section_info_agreement(
        &self,
        section_auth: SectionAuthorityProvider,
        sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        let equal_or_extension = section_auth.prefix() == self.network_knowledge.prefix().await
            || section_auth
                .prefix()
                .is_extension_of(&self.network_knowledge.prefix().await);

        if equal_or_extension {
            // Our section or sub-section
            debug!(
                "Updating section info for our prefix: {:?}",
                section_auth.prefix()
            );

            let signed_section_auth = SectionAuth::new(section_auth, sig.clone());
            let saps_candidates = self
                .network_knowledge
                .promote_and_demote_elders(&self.info.read().await.name(), &BTreeSet::new())
                .await;

            if !saps_candidates.contains(&signed_section_auth.elder_candidates()) {
                // SectionInfo out of date, ignore.
                return Ok(vec![]);
            }

            // Send the `OurElder` proposal to all of the to-be-Elders so it's aggregated by them.
            let proposal_recipients = saps_candidates
                .iter()
                .flat_map(|info| info.elders())
                .cloned()
                .collect();

            self.send_proposal(
                proposal_recipients,
                Proposal::NewElders(signed_section_auth),
            )
            .await
        } else {
            // Other section. We shouln't be receiving or updating a SAP for
            // a remote section here, that is done with a AE msg response.
            debug!(
                "Ignoring Proposal::SectionInfo since prefix doesn't match ours: {:?}",
                section_auth
            );
            Ok(vec![])
        }
    }

    #[instrument(skip(self), level = "trace")]
    pub(crate) async fn handle_new_elders_agreement(
        &self,
        signed_section_auth: SectionAuth<SectionAuthorityProvider>,
        key_sig: KeyedSig,
    ) -> Result<Vec<Cmd>> {
        trace!("{}", LogMarker::HandlingNewEldersAgreement);
        let updates = self.split_barrier.write().await.process(
            &self.network_knowledge.prefix().await,
            signed_section_auth.clone(),
            key_sig,
        );

        if updates.is_empty() {
            return Ok(vec![]);
        }

        let snapshot = self.state_snapshot().await;
        let old_chain = self.section_chain().await.clone();

        for (signed_sap, key_sig) in updates {
            let prefix = signed_sap.prefix();
            trace!("{}: for {:?}", LogMarker::NewSignedSap, prefix);

            info!("New SAP agreed for:{}", *signed_sap);

            let our_name = self.info.read().await.name();

            // Let's update our network knowledge, including our
            // section SAP and chain if the new SAP's prefix matches our name
            // We need to generate the proof chain to connect our current chain to new SAP.
            let mut proof_chain = old_chain.clone();
            match proof_chain.insert(
                old_chain.last_key(),
                signed_sap.section_key(),
                key_sig.signature,
            ) {
                Err(err) => error!("Failed to generate proof chain for new SAP: {:?}", err),
                Ok(()) => match self
                    .network_knowledge
                    .update_knowledge_if_valid(
                        signed_sap.clone(),
                        &proof_chain,
                        None,
                        &our_name,
                        &self.section_keys_provider,
                    )
                    .await
                {
                    Err(err) => error!(
                        "Error updating our network knowledge for {:?}: {:?}",
                        prefix, err
                    ),
                    Ok(true) => {
                        info!("Updated our network knowledge for {:?}", prefix);
                        info!("Writing updated knowledge to disk");
                        self.write_prefix_map().await
                    }
                    _ => {}
                },
            }
        }

        info!(
            "Prefixes we know about: {:?}",
            self.network_knowledge.prefix_map()
        );

        self.update_self_for_new_node_state(snapshot).await
    }
}
