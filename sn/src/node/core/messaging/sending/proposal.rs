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
    messages::WireMsgUtils,
    Result,
};
use sn_interface::messaging::{system::SystemMsg, DstLocation, WireMsg};
use sn_interface::network_knowledge::SectionKeyShare;
use sn_interface::types::Peer;

impl Node {
    /// Send proposal to all our elders.
    pub(crate) async fn propose(&self, proposal: Proposal) -> Result<Vec<Cmd>> {
        let elders = self
            .network_knowledge
            .authority_provider()
            .await
            .elders_vec();
        self.send_proposal(elders, proposal).await
    }

    /// Send `proposal` to `recipients`.
    pub(crate) async fn send_proposal(
        &self,
        recipients: Vec<Peer>,
        proposal: Proposal,
    ) -> Result<Vec<Cmd>> {
        let section_key = self.network_knowledge.section_key().await;

        let key_share = self
            .section_keys_provider
            .key_share(&section_key)
            .await
            .map_err(|err| {
                trace!("Can't propose {:?}: {:?}", proposal, err);
                err
            })?;

        self.send_proposal_with(recipients, proposal, &key_share)
            .await
    }

    /// Send `proposal` to `recipients` signing it with the provided key share.
    pub(crate) async fn send_proposal_with(
        &self,
        recipients: Vec<Peer>,
        proposal: Proposal,
        key_share: &SectionKeyShare,
    ) -> Result<Vec<Cmd>> {
        trace!(
            "Propose {:?}, key_share: {:?}, aggregators: {:?}",
            proposal,
            key_share,
            recipients,
        );

        let sig_share = proposal.sign_with_key_share(
            key_share.public_key_set.clone(),
            key_share.index,
            &key_share.secret_key_share,
        )?;

        // Broadcast the proposal to the rest of the section elders.
        let node_msg = SystemMsg::Propose {
            proposal: proposal.clone().into_msg(),
            sig_share: sig_share.clone(),
        };
        // Name of the section_pk may not matches the section prefix.
        // Carry out a substitution to prevent the dst_location becomes other section.
        let section_key = self.network_knowledge.section_key().await;
        let wire_msg = WireMsg::single_src(
            &self.info.read().await.clone(),
            DstLocation::Section {
                name: self.network_knowledge.prefix().await.name(),
                section_pk: section_key,
            },
            node_msg,
            section_key,
        )?;

        let msg_id = wire_msg.msg_id();

        let mut cmds = vec![];
        let our_name = self.info.read().await.name();
        // handle ourselves if we should
        for peer in recipients.clone() {
            if peer.name() == our_name {
                cmds.extend(
                    super::super::handle_proposal(
                        msg_id,
                        proposal.clone(),
                        sig_share.clone(),
                        peer,
                        &self.network_knowledge,
                        &self.proposal_aggregator,
                    )
                    .await?,
                )
            }
        }

        // remove ourself from recipients
        let recipients = recipients
            .into_iter()
            .filter(|peer| peer.name() != our_name)
            .collect();

        cmds.extend(
            self.send_messages_to_all_nodes_or_directly_handle_for_accumulation(
                recipients, wire_msg,
            )
            .await?,
        );

        Ok(cmds)
    }
}
