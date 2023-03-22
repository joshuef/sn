// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Cmd, Error, MyNode, Result};

use sn_interface::network_knowledge::SectionTreeUpdate;

use std::time::Instant;

impl MyNode {
    /// Handles a single cmd.
    pub(crate) async fn process_cmd(cmd: Cmd, node: &mut MyNode) -> Result<Vec<Cmd>> {
        let context = node.context();
        let start = Instant::now();
        let cmd_string = format!("{cmd}");
        let result = match cmd {
            Cmd::TryJoinNetwork => MyNode::try_join_section(context, None)
                .into_iter()
                .collect(),
            Cmd::TrackNodeIssue { name, issue } => {
                context.track_node_issue(name, issue);
                vec![]
            }
            Cmd::ProcessNodeMsg {
                msg_id,
                msg,
                node_id,
                send_stream,
            } => MyNode::handle_node_msg(node, context, msg_id, msg, node_id, send_stream)?,
            Cmd::ProcessAeMsg {
                msg_id,
                kind,
                section_tree_update,
                sender,
            } => {
                trace!("Handling msg: AE from {sender}: {msg_id:?}");
                MyNode::handle_anti_entropy_msg(node, context, section_tree_update, kind, sender)
                    .await?
            }
            Cmd::UpdateNetworkAndHandleValidClientMsg {
                proof_chain,
                signed_sap,
                msg_id,
                msg,
                client_id,
                auth,
                send_stream,
            } => {
                debug!("Updating network knowledge before handling message");
                // we create a block to make sure the node's lock is released
                let updated = {
                    let name = node.name();
                    node.network_knowledge.update_sap_knowledge_if_valid(
                        SectionTreeUpdate::new(signed_sap, proof_chain),
                        &name,
                    )?
                };
                info!("Network knowledge was updated: {updated}");

                let context = if updated { node.context() } else { context };

                let network_knowledge = context.network_knowledge.clone();
                let our_name = context.name.clone();
                let data_storage = context.data_storage.clone();
                let is_elder = context.is_elder;
                let joins_allowed = context.joins_allowed;
                let reward_key = context.reward_key;
                let store_cost = context.store_cost;
                let ed_keypair = context.keypair;
                let joins_allowed_until_split = context.joins_allowed_until_split;
                let section_keys_provider = context.section_keys_provider.clone();

                // TODO: This could be slow and should be moved out of blocking context
                MyNode::handle_client_msg_for_us(
                    msg_id,
                    msg,
                    auth,
                    client_id,
                    send_stream,
                    our_name,
                    &network_knowledge,
                    ed_keypair,
                    &section_keys_provider,
                    reward_key,
                    store_cost,
                    joins_allowed,
                    joins_allowed_until_split,
                    is_elder,
                    data_storage,
                )
                .await?
            }
            Cmd::HandleNodeOffAgreement { proposal, sig } => {
                node.handle_section_decision_agreement(proposal, sig)?
            }
            Cmd::HandleMembershipDecision(decision) => {
                node.handle_membership_decision(decision).await?
            }
            Cmd::HandleNewEldersAgreement { new_elders, sig } => {
                node.handle_new_elders_agreement(new_elders, sig).await?
            }
            Cmd::HandleNewSectionsAgreement {
                sap1,
                sig1,
                sap2,
                sig2,
            } => {
                node.handle_new_sections_agreement(sap1, sig1, sap2, sig2)
                    .await?
            }
            Cmd::HandleCommsError { participant, error } => {
                trace!("Comms error {error}");
                node.handle_comms_error(participant, error);
                vec![]
            }
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
            } => node.handle_dkg_outcome(section_auth, outcome).await?,
            Cmd::EnqueueDataForReplication {
                recipient,
                data_batch,
            } => {
                if let Some(sender) = &node.data_replication_sender {
                    sender
                        .send((data_batch, recipient))
                        .await
                        .map_err(|_| Error::DataReplicationChannel)?;
                } else {
                    warn!("No data replication sender set!");
                }
                vec![]
            }
            Cmd::ProposeVoteNodesOffline(names) => node.cast_offline_proposals(&names)?,
            Cmd::SetJoinsAllowed(joins_allowed) => {
                node.joins_allowed = joins_allowed;
                vec![]
            }
            Cmd::SetJoinsAllowedUntilSplit(joins_allowed_until_split) => {
                node.joins_allowed = joins_allowed_until_split;
                node.joins_allowed_until_split = joins_allowed_until_split;
                vec![]
            }
            cmd => {
                error!("This cmd should not be handled in the blocking loop. Move it to FlowCtrlCmd channel: {cmd:?}");

                vec![]
            }
        };

        let elapsed = start.elapsed();
        trace!("Cmd took {elapsed:?} {cmd_string:?} ");

        Ok(result)
    }
}
