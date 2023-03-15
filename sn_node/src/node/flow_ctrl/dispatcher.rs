// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{messaging::Recipients, Cmd, Error, MyNode, Result};

use sn_interface::{
    messaging::{AntiEntropyMsg, NetworkMsg},
    network_knowledge::SectionTreeUpdate,
    types::{NodeId, Participant},
};

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
            Cmd::UpdateCaller {
                caller,
                correlation_id,
                kind,
                section_tree_update,
            } => {
                info!("Sending ae response msg for {correlation_id:?}");
                vec![Cmd::send_network_msg(
                    NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                        section_tree_update,
                        kind,
                    }),
                    Recipients::Single(Participant::from_node(caller)), // we're doing a mapping again here.. but this is a necessary evil while transitioning to more clarity and type safety, i.e. TO BE FIXED
                )]
            }
            Cmd::UpdateCallerOnStream {
                caller,
                msg_id,
                kind,
                section_tree_update,
                correlation_id,
                stream,
            } => MyNode::send_ae_response(
                AntiEntropyMsg::AntiEntropy {
                    kind,
                    section_tree_update,
                },
                msg_id,
                caller,
                correlation_id,
                stream,
                context,
            )
            .await?
            .into_iter()
            .collect(),
            Cmd::SendMsg {
                msg,
                msg_id,
                recipients, // NB: SendMsg only calls out via comms, so it should be possible to make this a set of node ids!
            } => {
                let recipients = recipients.into_iter().map(NodeId::from).collect();
                MyNode::send_msg(msg, msg_id, recipients, context)?;
                vec![]
            }
            Cmd::SendMsgEnqueueAnyResponse {
                msg,
                msg_id,
                recipients,
            } => {
                debug!("send msg enque cmd...?");
                MyNode::send_and_enqueue_any_response(msg, msg_id, context, recipients)?;
                vec![]
            }
            Cmd::SendAndForwardResponseToClient {
                wire_msg,
                targets,
                client_stream,
                client_id,
            } => {
                MyNode::send_and_forward_response_to_client(
                    wire_msg,
                    context,
                    targets,
                    client_stream,
                    client_id,
                )?;
                vec![]
            }
            Cmd::SendNodeMsgResponse {
                msg,
                msg_id,
                correlation_id,
                node_id,
                send_stream,
            } => MyNode::send_node_msg_response(
                msg,
                msg_id,
                correlation_id,
                node_id,
                context,
                send_stream,
            )
            .await?
            .into_iter()
            .collect(),
            Cmd::SendDataResponse {
                msg,
                msg_id,
                correlation_id,
                send_stream,
                client_id,
            } => MyNode::send_data_response(
                msg,
                msg_id,
                correlation_id,
                send_stream,
                context,
                client_id,
            )
            .await?
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
            } => MyNode::handle_node_msg(node, context, msg_id, msg, node_id, send_stream).await?,
            Cmd::ProcessClientMsg {
                msg_id,
                msg,
                auth,
                client_id,
                send_stream,
            } => {
                trace!("Client msg {msg_id:?} reached its destination.");

                let Some(stream) = send_stream else {
                        error!("No stream for client {client_id} tho....");
                        // TODO: clarify this err w/ client id
                        return Err(Error::NoClientResponseStream);
                    };
                MyNode::handle_client_msg_for_us(context, msg_id, msg, auth, client_id, stream)
                    .await?
            }
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
            Cmd::HandleMsg {
                sender,
                wire_msg,
                send_stream,
            } => MyNode::handle_msg(node.context(), sender, wire_msg, send_stream).await?,
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

                MyNode::handle_client_msg_for_us(context, msg_id, msg, auth, client_id, send_stream)
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
        };

        let elapsed = start.elapsed();
        debug!("Cmd took {elapsed:?} {cmd_string:?} ");

        Ok(result)
    }
}
