// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::RejoinReason, messaging::Peers, Cmd, Error, MyNode, Result,
    STANDARD_CHANNEL_SIZE,
};

use sn_interface::{
    messaging::{AntiEntropyMsg, NetworkMsg},
    network_knowledge::SectionTreeUpdate,
    types::{DataAddress, Peer},
};

use std::time::Instant;
use tokio::sync::mpsc::{channel, Receiver, Sender};

// Cmd Dispatcher.
pub(crate) struct Dispatcher {
    data_replication_sender: Sender<(Vec<DataAddress>, Peer)>,
}

impl Dispatcher {
    /// Creates dispatcher and returns a receiver for enqueing DataAddresses for replication to specific peers
    pub(crate) fn new() -> (Self, Receiver<(Vec<DataAddress>, Peer)>) {
        let (data_replication_sender, data_replication_receiver) = channel(STANDARD_CHANNEL_SIZE);
        let dispatcher = Self {
            data_replication_sender,
        };

        (dispatcher, data_replication_receiver)
    }

    /// Handles a single cmd.
    pub(crate) fn process_cmd_off_thread(
        &self,
        cmd: Cmd,
        context: NodeContext,
        id: Vec<usize>,

        cmd_process_api: Sender<(Cmd, Vec<usize>)>,
        rejoin_network_sender: Sender<RejoinReason>,
    ) {
        let _handle = tokio::spawn(async move {
            trace!("Moving Cmd onto a new thread: {cmd:?}");
            let start = Instant::now();
            let cmd_string = format!("{}", cmd);
            let result = match cmd {
                Cmd::TrackNodeIssue { name, issue } => {
                    context.track_node_issue(name, issue);
                    Ok(vec![])
                }
                Cmd::ProcessClientMsg {
                    msg_id,
                    msg,
                    auth,
                    origin,
                    send_stream,
                } => {
                    trace!("Client msg {msg_id:?} reached its destination.");

                    // TODO: clarify this err w/ peer
                    let Some(stream) = send_stream else {
                        error!("No stream for client tho....");
                        return ;
                        // return Err(Error::NoClientResponseStream);
                    };
                    MyNode::handle_client_msg_for_us(context, msg_id, msg, auth, origin, stream)
                        .await
                }

                Cmd::HandleMsg {
                    origin,
                    wire_msg,
                    send_stream,
                } => MyNode::handle_msg(context, origin, wire_msg, send_stream).await,
                cmd => {
                    error!("Cmd should not be moved off thread. It was not processsed {cmd:?}");

                    Err(Error::InvalidMessage)
                }
            };

            let elapsed = start.elapsed();
            trace!("Running Cmd {cmd_string:?} took {:?}", elapsed);

            match result {
                Ok(cmds) => {
                    for (child_nr, cmd) in cmds.into_iter().enumerate() {
                        // zero based, first child of first cmd => [0, 0], second child => [0, 1], first child of second child => [0, 1, 0]
                        let child_id = [id.clone(), [child_nr].to_vec()].concat();
                        match cmd_process_api.send((cmd, child_id)).await {
                            Ok(_) => (), // no issues
                            Err(error) => {
                                let child_id = [id.clone(), [child_nr].to_vec()].concat();
                                error!(
                                    "Could not enqueue child cmd with id: {child_id:?}: {error:?}",
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!("Error when processing cmd: {:?}", error);
                    if let Error::RejoinRequired(reason) = error {
                        if rejoin_network_sender.send(reason).await.is_err() {
                            error!("Could not send rejoin reason through channel.");
                        }
                    }
                }
            }
        });
    }

    /// Handles a single cmd.
    pub(crate) async fn process_cmd(&self, cmd: Cmd, node: &mut MyNode) -> Result<Vec<Cmd>> {
        let context = node.context();
        let start = Instant::now();
        let cmd_string = format!("{}", cmd);
        let result = match cmd {
            Cmd::TryJoinNetwork => Ok(MyNode::try_join_section(context, None)
                .into_iter()
                .collect()),
            Cmd::UpdateCaller {
                caller,
                correlation_id,
                kind,
                section_tree_update,
            } => {
                info!("Sending ae response msg for {correlation_id:?}");
                Ok(vec![Cmd::send_network_msg(
                    NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                        section_tree_update,
                        kind,
                    }),
                    Peers::Single(caller),
                )])
            }
            Cmd::UpdateCallerOnStream {
                caller,
                msg_id,
                kind,
                section_tree_update,
                correlation_id,
                stream,
            } => Ok(MyNode::send_ae_response(
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
            .collect()),
            Cmd::SendMsg {
                msg,
                msg_id,
                recipients,
            } => {
                MyNode::send_msg(msg, msg_id, recipients, context)?;
                Ok(vec![])
            }
            Cmd::SendMsgEnqueueAnyResponse {
                msg,
                msg_id,
                recipients,
            } => {
                debug!("send msg enque cmd...?");
                MyNode::send_and_enqueue_any_response(msg, msg_id, context, recipients)?;
                Ok(vec![])
            }
            Cmd::SendAndForwardResponseToClient {
                wire_msg,
                targets,
                client_stream,
                source_client,
            } => {
                MyNode::send_and_forward_response_to_client(
                    wire_msg,
                    context,
                    targets,
                    client_stream,
                    source_client,
                )?;
                Ok(vec![])
            }
            Cmd::SendNodeMsgResponse {
                msg,
                msg_id,
                correlation_id,
                recipient,
                send_stream,
            } => Ok(MyNode::send_node_msg_response(
                msg,
                msg_id,
                correlation_id,
                recipient,
                context,
                send_stream,
            )
            .await?
            .into_iter()
            .collect()),
            Cmd::SendDataResponse {
                msg,
                msg_id,
                correlation_id,
                send_stream,
                source_client,
            } => Ok(MyNode::send_data_response(
                msg,
                msg_id,
                correlation_id,
                send_stream,
                context,
                source_client,
            )
            .await?
            .into_iter()
            .collect()),
            Cmd::TrackNodeIssue { name, issue } => {
                context.track_node_issue(name, issue);
                Ok(vec![])
            }
            Cmd::ProcessNodeMsg {
                msg_id,
                msg,
                origin,
                send_stream,
            } => MyNode::handle_node_msg(node, context, msg_id, msg, origin, send_stream).await,
            Cmd::ProcessClientMsg {
                msg_id,
                msg,
                auth,
                origin,
                send_stream,
            } => {
                trace!("Client msg {msg_id:?} reached its destination.");

                // TODO: clarify this err w/ peer
                let Some(stream) = send_stream else {
                        error!("No stream for client tho....");
                        return Err(Error::NoClientResponseStream);
                    };
                MyNode::handle_client_msg_for_us(context, msg_id, msg, auth, origin, stream).await
            }
            Cmd::ProcessAeMsg {
                msg_id,
                kind,
                section_tree_update,
                origin,
            } => {
                trace!("Handling msg: AE from {origin}: {msg_id:?}");
                MyNode::handle_anti_entropy_msg(node, context, section_tree_update, kind, origin)
                    .await
            }
            Cmd::HandleMsg {
                origin,
                wire_msg,
                send_stream,
            } => MyNode::handle_msg(context, origin, wire_msg, send_stream).await,
            Cmd::UpdateNetworkAndHandleValidClientMsg {
                proof_chain,
                signed_sap,
                msg_id,
                msg,
                origin,
                auth,
                send_stream,
            } => {
                debug!("Updating network knowledge before handling message");
                // we create a block to make sure the node's lock is released
                let updated = {
                    let name = node.name();
                    node.network_knowledge.update_knowledge_if_valid(
                        SectionTreeUpdate::new(signed_sap, proof_chain),
                        None,
                        &name,
                    )?
                };
                info!("Network knowledge was updated: {updated}");

                let context = if updated { node.context() } else { context };

                MyNode::handle_client_msg_for_us(context, msg_id, msg, auth, origin, send_stream)
                    .await
            }
            Cmd::HandleSectionDecisionAgreement { proposal, sig } => {
                node.handle_section_decision_agreement(proposal, sig)
            }
            Cmd::HandleMembershipDecision(decision) => {
                node.handle_membership_decision(decision).await
            }
            Cmd::HandleNewEldersAgreement { new_elders, sig } => {
                node.handle_new_elders_agreement(new_elders, sig).await
            }
            Cmd::HandleNewSectionsAgreement {
                sap1,
                sig1,
                sap2,
                sig2,
            } => {
                node.handle_new_sections_agreement(sap1, sig1, sap2, sig2)
                    .await
            }
            Cmd::HandleCommsError { peer, error } => {
                trace!("Comms error {error}");
                node.handle_comms_error(peer, error);
                Ok(vec![])
            }
            Cmd::HandleDkgOutcome {
                section_auth,
                outcome,
            } => node.handle_dkg_outcome(section_auth, outcome).await,
            Cmd::EnqueueDataForReplication {
                recipient,
                data_batch,
            } => {
                self.data_replication_sender
                    .send((data_batch, recipient))
                    .await
                    .map_err(|_| Error::DataReplicationChannel)?;
                Ok(vec![])
            }
            Cmd::ProposeVoteNodesOffline(names) => node.cast_offline_proposals(&names),
            Cmd::SetJoinsAllowed(joins_allowed) => {
                node.joins_allowed = joins_allowed;
                Ok(vec![])
            }
            Cmd::SetJoinsAllowedUntilSplit(joins_allowed_until_split) => {
                node.joins_allowed = joins_allowed_until_split;
                node.joins_allowed_until_split = joins_allowed_until_split;
                Ok(vec![])
            }
        };

        let elapsed = start.elapsed();
        trace!("Running Cmd {cmd_string:?} took {:?}", elapsed);

        result
    }
}
