// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Core;
use crate::dbs::convert_to_error_message as convert_db_error_to_error_message;
use crate::messaging::{
    data::{CmdError, DataCmd, DataQuery, QueryResponse, RegisterRead, RegisterWrite, ServiceMsg},
    system::{NodeQueryResponse, SystemMsg},
    AuthorityProof, DstLocation, EndUser, MessageId, MsgKind, NodeAuth, ServiceAuth, WireMsg,
};
use crate::peer::Peer;
use crate::routing::{
    core::capacity::CHUNK_COPY_COUNT, error::Result, log_markers::LogMarker,
    routing_api::command::Command,
};
use crate::types::{ChunkAddress, PublicKey};
use itertools::Itertools;
use std::{cmp::Ordering, collections::BTreeSet};
use xor_name::XorName;

impl Core {
    /// Forms a command to send the provided node error out
    pub(crate) fn send_cmd_error_response(
        &self,
        error: CmdError,
        target: Peer,
        msg_id: MessageId,
    ) -> Result<Vec<Command>> {
        let the_error_msg = ServiceMsg::CmdError {
            error,
            correlation_id: msg_id,
        };

        let dst = DstLocation::EndUser(EndUser(target.name()));

        // FIXME: define which signature/authority this message should really carry,
        // perhaps it needs to carry Node signature on a NodeMsg::QueryResponse msg type.
        // Giving a random sig temporarily
        let (msg_kind, payload) = Self::random_client_signature(&the_error_msg)?;
        let wire_msg = WireMsg::new_msg(MessageId::new(), payload, msg_kind, dst)?;

        let command = Command::SendMessage {
            recipients: vec![target],
            wire_msg,
        };

        Ok(vec![command])
    }

    /// Handle register commands
    pub(crate) async fn handle_register_write(
        &self,
        msg_id: MessageId,
        register_write: RegisterWrite,
        user: Peer,
        auth: AuthorityProof<ServiceAuth>,
    ) -> Result<Vec<Command>> {
        trace!(
            "{:?} preparing to write register {:?}",
            LogMarker::RegisterWrite,
            register_write.address(),
        );

        match self.register_storage.write(register_write, auth).await {
            Ok(_) => {
                info!("Successfully wrote Register from Message: {:?}", msg_id);
                Ok(vec![])
            }
            Err(error) => {
                trace!("Problem on writing Register! {:?}", error);
                let error = convert_db_error_to_error_message(error);

                let error = CmdError::Data(error);
                self.send_cmd_error_response(error, user, msg_id)
            }
        }
    }

    /// Handle register reads
    pub(crate) fn handle_register_read(
        &self,
        msg_id: MessageId,
        query: RegisterRead,
        user: Peer,
        auth: AuthorityProof<ServiceAuth>,
    ) -> Result<Vec<Command>> {
        trace!(
            "{:?} preparing to read {:?}",
            LogMarker::RegisterQueryReceived,
            query.dst_address(),
        );

        match self.register_storage.read(&query, auth.public_key) {
            Ok(response) => {
                if response.failed_with_data_not_found() {
                    // we don't return data not found errors.
                    return Ok(vec![]);
                }

                trace!(
                    "Responding to regsiter read, msg_id {:?} with {:?}",
                    msg_id,
                    response
                );
                let msg = ServiceMsg::QueryResponse {
                    response,
                    correlation_id: msg_id,
                };

                // FIXME: define which signature/authority this message should really carry,
                // perhaps it needs to carry Node signature on a NodeMsg::QueryResponse msg type.
                // Giving a random sig temporarily
                let (msg_kind, payload) = Self::random_client_signature(&msg)?;

                let dst = DstLocation::EndUser(EndUser(user.name()));
                let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

                let command = Command::SendMessage {
                    recipients: vec![user],
                    wire_msg,
                };

                Ok(vec![command])
            }
            Err(error) => {
                trace!("Problem on reading Register! {:?}", error);
                let error = convert_db_error_to_error_message(error);
                let error = CmdError::Data(error);

                self.send_cmd_error_response(error, user, msg_id)
            }
        }
    }

    /// Sign and serialize node message to be sent
    pub(crate) async fn prepare_node_msg(
        &self,
        msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<Vec<Command>> {
        let msg_id = MessageId::new();

        let section_pk = self.network_knowledge().section_key().await;

        let payload = WireMsg::serialize_msg_payload(&msg)?;

        let auth = NodeAuth::authorize(section_pk, &self.node.read().await.keypair, &payload);
        let msg_kind = MsgKind::NodeAuthMsg(auth.into_inner());

        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

        let command = Command::ParseAndSendWireMsg(wire_msg);

        Ok(vec![command])
    }

    /// Handle chunk read
    pub(crate) async fn handle_get_chunk_at_adult(
        &self,
        msg_id: MessageId,
        address: &ChunkAddress,
        user: EndUser,
        requesting_elder: XorName,
    ) -> Result<Vec<Command>> {
        trace!("Handling chunk read at adult");
        let mut commands = vec![];

        let msg = SystemMsg::NodeQueryResponse {
            response: self.chunk_storage.get(address),
            correlation_id: msg_id,
            user,
        };

        // Setup node authority on this response and send this back to our elders
        let section_pk = self.network_knowledge().section_key().await;
        let dst = DstLocation::Node {
            name: requesting_elder,
            section_pk,
        };

        commands.push(Command::PrepareNodeMsgToSend { msg, dst });

        Ok(commands)
    }

    /// Handle chunk read
    /// Records response in liveness tracking
    /// Forms a response to send to the requester
    pub(crate) async fn handle_chunk_query_response_at_elder(
        &self,
        // msg_id: MessageId,
        correlation_id: MessageId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_nodes_pk: PublicKey,
    ) -> Result<Vec<Command>> {
        let msg_id = MessageId::new();
        let mut commands = vec![];
        debug!(
            "Handling chunk read @ elders, received from {:?} ",
            sending_nodes_pk
        );

        let NodeQueryResponse::GetChunk(response) = response;

        let query_response = QueryResponse::GetChunk(response);

        let pending_removed = match query_response.operation_id() {
            Ok(op_id) => {
                let node_id = XorName::from(sending_nodes_pk);
                self.liveness
                    .request_operation_fulfilled(&node_id, op_id)
                    .await
            }
            Err(error) => {
                warn!("Node problems noted when retrieving data: {:?}", error);
                false
            }
        };

        // Check for unresponsive adults here.
        for (name, count) in self.liveness.find_unresponsive_nodes().await {
            warn!(
                "Node {} has {} pending ops. It might be unresponsive",
                name, count
            );
            commands.push(Command::ProposeOffline(name));
        }

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return Ok(commands);
        }

        // Send response if one is warrented
        if query_response.failed_with_data_not_found()
            || (!query_response.is_success()
                && self
                    .capacity
                    .is_full(&XorName::from(sending_nodes_pk))
                    .await
                    .unwrap_or(false))
        {
            // we don't return data not found errors.
            trace!("Node {:?}, reported data not found", sending_nodes_pk);

            return Ok(commands);
        }

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };

        let origin = if let Some(origin) = self.pending_chunk_queries.get(&user.0).await {
            origin
        } else {
            warn!(
                "Dropping chunk query response from {} for unknown origin: {}",
                sending_nodes_pk, user.0
            );
            return Ok(commands);
        };

        // FIXME: define which signature/authority this message should really carry,
        // perhaps it needs to carry Node signature on a NodeMsg::QueryResponse msg type.
        // Giving a random sig temporarily
        let (msg_kind, payload) = Self::random_client_signature(&msg)?;

        let dst = DstLocation::EndUser(EndUser(origin.name()));
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

        let command = Command::SendMessage {
            recipients: vec![origin],
            wire_msg,
        };
        commands.push(command);
        Ok(commands)
    }

    /// Handle ServiceMsgs received from EndUser
    pub(crate) async fn handle_service_msg_received(
        &self,
        msg_id: MessageId,
        msg: ServiceMsg,
        user: Peer,
        auth: AuthorityProof<ServiceAuth>,
    ) -> Result<Vec<Command>> {
        match msg {
            // Register
            // Commands to be handled at elder.
            ServiceMsg::Cmd(DataCmd::Register(register_write)) => {
                self.handle_register_write(msg_id, register_write, user, auth)
                    .await
            }
            ServiceMsg::Query(DataQuery::Register(read)) => {
                self.handle_register_read(msg_id, read, user, auth)
            }
            // These will only be received at elders.
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => {
                self.send_chunk_to_adults(chunk, msg_id, auth, user).await
            }
            ServiceMsg::Query(DataQuery::GetChunk(address)) => {
                self.read_chunk_from_adults(address, msg_id, user).await
            }
            _ => {
                warn!("!!!! Unexpected ServiceMsg received in routing. Was not sent to node layer: {:?}", msg);
                Ok(vec![])
            }
        }
    }

    // Used to fetch the list of holders for a given chunk.
    pub(crate) async fn get_adults_holding_chunk(&self, target: &XorName) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;

        debug!(">>> FULL ADULTS FOUND: {:?}", full_adults);
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults().await;

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let mut candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(CHUNK_COPY_COUNT)
            .collect::<BTreeSet<_>>();


        trace!(
            "Chunk holders of {:?} are empty adults: {:?} and full adults: {:?}",
            target,
            candidates,
            full_adults
        );

        // Full adults that are close to the chunk, shall still be considered as candidates
        // to allow chunks stored to empty adults can be queried when nodes become full.
        let close_full_adults = if let Some(closest_empty) = candidates.iter().next() {
            full_adults
                .iter()
                .filter_map(|name| {
                    if target.cmp_distance(name, closest_empty) == Ordering::Less {
                        Some(*name)
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>()
        } else {
            // In case there is no empty candidates, query all full_adults
            full_adults
        };

        candidates.extend(close_full_adults);
        candidates
    }

    /// Handle incoming data msgs.
    pub(crate) async fn handle_service_message(
        &self,
        msg_id: MessageId,
        auth: AuthorityProof<ServiceAuth>,
        msg: ServiceMsg,
        dst_location: DstLocation,
        user: Peer,
    ) -> Result<Vec<Command>> {
        trace!("{:?} {:?}", LogMarker::ServiceMsgToBeHandled, msg);
        if let DstLocation::EndUser(_) = dst_location {
            warn!(
                "Service msg has been dropped as its destination location ({:?}) is invalid: {:?}",
                dst_location, msg
            );
            return Ok(vec![]);
        }

        self.handle_service_msg_received(msg_id, msg, user, auth)
            .await
    }
}
