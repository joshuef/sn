// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::data_copy_count;
use crate::messaging::{
    data::{CmdError, DataCmd, DataQuery, Error as ErrorMsg, ServiceMsg},
    system::{NodeQueryResponse, SystemMsg},
    DstLocation, EndUser, MsgId, MsgKind, NodeAuth, WireMsg,
};
use crate::messaging::{AuthorityProof, ServiceAuth};
use crate::node::{api::cmds::Cmd, core::Core, Result};
use crate::peer::Peer;
use crate::types::{log_markers::LogMarker, register::User, PublicKey, ReplicatedData};

use itertools::Itertools;
use std::{cmp::Ordering, collections::BTreeSet};
use xor_name::XorName;

impl Core {
    /// Forms a CmdError msg to send back to the client
    pub(crate) async fn send_cmd_error_response(
        &self,
        error: CmdError,
        target: Peer,
        msg_id: MsgId,
    ) -> Result<Vec<Cmd>> {
        let the_error_msg = ServiceMsg::CmdError {
            error,
            correlation_id: msg_id,
        };
        self.send_cmd_response(target, the_error_msg).await
    }

    /// Forms a CmdAck msg to send back to the client
    pub(crate) async fn send_cmd_ack(&self, target: Peer, msg_id: MsgId) -> Result<Vec<Cmd>> {
        let the_ack_msg = ServiceMsg::CmdAck {
            correlation_id: msg_id,
        };
        self.send_cmd_response(target, the_ack_msg).await
    }

    /// Forms a cmd to send a cmd response error/ack to the client
    async fn send_cmd_response(&self, target: Peer, msg: ServiceMsg) -> Result<Vec<Cmd>> {
        let dst = DstLocation::EndUser(EndUser(target.name()));

        let (msg_kind, payload) = self.ed_sign_client_msg(&msg).await?;
        let wire_msg = WireMsg::new_msg(MsgId::new(), payload, msg_kind, dst)?;

        let cmd = Cmd::SendMsg {
            recipients: vec![target],
            wire_msg,
        };

        Ok(vec![cmd])
    }

    /// Sign and serialize system message to be sent
    pub(crate) async fn sign_system_msg(
        &self,
        msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<Vec<Cmd>> {
        let msg_id = MsgId::new();
        let section_pk = self.network_knowledge().section_key().await;
        let payload = WireMsg::serialize_msg_payload(&msg)?;

        let auth = NodeAuth::authorize(section_pk, &self.node.read().await.keypair, &payload);
        let msg_kind = MsgKind::NodeAuthMsg(auth.into_inner());

        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

        Ok(vec![Cmd::SendWireMsgToNodes(wire_msg)])
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_at_adult(
        &self,
        correlation_id: MsgId,
        query: &DataQuery,
        auth: ServiceAuth,
        user: EndUser,
        requesting_elder: XorName,
    ) -> Result<Vec<Cmd>> {
        trace!("Handling data query at adult");
        let mut cmds = vec![];

        let response = self
            .data_storage
            .query(query, User::Key(auth.public_key))
            .await;

        trace!("data query response at adult is:  {:?}", response);

        let msg = SystemMsg::NodeQueryResponse {
            response,
            correlation_id,
            user,
        };

        // Setup node authority on this response and send this back to our elders
        let section_pk = self.network_knowledge().section_key().await;
        let dst = DstLocation::Node {
            name: requesting_elder,
            section_pk,
        };

        cmds.push(Cmd::SignOutgoingSystemMsg { msg, dst });

        Ok(cmds)
    }

    /// Handle data read
    /// Records response in liveness tracking
    /// Forms a response to send to the requester
    pub(crate) async fn handle_data_query_response_at_elder(
        &self,
        // msg_id: MsgId,
        correlation_id: MsgId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_node_pk: PublicKey,
    ) -> Result<Vec<Cmd>> {
        let msg_id = MsgId::new();
        let mut cmds = vec![];
        debug!(
            "Handling data read @ elders, received from {:?} ",
            sending_node_pk
        );

        let node_id = XorName::from(sending_node_pk);
        let op_id = response.operation_id()?;
        let origin = if let Some(origin) = self.pending_data_queries.remove(&op_id).await {
            origin
        } else {
            warn!(
                "Dropping chunk query response from Adult {}. We might have already forwarded this chunk to the requesting client or \
                have not registered the client: {}",
                sending_node_pk, user.0
            );
            return Ok(cmds);
        };

        // Clear expired queries from the cache.
        self.pending_data_queries.remove_expired().await;

        let query_response = response.convert();

        let pending_removed = match query_response.operation_id() {
            Ok(op_id) => {
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
            cmds.push(Cmd::ProposeOffline(name));
        }

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return Ok(cmds);
        }

        // Send response if one is warrented
        if query_response.failed_with_data_not_found()
            || (!query_response.is_success()
                && self
                    .capacity
                    .is_full(&XorName::from(sending_node_pk))
                    .await
                    .unwrap_or(false))
        {
            // we don't return data not found errors.
            trace!("Node {:?}, reported data not found", sending_node_pk);

            return Ok(cmds);
        }

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };
        let (msg_kind, payload) = self.ed_sign_client_msg(&msg).await?;

        let dst = DstLocation::EndUser(EndUser(origin.name()));
        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst)?;

        trace!(
            "Responding with the first chunk query response to {:?}",
            dst
        );

        cmds.push(Cmd::SendMsg {
            recipients: vec![origin],
            wire_msg,
        });

        Ok(cmds)
    }

    /// Handle ServiceMsgs received from EndUser
    pub(crate) async fn handle_service_msg_received(
        &self,
        msg_id: MsgId,
        msg: ServiceMsg,
        auth: AuthorityProof<ServiceAuth>,
        origin: Peer,
    ) -> Result<Vec<Cmd>> {
        if self.is_not_elder().await {
            error!("Received unexpected message while Adult");
            return Ok(vec![]);
        }
        // extract the data from the request
        let data = match msg {
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::Register(cmd)) => ReplicatedData::RegisterWrite(cmd),
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => ReplicatedData::Chunk(chunk),
            ServiceMsg::Query(query) => {
                return self
                    .read_data_from_adults(query, msg_id, auth, origin)
                    .await
            }
            _ => {
                warn!("!!!! Unexpected ServiceMsg received in routing. Was not sent to node layer: {:?}", msg);
                return Ok(vec![]);
            }
        };
        // build the replication cmds
        let mut cmds = self.replicate_data(data).await?;
        // make sure the expected replication factor is achieved
        if data_copy_count() > cmds.len() {
            let error = CmdError::Data(ErrorMsg::InsufficientAdults {
                prefix: self.network_knowledge().prefix().await,
                expected: data_copy_count() as u8,
                found: cmds.len() as u8,
            });
            return self.send_cmd_error_response(error, origin, msg_id).await;
        }
        cmds.extend(self.send_cmd_ack(origin, msg_id).await?);
        Ok(cmds)
    }

    // Used to fetch the list of holders for given data name.
    pub(crate) async fn get_adults_holding_data(&self, target: &XorName) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults().await;

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let mut candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
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

    // Used to fetch the list of holders for given name of data.
    pub(crate) async fn get_adults_who_should_store_data(
        &self,
        target: XorName,
    ) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults().await;

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        debug!(
            "Target chunk holders of {:?} are empty adults: {:?} and full adults that were ignored: {:?}",
            target,
            candidates,
            full_adults
        );

        candidates
    }

    /// Handle incoming data msgs.
    pub(crate) async fn handle_service_msg(
        &self,
        msg_id: MsgId,
        msg: ServiceMsg,
        dst_location: DstLocation,
        auth: AuthorityProof<ServiceAuth>,
        user: Peer,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?} {:?}", LogMarker::ServiceMsgToBeHandled, msg);
        if let DstLocation::EndUser(_) = dst_location {
            warn!(
                "Service msg has been dropped as its destination location ({:?}) is invalid: {:?}",
                dst_location, msg
            );
            return Ok(vec![]);
        }

        self.handle_service_msg_received(msg_id, msg, auth, user)
            .await
    }
}
