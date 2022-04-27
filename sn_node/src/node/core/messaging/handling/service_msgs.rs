// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Result};
use sn_interface::data_copy_count;
use sn_interface::messaging::{
    data::{CmdError, DataCmd, DataQuery, Error as ErrorMsg, ServiceMsg},
    system::{NodeQueryResponse, SystemMsg},
    AuthorityProof, DstLocation, EndUser, MsgId, ServiceAuth, WireMsg,
};
// use crate::node::{api::cmds::Cmd, core::Node, Result};
// use sn_interface::types::{log_markers::LogMarker, register::User, Peer, PublicKey, ReplicatedData};
use sn_interface::types::{
    log_markers::LogMarker, register::User, Peer, PublicKey, ReplicatedData,
};

use xor_name::XorName;

impl Node {
    /// Handle data query
    pub(crate) async fn handle_data_query_at_adult(
        &self,
        correlation_id: MsgId,
        query: &DataQuery,
        auth: ServiceAuth,
        user: EndUser,
        requesting_elder: XorName,
    ) -> Result<Vec<Cmd>> {
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

    /// see if a client is waiting in the pending queries for a response
    /// If not, we could remove its link eg.
    #[cfg(any(feature = "chunks", feature = "registers"))]
    pub(crate) async fn pending_data_queries_contains_client(&self, peer: &Peer) -> bool {
        // now we check if our peer is still waiting...
        for (_op_id, peer_vec) in self.pending_data_queries.get_items().await {
            let vec = peer_vec.object;
            if vec.contains(peer) {
                return true;
            }
        }

        false
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

        let querys_peers = self.pending_data_queries.remove(&op_id).await;
        // Clear expired queries from the cache.
        self.pending_data_queries.remove_expired().await;

        // First check for waiting peers. If no one is waiting, we drop the response
        let waiting_peers = if let Some(peers) = querys_peers {
            peers
        } else {
            warn!(
                "Dropping chunk query response from Adult {}. We might have already forwarded this chunk to the requesting client or the client connection cache has expired: {}",
                sending_node_pk, user.0
            );

            return Ok(cmds);
        };

        let query_response = response.convert();

        let pending_removed = self
            .dysfunction_tracking
            .request_operation_fulfilled(&node_id, op_id)
            .await;

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return Ok(cmds);
        }

        // dont reply if data not found (but do keep peers around...)
        if query_response.failed_with_data_not_found()
            || (!query_response.is_success()
                && self
                    .capacity
                    .is_full(&XorName::from(sending_node_pk))
                    .await
                    .unwrap_or(false))
        {
            // lets requeue waiting peers in case another adult has the data...
            // if no more responses come in this query should eventually time out
            // TODO: What happens if we keep getting queries / client for some data that's always not found?
            // We need to handle that
            let _prev = self
                .pending_data_queries
                .set(op_id, waiting_peers.clone(), None)
                .await;
            trace!("Node {:?}, reported data not found ", sending_node_pk);
            return Ok(cmds);
        }

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };
        let (msg_kind, payload) = self.ed_sign_client_msg(&msg).await?;

        for peer in waiting_peers.iter() {
            let dst = DstLocation::EndUser(EndUser(peer.name()));
            let wire_msg = WireMsg::new_msg(msg_id, payload.clone(), msg_kind.clone(), dst)?;

            debug!("Responding with the first query response to {:?}", dst);

            cmds.push(Cmd::SendMsg {
                recipients: vec![*peer],
                wire_msg,
            });
        }

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
            error!("InsufficientAdults for storing data reliably");
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
