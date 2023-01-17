// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::fault_detection::FaultsCmd, messaging::Peers, Cmd, Error, MyNode,
    Result,
};
use crate::storage::{Error as StorageError, StorageLevel};

use sn_interface::{
    data_copy_count,
    messaging::{
        data::{DataCmd, DataQuery},
        system::{NodeDataCmd, NodeDataQuery, NodeEvent, NodeMsg, OperationId},
        AuthorityProof, ClientAuth, MsgId,
    },
    types::{log_markers::LogMarker, Keypair, Peer, PublicKey, ReplicatedData},
};

use qp2p::SendStream;
use xor_name::XorName;

use bytes::Bytes;
use itertools::Itertools;
use std::collections::BTreeSet;
use tracing::info;

impl MyNode {
    // Instruct data holders to store the data awaiting for their confirmation response to ack the client
    pub(crate) async fn store_data_at_nodes_and_ack_to_client(
        context: NodeContext,
        data_cmd: DataCmd,
        data: ReplicatedData,
        msg_id: MsgId,
        client_stream: SendStream,
        source_client: Peer,
    ) -> Result<Vec<Cmd>> {
        let data_name = data.name();
        let targets = Self::target_data_holders(&context, data_name);

        // make sure the expected replication factor is achieved
        if data_copy_count() > targets.len() {
            error!("InsufficientNodeCount for storing data reliably for {msg_id:?}");
            let error = Error::InsufficientNodeCount {
                prefix: context.network_knowledge.prefix(),
                expected: data_copy_count() as u8,
                found: targets.len() as u8,
            };

            debug!("Will send error response back to client");

            let cmd = MyNode::send_cmd_error_response_over_stream(
                context,
                data_cmd,
                error,
                msg_id,
                client_stream,
                source_client,
            );
            return Ok(vec![cmd]);
        }

        info!("Replicating data from {msg_id:?} {data_name:?} to holders: {targets:?}");

        // TODO: general ReplicateData flow could go bidi?
        // Right now we've a new msg for just one datum.
        // Atm that's perhaps more bother than its worth..
        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data));

        let cmd = Cmd::SendMsgAwaitResponseAndRespondToClient {
            msg_id,
            msg,
            context,
            targets,
            client_stream,
            source_client,
        };

        Ok(vec![cmd])
    }

    /// Find target node, sends a bidi msg, awaiting response, and then sends this on to the client
    pub(crate) async fn read_data_and_respond_to_client(
        context: NodeContext,
        query: DataQuery,
        msg_id: MsgId,
        auth: AuthorityProof<ClientAuth>,
        source_client: Peer,
        client_stream: SendStream,
    ) -> Result<Vec<Cmd>> {
        // We accept that we might be sending a WireMsg to ourselves.
        // The extra load is not that big. But we can optimize this later if necessary.

        // We generate the operation id to track the response from the node
        // by using the query msg id, which shall be unique per query.
        let operation_id = OperationId::from(&Bytes::copy_from_slice(msg_id.as_ref()));
        let address = query.variant.address();
        trace!(
            "{:?} preparing to query other nodes for data at {address:?} with op_id: {operation_id:?}",
            LogMarker::DataQueryReceviedAtElder,
        );

        let targets = Self::target_data_holders(&context, *address.name());

        // We accept the chance that we will be querying an Elder that the client already queried directly.
        // The extra load is not that big. But we can optimize this later if necessary.

        // Query only the nth node
        let recipient = if let Some(peer) = targets.iter().nth(query.node_index) {
            *peer
        } else {
            debug!("No targets found for {msg_id:?}");
            let error = Error::InsufficientNodeCount {
                prefix: context.network_knowledge.prefix(),
                expected: query.node_index as u8 + 1,
                found: targets.len() as u8,
            };

            let cmd = MyNode::send_query_error_response_over_stream(
                context,
                error,
                &query.variant,
                source_client,
                msg_id,
                client_stream,
            );
            return Ok(vec![cmd]);
        };

        // Form a msg to the node
        let msg = NodeMsg::NodeDataQuery(NodeDataQuery {
            query: query.variant,
            auth: auth.into_inner(),
            operation_id,
        });

        let cmd = Cmd::SendMsgAwaitResponseAndRespondToClient {
            msg_id,
            msg,
            context,
            targets: BTreeSet::from([recipient]),
            client_stream,
            source_client,
        };

        Ok(vec![cmd])
    }

    /// Registered holders not present in provided list of members
    /// will no longer be tracked for liveness.
    pub(crate) async fn liveness_retain_only(&mut self, members: BTreeSet<XorName>) {
        // stop tracking liveness of absent holders
        if let Err(error) = self
            .fault_cmds_sender
            .send(FaultsCmd::RetainNodes(members))
            .await
        {
            warn!("Could not send RetainNodes through fault_cmds_tx: {error}");
        };
    }

    /// Adds the new adult to the Liveness trackers.
    pub(crate) async fn add_new_adult_to_trackers(&mut self, adult: XorName) {
        info!("Adding new Adult: {adult} to trackers");
        if let Err(error) = self.fault_cmds_sender.send(FaultsCmd::AddNode(adult)).await {
            warn!("Could not send AddNode through fault_cmds_tx: {error}");
        };
    }

    /// Used to fetch the list of holders for given name of data.
    /// Sorts members by closeness to data address, returns data_copy_count of them
    fn target_data_holders(context: &NodeContext, target: XorName) -> BTreeSet<Peer> {
        // TODO: reuse our_members_sorted_by_distance_to API when core is merged into upper layer
        let members = context.network_knowledge.members();

        trace!("Total members known about: {:?}", members.len());

        let candidates = members
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!("Target holders of {:?} are : {:?}", target, candidates,);

        candidates
    }

    /// Replicate data in the batch locally and then trigger further update reqeusts
    /// Requests for more data will go to sending node if there is more to come, or to the next
    /// furthest nodes if there was no data sent.
    pub(crate) async fn replicate_data_batch(
        context: &NodeContext,
        sender: Peer,
        data_batch: Vec<ReplicatedData>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let section_pk = PublicKey::Bls(context.network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(context.keypair.clone());

        let mut is_full = false;
        let data_batch_is_empty = data_batch.is_empty();

        let mut new_storage_level_passed = false;

        for data in data_batch {
            let store_result = context
                .data_storage
                .store(&data, section_pk, node_keypair.clone())
                .await;

            // This may return a DatabaseFull error... but we should have reported StorageError::NotEnoughSpace
            // well before this
            match store_result {
                Ok(StorageLevel::NoChange) => debug!("Data item stored."),
                Ok(StorageLevel::Updated(_level)) => {
                    debug!("Data item stored.");
                    // we add a new node for every level of used space increment
                    if !new_storage_level_passed && !context.joins_allowed {
                        new_storage_level_passed = true;
                        cmds.push(Cmd::SetJoinsAllowed(true));
                    }
                }
                Err(StorageError::NotEnoughSpace) => {
                    // storage full
                    error!("Not enough space to store more data");

                    let node_id = PublicKey::from(context.keypair.public);
                    let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                        node_id,
                        data_address: data.address(),
                        full: true,
                    });
                    is_full = true;

                    cmds.push(MyNode::send_msg_to_our_elders(context, msg))
                }
                Err(error) => {
                    // the rest seem to be non-problematic errors.. (?)
                    error!("Problem storing data, but it was ignored: {error}");
                }
            }
        }

        // As long as the data batch is not empty, we send back a query again
        // to continue the replication process (like pageing).
        // This means there that there will be a number of repeated `give-me-data -> here_you_go` msg
        // exchanges, until there is no more data missing on this node.
        if !is_full && !data_batch_is_empty {
            let data_i_have = context.data_storage.data_addrs().await;
            let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
            let cmd = MyNode::send_system_msg(msg, Peers::Single(sender), context.clone());
            cmds.push(cmd);
        }

        Ok(cmds)
    }
}
