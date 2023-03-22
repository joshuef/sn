// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::fault_detection::FaultsCmd, messaging::Recipients, Cmd, Error, MyNode, Result,
};
use crate::storage::{DataStorage, Error as StorageError, StorageLevel};

use bytes::BufMut;
use ed25519_dalek::Keypair as EdKeypair;
use itertools::Itertools;
use qp2p::SendStream;
use sn_dbc::SpentProofShare;
use sn_interface::network_knowledge::NetworkKnowledge;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{DataResponse, EditRegister, SignedRegisterEdit},
        system::{NodeDataCmd, NodeEvent, NodeMsg},
        ClientAuth, MsgId, MsgKind, WireMsg,
    },
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        ClientId, DataError, Keypair, NodeId, Participant, PublicKey, RegisterCmd, ReplicatedData,
        SPENTBOOK_TYPE_TAG,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tracing::info;
use xor_name::XorName;

impl MyNode {
    /// Find target node, sends a bidi msg, awaiting response, and then sends this on to the client
    pub(crate) fn forward_data_and_respond_to_client(
        network_knowledge: &NetworkKnowledge,
        wire_msg: WireMsg,
        client_id: ClientId,
        client_stream: SendStream,
    ) -> Cmd {
        let msg_id = wire_msg.msg_id();
        // We accept that we might be sending a WireMsg to ourselves.
        // The extra load is not that big. But we can optimize this later if necessary.

        let target_addr = wire_msg.dst().name;

        let kind = wire_msg.kind();
        let query_index = match kind {
            MsgKind::Client {
                auth: _,
                is_spend: _,
                query_index,
            } => *query_index,
            _ => None,
        };

        let targets = Self::target_data_holders(&network_knowledge, target_addr, query_index);

        // make sure the expected replication factor is achieved
        if query_index.is_none() && data_copy_count() > targets.len() {
            error!(
                "InsufficientNodeCount for storing data reliably for {msg_id:?}, {:?}",
                targets.len()
            );
            let error = DataError::InsufficientNodeCount {
                prefix: network_knowledge.prefix(),
                expected: data_copy_count() as u8,
                found: targets.len() as u8,
            };

            let data_response = DataResponse::NetworkIssue(error);

            return MyNode::send_cmd_error_response_over_stream(
                data_response,
                msg_id,
                client_stream,
                client_id,
            );
        }

        // error out if a query and the index does not exist
        if let Some(index) = query_index {
            // could not find our desired index
            if targets.is_empty() {
                error!(
                    "InsufficientNodeCount for querying reliably for {msg_id:?} index: {index:?}"
                );
                let error = DataError::InsufficientNodeCount {
                    prefix: network_knowledge.prefix(),
                    expected: index as u8 + 1, // plus one here as we're 0 index
                    found: network_knowledge.members().len() as u8,
                };

                let data_response = DataResponse::NetworkIssue(error);

                return MyNode::send_cmd_error_response_over_stream(
                    data_response,
                    msg_id,
                    client_stream,
                    client_id,
                );
            }
        }

        Cmd::SendAndForwardResponseToClient {
            wire_msg,
            targets,
            client_stream,
            client_id,
        }
    }

    /// Registered holders not present in provided list of members
    /// will no longer be tracked for faults.
    pub(crate) async fn fault_detection_retain_only(
        &mut self,
        adults: BTreeSet<XorName>,
        elders: BTreeSet<XorName>,
    ) {
        // stop tracking liveness of absent holders
        if let Err(error) = self
            .fault_cmds_sender
            .send(FaultsCmd::UpdateNodes(adults, elders))
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

    /// Select targets to send out the SpentProofShare for storing to spentbook
    /// on storage nodes. The response is then forwarded back on to the client.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn forward_spent_share(
        msg_id: MsgId,
        share: SpentProofShare,
        public_key: bls::PublicKey,
        client_id: ClientId,
        client_stream: SendStream,
        our_name: XorName,
        network_knowledge: &NetworkKnowledge,
        ed_keypair: Arc<EdKeypair>,
    ) -> Result<Vec<Cmd>> {
        debug!("{msg_id:?} Forwarding SpentProofShare for Spentbook.");

        let reg_cmd = gen_register_cmd(share, public_key, ed_keypair)?;
        let name = reg_cmd.name();
        let node_msg = NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(
            ReplicatedData::SpentbookWrite(reg_cmd),
        ));
        let section_key = network_knowledge.section_auth_by_name(&name)?.section_key();
        let dst = sn_interface::messaging::Dst { name, section_key };
        let (kind, payload) = MyNode::serialize_node_msg(our_name, &node_msg)?;
        let wire_msg = WireMsg::new_msg(msg_id, payload, kind, dst);

        let targets = MyNode::target_data_holders(network_knowledge, name, None);
        debug!("{msg_id:?} Forwarding SpentProofShare for Spentbook to data holders: {targets:?}");

        Ok(vec![Cmd::SendAndForwardResponseToClient {
            wire_msg,
            targets,
            client_stream,
            client_id,
        }])
    }

    /// Used to fetch the list of holders for given name of data.
    /// Sorts members by closeness to data address, returns data_copy_count of them
    fn target_data_holders(
        network_knowledge: &NetworkKnowledge,
        target: XorName,
        query_index: Option<usize>,
    ) -> BTreeSet<NodeId> {
        // TODO: reuse our_members_sorted_by_distance_to API when core is merged into upper layer
        let members = network_knowledge.members();

        debug!("Total members known about: {:?}", members.len());

        let candidates = members
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(&lhs.name(), &rhs.name()))
            .take(data_copy_count())
            .enumerate()
            .filter(|(i, _node)| {
                if let Some(index) = query_index {
                    i == &index
                } else {
                    // always return them
                    true
                }
            })
            .map(|(_i, p)| p)
            .collect::<BTreeSet<_>>();

        debug!("Target holders of {:?} are : {:?}", target, candidates,);

        candidates
    }

    /// Replicate data in the batch locally and then trigger further update reqeusts
    /// Requests for more data will go to sending node if there is more to come, or to the next
    /// furthest nodes if there was no data sent.
    pub(crate) async fn replicate_data_batch(
        ed_keypair: Arc<EdKeypair>,
        data_storage: DataStorage,
        network_knowledge: &NetworkKnowledge,
        joins_allowed: bool,
        sender: NodeId,
        data_batch: Vec<ReplicatedData>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        let section_pk = PublicKey::Bls(network_knowledge.section_key());
        let node_keypair = Keypair::Ed25519(ed_keypair.clone());

        let mut is_full = false;
        let data_batch_is_empty = data_batch.is_empty();

        let mut new_storage_level_passed = false;

        for data in data_batch {
            let store_result = data_storage
                .store(&data, section_pk, node_keypair.clone())
                .await;

            // This may return a DatabaseFull error... but we should have reported StorageError::NotEnoughSpace
            // well before this
            match store_result {
                Ok(StorageLevel::NoChange) => trace!("Data item stored."),
                Ok(StorageLevel::Updated(_level)) => {
                    trace!("Data item stored.");
                    // we add a new node for every level of used space increment
                    if !new_storage_level_passed && !joins_allowed {
                        new_storage_level_passed = true;
                        cmds.push(Cmd::SetJoinsAllowed(true));
                    }
                }
                Err(StorageError::NotEnoughSpace) => {
                    // storage full
                    error!("Not enough space to store more data");

                    let node_id = node_keypair.public_key();
                    let msg = NodeMsg::NodeEvent(NodeEvent::CouldNotStoreData {
                        node_id,
                        data_address: data.address(),
                        full: true,
                    });
                    is_full = true;

                    cmds.push(MyNode::send_to_elders(network_knowledge, msg))
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
            let data_i_have = data_storage.data_addrs().await;
            trace!(
                "{:?} - as batch was not empty",
                LogMarker::DataReorganisationUnderway
            );
            let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
            let cmd = Cmd::send_msg(msg, Recipients::Single(Participant::from_node(sender)));
            cmds.push(cmd);
        } else if is_full {
            warn!("Not attempting further retrieval of missing data as we are full");
        }

        Ok(cmds)
    }
}

/// Generate the RegisterCmd to write the SpentProofShare as an entry in the Spentbook
/// (Register).
fn gen_register_cmd(
    spent_proof_share: SpentProofShare,
    public_key: bls::PublicKey,
    ed_keypair: Arc<EdKeypair>,
) -> Result<RegisterCmd> {
    let mut permissions = BTreeMap::new();
    let _ = permissions.insert(User::Anyone, Permissions::new(true));

    // use our own keypair for generating the register command
    let own_keypair = Keypair::Ed25519(ed_keypair);
    let owner = User::Key(own_keypair.public_key());
    let policy = Policy { owner, permissions };

    let mut register = Register::new(
        owner,
        XorName::from_content(&public_key.to_bytes()),
        SPENTBOOK_TYPE_TAG,
        policy,
    );
    let mut entry = vec![].writer();
    rmp_serde::encode::write(&mut entry, &spent_proof_share).map_err(|err| {
        Error::SpentbookError(format!(
            "Failed to serialise SpentProofShare to insert it into the spentbook (Register): {err:?}",
        ))
    })?;

    let (_, op) = register.write(entry.into_inner(), BTreeSet::default())?;
    let op = EditRegister {
        address: *register.address(),
        edit: op,
    };

    let signature = own_keypair.sign(&bincode::serialize(&op)?);
    let signed_edit = SignedRegisterEdit {
        op,
        auth: ClientAuth {
            public_key: own_keypair.public_key(),
            signature,
        },
    };

    debug!("Successfully generated spent proof share for spend request");
    Ok(RegisterCmd::Edit(signed_edit))
}
