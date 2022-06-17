// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Error, Result};

use sn_dbc::{
    Hash, IndexedSignatureShare, KeyImage, RingCtTransaction, SpentProofContent, SpentProofShare,
};
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{
            CmdError, DataCmd, DataQuery, EditRegister, Error as ErrorMsg, ServiceMsg,
            SignedRegisterEdit, SpentbookCmd,
        },
        system::{NodeQueryResponse, SystemMsg},
        AuthorityProof, DstLocation, EndUser, MsgId, ServiceAuth, WireMsg,
    },
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        Keypair, Peer, PublicKey, RegisterCmd, ReplicatedData, SPENTBOOK_TYPE_TAG,
    },
};

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
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

        trace!("data query response at adult is: {:?}", response);
        let msg = SystemMsg::NodeQueryResponse {
            response,
            correlation_id,
            user,
        };

        // Setup node authority on this response and send this back to our elders
        let section_pk = self.network_knowledge().section_key();
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
        correlation_id: MsgId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_node_pk: PublicKey,
    ) -> Result<Vec<Cmd>> {
        let msg_id = MsgId::new();
        let mut cmds = vec![];
        let op_id = response.operation_id()?;
        debug!(
            "Handling data read @ elders, received from {:?}, op id: {:?}",
            sending_node_pk, op_id
        );

        let node_id = XorName::from(sending_node_pk);

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
            trace!(
                "Node {:?}, reported data not found {:?}",
                sending_node_pk,
                op_id
            );
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
        // extract the data from the request
        let data = match msg {
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::Register(cmd)) => ReplicatedData::RegisterWrite(cmd),
            ServiceMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend { key_image, tx })) => {
                // generate and sign spent proof share
                let spent_proof_share = self.gen_spent_proof_share(&key_image, &tx).await?;

                // store spent proof share to adults
                let reg_cmd = gen_register_cmd(&key_image, &spent_proof_share)?;
                ReplicatedData::SpentbookWrite(reg_cmd)
            }
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => ReplicatedData::Chunk(chunk),
            ServiceMsg::Query(query) => {
                return self
                    .read_data_from_adults(query, msg_id, auth, origin)
                    .await;
            }
            _ => {
                warn!(
                    "!!!! Unexpected ServiceMsg received, and it was not handled: {:?}",
                    msg
                );
                return Ok(vec![]);
            }
        };
        // build the replication cmds
        let mut cmds = self.replicate_data(data).await?;
        // make sure the expected replication factor is achieved
        if data_copy_count() > cmds.len() {
            error!("InsufficientAdults for storing data reliably");
            let error = CmdError::Data(ErrorMsg::InsufficientAdults {
                prefix: self.network_knowledge().prefix(),
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

        if self.is_not_elder() {
            error!("Received unexpected message while Adult: {:?}", msg_id);
            return Ok(vec![]);
        }

        self.handle_service_msg_received(msg_id, msg, auth, user)
            .await
    }

    // Private helper to generate spent proof share
    async fn gen_spent_proof_share(
        &self,
        key_image: &KeyImage,
        tx: &RingCtTransaction,
    ) -> Result<SpentProofShare> {
        // TODO:
        // 1- perform all validations on the key image and tx received
        // 2- if everything is ok then sign the spent proof

        let sap = self.network_knowledge.authority_provider();
        let current_section_key = sap.section_key();
        let spentbook_pks = sap.public_key_set();

        // FIXME!!!: populate with real commitments taken from tx
        let public_commitments = vec![];

        let content = SpentProofContent {
            key_image: *key_image,
            transaction_hash: Hash::from(tx.hash()),
            public_commitments,
        };

        let (index, sig_share) = self
            .section_keys_provider
            .sign_with(content.hash().as_ref(), &current_section_key)?;
        let spentbook_sig_share = IndexedSignatureShare::new(index as u64, sig_share);

        Ok(SpentProofShare {
            content,
            spentbook_pks,
            spentbook_sig_share,
        })
    }
}

// Helper to generate the RegisterCmd to write the SpentProofShare
// as an entry in the Spentbook (Register).
// TODO: store not only the SpentProofShare but also the linked Tx
fn gen_register_cmd(
    key_image: &KeyImage,
    spent_proof_share: &SpentProofShare,
) -> Result<RegisterCmd> {
    // TODO: use the node's own keypair and section key share for signatures
    let mut permissions = BTreeMap::new();
    let _ = permissions.insert(User::Anyone, Permissions::new(true));
    let keypair = Keypair::new_ed25519();
    let owner = User::Key(keypair.public_key());
    let policy = Policy { owner, permissions };

    let mut register = Register::new(
        owner,
        XorName::from_content(&key_image.to_bytes()),
        SPENTBOOK_TYPE_TAG,
        policy,
        u16::MAX,
    );

    let entry = Bytes::from(rmp_serde::to_vec_named(spent_proof_share).map_err(|err| {
        Error::SpentbookError(format!(
            "Failed to serialise SpentProofShare to insert it into the spentbook (Register): {:?}",
            err
        ))
    })?);

    let (_, op) = register.write(entry.to_vec(), BTreeSet::default())?;
    let op = EditRegister {
        address: *register.address(),
        edit: op,
    };

    let signature = keypair.sign(&bincode::serialize(&op)?);
    let signed_edit = SignedRegisterEdit {
        op,
        auth: ServiceAuth {
            public_key: keypair.public_key(),
            signature,
        },
    };

    Ok(RegisterCmd::Edit(signed_edit))
}
