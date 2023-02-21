// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, Error, MyNode, Result};

use bytes::BufMut;
use qp2p::SendStream;
use sn_dbc::{
    get_public_commitments_from_transaction, Commitment, DbcTransaction, PublicKey, SpentProof,
    SpentProofShare,
};
use sn_interface::{
    messaging::{
        data::{
            ClientMsg, DataCmd, DataQuery, DataResponse, EditRegister, SignedRegisterEdit,
            SpentbookCmd,
        },
        AuthorityProof, ClientAuth, MsgId,
    },
    network_knowledge::{section_keys::build_spent_proof_share, SectionTreeUpdate},
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        Keypair, Peer, RegisterCmd, ReplicatedData, SPENTBOOK_TYPE_TAG,
    },
};

use std::collections::{BTreeMap, BTreeSet};
use xor_name::XorName;

impl MyNode {
    /// Forms a `CmdError` msg to send back to the client over the response stream
    pub(crate) fn send_cmd_error_response_over_stream(
        msg: DataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        source_client: Peer,
    ) -> Cmd {
        debug!("{correlation_id:?} sending cmd response error back to client");
        Cmd::send_data_response(msg, correlation_id, source_client, send_stream)
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_where_stored(
        msg_id: MsgId,
        query: &DataQuery,
        auth: ClientAuth,
        source_client: Peer,
        send_stream: SendStream,
        context: NodeContext,
    ) -> Vec<Cmd> {
        let response = context
            .data_storage
            .query(query, User::Key(auth.public_key))
            .await;

        trace!("{msg_id:?} data query response at node is: {response:?}");

        let msg = DataResponse::QueryResponse {
            response,
            correlation_id: msg_id,
        };

        vec![Cmd::send_data_response(
            msg,
            msg_id,
            source_client,
            send_stream,
        )]
    }

    /// Handle incoming client msgs.
    /// If this is a store request, and we are an Elder and one of
    /// the `data_copy_count()` nodes, then we will send a wiremsg
    /// to ourselves, among the msgs sent to the other holders.
    pub(crate) async fn handle_client_msg_for_us(
        mut context: NodeContext,
        msg_id: MsgId,
        msg: ClientMsg,
        auth: AuthorityProof<ClientAuth>,
        origin: Peer,
        send_stream: SendStream,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}: {msg_id:?} {msg:?}", LogMarker::ClientMsgToBeHandled);

        match msg {
            ClientMsg::Cmd(cmd) => {
                MyNode::handle_data_cmd(&mut context, cmd, msg_id, origin, auth, send_stream).await
            }
            ClientMsg::Query(query) => Ok(MyNode::handle_data_query_where_stored(
                msg_id,
                &query,
                auth.into_inner(),
                origin,
                send_stream,
                context,
            )
            .await),
        }
    }

    /// Handle the DataCmd variant
    async fn handle_data_cmd(
        context: &mut NodeContext,
        cmd: DataCmd,
        msg_id: MsgId,
        origin: Peer,
        auth: AuthorityProof<ClientAuth>,
        send_stream: SendStream,
    ) -> Result<Vec<Cmd>> {
        // extract the data from the request
        let data_result = match cmd.clone() {
            DataCmd::StoreChunk(chunk) => Ok(ReplicatedData::Chunk(chunk)),
            DataCmd::Register(cmd) => Ok(ReplicatedData::RegisterWrite(cmd)),
            DataCmd::Spentbook(cmd) => {
                let SpentbookCmd::Spend {
                    network_knowledge,
                    public_key,
                    tx,
                    spent_proofs,
                    spent_transactions,
                } = cmd.clone();
                if let Some((proof_chain, signed_sap)) = network_knowledge {
                    info!(
                        "Received updated network knowledge with the request. Will return new command \
                        to update the node network knowledge before processing the spend."
                    );
                    let name = context.name;
                    let there_was_an_update =
                        context.network_knowledge.update_sap_knowledge_if_valid(
                            SectionTreeUpdate::new(signed_sap.clone(), proof_chain.clone()),
                            &name,
                        )?;

                    if there_was_an_update {
                        // To avoid a loop, recompose the message without the updated proof_chain.
                        let updated_client_msg =
                            ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
                                public_key,
                                tx,
                                spent_proofs,
                                spent_transactions,
                                network_knowledge: None,
                            }));
                        let update_command = Cmd::UpdateNetworkAndHandleValidClientMsg {
                            proof_chain,
                            signed_sap,
                            msg_id,
                            msg: updated_client_msg,
                            origin,
                            send_stream,
                            auth,
                        };
                        return Ok(vec![update_command]);
                    }
                }
                // THis is not being forwarded
                MyNode::extract_spentproof_contents_as_replicated_data(context, cmd)
            }
        };

        // here we pull out spentbook writes and forward those as node data cmd
        match data_result {
            Ok(data) => {
                // Spentbook register cmds now need to be sent on to data holders
                if let ReplicatedData::SpentbookWrite(_) = &data {
                    return MyNode::forward_on_spentbook_cmd(
                        data,
                        context,
                        msg_id,
                        origin,
                        send_stream,
                    );
                }
                // TODO: This would mean all spendbook is at elders...
                MyNode::store_data_and_respond(context, data, send_stream, origin, msg_id).await
            }
            Err(error) => {
                let data_response = DataResponse::CmdResponse {
                    response: cmd.to_error_response(error.into()),
                    correlation_id: msg_id,
                };

                let cmd = MyNode::send_cmd_error_response_over_stream(
                    data_response,
                    msg_id,
                    send_stream,
                    origin,
                );
                Ok(vec![cmd])
            }
        }
    }
    // helper to extract the contents of the cmd as ReplicatedData
    fn extract_spentproof_contents_as_replicated_data(
        context: &NodeContext,
        cmd: SpentbookCmd,
    ) -> Result<ReplicatedData> {
        let SpentbookCmd::Spend {
            public_key,
            tx,
            spent_proofs,
            spent_transactions,
            ..
        } = cmd;

        info!("Processing spend request for public key: {:?}", public_key);

        let spent_proof_share = MyNode::gen_spent_proof_share(
            context,
            &public_key,
            &tx,
            &spent_proofs,
            &spent_transactions,
        )?;
        let reg_cmd = MyNode::gen_register_cmd(context, &public_key, &spent_proof_share)?;
        Ok(ReplicatedData::SpentbookWrite(reg_cmd))
    }

    /// Generate a spent proof share from the information provided by the client.
    fn gen_spent_proof_share(
        context: &NodeContext,
        public_key: &PublicKey,
        tx: &DbcTransaction,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<DbcTransaction>,
    ) -> Result<SpentProofShare> {
        // Verify spent proof signatures are valid.
        let mut spent_proofs_keys = BTreeSet::new();
        for proof in spent_proofs.iter() {
            if !proof
                .spentbook_pub_key
                .verify(&proof.spentbook_sig, proof.content.hash().as_ref())
            {
                let msg = format!(
                    "Spent proof signature {:?} is invalid",
                    proof.spentbook_pub_key
                );
                warn!("Dropping spend request: {msg}");
                return Err(Error::SpentbookError(msg));
            }
            let _ = spent_proofs_keys.insert(proof.spentbook_pub_key);
        }

        // Verify each spent proof is signed by a known section key (or the genesis key).
        for pk in &spent_proofs_keys {
            if !context.network_knowledge.verify_section_key_is_known(pk) {
                warn!(
                    "Dropping spend request: spent proof is signed by unknown section with public \
                    key {:?}",
                    pk
                );
                return Err(Error::SpentProofUnknownSectionKey(*pk));
            }
        }

        let public_commitments_info =
            get_public_commitments_from_transaction(tx, spent_proofs, spent_transactions)?;

        // Do not sign invalid TX.
        let tx_public_commitments: Vec<Commitment> = public_commitments_info
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect();

        if let Err(err) = tx.verify(&tx_public_commitments) {
            warn!("Dropping spend request: {:?}", err.to_string());
            return Err(Error::SpentbookError(err.to_string()));
        }

        // TODO:
        // Check the public_key wasn't already spent with a different TX (i.e. double spent)

        // Grab the commitment specific to the spent public key.
        let public_commitment: Commitment = public_commitments_info
            .into_iter()
            .find(|(k, _c)| k == public_key)
            .map(|(_k, c)| c)
            .ok_or_else(|| {
                let msg =
                    format!("There are no commitments for the given public key {public_key:?}",);
                warn!("Dropping spend request: {msg}");
                Error::SpentbookError(msg)
            })?;

        let spent_proof_share = build_spent_proof_share(
            public_key,
            tx,
            &context.network_knowledge.section_auth(),
            &context.section_keys_provider,
            public_commitment,
        )?;
        Ok(spent_proof_share)
    }

    /// Generate the RegisterCmd to write the SpentProofShare as an entry in the Spentbook
    /// (Register).
    fn gen_register_cmd(
        context: &NodeContext,
        public_key: &PublicKey,
        spent_proof_share: &SpentProofShare,
    ) -> Result<RegisterCmd> {
        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(User::Anyone, Permissions::new(true));

        // use our own keypair for generating the register command
        let own_keypair = Keypair::Ed25519(context.keypair.clone());
        let owner = User::Key(own_keypair.public_key());
        let policy = Policy { owner, permissions };

        let mut register = Register::new(
            owner,
            XorName::from_content(&public_key.to_bytes()),
            SPENTBOOK_TYPE_TAG,
            policy,
        );
        let mut entry = vec![].writer();
        rmp_serde::encode::write(&mut entry, spent_proof_share).map_err(|err| {
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
}
