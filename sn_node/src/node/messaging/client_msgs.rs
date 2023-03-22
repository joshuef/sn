// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{flow_ctrl::cmds::Cmd, Error, MyNode, Result},
    storage::DataStorage,
};

use ed25519_dalek::Keypair as EdKeypair;
use sn_dbc::{
    get_public_commitments_from_transaction, Commitment, DbcTransaction, PublicKey, SpentProof,
    SpentProofShare,
};
use sn_interface::{
    dbcs::DbcReason,
    messaging::{
        data::{ClientMsg, DataCmd, DataQuery, DataResponse, SpendQuery, SpentbookCmd},
        system::NodeQueryResponse,
        AuthorityProof, ClientAuth, MsgId,
    },
    network_knowledge::{
        section_keys::build_spent_proof_share, NetworkKnowledge, SectionKeysProvider,
        SectionTreeUpdate,
    },
    types::{log_markers::LogMarker, register::User, ClientId, ReplicatedData},
};

use qp2p::SendStream;
use std::collections::BTreeSet;
use std::sync::Arc;
use xor_name::XorName;
impl MyNode {
    /// Forms a `CmdError` msg to send back to the client over the response stream
    pub(crate) fn send_cmd_error_response_over_stream(
        msg: DataResponse,
        correlation_id: MsgId,
        send_stream: SendStream,
        client_id: ClientId,
    ) -> Cmd {
        debug!("{correlation_id:?} sending cmd response error back to client");
        Cmd::send_data_response(msg, correlation_id, client_id, send_stream)
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_where_stored(
        msg_id: MsgId,
        query: &DataQuery,
        auth: ClientAuth,
        client_id: ClientId,
        send_stream: SendStream,
        reward_key: PublicKey,
        store_cost: sn_dbc::Token,
        data_storage: DataStorage,
    ) -> Vec<Cmd> {
        let response = if let DataQuery::Spentbook(SpendQuery::GetFees(_)) = query {
            // We receive this directly from client, as an Elder, since `is_spend` is set to true (that is a very messy/confusing pattern, to be fixed).
            NodeQueryResponse::GetFees(Ok((reward_key, store_cost)))
        } else {
            data_storage.query(query, User::Key(auth.public_key)).await
        };

        trace!("{msg_id:?} data query response at node is: {response:?}");

        let msg = DataResponse::QueryResponse {
            response,
            correlation_id: msg_id,
        };

        vec![Cmd::send_data_response(msg, msg_id, client_id, send_stream)]
    }

    /// Handle incoming client msgs.
    /// If this is a store request, and we are an Elder and one of
    /// the `data_copy_count()` nodes, then we will send a wiremsg
    /// to ourselves, among the msgs sent to the other holders.
    pub(crate) async fn handle_client_msg_for_us(
        msg_id: MsgId,
        msg: ClientMsg,
        auth: AuthorityProof<ClientAuth>,
        client_id: ClientId,
        send_stream: SendStream,
        our_name: XorName,
        network_knowledge: &NetworkKnowledge,
        ed_keypair: Arc<EdKeypair>,
        section_keys_provider: &SectionKeysProvider,
        reward_key: PublicKey,
        store_cost: sn_dbc::Token,
        joins_allowed: bool,
        joins_allowed_until_split: bool,
        is_elder: bool,
        data_storage: DataStorage,
    ) -> Result<Vec<Cmd>> {
        trace!("{:?}: {msg_id:?} {msg:?}", LogMarker::ClientMsgToBeHandled);

        match msg {
            ClientMsg::Cmd(cmd) => {
                MyNode::handle_data_cmd(
                    cmd,
                    msg_id,
                    client_id,
                    auth,
                    send_stream,
                    our_name,
                    network_knowledge,
                    ed_keypair,
                    section_keys_provider,
                    reward_key,
                    store_cost,
                    joins_allowed,
                    joins_allowed_until_split,
                    is_elder,
                    data_storage,
                )
                .await
            }
            ClientMsg::Query(query) => Ok(MyNode::handle_data_query_where_stored(
                msg_id,
                &query,
                auth.into_inner(),
                client_id,
                send_stream,
                reward_key,
                store_cost,
                data_storage,
            )
            .await),
        }
    }

    /// Handle the DataCmd variant
    async fn handle_data_cmd(
        data_cmd: DataCmd,
        msg_id: MsgId,
        client_id: ClientId,
        auth: AuthorityProof<ClientAuth>,
        send_stream: SendStream,
        our_name: XorName,
        network_knowledge: &NetworkKnowledge,
        ed_keypair: Arc<EdKeypair>,
        section_keys_provider: &SectionKeysProvider,
        reward_key: PublicKey,
        store_cost: sn_dbc::Token,
        joins_allowed: bool,
        joins_allowed_until_split: bool,
        is_elder: bool,
        data_storage: DataStorage,
    ) -> Result<Vec<Cmd>> {
        // extract the data from the request
        let data_result: Result<ReplicatedData> = match data_cmd.clone() {
            DataCmd::StoreChunk(chunk) => Ok(ReplicatedData::Chunk(chunk)),
            DataCmd::Register(cmd) => Ok(ReplicatedData::RegisterWrite(cmd)),
            DataCmd::Spentbook(cmd) => {
                let SpentbookCmd::Spend {
                    network_knowledge: proof_chain_sap_dag,
                    public_key,
                    tx,
                    reason,
                    spent_proofs,
                    spent_transactions,
                } = cmd.clone();
                if let Some((proof_chain, signed_sap)) = proof_chain_sap_dag {
                    info!(
                        "Received updated network knowledge with the request. Will return new command \
                        to update the node network knowledge before processing the spend."
                    );
                    let there_was_an_update =
                        network_knowledge.clone().update_sap_knowledge_if_valid(
                            SectionTreeUpdate::new(signed_sap.clone(), proof_chain.clone()),
                            &our_name,
                        )?;

                    if there_was_an_update {
                        // To avoid a loop, recompose the message without the updated proof_chain.
                        let updated_client_msg =
                            ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
                                public_key,
                                tx,
                                reason,
                                spent_proofs,
                                spent_transactions,
                                network_knowledge: None,
                            }));
                        let update_command = Cmd::UpdateNetworkAndHandleValidClientMsg {
                            proof_chain,
                            signed_sap,
                            msg_id,
                            msg: updated_client_msg,
                            client_id,
                            send_stream,
                            auth,
                        };
                        return Ok(vec![update_command]);
                    }
                }

                // first we validate it here at the Elder
                let spent_share = match MyNode::validate_spentbook_cmd(
                    cmd,
                    &network_knowledge,
                    section_keys_provider,
                    reward_key,
                    store_cost,
                ) {
                    Ok(share) => share,
                    Err(e) => {
                        return MyNode::send_error(msg_id, data_cmd, e, send_stream, client_id)
                    }
                };

                // then we forward it to data holders
                return MyNode::forward_spent_share(
                    msg_id,
                    spent_share,
                    public_key,
                    client_id,
                    send_stream,
                    our_name,
                    network_knowledge,
                    ed_keypair,
                );
            }
        };

        match data_result {
            Ok(data) => {
                MyNode::store_data_and_respond(
                    &network_knowledge,
                    ed_keypair,
                    data_storage,
                    joins_allowed_until_split,
                    joins_allowed,
                    is_elder,
                    data,
                    send_stream,
                    client_id,
                    msg_id,
                )
                .await
            }
            Err(error) => MyNode::send_error(msg_id, data_cmd, error, send_stream, client_id),
        }
    }

    fn send_error(
        msg_id: MsgId,
        cmd: DataCmd,
        error: Error,
        send_stream: SendStream,
        client_id: ClientId,
    ) -> Result<Vec<Cmd>> {
        let data_response = DataResponse::CmdResponse {
            response: cmd.to_error_response(error.into()),
            correlation_id: msg_id,
        };
        let cmd = MyNode::send_cmd_error_response_over_stream(
            data_response,
            msg_id,
            send_stream,
            client_id,
        );
        Ok(vec![cmd])
    }

    fn validate_spentbook_cmd(
        cmd: SpentbookCmd,
        network_knowledge: &NetworkKnowledge,
        section_keys_provider: &SectionKeysProvider,
        reward_key: PublicKey,
        store_cost: sn_dbc::Token,
    ) -> Result<SpentProofShare> {
        let SpentbookCmd::Spend {
            public_key,
            tx,
            reason,
            spent_proofs,
            spent_transactions,
            ..
        } = cmd;

        info!("Processing spend request for public key: {:?}", public_key);

        let spent_proof_share = MyNode::gen_spent_proof_share(
            &public_key,
            &tx,
            reason,
            &spent_proofs,
            &spent_transactions,
            network_knowledge,
            section_keys_provider,
            reward_key,
            store_cost,
        )?;

        Ok(spent_proof_share)
    }

    /// Generate a spent proof share from the information provided by the client.
    fn gen_spent_proof_share(
        public_key: &PublicKey,
        tx: &DbcTransaction,
        reason: DbcReason,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<DbcTransaction>,
        network_knowledge: &NetworkKnowledge,
        section_keys_provider: &SectionKeysProvider,
        reward_key: PublicKey,
        store_cost: sn_dbc::Token,
    ) -> Result<SpentProofShare> {
        // verify that fee is paid (we are included as output)
        MyNode::verify_fee(store_cost, reward_key, tx)?;

        // verify the spent proofs
        MyNode::verify_spent_proofs(spent_proofs, &network_knowledge)?;

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
            reason,
            &network_knowledge.section_auth(),
            section_keys_provider,
            public_commitment,
        )?;

        Ok(spent_proof_share)
    }

    fn verify_fee(
        _store_cost: sn_dbc::Token,
        _our_key: PublicKey,
        _tx: &DbcTransaction,
    ) -> Result<()> {
        // TODO: check that we have an output to us, and that it is of sufficient value.

        // pseudo code:

        // let paid_to_us = match tx.get(our_key) {
        //     Some(output) => output.amount(),
        //     None => {
        //         return Err(Error::InsufficientFeesPaid {
        //             min_required: store_cost,
        //             paid: sn_dbc::Token::zero(),
        //         })
        //     }
        // };

        // if store_cost > paid_to_us {
        //     return Err(Error::InsufficientFeesPaid {
        //         min_required: store_cost,
        //         paid: paid_to_us,
        //     });
        // }

        Ok(())
    }

    // Verify spent proof signatures are valid, and each spent proof is signed by a known section key.
    fn verify_spent_proofs(
        spent_proofs: &BTreeSet<SpentProof>,
        network_knowledge: &NetworkKnowledge,
    ) -> Result<()> {
        let mut spent_proofs_keys = BTreeSet::new();

        // Verify each spent proof signature is valid.
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

        // Verify each spent proof is signed by a known section key.
        for pk in &spent_proofs_keys {
            if !network_knowledge.verify_section_key_is_known(pk) {
                warn!(
                    "Dropping spend request: spent proof is signed by unknown section with public \
                    key {:?}",
                    pk
                );
                return Err(Error::SpentProofUnknownSectionKey(*pk));
            }
        }

        Ok(())
    }
}
