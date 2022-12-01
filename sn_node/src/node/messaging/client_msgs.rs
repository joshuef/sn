// Copyright 2022 MaidSafe.net limited.
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
    get_public_commitments_from_transaction, Commitment, KeyImage, RingCtTransaction, SpentProof,
    SpentProofShare,
};
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{
            ClientDataResponse, ClientMsg, DataCmd, DataQueryVariant, EditRegister,
            SignedRegisterEdit, SpentbookCmd,
        },
        system::{NodeDataResponse, OperationId},
        AuthorityProof, ClientAuth, MsgId,
    },
    network_knowledge::section_keys::build_spent_proof_share,
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        Keypair, Peer, RegisterCmd, ReplicatedData, SPENTBOOK_TYPE_TAG,
    },
};
use tokio::sync::Mutex;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use xor_name::XorName;

impl MyNode {
    /// Forms a `QueryError` msg to send back to the client on a stream
    pub(crate) async fn send_query_error_response_on_stream(
        context: NodeContext,
        error: Error,
        query: &DataQueryVariant,
        source_peer: Peer,
        correlation_id: MsgId,
        send_stream: Arc<Mutex<SendStream>>,
    ) -> Result<()> {
        let the_error_msg = ClientDataResponse::QueryResponse {
            response: query.to_error_response(error.into()),
            correlation_id,
        };

        let (kind, payload) = MyNode::serialize_client_msg_response(context.name, the_error_msg)?;

        MyNode::send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            send_stream,
            Some(source_peer),
            correlation_id,
        )
        .await
    }

    /// Forms a `CmdError` msg to send back to the client over the response stream
    pub(crate) async fn send_cmd_error_response_over_stream(
        context: &NodeContext,
        cmd: DataCmd,
        error: Error,
        correlation_id: MsgId,
        send_stream: Arc<Mutex<SendStream>>,
    ) -> Result<()> {
        let client_msg = ClientDataResponse::CmdResponse {
            response: cmd.to_error_response(error.into()),
            correlation_id,
        };

        let (kind, payload) = MyNode::serialize_client_msg_response(context.name, client_msg)?;

        debug!("{correlation_id:?} sending cmd response error back to client");
        MyNode::send_msg_on_stream(
            context.network_knowledge.section_key(),
            payload,
            kind,
            send_stream,
            None, // we shouldn't need this...
            correlation_id,
        )
        .await
    }

    /// Handle data query
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_data_query_at_adult(
        context: &NodeContext,
        operation_id: OperationId,
        query: &DataQueryVariant,
        auth: ClientAuth,
        requesting_elder: Peer,
        msg_id: MsgId,
        send_stream: Option<Arc<Mutex<SendStream>>>,
    ) -> Result<()> {
        let response = context
            .data_storage
            .query(query, User::Key(auth.public_key))
            .await;

        trace!("{msg_id:?} data query response at adult is: {:?}", response);
        let msg = NodeDataResponse::QueryResponse {
            response,
            operation_id,
        };

        let (kind, payload) = MyNode::serialize_node_msg_response(context.name, msg)?;

        let bytes = MyNode::form_usr_msg_bytes_to_node(
            context.network_knowledge.section_key(),
            payload,
            kind,
            Some(requesting_elder),
            msg_id,
        )?;

        if let Some(send_stream) = send_stream {
            // send response on the stream
            trace!("{msg_id:?} Sending response to {requesting_elder:?}");
            let stream_prio = 10;
            let mut send_stream = send_stream.lock().await;
            send_stream.set_priority(stream_prio);
            let stream_id = send_stream.id();
            if let Err(error) = send_stream.send_user_msg(bytes).await {
                error!("Could not send msg {msg_id:?} over response {stream_id} to {requesting_elder:?}: {error:?}");
            }
            if let Err(error) = send_stream.finish().await {
                error!("Could not close response {stream_id} with {requesting_elder:?}, for {msg_id:?}: {error:?}");
            }
            trace!("{msg_id:?} Response sent: to {requesting_elder:?}");
        } else {
            error!("Send stream missing from {requesting_elder:?}, data request response was not sent out.")
        }

        Ok(())
    }

    /// Handle incoming client msgs.
    pub(crate) async fn handle_valid_client_msg(
        context: NodeContext,
        msg_id: MsgId,
        msg: ClientMsg,
        auth: AuthorityProof<ClientAuth>,
        origin: Peer,
        send_stream: Arc<Mutex<SendStream>>,
    ) -> Result<Vec<Cmd>> {
        debug!("Handling client {msg_id:?}");

        trace!("{:?}: {:?} ", LogMarker::ClientMsgToBeHandled, msg);

        let cmd = match msg {
            ClientMsg::Cmd(cmd) => cmd,
            ClientMsg::Query(query) => {
                return MyNode::read_data_from_adult_and_respond_to_client(
                    context,
                    query,
                    msg_id,
                    auth,
                    origin,
                    send_stream,
                )
                .await
            }
        };

        // extract the data from the request
        let data_result = match cmd.clone() {
            // These reads/writes are for adult nodes...
            DataCmd::StoreChunk(chunk) => Ok(ReplicatedData::Chunk(chunk)),
            DataCmd::Register(cmd) => Ok(ReplicatedData::RegisterWrite(cmd)),
            DataCmd::Spentbook(cmd) => {
                let SpentbookCmd::Spend {
                    network_knowledge,
                    key_image,
                    tx,
                    spent_proofs,
                    spent_transactions,
                } = cmd.clone();
                if let Some((proof_chain, signed_sap)) = network_knowledge {
                    debug!(
                        "Received updated network knowledge with the request. Will return new command \
                        to update the node network knowledge before processing the spend."
                    );
                    // To avoid a loop, recompose the message without the updated proof_chain.
                    let updated_client_msg =
                        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
                            key_image,
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
                MyNode::extract_contents_as_replicated_data(&context, cmd)
            }
        };

        let data = match data_result {
            Ok(data) => data,
            Err(error) => {
                debug!("Will send error response back to client");
                MyNode::send_cmd_error_response_over_stream(
                    &context,
                    cmd,
                    error,
                    msg_id,
                    send_stream,
                )
                .await?;

                return Ok(vec![]);
            }
        };

        trace!("{:?}: {:?}", LogMarker::DataStoreReceivedAtElder, data);

        let cmds = vec![];
        let targets = MyNode::target_data_holders(&context, data.name());

        // make sure the expected replication factor is achieved
        if data_copy_count() > targets.len() {
            error!("InsufficientAdults for storing data reliably");
            let error = Error::InsufficientAdults {
                prefix: context.network_knowledge.prefix(),
                expected: data_copy_count() as u8,
                found: targets.len() as u8,
            };

            debug!("Will send error response back to client");

            // TODO: Use response stream here. This wont work anymore!
            MyNode::send_cmd_error_response_over_stream(&context, cmd, error, msg_id, send_stream)
                .await?;
            return Ok(vec![]);
        }

        // the replication msg sent to adults
        // cmds here may be dysfunction tracking.
        // CmdAcks are sent over the send stream herein
        MyNode::replicate_data_to_adults_and_ack_to_client(
            &context,
            cmd,
            data,
            msg_id,
            targets,
            send_stream,
        )
        .await?;

        // TODO: handle failed responses
        // cmds.extend();

        Ok(cmds)
    }

    // helper to extract the contents of the cmd as ReplicatedData
    fn extract_contents_as_replicated_data(
        context: &NodeContext,
        cmd: SpentbookCmd,
    ) -> Result<ReplicatedData> {
        let SpentbookCmd::Spend {
            key_image,
            tx,
            spent_proofs,
            spent_transactions,
            ..
        } = cmd;

        info!("Processing spend request for key image: {:?}", key_image);

        let spent_proof_share = MyNode::gen_spent_proof_share(
            context,
            &key_image,
            &tx,
            &spent_proofs,
            &spent_transactions,
        )?;
        let reg_cmd = MyNode::gen_register_cmd(context, &key_image, &spent_proof_share)?;
        Ok(ReplicatedData::SpentbookWrite(reg_cmd))
    }

    /// Generate a spent proof share from the information provided by the client.
    fn gen_spent_proof_share(
        context: &NodeContext,
        key_image: &KeyImage,
        tx: &RingCtTransaction,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<RingCtTransaction>,
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
                debug!("Dropping spend request: {msg}");
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
        let tx_public_commitments: Vec<Vec<Commitment>> = public_commitments_info
            .clone()
            .into_iter()
            .map(|(_, v)| v)
            .collect();

        if let Err(err) = tx.verify(&tx_public_commitments) {
            debug!("Dropping spend request: {:?}", err.to_string());
            return Err(Error::SpentbookError(err.to_string()));
        }

        // TODO:
        // Check the key_image wasn't already spent with a different TX (i.e. double spent)

        // Grab the commitments specific to the spent key image.
        let public_commitments: Vec<Commitment> = public_commitments_info
            .into_iter()
            .flat_map(|(k, v)| if &k == key_image { v } else { vec![] })
            .collect();
        if public_commitments.is_empty() {
            let msg = format!(
                "There are no commitments for the given key image {:?}",
                key_image
            );
            debug!("Dropping spend request: {msg}");
            return Err(Error::SpentbookError(msg));
        }
        let spent_proof_share = build_spent_proof_share(
            key_image,
            tx,
            &context.network_knowledge.section_auth(),
            &context.section_keys_provider,
            public_commitments,
        )?;
        Ok(spent_proof_share)
    }

    /// Generate the RegisterCmd to write the SpentProofShare as an entry in the Spentbook
    /// (Register).
    fn gen_register_cmd(
        context: &NodeContext,
        key_image: &KeyImage,
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
            XorName::from_content(&key_image.to_bytes()),
            SPENTBOOK_TYPE_TAG,
            policy,
        );
        let mut entry = vec![].writer();
        rmp_serde::encode::write(&mut entry, spent_proof_share).map_err(|err| {
            Error::SpentbookError(format!(
                "Failed to serialise SpentProofShare to insert it into the spentbook (Register): {:?}",
                err
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
