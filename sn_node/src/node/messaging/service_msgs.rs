// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::messaging::{OutgoingMsg, Peers};

use crate::node::{flow_ctrl::cmds::Cmd, Error, Node, Result};
use bytes::Bytes;

use sn_dbc::{
    Commitment, Hash, IndexedSignatureShare, KeyImage, RingCtTransaction, SpentProof,
    SpentProofContent, SpentProofShare,
};
#[cfg(feature = "traceroute")]
use sn_interface::messaging::Traceroute;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{
            DataCmd, DataQueryVariant, EditRegister, ServiceMsg, SignedRegisterEdit, SpentbookCmd,
        },
        system::{NodeQueryResponse, SystemMsg},
        AuthorityProof, EndUser, MsgId, ServiceAuth,
    },
    network_knowledge::{SectionAuthorityProvider, SectionKeysProvider},
    types::{
        log_markers::LogMarker,
        register::{Permissions, Policy, Register, User},
        Keypair, Peer, PublicKey, RegisterCmd, ReplicatedData, SPENTBOOK_TYPE_TAG,
    },
};

use std::collections::{BTreeMap, BTreeSet};
use xor_name::XorName;

impl Node {
    /// Forms a `CmdAck` msg to send back to the client
    pub(crate) fn send_cmd_ack(
        &self,
        target: Peer,
        msg_id: MsgId,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Cmd {
        debug!("Sending cmd ack to {target} for {msg_id:?}");
        let the_ack_msg = ServiceMsg::CmdAck {
            correlation_id: msg_id,
        };
        self.send_service_msg(
            the_ack_msg,
            Peers::Single(target),
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    /// Forms a cmd to send a cmd response error/ack to the client
    fn send_service_msg(
        &self,
        msg: ServiceMsg,
        recipients: Peers,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Cmd {
        let new_msg_id = MsgId::new();

        debug!("SendMSg formed for {:?}", new_msg_id);
        Cmd::SendMsg {
            msg: OutgoingMsg::Service(msg),
            msg_id: MsgId::new(),
            recipients,
            #[cfg(feature = "traceroute")]
            traceroute,
        }
    }

    /// Handle data query
    pub(crate) async fn handle_data_query_at_adult(
        &self,
        correlation_id: MsgId,
        query: &DataQueryVariant,
        auth: ServiceAuth,
        user: EndUser,
        requesting_elder: Peer,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Cmd {
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

        self.trace_system_msg(
            msg,
            Peers::Single(requesting_elder),
            #[cfg(feature = "traceroute")]
            traceroute,
        )
    }

    /// Handle data read
    /// Records response in liveness tracking
    /// Forms a response to send to the requester
    pub(crate) async fn handle_data_query_response_at_elder(
        &mut self,
        correlation_id: MsgId,
        response: NodeQueryResponse,
        user: EndUser,
        sending_node_pk: PublicKey,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Option<Cmd> {
        let op_id = if let Ok(op_id) = response.operation_id() {
            op_id
        } else {
            warn!(
                "There is no operation id. Dropping chunk query response from Adult {}, to user: {}.",
                sending_node_pk, user.0
            );
            return None;
        };

        debug!(
            "Handling data read @ elders, received from {:?}, op id: {:?}",
            sending_node_pk, op_id
        );

        let node_id = XorName::from(sending_node_pk);
        let query_peers = self.pending_data_queries.remove(&(op_id, node_id));
        // Clear expired queries from the cache.
        self.pending_data_queries.remove_expired();

        // First check for waiting peers. If no one is waiting, we drop the response
        let waiting_peers = if let Some(peers) = query_peers {
            if peers.is_empty() {
                // nothing to do
                return None;
            }
            peers
        } else {
            warn!(
                "Dropping chunk query response from Adult {}. We might have already forwarded this chunk to the requesting client or the client connection cache has expired: {}",
                sending_node_pk, user.0
            );
            return None;
        };

        let pending_removed = self
            .dysfunction_tracking
            .request_operation_fulfilled(&node_id, op_id);

        if !pending_removed {
            trace!("Ignoring un-expected response");
            return None;
        }

        let query_response = response.convert();

        let msg = ServiceMsg::QueryResponse {
            response: query_response,
            correlation_id,
        };

        Some(self.send_service_msg(
            msg,
            Peers::Multiple(waiting_peers),
            #[cfg(feature = "traceroute")]
            traceroute,
        ))
    }

    /// Handle incoming service msgs. Though NOT queries, as this requires
    /// mutable access to the Node
    pub(crate) async fn handle_valid_service_msg(
        &self,
        msg_id: MsgId,
        msg: ServiceMsg,
        auth: AuthorityProof<ServiceAuth>,
        origin: Peer,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        if !self.is_elder() {
            return Ok(vec![]);
        }

        trace!("{:?} {:?}", LogMarker::ServiceMsgToBeHandled, msg);

        // extract the data from the request
        let data = match msg {
            // These reads/writes are for adult nodes...
            ServiceMsg::Cmd(DataCmd::Register(cmd)) => ReplicatedData::RegisterWrite(cmd),
            ServiceMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
                key_image,
                tx,
                spent_proofs,
                spent_transactions,
            })) => {
                let spent_proof_share = self.gen_spent_proof_share(
                    &key_image,
                    &tx,
                    &spent_proofs,
                    &spent_transactions,
                )?;
                let reg_cmd = self.gen_register_cmd(&key_image, &spent_proof_share)?;
                ReplicatedData::SpentbookWrite(reg_cmd)
            }
            ServiceMsg::Cmd(DataCmd::StoreChunk(chunk)) => ReplicatedData::Chunk(chunk),
            ServiceMsg::Query(query) => {
                return self
                    .read_data_from_adults(
                        query,
                        msg_id,
                        auth,
                        origin,
                        #[cfg(feature = "traceroute")]
                        traceroute,
                    )
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

        trace!("{:?}: {:?}", LogMarker::DataStoreReceivedAtElder, data);

        let mut cmds = vec![];
        let targets = self.target_data_holders(data.name());

        // make sure the expected replication factor is achieved
        if data_copy_count() > targets.len() {
            error!("InsufficientAdults for storing data reliably");
            return Err(Error::InsufficientAdults {
                prefix: self.network_knowledge().prefix(),
                expected: data_copy_count() as u8,
                found: targets.len() as u8,
            });
        }

        // the replication msg sent to adults
        cmds.push(self.replicate_data(
            data,
            targets,
            #[cfg(feature = "traceroute")]
            traceroute.clone(),
        ));

        // the ack sent to client
        cmds.push(self.send_cmd_ack(
            origin,
            msg_id,
            #[cfg(feature = "traceroute")]
            traceroute,
        ));

        Ok(cmds)
    }

    /// Get the public commitments for the transaction for this key image spend.
    ///
    /// They will be assigned to the spent proof share that is generated.
    ///
    /// In the process of doing so, we verify the correct set of spent proofs and transactions have
    /// been sent by the client.
    ///
    /// This is in its own function because we share this code between the message handler and a
    /// test utility. It may be moved inside on of the `sn_dbc` APIs.
    pub(crate) fn get_public_commitments_from_transaction(
        tx: &RingCtTransaction,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<RingCtTransaction>,
    ) -> Result<Vec<(KeyImage, Vec<Commitment>)>> {
        let mut public_commitments_info = Vec::<(KeyImage, Vec<Commitment>)>::new();
        for mlsag in &tx.mlsags {
            // For each public key in ring, look up the matching Commitment
            // using the SpentProofs and spent TX set provided by the client.
            let commitments: Vec<Commitment> = mlsag
                .public_keys()
                .iter()
                .flat_map(|input_pk| {
                    spent_proofs.iter().flat_map(move |proof| {
                        // Make sure the spent proof corresponds to any of the spent TX provided,
                        // and the TX output PK matches the ring PK
                        spent_transactions.iter().filter_map(|spent_tx| {
                            let tx_hash = Hash::from(spent_tx.hash());
                            if tx_hash == proof.transaction_hash() {
                                spent_tx
                                    .outputs
                                    .iter()
                                    .find(|output| output.public_key() == &input_pk.clone())
                                    .map(|output| output.commitment())
                            } else {
                                None
                            }
                        })
                    })
                })
                .collect();

            if commitments.len() != mlsag.public_keys().len() {
                let error_msg = format!(
                    "The number of spent proofs ({}) does not match the number \
                    of input public keys ({})",
                    commitments.len(),
                    mlsag.public_keys().len()
                );
                debug!("Dropping spend request: {}", error_msg);
                return Err(Error::SpentbookError(error_msg));
            }

            public_commitments_info.push((mlsag.key_image.into(), commitments));
        }
        Ok(public_commitments_info)
    }

    /// Builds the spent proof share based on the given inputs.
    ///
    /// This is in its own function because we share this code between the message handler and a
    /// test utility.
    pub(crate) fn build_spent_proof_share(
        key_image: &bls::PublicKey,
        tx: &RingCtTransaction,
        sap: &SectionAuthorityProvider,
        skp: &SectionKeysProvider,
        public_commitments: Vec<Commitment>,
    ) -> Result<SpentProofShare> {
        let content = SpentProofContent {
            key_image: *key_image,
            transaction_hash: Hash::from(tx.hash()),
            public_commitments,
        };
        let (index, sig_share) = skp.sign_with(content.hash().as_ref(), &sap.section_key())?;
        Ok(SpentProofShare {
            content,
            spentbook_pks: sap.public_key_set(),
            spentbook_sig_share: IndexedSignatureShare::new(index as u64, sig_share),
        })
    }

    /// Generate a spent proof share from the information provided by the client.
    fn gen_spent_proof_share(
        &self,
        key_image: &KeyImage,
        tx: &RingCtTransaction,
        spent_proofs: &BTreeSet<SpentProof>,
        spent_transactions: &BTreeSet<RingCtTransaction>,
    ) -> Result<SpentProofShare> {
        info!("Processing spend request for key image: {:?}", key_image);

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
            if !self.network_knowledge.verify_section_key_is_known(pk) {
                warn!(
                    "Dropping spend request: spent proof is signed by unknown section with public \
                    key {:?}",
                    pk
                );
                return Err(Error::SpentProofUnknownSectionKey(*pk));
            }
        }

        let public_commitments_info =
            Self::get_public_commitments_from_transaction(tx, spent_proofs, spent_transactions)?;

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
        let spent_proof_share = Self::build_spent_proof_share(
            key_image,
            tx,
            &self.network_knowledge.authority_provider(),
            &self.section_keys_provider,
            public_commitments,
        )?;
        Ok(spent_proof_share)
    }

    /// Generate the RegisterCmd to write the SpentProofShare as an entry in the Spentbook
    /// (Register).
    fn gen_register_cmd(
        &self,
        key_image: &KeyImage,
        spent_proof_share: &SpentProofShare,
    ) -> Result<RegisterCmd> {
        let mut permissions = BTreeMap::new();
        let _ = permissions.insert(User::Anyone, Permissions::new(true));

        // use our own keypair for generating the register command
        let own_keypair = Keypair::Ed25519(self.info().keypair);
        let owner = User::Key(own_keypair.public_key());
        let policy = Policy { owner, permissions };

        let mut register = Register::new(
            owner,
            XorName::from_content(&key_image.to_bytes()),
            SPENTBOOK_TYPE_TAG,
            policy,
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

        let signature = own_keypair.sign(&bincode::serialize(&op)?);
        let signed_edit = SignedRegisterEdit {
            op,
            auth: ServiceAuth {
                public_key: own_keypair.public_key(),
                signature,
            },
        };

        Ok(RegisterCmd::Edit(signed_edit))
    }
}
