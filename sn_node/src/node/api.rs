// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{
        delivery_group,
        node_api::{cmds::Cmd, event_channel::EventSender},
        Error, Event, Node, Result, GENESIS_DBC_AMOUNT,
    },
    UsedSpace,
};

use sn_interface::{
    messaging::WireMsg,
    network_knowledge::{NetworkKnowledge, NodeInfo, SectionAuthorityProvider, SectionKeyShare},
    types::log_markers::LogMarker,
};

use ed25519_dalek::Keypair;
use secured_linked_list::SecuredLinkedList;
use sn_dbc::{
    bls_ringct::{bls_bulletproofs::PedersenGens, group::Curve},
    rng, Dbc, Hash, IndexedSignatureShare, MlsagMaterial, Owner, OwnerOnce, RevealedCommitment,
    SpentProofContent, SpentProofShare, TransactionBuilder, TrueInput,
};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc};
use xor_name::XorName;

impl Node {
    pub(crate) async fn first_node(
        our_addr: SocketAddr,
        keypair: Arc<Keypair>,
        event_sender: EventSender,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
        genesis_sk_set: bls::SecretKeySet,
    ) -> Result<(Self, Dbc)> {
        let info = NodeInfo {
            keypair: keypair.clone(),
            addr: our_addr,
        };

        // Mint the genesis DBC to be owned by the provided genesis key
        let genesis_dbc = gen_genesis_dbc(&genesis_sk_set)?;

        let (network_knowledge, section_key_share) =
            NetworkKnowledge::first_node(info.peer(), genesis_sk_set)?;

        let node = Self::new(
            our_addr,
            keypair.clone(),
            network_knowledge,
            Some(section_key_share),
            event_sender,
            used_space,
            root_storage_dir,
        )
        .await?;

        Ok((node, genesis_dbc))
    }

    pub(crate) fn relocate(
        &mut self,
        new_keypair: Arc<Keypair>,
        new_section: NetworkKnowledge,
    ) -> Result<()> {
        // we first try to relocate section info.
        self.network_knowledge.relocated_to(new_section)?;

        self.keypair = new_keypair;

        Ok(())
    }

    pub(crate) fn network_knowledge(&self) -> &NetworkKnowledge {
        &self.network_knowledge
    }

    pub(crate) fn section_chain(&self) -> SecuredLinkedList {
        self.network_knowledge.section_chain()
    }

    /// Is this node an elder?
    pub(crate) fn is_elder(&self) -> bool {
        self.network_knowledge.is_elder(&self.info().name())
    }

    pub(crate) fn is_not_elder(&self) -> bool {
        !self.is_elder()
    }

    /// Returns the current BLS public key set
    pub(crate) fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        Ok(self.key_share()?.public_key_set)
    }

    /// Returns the SAP of the section matching the name.
    pub(crate) fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.network_knowledge
            .section_by_name(name)
            .map_err(Error::from)
    }

    /// Returns our key share in the current BLS group if this node is a member of one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub(crate) fn key_share(&self) -> Result<SectionKeyShare> {
        let section_key = self.network_knowledge.section_key();
        self.section_keys_provider
            .key_share(&section_key)
            .map_err(Error::from)
    }

    pub(crate) async fn send_event(&self, event: Event) {
        self.event_sender.send(event).await
    }

    // ----------------------------------------------------------------------------------------
    //   ---------------------------------- Mut ------------------------------------------
    // ----------------------------------------------------------------------------------------

    pub(crate) fn handle_dkg_timeout(&self, token: u64) -> Result<Vec<Cmd>> {
        self.dkg_voter
            .handle_timeout(&self.info(), token, self.network_knowledge().section_key())
    }

    // Send message to peers on the network.
    pub(crate) fn send_msg_to_nodes(&self, mut wire_msg: WireMsg) -> Result<Option<Cmd>> {
        let dst_location = wire_msg.dst_location();
        let (targets, dg_size) = delivery_group::delivery_targets(
            dst_location,
            &self.info().name(),
            &self.network_knowledge,
        )?;

        let target_name = dst_location.name();

        // To avoid loop: if destination is to Node, targets are multiple, self is an elder,
        //     self section prefix matches the destination name, then don't carry out a relay.
        if self.is_elder()
            && targets.len() > 1
            && dst_location.is_to_node()
            && self.network_knowledge.prefix().matches(&target_name)
        {
            // This actually means being an elder, but we don't know the member yet. Which most likely
            // happens during the join process that a node's name is changed.
            // we just drop the message
            return Ok(None);
        }

        trace!(
            "relay {:?} to first {:?} of {:?} (Section PK: {:?})",
            wire_msg,
            dg_size,
            targets,
            wire_msg.src_section_pk(),
        );

        let dst_pk = self.section_key_by_name(&target_name);
        wire_msg.set_dst_section_pk(dst_pk);

        let cmd = Cmd::SendMsg {
            recipients: targets.into_iter().collect(),
            wire_msg,
        };

        Ok(Some(cmd))
    }

    // Generate a new section info based on the current set of members, but
    // excluding the ones in the provided list. And if the outcome list of candidates
    // differs from the current elders, trigger a DKG.
    pub(crate) fn promote_and_demote_elders_except(
        &mut self,
        excluded_names: &BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::TriggeringPromotionAndDemotion);
        let mut cmds = vec![];
        // TODO: move `promote_and_demote_elders` to Membership
        for session_id in self.promote_and_demote_elders(excluded_names)? {
            cmds.extend(self.send_dkg_start(session_id)?);
        }

        Ok(cmds)
    }
}

// Helper to generate the (currently bearer) genesis DBC to be owned by the provided key.
fn gen_genesis_dbc(input_sk_set: &bls::SecretKeySet) -> Result<Dbc> {
    // Use the same key as the input and output of Genesis Tx.
    let output_sk = input_sk_set.secret_key();
    let output_owner = OwnerOnce::from_owner_base(Owner::from(output_sk), &mut rng::thread_rng());

    let revealed_commitment =
        RevealedCommitment::from_value(GENESIS_DBC_AMOUNT, &mut rng::thread_rng());

    // Use the same key as the input and output of Genesis Tx.
    let true_input = TrueInput::new(input_sk_set.secret_key(), revealed_commitment);

    // build our MlsagMaterial manually without randomness.
    // note: no decoy inputs because no other DBCs exist prior to genesis DBC.
    let mlsag_material = MlsagMaterial {
        true_input,
        decoy_inputs: vec![],
        pi_base: 0,
        alpha: (Default::default(), Default::default()),
        r: vec![(Default::default(), Default::default())],
    };

    let mut dbc_builder = TransactionBuilder::default()
        .add_input(mlsag_material)
        .add_output_by_amount(GENESIS_DBC_AMOUNT, output_owner)
        .build(&mut rng::thread_rng())
        .map_err(|err| {
            Error::GenesisDbcError(format!(
                "Failed to build the ringct transaction for genesis DBC: {}",
                err
            ))
        })?;

    let (key_image, tx) = dbc_builder.inputs().into_iter().next().ok_or_else(|| {
        Error::GenesisDbcError(
            "DBC builder (unexpectedly) contains an empty set of inputs.".to_string(),
        )
    })?;

    // let's build the spent proof and add it to the DBC builder
    let content = SpentProofContent {
        key_image,
        transaction_hash: Hash::from(tx.hash()),
        public_commitments: vec![revealed_commitment
            .commit(&PedersenGens::default())
            .to_affine()],
    };

    let sk_share_index = 0;
    let sig_share = input_sk_set
        .secret_key_share(sk_share_index)
        .sign(content.hash().as_ref());
    let spentbook_sig_share = IndexedSignatureShare::new(sk_share_index, sig_share);

    let spent_proof_share = SpentProofShare {
        content,
        spentbook_pks: input_sk_set.public_keys(),
        spentbook_sig_share,
    };

    dbc_builder = dbc_builder
        .add_spent_proof_share(spent_proof_share)
        .add_spent_transaction(tx);

    // build the output DBCs
    let outputs = dbc_builder.build_without_verifying().map_err(|err| {
        Error::GenesisDbcError(format!(
            "DBC builder failed to create output genesis DBC: {}",
            err
        ))
    })?;

    // just one output DBC is expected which is the genesis DBC
    let (genesis_dbc, _, _) = outputs.into_iter().next().ok_or_else(|| {
        Error::GenesisDbcError(
            "DBC builder (unexpectedly) contains an empty set of outputs.".to_string(),
        )
    })?;

    Ok(genesis_dbc)
}
