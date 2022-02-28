// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{delivery_group, Comm, Node};

use crate::node::{
    api::cmds::Cmd,
    error::{Error, Result},
    Event, NodeInfo,
};
use crate::UsedSpace;
use sn_interface::messaging::{
    system::{JoinResponse, MembershipState, NodeState, SigShare, SystemMsg},
    WireMsg,
};
use sn_interface::network_knowledge::{
    NetworkKnowledge, SectionAuthorityProvider, SectionKeyShare,
};
use sn_interface::types::{log_markers::LogMarker, Peer};

use secured_linked_list::SecuredLinkedList;
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf};
use tokio::sync::mpsc;
use xor_name::XorName;

impl Node {
    // Creates `Core` for the first node in the network
    pub(crate) async fn first_node(
        comm: Comm,
        mut node: NodeInfo,
        event_tx: mpsc::Sender<Event>,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
        genesis_sk_set: bls::SecretKeySet,
    ) -> Result<Self> {
        // make sure the Node has the correct local addr as Comm
        node.addr = comm.our_connection_info();

        let (section, section_key_share) =
            NetworkKnowledge::first_node(node.peer(), genesis_sk_set).await?;
        Self::new(
            comm,
            node,
            section,
            Some(section_key_share),
            event_tx,
            used_space,
            root_storage_dir,
        )
        .await
    }

    pub(crate) async fn relocate(
        &self,
        mut new_node: NodeInfo,
        new_section: NetworkKnowledge,
    ) -> Result<()> {
        // we first try to relocate section info.
        self.network_knowledge.relocated_to(new_section).await?;

        // make sure the new Node has the correct local addr as Comm
        new_node.addr = self.comm.our_connection_info();

        let mut our_node = self.info.write().await;
        *our_node = new_node;

        Ok(())
    }

    pub(crate) fn network_knowledge(&self) -> &NetworkKnowledge {
        &self.network_knowledge
    }

    pub(crate) async fn section_chain(&self) -> SecuredLinkedList {
        self.network_knowledge.section_chain().await
    }

    /// Is this node an elder?
    pub(crate) async fn is_elder(&self) -> bool {
        self.network_knowledge
            .is_elder(&self.info.read().await.name())
            .await
    }

    pub(crate) async fn is_not_elder(&self) -> bool {
        !self.is_elder().await
    }

    /// Returns connection info of this node.
    pub(crate) fn our_connection_info(&self) -> SocketAddr {
        self.comm.our_connection_info()
    }

    /// Tries to sign with the secret corresponding to the provided BLS public key
    pub(crate) async fn sign_with_section_key_share(
        &self,
        data: &[u8],
        public_key: &bls::PublicKey,
    ) -> Result<(usize, bls::SignatureShare)> {
        self.section_keys_provider
            .sign_with(data, public_key)
            .await
            .map_err(Error::from)
    }

    /// Returns the current BLS public key set
    pub(crate) async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        Ok(self.key_share().await?.public_key_set)
    }

    /// Returns the SAP of the section matching the name.
    pub(crate) async fn matching_section(
        &self,
        name: &XorName,
    ) -> Result<SectionAuthorityProvider> {
        self.network_knowledge
            .section_by_name(name)
            .map_err(Error::from)
    }

    /// Returns our key share in the current BLS group if this node is a member of one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub(crate) async fn key_share(&self) -> Result<SectionKeyShare> {
        let section_key = self.network_knowledge.section_key().await;
        self.section_keys_provider
            .key_share(&section_key)
            .await
            .map_err(Error::from)
    }

    pub(crate) async fn send_event(&self, event: Event) {
        // Note: cloning the sender to avoid mutable access. Should have negligible cost.
        if self.event_tx.clone().send(event).await.is_err() {
            error!("Event receiver has been closed");
        }
    }

    // ----------------------------------------------------------------------------------------
    //   ---------------------------------- Mut ------------------------------------------
    // ----------------------------------------------------------------------------------------

    pub(crate) async fn handle_timeout(&self, token: u64) -> Result<Vec<Cmd>> {
        self.dkg_voter.handle_timeout(
            &self.info.read().await.clone(),
            token,
            self.network_knowledge().section_key().await,
        )
    }

    // Send message to peers on the network.
    pub(crate) async fn send_msg_to_nodes(&self, mut wire_msg: WireMsg) -> Result<Option<Cmd>> {
        let dst_location = wire_msg.dst_location();
        let (targets, dg_size) = delivery_group::delivery_targets(
            dst_location,
            &self.info.read().await.name(),
            &self.network_knowledge,
        )
        .await?;

        trace!(
            "relay {:?} to first {:?} of {:?} (Section PK: {:?})",
            wire_msg,
            dg_size,
            targets,
            wire_msg.src_section_pk(),
        );

        let target_name = dst_location.name();

        // To avoid loop: if destination is to Node, targets are multiple, self is an elder,
        //     self section prefix matches the destination name, then don't carry out a relay.
        if self.is_elder().await
            && targets.len() > 1
            && dst_location.is_to_node()
            && self.network_knowledge.prefix().await.matches(&target_name)
        {
            // This actually means being an elder, but we don't know the member yet. Which most likely
            // happens during the join process that a node's name is changed.
            // we just drop the message
            return Ok(None);
        }

        let dst_pk = self.section_key_by_name(&target_name).await;
        wire_msg.set_dst_section_pk(dst_pk);

        let cmd = Cmd::SendMsgDeliveryGroup {
            recipients: targets.into_iter().collect(),
            delivery_group_size: dg_size,
            wire_msg,
        };

        Ok(Some(cmd))
    }

    // Generate a new section info based on the current set of members and if it differs from the
    // current elders, trigger a DKG.
    pub(crate) async fn promote_and_demote_elders(&self) -> Result<Vec<Cmd>> {
        self.promote_and_demote_elders_except(&BTreeSet::new())
            .await
    }

    // Generate a new section info based on the current set of members, but
    // excluding the ones in the provided list. And if the outcome list of candidates
    // differs from the current elders, trigger a DKG.
    pub(crate) async fn promote_and_demote_elders_except(
        &self,
        excluded_names: &BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let our_name = self.info.read().await.name();

        debug!("{}", LogMarker::TriggeringPromotionAndDemotion);
        for elder_candidates in self
            .network_knowledge
            .promote_and_demote_elders(&our_name, excluded_names)
            .await
        {
            cmds.extend(self.send_dkg_start(elder_candidates).await?);
        }

        Ok(cmds)
    }

    pub(crate) async fn send_accepted_online_share(
        &self,
        peer: Peer,
        previous_name: Option<XorName>,
    ) -> Result<Vec<Cmd>> {
        let public_key_set = self.public_key_set().await?;
        let section_key = public_key_set.public_key();

        let node_state = NodeState {
            name: peer.name(),
            addr: peer.addr(),
            state: MembershipState::Joined,
            previous_name,
        };
        let serialized_details = bincode::serialize(&node_state)?;
        let (index, signature_share) = self
            .sign_with_section_key_share(&serialized_details, &section_key)
            .await?;

        let sig_share = SigShare {
            public_key_set,
            index,
            signature_share,
        };

        let members = self
            .network_knowledge
            .section_signed_members()
            .await
            .into_iter()
            .map(|itr| itr.into_authed_msg())
            .collect();
        let signed_sap = self
            .network_knowledge
            .section_signed_authority_provider()
            .await;

        let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::ApprovalShare {
            node_state,
            sig_share,
            section_auth: signed_sap.value.to_msg(),
            section_signed: signed_sap.sig,
            section_chain: self.network_knowledge.section_chain().await,
            members,
        }));

        Ok(vec![
            self.send_direct_msg(peer, node_msg, section_key).await?,
        ])
    }
}
