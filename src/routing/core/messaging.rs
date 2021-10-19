// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Core;
use crate::messaging::{
    system::{
        DkgSessionId, ElderCandidates, JoinResponse, NodeState, Peer, Proposal, RelocateDetails,
        RelocatePromise, Section, SectionAuth, SystemMsg,
    },
    DstLocation, WireMsg,
};
use crate::routing::{
    dkg::{DkgSessionIdUtils, ProposalUtils},
    error::Result,
    log_markers::LogMarker,
    messages::WireMsgUtils,
    peer::PeerUtils,
    relocation::RelocateState,
    routing_api::command::Command,
    section::{ElderCandidatesUtils, SectionKeyShare},
    SectionAuthorityProviderUtils,
};
use crate::types::PublicKey;
use bls::PublicKey as BlsPublicKey;
use std::net::SocketAddr;
use xor_name::XorName;
impl Core {
    // Send proposal to all our elders.
    pub(crate) async fn propose(&self, proposal: Proposal) -> Result<Vec<Command>> {
        let elders: Vec<_> = self.section.authority_provider().peers().collect();
        self.send_proposal(elders, proposal).await
    }

    // Send `proposal` to `recipients`.
    pub(crate) async fn send_proposal(
        &self,
        recipients: Vec<Peer>,
        proposal: Proposal,
    ) -> Result<Vec<Command>> {
        let key_share = self
            .section_keys_provider
            .key_share()
            .await
            .map_err(|err| {
                trace!("Can't propose {:?}: {:?}", proposal, err);
                err
            })?;
        self.send_proposal_with(recipients, proposal, &key_share)
    }

    pub(crate) fn send_proposal_with(
        &self,
        recipients: Vec<Peer>,
        proposal: Proposal,
        key_share: &SectionKeyShare,
    ) -> Result<Vec<Command>> {
        trace!(
            "Propose {:?}, key_share: {:?}, aggregators: {:?}",
            proposal,
            key_share,
            recipients,
        );

        let sig_share = proposal.prove(
            key_share.public_key_set.clone(),
            key_share.index,
            &key_share.secret_key_share,
        )?;

        // Broadcast the proposal to the rest of the section elders.
        let node_msg = SystemMsg::Propose {
            proposal,
            sig_share,
        };
        // Name of the section_pk may not matches the section prefix.
        // Carry out a substitution to prevent the dst_location becomes other section.
        let wire_msg = WireMsg::single_src(
            &self.node,
            DstLocation::Section {
                name: self.section.prefix().name(),
                section_pk: *self.section.chain().last_key(),
            },
            node_msg,
            self.section.authority_provider().section_key(),
        )?;

        Ok(self.send_or_handle(wire_msg, recipients))
    }

    // ------------------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------------------

    /// Generate AntiEntropyUpdate message to update a peer with proof_chain,
    /// and members_info if required.
    pub(crate) fn generate_ae_update(
        &self,
        dst_section_key: BlsPublicKey,
        add_peer_info_to_update: bool,
    ) -> Result<SystemMsg> {
        let section_signed_auth = self.section.section_signed_authority_provider().clone();
        let section_auth = section_signed_auth.value;
        let section_signed = section_signed_auth.sig;

        let proof_chain = match self
            .section
            .chain()
            .get_proof_chain_to_current(&dst_section_key)
        {
            Ok(chain) => chain,
            Err(_) => {
                // error getting chain from key, so lets send the whole thing
                self.section.chain().clone()
            }
        };

        let members = if add_peer_info_to_update {
            Some(self.section.members().clone())
        } else {
            None
        };

        Ok(SystemMsg::AntiEntropyUpdate {
            section_auth,
            section_signed,
            proof_chain,
            members,
        })
    }

    pub(crate) fn check_lagging(
        &self,
        peer: (XorName, SocketAddr),
        public_key: &BlsPublicKey,
    ) -> Result<Option<Command>> {
        if self.section.chain().has_key(public_key) && public_key != self.section.chain().last_key()
        {
            let msg = self.generate_ae_update(*public_key, true)?;
            trace!("{}", LogMarker::SendingAeUpdateAfterLagCheck);

            let cmd = self.send_direct_message(peer, msg, *public_key)?;
            Ok(Some(cmd))
        } else {
            Ok(None)
        }
    }

    // Send NodeApproval to a joining node which makes them a section member
    pub(crate) fn send_node_approval(&self, node_state: SectionAuth<NodeState>) -> Result<Command> {
        info!(
            "Our section with {:?} has approved peer {:?}.",
            self.section.prefix(),
            node_state.value.peer
        );

        let addr = *node_state.value.peer.addr();
        let name = *node_state.value.peer.name();

        let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Approval {
            genesis_key: *self.section.genesis_key(),
            section_auth: self.section.section_signed_authority_provider().clone(),
            node_state,
            section_chain: self.section.chain().clone(),
        }));

        let dst_section_pk = *self.section.chain().last_key();
        trace!("{}", LogMarker::SendNodeApproval);
        let cmd = self.send_direct_message((name, addr), node_msg, dst_section_pk)?;

        Ok(cmd)
    }

    pub(crate) fn send_ae_update_to_our_section(&self, section: &Section) -> Result<Vec<Command>> {
        let nodes: Vec<_> = section
            .active_members()
            .iter()
            .filter(|peer| peer.name() != &self.node.name())
            .map(|peer| (*peer.name(), *peer.addr()))
            .collect();

        // the PK is that of our section (as we know it; and we're ahead of our adults here)
        let dst_section_pk = *self.section_chain().last_key();
        // the previous PK which is likely what adults know
        let previous_pk = *self.section_chain().prev_key();
        let node_msg = self.generate_ae_update(previous_pk, true)?;
        let cmd = self.send_direct_message_to_nodes(
            nodes,
            node_msg,
            self.section().prefix().name(),
            dst_section_pk,
        )?;

        Ok(vec![cmd])
    }

    pub(crate) fn send_ae_update_to_adults(&mut self) -> Result<Vec<Command>> {
        let adults: Vec<_> = self
            .section
            .live_adults()
            .iter()
            .map(|peer| (*peer.name(), *peer.addr()))
            .collect();

        let dst_section_pk = *self.section_chain().last_key();
        let node_msg = self.generate_ae_update(dst_section_pk, true)?;

        let cmd = self.send_direct_message_to_nodes(
            adults,
            node_msg,
            self.section().prefix().name(),
            dst_section_pk,
        )?;

        Ok(vec![cmd])
    }

    pub(crate) async fn send_relocate(
        &self,
        recipient: Peer,
        details: RelocateDetails,
    ) -> Result<Vec<Command>> {
        let src = details.pub_id;
        let dst = DstLocation::Node {
            name: details.pub_id,
            section_pk: *self.section.chain().last_key(),
        };
        let node_msg = SystemMsg::Relocate(details);

        self.send_message_for_dst_accumulation(src, dst, node_msg, vec![recipient])
            .await
    }

    pub(crate) async fn send_relocate_promise(
        &self,
        recipient: Peer,
        promise: RelocatePromise,
    ) -> Result<Vec<Command>> {
        // Note: this message is first sent to a single node who then sends it back to the section
        // where it needs to be handled by all the elders. This is why the destination is
        // `Section`, not `Node`.
        let src = promise.name;
        let dst = DstLocation::Section {
            name: promise.name,
            section_pk: *self.section.chain().last_key(),
        };
        let node_msg = SystemMsg::RelocatePromise(promise);

        self.send_message_for_dst_accumulation(src, dst, node_msg, vec![recipient])
            .await
    }

    pub(crate) fn return_relocate_promise(&self) -> Option<Command> {
        // TODO: keep sending this periodically until we get relocated.
        if let Some(RelocateState::Delayed(msg)) = &self.relocate_state {
            self.send_message_to_our_elders(msg.clone()).ok()
        } else {
            None
        }
    }

    pub(crate) async fn send_dkg_start(
        &self,
        elder_candidates: ElderCandidates,
    ) -> Result<Vec<Command>> {
        let src_prefix = elder_candidates.prefix;
        let generation = self.section.chain().main_branch_len() as u64;
        let session_id = DkgSessionId::new(&elder_candidates, generation);

        // Send DKG start to all candidates
        let recipients: Vec<_> = elder_candidates.peers().collect();

        trace!(
            "Send DkgStart for {:?} with {:?} to {:?}",
            elder_candidates,
            session_id,
            recipients
        );

        let node_msg = SystemMsg::DkgStart {
            session_id,
            elder_candidates,
        };
        let section_pk = *self.section.chain().last_key();
        self.send_message_for_dst_accumulation(
            src_prefix.name(),
            DstLocation::Section {
                name: XorName::from(PublicKey::Bls(section_pk)),
                section_pk,
            },
            node_msg,
            recipients,
        )
        .await
    }

    pub(crate) async fn send_message_for_dst_accumulation(
        &self,
        src: XorName,
        dst: DstLocation,
        node_msg: SystemMsg,
        recipients: Vec<Peer>,
    ) -> Result<Vec<Command>> {
        let key_share = self
            .section_keys_provider
            .key_share()
            .await
            .map_err(|err| {
                trace!(
                    "Can't create message {:?} for accumulation at dst {:?}: {:?}",
                    node_msg,
                    dst,
                    err
                );
                err
            })?;

        let wire_msg = WireMsg::for_dst_accumulation(
            &key_share,
            src,
            dst,
            node_msg,
            *self.section.chain().last_key(),
        )?;

        trace!(
            "Send {:?} for accumulation at dst to {:?}",
            wire_msg,
            recipients
        );

        Ok(self.send_or_handle(wire_msg, recipients))
    }

    // Send the message to all `recipients`. If one of the recipients is us, don't send it over the
    // network but handle it directly.
    pub(crate) fn send_or_handle(
        &self,
        mut wire_msg: WireMsg,
        recipients: Vec<Peer>,
    ) -> Vec<Command> {
        let mut commands = vec![];
        let mut others = Vec::new();
        let mut handle = false;

        trace!("Send {:?} to {:?}", wire_msg, recipients);

        for recipient in recipients {
            if recipient.name() == &self.node.name() {
                handle = true;
            } else {
                others.push((*recipient.name(), *recipient.addr()));
            }
        }

        if !others.is_empty() {
            let dst_section_pk = self.section_key_by_name(&others[0].0);
            wire_msg.set_dst_section_pk(dst_section_pk);

            trace!("{}", LogMarker::SendOrHandle);
            commands.push(Command::SendMessage {
                recipients: others,
                wire_msg: wire_msg.clone(),
            });
        }

        if handle {
            wire_msg.set_dst_section_pk(*self.section_chain().last_key());
            wire_msg.set_dst_xorname(self.node.name());

            commands.push(Command::HandleMessage {
                sender: self.node.addr,
                wire_msg,
                original_bytes: None,
            });
        }

        commands
    }

    pub(crate) fn send_direct_message(
        &self,
        recipient: (XorName, SocketAddr),
        node_msg: SystemMsg,
        dst_section_pk: BlsPublicKey,
    ) -> Result<Command> {
        let wire_msg = WireMsg::single_src(
            &self.node,
            DstLocation::Section {
                name: recipient.0,
                section_pk: dst_section_pk,
            },
            node_msg,
            self.section.authority_provider().section_key(),
        )?;

        trace!("{}", LogMarker::SendDirect);

        Ok(Command::SendMessage {
            recipients: vec![recipient],
            wire_msg,
        })
    }

    pub(crate) fn send_direct_message_to_nodes(
        &self,
        recipients: Vec<(XorName, SocketAddr)>,
        node_msg: SystemMsg,
        dst_name: XorName,
        dst_section_pk: BlsPublicKey,
    ) -> Result<Command> {
        let wire_msg = WireMsg::single_src(
            &self.node,
            DstLocation::Section {
                name: dst_name,
                section_pk: dst_section_pk,
            },
            node_msg,
            self.section.authority_provider().section_key(),
        )?;

        trace!("{}", LogMarker::SendDirectToNodes);

        Ok(Command::SendMessage {
            recipients,
            wire_msg,
        })
    }

    // TODO: consider changing this so it sends only to a subset of the elders
    // (say 1/3 of the ones closest to our name or so)
    pub(crate) fn send_message_to_our_elders(&self, node_msg: SystemMsg) -> Result<Command> {
        let targets: Vec<_> = self
            .section
            .authority_provider()
            .elders()
            .iter()
            .map(|(name, address)| (*name, *address))
            .collect();

        let dst_section_pk = *self.section_chain().last_key();
        let cmd = self.send_direct_message_to_nodes(
            targets,
            node_msg,
            self.section.authority_provider().prefix().name(),
            dst_section_pk,
        )?;

        Ok(cmd)
    }
}
