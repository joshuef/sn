// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, SystemMsg},
    DstLocation,
};
use crate::node::{
    api::command::Command,
    core::{msg_handling::WireMsg, Core, Proposal},
    dkg::DkgFailureSigSetUtils,
    messages::WireMsgUtils,
    network_knowledge::{ElderCandidates, SectionAuthorityProvider, SectionKeyShare},
    Error, Result,
};
use crate::peer::Peer;
use crate::types::log_markers::LogMarker;

use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::message::Message as DkgMessage;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
use xor_name::{Prefix, XorName};

impl Core {
    pub(crate) async fn handle_dkg_start(
        &self,
        session_id: DkgSessionId,
        prefix: Prefix,
        elders: BTreeMap<XorName, SocketAddr>,
    ) -> Result<Vec<Command>> {
        let current_generation = self.network_knowledge.chain_len().await;
        if session_id.generation < current_generation {
            trace!("Skipping DkgStart for older generation: {:?}", &session_id);
            return Ok(vec![]);
        }
        let section_auth = self.network_knowledge().authority_provider().await;

        let mut peers = vec![];
        for (name, addr) in elders.into_iter() {
            // Reuse known peers from network_knowledge, in order to preserve connections
            let peer = if let Some(elder) = section_auth
                .get_elder(&name)
                .filter(|elder| elder.addr() == addr)
            {
                elder.clone()
            } else if let Some(peer) = self.network_knowledge().find_member_by_addr(&addr).await {
                peer
            } else {
                Peer::new(name, addr)
            };

            peers.push(peer);
        }

        let elder_candidates = ElderCandidates::new(prefix, peers);

        trace!(
            "Received DkgStart for {:?} - {:?}",
            session_id,
            elder_candidates
        );
        self.dkg_sessions
            .write()
            .await
            .retain(|existing_session_id, _| {
                existing_session_id.generation >= session_id.generation
            });
        let commands = self
            .dkg_voter
            .start(
                &self.node.read().await.clone(),
                session_id,
                elder_candidates,
                self.network_knowledge().section_key().await,
            )
            .await?;
        Ok(commands)
    }

    pub(crate) async fn handle_dkg_message(
        &self,
        session_id: DkgSessionId,
        message: DkgMessage,
        sender: Peer,
    ) -> Result<Vec<Command>> {
        trace!(
            "{} {:?} from {}",
            LogMarker::DkgMessageHandling,
            message,
            sender
        );

        self.dkg_voter
            .process_message(
                sender,
                &self.node.read().await.clone(),
                &session_id,
                message,
                self.network_knowledge().section_key().await,
            )
            .await
    }

    pub(crate) async fn handle_dkg_not_ready(
        &self,
        sender: Peer,
        message: DkgMessage,
        session_id: DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        let message_history = self.dkg_voter.get_cached_messages(&session_id);
        let node_msg = SystemMsg::DkgRetry {
            message_history,
            message,
            session_id,
        };
        let wire_msg = WireMsg::single_src(
            &self.node.read().await.clone(),
            DstLocation::Node {
                name: sender.name(),
                section_pk,
            },
            node_msg,
            section_pk,
        )?;

        Ok(vec![Command::SendMessage {
            recipients: vec![sender],
            wire_msg,
        }])
    }

    pub(crate) async fn handle_dkg_retry(
        &self,
        session_id: DkgSessionId,
        message_history: Vec<DkgMessage>,
        message: DkgMessage,
        sender: Peer,
    ) -> Result<Vec<Command>> {
        let section_key = self.network_knowledge().section_key().await;
        let current_generation = self.network_knowledge.chain_len().await;
        if session_id.generation < current_generation {
            trace!(
                "Ignoring DkgRetry for expired DKG session: {:?}",
                &session_id
            );
            return Ok(vec![]);
        }
        let mut commands = self
            .dkg_voter
            .handle_dkg_history(
                &self.node.read().await.clone(),
                session_id,
                message_history,
                sender.name(),
                section_key,
            )
            .await?;

        commands.extend(
            self.dkg_voter
                .process_message(
                    sender,
                    &self.node.read().await.clone(),
                    &session_id,
                    message,
                    section_key,
                )
                .await?,
        );
        Ok(commands)
    }

    pub(crate) fn handle_dkg_failure_observation(
        &self,
        session_id: DkgSessionId,
        failed_participants: &BTreeSet<XorName>,
        signed: DkgFailureSig,
    ) -> Result<Vec<Command>> {
        match self
            .dkg_voter
            .process_failure(&session_id, failed_participants, signed)
        {
            None => Ok(vec![]),
            Some(cmd) => Ok(vec![cmd]),
        }
    }

    pub(crate) async fn handle_dkg_failure_agreement(
        &self,
        sender: &XorName,
        failure_set: &DkgFailureSigSet,
    ) -> Result<Vec<Command>> {
        if !self.network_knowledge.is_section_member(sender).await {
            return Err(Error::InvalidSrcLocation);
        }

        let generation = self.network_knowledge.chain_len().await;

        let elder_candidates = if let Some(elder_candidates) = self
            .network_knowledge
            .promote_and_demote_elders(&self.node.read().await.name(), &BTreeSet::new())
            .await
            .into_iter()
            .find(|elder_candidates| failure_set.verify(elder_candidates, generation))
        {
            elder_candidates
        } else {
            trace!("Ignore DKG failure agreement with invalid signeds or outdated participants",);
            return Ok(vec![]);
        };

        let mut commands = vec![];

        if !failure_set.failed_participants.is_empty() {
            // The DKG failure is regarding failed_participants, i.e. potential unresponsive node.
            trace!(
                "Received DKG failure agreement, propose offline for failed participants: {:?} , DKG generation({}), candidates: {:?}",
                failure_set.failed_participants,
                generation, elder_candidates
            );
            commands.extend(
                self.cast_offline_proposals(&failure_set.failed_participants)
                    .await?,
            );
        }

        trace!(
            "Received DKG failure agreement, we will restart with candidates: {:?} except failed participants: {:?}",
            elder_candidates, failure_set.failed_participants
        );

        commands.extend(
            self.promote_and_demote_elders_except(&failure_set.failed_participants)
                .await?,
        );
        Ok(commands)
    }

    pub(crate) async fn handle_dkg_outcome(
        &self,
        sap: SectionAuthorityProvider,
        key_share: SectionKeyShare,
    ) -> Result<Vec<Command>> {
        let key_share_pk = key_share.public_key_set.public_key();
        trace!(
            "{} public_key={:?}",
            LogMarker::HandlingDkgSuccessfulOutcome,
            key_share_pk
        );

        // Add our new keyshare to our cache, we will then use
        // it to sign any msg that needs section agreement.
        self.section_keys_provider.insert(key_share.clone()).await;

        let snapshot = self.state_snapshot().await;

        // If we are lagging, we may have been already approved as new Elder, and
        // an AE update provided us with this same SAP but already signed by previous Elders,
        // if so we can skip the SectionInfo agreement proposal phase.
        if self
            .network_knowledge
            .try_update_current_sap(key_share_pk, &sap.prefix())
            .await
        {
            self.update_self_for_new_node_state(snapshot).await
        } else {
            // This proposal is sent to the current set of elders to be aggregated
            // and section signed.
            let proposal = Proposal::SectionInfo(sap);
            let recipients: Vec<_> = self
                .network_knowledge
                .authority_provider()
                .await
                .elders_vec();
            self.send_proposal_with(recipients, proposal, &key_share)
                .await
        }
    }

    pub(crate) async fn handle_dkg_failure(
        &self,
        failure_set: DkgFailureSigSet,
    ) -> Result<Command> {
        let node_msg = SystemMsg::DkgFailureAgreement(failure_set);
        self.send_message_to_our_elders(node_msg).await
    }
}
