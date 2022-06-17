// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{Node, Proposal, SystemMsg},
    handover::{Error as HandoverError, Handover},
    membership::{elder_candidates, try_split_dkg},
    Error, Peer, Result,
};

use sn_consensus::{Generation, SignedVote, VoteResponse};
use sn_interface::{
    messaging::system::{NodeState, SectionAuth},
    network_knowledge::SapCandidate,
    types::log_markers::LogMarker,
    SectionAuthorityProvider,
};

use std::collections::{BTreeMap, BTreeSet};
use xor_name::{Prefix, XorName};

impl Node {
    /// helper to handle a handover vote
    #[instrument(skip(self), level = "trace")]
    fn handle_vote(
        &self,
        handover_state: &mut Handover,
        signed_vote: SignedVote<SapCandidate>,
        peer: Peer,
    ) -> Result<Vec<Cmd>> {
        match handover_state.handle_signed_vote(signed_vote.clone()) {
            Ok(VoteResponse::Broadcast(signed_vote)) => {
                trace!(
                    ">>> Handover Vote msg successfully handled, broadcasting our vote: {:?}",
                    signed_vote
                );
                Ok(self.broadcast_handover_vote_msg(signed_vote))
            }
            Ok(VoteResponse::WaitingForMoreVotes) => {
                trace!(
                    ">>> Handover Vote msg successfully handled, awaiting for more votes: {:?}",
                    signed_vote
                );
                Ok(vec![])
            }
            Err(HandoverError::RequestAntiEntropy) => {
                trace!("Handover - We are behind the voter, requesting AE");
                Err(Error::RequestHandoverAntiEntropy(
                    handover_state.generation(),
                ))
            }
            Err(err) => {
                error!(">>> Failed to handle handover Vote msg: {:?}", err);
                Ok(vec![])
            }
        }
    }

    /// Verifies the SAP signature and checks that the signature's public key matches the
    /// signature of the SAP, because SAP candidates are signed by the candidate section key
    fn check_sap_sig(
        &self,
        sap: &SectionAuth<SectionAuthorityProvider>,
        gen: Generation,
    ) -> Result<()> {
        let sap_bytes = Proposal::SectionInfo {
            sap: sap.value.clone(),
            generation: gen,
        }
        .as_signable_bytes()?;
        if !sap.sig.verify(&sap_bytes) {
            return Err(Error::InvalidSignature);
        }
        if sap.value.section_key() != sap.sig.public_key {
            return Err(Error::UntrustedSectionAuthProvider(format!(
                "the auth around the SAP does not match the SAP's public key: {:?} != {:?}",
                sap.sig.public_key,
                sap.value.section_key(),
            )));
        }
        Ok(())
    }

    async fn get_members_at_gen(&self, gen: Generation) -> Result<BTreeMap<XorName, NodeState>> {
        if let Some(m) = &*self.membership.borrow() {
            Ok(m.section_members(gen)?)
        } else {
            error!("Missing membership instance when checking handover SAP candidates");
            Err(Error::MissingMembershipInstance)
        }
    }

    async fn get_sap_for_prefix(&self, prefix: Prefix) -> Result<SectionAuthorityProvider> {
        self.network_knowledge
            .prefix_map()
            .get(&prefix)
            .ok_or(Error::FailedToGetSAPforPrefix(prefix))
    }

    async fn check_elder_handover_candidates(&self, sap: &SectionAuthorityProvider) -> Result<()> {
        // in regular handover the previous SAP's prefix is the same
        let previous_gen_sap = self.get_sap_for_prefix(sap.prefix()).await?;
        let members = self.get_members_at_gen(sap.membership_gen()).await?;
        let received_candidates: BTreeSet<&Peer> = sap.elders().collect();

        let expected_peers: BTreeSet<Peer> =
            elder_candidates(members.values().cloned(), &previous_gen_sap)
                .iter()
                .map(|node| Peer::new(node.name, node.addr))
                .collect();
        let expected_candidates: BTreeSet<&Peer> = expected_peers.iter().collect();
        if received_candidates != expected_candidates {
            debug!("InvalidElderCandidates: received SAP at gen {} with candidates {:#?}, expected candidates {:#?}", sap.membership_gen(), received_candidates, expected_candidates);
            return Err(Error::InvalidElderCandidates);
        }
        Ok(())
    }

    async fn check_section_split_candidates(
        &self,
        sap1: &SectionAuthorityProvider,
        sap2: &SectionAuthorityProvider,
    ) -> Result<()> {
        // in split handover, the previous SAP's prefix is prefix.popped()
        // we use gen/prefix from sap1, both SAPs in a split have the same generation
        // and the same ancestor prefix
        let prev_prefix = sap1.prefix().popped();
        let previous_gen_sap = self.get_sap_for_prefix(prev_prefix).await?;
        let members = self.get_members_at_gen(sap1.membership_gen()).await?;
        let dummy_chain_len = 0;
        let dummy_gen = 0;

        let received_candidates1: BTreeSet<&Peer> = sap1.elders().collect();
        let received_candidates2: BTreeSet<&Peer> = sap2.elders().collect();

        if let Some((dkg1, dkg2)) =
            try_split_dkg(&members, &previous_gen_sap, dummy_chain_len, dummy_gen)
        {
            let expected_peers1: BTreeSet<Peer> =
                dkg1.elders.iter().map(|(n, a)| Peer::new(*n, *a)).collect();
            let expected_peers2: BTreeSet<Peer> =
                dkg2.elders.iter().map(|(n, a)| Peer::new(*n, *a)).collect();
            let expected_candidates1: BTreeSet<&Peer> = expected_peers1.iter().collect();
            let expected_candidates2: BTreeSet<&Peer> = expected_peers2.iter().collect();

            // the order of these SAPs is not absolute, so we try both comparisons
            if (received_candidates1 != expected_candidates1
                || received_candidates2 != expected_candidates2)
                && (received_candidates2 != expected_candidates1
                    || received_candidates1 != expected_candidates2)
            {
                debug!("InvalidElderCandidates: received SAP1 at gen {} with candidates {:#?}, expected candidates {:#?}", sap1.membership_gen(), received_candidates1, expected_candidates1);
                debug!("InvalidElderCandidates: received SAP2 at gen {} with candidates {:#?}, expected candidates {:#?}", sap2.membership_gen(), received_candidates2, expected_candidates2);
                return Err(Error::InvalidElderCandidates);
            }
            Ok(())
        } else {
            Err(Error::InvalidSplitCandidates)
        }
    }

    async fn check_sap_candidate_prefix(&self, sap_candidate: &SapCandidate) -> Result<()> {
        let section_prefix = self.network_knowledge.prefix();
        match sap_candidate {
            SapCandidate::ElderHandover(single_sap) => {
                // single handover, must be same prefix
                if single_sap.prefix() == section_prefix {
                    Ok(())
                } else {
                    Err(Error::InvalidSectionPrefixForCandidate)
                }
            }
            SapCandidate::SectionSplit(sap1, sap2) => {
                // section split, must be 2 distinct children prefixes
                let our_p = &section_prefix;
                let p1 = sap1.prefix();
                let p2 = sap2.prefix();
                if p1.is_extension_of(our_p)
                    && p2.is_extension_of(our_p)
                    && p1.bit_count() == our_p.bit_count() + 1
                    && p2.bit_count() == our_p.bit_count() + 1
                    && p1 != p2
                {
                    Ok(())
                } else {
                    Err(Error::InvalidSectionPrefixForSplitCandidates)
                }
            }
        }
    }

    /// Checks if the elder candidates in the SAP match the oldest elders in the corresponding
    /// membership generation this SAP was proposed at
    /// Also checks the SAP signature
    async fn check_sap_candidate(
        &self,
        sap_candidate: &SapCandidate,
        gen: Generation,
    ) -> Result<()> {
        self.check_sap_candidate_prefix(sap_candidate).await?;
        match sap_candidate {
            SapCandidate::ElderHandover(authed_sap) => {
                self.check_sap_sig(authed_sap, gen)?;
                self.check_elder_handover_candidates(&authed_sap.value)
                    .await
            }
            SapCandidate::SectionSplit(authed_sap1, authed_sap2) => {
                self.check_sap_sig(authed_sap1, gen)?;
                self.check_sap_sig(authed_sap2, gen)?;
                self.check_section_split_candidates(&authed_sap1.value, &authed_sap2.value)
                    .await
            }
        }
    }

    async fn check_signed_vote_saps(&self, signed_vote: &SignedVote<SapCandidate>) -> Result<()> {
        let sap_candidates = signed_vote.proposals();
        let gen = signed_vote.vote.gen;
        let checks: Vec<_> = sap_candidates
            .iter()
            .map(|sap_can| self.check_sap_candidate(sap_can, gen))
            .collect();
        let _ = futures::future::try_join_all(checks).await?;
        Ok(())
    }

    async fn handle_handover_vote(
        &self,
        peer: Peer,
        signed_vote: SignedVote<SapCandidate>,
    ) -> Result<Vec<Cmd>> {
        self.check_signed_vote_saps(&signed_vote).await?;

        if let Some(ref mut handover_state) = *self.handover_voting.borrow_mut() {
            let mut cmds = self.handle_vote(handover_state, signed_vote, peer)?;

            // check for unsuccessful termination
            handover_state.handle_empty_set_decision();

            // check for successful termination
            if let Some(candidates_sap) = handover_state.consensus_value() {
                debug!(
                    "{}: {:?}",
                    LogMarker::HandoverConsensusTermination,
                    candidates_sap
                );

                let bcast_cmds = self.broadcast_handover_decision(candidates_sap);
                cmds.extend(bcast_cmds);
            }
            Ok(cmds)
        } else {
            trace!("Non-elder node unexpectedly received handover Vote msg, ignoring...");
            Ok(vec![])
        }
    }

    pub(crate) async fn handle_handover_msg(
        &self,
        peer: Peer,
        signed_votes: Vec<SignedVote<SapCandidate>>,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::HandoverMsgBeingHandled);

        let mut cmds = vec![];

        for vote in signed_votes {
            match self.handle_handover_vote(peer, vote).await {
                Ok(commands) => {
                    cmds.extend(commands);
                }
                Err(Error::RequestHandoverAntiEntropy(gen)) => {
                    // We hit an error while processing this vote, perhaps we are missing information.
                    // We'll send a handover AE request to see if they can help us catch up.
                    let sap = self.network_knowledge.authority_provider();
                    let dst_section_pk = sap.section_key();
                    let section_name = self.network_knowledge.prefix().name();
                    let msg = SystemMsg::HandoverAE(gen);
                    let cmd = self.send_direct_msg_to_nodes(
                        vec![peer],
                        msg,
                        section_name,
                        dst_section_pk,
                    )?;

                    debug!("{:?}", LogMarker::HandoverSendingAeUpdateRequest);
                    cmds.push(cmd);
                    // return the vec w/ the AE cmd there so as not to loop and generate AE for
                    // any subsequent commands
                    return Ok(cmds);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(cmds)
    }

    pub(crate) async fn handle_handover_anti_entropy(
        &self,
        peer: Peer,
        gen: Generation,
    ) -> Result<Vec<Cmd>> {
        debug!(
            "{:?} handover anti-entropy request for gen {:?} from {}",
            LogMarker::HandoverAeRequestReceived,
            gen,
            peer,
        );

        let cmds = if let Some(handover) = &*self.handover_voting.borrow() {
            match handover.anti_entropy(gen) {
                Ok(catchup_votes) => {
                    vec![self.send_direct_msg(
                        peer,
                        SystemMsg::HandoverVotes(catchup_votes),
                        self.network_knowledge.section_key(),
                    )?]
                }
                Err(e) => {
                    error!("Handover - Error while processing anti-entropy {:?}", e);
                    vec![]
                }
            }
        } else {
            error!("Unexpected attempt to handle handover anti-entropy when we don't have a handover instance (handover is for elders only)");
            vec![]
        };

        Ok(cmds)
    }
}
