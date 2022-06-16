use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use bls_dkg::{PublicKeySet, SecretKeyShare};
use core::fmt::Debug;
use sn_interface::messaging::system::DkgSessionId;
use sn_interface::{
    messaging::system::{MembershipState, NodeState},
    network_knowledge::{partition_by_prefix, recommended_section_size, SectionAuthorityProvider},
    types::log_markers::LogMarker,
};

use thiserror::Error;
use xor_name::{Prefix, XorName};

use sn_consensus::{
    Ballot, Consensus, Decision, Generation, NodeId, SignedVote, Vote, VoteResponse,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Consensus error while processing vote {0}")]
    Consensus(#[from] sn_consensus::Error),
    #[error("We are behind the voter, caller should request anti-entropy")]
    RequestAntiEntropy,
    #[error("Invalid proposal")]
    InvalidProposal,
    #[error("Network Knowledge error {0:?}")]
    NetworkKnowledge(#[from] sn_interface::network_knowledge::Error),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

fn get_split_info(
    prefix: Prefix,
    members: &BTreeMap<XorName, NodeState>,
) -> Option<(BTreeSet<NodeState>, BTreeSet<NodeState>)> {
    let (zero, one) = partition_by_prefix(&prefix, members.keys().copied())?;

    // make sure the sections contain enough entries
    let split_threshold = recommended_section_size();
    if zero.len() < split_threshold || one.len() < split_threshold {
        return None;
    }

    Some((
        BTreeSet::from_iter(zero.into_iter().map(|n| members[&n].clone())),
        BTreeSet::from_iter(one.into_iter().map(|n| members[&n].clone())),
    ))
}

/// Checks if we can split the section
/// If we have enough nodes for both subsections, returns the DkgSessionId's
pub(crate) fn try_split_dkg(
    members: &BTreeMap<XorName, NodeState>,
    sap: &SectionAuthorityProvider,
    section_chain_len: u64,
    membership_gen: Generation,
) -> Option<(DkgSessionId, DkgSessionId)> {
    let prefix = sap.prefix();

    let (zero, one) = get_split_info(prefix, members)?;

    // get elders for section ...0
    let zero_prefix = prefix.pushed(false);
    let zero_elders = elder_candidates(zero.iter().cloned(), sap);

    // get elders for section ...1
    let one_prefix = prefix.pushed(true);
    let one_elders = elder_candidates(one.iter().cloned(), sap);

    // create the DKG session IDs
    let zero_id = DkgSessionId {
        prefix: zero_prefix,
        elders: BTreeMap::from_iter(zero_elders.iter().map(|node| (node.name, node.addr))),
        section_chain_len,
        bootstrap_members: zero,
        membership_gen,
    };
    let one_id = DkgSessionId {
        prefix: one_prefix,
        elders: BTreeMap::from_iter(one_elders.iter().map(|node| (node.name, node.addr))),
        section_chain_len,
        bootstrap_members: one,
        membership_gen,
    };

    Some((zero_id, one_id))
}

/// Returns the nodes that should be candidates to become the next elders, sorted by names.
pub(crate) fn elder_candidates(
    candidates: impl IntoIterator<Item = NodeState>,
    current_elders: &SectionAuthorityProvider,
) -> BTreeSet<NodeState> {
    use itertools::Itertools;
    use std::cmp::Ordering;

    // Compare candidates for the next elders. The one comparing `Less` wins.
    fn cmp_elder_candidates(
        lhs: &NodeState,
        rhs: &NodeState,
        current_elders: &SectionAuthorityProvider,
    ) -> Ordering {
        // Older nodes are preferred. In case of a tie, prefer current elders. If still a tie, break
        // it comparing by the signed signatures because it's impossible for a node to predict its
        // signature and therefore game its chances of promotion.
        rhs.age()
            .cmp(&lhs.age())
            .then_with(|| {
                let lhs_is_elder = current_elders.contains_elder(&lhs.name);
                let rhs_is_elder = current_elders.contains_elder(&rhs.name);

                match (lhs_is_elder, rhs_is_elder) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => Ordering::Equal,
                }
            })
            .then_with(|| lhs.name.cmp(&rhs.name))
        // TODO: replace name cmp above with sig cmp.
        // .then_with(|| lhs.sig.signature.cmp(&rhs.sig.signature))
    }

    candidates
        .into_iter()
        .sorted_by(|lhs, rhs| cmp_elder_candidates(lhs, rhs, current_elders))
        .take(sn_interface::elder_count())
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct Membership {
    consensus: Consensus<NodeState>,
    bootstrap_members: BTreeSet<NodeState>,
    gen: Generation,
    history: BTreeMap<Generation, (Decision<NodeState>, Consensus<NodeState>)>,
}

impl Membership {
    pub(crate) fn from(
        secret_key: (NodeId, SecretKeyShare),
        elders: PublicKeySet,
        n_elders: usize,
        bootstrap_members: BTreeSet<NodeState>,
    ) -> Self {
        Membership {
            consensus: Consensus::from(secret_key, elders, n_elders),
            bootstrap_members,
            gen: 0,
            history: BTreeMap::default(),
        }
    }

    pub(crate) fn generation(&self) -> Generation {
        self.gen
    }

    pub(crate) fn voters_public_key_set(&self) -> &PublicKeySet {
        &self.consensus.elders
    }

    pub(crate) fn most_recent_decision(&self) -> Option<&Decision<NodeState>> {
        self.history.values().last().map(|(d, _)| d)
    }

    #[cfg(test)]
    pub(crate) fn is_churn_in_progress(&self) -> bool {
        !self.consensus.votes.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn force_bootstrap(&mut self, state: NodeState) {
        let _ = self.bootstrap_members.insert(state);
    }

    /// Return the current generation vote we have made
    pub(crate) fn get_our_latest_vote(&self) -> Option<&SignedVote<NodeState>> {
        let current_consensus = &self.consensus;
        let (id, _) = current_consensus.secret_key;

        current_consensus.votes.get(&id)
    }

    fn consensus_at_gen(&self, gen: Generation) -> Result<&Consensus<NodeState>> {
        if gen == self.gen + 1 {
            Ok(&self.consensus)
        } else {
            self.history
                .get(&gen)
                .map(|(_, c)| c)
                .ok_or(Error::Consensus(sn_consensus::Error::BadGeneration {
                    requested_gen: gen,
                    gen: self.gen,
                }))
        }
    }

    fn consensus_at_gen_mut(&mut self, gen: Generation) -> Result<&mut Consensus<NodeState>> {
        if gen == self.gen + 1 {
            Ok(&mut self.consensus)
        } else {
            self.history
                .get_mut(&gen)
                .map(|(_, c)| c)
                .ok_or(Error::Consensus(sn_consensus::Error::BadGeneration {
                    requested_gen: gen,
                    gen: self.gen,
                }))
        }
    }

    pub(crate) fn is_leaving_section(&self, node: &NodeState, our_prefix: Prefix) -> bool {
        // TODO: ideally this logic is combined with the logic in self.section_members() for deciding if a node is leaving
        match &node.state {
            MembershipState::Joined => false,
            MembershipState::Left => true,
            MembershipState::Relocated(r) => !our_prefix.matches(&r.dst),
        }
    }

    pub(crate) fn current_section_members(&self) -> BTreeMap<XorName, NodeState> {
        self.section_members(self.gen).unwrap_or_default()
    }

    pub(crate) fn section_members(&self, gen: Generation) -> Result<BTreeMap<XorName, NodeState>> {
        let mut members =
            BTreeMap::from_iter(self.bootstrap_members.iter().cloned().map(|n| (n.name, n)));

        if gen == 0 {
            return Ok(members);
        }

        for (history_gen, (decision, _)) in self.history.iter() {
            for (node_state, _sig) in decision.proposals.iter() {
                match node_state.state {
                    MembershipState::Joined => {
                        let _ = members.insert(node_state.name, node_state.clone());
                    }
                    MembershipState::Left => {
                        let _ = members.remove(&node_state.name);
                    }
                    MembershipState::Relocated(_) => {
                        if let Entry::Vacant(e) = members.entry(node_state.name) {
                            let _ = e.insert(node_state.clone());
                        } else {
                            let _ = members.remove(&node_state.name);
                        }
                    }
                }
            }

            if history_gen == &gen {
                return Ok(members);
            }
        }

        Err(Error::Consensus(sn_consensus::Error::InvalidGeneration(
            gen,
        )))
    }

    pub(crate) fn propose(
        &mut self,
        node_state: NodeState,
        prefix: &Prefix,
    ) -> Result<SignedVote<NodeState>> {
        info!("[{}] proposing {:?}", self.id(), node_state);
        let vote = Vote {
            gen: self.gen + 1,
            ballot: Ballot::Propose(node_state),
            faults: self.consensus.faults(),
        };
        let signed_vote = self.sign_vote(vote)?;

        self.validate_proposals(&signed_vote, prefix)?;
        if let Err(e) = signed_vote.detect_byzantine_faults(
            &self.consensus.elders,
            &self.consensus.votes,
            &self.consensus.processed_votes_cache,
        ) {
            error!("Attempted invalid proposal: {e:?}");
            return Err(Error::InvalidProposal);
        }

        self.cast_vote(signed_vote)
    }

    pub(crate) fn anti_entropy(&self, from_gen: Generation) -> Result<Vec<SignedVote<NodeState>>> {
        let mut msgs = self
            .history
            .iter() // history is a BTreeSet, .iter() is ordered by generation
            .filter(|(gen, _)| **gen > from_gen)
            .map(|(gen, (decision, c))| {
                Ok(c.build_super_majority_vote(decision.votes.clone(), decision.faults.clone(), *gen)?)
            })
            .collect::<Result<Vec<_>>>()?;

        // include the current in-progres votes as well.
        msgs.extend(self.consensus.votes.values().cloned());

        info!(
            "Membership - anti-entropy from gen {}..{}: {} msgs",
            from_gen,
            self.gen,
            msgs.len()
        );

        Ok(msgs)
    }

    pub(crate) fn id(&self) -> NodeId {
        self.consensus.id()
    }

    pub(crate) fn handle_signed_vote(
        &mut self,
        signed_vote: SignedVote<NodeState>,
        prefix: &Prefix,
    ) -> Result<VoteResponse<NodeState>> {
        self.validate_proposals(&signed_vote, prefix)?;

        let vote_gen = signed_vote.vote.gen;

        let consensus = self.consensus_at_gen_mut(vote_gen)?;

        info!(
            "Membership - accepted signed vote from voter {:?}",
            signed_vote.voter
        );
        let vote_response = consensus.handle_signed_vote(signed_vote)?;

        if let Some(decision) = consensus.decision.clone() {
            if vote_gen == self.gen + 1 {
                info!(
                    "Membership - decided {:?}",
                    BTreeSet::from_iter(decision.proposals.keys())
                );

                debug!("{}", LogMarker::VotedOffline);
                let next_consensus = Consensus::from(
                    self.consensus.secret_key.clone(),
                    self.consensus.elders.clone(),
                    self.consensus.n_elders,
                );

                let decided_consensus = std::mem::replace(&mut self.consensus, next_consensus);
                let _ = self.history.insert(vote_gen, (decision, decided_consensus));
                self.gen = vote_gen
            }
        }

        Ok(vote_response)
    }

    fn sign_vote(&self, vote: Vote<NodeState>) -> Result<SignedVote<NodeState>> {
        Ok(self.consensus.sign_vote(vote)?)
    }

    pub(crate) fn cast_vote(
        &mut self,
        signed_vote: SignedVote<NodeState>,
    ) -> Result<SignedVote<NodeState>> {
        Ok(self.consensus.cast_vote(signed_vote)?)
    }

    /// Returns true if the proposal is valid
    fn validate_proposals(
        &self,
        signed_vote: &SignedVote<NodeState>,
        prefix: &Prefix,
    ) -> Result<()> {
        // check we're section the vote is for our current membership state
        signed_vote.validate_signature(&self.consensus.elders)?;

        // ensure we have a consensus instance for this votes generations
        let _ = self
            .consensus_at_gen(signed_vote.vote.gen)
            .map_err(|_| Error::RequestAntiEntropy)?;

        let members =
            BTreeSet::from_iter(self.section_members(signed_vote.vote.gen - 1)?.into_keys());

        for proposal in signed_vote.proposals() {
            proposal.into_state().validate(prefix, &members)?;
        }

        Ok(())
    }
}
