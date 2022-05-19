use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use bls_dkg::{PublicKeySet, SecretKeyShare};
use core::fmt::Debug;
use sn_interface::{
    messaging::system::{MembershipState, NodeState},
    network_knowledge::{recommended_section_size, SectionAuthorityProvider, MIN_ADULT_AGE},
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
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) fn split(
    prefix: &Prefix,
    nodes: impl IntoIterator<Item = XorName>,
) -> Option<(BTreeSet<XorName>, BTreeSet<XorName>)> {
    let decision_index: u8 = if let Ok(idx) = prefix.bit_count().try_into() {
        idx
    } else {
        return None;
    };

    let (one, zero) = nodes
        .into_iter()
        .filter(|name| prefix.matches(name))
        .partition(|name| name.bit(decision_index));

    Some((zero, one))
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

        let is_invalid_proposal = !self.validate_proposals(&signed_vote, prefix)?;
        let is_byzantine = self
            .consensus
            .detect_byzantine_voters(&signed_vote)
            .is_err();
        if is_invalid_proposal || is_byzantine {
            return Err(Error::Consensus(
                sn_consensus::Error::AttemptedFaultyProposal,
            ));
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
        if !self.validate_proposals(&signed_vote, prefix)? {
            error!("Membership - dropping faulty vote {signed_vote:?}");
            return Err(Error::Consensus(
                sn_consensus::Error::AttemptedFaultyProposal,
            ));
        }

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
    ) -> Result<bool> {
        // check we're section the vote is for our current membership state
        if signed_vote
            .validate_signature(&self.consensus.elders)
            .is_err()
        {
            error!("Membership - dropping signed vote that was not signed by current elders");
            return Ok(false);
        }

        // ensure we have a consensus instance for this votes generations
        if self.consensus_at_gen(signed_vote.vote.gen).is_err() {
            error!(
                "Membership - dropping signed vote from invalid gen {:?}",
                signed_vote.vote.gen
            );
            return Err(Error::RequestAntiEntropy);
        }

        for proposal in signed_vote.proposals() {
            if !self.validate_node_state(proposal, signed_vote.vote.gen, prefix)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn allowed_to_join(
        &self,
        joining_name: XorName,
        prefix: &Prefix,
        members: impl IntoIterator<Item = XorName>,
    ) -> bool {
        // We multiply by two to allow a buffer for when nodes are joining sequentially.
        let split_section_size_cap = recommended_section_size() * 2;

        match split(prefix, members) {
            Some((zeros, ones)) => {
                info!(
                    "Membership - Section {prefix:?} would split into {} zero and {} one nodes",
                    zeros.len(),
                    ones.len()
                );
                match joining_name.bit(prefix.bit_count() as u8) {
                    // joining node would be part of the `ones` child section
                    true => ones.len() < split_section_size_cap,

                    // joining node would be part of the `zeros` child section
                    false => zeros.len() < split_section_size_cap,
                }
            }
            None => false,
        }
    }

    #[allow(dead_code)]
    fn validate_node_age(&self, node_state: &NodeState) -> bool {
        let age = node_state.age();
        match node_state.state {
            MembershipState::Joined => age == MIN_ADULT_AGE,
            MembershipState::Relocated(_) => age > MIN_ADULT_AGE,
            MembershipState::Left => true,
        }
    }

    fn is_relocated_to_our_section(&self, name: &XorName) -> bool {
        self.history
            .values()
            .flat_map(|(decision, _)| decision.proposals.keys())
            .any(|node_state| {
                node_state.clone().into_state().previous_name().as_ref() == Some(name)
            })
    }

    fn validate_relocation_details(&self, node_state: &NodeState, prefix: &Prefix) -> bool {
        let name = node_state.name;
        if let MembershipState::Relocated(details) = &node_state.state {
            let dest = details.dst;

            if !prefix.matches(&dest) {
                info!(
		    "Membership - Ignoring relocate request from {name} - {dest} doesn't match our prefix {prefix:?}."
		);
                return false;
            }

            // We requires the node name matches the relocation details age.
            let age = details.age;
            let state_age = node_state.age();
            if age != state_age {
                info!(
		    "Membership - Ignoring relocation req from {name} - relocation age ({age}) doesn't match peer's age ({state_age})."
		);
                return false;
            }

            let prev_name = &details.previous_name;
            if self.is_relocated_to_our_section(prev_name) {
                info!("Membership - Ignoring relocation req from {name} - original node {prev_name:?} already relocated to us.");
                return false;
            }
        }

        true
    }

    fn validate_node_state(
        &self,
        node_state: NodeState,
        gen: Generation,
        prefix: &Prefix,
    ) -> Result<bool> {
        let name = node_state.name;
        info!(
            "Membership - validating node state for {name}, state {:?}",
            node_state.state
        );

        if !prefix.matches(&node_state.name) {
            warn!("Membership - rejecting node {name}, name doesn't match our prefix {prefix:?}");
            return Ok(false);
        }

        // TODO: disabled temporarily, until we can resolve node age issues
        // if !self.validate_node_age(&node_state) {
        //     warn!("Membership - rejecting node {name} with invalid age {}", node_state.age());
        //     return Ok(false);
        // }

        if !self.validate_relocation_details(&node_state, prefix) {
            warn!("Membership - rejecting node {name} with invalid relocation details");
            return Ok(false);
        }

        let members = self.section_members(gen - 1)?;
        let is_valid = match node_state.state {
            MembershipState::Joined | MembershipState::Relocated(_) => {
                if members.contains_key(&node_state.name) {
                    warn!("Membership - rejecting join from existing member {name}");
                    false
                } else if !self.allowed_to_join(node_state.name, prefix, members.keys().copied()) {
                    warn!("Membership - rejecting join since we are at capacity");
                    false
                } else {
                    true
                }
            }
            MembershipState::Left => {
                if members.get(&node_state.name).map(|n| &n.state) != Some(&MembershipState::Joined)
                {
                    warn!("Membership - rejecting leave from non-existing member");
                    false
                } else {
                    true
                }
            }
        };

        Ok(is_valid)
    }
}
