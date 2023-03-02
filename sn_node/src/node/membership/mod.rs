// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.
use bls::{PublicKeySet, SecretKeyShare};
use core::fmt::Debug;
use sn_consensus::{
    Ballot, Consensus, Decision, Generation, NodeId, SignedVote, Vote, VoteResponse,
};
use sn_interface::{
    messaging::system::DkgSessionId,
    network_knowledge::{
        partition_by_prefix, recommended_section_size, MembershipState, NodeState,
        SectionAuthorityProvider,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use thiserror::Error;
use xor_name::{Prefix, XorName};

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
/// If we have enough nodes for both subsections, returns the `DkgSessionId`'s
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
        elders: BTreeMap::from_iter(zero_elders.iter().map(|node| (node.name(), node.addr()))),
        section_chain_len,
        bootstrap_members: zero,
        membership_gen,
    };
    let one_id = DkgSessionId {
        prefix: one_prefix,
        elders: BTreeMap::from_iter(one_elders.iter().map(|node| (node.name(), node.addr()))),
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
                let lhs_is_elder = current_elders.contains_elder(&lhs.name());
                let rhs_is_elder = current_elders.contains_elder(&rhs.name());

                match (lhs_is_elder, rhs_is_elder) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => Ordering::Equal,
                }
            })
            .then_with(|| lhs.name().cmp(&rhs.name()))
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
    // last membership vote timestamp
    last_received_vote_time: Option<Instant>,
}

impl Membership {
    #[instrument]
    pub(crate) fn from(
        secret_key: (NodeId, SecretKeyShare),
        elders: PublicKeySet,
        n_elders: usize,
        bootstrap_members: BTreeSet<NodeState>,
    ) -> Self {
        trace!("Membership - Creating new membership instance");
        Membership {
            consensus: Consensus::from(secret_key, elders, n_elders),
            bootstrap_members,
            gen: 0,
            history: BTreeMap::default(),
            last_received_vote_time: None,
        }
    }

    pub(crate) fn section_key_set(&self) -> PublicKeySet {
        self.consensus.elders.clone()
    }

    pub(crate) fn last_received_vote_time(&self) -> Option<Instant> {
        self.last_received_vote_time
    }

    pub(crate) fn generation(&self) -> Generation {
        self.gen
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

    pub(crate) fn current_section_members(&self) -> BTreeMap<XorName, NodeState> {
        self.joined_section_members_at_gen(self.gen)
            .unwrap_or_default()
    }

    pub(crate) fn archived_members(&self) -> BTreeSet<XorName> {
        let mut members = BTreeSet::from_iter(
            self.bootstrap_members
                .iter()
                .filter(|n| {
                    matches!(
                        n.state(),
                        MembershipState::Left | MembershipState::Relocated(..)
                    )
                })
                .map(|n| n.name()),
        );

        for (decision, _) in self.history.values() {
            for node_state in decision.proposals.keys() {
                match node_state.state() {
                    MembershipState::Joined => {
                        continue;
                    }
                    MembershipState::Left | MembershipState::Relocated(_) => {
                        let _ = members.insert(node_state.name());
                    }
                }
            }
        }

        members
    }

    /// get only section members reporting Joined at gen
    pub(crate) fn joined_section_members_at_gen(
        &self,
        gen: Generation,
    ) -> Result<BTreeMap<XorName, NodeState>> {
        let mut members = BTreeMap::from_iter(
            self.bootstrap_members
                .iter()
                .cloned()
                .filter(|n| matches!(n.state(), MembershipState::Joined))
                .map(|n| (n.name(), n)),
        );

        if gen == 0 {
            return Ok(members);
        }

        for (history_gen, (decision, _)) in &self.history {
            for node_state in decision.proposals.keys() {
                match node_state.state() {
                    MembershipState::Joined => {
                        let _ = members.insert(node_state.name(), node_state.clone());
                    }
                    MembershipState::Left => {
                        let _ = members.remove(&node_state.name());
                    }
                    MembershipState::Relocated(_) => {
                        let _ = members.remove(&node_state.name());
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
            ballot: Ballot::Propose(node_state.clone()),
            faults: self.consensus.faults(),
        };
        let signed_vote = self.sign_vote(vote)?;

        // For relocation, the `validate_proposals` will call `NodeState::validate`,
        // where the name of the node_state is using old_name, and won't match the relocate_details
        // within the node_state, hence fail the `expected age` check.
        self.validate_proposals(&signed_vote, prefix)?;
        if let Err(e) = signed_vote.detect_byzantine_faults(
            &self.consensus.elders,
            &self.consensus.votes,
            &self.consensus.processed_votes_cache,
        ) {
            error!(
                "Attempted proposal {node_state:?} at genereation {:?} invalidated with error {e:?}",
                self.gen + 1
            );
            return Err(Error::InvalidProposal);
        }

        self.cast_vote(signed_vote)
    }

    pub(crate) fn anti_entropy(&self, from_gen: Generation) -> Result<Vec<SignedVote<NodeState>>> {
        let mut msgs = self
            .history
            .iter() // history is a BTreeSet, .iter() is ordered by generation
            .filter(|(gen, _)| **gen >= from_gen)
            .map(|(gen, (decision, c))| {
                Ok(c.build_super_majority_vote(
                    decision.votes.clone(),
                    decision.faults.clone(),
                    *gen,
                )?)
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
    ) -> Result<(VoteResponse<NodeState>, Option<Decision<NodeState>>)> {
        self.validate_proposals(&signed_vote, prefix)?;

        let vote_gen = signed_vote.vote.gen;
        let is_ongoing_consensus = vote_gen == self.gen + 1;
        let consensus = self.consensus_at_gen_mut(vote_gen)?;
        let is_fresh_vote = !consensus.processed_votes_cache.contains(&signed_vote.sig);

        info!(
            "Membership - accepted signed vote from voter {:?}",
            signed_vote.voter
        );
        let vote_response = consensus.handle_signed_vote(signed_vote)?;

        debug!("Membership - Vote response: {vote_response:?}");
        let decision = if let Some(decision) = consensus.decision.clone() {
            if is_ongoing_consensus {
                info!(
                    "Membership - decided {:?}",
                    BTreeSet::from_iter(decision.proposals.keys())
                );

                // wipe the last vote time
                self.last_received_vote_time = None;

                let next_consensus = Consensus::from(
                    self.consensus.secret_key.clone(),
                    self.consensus.elders.clone(),
                    self.consensus.n_elders,
                );

                let decided_consensus = std::mem::replace(&mut self.consensus, next_consensus);
                let _ = self
                    .history
                    .insert(vote_gen, (decision.clone(), decided_consensus));
                info!(
                    "Membership - updated generation from {:?} to {:?}",
                    self.gen, vote_gen
                );
                trace!("Membership - history is {:?}", self.history);
                self.gen = vote_gen;

                Some(decision)
            } else {
                None
            }
        } else {
            // if this is our ongoing round, lets log the vote
            if is_ongoing_consensus && is_fresh_vote {
                self.last_received_vote_time = Some(Instant::now());
            }

            None
        };

        Ok((vote_response, decision))
    }

    fn sign_vote(&self, vote: Vote<NodeState>) -> Result<SignedVote<NodeState>> {
        Ok(self.consensus.sign_vote(vote)?)
    }

    pub(crate) fn cast_vote(
        &mut self,
        signed_vote: SignedVote<NodeState>,
    ) -> Result<SignedVote<NodeState>> {
        self.last_received_vote_time = Some(Instant::now());
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

        let members = BTreeMap::from_iter(
            self.joined_section_members_at_gen(signed_vote.vote.gen - 1)?
                .into_iter(),
        );

        let archived_members = self.archived_members();

        for proposal in signed_vote.proposals() {
            if let Err(err) = proposal.validate_node_state(prefix, &members, &archived_members) {
                warn!("Failed to validate {proposal:?} with error {:?}", err);
                // TODO: certain errors need AE?
                warn!(
                    "Members at generation {} are: {:?}",
                    signed_vote.vote.gen - 1,
                    members
                );
                warn!("Archived members are {:?}", archived_members);
                return Err(Error::NetworkKnowledge(err));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::node::flow_ctrl::tests::network_builder::TestNetworkBuilder;
    use assert_matches::assert_matches;
    use eyre::Result;
    use rand::thread_rng;
    use sn_interface::{network_knowledge::NodeState, test_utils::gen_peer};
    use xor_name::Prefix;

    #[tokio::test]
    async fn multiple_proposals_in_a_single_generation_should_not_be_possible() -> Result<()> {
        let prefix = Prefix::default();
        let env = TestNetworkBuilder::new(thread_rng())
            .sap(prefix, 7, 0, None, None)
            .build()?;

        let mut membership = env
            .get_nodes(prefix, 1, 0, None)?
            .remove(0)
            .membership
            .expect("Membership for the elder should've been initialized");

        let state1 = NodeState::joined(gen_peer(5), None);
        let state2 = NodeState::joined(gen_peer(5), None);

        let _ = membership.propose(state1, &prefix)?;
        assert_matches!(
            membership.propose(state2, &prefix),
            Err(Error::InvalidProposal)
        );

        Ok(())
    }
}
