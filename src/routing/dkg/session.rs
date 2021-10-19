// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::{
    system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, ElderCandidates, SystemMsg},
    DstLocation, SectionAuthorityProvider, WireMsg,
};
use crate::routing::{
    dkg::dkg_msgs_utils::{DkgFailureSigSetUtils, DkgFailureSigUtils},
    ed25519,
    error::Result,
    log_markers::LogMarker,
    messages::WireMsgUtils,
    node::Node,
    routing_api::command::{next_timer_token, Command},
    section::SectionKeyShare,
    SectionAuthorityProviderUtils,
};
use crate::types::PublicKey;
use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::{message::Message as DkgMessage, KeyGen};
use itertools::Itertools;
use std::{
    collections::{BTreeSet, VecDeque},
    iter, mem,
    net::SocketAddr,
    time::Duration,
};
use xor_name::XorName;

// Interval to progress DKG timed phase
const DKG_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

const BACKLOG_CAPACITY: usize = 100;

// Data for a DKG participant.
pub(crate) struct Session {
    pub(crate) elder_candidates: ElderCandidates,
    pub(crate) participant_index: usize,
    pub(crate) key_gen: KeyGen,
    pub(crate) timer_token: u64,
    pub(crate) failures: DkgFailureSigSet,
    // Flag to track whether this session has completed (either with success or failure). We don't
    // remove complete sessions because the other participants might still need us to respond to
    // their messages.
    pub(crate) complete: bool,
}

impl Session {
    pub(crate) fn timer_token(&self) -> u64 {
        self.timer_token
    }

    pub(crate) fn process_message(
        &mut self,
        node: &Node,
        session_id: &DkgSessionId,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        trace!("process DKG message {:?}", message);
        let responses = self
            .key_gen
            .handle_message(&mut rand::thread_rng(), message)
            .unwrap_or_default();

        // Only a valid DkgMessage, which results in some responses, shall reset the ticker.
        let add_reset_timer = !responses.is_empty();

        let mut commands = vec![];
        for response in responses.into_iter() {
            commands.extend(self.broadcast(node, session_id, response, section_pk)?);
        }
        if add_reset_timer {
            commands.push(self.reset_timer());
        }
        commands.extend(self.check(node, session_id, section_pk)?);
        Ok(commands)
    }

    fn recipients(&self) -> Vec<(XorName, SocketAddr)> {
        self.elder_candidates
            .elders
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.participant_index)
            .map(|(_, (name, addr))| (*name, *addr))
            .collect()
    }

    pub(crate) fn broadcast(
        &mut self,
        node: &Node,
        session_id: &DkgSessionId,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];

        let recipients = self.recipients();
        if !recipients.is_empty() {
            trace!(
                "DKG broadcasting {:?} - {:?} to {:?}",
                message,
                session_id,
                recipients
            );
            let node_msg = SystemMsg::DkgMessage {
                session_id: *session_id,
                message: message.clone(),
            };
            let wire_msg = WireMsg::single_src(
                node,
                DstLocation::Section {
                    name: XorName::from(PublicKey::Bls(section_pk)),
                    section_pk,
                },
                node_msg,
                section_pk,
            )?;
            trace!("{}", LogMarker::DkgBroadcastMsg);

            commands.push(Command::SendMessage {
                recipients,
                wire_msg,
            });
        }

        commands.extend(self.process_message(node, session_id, message, section_pk)?);
        Ok(commands)
    }

    pub(crate) fn handle_timeout(
        &mut self,
        node: &Node,
        session_id: &DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if self.complete {
            return Ok(vec![]);
        }

        trace!("DKG progressing for {:?}", self.elder_candidates);

        match self.key_gen.timed_phase_transition(&mut rand::thread_rng()) {
            Ok(messages) => {
                let mut commands = vec![];
                for message in messages.into_iter() {
                    commands.extend(self.broadcast(node, session_id, message, section_pk)?);
                }
                commands.push(self.reset_timer());
                commands.extend(self.check(node, session_id, section_pk)?);
                Ok(commands)
            }
            Err(error) => {
                trace!("DKG failed for {:?}: {}", self.elder_candidates, error);
                let failed_participants = self.key_gen.possible_blockers();

                self.report_failure(node, session_id, failed_participants, section_pk)
            }
        }
    }

    // Check whether a key generator is finalized to give a DKG outcome.
    fn check(
        &mut self,
        node: &Node,
        session_id: &DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if self.complete {
            trace!("DKG check: complete");
            return Ok(vec![]);
        }

        if !self.key_gen.is_finalized() {
            trace!("DKG check: not finalised");
            return Ok(vec![]);
        }

        let (participants, outcome) = if let Some(tuple) = self.key_gen.generate_keys() {
            tuple
        } else {
            return Ok(vec![]);
        };

        // Less than 100% participation
        if !participants.iter().eq(self.elder_candidates.elders.keys()) {
            trace!(
                "DKG failed due to unexpected participants for {:?}: {:?}",
                self.elder_candidates,
                participants.iter().format(", ")
            );

            let failed_participants: BTreeSet<_> = self
                .elder_candidates
                .elders
                .keys()
                .filter_map(|elder| {
                    if !participants.contains(elder) {
                        Some(*elder)
                    } else {
                        None
                    }
                })
                .collect();

            return self.report_failure(node, session_id, failed_participants, section_pk);
        }

        // Corrupted DKG outcome. This can happen when a DKG session is restarted using the same set
        // of participants and the same generation, but some of the participants are unaware of the
        // restart (due to lag, etc...) and keep sending messages for the original session which
        // then get mixed with the messages of the restarted session.
        if outcome
            .public_key_set
            .public_key_share(self.participant_index)
            != outcome.secret_key_share.public_key_share()
        {
            trace!(
                "DKG failed due to corrupted outcome for {:?}",
                self.elder_candidates
            );
            return self.report_failure(node, session_id, BTreeSet::new(), section_pk);
        }

        trace!(
            "DKG complete for {:?}: {:?}",
            self.elder_candidates,
            outcome.public_key_set.public_key()
        );

        self.complete = true;
        let section_auth = SectionAuthorityProvider::from_elder_candidates(
            self.elder_candidates.clone(),
            outcome.public_key_set.clone(),
        );

        let outcome = SectionKeyShare {
            public_key_set: outcome.public_key_set,
            index: self.participant_index,
            secret_key_share: outcome.secret_key_share,
        };

        Ok(vec![Command::HandleDkgOutcome {
            section_auth,
            outcome,
        }])
    }

    fn report_failure(
        &mut self,
        node: &Node,
        session_id: &DkgSessionId,
        failed_participants: BTreeSet<XorName>,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        let sig = DkgFailureSig::new(&node.keypair, &failed_participants, session_id);

        if !self.failures.insert(sig, &failed_participants) {
            return Ok(vec![]);
        }

        let cmds = self
            .check_failure_agreement()
            .into_iter()
            .chain(iter::once({
                let node_msg = SystemMsg::DkgFailureObservation {
                    session_id: *session_id,
                    sig,
                    failed_participants,
                };
                let wire_msg = WireMsg::single_src(
                    node,
                    DstLocation::Section {
                        name: XorName::from(PublicKey::Bls(section_pk)),
                        section_pk,
                    },
                    node_msg,
                    section_pk,
                )?;
                trace!("{}", LogMarker::DkgSendFailureObservation);
                Command::SendMessage {
                    recipients: self.recipients(),
                    wire_msg,
                }
            }))
            .collect();

        Ok(cmds)
    }

    pub(crate) fn process_failure(
        &mut self,
        session_id: &DkgSessionId,
        failed_participants: &BTreeSet<XorName>,
        signed: DkgFailureSig,
    ) -> Option<Command> {
        if !self
            .elder_candidates
            .elders
            .contains_key(&ed25519::name(&signed.public_key))
        {
            return None;
        }

        if !signed.verify(session_id, failed_participants) {
            return None;
        }

        if !self.failures.insert(signed, failed_participants) {
            return None;
        }

        self.check_failure_agreement()
    }

    fn check_failure_agreement(&mut self) -> Option<Command> {
        if self.failures.has_agreement(&self.elder_candidates) {
            self.complete = true;

            Some(Command::HandleDkgFailure(mem::take(&mut self.failures)))
        } else {
            None
        }
    }

    fn reset_timer(&mut self) -> Command {
        self.timer_token = next_timer_token();
        Command::ScheduleTimeout {
            duration: DKG_PROGRESS_INTERVAL,
            token: self.timer_token,
        }
    }
}

pub(crate) struct Backlog(VecDeque<(DkgSessionId, DkgMessage)>);

impl Backlog {
    pub(crate) fn new() -> Self {
        Self(VecDeque::with_capacity(BACKLOG_CAPACITY))
    }

    pub(crate) fn push(&mut self, session_id: DkgSessionId, message: DkgMessage) {
        if self.0.len() == self.0.capacity() {
            let _ = self.0.pop_front();
        }

        self.0.push_back((session_id, message))
    }

    pub(crate) fn take(&mut self, session_id: &DkgSessionId) -> Vec<DkgMessage> {
        let mut output = Vec::new();
        let max = self.0.len();

        for _ in 0..max {
            if let Some((message_dkg_key, message)) = self.0.pop_front() {
                if &message_dkg_key == session_id {
                    output.push(message)
                } else {
                    self.0.push_back((message_dkg_key, message))
                }
            }
        }

        output
    }

    pub(crate) fn prune(&mut self, session_id: &DkgSessionId) {
        self.0
            .retain(|(old_dkg_key, _)| old_dkg_key.generation >= session_id.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::MessageType;
    use crate::routing::{
        dkg::voter::DkgVoter, dkg::DkgSessionIdUtils, ed25519,
        node::test_utils::arbitrary_unique_nodes, node::Node,
        section::section_authority_provider::ElderCandidatesUtils, section::test_utils::gen_addr,
        ELDER_SIZE, MIN_ADULT_AGE,
    };
    use assert_matches::assert_matches;
    use eyre::{bail, ContextCompat, Result};
    use proptest::prelude::*;
    use rand::{rngs::SmallRng, SeedableRng};
    use std::{collections::HashMap, iter};
    use xor_name::Prefix;

    #[tokio::test]
    async fn single_participant() -> Result<()> {
        // If there is only one participant, the DKG should complete immediately.

        let voter = DkgVoter::default();
        let section_pk = bls::SecretKey::random().public_key();

        let node = Node::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );
        let elder_candidates = ElderCandidates::new(iter::once(node.peer()), Prefix::default());
        let session_id = DkgSessionId::new(&elder_candidates, 0);

        let commands = voter
            .start(&node, session_id, elder_candidates, section_pk)
            .await?;
        assert_matches!(&commands[..], &[Command::HandleDkgOutcome { .. }]);

        Ok(())
    }

    proptest! {
        // Run a DKG session where every participant handles every message sent to them.
        // Expect the session to successfully complete without timed transitions.
        // NOTE: `seed` is for seeding the rng that randomizes the message order.
        #[test]
        fn proptest_full_participation(nodes in arbitrary_elder_nodes(), seed in any::<u64>()) {
            prop_assert!(proptest_full_participation_impl(nodes, seed).is_ok());
        }
    }

    fn proptest_full_participation_impl(nodes: Vec<Node>, seed: u64) -> Result<()> {
        // Rng used to randomize the message order.
        let mut rng = SmallRng::seed_from_u64(seed);
        let section_pk = bls::SecretKey::random().public_key();
        let mut messages = Vec::new();

        let elder_candidates =
            ElderCandidates::new(nodes.iter().map(Node::peer), Prefix::default());
        let session_id = DkgSessionId::new(&elder_candidates, 0);

        let mut actors: HashMap<_, _> = nodes
            .into_iter()
            .map(|node| (node.addr, Actor::new(node)))
            .collect();

        for actor in actors.values_mut() {
            let commands = futures::executor::block_on(actor.voter.start(
                &actor.node,
                session_id,
                elder_candidates.clone(),
                section_pk,
            ))?;

            for command in commands {
                messages.extend(actor.handle(command, &session_id)?)
            }
        }

        loop {
            match actors
                .values()
                .filter_map(|actor| actor.outcome.as_ref())
                .unique()
                .count()
            {
                0 => {}
                1 => return Ok(()),
                _ => bail!("Inconsistent DKG outcomes"),
            }

            // NOTE: this panics if `messages` is empty, but that's OK because it would mean
            // failure anyway.
            let index = rng.gen_range(0, messages.len());
            let (addr, message) = messages.swap_remove(index);

            let actor = actors.get_mut(&addr).context("Unknown message recipient")?;

            let commands = futures::executor::block_on(actor.voter.process_message(
                &actor.node,
                &session_id,
                message,
                section_pk,
            ))?;

            for command in commands {
                messages.extend(actor.handle(command, &session_id)?)
            }
        }
    }

    struct Actor {
        node: Node,
        voter: DkgVoter,
        outcome: Option<bls::PublicKey>,
    }

    impl Actor {
        fn new(node: Node) -> Self {
            Self {
                node,
                voter: DkgVoter::default(),
                outcome: None,
            }
        }

        fn handle(
            &mut self,
            command: Command,
            expected_dkg_key: &DkgSessionId,
        ) -> Result<Vec<(SocketAddr, DkgMessage)>> {
            match command {
                Command::SendMessage {
                    recipients,
                    wire_msg,
                } => match wire_msg.into_message()? {
                    MessageType::System {
                        msg:
                            SystemMsg::DkgMessage {
                                session_id,
                                message,
                            },
                        ..
                    } => {
                        assert_eq!(session_id, *expected_dkg_key);
                        Ok(recipients
                            .into_iter()
                            .map(|addr| (addr.1, message.clone()))
                            .collect())
                    }
                    other_message => bail!("Unexpected message: {:?}", other_message),
                },
                Command::HandleDkgOutcome { outcome, .. } => {
                    self.outcome = Some(outcome.public_key_set.public_key());
                    Ok(vec![])
                }
                Command::ScheduleTimeout { .. } => Ok(vec![]),
                other_command => {
                    bail!("Unexpected command: {:?}", other_command)
                }
            }
        }
    }

    fn arbitrary_elder_nodes() -> impl Strategy<Value = Vec<Node>> {
        arbitrary_unique_nodes(2..=ELDER_SIZE)
    }
}
