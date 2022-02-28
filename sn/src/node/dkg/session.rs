// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::{next_timer_token, Cmd},
    dkg::dkg_msgs_utils::{DkgFailureSigSetUtils, DkgFailureSigUtils},
    ed25519,
    messages::WireMsgUtils,
    NodeInfo, Result,
};
use sn_interface::messaging::{
    system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, SystemMsg},
    DstLocation, WireMsg,
};
use sn_interface::network_knowledge::{ElderCandidates, SectionAuthorityProvider, SectionKeyShare};
use sn_interface::types::{log_markers::LogMarker, Peer, PublicKey};

use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::{
    message::Message as DkgMessage, Error as DkgError, KeyGen, MessageAndTarget, Phase,
};
use itertools::Itertools;
use std::{
    collections::{BTreeMap, BTreeSet},
    iter, mem,
    time::Duration,
};
use xor_name::XorName;

// Interval to progress DKG timed phase
const DKG_PROGRESS_INTERVAL: Duration = Duration::from_secs(6);

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

fn is_dkg_behind(expected: Phase, actual: Phase) -> bool {
    if let (Phase::Contribution, Phase::Initialization) = (expected, actual) {
        true
    } else {
        trace!("Our DKG session is ahead. Skipping DkgAE");
        false
    }
}

impl Session {
    pub(crate) fn timer_token(&self) -> u64 {
        self.timer_token
    }

    fn send_dkg_not_ready(
        &mut self,
        node: &NodeInfo,
        message: DkgMessage,
        session_id: &DkgSessionId,
        sender: XorName,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        // When the message in trouble is an Acknowledgement,
        // we shall query the ack.proposer .
        let target = match message {
            DkgMessage::Acknowledgment { ref ack, .. } => {
                if let Some(name) = self.key_gen.node_id_from_index(ack.0) {
                    name
                } else {
                    warn!("Cannot get node_id for index {:?}", ack.0);
                    return Ok(vec![]);
                }
            }
            _ => sender,
        };

        if let Some(peer) = self.peers().get(&target) {
            trace!(
                "Targeting DkgNotReady to {:?} on unhandable message {:?}",
                target,
                message
            );
            let node_msg = SystemMsg::DkgNotReady {
                session_id: *session_id,
                message,
            };
            let wire_msg = WireMsg::single_src(
                node,
                DstLocation::Node {
                    name: target,
                    section_pk,
                },
                node_msg,
                section_pk,
            )?;

            cmds.push(Cmd::SendMsg {
                recipients: vec![peer.clone()],
                wire_msg,
            });
        } else {
            warn!(
                "Failed to fetch peer of {:?} among {:?}",
                target, self.elder_candidates
            );
        }
        Ok(cmds)
    }

    pub(crate) fn process_msg(
        &mut self,
        node: &NodeInfo,
        sender: XorName,
        session_id: &DkgSessionId,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        trace!("process DKG message {:?}", message);
        let mut cmds = vec![];
        match self
            .key_gen
            .handle_message(&mut rand::thread_rng(), message.clone())
        {
            Ok(responses) => {
                // Only a valid DkgMessage, which results in some responses, shall reset the ticker.
                let add_reset_timer = !responses.is_empty();

                cmds.extend(self.broadcast(node, session_id, responses, section_pk)?);

                if add_reset_timer {
                    cmds.push(self.reset_timer());
                }
                cmds.extend(self.check(node, session_id, section_pk)?);
            }
            Err(DkgError::UnexpectedPhase { expected, actual })
                if is_dkg_behind(expected, actual) =>
            {
                cmds.extend(
                    self.send_dkg_not_ready(node, message, session_id, sender, section_pk)?,
                );
            }
            Err(DkgError::MissingPart) => {
                cmds.extend(
                    self.send_dkg_not_ready(node, message, session_id, sender, section_pk)?,
                );
            }
            Err(error) => {
                error!("Error processing DKG message: {:?}", error);
            }
        }

        Ok(cmds)
    }

    fn recipients(&self) -> Vec<Peer> {
        self.elder_candidates
            .elders()
            .enumerate()
            .filter_map(|(index, peer)| (index != self.participant_index).then(|| peer.clone()))
            .collect()
    }

    fn peers(&self) -> BTreeMap<XorName, Peer> {
        self.elder_candidates
            .elders()
            .map(|peer| (peer.name(), peer.clone()))
            .collect()
    }

    pub(crate) fn broadcast(
        &mut self,
        node: &NodeInfo,
        session_id: &DkgSessionId,
        messages: Vec<MessageAndTarget>,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];

        trace!(
            "{} to {:?} targets",
            LogMarker::DkgBroadcastMsg,
            messages.len()
        );

        let peers = self.peers();
        for (target, message) in messages {
            if target == node.name() {
                match self.process_msg(node, node.name(), session_id, message.clone(), section_pk) {
                    Ok(result) => cmds.extend(result),
                    Err(err) => error!(
                        "Within session {:?}, failed to process DkgMessage {:?} with error {:?}",
                        session_id, message, err
                    ),
                }
            } else if let Some(peer) = peers.get(&target) {
                trace!(
                    "DKG sending {:?} - {:?} to {:?}",
                    message,
                    session_id,
                    target
                );
                let node_msg = SystemMsg::DkgMessage {
                    session_id: *session_id,
                    message: message.clone(),
                };
                let wire_msg = WireMsg::single_src(
                    node,
                    DstLocation::Node {
                        name: target,
                        section_pk,
                    },
                    node_msg,
                    section_pk,
                )?;

                trace!(
                    "DKG sending {:?} with msg_id {:?}",
                    message,
                    wire_msg.msg_id()
                );

                cmds.push(Cmd::SendMsg {
                    recipients: vec![peer.clone()],
                    wire_msg,
                });
            } else {
                error!("Failed to find target {:?} among peers {:?}", target, peers);
            }
        }

        Ok(cmds)
    }

    pub(crate) fn handle_timeout(
        &mut self,
        node: &NodeInfo,
        session_id: &DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        if self.complete {
            return Ok(vec![]);
        }

        trace!("DKG progressing for {:?}", self.elder_candidates);

        match self.key_gen.timed_phase_transition(&mut rand::thread_rng()) {
            Ok(messages) => {
                let mut cmds = vec![];
                cmds.extend(self.broadcast(node, session_id, messages, section_pk)?);
                cmds.push(self.reset_timer());
                cmds.extend(self.check(node, session_id, section_pk)?);
                Ok(cmds)
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
        node: &NodeInfo,
        session_id: &DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        if self.complete {
            trace!("{} {:?}", LogMarker::DkgSessionAlreadyCompleted, session_id);
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
        if !participants
            .iter()
            .copied()
            .eq(self.elder_candidates.names())
        {
            trace!(
                "DKG failed due to unexpected participants for {:?}: {:?}",
                self.elder_candidates,
                participants.iter().format(", ")
            );

            let failed_participants: BTreeSet<_> = self
                .elder_candidates
                .names()
                .filter(|elder| !participants.contains(elder))
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
            "{} {:?}: {:?}",
            LogMarker::DkgSessionComplete,
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

        Ok(vec![Cmd::HandleDkgOutcome {
            section_auth,
            outcome,
        }])
    }

    fn report_failure(
        &mut self,
        node: &NodeInfo,
        session_id: &DkgSessionId,
        failed_participants: BTreeSet<XorName>,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
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
                Cmd::SendMsg {
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
    ) -> Option<Cmd> {
        if !self
            .elder_candidates
            .contains(&ed25519::name(&signed.public_key))
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

    pub(crate) fn get_cached_msgs(&self) -> Vec<DkgMessage> {
        self.key_gen.get_cached_message()
    }

    pub(crate) fn handle_dkg_history(
        &mut self,
        node: &NodeInfo,
        session_id: DkgSessionId,
        msg_history: Vec<DkgMessage>,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let (responses, unhandleable) = self
            .key_gen
            .handle_pre_session_messages(&mut rand::thread_rng(), msg_history);
        let add_reset_timer = !responses.is_empty();

        cmds.extend(self.broadcast(node, &session_id, responses, section_pk)?);

        if add_reset_timer {
            cmds.push(self.reset_timer());
        }
        cmds.extend(self.check(node, &session_id, section_pk)?);

        if !unhandleable.is_empty() {
            trace!(
                "Having unhandleables among the msg_history. {:?}",
                unhandleable
            );
        }

        Ok(cmds)
    }

    fn check_failure_agreement(&mut self) -> Option<Cmd> {
        if self.failures.has_agreement(&self.elder_candidates) {
            self.complete = true;

            Some(Cmd::HandleDkgFailure(mem::take(&mut self.failures)))
        } else {
            None
        }
    }

    fn reset_timer(&mut self) -> Cmd {
        self.timer_token = next_timer_token();
        Cmd::ScheduleTimeout {
            duration: DKG_PROGRESS_INTERVAL,
            token: self.timer_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::elder_count;
    use crate::node::{
        dkg::voter::DkgVoter, dkg::DkgSessionIdUtils, ed25519,
        sn_interface::network_knowledge::test_utils::gen_addr, NodeInfo, MIN_ADULT_AGE,
    };
    use sn_interface::messaging::MsgType;

    use assert_matches::assert_matches;
    use eyre::{bail, ContextCompat, Result};
    use itertools::Itertools;
    use proptest::{collection::SizeRange, prelude::*};
    use rand::{rngs::SmallRng, SeedableRng};
    use std::{collections::HashMap, iter, net::SocketAddr};
    use xor_name::Prefix;

    #[tokio::test]
    async fn single_participant() -> Result<()> {
        // If there is only one participant, the DKG should complete immediately.

        let voter = DkgVoter::default();
        let section_pk = bls::SecretKey::random().public_key();

        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );
        let elder_candidates = ElderCandidates::new(Prefix::default(), iter::once(node.peer()));
        let session_id = DkgSessionId::new(&elder_candidates, 0);

        let cmds = voter
            .start(&node, session_id, elder_candidates, section_pk)
            .await?;
        assert_matches!(&cmds[..], &[Cmd::HandleDkgOutcome { .. }]);

        Ok(())
    }

    proptest! {
        // Run a DKG session where every participant handles every message sent to them.
        // Expect the session to successfully complete without timed transitions.
        // NOTE: `seed` is for seeding the rng that randomizes the message order.
        #[test]
        fn proptest_full_participation(nodes in arbitrary_elder_nodes(), seed in any::<u64>()) {
            if let Err(error) = proptest_full_participation_impl(nodes, seed) {
                panic!("{}", error);
            }
        }
    }

    fn proptest_full_participation_impl(nodes: Vec<NodeInfo>, seed: u64) -> Result<()> {
        // Rng used to randomize the message order.
        let mut rng = SmallRng::seed_from_u64(seed);
        let section_pk = bls::SecretKey::random().public_key();
        let mut messages = Vec::new();

        let elder_candidates =
            ElderCandidates::new(Prefix::default(), nodes.iter().map(NodeInfo::peer));
        let session_id = DkgSessionId::new(&elder_candidates, 0);

        let mut actors: HashMap<_, _> = nodes
            .into_iter()
            .map(|node| (node.addr, Actor::new(node)))
            .collect();

        for actor in actors.values_mut() {
            let cmds = futures::executor::block_on(actor.voter.start(
                &actor.node,
                session_id,
                elder_candidates.clone(),
                section_pk,
            ))?;

            for cmd in cmds {
                messages.extend(actor.handle(cmd, &session_id)?)
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

            let cmds = futures::executor::block_on(actor.voter.process_msg(
                actor.peer(),
                &actor.node,
                &session_id,
                message,
                section_pk,
            ))?;

            for cmd in cmds {
                messages.extend(actor.handle(cmd, &session_id)?)
            }
        }
    }

    struct Actor {
        node: NodeInfo,
        voter: DkgVoter,
        outcome: Option<bls::PublicKey>,
    }

    impl Actor {
        fn new(node: NodeInfo) -> Self {
            Self {
                node,
                voter: DkgVoter::default(),
                outcome: None,
            }
        }

        fn peer(&self) -> Peer {
            self.node.peer()
        }

        fn handle(
            &mut self,
            cmd: Cmd,
            expected_dkg_key: &DkgSessionId,
        ) -> Result<Vec<(SocketAddr, DkgMessage)>> {
            match cmd {
                Cmd::SendMsg {
                    recipients,
                    wire_msg,
                } => match wire_msg.into_msg()? {
                    MsgType::System {
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
                            .map(|peer| (peer.addr(), message.clone()))
                            .collect())
                    }
                    MsgType::System {
                        msg: SystemMsg::DkgNotReady { message, .. },
                        ..
                    } => Ok(vec![(self.node.addr, message)]),
                    other_msg => bail!("Unexpected msg: {:?}", other_msg),
                },
                Cmd::HandleDkgOutcome { outcome, .. } => {
                    self.outcome = Some(outcome.public_key_set.public_key());
                    Ok(vec![])
                }
                Cmd::ScheduleTimeout { .. } => Ok(vec![]),
                other_cmd => {
                    bail!("Unexpected cmd: {:?}", other_cmd)
                }
            }
        }
    }

    fn arbitrary_elder_nodes() -> impl Strategy<Value = Vec<NodeInfo>> {
        arbitrary_unique_nodes(2..=elder_count())
    }

    // Generate Vec<Node> where no two nodes have the same name.
    pub(crate) fn arbitrary_unique_nodes(
        count: impl Into<SizeRange>,
    ) -> impl Strategy<Value = Vec<NodeInfo>> {
        proptest::collection::vec(arbitrary_node(), count).prop_filter("non-unique keys", |nodes| {
            nodes
                .iter()
                .unique_by(|node| node.keypair.secret.as_bytes())
                .unique_by(|node| node.addr)
                .count()
                == nodes.len()
        })
    }

    fn arbitrary_node() -> impl Strategy<Value = NodeInfo> {
        (
            ed25519::test_utils::arbitrary_keypair(),
            any::<SocketAddr>(),
        )
            .prop_map(|(keypair, addr)| NodeInfo::new(keypair, addr))
    }
}
