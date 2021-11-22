// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId};
use crate::routing::{
    dkg::session::{Backlog, Session},
    ed25519,
    error::Result,
    network_knowledge::{ElderCandidates, SectionAuthorityProvider, SectionKeyShare},
    node::Node,
    routing_api::command::Command,
    supermajority,
};
use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::{message::Message as DkgMessage, KeyGen};
use dashmap::DashMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use xor_name::XorName;

/// DKG voter carries out the work of participating and/or observing a DKG.
///
/// # Usage
///
/// 1. First the current elders propose the new elder candidates in the form of
///    `SectionAuthorityProvider`structure.
/// 2. They send an accumulating message `DkgStart` containing this proposed
///    `SectionAuthorityProvider` to the new elders candidates (DKG participants).
/// 3. When the `DkgStart` message accumulates, the participants call `start`.
/// 4. The participants keep exchanging the DKG messages and calling `process_message`.
/// 5. On DKG completion, the participants send `DkgResult` vote to the current elders (observers)
/// 6. When the observers accumulate the votes, they can proceed with voting for the section update.
///
/// Note: in case of heavy churn, it can happen that more than one DKG session completes
/// successfully. Some kind of disambiguation strategy needs to be employed in that case, but that
/// is currently not a responsibility of this module.
#[derive(Clone)]
pub(crate) struct DkgVoter {
    sessions: Arc<DashMap<DkgSessionId, Session>>,

    // Due to the asyncronous nature of the network we might sometimes receive a DKG message before
    // we created the corresponding session. To avoid losing those messages, we store them in this
    // backlog and replay them once we create the session.
    backlog: Arc<RwLock<Backlog>>,
}

impl Default for DkgVoter {
    fn default() -> Self {
        Self {
            sessions: Arc::new(DashMap::default()),
            backlog: Arc::new(RwLock::new(Backlog::new())),
        }
    }
}

impl DkgVoter {
    // Starts a new DKG session.
    pub(crate) async fn start(
        &self,
        node: &Node,
        session_id: DkgSessionId,
        elder_candidates: ElderCandidates,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if self.sessions.contains_key(&session_id) {
            trace!("DKG already in progress for {:?}", elder_candidates);
            return Ok(vec![]);
        }

        let name = ed25519::name(&node.keypair.public);
        let participant_index =
            if let Some(index) = elder_candidates.names().position(|n| n == name) {
                index
            } else {
                error!(
                    "DKG failed to start for {:?}: {} is not a participant",
                    elder_candidates, name
                );
                return Ok(vec![]);
            };

        // Special case: only one participant.
        if elder_candidates.len() == 1 {
            let secret_key_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let section_auth = SectionAuthorityProvider::from_elder_candidates(
                elder_candidates,
                secret_key_set.public_keys(),
            );
            return Ok(vec![Command::HandleDkgOutcome {
                section_auth,
                outcome: SectionKeyShare {
                    public_key_set: secret_key_set.public_keys(),
                    index: participant_index,
                    secret_key_share: secret_key_set.secret_key_share(0),
                },
            }]);
        }

        let threshold = supermajority(elder_candidates.len()) - 1;
        let participants = elder_candidates.names().collect();

        match KeyGen::initialize(name, threshold, participants) {
            Ok((key_gen, message)) => {
                trace!("DKG starting for {:?}", elder_candidates);

                let mut session = Session {
                    key_gen,
                    elder_candidates,
                    participant_index,
                    timer_token: 0,
                    failures: DkgFailureSigSet::default(),
                    complete: false,
                };

                let mut commands = vec![];
                commands.extend(session.broadcast(node, &session_id, message, section_pk)?);

                trace!("Start draining backlog");

                for message in self.backlog.write().await.take(&session_id).into_iter() {
                    commands.extend(session.process_message(
                        node,
                        name,
                        &session_id,
                        message,
                        section_pk,
                    )?);
                }

                trace!("Draining backlog completed");

                let _prev = self.sessions.insert(session_id, session);

                // Remove unneeded old sessions.
                self.sessions.retain(|existing_session_id, _| {
                    existing_session_id.generation >= session_id.generation
                });
                self.backlog.write().await.prune(&session_id);

                Ok(commands)
            }
            Err(error) => {
                // TODO: return a separate error here.
                error!("DKG failed to start for {:?}: {}", elder_candidates, error);
                Ok(vec![])
            }
        }
    }

    // Make key generator progress with timed phase.
    pub(crate) fn handle_timeout(
        &self,
        node: &Node,
        timer_token: u64,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if let Some(mut ref_mut_multi) = self.sessions.iter_mut().find(|ref_mut_multi| {
            let session = ref_mut_multi.value();
            session.timer_token() == timer_token
        }) {
            let (session_id, session) = ref_mut_multi.pair_mut();
            session.handle_timeout(node, session_id, section_pk)
        } else {
            Ok(vec![])
        }
    }

    // Handle a received DkgMessage.
    pub(crate) async fn process_message(
        &self,
        sender: XorName,
        node: &Node,
        session_id: &DkgSessionId,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        let mut commands = Vec::new();

        if let Some(mut session) = self.sessions.get_mut(session_id) {
            // There is chance that when there is too many messages in the backlog,
            // during the handling of them there will be new message reached,
            // which be pushed to the backlog.
            trace!("Draining backlog before process message");
            for message in self.backlog.write().await.take(session_id).into_iter() {
                commands.extend(
                    session.process_message(node, sender, session_id, message, section_pk)?,
                );
            }

            commands.extend(session.process_message(node, sender, session_id, message, section_pk)?)
        } else {
            trace!("Pushing to backlog {:?} - {:?}", session_id, message);
            self.backlog.write().await.push(*session_id, message);
        }
        Ok(commands)
    }

    pub(crate) fn process_failure(
        &self,
        session_id: &DkgSessionId,
        failed_participants: &BTreeSet<XorName>,
        signed: DkgFailureSig,
    ) -> Option<Command> {
        self.sessions
            .get_mut(session_id)?
            .process_failure(session_id, failed_participants, signed)
    }

    pub(crate) fn get_cached_messages(&self, session_id: &DkgSessionId) -> Vec<DkgMessage> {
        if let Some(session) = self.sessions.get_mut(session_id) {
            session.get_cached_messages()
        } else {
            Vec::new()
        }
    }

    pub(crate) async fn handle_dkg_history(
        &self,
        node: &Node,
        session_id: DkgSessionId,
        message_history: Vec<DkgMessage>,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Command>> {
        if let Some(mut session) = self.sessions.get_mut(&session_id) {
            session.handle_dkg_history(node, session_id, message_history, section_pk)
        } else {
            for message in message_history {
                trace!("Pushing to backlog {:?} - {:?}", session_id, message);
                self.backlog.write().await.push(session_id, message);
            }
            Ok(vec![])
        }
    }
}
