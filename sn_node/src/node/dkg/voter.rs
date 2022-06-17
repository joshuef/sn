// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, dkg::session::Session, messages::WireMsgUtils, Result};

use sn_interface::{
    messaging::{
        system::{DkgFailureSig, DkgFailureSigSet, DkgSessionId, SystemMsg},
        DstLocation, WireMsg,
    },
    network_knowledge::{supermajority, NodeInfo, SectionAuthorityProvider, SectionKeyShare},
    types::{
        keys::ed25519::{self, Digest256},
        Peer,
    },
};

use bls::PublicKey as BlsPublicKey;
use bls_dkg::key_gen::{message::Message as DkgMessage, KeyGen};
use dashmap::DashMap;
use std::{collections::BTreeSet, sync::Arc};
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
    sessions: Arc<DashMap<Digest256, Session>>,
}

impl Default for DkgVoter {
    fn default() -> Self {
        Self {
            sessions: Arc::new(DashMap::default()),
        }
    }
}

impl DkgVoter {
    // Starts a new DKG session.
    pub(crate) fn start(
        &self,
        node: &NodeInfo,
        session_id: DkgSessionId,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        if self.sessions.contains_key(&session_id.hash()) {
            trace!("DKG already in progress for {session_id:?}");
            return Ok(vec![]);
        }

        let name = ed25519::name(&node.keypair.public);
        let participant_index = if let Some(index) = session_id.elder_index(name) {
            index
        } else {
            error!("DKG failed to start for {session_id:?}: {name} is not a participant");
            return Ok(vec![]);
        };

        // Special case: only one participant.
        if session_id.elders.len() == 1 {
            let secret_key_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let section_auth = SectionAuthorityProvider::from_dkg_session(
                session_id,
                secret_key_set.public_keys(),
            );
            return Ok(vec![Cmd::HandleDkgOutcome {
                section_auth,
                outcome: SectionKeyShare {
                    public_key_set: secret_key_set.public_keys(),
                    index: participant_index,
                    secret_key_share: secret_key_set.secret_key_share(0u64),
                },
                generation: 0,
            }]);
        }

        let threshold = supermajority(session_id.elders.len()) - 1;
        let participants = session_id.elder_names().collect();

        match KeyGen::initialize(name, threshold, participants) {
            Ok((key_gen, messages)) => {
                trace!("DKG starting for {session_id:?}");

                let mut session = Session {
                    key_gen,
                    session_id: session_id.clone(),
                    participant_index,
                    timer_token: 0,
                    failures: DkgFailureSigSet::from(session_id.clone()),
                    complete: false,
                    last_message_broadcast: vec![],
                    retries: 0,
                };

                let mut cmds = vec![];
                cmds.extend(session.broadcast(node, messages, section_pk)?);

                // This is to avoid the case that between the above existence check
                // and the insertion, there is another thread created and updated the session.
                if self.sessions.contains_key(&session_id.hash()) {
                    warn!("DKG already in progress for {:?}", session_id);
                    return Ok(vec![]);
                } else {
                    let _prev = self.sessions.insert(session_id.hash(), session);
                }

                // Remove unneeded old sessions.
                self.sessions.retain(|_, existing_session| {
                    existing_session.session_id.section_chain_len >= session_id.section_chain_len
                });

                Ok(cmds)
            }
            Err(error) => {
                // TODO: return a separate error here.
                error!("DKG failed to start for {session_id:?}: {error}");
                Ok(vec![])
            }
        }
    }

    // Make key generator progress with timed phase.
    pub(crate) fn handle_timeout(
        &self,
        node: &NodeInfo,
        timer_token: u64,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        if let Some(mut ref_mut_multi) = self.sessions.iter_mut().find(|ref_mut_multi| {
            let session = ref_mut_multi.value();
            session.timer_token() == timer_token
        }) {
            let (_, session) = ref_mut_multi.pair_mut();
            session.handle_timeout(node, section_pk)
        } else {
            Ok(vec![])
        }
    }

    // Handle a received DkgMessage.
    pub(crate) fn process_msg(
        &self,
        sender: Peer,
        node: &NodeInfo,
        session_id: &DkgSessionId,
        message: DkgMessage,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = Vec::new();

        if let Some(mut session) = self.sessions.get_mut(&session_id.hash()) {
            cmds.extend(session.process_msg(node, sender.name(), message, section_pk)?)
        } else {
            trace!(
                "Sending DkgSessionUnknown {{ {:?} }} to {}",
                &session_id,
                &sender
            );

            let node_msg = SystemMsg::DkgSessionUnknown {
                session_id: session_id.clone(),
                message,
            };
            let wire_msg = WireMsg::single_src(
                node,
                DstLocation::Node {
                    name: sender.name(),
                    section_pk,
                },
                node_msg,
                section_pk,
            )?;

            cmds.push(Cmd::SendMsg {
                recipients: vec![sender],
                wire_msg,
            });
        }
        Ok(cmds)
    }

    pub(crate) fn process_failure(
        &self,
        session_id: &DkgSessionId,
        failed_participants: &BTreeSet<XorName>,
        signed: DkgFailureSig,
    ) -> Option<Cmd> {
        let hash = session_id.hash();
        self.sessions
            .get_mut(&hash)?
            .process_failure(session_id, failed_participants, signed)
    }

    pub(crate) fn get_cached_msgs(&self, session_id: &DkgSessionId) -> Vec<DkgMessage> {
        if let Some(session) = self.sessions.get_mut(&session_id.hash()) {
            session.get_cached_msgs()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn handle_dkg_history(
        &self,
        node: &NodeInfo,
        session_id: &DkgSessionId,
        message_history: Vec<DkgMessage>,
        sender: XorName,
        section_pk: BlsPublicKey,
    ) -> Result<Vec<Cmd>> {
        if let Some(mut session) = self.sessions.get_mut(&session_id.hash()) {
            session.handle_dkg_history(node, message_history, section_pk)
        } else {
            warn!(
                "Recieved DKG message cache from {} without an active DKG session: {:?}",
                &sender, &session_id,
            );
            Ok(vec![])
        }
    }
}
