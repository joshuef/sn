// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::DkgSessionInfo,
    dkg::{check_ephemeral_dkg_key, DkgPubKeys},
    flow_ctrl::cmds::Cmd,
    messaging::Peers,
    Error, MyNode, Result,
};

use sn_interface::{
    messaging::{
        system::{DkgSessionId, NodeMsg, SectionSigShare},
        AuthorityProof, MsgId, SectionSig,
    },
    network_knowledge::{SectionAuthorityProvider, SectionKeyShare},
    types::{self, log_markers::LogMarker, Peer},
};

use bls::{PublicKey as BlsPublicKey, PublicKeySet, SecretKeyShare};
use ed25519::Signature;
use sn_sdkg::{DkgSignedVote, VoteResponse};
use std::collections::BTreeSet;
use xor_name::XorName;

/// Helper to get our DKG peers (excluding us)
fn dkg_peers(our_index: usize, session_id: &DkgSessionId) -> BTreeSet<Peer> {
    session_id
        .elder_peers()
        .enumerate()
        .filter_map(|(index, peer)| (index != our_index).then_some(peer))
        .collect()
}

fn acknowledge_dkg_outcome(
    session_id: &DkgSessionId,
    participant_index: usize,
    pub_key_set: PublicKeySet,
    sec_key_share: SecretKeyShare,
) -> Cmd {
    let section_auth = SectionAuthorityProvider::from_dkg_session(session_id, pub_key_set.clone());
    let outcome = SectionKeyShare {
        public_key_set: pub_key_set,
        index: participant_index,
        secret_key_share: sec_key_share,
    };

    Cmd::HandleDkgOutcome {
        section_auth,
        outcome,
    }
}

impl MyNode {
    /// Send a `DkgStart` message to the provided set of candidates
    /// Before a DKG session kicks off, the `DkgStart { ... }` message is individually signed
    /// by the current _set of elders_ and sent to the _new elder candidates_ to be accumulated.
    /// This is to prevent nodes from spamming `DkgStart` messages which might lead to unnecessary
    /// DKG sessions.
    /// Whenever there is a change in Elders in the network Distributed Key Generation
    /// is used to generate a new set of BLS key shares for individual Elders along with the
    /// SectionKey which will represent the section.
    /// DKG is triggered by the following events:
    /// - A change in the Elders
    /// - Section Split
    pub(crate) fn send_dkg_start(&mut self, session_id: DkgSessionId) -> Result<Vec<Cmd>> {
        // Send DKG start to all candidates
        let recipients = Vec::from_iter(session_id.elder_peers());

        trace!(
            "{} s{} {:?} with {:?} to {:?}",
            LogMarker::SendDkgStart,
            session_id.sh(),
            session_id.prefix,
            session_id.elders,
            recipients
        );

        let mut we_are_a_participant = false;
        let mut cmds = vec![];
        let mut others = BTreeSet::new();

        // remove ourself from recipients
        let our_name = self.info().name();
        for recipient in recipients {
            if recipient.name() == our_name {
                we_are_a_participant = true;
            } else {
                let _ = others.insert(recipient);
            }
        }

        // sign the DkgSessionId
        let section_sig_share = self.sign_session_id(&session_id)?;
        let node_msg = NodeMsg::DkgStart(session_id.clone(), section_sig_share.clone());

        // send it to the other participants
        if !others.is_empty() {
            cmds.push(Cmd::send_msg(
                node_msg,
                Peers::Multiple(others),
                self.context(),
            ))
        }

        // handle our own
        if we_are_a_participant {
            cmds.extend(self.handle_dkg_start(session_id, section_sig_share)?);
        }

        Ok(cmds)
    }

    fn sign_session_id(&self, session_id: &DkgSessionId) -> Result<SectionSigShare> {
        // get section key
        let section_key = self.network_knowledge.section_key();
        let key_share = self
            .section_keys_provider
            .key_share(&section_key)
            .map_err(|err| {
                warn!(
                    "Can't obtain key share to sign DkgSessionId s{} {:?}",
                    session_id.sh(),
                    err
                );
                err
            })?;

        // sign
        let serialized_session_id = bincode::serialize(session_id)?;
        Ok(SectionSigShare {
            public_key_set: key_share.public_key_set.clone(),
            index: key_share.index,
            signature_share: key_share.secret_key_share.sign(serialized_session_id),
        })
    }

    fn broadcast_dkg_votes(
        &self,
        session_id: &DkgSessionId,
        pub_keys: DkgPubKeys,
        participant_index: usize,
        votes: Vec<DkgSignedVote>,
    ) -> Cmd {
        let recipients = dkg_peers(participant_index, session_id);
        trace!(
            "{} s{}: {:?}",
            LogMarker::DkgBroadcastVote,
            session_id.sh(),
            votes
        );
        let node_msg = NodeMsg::DkgVotes {
            session_id: session_id.clone(),
            pub_keys,
            votes,
        };
        Cmd::send_msg(node_msg, Peers::Multiple(recipients), self.context())
    }

    fn request_dkg_ae(&self, session_id: &DkgSessionId, sender: Peer) -> Cmd {
        let node_msg = NodeMsg::DkgAE(session_id.clone());
        Cmd::send_msg(node_msg, Peers::Single(sender), self.context())
    }

    fn aggregate_dkg_start(
        &mut self,
        session_id: &DkgSessionId,
        elder_sig: SectionSigShare,
    ) -> Result<Option<SectionSig>> {
        // check sig share
        let public_key = elder_sig.public_key_set.public_key();
        if self.network_knowledge.section_key() != public_key {
            return Err(Error::InvalidKeyShareSectionKey);
        }
        let serialized_session_id = bincode::serialize(session_id)?;

        // try aggregate
        self.dkg_start_aggregator
            .try_aggregate(&serialized_session_id, elder_sig)
            .map_err(|err| {
                warn!(
                    "Error aggregating signature in DkgStart s{}: {err:?}",
                    session_id.sh()
                );
                Error::InvalidSignatureShare
            })
    }

    pub(crate) fn handle_dkg_start(
        &mut self,
        session_id: DkgSessionId,
        elder_sig: SectionSigShare,
    ) -> Result<Vec<Cmd>> {
        // try to create a section sig by aggregating the elder_sig
        match self.aggregate_dkg_start(&session_id, elder_sig) {
            Ok(Some(section_sig)) => {
                trace!(
                    "DkgStart: section key aggregated, starting session s{}",
                    session_id.sh()
                );
                self.dkg_start(session_id, section_sig)
            }
            Ok(None) => {
                trace!(
                    "DkgStart: waiting for more shares for session s{}",
                    session_id.sh()
                );
                Ok(vec![])
            }
            Err(e) => {
                warn!(
                    "DkgStart: failed to aggregate received elder sig in s{}: {e:?}",
                    session_id.sh()
                );
                Ok(vec![])
            }
        }
    }

    pub(crate) fn dkg_start(
        &mut self,
        session_id: DkgSessionId,
        section_sig: SectionSig,
    ) -> Result<Vec<Cmd>> {
        // make sure we are in this dkg session
        let our_name = types::keys::ed25519::name(&self.keypair.public);
        let our_id = if let Some(index) = session_id.elder_index(our_name) {
            index
        } else {
            error!(
                "DKG failed to start s{}: {our_name} is not a participant",
                session_id.sh()
            );
            return Ok(vec![]);
        };

        // ignore DkgStart from old chains
        let current_chain_len = self.network_knowledge.section_chain_len();
        if session_id.section_chain_len < current_chain_len {
            trace!("Skipping DkgStart for older chain: s{}", session_id.sh());
            return Ok(vec![]);
        }

        // acknowledge Dkg session
        let session_info = DkgSessionInfo {
            session_id: session_id.clone(),
            authority: AuthorityProof(section_sig),
        };
        let section_auth = session_info.authority.clone();
        let _existing = self
            .dkg_sessions_info
            .insert(session_id.hash(), session_info);

        // gen key
        let (ephemeral_pub_key, sig) =
            match self
                .dkg_voter
                .gen_ephemeral_key(session_id.hash(), our_name, &self.keypair)
            {
                Ok(k) => k,
                Err(Error::DkgEphemeralKeyAlreadyGenerated) => {
                    trace!(
                        "Skipping already acknowledged DkgStart s{}",
                        session_id.sh()
                    );
                    return Ok(vec![]);
                }
                Err(e) => return Err(e),
            };

        // assert people can check key
        assert!(check_ephemeral_dkg_key(&session_id, our_name, ephemeral_pub_key, sig).is_ok());

        // broadcast signed pub key
        trace!(
            "{} s{} from {our_id:?}",
            LogMarker::DkgBroadcastEphemeralPubKey,
            session_id.sh(),
        );
        let peers = dkg_peers(our_id, &session_id);
        let node_msg = NodeMsg::DkgEphemeralPubKey {
            session_id,
            section_auth,
            pub_key: ephemeral_pub_key,
            sig,
        };

        let cmd = Cmd::send_msg(node_msg, Peers::Multiple(peers), self.context());
        Ok(vec![cmd])
    }

    fn handle_missed_dkg_start(
        &mut self,
        session_id: &DkgSessionId,
        section_auth: AuthorityProof<SectionSig>,
        pub_key: BlsPublicKey,
        sig: Signature,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        trace!(
            "Detected missed dkg start for s{:?} after msg from {sender:?}",
            session_id.sh()
        );

        // check the signature
        let serialized_session_id = bincode::serialize(session_id)?;
        let section_sig = section_auth.clone().into_inner();
        if self.network_knowledge.section_key() != section_sig.public_key {
            warn!(
                "Invalid section key in dkg auth proof in s{:?}: {sender:?}",
                session_id.sh()
            );
            return Ok(vec![]);
        }
        if let Err(err) = AuthorityProof::verify(section_sig.clone(), serialized_session_id) {
            error!(
                "Invalid signature in dkg auth proof in s{:?}: {err:?}",
                session_id.sh()
            );
            return Ok(vec![]);
        }

        // catch back up
        info!(
            "Handling missed dkg start for s{:?} after msg from {sender:?}",
            session_id.sh()
        );
        let mut cmds = vec![];
        cmds.extend(self.dkg_start(session_id.clone(), section_sig)?);
        cmds.extend(self.handle_dkg_ephemeral_pubkey(
            session_id,
            section_auth,
            pub_key,
            sig,
            sender,
        )?);
        Ok(cmds)
    }

    pub(crate) fn handle_dkg_ephemeral_pubkey(
        &mut self,
        session_id: &DkgSessionId,
        section_auth: AuthorityProof<SectionSig>,
        pub_key: BlsPublicKey,
        sig: Signature,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        // make sure we are in this dkg session
        let name = types::keys::ed25519::name(&self.keypair.public);
        let our_id = if let Some(index) = session_id.elder_index(name) {
            index
        } else {
            error!(
                "DKG ephemeral key ignored for s{}: {name} is not a participant",
                session_id.sh()
            );
            return Ok(vec![]);
        };

        // try to start DKG if we've got all the keys
        let outcome =
            match self
                .dkg_voter
                .try_init_dkg(session_id, our_id, pub_key, sig, sender.name())
            {
                Ok(o) => o,
                Err(Error::NoDkgKeysForSession(_)) => {
                    return self.handle_missed_dkg_start(
                        session_id,
                        section_auth,
                        pub_key,
                        sig,
                        sender,
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to init DKG s{} id:{our_id:?}: {:?}",
                        session_id.sh(),
                        e
                    );
                    return Ok(vec![]);
                }
            };
        let (vote, pub_keys) = if let Some(start) = outcome {
            start
        } else {
            // we don't have all the keys yet
            return Ok(vec![]);
        };

        // send first vote
        trace!(
            "{} s{} from id:{our_id:?}",
            LogMarker::DkgBroadcastFirstVote,
            session_id.sh()
        );
        let cmd = self.broadcast_dkg_votes(session_id, pub_keys, our_id, vec![vote]);
        Ok(vec![cmd])
    }

    fn handle_vote_response(
        &mut self,
        session_id: &DkgSessionId,
        pub_keys: DkgPubKeys,
        sender: Peer,
        our_id: usize,
        vote_response: VoteResponse,
    ) -> (Vec<Cmd>, Vec<Cmd>) {
        let mut cmds = vec![];
        let mut ae_cmds = vec![];
        match vote_response {
            VoteResponse::WaitingForMoreVotes => {}
            VoteResponse::RequestAntiEntropy => {
                ae_cmds.push(self.request_dkg_ae(session_id, sender))
            }
            VoteResponse::BroadcastVote(vote) => {
                cmds.push(self.broadcast_dkg_votes(session_id, pub_keys, our_id, vec![*vote]))
            }
            VoteResponse::DkgComplete(new_pubs, new_sec) => {
                trace!(
                    "{} s{:?} {:?}: {} elders: {:?}",
                    LogMarker::DkgComplete,
                    session_id.sh(),
                    session_id.prefix,
                    session_id.elders.len(),
                    new_pubs.public_key(),
                );
                cmds.push(acknowledge_dkg_outcome(
                    session_id, our_id, new_pubs, new_sec,
                ))
            }
        }
        (cmds, ae_cmds)
    }

    pub(crate) fn handle_dkg_votes(
        &mut self,
        session_id: &DkgSessionId,
        msg_keys: DkgPubKeys,
        votes: Vec<DkgSignedVote>,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        // make sure we are in this dkg session
        let name = types::keys::ed25519::name(&self.keypair.public);
        let our_id = if let Some(index) = session_id.elder_index(name) {
            index
        } else {
            error!(
                "DKG failed to handle vote in s{}: {name} is not a participant",
                session_id.sh()
            );
            return Ok(vec![]);
        };

        // make sure the keys are valid
        let (pub_keys, just_completed) = self.dkg_voter.check_keys(session_id, msg_keys)?;

        // if we just completed our keyset thanks to the incoming keys, bcast 1st vote
        let mut cmds = Vec::new();
        if just_completed {
            let (first_vote, _) = self.dkg_voter.initialize_dkg_state(session_id, our_id)?;
            cmds.push(self.broadcast_dkg_votes(
                session_id,
                pub_keys.clone(),
                our_id,
                vec![first_vote],
            ));
        }

        // handle vote
        let mut cmds: Vec<Cmd> = Vec::new();
        let mut ae_cmds: Vec<Cmd> = Vec::new();
        let mut is_old_gossip = true;
        let their_votes_len = votes.len();
        for v in votes {
            let v_string = format!("{v:?}");
            match self.dkg_voter.handle_dkg_vote(session_id, v) {
                Ok(vote_responses) => {
                    debug!(
                        "Dkg s{}: {:?} after: {v_string}",
                        session_id.sh(),
                        vote_responses,
                    );
                    if !vote_responses.is_empty() {
                        self.dkg_voter.learned_something_from_message();
                        is_old_gossip = false;
                    }
                    for r in vote_responses {
                        let (cmd, ae_cmd) = self.handle_vote_response(
                            session_id,
                            pub_keys.clone(),
                            sender,
                            our_id,
                            r,
                        );
                        cmds.extend(cmd);
                        ae_cmds.extend(ae_cmd);
                    }
                }
                Err(err) => {
                    error!(
                        "Error processing DKG vote s{} id:{our_id:?} {v_string} from {sender:?}: {err:?}",
                        session_id.sh()
                    );
                }
            }
        }

        // ae is not necessary if we have votes or termination cmds
        if cmds.is_empty() {
            cmds.append(&mut ae_cmds);
        }

        // if their un-interesting gossip is missing votes, send them ours
        if is_old_gossip && their_votes_len != 1 {
            let mut manual_ae = match self.gossip_missing_votes(session_id, sender, their_votes_len)
            {
                Ok(g) => g,
                Err(err) => {
                    error!(
                        "Error gossiping s{} id:{our_id:?} from {sender:?}: {err:?}",
                        session_id.sh()
                    );
                    vec![]
                }
            };
            cmds.append(&mut manual_ae);
        }

        Ok(cmds)
    }

    /// Gossips all our votes if they have less votes than us
    /// Assumes we know all their votes so the length difference is enough to know that they
    /// are missing votes
    fn gossip_missing_votes(
        &self,
        session_id: &DkgSessionId,
        sender: Peer,
        their_votes_len: usize,
    ) -> Result<Vec<Cmd>> {
        let our_votes = self.dkg_voter.get_all_votes(session_id)?;
        if their_votes_len < our_votes.len() {
            let pub_keys = self.dkg_voter.get_dkg_keys(session_id)?;
            trace!(
                "{} s{}: gossip including missing votes to {sender:?}",
                LogMarker::DkgBroadcastVote,
                session_id.sh()
            );
            let node_msg = NodeMsg::DkgVotes {
                session_id: session_id.clone(),
                pub_keys,
                votes: our_votes,
            };
            let cmd = Cmd::send_msg(node_msg, Peers::Single(sender), self.context());
            Ok(vec![cmd])
        } else {
            Ok(vec![])
        }
    }

    pub(crate) fn handle_dkg_anti_entropy(
        &self,
        session_id: DkgSessionId,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        let pub_keys = self.dkg_voter.get_dkg_keys(&session_id)?;
        let votes = self.dkg_voter.get_all_votes(&session_id)?;
        trace!(
            "{} s{}: AE to {sender:?}",
            LogMarker::DkgBroadcastVote,
            session_id.sh()
        );
        let node_msg = NodeMsg::DkgVotes {
            session_id,
            pub_keys,
            votes,
        };
        let cmd = Cmd::send_msg(node_msg, Peers::Single(sender), self.context());
        Ok(vec![cmd])
    }

    // broadcasts our current known votes
    fn gossip_votes(
        &self,
        session_id: DkgSessionId,
        votes: Vec<DkgSignedVote>,
        our_id: usize,
    ) -> Vec<Cmd> {
        let pub_keys = match self.dkg_voter.get_dkg_keys(&session_id) {
            Ok(k) => k,
            Err(_) => {
                warn!(
                    "Unexpectedly missing dkg keys when gossiping s{}",
                    session_id.sh()
                );
                return vec![];
            }
        };
        trace!(
            "{} s{}: gossiping votes",
            LogMarker::DkgBroadcastVote,
            session_id.sh()
        );
        let cmd = self.broadcast_dkg_votes(&session_id, pub_keys, our_id, votes);
        vec![cmd]
    }

    // broadcasts our ephemeral key
    fn gossip_our_key(
        &self,
        session_id: DkgSessionId,
        our_name: XorName,
        our_id: usize,
    ) -> Vec<Cmd> {
        // get the keys
        let dkg_keys = match self.dkg_voter.get_dkg_keys(&session_id) {
            Ok(k) => k,
            Err(_) => {
                warn!(
                    "Unexpectedly missing dkg pub keys when gossiping s{}",
                    session_id.sh()
                );
                return vec![];
            }
        };
        let (pub_key, sig) = match dkg_keys.get(&our_name) {
            Some(res) => res,
            None => {
                warn!(
                    "Unexpectedly missing our dkg key when gossiping s{}",
                    session_id.sh()
                );
                return vec![];
            }
        };

        // get original auth (as proof for those who missed the original DkgStart msg)
        let section_info = match self.dkg_sessions_info.get(&session_id.hash()) {
            Some(auth) => auth,
            None => {
                warn!(
                    "Unexpectedly missing dkg section info when gossiping s{}",
                    session_id.sh()
                );
                return vec![];
            }
        };
        let section_auth = section_info.authority.clone();

        trace!(
            "{} s{}: gossiping ephemeral key",
            LogMarker::DkgBroadcastVote,
            session_id.sh()
        );

        // broadcast our key
        let peers = dkg_peers(our_id, &session_id);
        let node_msg = NodeMsg::DkgEphemeralPubKey {
            session_id,
            section_auth,
            pub_key: *pub_key,
            sig: *sig,
        };
        let cmd = Cmd::send_msg(node_msg, Peers::Multiple(peers), self.context());
        vec![cmd]
    }

    pub(crate) fn had_sap_change_since(&self, session_id: &DkgSessionId) -> bool {
        self.network_knowledge.section_chain_len() != session_id.section_chain_len
    }

    pub(crate) fn gossip_handover_trigger(&self, session_id: &DkgSessionId) -> Vec<Cmd> {
        match self.dkg_voter.outcome(session_id) {
            Ok(Some((our_id, new_pubs, new_sec))) => {
                trace!(
                    "Gossiping DKG outcome for s{} as we didn't notice SAP change",
                    session_id.sh()
                );
                let cmd = acknowledge_dkg_outcome(session_id, our_id.into(), new_pubs, new_sec);
                vec![cmd]
            }
            Ok(None) => {
                error!(
                    "Missing DKG outcome for s{}, when trying to gossip outcome",
                    session_id.sh()
                );
                vec![]
            }
            Err(e) => {
                error!(
                    "Failed to get DKG outcome for s{}, when trying to gossip outcome: {}",
                    session_id.sh(),
                    e
                );
                vec![]
            }
        }
    }

    /// Force DKG termination for a given session_id
    /// Either fails and logs errors or saves the keyshare in our section_keys_provider
    pub(crate) fn force_dkg_termination(&mut self, session_id: &DkgSessionId) {
        match self.dkg_voter.force_termination(session_id) {
            Ok(Some((our_id, new_pubs, new_sec))) => {
                trace!(
                    "DKG forced termination for s{} successful: generated and saved key_share: {new_pubs:?} with index {our_id:?}",
                    session_id.sh(),
                );
                // Add our new keyshare to our cache, we will then use
                // it to sign any msg that needs section agreement.
                let key_share = SectionKeyShare {
                    public_key_set: new_pubs,
                    index: our_id.into(),
                    secret_key_share: new_sec,
                };
                self.section_keys_provider.insert(key_share);
            }
            Ok(None) => {
                error!(
                    "DKG forced termination for s{} missing outcome",
                    session_id.sh()
                );
            }
            Err(e) => {
                error!(
                    "Failed DKG forced termination for s{}: {e}",
                    session_id.sh()
                );
            }
        }
    }

    /// For all the ongoing DKG sessions, sends out all the current known votes to all DKG
    /// participants if we don't have any votes yet, sends out our ephemeral key
    pub(crate) fn dkg_gossip_msgs(&self) -> Vec<Cmd> {
        let mut cmds = vec![];
        for (_hash, session_info) in self.dkg_sessions_info.iter() {
            // get our id
            let name = types::keys::ed25519::name(&self.keypair.public);
            let our_id = if let Some(index) = session_info.session_id.elder_index(name) {
                index
            } else {
                error!(
                    "DKG failed gossip in s{}: {name} is not a participant",
                    session_info.session_id.sh()
                );
                continue;
            };

            // skip if we already reached termination
            match self.dkg_voter.reached_termination(&session_info.session_id) {
                Ok(true) => {
                    trace!(
                        "Skipping DKG gossip for s{} as we have reached termination",
                        session_info.session_id.sh()
                    );

                    if !self.had_sap_change_since(&session_info.session_id) {
                        cmds.extend(self.gossip_handover_trigger(&session_info.session_id));
                    }

                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    error!(
                        "DKG failed gossip in s{}: {:?}",
                        session_info.session_id.sh(),
                        err
                    );
                }
            }

            // gossip votes else gossip our key
            if let Ok(votes) = self.dkg_voter.get_all_votes(&session_info.session_id) {
                cmds.extend(self.gossip_votes(session_info.session_id.clone(), votes, our_id));
            } else {
                cmds.extend(self.gossip_our_key(session_info.session_id.clone(), name, our_id));
            }
        }
        cmds
    }

    pub(crate) async fn handle_dkg_outcome(
        &mut self,
        sap: SectionAuthorityProvider,
        key_share: SectionKeyShare,
    ) -> Result<Vec<Cmd>> {
        let key_share_pk = key_share.public_key_set.public_key();
        trace!(
            "{} public_key={:?}",
            LogMarker::HandlingDkgSuccessfulOutcome,
            key_share_pk
        );

        // Add our new keyshare to our cache, we will then use
        // it to sign any msg that needs section agreement.
        self.section_keys_provider.insert(key_share.clone());

        let mut cmds = self.update_on_section_change(&self.context()).await?;

        if !self.network_knowledge.has_chain_key(&sap.section_key()) {
            // This request is sent to the current set of elders to be aggregated
            let serialized_sap = bincode::serialize(&sap)?;
            let sig_share = MyNode::sign_with_key_share(serialized_sap, &key_share);
            let msg = NodeMsg::RequestHandover {
                sap: sap.clone(),
                sig_share: sig_share.clone(),
            };
            let current_elders: Vec<_> = self.network_knowledge.section_auth().elders_vec();
            let (other_elders, myself) = self.split_peers_and_self(current_elders);
            let peers = Peers::Multiple(other_elders);
            cmds.push(Cmd::send_msg(msg, peers, self.context()));

            // Handle it if we are an elder
            if let Some(elder) = myself {
                match self.handle_handover_request(MsgId::new(), sap, sig_share, elder) {
                    Ok(c) => cmds.extend(c),
                    Err(e) => error!("Failed to handle our own handover request: {e:?}"),
                };
            }
        }

        Ok(cmds)
    }
}

#[cfg(test)]
mod tests {
    use super::MyNode;
    use crate::node::flow_ctrl::{
        cmds::Cmd,
        dispatcher::Dispatcher,
        tests::{cmd_utils::get_next_msg, network_builder::TestNetworkBuilder},
    };

    use sn_comms::MsgFromPeer;
    use sn_interface::{
        init_logger,
        messaging::{
            signature_aggregator::SignatureAggregator,
            system::{DkgSessionId, NodeMsg, SectionStateVote},
            MsgType, SectionSigShare,
        },
        network_knowledge::{supermajority, NodeState, SectionKeyShare, SectionsDAG},
        test_utils::{TestKeys, TestSectionTree},
        types::Peer,
        SectionAuthorityProvider,
    };

    use assert_matches::assert_matches;
    use bls::SecretKeySet;
    use eyre::{eyre, Result};
    use rand::{Rng, RngCore};
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };
    use tokio::sync::{mpsc, RwLock};
    use xor_name::{Prefix, XorName};

    /// Simulate an entire round of dkg till termination; The dkg round creates a new keyshare set
    /// without any elder change (i.e., the dkg is between the same set of elders).
    #[tokio::test]
    async fn simulate_dkg_round() -> Result<()> {
        init_logger();
        let mut rng = rand::thread_rng();
        let node_count = 7;
        let (mut node_instances, mut comm_receivers, _) = create_elders(node_count, &mut rng).await;

        // let the current set of elders start the dkg round
        let _ = start_dkg(&mut node_instances).await?;

        let mut new_sk_shares = BTreeMap::new();
        // run until all the node buffers are empty
        let mut empty_nodes = BTreeSet::new();
        while empty_nodes.len() != node_count {
            empty_nodes = BTreeSet::new();
            for dispatcher in node_instances.values() {
                let name = dispatcher.node().read().await.name();
                let comm_rx = comm_receivers
                    .get_mut(&name)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;
                info!("\n\n NODE: {name}");

                // used to check if the buffer is empty during the first iteration of the buffer.
                // If all the node buffers are empty during the first try, break out of main loop
                let mut empty_at_first_try = true;
                while let Some(msg) = get_next_msg(comm_rx) {
                    if empty_at_first_try {
                        empty_at_first_try = false;
                    }
                    let cmds = dispatcher.mock_handle_node_msg(msg).await;
                    for cmd in cmds {
                        info!("Got cmd {}", cmd);
                        if let Cmd::SendMsg { .. } = &cmd {
                            dispatcher.mock_send_msg(cmd, None).await;
                        } else if let Cmd::HandleDkgOutcome {
                            section_auth: _,
                            outcome,
                        } = &cmd
                        {
                            let _ = new_sk_shares.insert(name, outcome.clone());
                            let dkg_cmds = dispatcher.process_cmd(cmd).await?;
                            verify_dkg_outcome_cmds(dkg_cmds);
                        } else {
                            panic!("got a different cmd {cmd:?}");
                        }
                    }
                }
                // the msg buffer is empty,
                if empty_at_first_try {
                    let _ = empty_nodes.insert(name);
                }
            }
        }

        // dkg done, make sure the new key_shares are valid
        verify_new_key(&new_sk_shares, node_count);

        Ok(())
    }

    /// If a node already has the new SAP in its `SectionTree`, then it should not propose `RequestHandover`
    #[tokio::test]
    async fn lagging_node_should_not_propose_new_section_info() -> Result<()> {
        init_logger();
        let mut rng = rand::thread_rng();
        let node_count = 7;
        let (mut node_instances, mut comm_receivers, initial_sk_set) =
            create_elders(node_count, &mut rng).await;

        // let current set of elders start the dkg round
        let _ = start_dkg(&mut node_instances).await?;

        let mut new_sk_shares: BTreeMap<XorName, SectionKeyShare> = BTreeMap::new();
        let mut new_sap: BTreeSet<SectionAuthorityProvider> = BTreeSet::new();
        let mut lagging = false;
        // run until all the node buffers are empty
        let mut empty_nodes = BTreeSet::new();
        while empty_nodes.len() != node_count {
            empty_nodes = BTreeSet::new();
            for dispatcher in node_instances.values() {
                let name = dispatcher.node().read().await.name();
                let comm_rx = comm_receivers
                    .get_mut(&name)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;
                info!("\n\n NODE: {name}");

                // used to check if the buffer is empty during the first iteration of the buffer.
                // If all the node buffers are empty during the first try, break out of main loop
                let mut empty_at_first_try = true;
                while let Some(msg) = get_next_msg(comm_rx) {
                    if empty_at_first_try {
                        empty_at_first_try = false;
                    }
                    let cmds = dispatcher.mock_handle_node_msg(msg).await;

                    // If supermajority of the nodes have terminated, then the remaining nodes
                    // can be considered as 'lagging'. So use the supermajority of the shares
                    // to sign the sap and insert them into the lagging nodes, now these nodes
                    // should not trigger `Proposal::RequestHandover`.
                    if !lagging && new_sk_shares.len() >= supermajority(node_count) {
                        let new_sk_set = TestKeys::get_sk_set_from_shares(
                            &new_sk_shares.values().cloned().collect::<Vec<_>>(),
                        )?;
                        let section_tree_update = {
                            assert_eq!(new_sap.len(), 1);
                            let new_sap = new_sap
                                .clone()
                                .into_iter()
                                .next()
                                .ok_or_else(|| eyre!("should contain a sap"))?;
                            let signed_sap = TestKeys::get_section_signed(
                                &new_sk_set.secret_key(),
                                new_sap.clone(),
                            );
                            let proof_chain = {
                                let parent = initial_sk_set.public_keys().public_key();
                                let mut dag = SectionsDAG::new(parent);
                                let sig = TestKeys::sign(
                                    &initial_sk_set.secret_key(),
                                    &new_sap.section_key(),
                                );
                                dag.verify_and_insert(&parent, new_sap.section_key(), sig)?;
                                dag
                            };
                            TestSectionTree::get_section_tree_update(
                                &signed_sap,
                                &proof_chain,
                                &initial_sk_set.secret_key(),
                            )
                        };

                        // find all the lagging nodes; i.e., ones that are yet to handle the dkg_outcome
                        let lagging_nodes = node_instances
                            .keys()
                            .filter(|node| !new_sk_shares.contains_key(node))
                            .cloned()
                            .collect::<Vec<_>>();
                        info!("Lagging node {lagging_nodes:?}");
                        // update them
                        for lag in lagging_nodes {
                            let _updated = node_instances
                                .get(&lag)
                                .ok_or_else(|| eyre!("node will be present"))?
                                .node()
                                .write()
                                .await
                                .network_knowledge
                                .update_knowledge_if_valid(
                                    section_tree_update.clone(),
                                    None,
                                    &name,
                                )?;
                            info!("nw update: {_updated} for {lag} ");
                        }
                        // successfully simulated lagging nodes
                        lagging = true;
                    }

                    for cmd in cmds {
                        info!("Got cmd {}", cmd);
                        if let Cmd::SendMsg { .. } = &cmd {
                            dispatcher.mock_send_msg(cmd, None).await;
                        }
                        // Dkg done, stop the test here
                        else if let Cmd::HandleDkgOutcome {
                            section_auth,
                            outcome,
                        } = &cmd
                        {
                            let _ = new_sk_shares.insert(name, outcome.clone());
                            let _ = new_sap.insert(section_auth.clone());
                            let cmds = dispatcher.process_cmd(cmd).await?;
                            if !lagging {
                                verify_dkg_outcome_cmds(cmds);
                            } else {
                                // Since the dkg session is for the same prefix, the
                                // lagging node should just complete the elder handover
                                // without requesting handover.
                                assert_eq!(cmds.len(), 2);
                                for cmd in cmds {
                                    let msg = assert_matches!(cmd, Cmd::SendMsg { msg, .. } => msg);

                                    match msg {
                                        NodeMsg::ProposeSectionState {
                                            proposal: SectionStateVote::JoinsAllowed(..),
                                            ..
                                        } => (),
                                        NodeMsg::AntiEntropy { .. } => (),
                                        msg => panic!("Unexpected msg {msg}"),
                                    }
                                }
                            }
                        } else {
                            panic!("got a different cmd {cmd:?}");
                        }
                    }
                }
                // the msg buffer is empty,
                if empty_at_first_try {
                    let _ = empty_nodes.insert(name);
                }
            }
        }

        // dkg done, make sure the new key_shares are valid
        verify_new_key(&new_sk_shares, node_count);

        Ok(())
    }

    // The dkg will stall even if a single node is not responsive.
    #[tokio::test]
    async fn total_participation_is_required_for_dkg_votes() -> Result<()> {
        init_logger();
        let mut rng = rand::thread_rng();
        let node_count = 7;
        let (mut node_instances, mut comm_receivers, _) = create_elders(node_count, &mut rng).await;

        // let current set of elders start the dkg round
        let _ = start_dkg(&mut node_instances).await?;

        let dead_node = node_instances
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| eyre!("node_instances is not empty"))?;
        // run until all the node buffers are empty (dead node's buffer is empty after it processes `dkg_start` msgs)
        let mut empty_nodes = BTreeSet::new();
        while empty_nodes.len() != node_count {
            empty_nodes = BTreeSet::new();
            for dispatcher in node_instances.values() {
                let name = dispatcher.node().read().await.name();
                let comm_rx = comm_receivers
                    .get_mut(&name)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;
                info!("\n\n NODE: {name}");

                // used to check if the buffer is empty during the first iteration of the buffer.
                // If all the node buffers are empty during the first try, break out of main loop
                let mut empty_at_first_try = true;
                while let Some(msg) = get_next_msg(comm_rx) {
                    if empty_at_first_try {
                        empty_at_first_try = false;
                    }
                    let cmds = dispatcher.mock_handle_node_msg(msg).await;
                    for cmd in cmds {
                        info!("Got cmd {}", cmd);
                        if let Cmd::SendMsg { .. } = cmd {
                            let filter = BTreeSet::from([dead_node]);
                            dispatcher.mock_send_msg(cmd, Some(filter)).await;
                        } else {
                            panic!("got a different cmd {cmd:?}");
                        }
                    }
                }
                // the msg buffer is empty,
                if empty_at_first_try {
                    let _ = empty_nodes.insert(name);
                }
            }
        }

        // all the msgs are processed and we couldn't reach dkg termination
        Ok(())
    }

    // We randomly drop an outbound `NodeMsg` to a peer, this will effectively stall the dkg since
    // some nodes don't receive certain votes. We solve this by gossiping the votes from a random
    // node until we reach termination.
    #[tokio::test]
    async fn nodes_should_be_brought_up_to_date_using_gossip() -> Result<()> {
        init_logger();
        let mut rng = rand::thread_rng();
        let node_count = 7;
        let (mut node_instances, mut comm_receivers, _) = create_elders(node_count, &mut rng).await;

        // let current set of elders start the dkg round
        let dkg_session_id = start_dkg(&mut node_instances).await?;

        let mut new_sk_shares = BTreeMap::new();
        // run until we get all the sk_shares
        while new_sk_shares.len() != node_count {
            let mut empty_nodes = BTreeSet::new();
            for dispatcher in node_instances.values() {
                let name = dispatcher.node().read().await.name();
                let comm_rx = comm_receivers
                    .get_mut(&name)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;
                info!("\n\n NODE: {name}");

                // used to check if the buffer is empty during the first iteration of the buffer.
                // If all the node buffers are empty during the first try, break out of main loop
                let mut empty_at_first_try = true;
                while let Some(msg) = get_next_msg(comm_rx) {
                    if empty_at_first_try {
                        empty_at_first_try = false;
                    }
                    let cmds = dispatcher.mock_handle_node_msg(msg).await;
                    for cmd in cmds {
                        info!("Got cmd {}", cmd);
                        if let Cmd::SendMsg { .. } = &cmd {
                            // (1/node_count) chance to drop a msg
                            let drop_recp = if rng.gen::<usize>() % node_count == 0 {
                                let recp = cmd.recipients()?;
                                let recp_count = recp.len();
                                let drop = recp
                                    .into_iter()
                                    .map(|p| p.name())
                                    .nth(rng.gen::<usize>() % recp_count)
                                    .ok_or_else(|| eyre!("Contains node_count peers"))?;
                                Some(BTreeSet::from([drop]))
                            } else {
                                None
                            };
                            dispatcher.mock_send_msg(cmd, drop_recp).await;
                        } else if let Cmd::HandleDkgOutcome {
                            section_auth: _,
                            outcome,
                        } = &cmd
                        {
                            // capture the sk_share here as we don't proceed with the SAP update
                            let _ = new_sk_shares.insert(name, outcome.clone());
                            let dkg_cmds = dispatcher.process_cmd(cmd).await?;
                            verify_dkg_outcome_cmds(dkg_cmds);
                        } else {
                            panic!("got a different cmd {cmd:?}");
                        }
                    }
                }
                // the msg buffer is empty,
                if empty_at_first_try {
                    let _ = empty_nodes.insert(name);
                }
            }

            // If the msg buffers are empty and if the current dkg session has not yet
            // terminated, then send a gossip msg from a random node. This
            // allows everyone to catchup.(in the real network each node sends out a
            // gossip if it has not received any valid dkg msg in 30 seconds).
            if empty_nodes.len() == node_count && new_sk_shares.len() != node_count {
                // select a random_node which has not terminated, since terminated node
                // sends out HandleDkgOutcome cmd instead of NodeMsg
                let random_node = loop {
                    let random_node = node_instances
                        .values()
                        .nth(rng.gen::<usize>() % node_count)
                        .ok_or_else(|| eyre!("there should be node_count nodes"))?;
                    if !random_node
                        .node()
                        .read()
                        .await
                        .dkg_voter
                        .reached_termination(&dkg_session_id)?
                    {
                        break random_node;
                    }
                };
                info!(
                    "Sending gossip from random node {:?}",
                    random_node.node().read().await.name()
                );
                let cmds = random_node.node().read().await.dkg_gossip_msgs();
                for cmd in cmds {
                    info!("Got cmd {}", cmd);
                    assert_matches!(&cmd, Cmd::SendMsg { .. });
                    random_node.mock_send_msg(cmd, None).await;
                }
            }
        }

        // dkg done, make sure the new key_shares are valid
        verify_new_key(&new_sk_shares, node_count);

        Ok(())
    }

    // Test helpers

    /// Generate a set of `MyNode` instances
    async fn create_elders(
        elder_count: usize,
        rng: impl RngCore,
    ) -> (
        BTreeMap<XorName, Dispatcher>,
        BTreeMap<XorName, mpsc::Receiver<MsgFromPeer>>,
        SecretKeySet,
    ) {
        let mut env = TestNetworkBuilder::new(rng)
            .sap(Prefix::default(), elder_count, 0, None, None)
            .build();
        let sk_set = env.get_secret_key_set(Prefix::default(), None);
        let node_instances = env
            .get_nodes(Prefix::default(), elder_count, 0, None)
            .into_iter()
            .map(|node| {
                let name = node.name();
                let (dispatcher, _) = Dispatcher::new(Arc::new(RwLock::new(node)));
                (name, dispatcher)
            })
            .collect::<BTreeMap<XorName, Dispatcher>>();
        let mut comm_receivers = BTreeMap::new();
        for (name, node) in node_instances.iter() {
            let pk = node.node().read().await.info().public_key();
            let comm = env.take_comm_rx(pk);
            let _ = comm_receivers.insert(*name, comm);
        }
        (node_instances, comm_receivers, sk_set)
    }

    // Each node sends out DKG start msg to the other nodes
    async fn start_dkg(nodes: &mut BTreeMap<XorName, Dispatcher>) -> Result<DkgSessionId> {
        let mut elders = BTreeMap::new();
        for (name, node) in nodes.iter() {
            let _ = elders.insert(*name, node.node().read().await.addr);
        }
        let bootstrap_members = elders
            .iter()
            .map(|(name, addr)| {
                let peer = Peer::new(*name, *addr);
                NodeState::joined(peer, None)
            })
            .collect::<BTreeSet<_>>();
        // A DKG session which just creates a new key for the same set of elders
        let session_id = DkgSessionId {
            prefix: Prefix::default(),
            elders,
            section_chain_len: 1,
            bootstrap_members,
            membership_gen: 0,
        };
        for node in nodes.values() {
            let mut cmd = node
                .node()
                .write()
                .await
                .send_dkg_start(session_id.clone())?;
            assert_eq!(cmd.len(), 1);
            let cmd = cmd.remove(0);
            matches!(cmd, Cmd::SendMsg { .. });
            node.mock_send_msg(cmd, None).await;
        }
        Ok(session_id)
    }

    // Verify that the newly generated key is valid. Aggregate the signature shares instead of
    // using `TestKeys::get_sk_set_from_shares`.
    fn verify_new_key(new_sk_shares: &BTreeMap<XorName, SectionKeyShare>, node_count: usize) {
        let mut pub_key_set = BTreeSet::new();
        let mut sig_shares = Vec::new();
        for key_share in new_sk_shares.values() {
            let pk = key_share.public_key_set.public_key();
            let _ = pub_key_set.insert(pk);

            let sig_share = SectionSigShare::new(
                key_share.public_key_set.clone(),
                key_share.index,
                &key_share.secret_key_share,
                "msg".as_bytes(),
            );
            sig_shares.push(sig_share);
        }
        assert_eq!(pub_key_set.len(), 1);
        let mut agg = SignatureAggregator::default();
        let mut sig_count = 1;
        for sig_share in sig_shares {
            // threshold = 4 i.e, we need 5 shares to gen the complete sig; Thus the first 4 return None, and 5th one
            // gives us the complete sig;
            if sig_count < supermajority(node_count) || sig_count > supermajority(node_count) {
                assert!(agg
                    .try_aggregate("msg".as_bytes(), sig_share)
                    .expect("Failed to aggregate sigs")
                    .is_none());
            } else if sig_count == supermajority(node_count) {
                let sig = agg
                    .try_aggregate("msg".as_bytes(), sig_share)
                    .expect("Failed to aggregate sigs")
                    .expect("Should return the SectionSig");
                assert!(sig.verify("msg".as_bytes()), "Failed to verify SectionSig");
            }
            sig_count += 1;
        }
        info!("the generated key is valid!");
    }

    fn verify_dkg_outcome_cmds(cmds: Vec<Cmd>) {
        assert_eq!(cmds.len(), 2);
        for cmd in cmds {
            let msg = assert_matches!(cmd, Cmd::SendMsg { msg, .. } => msg);

            match msg {
                NodeMsg::RequestHandover { .. } => (),
                NodeMsg::AntiEntropy { .. } => (),
                msg => panic!("Unexpected msg {msg}"),
            }
        }
    }

    impl Dispatcher {
        // the actual handler has an AE check. Skipping it here
        async fn mock_handle_node_msg(&self, msg: MsgFromPeer) -> Vec<Cmd> {
            let context = self.node().read().await.context();
            let origin = msg.sender;
            let (msg_id, msg) = assert_matches!(
                msg.wire_msg.into_msg().expect("Failed to deserialize wire_msg"),
                MsgType::Node { msg_id, msg, .. } => (msg_id, msg)
            );
            MyNode::handle_node_msg(self.node(), context, msg_id, msg, origin, None)
                .await
                .expect("Error while handling node msg")
        }
    }
}
