// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Error, Result};
use ed25519::Signature;
use ed25519_dalek::{Keypair, Verifier};

use sn_interface::{
    messaging::system::DkgSessionId,
    network_knowledge::threshold,
    types::{
        self,
        keys::ed25519::{pub_key, Digest256},
    },
};

use bls::{PublicKey as BlsPublicKey, PublicKeySet, SecretKey as BlsSecretKey, SecretKeyShare};
use sn_sdkg::{DkgSignedVote, DkgState, NodeId, VoteResponse};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use xor_name::XorName;

/// A mapping of DKG participant XorName to their ephemeral bls public key along
/// with their ed signature over it as proof that we can trust it
pub(crate) type DkgPubKeys = BTreeMap<XorName, (BlsPublicKey, Signature)>;

/// Ephemeral bls keys used for a single DKG session
pub(crate) struct DkgEphemeralKeys {
    /// our own generated secret key
    secret_key: BlsSecretKey,
    /// the pub keys of other participants
    pub_keys: DkgPubKeys,
}

#[derive(Default)]
pub(crate) struct DkgVoter {
    /// Ephemeral keys used by participants for each DKG session
    /// keyed by DkgSessionId hash
    dkg_ephemeral_keys: HashMap<Digest256, DkgEphemeralKeys>,
    /// Once we've got our ephemeral keys, we can go on with DKG with DKG states
    /// keyed by DkgSessionId hash
    dkg_states: HashMap<Digest256, DkgState>,
    // last dkg message timestamp
    last_received_dkg_msg_time: Option<Instant>,
}

/// Helper that creates a dkg state
fn create_dkg_state(
    session_id: &DkgSessionId,
    participant_index: usize,
    secret_key: BlsSecretKey,
    ephemeral_bls_pks: DkgPubKeys,
) -> Result<DkgState> {
    let mut rng = bls::rand::rngs::OsRng;
    let threshold = threshold(session_id.elders.len());
    let mut public_keys: BTreeMap<NodeId, BlsPublicKey> = BTreeMap::new();
    for (xorname, (ephemeral_pk, _)) in ephemeral_bls_pks.iter() {
        if let Some(index) = session_id.elder_index(*xorname) {
            let _ = public_keys.insert(index as u8, *ephemeral_pk);
        } else {
            return Err(Error::NodeNotInDkgSession(*xorname));
        }
    }
    Ok(DkgState::new(
        participant_index as u8,
        secret_key,
        public_keys,
        threshold,
        &mut rng,
    )?)
}

// Helper that checks an ephemeral pubkey
pub(crate) fn check_ephemeral_dkg_key(
    session_id: &DkgSessionId,
    key_owner: XorName,
    key: BlsPublicKey,
    key_sig: Signature,
) -> Result<()> {
    // check key owner is in dkg session
    if !session_id.elders.contains_key(&key_owner) {
        return Err(Error::NodeNotInDkgSession(key_owner));
    }

    // check key_sig
    let sender_pubkey = pub_key(&key_owner).map_err(|_| Error::InvalidXorname(key_owner))?;
    debug!(
        "Checking dkg ephemeral key s{} from {:?}",
        session_id.sh(),
        key_owner
    );
    let serialized_key = bincode::serialize(&key)?;
    if sender_pubkey.verify(&serialized_key, &key_sig).is_err() {
        warn!(
            "Got an invalid signature in Dkg s{} from {:?} key_sig: {:?} pubkey: {:?}",
            session_id.sh(),
            key_owner,
            key_sig,
            sender_pubkey
        );
        return Err(Error::InvalidSignature);
    }

    Ok(())
}

impl DkgVoter {
    /// Generate ephemeral secret key and save the key pair
    /// If we already have a key for the current session_id,
    /// this function mutates nothing and returns an error
    pub(crate) fn gen_ephemeral_key(
        &mut self,
        session_id_hash: Digest256,
        our_name: XorName,
        keypair: &Arc<Keypair>,
    ) -> Result<(BlsPublicKey, Signature)> {
        // error out if we already have a key
        if self.dkg_ephemeral_keys.get(&session_id_hash).is_some() {
            return Err(Error::DkgEphemeralKeyAlreadyGenerated);
        }

        // gen new key
        let new_secret_key: BlsSecretKey = bls::rand::random();
        let new_pub_key = new_secret_key.public_key();
        let serialized_key = bincode::serialize(&new_pub_key)?;
        let key_sig = types::keys::ed25519::sign(&serialized_key, keypair);
        let ephemeral_keys = DkgEphemeralKeys {
            secret_key: new_secret_key,
            pub_keys: BTreeMap::from_iter([(our_name, (new_pub_key, key_sig))]),
        };

        // insert the key
        let _did_insert = self
            .dkg_ephemeral_keys
            .insert(session_id_hash, ephemeral_keys);

        debug!(
            "Signing Dkg ephemeral key s{} from {:?} key_sig: {:?} pubkey: {:?}",
            session_id_hash.iter().sum::<u8>(),
            our_name,
            key_sig,
            new_pub_key,
        );
        Ok((new_pub_key, key_sig))
    }

    pub(crate) fn last_received_dkg_message(&self) -> Option<Instant> {
        self.last_received_dkg_msg_time
    }

    pub(crate) fn learned_something_from_message(&mut self) {
        self.last_received_dkg_msg_time = Some(Instant::now());
    }

    /// Initializes our DKG state and returns our first vote and dkg keys
    /// If we already have a DKG state, this function does nothing
    pub(crate) fn initialize_dkg_state(
        &mut self,
        session_id: &DkgSessionId,
        participant_index: usize,
    ) -> Result<(DkgSignedVote, DkgPubKeys)> {
        // get our keys
        let our_keys = self
            .dkg_ephemeral_keys
            .get(&session_id.hash())
            .ok_or_else(|| Error::NoDkgKeysForSession(session_id.clone()))?;

        // initialize dkg state if it doesn't exist yet
        let dkg_state = self
            .dkg_states
            .entry(session_id.hash())
            .or_insert(create_dkg_state(
                session_id,
                participant_index,
                our_keys.secret_key.clone(),
                our_keys.pub_keys.clone(),
            )?);

        // return our vote along with the dkg keys
        let first_vote = dkg_state.first_vote()?;

        Ok((first_vote, our_keys.pub_keys.clone()))
    }

    /// Try to initialize DKG with given key, and return first vote
    pub(crate) fn try_init_dkg(
        &mut self,
        session_id: &DkgSessionId,
        participant_index: usize,
        ephemeral_pub_key: BlsPublicKey,
        key_sig: Signature,
        sender: XorName,
    ) -> Result<Option<(DkgSignedVote, DkgPubKeys)>> {
        // check and save key
        let just_completed = self.save_key(session_id, sender, ephemeral_pub_key, key_sig)?;
        if !just_completed {
            debug!(
                "Waiting for more Dkg keys s{} id:{participant_index}...",
                session_id.sh()
            );
            return Ok(None);
        }
        debug!(
            "Got all Dkg keys s{} id:{participant_index}",
            session_id.sh()
        );

        let (first_vote, pub_keys) = self.initialize_dkg_state(session_id, participant_index)?;

        Ok(Some((first_vote, pub_keys)))
    }

    /// Check and save ephemeral bls keys
    /// Returns true if we just completed the set (and need to initialize DKG state)
    pub(crate) fn save_key(
        &mut self,
        session_id: &DkgSessionId,
        key_owner: XorName,
        key: BlsPublicKey,
        key_sig: Signature,
    ) -> Result<bool> {
        // check key
        check_ephemeral_dkg_key(session_id, key_owner, key, key_sig)?;

        // check if we have our secret key yet
        let our_keys = self
            .dkg_ephemeral_keys
            .get_mut(&session_id.hash())
            .ok_or_else(|| Error::NoDkgKeysForSession(session_id.clone()))?;

        // check for double key attack
        if let Some((already_had, old_sig)) = our_keys.pub_keys.get(&key_owner) {
            if already_had != &key {
                return Err(Error::DoubleKeyAttackDetected(
                    key_owner,
                    Box::new(key),
                    Box::new(key_sig),
                    Box::new(*already_had),
                    Box::new(*old_sig),
                ));
            } else {
                debug!(
                    "Ignoring known ephemeral key from {} in s{}",
                    key_owner,
                    session_id.sh()
                );
                return Ok(false);
            }
        }

        let did_insert = our_keys
            .pub_keys
            .insert(key_owner, (key, key_sig))
            .is_some();
        let what_we_have = our_keys.pub_keys.keys().collect::<BTreeSet<_>>();
        let what_we_need = session_id.elders.keys().collect::<BTreeSet<_>>();
        let just_completed = what_we_have == what_we_need;
        debug!(
            "Dkg keys s{}: ours: {:?}, in session_id: {:?}",
            session_id.sh(),
            what_we_have,
            what_we_need,
        );

        if did_insert {
            self.learned_something_from_message();
        }
        Ok(just_completed)
    }

    /// Checks the given keys and returns them
    /// Catches if we have missing keys locally
    /// Tell caller if that update helped us complete the set
    pub(crate) fn check_keys(
        &mut self,
        session_id: &DkgSessionId,
        keys: DkgPubKeys,
    ) -> Result<(DkgPubKeys, bool)> {
        let our_keys = &self
            .dkg_ephemeral_keys
            .get(&session_id.hash())
            .ok_or_else(|| Error::NoDkgKeysForSession(session_id.clone()))?
            .pub_keys;

        // check if our keys match
        if &keys == our_keys {
            return Ok((keys, false));
        }

        // catch up with their keys
        let completed = keys
            .iter()
            .map(|(name, (key, key_sig))| self.save_key(session_id, *name, *key, *key_sig))
            .collect::<Result<Vec<bool>>>()?;

        // we should now have the same keys, tell caller if update helped us complete the set
        Ok((keys, completed.iter().any(|b| *b)))
    }

    /// Get the dkg keys for a given session
    pub(crate) fn get_dkg_keys(&self, session_id: &DkgSessionId) -> Result<DkgPubKeys> {
        let our_keys = self
            .dkg_ephemeral_keys
            .get(&session_id.hash())
            .ok_or_else(|| Error::NoDkgKeysForSession(session_id.clone()))?
            .pub_keys
            .clone();
        Ok(our_keys)
    }

    /// Get all the votes we received for a given session
    pub(crate) fn get_all_votes(&self, session_id: &DkgSessionId) -> Result<Vec<DkgSignedVote>> {
        match self.dkg_states.get(&session_id.hash()) {
            Some(state) => Ok(state.all_votes()),
            None => Err(Error::NoDkgStateForSession(session_id.clone())),
        }
    }

    /// Handles Dkg vote
    pub(crate) fn handle_dkg_vote(
        &mut self,
        session_id: &DkgSessionId,
        vote: DkgSignedVote,
    ) -> Result<Vec<VoteResponse>> {
        let rng = bls::rand::rngs::OsRng;
        match self.dkg_states.get_mut(&session_id.hash()) {
            Some(state) => Ok(state.handle_signed_vote(vote, rng)?),
            None => Err(Error::NoDkgStateForSession(session_id.clone())),
        }
    }

    /// Checks a dkg session for termination
    pub(crate) fn reached_termination(&self, session_id: &DkgSessionId) -> Result<bool> {
        match self.dkg_states.get(&session_id.hash()) {
            Some(state) => Ok(state.reached_termination()?),
            None => Ok(false),
        }
    }

    /// Get the DKG outome
    pub(crate) fn outcome(
        &self,
        session_id: &DkgSessionId,
    ) -> Result<Option<(NodeId, PublicKeySet, SecretKeyShare)>> {
        match self.dkg_states.get(&session_id.hash()) {
            Some(state) => {
                let our_id = state.id();
                if let Some((pks, sks)) = state.outcome()? {
                    Ok(Some((our_id, pks, sks)))
                } else {
                    Ok(None)
                }
            }
            None => Err(Error::NoDkgStateForSession(session_id.clone())),
        }
    }

    /// Force DKG termination
    pub(crate) fn force_termination(
        &mut self,
        session_id: &DkgSessionId,
    ) -> Result<Option<(NodeId, PublicKeySet, SecretKeyShare)>> {
        match self.dkg_states.get_mut(&session_id.hash()) {
            Some(state) => {
                let our_id = state.id();
                if let Some((pks, sks)) = state.force_termination()? {
                    Ok(Some((our_id, pks, sks)))
                } else {
                    Ok(None)
                }
            }
            None => Err(Error::NoDkgStateForSession(session_id.clone())),
        }
    }

    /// Permanently removes a session from the DkgVoter
    /// Make sure this function is only called for outdated DKG sessions!
    pub(crate) fn remove(&mut self, sessions_hash: &Digest256) {
        let _ = self.dkg_ephemeral_keys.remove(sessions_hash);
        let _ = self.dkg_states.remove(sessions_hash);
    }
}
