// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::super::Core;
use crate::elder_count;
use crate::messaging::{
    system::{
        JoinAsRelocatedRequest, JoinAsRelocatedResponse, JoinRejectionReason, JoinRequest,
        JoinResponse, SystemMsg,
    },
    WireMsg,
};
use crate::routing::{
    error::{Error, Result},
    log_markers::LogMarker,
    relocation::RelocatePayloadUtils,
    routing_api::command::Command,
    Peer, SectionAuthUtils, FIRST_SECTION_MAX_AGE, MIN_ADULT_AGE, MIN_AGE,
};
use bls::PublicKey as BlsPublicKey;
use std::vec;

const FIRST_SECTION_MIN_ELDER_AGE: u8 = 90;

// Message handling
impl Core {
    /// Check if we already have this peer in our section
    fn peer_is_already_a_member(&self, peer: &Peer) -> bool {
        if self.network_knowledge.members().is_joined(&peer.name()) {
            debug!(
                "Ignoring JoinRequest from {} - already member of our section.",
                peer
            );
            return true;
        }

        false
    }

    pub(crate) async fn handle_join_request(
        &self,
        peer: Peer,
        join_request: JoinRequest,
    ) -> Result<Vec<Command>> {
        debug!("Received {:?} from {}", join_request, peer);

        let _permit = self
            .current_joins_semaphore
            .acquire()
            .await
            .map_err(|_| Error::PermitAcquisitionFailed)?;

        let our_section_key = self.network_knowledge.section_key().await;
        let section_key_matches = join_request.section_key == our_section_key;

        // Ignore `JoinRequest` if we are not elder, unless the join request
        // is outdated in which case we'll reply with `JoinResponse::Retry`
        // with the up-to-date info.
        if self.is_not_elder().await && section_key_matches {
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(vec![]);
        }

        if self.peer_is_already_a_member(&peer) {
            return Ok(vec![]);
        }

        let our_prefix = self.network_knowledge.prefix().await;
        if !our_prefix.matches(&peer.name()) {
            debug!(
                "Redirecting JoinRequest from {} - name doesn't match our prefix {:?}.",
                peer, our_prefix
            );

            let retry_sap = self.matching_section(&peer.name()).await?;

            let node_msg =
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(retry_sap.to_msg())));

            trace!("Sending {:?} to {}", node_msg, peer);
            trace!("{}", LogMarker::SendJoinRedirected);
            return Ok(vec![
                self.send_direct_message(peer, node_msg, our_section_key)
                    .await?,
            ]);
        }

        if *self.is_dkg_underway.read().await {
            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                JoinRejectionReason::DKGUnderway,
            )));

            trace!("{}", LogMarker::SendDKGUnderway);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![
                self.send_direct_message(peer, node_msg, our_section_key)
                    .await?,
            ]);
        }

        if !*self.joins_allowed.read().await {
            debug!(
                "Rejecting JoinRequest from {} - joins currently not allowed.",
                peer,
            );
            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                JoinRejectionReason::JoinsDisallowed,
            )));

            trace!("{}", LogMarker::SendJoinsDisallowed);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![
                self.send_direct_message(peer, node_msg, our_section_key)
                    .await?,
            ]);
        }

        // During the first section, nodes shall use ranged age to avoid too many nodes getting
        // relocated at the same time. After the first section splits, nodes shall only
        // start with an age of MIN_ADULT_AGE

        // Prefix will be empty for first section
        let (is_age_invalid, expected_age): (bool, u8) = if our_prefix.is_empty() {
            let elders = self.network_knowledge.elders().await;
            let section_members = self.network_knowledge.active_members().await.len();
            // Forces the joining node to be younger than the youngest elder in genesis section
            // avoiding unnecessary churn.

            // Check if `elder_count()` Elders are already present
            if elders.len() == elder_count() {
                // Check if the joining node is younger than the youngest elder and older than
                // MIN_AGE in the first section to avoid unnecessary churn during genesis.
                let is_age_valid = FIRST_SECTION_MIN_ELDER_AGE > peer.age() && peer.age() > MIN_AGE;
                let expected_age = FIRST_SECTION_MIN_ELDER_AGE - section_members as u8 * 2;
                (is_age_valid, expected_age)
            } else {
                // Since enough elders haven't joined the first section calculate a value
                //  within the range [FIRST_SECTION_MIN_ELDER_AGE, FIRST_SECTION_MAX_AGE].
                let expected_age = FIRST_SECTION_MAX_AGE - section_members as u8 * 2;
                let is_age_invalid =
                    peer.age() == FIRST_SECTION_MIN_ELDER_AGE || peer.age() > expected_age;
                (is_age_invalid, expected_age)
            }
        } else {
            // Age should be MIN_ADULT_AGE for joining nodes after genesis section.
            let is_age_invalid = peer.age() != MIN_ADULT_AGE;
            (is_age_invalid, MIN_ADULT_AGE)
        };

        if !section_key_matches || is_age_invalid {
            if !section_key_matches {
                trace!("{}", LogMarker::SendJoinRetryNotCorrectKey);
                trace!(
                    "JoinRequest from {} doesn't have our latest section_key {:?}, presented {:?}.",
                    peer,
                    our_section_key,
                    join_request.section_key
                );
            } else {
                trace!("{}", LogMarker::SendJoinRetryAgeIssue);
                trace!(
                    "JoinRequest from {} (with age {}) doesn't have the expected: {}",
                    peer,
                    peer.age(),
                    expected_age,
                );
            }

            let proof_chain = self.network_knowledge.section_chain().await;
            let signed_sap = self
                .network_knowledge
                .section_signed_authority_provider()
                .await;

            let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Retry {
                section_auth: signed_sap.value.to_msg(),
                section_signed: signed_sap.sig,
                proof_chain,
                expected_age,
            }));

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![
                self.send_direct_message(peer, node_msg, our_section_key)
                    .await?,
            ]);
        }

        // If the joining node has aggregated shares from enough elders,
        // verify the produced auth and accept it into the network.
        if let Some(response) = join_request.aggregated {
            if response.verify(&self.section_chain().await) {
                info!("Handling Online agreement of {:?}", peer);
                return Ok(vec![Command::HandleNewNodeOnline(response)]);
                // self
                // .handle_online_agreement(response.value.clone(), response.sig.clone())
                // .await;
            }
        }

        // Require resource signed if joining as a new node.
        if let Some(response) = join_request.resource_proof_response {
            if !self
                .validate_resource_proof_response(&peer.name(), response)
                .await
            {
                debug!(
                    "Ignoring JoinRequest from {} - invalid resource signed response",
                    peer
                );
                return Ok(vec![]);
            }
        } else {
            // Do reachability check only for the initial join request
            let cmd = if self.comm.is_reachable(&peer.addr()).await.is_err() {
                let node_msg = SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                    JoinRejectionReason::NodeNotReachable(peer.addr()),
                )));

                trace!("{}", LogMarker::SendJoinRejected);

                trace!("Sending {:?} to {}", node_msg, peer);
                self.send_direct_message(peer, node_msg, our_section_key)
                    .await?
            } else {
                // It's reachable, let's then send the proof challenge
                self.send_resource_proof_challenge(peer).await?
            };

            return Ok(vec![cmd]);
        }

        Ok(vec![Command::SendAcceptedOnlineShare {
            peer,
            previous_name: None,
        }])
    }

    pub(crate) async fn handle_join_as_relocated_request(
        &self,
        peer: Peer,
        join_request: JoinAsRelocatedRequest,
        known_keys: Vec<BlsPublicKey>,
    ) -> Result<Vec<Command>> {
        let _permit = self
            .current_joins_semaphore
            .acquire()
            .await
            .map_err(|_| Error::PermitAcquisitionFailed)?;

        debug!("Received {:?} from {}", join_request, peer);
        let relocate_payload = if let Some(relocate_payload) = join_request.relocate_payload {
            relocate_payload
        } else {
            // Do reachability check
            let node_msg = if self.comm.is_reachable(&peer.addr()).await.is_err() {
                SystemMsg::JoinAsRelocatedResponse(Box::new(
                    JoinAsRelocatedResponse::NodeNotReachable(peer.addr()),
                ))
            } else {
                SystemMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(
                    self.network_knowledge.authority_provider().await.to_msg(),
                )))
            };
            trace!("{}", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![
                self.send_direct_message(
                    peer,
                    node_msg,
                    self.network_knowledge.section_key().await,
                )
                .await?,
            ]);
        };

        if !self.network_knowledge.prefix().await.matches(&peer.name())
            || join_request.section_key != self.network_knowledge.section_key().await
        {
            debug!(
                "JoinAsRelocatedRequest from {} - name doesn't match our prefix {:?}.",
                peer,
                self.network_knowledge.prefix().await
            );

            let node_msg =
                SystemMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(
                    self.network_knowledge.authority_provider().await.to_msg(),
                )));

            trace!("{} b", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {:?} to {}", node_msg, peer);
            return Ok(vec![
                self.send_direct_message(
                    peer,
                    node_msg,
                    self.network_knowledge.section_key().await,
                )
                .await?,
            ]);
        }

        if self.network_knowledge.members().is_joined(&peer.name()) {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - already member of our section.",
                peer
            );
            return Ok(vec![]);
        }

        if !relocate_payload.verify_identity(&peer.name()) {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - invalid signature.",
                peer
            );
            return Ok(vec![]);
        }

        let details = relocate_payload.relocate_details()?;

        if !self.network_knowledge.prefix().await.matches(&details.dst) {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - destination {} doesn't match \
                         our prefix {:?}.",
                peer,
                details.dst,
                self.network_knowledge.prefix().await
            );
            return Ok(vec![]);
        }

        // Requires the node name matches the age.
        let age = details.age;
        if age != peer.age() {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - relocation age ({}) doesn't match peer's age ({}).",
                peer, age, peer.age(),
            );
            return Ok(vec![]);
        }

        // Check for signatures and trust of the relocate_payload msg
        let serialised_relocate_details =
            WireMsg::serialize_msg_payload(&SystemMsg::Relocate(details.clone()))?;

        let payload_section_signed = &relocate_payload.section_signed;
        let is_valid_sig = payload_section_signed.sig.public_key.verify(
            &payload_section_signed.sig.signature,
            serialised_relocate_details,
        );
        let is_key_unknown = !known_keys
            .iter()
            .any(|key| *key == payload_section_signed.sig.public_key);

        if !is_valid_sig || is_key_unknown {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - invalid signature or untrusted src.",
                peer
            );
            return Ok(vec![]);
        }

        let previous_name = Some(details.pub_id);

        if self
            .network_knowledge
            .members()
            .is_relocated_to_our_section(&details.pub_id)
        {
            debug!(
                "Ignoring JoinAsRelocatedRequest from {} - original node {:?} already relocated to us.",
                peer, previous_name
            );
            return Ok(vec![]);
        }

        Ok(vec![Command::SendAcceptedOnlineShare {
            peer,
            previous_name,
        }])
    }
}
