// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::cmds::Cmd, messaging::Peers, MyNode, Result};

use sn_interface::{
    messaging::{
        system::{
            JoinAsRelocatedRequest, JoinAsRelocatedResponse, JoinRejectionReason, JoinRequest,
            JoinResponse, NodeMsg,
        },
        MsgId,
    },
    network_knowledge::{
        MembershipState, NodeState, SectionAuthUtils, SectionTreeUpdate, MIN_ADULT_AGE,
    },
    types::{log_markers::LogMarker, Peer},
};

use std::sync::Arc;
use tokio::sync::RwLock;

// Message handling
impl MyNode {
    pub(crate) async fn handle_join_request(
        node: Arc<RwLock<MyNode>>,
        msg_id: MsgId,
        peer: Peer,
        join_request: JoinRequest,
    ) -> Result<Option<Cmd>> {
        debug!(
            "{msg_id:?} Handling join. Received {:?} from {}",
            join_request, peer
        );

        let node_read_lock = node.read().await;

        debug!("{msg_id:?} Handling join. node read lock got for {join_request:?}");

        let provided_section_key = join_request.section_key();

        let our_section_key = node_read_lock.network_knowledge.section_key();
        let section_key_matches = provided_section_key == our_section_key;

        // Ignore `JoinRequest` if we are not elder, unless the join request
        // is outdated in which case we'll reply with `JoinResponse::Retry`
        // with the up-to-date info.
        if node_read_lock.is_not_elder() && section_key_matches {
            debug!("{msg_id:?} Ignoring join as we're not an elder");
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(None);
        }

        let our_prefix = node_read_lock.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name()) {
            debug!("{msg_id:?} Redirecting JoinRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");
            let retry_sap = node_read_lock.matching_section(&peer.name())?;
            let msg = NodeMsg::JoinResponse(Box::new(JoinResponse::Redirect(retry_sap)));
            trace!("{msg_id:?} Sending {:?} to {}", msg, peer);
            trace!("{}", LogMarker::SendJoinRedirected);
            return Ok(Some(
                node_read_lock.send_system_msg(msg, Peers::Single(peer)),
            ));
        }

        if !node_read_lock.joins_allowed {
            debug!("{msg_id:?} Rejecting JoinRequest from {peer} - joins currently not allowed.");
            let msg = NodeMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                JoinRejectionReason::JoinsDisallowed,
            )));
            trace!("{}", LogMarker::SendJoinsDisallowed);
            trace!("Sending {:?} to {}", msg, peer);
            return Ok(Some(
                node_read_lock.send_system_msg(msg, Peers::Single(peer)),
            ));
        }

        let is_age_valid = node_read_lock.verify_joining_node_age(&peer);

        trace!("{msg_id:?} our_prefix={our_prefix:?}, is_age_valid={is_age_valid:?}");

        if !section_key_matches {
            trace!("{}", LogMarker::SendJoinRetryNotCorrectKey);
            trace!("{msg_id:?} JoinRequest from {peer} doesn't have our latest section_key {our_section_key:?}, provided {provided_section_key:?}.");
        }

        if !is_age_valid {
            trace!("{}", LogMarker::SendJoinRetryAgeIssue);
            trace!(
                "{msg_id:?} JoinRequest from {peer} (with age {}) has invalid age",
                peer.age()
            );
        }

        if !section_key_matches || !is_age_valid {
            let signed_sap = node_read_lock.network_knowledge.signed_sap();
            let proof_chain = node_read_lock.network_knowledge.section_chain();
            let msg = NodeMsg::JoinResponse(Box::new(JoinResponse::Retry {
                section_tree_update: SectionTreeUpdate::new(signed_sap, proof_chain),
            }));
            trace!(
                "{msg_id:?} Mismatch of key or age: Sending {:?} to {}",
                msg,
                peer
            );
            return Ok(Some(
                node_read_lock.send_system_msg(msg, Peers::Single(peer)),
            ));
        }

        // It's reachable, let's then propose membership
        let node_state = NodeState::joined(peer, None);

        // drop readlock and get write lock
        drop(node_read_lock);

        debug!("{msg_id:?} Node read lock dropped during join");
        let mut node = node.write().await;
        debug!("{msg_id:?} Node write lock got during join");

        Ok(node.propose_membership_change(node_state))
    }

    pub(crate) fn verify_joining_node_age(&self, peer: &Peer) -> bool {
        // Age should be MIN_ADULT_AGE for joining nodes.
        peer.age() == MIN_ADULT_AGE
    }

    pub(crate) async fn handle_join_as_relocated_request(
        node: Arc<RwLock<MyNode>>,
        peer: Peer,
        join_request: JoinAsRelocatedRequest,
    ) -> Option<Cmd> {
        debug!("Received JoinAsRelocatedRequest {join_request:?} from {peer}",);
        let read_locked_node = node.read().await;
        let comm = read_locked_node.comm.clone();
        let our_prefix = read_locked_node.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name())
            || join_request.section_key != read_locked_node.network_knowledge.section_key()
        {
            debug!("JoinAsRelocatedRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");

            let msg = NodeMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(
                read_locked_node.network_knowledge.section_auth(),
            )));

            trace!("{} b", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {msg:?} to {peer}");
            return Some(read_locked_node.send_system_msg(msg, Peers::Single(peer)));
        }

        let state = join_request.relocate_proof.value.state();
        let relocate_details = if let MembershipState::Relocated(ref details) = state {
            // Check for signatures and trust of the relocate_proof
            if !join_request.relocate_proof.self_verify() {
                debug!("Ignoring JoinAsRelocatedRequest from {peer} - invalid sig.");
                return None;
            }
            let known_keys = read_locked_node.network_knowledge.known_keys();
            if !known_keys.contains(&join_request.relocate_proof.sig.public_key) {
                debug!("Ignoring JoinAsRelocatedRequest from {peer} - untrusted src.");
                return None;
            }

            details
        } else {
            debug!("Ignoring JoinAsRelocatedRequest from {peer} with invalid relocate proof state: {state:?}");
            return None;
        };

        if !relocate_details.verify_identity(&peer.name(), &join_request.signature_over_new_name) {
            debug!("Ignoring JoinAsRelocatedRequest from {peer} - invalid node name signature.");
            return None;
        }

        // drop read lock before we do reachability check
        drop(read_locked_node);

        // Finally do reachability check
        if comm.is_reachable(&peer.addr()).await.is_err() {
            let msg = NodeMsg::JoinAsRelocatedResponse(Box::new(
                JoinAsRelocatedResponse::NodeNotReachable(peer.addr()),
            ));
            trace!("{}", LogMarker::SendJoinAsRelocatedResponse);
            let read_locked_node = node.read().await;

            trace!("Sending {:?} to {}", msg, peer);
            return Some(read_locked_node.send_system_msg(msg, Peers::Single(peer)));
        };

        node.write()
            .await
            .propose_membership_change(join_request.relocate_proof.value)
    }
}
