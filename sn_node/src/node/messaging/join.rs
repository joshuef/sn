// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, MyNode, Result};

use sn_interface::{
    messaging::system::{
        JoinAsRelocatedRequest, JoinAsRelocatedResponse, JoinRejectionReason, JoinRequest,
        JoinResponse, NodeMsg,
    },
    network_knowledge::{MembershipState, NodeState, SectionAuthUtils, MIN_ADULT_AGE},
    types::{log_markers::LogMarker, Peer},
};

use std::sync::Arc;
use tokio::sync::RwLock;

// Message handling
impl MyNode {
    pub(crate) async fn handle_join_request(
        node: Arc<RwLock<MyNode>>,
        context: &NodeContext,
        peer: Peer,
        join_request: JoinRequest,
    ) -> Result<Option<Cmd>> {
        debug!("Handling join. Received {join_request:?} from {peer:?}");

        let provided_section_key = join_request.section_key();

        let our_section_key = context.network_knowledge.section_key();
        let section_key_matches = provided_section_key == our_section_key;

        // Ignore `JoinRequest` if we are not elder, unless the join request
        // is outdated in which case we'll reply with `JoinResponse::Retry`
        // with the up-to-date info.
        if !context.is_elder && section_key_matches {
            warn!("Join req received to our section key, but I am not an elder...");
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(None);
        }

        let our_prefix = context.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name()) {
            // TODO: Replace Redirect with a Retry + AEProbe.
            debug!("Redirecting JoinRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");
            let retry_sap = context.section_sap_matching_name(&peer.name())?;
            let msg = NodeMsg::JoinResponse(JoinResponse::Redirect(retry_sap));
            trace!("Sending {:?} to {}", msg, peer);
            trace!("{}", LogMarker::SendJoinRedirected);
            return Ok(Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            )));
        }

        // check if we're limiting section size using the feature flag
        let we_should_reject_joins = if cfg!(feature = "limit-network-size") {
            const TEMP_SECTION_LIMIT: usize = 25;
            context.network_knowledge.members().len() >= TEMP_SECTION_LIMIT
        } else {
            !context.joins_allowed
        };

        if we_should_reject_joins {
            debug!("Rejecting JoinRequest from {peer} - joins currently not allowed.");
            let msg =
                NodeMsg::JoinResponse(JoinResponse::Rejected(JoinRejectionReason::JoinsDisallowed));
            trace!("{}", LogMarker::SendJoinsDisallowed);
            trace!("Sending {:?} to {}", msg, peer);
            return Ok(Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            )));
        }

        let is_age_valid = MyNode::verify_joining_node_age(&peer);

        trace!("Join proceeding: our_prefix={our_prefix:?}, is_age_valid={is_age_valid:?}");

        if !section_key_matches {
            trace!("{}", LogMarker::SendJoinRetryNotCorrectKey);
            trace!("JoinRequest from {peer} doesn't have our latest section_key {our_section_key:?}, provided {provided_section_key:?}.");
        }

        if !is_age_valid {
            trace!("{}", LogMarker::SendJoinRetryAgeIssue);
            trace!(
                "JoinRequest from {peer} (with age {}) has invalid age",
                peer.age()
            );
        }

        if !section_key_matches || !is_age_valid {
            let msg = NodeMsg::JoinResponse(JoinResponse::Retry);
            trace!("Sending {msg:?} to {peer}");
            return Ok(Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            )));
        }

        // It's reachable, let's then propose membership
        let node_state = NodeState::joined(peer, None);

        debug!("[NODE WRITE]: join propose membership write...");
        let mut node = node.write().await;
        debug!("[NODE WRITE]: join propose membership write gottt...");
        Ok(node.propose_membership_change(node_state))
    }

    pub(crate) fn verify_joining_node_age(peer: &Peer) -> bool {
        // Age should be MIN_ADULT_AGE for joining nodes.
        peer.age() == MIN_ADULT_AGE
    }

    pub(crate) async fn handle_join_as_relocated_request(
        node: Arc<RwLock<MyNode>>,
        context: &NodeContext,
        peer: Peer,
        join_request: JoinAsRelocatedRequest,
    ) -> Option<Cmd> {
        debug!("Received JoinAsRelocatedRequest {join_request:?} from {peer}",);

        let comm = context.comm.clone();
        let our_prefix = context.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name())
            || join_request.section_key != context.network_knowledge.section_key()
        {
            debug!("JoinAsRelocatedRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");

            let msg = NodeMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(
                context.network_knowledge.section_auth(),
            )));

            trace!("{} b", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {msg:?} to {peer}");
            return Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            ));
        }

        let state = join_request.relocate_proof.value.state();
        let relocate_details = if let MembershipState::Relocated(ref details) = state {
            // Check for signatures and trust of the relocate_proof
            if !join_request.relocate_proof.self_verify() {
                debug!("Ignoring JoinAsRelocatedRequest from {peer} - invalid sig.");
                return None;
            }
            let known_keys = context.network_knowledge.known_keys();
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
        };

        // Finally do reachability check
        if comm.is_reachable(&peer.addr()).await.is_err() {
            let msg = NodeMsg::JoinAsRelocatedResponse(Box::new(
                JoinAsRelocatedResponse::NodeNotReachable(peer.addr()),
            ));
            trace!("{}", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {:?} to {}", msg, peer);
            return Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            ));
        };

        debug!("[NODE WRITE]: join as relocated write...");

        let mut x = node.write().await;
        debug!("[NODE WRITE]: join as relocated write gottt...");

        x.propose_membership_change(join_request.relocate_proof.value)
    }
}
