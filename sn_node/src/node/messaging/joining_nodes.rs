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
        JoinAsRelocatedRequest, JoinAsRelocatedResponse, JoinRejectReason, JoinResponse, NodeMsg,
    },
    network_knowledge::{MembershipState, NodeState, SectionAuthUtils, MIN_ADULT_AGE},
    types::{log_markers::LogMarker, Peer},
};

use std::sync::Arc;
use tokio::sync::RwLock;

// Message handling
impl MyNode {
    pub(crate) async fn handle_join(
        node: Arc<RwLock<MyNode>>,
        context: &NodeContext,
        peer: Peer,
    ) -> Result<Option<Cmd>> {
        debug!("Handling join from {peer:?}");

        // Ignore a join request if we are not elder.
        if !context.is_elder {
            warn!("Join request received to our section, but I am not an elder...");
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(None);
        }
        let our_prefix = context.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name()) {
            debug!("Unreachable path; {peer} name doesn't match our prefix. Should be covered by AE. Dropping the msg.");
            return Ok(None);
        }
        if !MyNode::verify_joining_node_age(&peer) {
            debug!("Unreachable path; {peer} age is invalid: {}. This should be a hard coded value in join logic. Dropping the msg.", peer.age());
            return Ok(None);
        }

        if !context.joins_allowed {
            debug!("Rejecting join request from {peer} - joins currently not allowed.");
            let msg =
                NodeMsg::JoinResponse(JoinResponse::Rejected(JoinRejectReason::JoinsDisallowed));
            trace!("{}", LogMarker::SendJoinRejected);
            trace!("Sending {msg:?} to {peer}");
            return Ok(Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            )));
        }

        // NB: No reachability check has been made here
        // We propose membership
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
        debug!("Received JoinAsRelocatedRequest {join_request:?} from {peer}");

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

        let mut shall_retry = false;

        // The peer shall match the previous_name to be trusted as relocated
        if relocate_details.previous_name == peer.name() {
            debug!("JoinAsRelocatedRequest from {peer} - using old name.");
            shall_retry = true;
        }

        let comm = context.comm.clone();
        let our_prefix = context.network_knowledge.prefix();
        // TODO: the prefix match shall against the `relocation_details.dst`?
        if !our_prefix.matches(&peer.name())
            || join_request.section_key != context.network_knowledge.section_key()
        {
            // The relocated node sent first JoinAsRelocatedRequest to the elders of target section,
            // using its old name. Which could be counted as incorrect here when cross sections?
            debug!("JoinAsRelocatedRequest from {peer} - name doesn't match our prefix {our_prefix:?}.");
            shall_retry = true;
        }

        if shall_retry {
            let dst_sap = if let Ok(sap) = context
                .network_knowledge
                .section_auth_by_name(&relocate_details.dst)
            {
                sap
            } else {
                warn!(
                    "Cannot get sap for target section {:?}",
                    relocate_details.dst
                );
                return None;
            };
            let msg =
                NodeMsg::JoinAsRelocatedResponse(Box::new(JoinAsRelocatedResponse::Retry(dst_sap)));

            trace!("{} b", LogMarker::SendJoinAsRelocatedResponse);

            trace!("Sending {msg:?} to {peer}");
            return Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            ));
        }

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

            trace!(
                "Relocation reachability check, sending {:?} to {}",
                msg,
                peer
            );
            return Some(MyNode::send_system_msg(
                msg,
                Peers::Single(peer),
                context.clone(),
            ));
        };

        debug!("[NODE WRITE]: join as relocated write...");

        let mut x = node.write().await;
        debug!("[NODE WRITE]: join as relocated write gottt...");

        // Shall propose as Joined with a new name generated by the relocated node.
        // Instead of propose as Relocated again.
        // The relocated node shall already reset to the new name.
        let new_joined_state = NodeState::joined(peer, Some(relocate_details.previous_name));
        x.propose_membership_change(new_joined_state)
    }
}
