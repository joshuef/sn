// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, Error, MyNode, Result,
};

use qp2p::SendStream;
use sn_interface::{
    messaging::{
        system::{JoinRejectReason, JoinResponse, NodeMsg},
        MsgId,
    },
    network_knowledge::{NodeState, RelocationProof, MIN_ADULT_AGE},
    types::{log_markers::LogMarker, Peer},
};

use std::sync::Arc;
use tokio::sync::RwLock;

// Message handling
impl MyNode {
    pub(crate) async fn handle_join(
        node: &mut MyNode,
        context: &NodeContext,
        peer: Peer,
        correlation_id: MsgId,
        relocation: Option<RelocationProof>,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        trace!("Handling join from {peer:?}");

        // Ignore a join request if we are not elder.
        if !context.is_elder {
            warn!("Join request received to our section, but I am not an elder...");
            // Note: We don't bounce this message because the current bounce-resend
            // mechanism wouldn't preserve the original SocketAddr which is needed for
            // properly handling this message.
            // This is OK because in the worst case the join request just timeouts and the
            // joining node sends it again.
            return Ok(vec![]);
        }
        let our_prefix = context.network_knowledge.prefix();
        if !our_prefix.matches(&peer.name()) {
            debug!("Unreachable path; {peer} name doesn't match our prefix. Should be covered by AE. Dropping the msg.");
            return Ok(vec![]);
        }

        let previous_name = if let Some(proof) = relocation {
            // Relocation ->
            // Verify that we know the src key..
            let src_key = proof.signed_by();
            if !context
                .network_knowledge
                .verify_section_key_is_known(src_key)
            {
                warn!("Peer {} is trying to join with signature by unknown source section key {src_key:?}. Message is dropped.", peer.name());
                return Ok(vec![]);
            }

            // Verify the signatures..
            proof.verify()?;

            // Verify the age..
            MyNode::verify_relocated_age(&peer, &proof)?;

            // NB: Relocated nodes that try to join, are accepted even if joins are disallowed.
            Some(proof.previous_name())
        } else {
            // New node ->
            if !MyNode::is_infant_node(&peer) {
                debug!("Unreachable path; {peer} age is invalid: {}. This should be a hard coded value in join logic. Dropping the msg.", peer.age());
                return Ok(vec![]);
            }

            if !context.joins_allowed {
                trace!("Rejecting join request from {peer} - joins currently not allowed.");
                let msg = NodeMsg::JoinResponse(JoinResponse::Rejected(
                    JoinRejectReason::JoinsDisallowed,
                ));
                trace!("{}", LogMarker::SendJoinRejected);
                trace!("Sending {msg:?} to {peer}");

                // Send it over response stream if we have one
                if let Some(stream) = send_stream {
                    return Ok(vec![Cmd::send_node_response(
                        msg,
                        correlation_id,
                        peer,
                        stream,
                    )]);
                }

                return Ok(vec![Cmd::send_msg(msg, Peers::Single(peer))]);
            }

            None
        };

        let mut cmds = vec![];

        // Let the joiner know we are considering.
        if let Some(send_stream) = send_stream {
            cmds.push(Cmd::send_node_response(
                NodeMsg::JoinResponse(JoinResponse::UnderConsideration),
                correlation_id,
                peer,
                send_stream,
            ));
        }

        // We propose membership
        let node_state = NodeState::joined(peer, previous_name);

        if let Some(cmd) = node.propose_membership_change(node_state) {
            cmds.push(cmd);
        }
        Ok(cmds)
    }

    pub(crate) fn is_infant_node(peer: &Peer) -> bool {
        // Age should be MIN_ADULT_AGE for joining infant.
        peer.age() == MIN_ADULT_AGE
    }

    pub(crate) fn verify_relocated_age(peer: &Peer, proof: &RelocationProof) -> Result<()> {
        let name = peer.name();
        let peer_age = peer.age();
        let previous_age = proof.previous_age();

        // We require peer current age to be one more than the previous age.
        if peer_age != previous_age.saturating_add(1) {
            info!(
                "Invalid relocation from {name} - peer new age ({peer_age}) should be one more than peer's previous age ({previous_age}), or same if {}.", u8::MAX
            );
            return Err(Error::InvalidRelocationDetails);
        }

        Ok(())
    }
}
