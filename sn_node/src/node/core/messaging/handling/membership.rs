// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd,
    core::{Node, Result},
    membership,
};

use sn_consensus::{Generation, SignedVote, VoteResponse};
use sn_interface::{
    messaging::system::{KeyedSig, NodeState, SectionAuth, SystemMsg},
    types::{log_markers::LogMarker, Peer},
};

use std::{collections::BTreeSet, vec};

// Message handling
impl Node {
    pub(crate) fn propose_membership_change(&self, node_state: NodeState) -> Result<Vec<Cmd>> {
        info!(
            "Proposing membership change: {} - {:?}",
            node_state.name, node_state.state
        );
        let prefix = self.network_knowledge.prefix();
        if let Some(ref mut membership) = *self.membership.borrow_mut() {
            let membership_vote = match membership.propose(node_state, &prefix) {
                Ok(vote) => vote,
                Err(e) => {
                    warn!("Membership - failed to propose change: {e:?}");
                    return Ok(vec![]);
                }
            };

            let cmds =
                self.send_msg_to_our_elders(SystemMsg::MembershipVotes(vec![membership_vote]))?;
            Ok(vec![cmds])
        } else {
            error!("Membership - Failed to propose membership change, no membership instance");
            Ok(vec![])
        }
    }

    pub(crate) fn handle_membership_votes(
        &self,
        peer: Peer,
        signed_votes: Vec<SignedVote<NodeState>>,
    ) -> Result<Vec<Cmd>> {
        debug!(
            "{:?} {signed_votes:?} from {peer}",
            LogMarker::MembershipVotesBeingHandled
        );
        let prefix = self.network_knowledge.prefix();

        let mut cmds = vec![];

        for signed_vote in signed_votes {
            let membership_res = if let Some(ref mut membership) = *self.membership.borrow_mut() {
                let result = membership.handle_signed_vote(signed_vote, &prefix);
                let generation = membership.generation();

                Some((result, generation))
            } else {
                None
            };

            if let Some((result, generation)) = membership_res {
                match result {
                    Ok(VoteResponse::Broadcast(response_vote)) => {
                        cmds.push(self.send_msg_to_our_elders(SystemMsg::MembershipVotes(
                            vec![response_vote],
                        ))?);
                    }
                    Ok(VoteResponse::WaitingForMoreVotes) => {
                        //do nothing
                    }
                    Err(membership::Error::RequestAntiEntropy) => {
                        debug!("Membership - We are behind the voter, requesting AE");

                        // We hit an error while processing this vote, perhaps we are missing information.
                        // We'll send a membership AE request to see if they can help us catch up.
                        let sap = self.network_knowledge.authority_provider();
                        let dst_section_pk = sap.section_key();
                        let section_name = prefix.name();
                        let msg = SystemMsg::MembershipAE(generation);
                        let cmd = self.send_direct_msg_to_nodes(
                            vec![peer],
                            msg,
                            section_name,
                            dst_section_pk,
                        )?;

                        debug!("{:?}", LogMarker::MembershipSendingAeUpdateRequest);
                        cmds.push(cmd);
                        // return the vec w/ the AE cmd there so as not to loop and generate AE for
                        // any subsequent commands
                        return Ok(cmds);
                    }
                    Err(e) => {
                        error!("Membership - error while processing vote {e:?}, dropping this and all votes in this batch thereafter");
                        break;
                    }
                };
            }

            // }

            let option_membership = &*self.membership.borrow();
            if let Some(membership) = option_membership {
                // TODO: We should be able to detect when a *new* decision is made
                //       As it stands, we will reprocess each decision for any new vote
                //       we receive, it should be safe to do as `HandleNewNodeOnline`
                //       should be idempotent.
                if let Some(decision) = membership.most_recent_decision() {
                    // process the membership change
                    debug!(
                        "Handling Membership Decision {:?}",
                        BTreeSet::from_iter(decision.proposals.keys())
                    );
                    for (state, signature) in &decision.proposals {
                        let sig = KeyedSig {
                            public_key: membership.voters_public_key_set().public_key(),
                            signature: signature.clone(),
                        };
                        if membership.is_leaving_section(state, prefix) {
                            cmds.push(Cmd::HandleNodeLeft(SectionAuth {
                                value: state.clone(),
                                sig,
                            }));
                        } else {
                            cmds.push(Cmd::HandleNewNodeOnline(SectionAuth {
                                value: state.clone(),
                                sig,
                            }));
                        }
                    }
                }
            } else {
                error!(
                    "Attempted to handle membership vote when we don't yet have a membership instance"
                );
            };
        }

        Ok(cmds)
    }

    pub(crate) fn handle_membership_anti_entropy(
        &self,
        peer: Peer,
        gen: Generation,
    ) -> Result<Vec<Cmd>> {
        debug!(
            "{:?} membership anti-entropy request for gen {:?} from {}",
            LogMarker::MembershipAeRequestReceived,
            gen,
            peer
        );

        let cmds = if let Some(membership) = self.membership.borrow().as_ref() {
            match membership.anti_entropy(gen) {
                Ok(catchup_votes) => {
                    vec![self.send_direct_msg(
                        peer,
                        SystemMsg::MembershipVotes(catchup_votes),
                        self.network_knowledge.section_key(),
                    )?]
                }
                Err(e) => {
                    error!("Membership - Error while processing anti-entropy {:?}", e);
                    vec![]
                }
            }
        } else {
            error!(
                "Attempted to handle membership anti-entropy when we don't yet have a membership instance"
            );
            vec![]
        };

        Ok(cmds)
    }
}
