// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::cmds::Cmd, messaging::Peers, Error, MyNode, Result};
use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{
        system::{NodeMsg, SectionSigShare},
        MsgId,
    },
    network_knowledge::NodeState,
    types::{log_markers::LogMarker, Peer, SectionSig},
};

impl MyNode {
    /// Send section state proposal to `recipients`
    pub(crate) fn send_node_off_proposal(
        &mut self,
        recipients: Vec<Peer>,
        proposal: NodeState,
    ) -> Result<Vec<Cmd>> {
        info!("Sending section state proposal: {proposal:?} to {recipients:?}");

        // sign the proposal
        let serialized_proposal = bincode::serialize(&proposal).map_err(|err| {
            error!(
                "Failed to serialize section state proposal {:?}: {:?}",
                proposal, err
            );
            err
        })?;
        let sig_share = self
            .sign_with_section_key_share(serialized_proposal)
            .map_err(|err| {
                error!(
                    "Failed to sign section state proposal {:?}: {:?}",
                    proposal, err
                );
                err
            })?;

        // broadcast the proposal to the recipients
        let mut cmds = vec![];
        let (other_peers, myself) = self.split_peers_and_self(recipients);

        for node in &other_peers {
            // log a knowledge issue for each that we're proposing
            // when they vote, this will be untracked
            self.track_node_issue(node.name(), IssueType::ElderVoting);
        }

        let peers = Peers::Multiple(other_peers);
        let msg = NodeMsg::ProposeNodeOff {
            vote_node_off: proposal.clone(),
            sig_share: sig_share.clone(),
        };
        cmds.push(Cmd::send_msg(msg, peers));

        // handle ourselves if we are in the recipients
        if let Some(me) = myself {
            cmds.extend(self.handle_section_state_proposal(
                MsgId::new(),
                proposal,
                sig_share,
                me,
            )?)
        }

        Ok(cmds)
    }

    pub(crate) fn handle_section_state_proposal(
        &mut self,
        msg_id: MsgId,
        proposal: NodeState,
        sig_share: SectionSigShare,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        // proposals from other sections shall be ignored
        let our_prefix = self.network_knowledge.prefix();
        if !our_prefix.matches(&sender.name()) {
            trace!(
                "Ignore section state proposal {msg_id:?} with prefix mismatch from {sender}: {proposal:?}"
            );
            return Ok(vec![]);
        }

        // let's now verify the section key in the msg authority is trusted
        // based on our current knowledge of the network and sections chains
        let sig_share_pk = &sig_share.public_key_set.public_key();
        if !self.network_knowledge.has_chain_key(sig_share_pk) {
            warn!(
                "Ignore section state proposal {msg_id:?} with untrusted sig share from {sender}: {proposal:?}"
            );
            return Ok(vec![]);
        }

        // try aggregate
        let serialized_proposal = bincode::serialize(&proposal).map_err(|err| {
            error!("Failed to serialise section state proposal {msg_id:?} from {sender}: {proposal:?}: {err:?}");
            err
        })?;
        match self
            .section_proposal_aggregator
            .try_aggregate(&serialized_proposal, sig_share)
        {
            Ok(Some(sig)) => Ok(vec![Cmd::HandleNodeOffAgreement { proposal, sig }]),
            Ok(None) => {
                info!("Section state proposal {msg_id:?} acknowledged, waiting for more...");
                Ok(vec![])
            }
            Err(err) => {
                error!(
                    "Failed to aggregate section state proposal {msg_id:?} from {sender}: {err:?}"
                );
                Ok(vec![])
            }
        }
    }

    #[instrument(skip(self), level = "trace")]
    pub(crate) fn handle_section_decision_agreement(
        &mut self,
        proposal: NodeState,
        sig: SectionSig,
    ) -> Result<Vec<Cmd>> {
        let serialized_proposal = bincode::serialize(&proposal).map_err(|err| {
            error!("Failed to serialise SectionStateVote {proposal:?}: {err:?}");
            err
        })?;
        if !sig.verify(&serialized_proposal) {
            return Err(Error::InvalidSignature);
        }

        debug!("{:?} {:?}", LogMarker::ProposalAgreed, proposal);
        let mut cmds = Vec::new();
        cmds.extend(self.handle_offline_agreement(proposal));
        Ok(cmds)
    }

    #[instrument(skip(self))]
    fn handle_offline_agreement(&mut self, node_state: NodeState) -> Option<Cmd> {
        info!(
            "Agreement - proposing membership change with node offline: {}",
            node_state.peer()
        );
        self.propose_membership_change(node_state)
    }
}
