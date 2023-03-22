// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod anti_entropy;
mod client_msgs;
mod data;
mod dkg;
mod handover;
mod join_section;
mod joining_nodes;
mod membership;
pub(crate) mod node_msgs;
mod promotion;
mod relocation;
mod section_state;
mod serialize;
mod signature;
mod streams;
mod update_section;

use crate::node::{flow_ctrl::cmds::Cmd, Error, MyNode, Result};
use sn_interface::{
    messaging::{AntiEntropyMsg, MsgKind, NetworkMsg, WireMsg},
    network_knowledge::NetworkKnowledge,
    types::{log_markers::LogMarker, ClientId, NodeId, Participant},
};

use qp2p::SendStream;
use std::collections::BTreeSet;
use xor_name::XorName;

#[derive(Debug, Clone)]
pub enum Recipients {
    Single(Participant),
    Multiple(BTreeSet<NodeId>),
}

impl Recipients {
    #[cfg(test)]
    pub(crate) fn get(&self) -> BTreeSet<Participant> {
        match self.clone() {
            Self::Single(p) => BTreeSet::from([p]),
            Self::Multiple(nodes) => nodes.into_iter().map(Participant::from_node).collect(),
        }
    }
}

impl IntoIterator for Recipients {
    type Item = Participant;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Recipients::Single(p) => Box::new(std::iter::once(p)),
            Recipients::Multiple(ps) => Box::new(ps.into_iter().map(Participant::from_node)),
        }
    }
}

// Message handling
impl MyNode {
    #[instrument(skip(wire_msg, send_stream))]
    pub(crate) async fn handle_msg(
        network_knowledge: &NetworkKnowledge,
        our_name: XorName,
        is_elder: bool,
        sender: Participant,
        wire_msg: WireMsg,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        let msg_id = wire_msg.msg_id();
        let msg_kind = wire_msg.kind();

        trace!("Handling msg {msg_id:?}. from {sender:?} Checking for AE first...");

        let all_members = network_knowledge.members();

        // we check if any member sent us this. Client messages could eg reach an elder via another elder and this
        // checks if _anyone_ forwarded it to us (including ourselves), and if so, we assume that's legit and process
        let members_with_sender_addr: Vec<_> = all_members
            .iter()
            .filter(|n| n.addr() == sender.addr())
            .collect();
        let sent_from_a_member = !members_with_sender_addr.is_empty();

        debug!("{msg_id:?} was sent from a member: {sent_from_a_member:?}");

        let is_for_us =
            sent_from_a_member || wire_msg.dst().name == our_name || msg_kind.is_client_spend();
        debug!(
            "{msg_id:?} is for us? {is_for_us}: wiremsg dst name: {:?} vs our name: {:?}",
            wire_msg.dst().name,
            our_name
        );

        // 1. is_from_us happens when we as an Elder forwarded a data msg to ourselves as data holder.
        // 2. client msg directly to us, only exists for
        //    A: payments (which today is DataCmd::Spentbook)
        //    B: AeProbe

        // When we have implemented the proper Spentbook type, we have type safety and
        // can only receive the specific client Spend cmd to us as Elder (client doesn't send it to others than the 7 Elder recipients),
        // i.e. it is never _forwarded_.
        // thus all msgs are then to us if dst.name == our name.
        // The msg from Elders to nodes are StoreSpentShare which are not forwarded either, but always to "us".

        // first check for AE, if this isn't an ae msg itself
        if !msg_kind.is_ae_msg() {
            let entropy =
                MyNode::check_for_entropy(is_elder, &wire_msg, &network_knowledge, &sender)?;
            if let Some((update, ae_kind)) = entropy {
                debug!("bailing early, AE found for {msg_id:?}");
                return MyNode::generate_anti_entropy_cmds(
                    &wire_msg,
                    sender,
                    update,
                    ae_kind,
                    send_stream,
                );
            }
        }

        // if it's not directly for us, but is a node msg, it's perhaps for the section, and so we handle it as normal
        if !is_for_us {
            if let MsgKind::Client { .. } = msg_kind {
                let Some(stream) = send_stream else {
                    return Err(Error::NoClientResponseStream);
                };

                trace!("{:?}: {msg_id:?} ", LogMarker::ClientMsgToBeForwarded);
                let cmd = MyNode::forward_data_and_respond_to_client(
                    network_knowledge,
                    wire_msg,
                    ClientId::from(sender),
                    stream,
                );
                return Ok(vec![cmd]);
            }
        }

        // Deserialize the payload of the incoming message
        let msg_type = match wire_msg.into_msg() {
            Ok(msg_type) => msg_type,
            Err(error) => {
                error!("Failed to deserialize message payload ({msg_id:?}): {error:?}");
                return Ok(vec![]);
            }
        };

        // if we got here, we are the destination
        match msg_type {
            NetworkMsg::Node(msg) => Ok(vec![Cmd::ProcessNodeMsg {
                msg_id,
                msg,
                node_id: NodeId::from(sender),
                send_stream,
            }]),
            NetworkMsg::Client { auth, msg } => Ok(vec![Cmd::ProcessClientMsg {
                msg_id,
                msg,
                auth,
                client_id: ClientId::from(sender),
                send_stream,
            }]),
            NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                section_tree_update,
                kind,
            }) => Ok(vec![Cmd::ProcessAeMsg {
                msg_id,
                section_tree_update,
                kind,
                sender,
            }]),
            // Respond to a probe msg
            // We always respond to probe msgs if we're an elder as health checks use this to see if elders are alive
            // and repsonsive, as well as being a method for a sender of the probe to keep up to date.
            NetworkMsg::AntiEntropy(AntiEntropyMsg::Probe(section_key)) => {
                debug!("Aeprobe in");
                let mut cmds = vec![];
                if !is_elder {
                    info!("Dropping AEProbe since we are not an elder");
                    // early return here as we do not get health checks as adults,
                    // normal AE rules should have applied
                    return Ok(cmds);
                }
                trace!("Received Probe message from {}: {:?}", sender, msg_id);
                cmds.push(MyNode::send_ae_update_to_nodes(
                    network_knowledge,
                    Recipients::Single(sender),
                    section_key,
                ));
                Ok(cmds)
            }
            other @ NetworkMsg::DataResponse { .. } => {
                error!(
                    "Data response {msg_id:?}, from {}, has been dropped since it's not \
                    meant to be handled this way (it is directly forwarded to client): {other:?}",
                    sender.addr()
                );
                Ok(vec![])
            }
        }
    }

    /// Utility to split a list of nodes between others and ourself.
    pub(crate) fn split_nodes_and_self(
        &self,
        all_nodes: Vec<NodeId>,
    ) -> (BTreeSet<NodeId>, Option<NodeId>) {
        let our_name = self.info().name();
        let (nodes, ourself): (BTreeSet<_>, BTreeSet<_>) = all_nodes
            .into_iter()
            .partition(|node_id| node_id.name() != our_name);
        let optional_self = ourself.into_iter().next();
        (nodes, optional_self)
    }
}
