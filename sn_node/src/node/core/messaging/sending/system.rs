// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, messages::WireMsgUtils, Error, Result};

use sn_interface::{
    messaging::{
        system::{SectionAuth, SystemMsg},
        AuthKind, DstLocation, WireMsg,
    },
    network_knowledge::NodeState,
    types::{log_markers::LogMarker, Peer},
};

use bls::PublicKey as BlsPublicKey;
use xor_name::XorName;

impl Node {
    /// Send a direct (`SystemMsg`) message to a node in the specified section
    pub(crate) fn send_direct_msg(
        &self,
        recipient: Peer,
        node_msg: SystemMsg,
        section_pk: BlsPublicKey,
    ) -> Result<Cmd> {
        let section_name = recipient.name();
        self.send_direct_msg_to_nodes(vec![recipient], node_msg, section_name, section_pk)
    }

    /// Send a direct (`SystemMsg`) message to a set of nodes in the specified section
    pub(crate) fn send_direct_msg_to_nodes(
        &self,
        recipients: Vec<Peer>,
        node_msg: SystemMsg,
        section_name: XorName,
        section_pk: BlsPublicKey,
    ) -> Result<Cmd> {
        trace!("{}", LogMarker::SendDirectToNodes);
        let our_node = self.info.borrow().clone();
        let our_section_key = self.network_knowledge.section_key();

        let wire_msg = WireMsg::single_src(
            &our_node,
            DstLocation::Section {
                name: section_name,
                section_pk,
            },
            node_msg,
            our_section_key,
        )?;

        Ok(Cmd::SendMsg {
            recipients,
            wire_msg,
        })
    }

    /// Send a `Relocate` message to the specified node
    pub(crate) fn send_relocate(
        &self,
        recipient: Peer,
        node_state: SectionAuth<NodeState>,
    ) -> Result<Cmd> {
        let node_msg = SystemMsg::Relocate(node_state.into_authed_msg());
        let section_pk = self.network_knowledge.section_key();
        self.send_direct_msg(recipient, node_msg, section_pk)
    }

    /// Send a direct (`SystemMsg`) message to all Elders in our section
    pub(crate) fn send_msg_to_our_elders(&self, node_msg: SystemMsg) -> Result<Cmd> {
        let sap = self.network_knowledge.authority_provider();
        let dst_section_pk = sap.section_key();
        let section_name = sap.prefix().name();
        let elders = sap.elders_vec();
        self.send_direct_msg_to_nodes(elders, node_msg, section_name, dst_section_pk)
    }

    // Send the message to all `recipients`. If one of the recipients is us, don't send it over the
    // network but handle it directly (should only be used when accumulation is necessary)
    pub(crate) fn send_messages_to_all_nodes_or_directly_handle_for_accumulation(
        &self,
        recipients: Vec<Peer>,
        mut wire_msg: WireMsg,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let mut others = Vec::new();
        let mut handle = false;

        trace!("Send {:?} to {:?}", wire_msg, recipients);

        let our_name = self.info.borrow().name();
        for recipient in recipients.into_iter() {
            if recipient.name() == our_name {
                match wire_msg.msg_kind() {
                    AuthKind::NodeBlsShare(_) => {
                        // do nothing, continue we should be accumulating this
                        handle = true;
                    }
                    _ => return Err(Error::SendOrHandlingNormalMsg),
                }
            } else {
                others.push(recipient);
            }
        }

        if !others.is_empty() {
            let dst_section_pk = self.section_key_by_name(&others[0].name());
            wire_msg.set_dst_section_pk(dst_section_pk);

            trace!("{}", LogMarker::SendOrHandle);
            cmds.push(Cmd::SendMsg {
                recipients: others,
                wire_msg: wire_msg.clone(),
            });
        }

        if handle {
            wire_msg.set_dst_section_pk(self.network_knowledge.section_key());
            wire_msg.set_dst_xorname(our_name);

            cmds.push(Cmd::HandleMsg {
                sender: Peer::new(our_name, self.our_connection_info()),
                wire_msg,
                original_bytes: None,
            });
        }

        Ok(cmds)
    }
}
