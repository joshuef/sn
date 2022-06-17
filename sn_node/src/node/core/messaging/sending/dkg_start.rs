// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, messages::WireMsgUtils, Result};

use sn_interface::{
    messaging::{
        system::{DkgSessionId, SystemMsg},
        DstLocation, WireMsg,
    },
    types::{log_markers::LogMarker, Peer},
};

use xor_name::XorName;

impl Node {
    /// Send a `DkgStart` message to the provided set of candidates
    pub(crate) async fn send_dkg_start(&self, session_id: DkgSessionId) -> Result<Vec<Cmd>> {
        // Send DKG start to all candidates
        let recipients = Vec::from_iter(session_id.elder_peers());

        trace!(
            "{} for {:?} with {:?} to {:?}",
            LogMarker::SendDkgStart,
            session_id.elders,
            session_id,
            recipients
        );

        let prefix = session_id.prefix;
        let node_msg = SystemMsg::DkgStart(session_id);
        let section_pk = self.network_knowledge.section_key();
        self.send_msg_for_dst_accumulation(
            prefix.name(),
            DstLocation::Section {
                name: prefix.name(),
                section_pk,
            },
            node_msg,
            recipients,
        )
    }

    fn send_msg_for_dst_accumulation(
        &self,
        src: XorName,
        dst: DstLocation,
        node_msg: SystemMsg,
        recipients: Vec<Peer>,
    ) -> Result<Vec<Cmd>> {
        let section_key = self.network_knowledge.section_key();

        let key_share = self
            .section_keys_provider
            .key_share(&section_key)
            .map_err(|err| {
                trace!(
                    "Can't create message {:?} for accumulation at dst {:?}: {:?}",
                    node_msg,
                    dst,
                    err
                );
                err
            })?;

        let wire_msg = WireMsg::for_dst_accumulation(&key_share, src, dst, node_msg, section_key)?;

        trace!(
            "Send {:?} for accumulation at dst to {:?}",
            wire_msg,
            recipients
        );

        self.send_messages_to_all_nodes_or_directly_handle_for_accumulation(recipients, wire_msg)
    }
}
