// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    api::cmds::Cmd, core::Node, dkg::DkgSessionIdUtils, messages::WireMsgUtils, Result,
};
use sn_interface::messaging::{
    system::{DkgSessionId, SystemMsg},
    DstLocation, WireMsg,
};
use sn_interface::network_knowledge::ElderCandidates;
use sn_interface::types::Peer;

use xor_name::XorName;

impl Node {
    /// Send a `DkgStart` message to the provided set of candidates
    pub(crate) async fn send_dkg_start(
        &self,
        elder_candidates: ElderCandidates,
    ) -> Result<Vec<Cmd>> {
        let src_prefix = elder_candidates.prefix();
        let generation = self.network_knowledge.chain_len().await;
        let session_id = DkgSessionId::new(&elder_candidates, generation);

        // Send DKG start to all candidates
        let recipients: Vec<_> = elder_candidates.elders().cloned().collect();

        trace!(
            "Send DkgStart for {:?} with {:?} to {:?}",
            elder_candidates,
            session_id,
            recipients
        );

        let node_msg = SystemMsg::DkgStart {
            session_id,
            prefix: elder_candidates.prefix(),
            elders: elder_candidates
                .elders()
                .map(|peer| (peer.name(), peer.addr()))
                .collect(),
        };
        let section_pk = self.network_knowledge.section_key().await;
        self.send_msg_for_dst_accumulation(
            src_prefix.name(),
            DstLocation::Section {
                name: src_prefix.name(),
                section_pk,
            },
            node_msg,
            recipients,
        )
        .await
    }

    async fn send_msg_for_dst_accumulation(
        &self,
        src: XorName,
        dst: DstLocation,
        node_msg: SystemMsg,
        recipients: Vec<Peer>,
    ) -> Result<Vec<Cmd>> {
        let section_key = self.network_knowledge.section_key().await;

        let key_share = self
            .section_keys_provider
            .key_share(&section_key)
            .await
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

        Ok(self
            .send_messages_to_all_nodes_or_directly_handle_for_accumulation(recipients, wire_msg)
            .await?)
    }
}
