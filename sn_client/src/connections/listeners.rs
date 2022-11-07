// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{messaging::NUM_OF_ELDERS_SUBSET_FOR_QUERIES, Session};

use crate::{Error, Result};

use qp2p::{RecvStream, UsrMsgBytes};
use sn_interface::{
    at_least_one_correct_elder,
    messaging::{data::ClientMsg, AuthorityProof, ClientAuth, Dst, MsgId, MsgType, WireMsg},
    network_knowledge::{SectionAuthorityProvider, SectionTreeUpdate},
    types::Peer,
};

use itertools::Itertools;
use rand::{rngs::OsRng, seq::SliceRandom};

impl Session {
    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn read_msg_from_recvstream(
        recv_stream: &mut RecvStream,
    ) -> Result<MsgType, Error> {
        let bytes = recv_stream.next().await?;
        let wire_msg = WireMsg::from(bytes)?;
        let msg_type = wire_msg.into_msg()?;

        #[cfg(feature = "traceroute")]
        {
            info!(
                "Message {msg_type} with the Traceroute received at client:\n {:?}",
                wire_msg.traceroute()
            )
        }

        Ok(msg_type)
    }

    /// Update our network knowledge making sure proof chain validates the
    /// new SAP based on currently known remote section SAP or genesis key.
    pub(crate) async fn update_network_knowledge(
        &mut self,
        section_tree_update: SectionTreeUpdate,
        src_peer: Peer,
    ) {
        let sap = section_tree_update.signed_sap.value.clone();
        // Update our network PrefixMap based upon passed in knowledge
        match self.network.write().await.update(section_tree_update) {
            Ok(true) => {
                debug!(
                    "Anti-Entropy: updated remote section SAP updated for {:?}",
                    sap.prefix()
                );
            }
            Ok(false) => {
                debug!(
                    "Anti-Entropy: discarded SAP for {:?} since it's the same as \
                    the one in our records: {sap:?}",
                    sap.prefix()
                );
            }
            Err(err) => {
                warn!(
                    "Anti-Entropy: failed to update remote section SAP and section DAG w/ err: {err:?}"
                );
                warn!(
                    "Anti-Entropy: bounced msg dropped. Failed section auth was {:?} sent by: {src_peer:?}",
                    sap.section_key(),
                );
            }
        }
    }

    /// Checks AE cache to see if we should be forwarding this msg (and to whom)
    /// or if it has already been dealt with
    #[instrument(skip_all, level = "debug")]
    #[allow(clippy::type_complexity)]
    pub(crate) async fn new_target_elders(
        bounced_msg: UsrMsgBytes,
        received_auth: &SectionAuthorityProvider,
    ) -> Result<Option<(MsgId, Vec<Peer>, ClientMsg, Dst, AuthorityProof<ClientAuth>)>, Error> {
        let (msg_id, service_msg, dst, auth) = match WireMsg::deserialize(bounced_msg)? {
            MsgType::Client {
                msg_id,
                msg,
                auth,
                dst,
            } => (msg_id, msg, dst, auth),
            other => {
                warn!("Unexpected non-ClientMsg returned in AE-Redirect response: {other:?}");
                return Ok(None);
            }
        };

        trace!("Bounced msg ({msg_id:?}) received in an AE response: {service_msg:?}");

        let (_target_count, dst_address_of_bounced_msg) = match service_msg.clone() {
            ClientMsg::Cmd(cmd) => (at_least_one_correct_elder(), cmd.dst_name()),
            ClientMsg::Query(query) => (NUM_OF_ELDERS_SUBSET_FOR_QUERIES, query.variant.dst_name()),
            _ => {
                warn!(
                    "Invalid bounced msg {msg_id:?} received in AE response: {service_msg:?}. Msg is of invalid type"
                );
                // Early return with random name as we will discard the msg at the caller func
                return Ok(None);
            }
        };

        let target_public_key;

        // We normally have received auth when we're in AE-Redirect
        let mut target_elders: Vec<_> = {
            target_public_key = received_auth.section_key();

            received_auth
                .elders_vec()
                .into_iter()
                .sorted_by(|lhs, rhs| {
                    dst_address_of_bounced_msg.cmp_distance(&lhs.name(), &rhs.name())
                })
                .collect()
        };
        // shuffle so elders sent to is random for better availability
        target_elders.shuffle(&mut OsRng);

        // Let's rebuild the msg with the updated destination details
        let dst = Dst {
            name: dst.name,
            section_key: target_public_key,
        };

        if !target_elders.is_empty() {
            debug!(
                "Final target elders for resending {msg_id:?}: {service_msg:?} msg \
                are {target_elders:?}"
            );
        }

        Ok(Some((msg_id, target_elders, service_msg, dst, auth)))
    }
}
