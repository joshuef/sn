// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod agreement;
mod anti_entropy;
mod dkg;
mod handover;
mod join;
mod membership;
mod proposal;
mod relocation;
mod resource_proof;
mod service_msgs;
mod signing;
mod system_msgs;
mod update_section;

use crate::node::{flow_ctrl::cmds::Cmd, Node, Result, DATA_QUERY_LIMIT};

use sn_interface::{
    messaging::{
        data::ServiceMsg, system::SystemMsg, BlsShareAuth, Dst, MsgType, NodeMsgAuthority, WireMsg,
    },
    network_knowledge::NetworkKnowledge,
    types::Peer,
};

use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum OutgoingMsg {
    System(SystemMsg),
    Service(ServiceMsg),
    DstAggregated((BlsShareAuth, Bytes)),
}

#[derive(Debug, Clone)]
pub(crate) enum Peers {
    Single(Peer),
    Multiple(BTreeSet<Peer>),
}

impl Peers {
    #[allow(unused)]
    pub(crate) fn get(&self) -> BTreeSet<Peer> {
        match self {
            Self::Single(peer) => BTreeSet::from([*peer]),
            Self::Multiple(peers) => peers.clone(),
        }
    }
}

// Message handling
impl Node {
    #[instrument(skip(self, original_bytes))]
    pub(crate) async fn validate_msg(
        &self,
        origin: Peer,
        wire_msg: WireMsg,
        original_bytes: Bytes,
    ) -> Result<Vec<Cmd>> {
        // Deserialize the payload of the incoming message
        let msg_id = wire_msg.msg_id();
        // payload needed for aggregation
        let wire_msg_payload = wire_msg.payload.clone();

        let msg_type = match wire_msg.into_msg() {
            Ok(msg_type) => msg_type,
            Err(error) => {
                error!(
                    "Failed to deserialize message payload ({:?}): {:?}",
                    msg_id, error
                );
                return Ok(vec![]);
            }
        };

        match msg_type {
            MsgType::System {
                msg_id,
                msg_authority,
                dst,
                msg,
            } => {
                // Verify that the section key in the msg authority is trusted
                if !self.verify_section_key(&msg_authority, &msg).await {
                    warn!(
                        "Untrusted message ({:?}) dropped from {:?}: {:?} ",
                        msg_id, origin, msg
                    );
                    return Ok(vec![]);
                };

                // Check for entropy before we proceed further
                // Anythign returned here means there's an issue and we should
                // short-circuit below
                let cmds = self
                    .apply_ae(
                        &origin,
                        &msg,
                        &wire_msg,
                        &dst,
                        original_bytes.clone(), // a cheap clone w/ Bytes
                    )
                    .await?;

                if !cmds.is_empty() {
                    // short circuit and send those AE responses
                    return Ok(cmds);
                }

                #[cfg(feature = "traceroute")]
                let traceroute = wire_msg.traceroute();

                Ok(vec![Cmd::HandleValidSystemMsg {
                    origin,
                    msg_id,
                    msg,
                    msg_authority,
                    wire_msg_payload,
                    #[cfg(feature = "traceroute")]
                    traceroute,
                }])
            }
            MsgType::Service {
                msg_id,
                msg,
                dst,
                auth,
            } => {
                let dst_name = match msg.dst_address() {
                    Some(name) => name,
                    None => {
                        error!(
                            "Service msg {:?} from {} has been dropped since there is no dst address.",
                            msg_id, origin.addr(),
                        );
                        return Ok(vec![]);
                    }
                };

                if self.is_not_elder() {
                    trace!("Redirecting from adult to section elders");
                    return Ok(vec![
                        self.ae_redirect_to_our_elders(origin, &original_bytes)?
                    ]);
                }

                // First we check if it's query and we have too many on the go at the moment...
                if let ServiceMsg::Query(_) = msg {
                    // we have a query, check if we have too many on the go....
                    if self.pending_data_queries.len() > DATA_QUERY_LIMIT {
                        warn!("Pending queries length exceeded, dropping query {msg:?}");
                        return Ok(vec![]);
                    }
                }

                // TODO: track clients for spam

                // Then we perform AE checks
                if let Some(cmd) =
                    self.check_for_entropy(original_bytes, &dst.section_key, dst_name, &origin)?
                {
                    // short circuit and send those AE responses
                    return Ok(vec![cmd]);
                }

                Ok(vec![Cmd::HandleValidServiceMsg {
                    msg_id,
                    msg,
                    origin,
                    auth,
                    #[cfg(feature = "traceroute")]
                    traceroute: wire_msg.traceroute(),
                }])
            }
        }
    }

    /// Verifies that the section key in the msg authority is trusted
    /// based on our current knowledge of the network and sections chains.
    #[instrument(skip_all)]
    async fn verify_section_key(&self, msg_authority: &NodeMsgAuthority, msg: &SystemMsg) -> bool {
        let known_keys = self.network_knowledge.known_keys();
        NetworkKnowledge::verify_node_msg_can_be_trusted(
            msg_authority.clone(),
            msg.clone(),
            &known_keys,
        )
    }

    /// Check if the origin needs to be updated on network structure/members.
    /// Returns an ae cmd if we need to halt msg validation and update the origin instead.
    #[instrument(skip_all)]
    async fn apply_ae(
        &self,
        origin: &Peer,
        msg: &SystemMsg,
        wire_msg: &WireMsg,
        dst: &Dst,
        msg_bytes: Bytes,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        // Adult nodes don't need to carry out entropy checking,
        // however the message shall always be handled.
        if !self.is_elder() {
            return Ok(cmds);
        }
        // For the case of receiving a join request not matching our prefix,
        // we just let the join request handler to deal with it later on.
        // We also skip AE check on Anti-Entropy messages
        //
        // TODO: consider changing the join and "join as relocated" flows to
        // make use of AntiEntropy retry/redirect responses.
        match msg {
            SystemMsg::AntiEntropyRetry { .. }
            | SystemMsg::AntiEntropyUpdate { .. }
            | SystemMsg::AntiEntropyRedirect { .. }
            | SystemMsg::JoinRequest(_)
            | SystemMsg::JoinAsRelocatedRequest(_) => {
                trace!(
                    "Entropy check skipped for {:?}, handling message directly",
                    wire_msg.msg_id()
                );
                Ok(cmds)
            }
            _ => {
                if let Some(ae_cmd) =
                    self.check_for_entropy(msg_bytes, &dst.section_key, dst.name, origin)?
                {
                    // we want to log issues with any node repeatedly out of sync here...
                    cmds.push(Cmd::TrackNodeIssueInDysfunction {
                        name: origin.name(),
                        issue: sn_dysfunction::IssueType::Knowledge,
                    });

                    cmds.push(ae_cmd);

                    return Ok(cmds);
                }

                trace!(
                    "Entropy check passed. Handling verified msg {:?}",
                    wire_msg.msg_id()
                );

                Ok(cmds)
            }
        }
    }
}
