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
mod join_network;
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

use qp2p::SendStream;
use sn_interface::{
    messaging::{MsgType, WireMsg},
    types::Peer,
};

use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::RwLock;

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

impl IntoIterator for Peers {
    type Item = Peer;

    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Peers::Single(p) => Box::new(std::iter::once(p)),
            Peers::Multiple(ps) => Box::new(ps.into_iter()),
        }
    }
}

// Message handling
impl MyNode {
    #[instrument(skip(node))]
    pub(crate) async fn handle_msg(
        node: Arc<RwLock<MyNode>>,
        origin: Peer,
        wire_msg: WireMsg,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        // Deserialize the payload of the incoming message
        let msg_id = wire_msg.msg_id();
        trace!("Handling msg {msg_id:?}. Validating first...");

        let msg_type = match wire_msg.into_msg() {
            Ok(msg_type) => msg_type,
            Err(error) => {
                error!("Failed to deserialize message payload ({msg_id:?}): {error:?}");
                return Ok(vec![]);
            }
        };

        trace!("[NODE READ]: Handle msg read lock attempt");
        let context = { node.read().await.context() };
        match msg_type {
            MsgType::Node { dst, msg, .. } => {
                MyNode::handle_node_msg_with_ae_check(
                    node,
                    context,
                    origin,
                    msg,
                    &wire_msg,
                    dst,
                    send_stream,
                )
                .await
            }
            MsgType::Client {
                msg_id,
                msg,
                dst,
                auth,
            } => {
                debug!("Valid client msg {msg_id:?}");

                let Some(send_stream) = send_stream else {
                    return Err(Error::NoClientResponseStream);
                };

                // Check for entropy before we proceed further
                MyNode::check_ae_on_client_msg(
                    context,
                    origin,
                    wire_msg,
                    dst,
                    msg,
                    auth,
                    send_stream,
                )
                .await
            }
            other @ MsgType::ClientDataResponse { .. } => {
                error!(
                    "Client data response {msg_id:?}, from {}, has been dropped since it's not \
                    meant to be handled by a node: {other:?}",
                    origin.addr()
                );
                Ok(vec![])
            }
            other @ MsgType::NodeDataResponse { .. } => {
                error!(
                    "Node data response {msg_id:?}, from {}, has been dropped since it's not \
                    meant to be handled this way (it is directly forwarded to client): {other:?}",
                    origin.addr()
                );
                Ok(vec![])
            }
        }
    }

    /// Utility to split a list of peers between others and ourself
    pub(crate) fn split_peers_and_self(
        &self,
        all_peers: Vec<Peer>,
    ) -> (BTreeSet<Peer>, Option<Peer>) {
        let our_name = self.info().name();
        let (peers, ourself): (BTreeSet<_>, BTreeSet<_>) = all_peers
            .into_iter()
            .partition(|peer| peer.name() != our_name);
        let optional_self = ourself.into_iter().next();
        (peers, optional_self)
    }
}
