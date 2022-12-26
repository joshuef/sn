// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, CmdChannel, Error, MyNode, Peer, Result};

use sn_interface::{
    messaging::system::NodeMsg,
    network_knowledge::{SectionAuthorityProvider, SectionsDAG},
};

use ed25519_dalek::PublicKey;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use xor_name::{Prefix, XorName};

use super::{flow_ctrl::cmds::Cmd, messaging::Peers};

/// Test interface for sending and receiving messages to and from other nodes.
///
/// A node is a part of the network that can route messages and be a member of a section or group
/// location. Its methods can be used to send requests and responses as either an individual
/// `Node` or as a part of a section or group location.
#[allow(missing_debug_implementations)]
pub struct NodeTestApi {
    node: Arc<RwLock<MyNode>>,
    cmd_channel: CmdChannel,
}

impl NodeTestApi {
    pub(crate) fn new(node: Arc<RwLock<MyNode>>, cmd_channel: CmdChannel) -> Self {
        Self { node, cmd_channel }
    }

    /// Returns the current age of this node.
    pub async fn age(&self) -> u8 {
        self.node.read().await.info().age()
    }

    /// Returns the current NodeContext
    pub async fn context(&self) -> NodeContext {
        self.node.read().await.context()
    }

    /// Returns the ed25519 public key of this node.
    pub async fn public_key(&self) -> PublicKey {
        self.node.read().await.keypair.public
    }

    /// The name of this node.
    pub async fn name(&self) -> XorName {
        self.node.read().await.info().name()
    }

    /// Returns connection info of this node.
    pub async fn our_connection_info(&self) -> SocketAddr {
        self.node.read().await.info().addr
    }

    /// Returns the Section Signed Chain
    pub async fn section_chain(&self) -> SectionsDAG {
        self.node.read().await.section_chain()
    }

    /// Returns the Section Chain's genesis key
    pub async fn genesis_key(&self) -> bls::PublicKey {
        *self.node.read().await.network_knowledge().genesis_key()
    }

    /// Prefix of our section
    pub async fn our_prefix(&self) -> Prefix {
        self.node.read().await.network_knowledge().prefix()
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.node.read().await.is_elder()
    }

    /// Returns the information of all the current section elders.
    pub async fn our_elders(&self) -> BTreeSet<Peer> {
        self.node.read().await.network_knowledge().elders()
    }

    /// Returns the information of all the current section adults.
    pub async fn our_adults(&self) -> BTreeSet<Peer> {
        self.node.read().await.network_knowledge().adults()
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        let context = &self.node.read().await.context();
        context.section_sap_matching_name(name)
    }

    /// Send a system msg.
    pub async fn send(&self, msg: NodeMsg, recipients: BTreeSet<Peer>) -> Result<()> {
        let cmd = Cmd::send_msg(msg, Peers::Multiple(recipients), self.context().await);
        self.send_cmd(cmd).await
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    async fn send_cmd(&self, cmd: Cmd) -> Result<()> {
        self.cmd_channel
            .send((cmd, vec![]))
            .await
            .map_err(|_| Error::CmdChannelSendError)?;

        Ok(())
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.node.read().await.public_key_set()
    }
}
