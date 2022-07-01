// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(crate) mod cmds;
mod dispatcher;
pub(super) mod event;
pub(super) mod event_channel;
pub(super) mod flow_ctrl;
#[cfg(test)]
pub(crate) mod tests;

use self::{
    cmds::Cmd,
    dispatcher::Dispatcher,
    event::{Elders, Event, MembershipEvent, NodeElderChange},
    event_channel::EventReceiver,
    flow_ctrl::{CmdCtrl, FlowCtrl},
};
use crate::comm::Comm;
use crate::node::{
    cfg::keypair_storage::{get_reward_pk, store_network_keypair, store_new_reward_keypair},
    core::{join_network, Node, RateLimits},
    error::{Error, Result},
    logging::{log_ctx::LogCtx, run_system_logger},
    messages::WireMsgUtils,
    Config, Peer,
};
use crate::UsedSpace;

use sn_interface::{
    messaging::{system::SystemMsg, DstLocation, WireMsg},
    network_knowledge::{NodeInfo, SectionAuthorityProvider, MIN_ADULT_AGE},
    types::{keys::ed25519, log_markers::LogMarker, PublicKey as TypesPublicKey},
};

use ed25519_dalek::PublicKey;
use itertools::Itertools;
use rand_07::rngs::OsRng;
use secured_linked_list::SecuredLinkedList;
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use xor_name::{Prefix, XorName};

/// Interface for sending and receiving messages to and from other nodes, in the role of a full
/// routing node.
///
/// A node is a part of the network that can route messages and be a member of a section or group
/// location. Its methods can be used to send requests and responses as either an individual
/// `Node` or as a part of a section or group location. Their `src` argument indicates that
/// role, and can be `use sn_interface::messaging::SrcLocation::Node` or `use sn_interface::messaging::SrcLocation::Section`.
#[allow(missing_debug_implementations)]
pub struct NodeApi {
    node: Arc<Node>,
    flow_ctrl: FlowCtrl,
}

static EVENT_CHANNEL_SIZE: usize = 20;

impl NodeApi {
    ////////////////////////////////////////////////////////////////////////////
    // Public API
    ////////////////////////////////////////////////////////////////////////////

    /// Initialize a new node.
    pub async fn new(config: &Config, join_timeout: Duration) -> Result<(Self, EventReceiver)> {
        let root_dir_buf = config.root_dir()?;
        let root_dir = root_dir_buf.as_path();
        tokio::fs::create_dir_all(root_dir).await?;

        let _reward_key = match get_reward_pk(root_dir).await? {
            Some(public_key) => TypesPublicKey::Ed25519(public_key),
            None => {
                let mut rng = OsRng;
                let keypair = ed25519_dalek::Keypair::generate(&mut rng);
                store_new_reward_keypair(root_dir, &keypair).await?;
                TypesPublicKey::Ed25519(keypair.public)
            }
        };

        let used_space = UsedSpace::new(config.max_capacity());

        let (api, network_events) =
            Self::start_node(config, used_space, root_dir, join_timeout).await?;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        let keypair_as_bytes = api.node.keypair.read().await.to_bytes();
        store_network_keypair(root_dir, keypair_as_bytes).await?;

        let our_pid = std::process::id();
        let node_prefix = api.our_prefix().await;
        let node_name = api.name().await;
        let node_age = api.age().await;
        let our_conn_info = api.our_connection_info().await;
        let our_conn_info_json = serde_json::to_string(&our_conn_info)
            .unwrap_or_else(|_| "Failed to serialize connection info".into());
        println!(
            "Node PID: {:?}, prefix: {:?}, name: {:?}, age: {}, connection info:\n{}",
            our_pid, node_prefix, node_name, node_age, our_conn_info_json,
        );
        info!(
            "Node PID: {:?}, prefix: {:?}, name: {:?}, age: {}, connection info: {}",
            our_pid, node_prefix, node_name, node_age, our_conn_info_json,
        );

        run_system_logger(LogCtx::new(api.node.clone()), config.resource_logs).await;

        Ok((api, network_events))
    }

    // Private helper to create a new node using the given config and bootstraps it to the network.
    async fn start_node(
        config: &Config,
        used_space: UsedSpace,
        root_storage_dir: &Path,
        join_timeout: Duration,
    ) -> Result<(Self, EventReceiver)> {
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(1);

        let local_addr = config
            .local_addr
            .unwrap_or_else(|| SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)));

        let monitoring = RateLimits::new();
        let (event_sender, event_receiver) = event_channel::new(EVENT_CHANNEL_SIZE);

        let (node, comm) = if config.is_first() {
            // Genesis node having a fix age of 255.
            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), 255);
            let node_name = ed25519::name(&keypair.public);

            info!(
                "{} Starting a new network as the genesis node (PID: {}).",
                node_name,
                std::process::id()
            );

            let comm = Comm::first_node(
                local_addr,
                config.network_config().clone(),
                monitoring.clone(),
                connection_event_tx,
            )
            .await?;

            let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let node = Node::first_node(
                comm.socket_addr(),
                Arc::new(keypair),
                event_sender.clone(),
                used_space.clone(),
                root_storage_dir.to_path_buf(),
                genesis_sk_set,
            )
            .await?;

            let network_knowledge = node.network_knowledge();

            let elders = Elders {
                prefix: network_knowledge.prefix(),
                key: network_knowledge.section_key(),
                remaining: BTreeSet::new(),
                added: network_knowledge.authority_provider().names(),
                removed: BTreeSet::new(),
            };

            info!("{}", LogMarker::PromotedToElder);
            node.send_event(Event::Membership(MembershipEvent::EldersChanged {
                elders,
                self_status_change: NodeElderChange::Promoted,
            }))
            .await;

            let genesis_key = network_knowledge.genesis_key();
            info!(
                "{} Genesis node started!. Genesis key {:?}, hex: {}",
                node_name,
                genesis_key,
                hex::encode(genesis_key.to_bytes())
            );

            (node, comm)
        } else {
            let genesis_key_str = config.genesis_key.as_ref().ok_or_else(|| {
                Error::Configuration("Network's genesis key was not provided.".to_string())
            })?;
            let genesis_key = TypesPublicKey::bls_from_hex(genesis_key_str)?
                .bls()
                .ok_or_else(|| {
                    Error::Configuration(
                        "Unexpectedly failed to obtain genesis key from configuration.".to_string(),
                    )
                })?;

            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE);
            let node_name = ed25519::name(&keypair.public);
            info!("{} Bootstrapping as a new node.", node_name);

            let (comm, bootstrap_addr) = Comm::bootstrap(
                local_addr,
                config
                    .hard_coded_contacts
                    .iter()
                    .copied()
                    .collect_vec()
                    .as_slice(),
                config.network_config().clone(),
                monitoring.clone(),
                connection_event_tx,
            )
            .await?;
            info!(
                "{} Joining as a new node (PID: {}) our socket: {}, bootstrapper was: {}, network's genesis key: {:?}",
                node_name,
                std::process::id(),
                comm.socket_addr(),
                bootstrap_addr,
                genesis_key
            );

            let joining_node = NodeInfo::new(keypair, comm.socket_addr());
            let (info, network_knowledge) = join_network(
                joining_node,
                &comm,
                &mut connection_event_rx,
                bootstrap_addr,
                genesis_key,
                join_timeout,
            )
            .await?;

            let node = Node::new(
                comm.socket_addr(),
                info.keypair.clone(),
                network_knowledge,
                None,
                event_sender.clone(),
                used_space.clone(),
                root_storage_dir.to_path_buf(),
            )
            .await?;

            info!("{} Joined the network!", node.info().await.name());
            info!("Our AGE: {}", node.info().await.age());

            (node, comm)
        };

        let node = Arc::new(node);
        let cmd_ctrl = CmdCtrl::new(
            Dispatcher::new(node.clone(), comm),
            monitoring,
            event_sender,
        );
        let flow_ctrl = FlowCtrl::new(cmd_ctrl, connection_event_rx);
        let api = Self { node, flow_ctrl };

        Ok((api, event_receiver))
    }

    /// Returns the current age of this node.
    pub async fn age(&self) -> u8 {
        self.node.info().await.age()
    }

    /// Returns the ed25519 public key of this node.
    pub async fn public_key(&self) -> PublicKey {
        self.node.keypair.read().await.public
    }

    /// The name of this node.
    pub async fn name(&self) -> XorName {
        self.node.info().await.name()
    }

    /// Returns connection info of this node.
    pub async fn our_connection_info(&self) -> SocketAddr {
        self.node.info().await.addr
    }

    /// Returns the Section Signed Chain
    pub async fn section_chain(&self) -> SecuredLinkedList {
        self.node.section_chain().await
    }

    /// Returns the Section Chain's genesis key
    pub async fn genesis_key(&self) -> bls::PublicKey {
        *self.node.network_knowledge().genesis_key()
    }

    /// Prefix of our section
    pub async fn our_prefix(&self) -> Prefix {
        self.node.network_knowledge().prefix()
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.node.is_elder().await
    }

    /// Returns the information of all the current section elders.
    pub async fn our_elders(&self) -> Vec<Peer> {
        self.node.network_knowledge().elders()
    }

    /// Returns the information of all the current section adults.
    pub async fn our_adults(&self) -> Vec<Peer> {
        self.node.network_knowledge().adults()
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.node.matching_section(name).await
    }

    /// Builds a WireMsg signed by this Node
    pub async fn sign_single_src_msg(
        &self,
        node_msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src_section_pk = *self.section_chain().await.last_key();
        WireMsg::single_src(&self.node.info().await, dst, node_msg, src_section_pk)
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    pub async fn send_msg_to_nodes(&self, wire_msg: WireMsg) -> Result<()> {
        trace!(
            "{:?} {:?}",
            LogMarker::DispatchSendMsgCmd,
            wire_msg.msg_id()
        );

        if let Some(cmd) = self.node.send_msg_to_nodes(wire_msg).await? {
            self.flow_ctrl.fire_and_forget(cmd).await?;
        }

        Ok(())
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.node.public_key_set().await
    }
}
