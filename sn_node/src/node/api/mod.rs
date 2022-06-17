// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub(crate) mod cmds;
#[cfg(test)]
pub(crate) mod tests;

pub(super) mod dispatcher;
pub(super) mod event;
pub(super) mod event_stream;

use self::{
    cmds::Cmd,
    dispatcher::Dispatcher,
    event::{Elders, Event, NodeElderChange},
    event_stream::EventStream,
};

use crate::node::{
    cfg::keypair_storage::{get_reward_pk, store_network_keypair, store_new_reward_keypair},
    core::{join_network, Comm, MsgEvent, Node},
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
use tokio::{sync::mpsc, task};
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
    dispatcher: Arc<Dispatcher>,
}

static EVENT_CHANNEL_SIZE: usize = 20;

impl NodeApi {
    ////////////////////////////////////////////////////////////////////////////
    // Public API
    ////////////////////////////////////////////////////////////////////////////

    /// Initialize a new node.
    pub async fn new(config: &Config, joining_timeout: Duration) -> Result<(Self, EventStream)> {
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

        let (api, network_events) = tokio::time::timeout(
            joining_timeout,
            Self::start_node(config, used_space, root_dir),
        )
        .await
        .map_err(|_| Error::JoinTimeout)??;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        let keypair_as_bytes = api.dispatcher.node.info.borrow().keypair.to_bytes();
        store_network_keypair(root_dir, keypair_as_bytes).await?;

        let our_pid = std::process::id();
        let node_prefix = api.our_prefix();
        let node_name = api.name();
        let node_age = api.age();
        let our_conn_info = api.our_connection_info();
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

        run_system_logger(LogCtx::new(api.dispatcher.clone()), config.resource_logs).await;

        Ok((api, network_events))
    }

    // Private helper to create a new node using the given config and bootstraps it to the network.
    //
    // NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
    // lost in transit during bootstrapping, or other reasons. It's the responsibility of the
    // caller to handle this case, for example by using a timeout.
    async fn start_node(
        config: &Config,
        used_space: UsedSpace,
        root_storage_dir: &Path,
    ) -> Result<(Self, EventStream)> {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_SIZE);
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(1);

        let local_addr = config
            .local_addr
            .unwrap_or_else(|| SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)));

        let node = if config.is_first() {
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
                connection_event_tx,
            )
            .await?;
            let info = NodeInfo::new(keypair, comm.our_connection_info());

            let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let node = Node::first_node(
                comm,
                info,
                event_tx,
                used_space.clone(),
                root_storage_dir.to_path_buf(),
                genesis_sk_set,
            )?;

            let network_knowledge = node.network_knowledge();

            let elders = Elders {
                prefix: network_knowledge.prefix(),
                key: network_knowledge.section_key(),
                remaining: BTreeSet::new(),
                added: network_knowledge.authority_provider().names(),
                removed: BTreeSet::new(),
            };

            info!("{}", LogMarker::PromotedToElder);
            node.send_event(Event::EldersChanged {
                elders,
                self_status_change: NodeElderChange::Promoted,
            })
            .await;

            let genesis_key = network_knowledge.genesis_key();
            info!(
                "{} Genesis node started!. Genesis key {:?}, hex: {}",
                node_name,
                genesis_key,
                hex::encode(genesis_key.to_bytes())
            );

            node
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
                connection_event_tx,
            )
            .await?;
            info!(
                "{} Joining as a new node (PID: {}) our socket: {}, bootstrapper was: {}, network's genesis key: {:?}",
                node_name,
                std::process::id(),
                comm.our_connection_info(),
                bootstrap_addr,
                genesis_key
            );

            let joining_node = NodeInfo::new(keypair, comm.our_connection_info());
            let (info, network_knowledge) = join_network(
                joining_node,
                &comm,
                &mut connection_event_rx,
                bootstrap_addr,
                genesis_key,
            )
            .await?;

            let node = Node::new(
                comm,
                info,
                network_knowledge,
                None,
                event_tx,
                used_space.clone(),
                root_storage_dir.to_path_buf(),
            )?;
            info!("{} Joined the network!", node.info.borrow().name());
            info!("Our AGE: {}", node.info.borrow().age());

            node
        };

        let dispatcher = Arc::new(Dispatcher::new(node));
        let event_stream = EventStream::new(event_rx);

        // Start listening to incoming connections.
        let _handle = task::spawn_local(handle_connection_events(
            dispatcher.clone(),
            connection_event_rx,
        ));

        dispatcher.clone().start_network_probing().await;
        dispatcher.clone().start_section_probing().await;
        dispatcher
            .clone()
            .check_for_dysfunction_periodically()
            .await;
        dispatcher.clone().start_sending_any_data_batches().await;

        #[cfg(feature = "back-pressure")]
        dispatcher
            .clone()
            .report_backpressure_to_our_section_periodically()
            .await;

        dispatcher.clone().start_cleaning_peer_links().await;
        dispatcher.clone().write_prefixmap_to_disk().await;

        let api = Self { dispatcher };

        Ok((api, event_stream))
    }

    /// Returns the current age of this node.
    pub fn age(&self) -> u8 {
        self.dispatcher.node.info.borrow().age()
    }

    /// Returns the ed25519 public key of this node.
    pub fn public_key(&self) -> PublicKey {
        self.dispatcher.node.info.borrow().keypair.public
    }

    /// The name of this node.
    pub fn name(&self) -> XorName {
        self.dispatcher.node.info.borrow().name()
    }

    /// Returns connection info of this node.
    pub fn our_connection_info(&self) -> SocketAddr {
        self.dispatcher.node.our_connection_info()
    }

    /// Returns the Section Signed Chain
    pub fn section_chain(&self) -> SecuredLinkedList {
        self.dispatcher.node.section_chain()
    }

    /// Returns the Section Chain's genesis key
    pub fn genesis_key(&self) -> bls::PublicKey {
        *self.dispatcher.node.network_knowledge().genesis_key()
    }

    /// Prefix of our section
    pub fn our_prefix(&self) -> Prefix {
        self.dispatcher.node.network_knowledge().prefix()
    }

    /// Returns whether the node is Elder.
    pub fn is_elder(&self) -> bool {
        self.dispatcher.node.is_elder()
    }

    /// Returns the information of all the current section elders.
    pub fn our_elders(&self) -> Vec<Peer> {
        self.dispatcher.node.network_knowledge().elders()
    }

    /// Returns the information of all the current section adults.
    pub fn our_adults(&self) -> Vec<Peer> {
        self.dispatcher.node.network_knowledge().adults()
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.dispatcher.node.matching_section(name).await
    }

    /// Builds a WireMsg signed by this Node
    pub fn sign_single_src_msg(&self, node_msg: SystemMsg, dst: DstLocation) -> Result<WireMsg> {
        let src_section_pk = *self.section_chain().last_key();
        WireMsg::single_src(
            &self.dispatcher.node.info.borrow().clone(),
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    pub async fn send_msg_to_nodes(&self, wire_msg: WireMsg) -> Result<()> {
        trace!(
            "{:?} {:?}",
            LogMarker::DispatchSendMsgCmd,
            wire_msg.msg_id()
        );

        if let Some(cmd) = self.dispatcher.node.send_msg_to_nodes(wire_msg).await? {
            self.dispatcher
                .clone()
                .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                .await?;
        }

        Ok(())
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.dispatcher.node.public_key_set()
    }
}

// Listen for incoming connection events and handle them.
async fn handle_connection_events(
    dispatcher: Arc<Dispatcher>,
    mut incoming_conns: mpsc::Receiver<MsgEvent>,
) {
    while let Some(event) = incoming_conns.recv().await {
        match event {
            MsgEvent::Received {
                sender,
                wire_msg,
                original_bytes,
            } => {
                debug!(
                    "New message ({} bytes) received from: {:?}",
                    original_bytes.len(),
                    sender
                );

                let span = {
                    let node = &dispatcher.node;
                    trace_span!("handle_message", name = %node.info.borrow().name(), ?sender, msg_id = ?wire_msg.msg_id())
                };
                let _span_guard = span.enter();

                trace!(
                    "{:?} from {:?} length {}",
                    LogMarker::DispatchHandleMsgCmd,
                    sender,
                    original_bytes.len(),
                );
                let cmd = Cmd::HandleMsg {
                    sender,
                    wire_msg,
                    original_bytes: Some(original_bytes),
                };

                let _handle = dispatcher
                    .clone()
                    .enqueue_and_handle_next_cmd_and_offshoots(cmd, None)
                    .await;
            }
        }
    }

    error!("Fatal error, the stream for incoming connections has been unexpectedly closed. No new connections or messages can be received from the network from here on.");
}
