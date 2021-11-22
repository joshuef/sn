// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(test)]
pub(crate) mod tests;

pub(crate) mod command;

pub(super) mod config;
mod dispatcher;
pub(super) mod event;
pub(super) mod event_stream;

use self::{
    command::Command,
    config::Config,
    dispatcher::Dispatcher,
    event::{Elders, Event, NodeElderChange},
    event_stream::EventStream,
};
use crate::dbs::UsedSpace;
use crate::messaging::{system::SystemMsg, DstLocation, WireMsg};
use crate::routing::{
    core::{join_network, Comm, ConnectionEvent, Core},
    ed25519,
    error::{Error, Result},
    log_markers::LogMarker,
    messages::WireMsgUtils,
    network_knowledge::SectionAuthorityProvider,
    node::Node,
    Peer, MIN_ADULT_AGE,
};
use ed25519_dalek::{PublicKey, Signature, Signer, KEYPAIR_LENGTH};

use crate::types::PublicKey as TypesPublicKey;
use itertools::Itertools;
use secured_linked_list::SecuredLinkedList;
use std::path::PathBuf;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};
use tokio::{sync::mpsc, task};
use xor_name::{Prefix, XorName};

/// Interface for sending and receiving messages to and from other nodes, in the role of a full
/// routing node.
///
/// A node is a part of the network that can route messages and be a member of a section or group
/// location. Its methods can be used to send requests and responses as either an individual
/// `Node` or as a part of a section or group location. Their `src` argument indicates that
/// role, and can be `crate::messaging::SrcLocation::Node` or `crate::messaging::SrcLocation::Section`.
#[allow(missing_debug_implementations)]
pub struct Routing {
    dispatcher: Arc<Dispatcher>,
}

static EVENT_CHANNEL_SIZE: usize = 20;

impl Routing {
    ////////////////////////////////////////////////////////////////////////////
    // Public API
    ////////////////////////////////////////////////////////////////////////////

    /// Creates new node using the given config and bootstraps it to the network.
    ///
    /// NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
    /// lost in transit during bootstrapping, or other reasons. It's the responsibility of the
    /// caller to handle this case, for example by using a timeout.
    pub async fn new(
        config: Config,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
    ) -> Result<(Self, EventStream)> {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_SIZE);
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(1);

        let core = if config.first {
            // Genesis node having a fix age of 255.
            let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), 255);
            let node_name = ed25519::name(&keypair.public);

            info!(
                "{} Starting a new network as the genesis node (PID: {}).",
                node_name,
                std::process::id()
            );

            let comm = Comm::new(
                config.local_addr,
                config.network_config,
                connection_event_tx,
            )
            .await?;
            let node = Node::new(keypair, comm.our_connection_info());

            let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
            let core = Core::first_node(
                comm,
                node,
                event_tx,
                used_space,
                root_storage_dir.clone(),
                genesis_sk_set,
            )
            .await?;

            let network_knowledge = core.network_knowledge();

            let elders = Elders {
                prefix: network_knowledge.prefix().await,
                key: network_knowledge.section_key().await,
                remaining: BTreeSet::new(),
                added: network_knowledge.authority_provider().await.names(),
                removed: BTreeSet::new(),
            };

            trace!("{}", LogMarker::PromotedToElder);
            core.send_event(Event::EldersChanged {
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

            core
        } else {
            let genesis_key_str = config.genesis_key.ok_or_else(|| {
                Error::Configuration("Network's genesis key was not provided.".to_string())
            })?;
            let genesis_key = TypesPublicKey::bls_from_hex(&genesis_key_str)?
                .bls()
                .ok_or_else(|| {
                    Error::Configuration(
                        "Unexpectedly failed to obtain genesis key from configuration.".to_string(),
                    )
                })?;

            let keypair = config.keypair.unwrap_or_else(|| {
                ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE)
            });
            let node_name = ed25519::name(&keypair.public);
            info!("{} Bootstrapping a new node.", node_name);

            let (comm, bootstrap_peer) = Comm::bootstrap(
                config.local_addr,
                config
                    .bootstrap_nodes
                    .iter()
                    .copied()
                    .collect_vec()
                    .as_slice(),
                config.network_config,
                connection_event_tx,
            )
            .await?;
            info!(
                "{} Joining as a new node (PID: {}) our socket: {}, bootstrapper was: {}, network's genesis key: {:?}",
                node_name,
                std::process::id(),
                comm.our_connection_info(),
                bootstrap_peer.addr(),
                genesis_key
            );

            let joining_node = Node::new(keypair, comm.our_connection_info());
            let (node, network_knowledge) = join_network(
                joining_node,
                &comm,
                &mut connection_event_rx,
                bootstrap_peer,
                genesis_key,
            )
            .await?;

            let core = Core::new(
                comm,
                node,
                network_knowledge,
                None,
                event_tx,
                used_space,
                root_storage_dir.to_path_buf(),
                false,
            )
            .await?;
            info!("{} Joined the network!", core.node.read().await.name());
            info!("Our AGE: {}", core.node.read().await.age());

            core
        };

        let dispatcher = Arc::new(Dispatcher::new(core));
        let event_stream = EventStream::new(event_rx);

        // Start listening to incoming connections.
        let _handle = task::spawn(handle_connection_events(
            dispatcher.clone(),
            connection_event_rx,
        ));

        dispatcher.clone().start_network_probing().await;
        dispatcher.clone().write_prefixmap_to_disk().await;

        let routing = Self { dispatcher };

        Ok((routing, event_stream))
    }

    /// Sets the JoinsAllowed flag.
    pub async fn set_joins_allowed(&self, joins_allowed: bool) -> Result<()> {
        let command = Command::SetJoinsAllowed(joins_allowed);
        self.dispatcher
            .clone()
            .enqueue_and_handle_next_command_and_any_offshoots(command, None)
            .await
    }

    /// Signals the Elders of our section to test connectivity to a node.
    pub async fn start_connectivity_test(&self, name: XorName) -> Result<()> {
        let command = Command::StartConnectivityTest(name);
        self.dispatcher
            .clone()
            .enqueue_and_handle_next_command_and_any_offshoots(command, None)
            .await
    }

    /// Returns the current age of this node.
    pub async fn age(&self) -> u8 {
        self.dispatcher.core.node.read().await.age()
    }

    /// Returns the ed25519 public key of this node.
    pub async fn public_key(&self) -> PublicKey {
        self.dispatcher.core.node.read().await.keypair.public
    }

    /// Returns the ed25519 keypair of this node, as bytes.
    pub async fn keypair_as_bytes(&self) -> [u8; KEYPAIR_LENGTH] {
        self.dispatcher.core.node.read().await.keypair.to_bytes()
    }

    /// Signs `data` with the ed25519 key of this node.
    pub async fn sign_as_node(&self, data: &[u8]) -> Signature {
        self.dispatcher.core.node.read().await.keypair.sign(data)
    }

    /// Signs `data` with the BLS secret key share of this node, if it has any. Returns
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn sign_as_elder(
        &self,
        data: &[u8],
        public_key: &bls::PublicKey,
    ) -> Result<(usize, bls::SignatureShare)> {
        self.dispatcher
            .core
            .sign_with_section_key_share(data, public_key)
            .await
    }

    /// Verifies `signature` on `data` with the ed25519 public key of this node.
    pub async fn verify(&self, data: &[u8], signature: &Signature) -> bool {
        self.dispatcher
            .core
            .node
            .read()
            .await
            .keypair
            .verify(data, signature)
            .is_ok()
    }

    /// The name of this node.
    pub async fn name(&self) -> XorName {
        self.dispatcher.core.node.read().await.name()
    }

    /// Returns connection info of this node.
    pub async fn our_connection_info(&self) -> SocketAddr {
        self.dispatcher.core.our_connection_info()
    }

    /// Returns the Section Signed Chain
    pub async fn section_chain(&self) -> SecuredLinkedList {
        self.dispatcher.core.section_chain().await
    }

    /// Returns the Section Chain's genesis key
    pub async fn genesis_key(&self) -> bls::PublicKey {
        *self.dispatcher.core.network_knowledge().genesis_key()
    }

    /// Prefix of our section
    pub async fn our_prefix(&self) -> Prefix {
        self.dispatcher.core.network_knowledge().prefix().await
    }

    /// Finds out if the given XorName matches our prefix.
    pub async fn matches_our_prefix(&self, name: &XorName) -> bool {
        self.our_prefix().await.matches(name)
    }

    /// Returns whether the node is Elder.
    pub async fn is_elder(&self) -> bool {
        self.dispatcher.core.is_elder().await
    }

    /// Returns the information of all the current section elders.
    pub async fn our_elders(&self) -> Vec<Peer> {
        self.dispatcher
            .core
            .network_knowledge()
            .authority_provider()
            .await
            .elders_vec()
    }

    /// Returns the elders of our section sorted by their distance to `name` (closest first).
    pub async fn our_elders_sorted_by_distance_to(&self, name: &XorName) -> Vec<Peer> {
        self.our_elders()
            .await
            .into_iter()
            .sorted_by(|lhs, rhs| name.cmp_distance(&lhs.name(), &rhs.name()))
            .collect()
    }

    /// Returns the information of all the current section adults.
    pub async fn our_adults(&self) -> Vec<Peer> {
        self.dispatcher.core.network_knowledge().adults().await
    }

    /// Returns the adults of our section sorted by their distance to `name` (closest first).
    /// If we are not elder or if there are no adults in the section, returns empty vec.
    pub async fn our_adults_sorted_by_distance_to(&self, name: &XorName) -> Vec<Peer> {
        self.our_adults()
            .await
            .into_iter()
            .sorted_by(|lhs, rhs| name.cmp_distance(&lhs.name(), &rhs.name()))
            .collect()
    }

    /// Returns the info about the section matching the name.
    pub async fn matching_section(&self, name: &XorName) -> Result<SectionAuthorityProvider> {
        self.dispatcher.core.matching_section(name).await
    }

    /// Builds a WireMsg signed by this Node
    pub async fn sign_single_src_msg(
        &self,
        node_msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src_section_pk = *self.section_chain().await.last_key();
        WireMsg::single_src(
            &self.dispatcher.core.node.read().await.clone(),
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Builds a WireMsg signed for accumulateion at destination
    pub async fn sign_msg_for_dst_accumulation(
        &self,
        node_msg: SystemMsg,
        dst: DstLocation,
    ) -> Result<WireMsg> {
        let src = self.name().await;
        let src_section_pk = *self.section_chain().await.last_key();

        WireMsg::for_dst_accumulation(
            &self.dispatcher.core.key_share().await.map_err(|err| err)?,
            src,
            dst,
            node_msg,
            src_section_pk,
        )
    }

    /// Send a message.
    /// Messages sent here, either section to section or node to node.
    pub async fn send_message(&self, wire_msg: WireMsg) -> Result<()> {
        trace!(
            "{:?} {:?}",
            LogMarker::DispatchSendMsgCmd,
            wire_msg.msg_id()
        );
        self.dispatcher
            .clone()
            .enqueue_and_handle_next_command_and_any_offshoots(
                Command::ParseAndSendWireMsg(wire_msg),
                None,
            )
            .await
    }

    /// Returns the current BLS public key set if this node has one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn public_key_set(&self) -> Result<bls::PublicKeySet> {
        self.dispatcher.core.public_key_set().await
    }

    /// Returns our index in the current BLS group if this node is a member of one, or
    /// `Error::MissingSecretKeyShare` otherwise.
    pub async fn our_index(&self) -> Result<usize> {
        self.dispatcher.core.our_index().await
    }
}

// Listen for incoming connection events and handle them.
async fn handle_connection_events(
    dispatcher: Arc<Dispatcher>,
    mut incoming_conns: mpsc::Receiver<ConnectionEvent>,
) {
    while let Some(event) = incoming_conns.recv().await {
        match event {
            ConnectionEvent::Received((sender, bytes)) => {
                trace!(
                    "New message ({} bytes) received from: {:?}",
                    bytes.len(),
                    sender
                );

                // bytes.clone is cheap
                let wire_msg = match WireMsg::from(bytes.clone()) {
                    Ok(wire_msg) => wire_msg,
                    Err(error) => {
                        error!("Failed to deserialize message header: {:?}", error);
                        continue;
                    }
                };

                let span = {
                    let core = &dispatcher.core;
                    trace_span!("handle_message", name = %core.node.read().await.name(), ?sender, msg_id = ?wire_msg.msg_id())
                };
                let _span_guard = span.enter();

                trace!(
                    "{:?} from {:?} length {}",
                    LogMarker::DispatchHandleMsgCmd,
                    sender,
                    bytes.len(),
                );
                let command = Command::HandleMessage {
                    sender,
                    wire_msg,
                    original_bytes: Some(bytes),
                };

                let _handle = dispatcher
                    .clone()
                    .enqueue_and_handle_next_command_and_any_offshoots(command, None)
                    .await;
            }
        }
    }

    error!("Fatal error, the stream for incoming connections has been unexpectedly closed. No new connections or messages can be received from the network from here on.");
}
