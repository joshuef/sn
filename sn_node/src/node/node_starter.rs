// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::comm::Comm;
use crate::node::{
    cfg::keypair_storage::{get_reward_pk, store_network_keypair, store_new_reward_keypair},
    flow_ctrl::{
        cmds::Cmd,
        dispatcher::Dispatcher,
        event::{Elders, Event, MembershipEvent, NodeElderChange},
        event_channel,
        event_channel::EventReceiver,
        CmdCtrl, FlowCtrl,
    },
    join_network,
    logging::{log_ctx::LogCtx, run_system_logger},
    Config, Error, Node, RateLimits, Result,
};
use crate::UsedSpace;

use sn_interface::{
    network_knowledge::{utils::read_prefix_map_from_disk, NodeInfo, MIN_ADULT_AGE},
    types::{keys::ed25519, log_markers::LogMarker, PublicKey as TypesPublicKey},
};

use rand_07::rngs::OsRng;
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs,
    sync::{mpsc, RwLock},
};
use xor_name::Prefix;

// Filename for storing the content of the genesis DBC.
// The Genesis DBC is generated and owned by the genesis PK of the network's section chain,
// i.e. the very first section key in the chain.
// The first node mints the genesis DBC (as a bearer DBC) and stores it in a file
// named `genesis_dbc`, located at it's configured root dir.
// In current implementation the amount owned by the Genesis DBC is
// set to GENESIS_DBC_AMOUNT (currently 4,525,524,120 * 10^9) individual units.
const GENESIS_DBC_FILENAME: &str = "genesis_dbc";

static EVENT_CHANNEL_SIZE: usize = 20;

pub(crate) type CmdChannel = mpsc::Sender<Cmd>;

/// Test only
pub async fn new_test_api(
    config: &Config,
    join_timeout: Duration,
) -> Result<(super::NodeTestApi, EventReceiver)> {
    let (node, cmd_channel, event_receiver) = new_node(config, join_timeout).await?;
    Ok((super::NodeTestApi::new(node, cmd_channel), event_receiver))
}

/// A reference held to the node to keep it running.
///
/// Meant to be held while looping over the event receiver
/// that transports events from the node.
#[allow(missing_debug_implementations, dead_code)]
pub struct NodeRef {
    node: Arc<RwLock<Node>>,
    /// Sender which can be used to add a Cmd to the Node's CmdQueue
    cmd_channel: CmdChannel,
}

/// Start a new node.
pub async fn start_node(
    config: &Config,
    join_timeout: Duration,
) -> Result<(NodeRef, EventReceiver)> {
    let (node, cmd_channel, event_receiver) = new_node(config, join_timeout).await?;

    Ok((NodeRef { node, cmd_channel }, event_receiver))
}

// Private helper to create a new node using the given config and bootstraps it to the network.
async fn new_node(
    config: &Config,
    join_timeout: Duration,
) -> Result<(Arc<RwLock<Node>>, CmdChannel, EventReceiver)> {
    let root_dir_buf = config.root_dir()?;
    let root_dir = root_dir_buf.as_path();
    fs::create_dir_all(root_dir).await?;

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

    let (node, cmd_channel, network_events) =
        bootstrap_node(config, used_space, root_dir, join_timeout).await?;

    {
        let read_only_node = node.read().await;

        // Network keypair may have to be changed due to naming criteria or network requirements.
        let keypair_as_bytes = read_only_node.keypair.to_bytes();
        store_network_keypair(root_dir, keypair_as_bytes).await?;

        let our_pid = std::process::id();
        let node_prefix = read_only_node.network_knowledge().prefix();
        let node_name = read_only_node.info().name();
        let node_age = read_only_node.info().age();
        let our_conn_info = read_only_node.info().addr;
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
    }

    run_system_logger(LogCtx::new(node.clone()), config.resource_logs).await;

    Ok((node, cmd_channel, network_events))
}

// Private helper to create a new node using the given config and bootstraps it to the network.
async fn bootstrap_node(
    config: &Config,
    used_space: UsedSpace,
    root_storage_dir: &Path,
    join_timeout: Duration,
) -> Result<(Arc<RwLock<Node>>, CmdChannel, EventReceiver)> {
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

        // Generate the genesis key, this will be the first key in the sections chain,
        // as well as the owner of the genesis DBC minted by this first node of the network.
        let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
        let (node, genesis_dbc) = Node::first_node(
            comm.socket_addr(),
            Arc::new(keypair),
            event_sender.clone(),
            used_space.clone(),
            root_storage_dir.to_path_buf(),
            genesis_sk_set,
        )
        .await?;

        // Write the genesis DBC to disk
        let path = root_storage_dir.join(GENESIS_DBC_FILENAME);
        fs::write(path, genesis_dbc.to_hex().unwrap()).await?;

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
        let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE);
        let node_name = ed25519::name(&keypair.public);
        info!("{} Bootstrapping as a new node.", node_name);

        let prefix_map = read_prefix_map_from_disk().await?;
        let section_elders = {
            let sap = prefix_map
                .closest_or_opposite(&xor_name::rand::random(), None)
                .ok_or_else(|| Error::Configuration("Could not obtain closest SAP".to_string()))?;
            sap.elders_vec()
        };
        let bootstrap_nodes: Vec<SocketAddr> =
            section_elders.iter().map(|node| node.addr()).collect();

        let (comm, bootstrap_addr) = Comm::bootstrap(
            local_addr,
            bootstrap_nodes.as_slice(),
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
            prefix_map.genesis_key()
        );

        let joining_node = NodeInfo::new(keypair, comm.socket_addr());
        let (info, network_knowledge) = join_network(
            joining_node,
            &comm,
            &mut connection_event_rx,
            bootstrap_addr,
            prefix_map,
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

        info!("{} Joined the network!", node.info().name());
        info!("Our AGE: {}", node.info().age());

        (node, comm)
    };

    let node = Arc::new(RwLock::new(node));
    let cmd_ctrl = CmdCtrl::new(
        Dispatcher::new(node.clone(), comm),
        monitoring,
        event_sender,
    );
    let (msg_and_period_ctrl, cmd_channel) = FlowCtrl::new(cmd_ctrl, connection_event_rx);

    let _ = tokio::task::spawn_local(async move {
        msg_and_period_ctrl
            .process_messages_and_periodic_checks()
            .await
    });

    Ok((node, cmd_channel, event_receiver))
}
