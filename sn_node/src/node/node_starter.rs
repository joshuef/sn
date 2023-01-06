// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::comm::{Comm, ConnectionEvent, MsgFromPeer};
use crate::node::{
    cfg::keypair_storage::{get_reward_pk, store_network_keypair, store_new_reward_keypair},
    flow_ctrl::{
        cmds::Cmd, dispatcher::Dispatcher, fault_detection::FaultsCmd, CmdCtrl, FlowCtrl,
        RejoinNetwork,
    },
    join_network,
    logging::log_system_details,
    Config, Error, MyNode, Result, STANDARD_CHANNEL_SIZE,
};
use crate::UsedSpace;

use sn_interface::{
    network_knowledge::{MyNodeInfo, SectionTree, MIN_ADULT_AGE},
    types::{keys::ed25519, log_markers::LogMarker, PublicKey as TypesPublicKey},
};

use rand_07::rngs::OsRng;
use std::{path::Path, sync::Arc, time::Duration};
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

pub(crate) type CmdChannel = mpsc::Sender<(Cmd, Vec<usize>)>;

/// Test only
pub async fn new_test_api(config: &Config, join_timeout: Duration) -> Result<super::NodeTestApi> {
    let (node, cmd_channel, _) = new_node(config, join_timeout).await?;
    Ok(super::NodeTestApi::new(node, cmd_channel))
}

/// A reference held to the node to keep it running.
///
/// Meant to be held while looping over the event receiver
/// that transports events from the node.
#[allow(missing_debug_implementations, dead_code)]
pub struct NodeRef {
    node: Arc<RwLock<MyNode>>,
    /// Sender which can be used to add a Cmd to the Node's CmdQueue
    cmd_channel: CmdChannel,
}

/// Start a new node.
pub async fn start_node(
    config: &Config,
    join_timeout: Duration,
) -> Result<(NodeRef, mpsc::Receiver<RejoinNetwork>)> {
    let (node, cmd_channel, rejoin_network_rx) = new_node(config, join_timeout).await?;

    Ok((NodeRef { node, cmd_channel }, rejoin_network_rx))
}

// Private helper to create a new node using the given config and bootstraps it to the network.
async fn new_node(
    config: &Config,
    join_timeout: Duration,
) -> Result<(
    Arc<RwLock<MyNode>>,
    CmdChannel,
    mpsc::Receiver<RejoinNetwork>,
)> {
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

    let used_space = UsedSpace::new(config.min_capacity(), config.max_capacity());

    let (node, cmd_channel, rejoin_network_rx) =
        bootstrap_node(config, used_space, root_dir, join_timeout).await?;

    {
        debug!("[NODE WRITE]: new node...");
        let context = node.read().await.context();
        debug!("[NODE WRITE]: new node write got");

        // Network keypair may have to be changed due to naming criteria or network requirements.
        let keypair_as_bytes = context.keypair.to_bytes();
        store_network_keypair(root_dir, keypair_as_bytes).await?;

        let our_pid = std::process::id();
        let node_prefix = context.network_knowledge.prefix();
        let node_name = context.name;
        let node_age = context.info.age();
        let our_conn_info = context.info.addr;
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

        log_system_details(node_prefix);
    }

    Ok((node, cmd_channel, rejoin_network_rx))
}

// Private helper to create a new node using the given config and bootstraps it to the network.
async fn bootstrap_node(
    config: &Config,
    used_space: UsedSpace,
    root_storage_dir: &Path,
    join_timeout: Duration,
) -> Result<(
    Arc<RwLock<MyNode>>,
    CmdChannel,
    mpsc::Receiver<RejoinNetwork>,
)> {
    let (incoming_msg_pipe, mut incoming_msg_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);
    let (fault_cmds_sender, fault_cmds_receiver) =
        mpsc::channel::<FaultsCmd>(STANDARD_CHANNEL_SIZE);

    let (comm, mut unfiltered_connection_receiver) = Comm::new(
        config.local_addr(),
        config.network_config(),
        incoming_msg_pipe,
    )
    .await?;

    let conn_filter_comm = comm.clone();

    let node = if config.is_first() {
        bootstrap_genesis_node(
            comm,
            used_space,
            root_storage_dir,
            fault_cmds_sender.clone(),
        )
        .await?
    } else {
        bootstrap_normal_node(
            config,
            comm,
            &mut incoming_msg_receiver,
            join_timeout,
            used_space,
            root_storage_dir,
            fault_cmds_sender.clone(),
        )
        .await?
    };

    let node = Arc::new(RwLock::new(node));
    let (dispatcher, data_replication_receiver) = Dispatcher::new(node.clone());
    let cmd_ctrl = CmdCtrl::new(dispatcher);
    let (cmd_channel, rejoin_network_rx) = FlowCtrl::start(
        cmd_ctrl,
        incoming_msg_receiver,
        data_replication_receiver,
        (fault_cmds_sender, fault_cmds_receiver),
    )
    .await;

    let filter_node = node.clone();
    // Spawn a task to filter incoming connections and ensure that we only cache our section's connections
    tokio::task::spawn(async move {
        while let Some(event) = unfiltered_connection_receiver.recv().await {
            match event {
                ConnectionEvent::Connected { peer, connection } => {
                    // TODO: some spam control here...
                    let members = filter_node.read().await.network_knowledge.members();
                    if members.contains(&peer) {
                        if let Err(error) = conn_filter_comm
                            .filtered_connection_sender
                            .send(ConnectionEvent::Connected { peer, connection })
                            .await
                        {
                            error!("Could not send filtered connection into comms for caching. {error:?}");
                        }
                    }
                }
                _ => {
                    // do nothing
                }
            }
        }
    });

    Ok((node, cmd_channel, rejoin_network_rx))
}

async fn bootstrap_genesis_node(
    comm: Comm,
    used_space: UsedSpace,
    root_storage_dir: &Path,
    fault_cmds_sender: mpsc::Sender<FaultsCmd>,
) -> Result<MyNode> {
    // Genesis node having a fix age of 255.
    let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), 255);
    let node_name = ed25519::name(&keypair.public);

    info!(
        "{} Starting a new network as the genesis node (PID: {}).",
        node_name,
        std::process::id()
    );

    // Generate the genesis key, this will be the first key in the sections chain,
    // as well as the owner of the genesis DBC minted by this first node of the network.
    let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
    let (node, genesis_dbc) = MyNode::first_node(
        comm,
        Arc::new(keypair),
        used_space.clone(),
        root_storage_dir.to_path_buf(),
        genesis_sk_set,
        fault_cmds_sender,
    )
    .await?;

    // Write the genesis DBC to disk
    let path = root_storage_dir.join(GENESIS_DBC_FILENAME);
    fs::write(path, genesis_dbc.to_hex()?).await?;

    info!("{}", LogMarker::PromotedToElder);

    let genesis_key = node.network_knowledge.genesis_key();
    info!(
        "{} Genesis node started!. Genesis key {:?}, hex: {}",
        node_name,
        genesis_key,
        hex::encode(genesis_key.to_bytes())
    );

    Ok(node)
}

#[allow(clippy::too_many_arguments)]
async fn bootstrap_normal_node(
    config: &Config,
    comm: Comm,
    incoming_msg_receiver: &mut tokio::sync::mpsc::Receiver<MsgFromPeer>,
    join_timeout: Duration,
    used_space: UsedSpace,
    root_storage_dir: &Path,
    fault_cmds_sender: mpsc::Sender<FaultsCmd>,
) -> Result<MyNode> {
    let keypair = ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE);
    let node_name = ed25519::name(&keypair.public);
    info!("{} Bootstrapping as a new node.", node_name);
    let section_tree_path = config.network_contacts_file().ok_or_else(|| {
        Error::Configuration("Could not obtain network contacts file path".to_string())
    })?;
    let section_tree = SectionTree::from_disk(&section_tree_path).await?;
    info!(
        "{} Joining as a new node (PID: {}) our socket: {}, network's genesis key: {:?}",
        node_name,
        std::process::id(),
        comm.socket_addr(),
        section_tree.genesis_key()
    );
    let joining_node = MyNodeInfo::new(keypair, comm.socket_addr());
    let (info, network_knowledge) = join_network(
        joining_node,
        &comm,
        incoming_msg_receiver,
        section_tree,
        join_timeout,
    )
    .await?;
    let node = MyNode::new(
        comm,
        info.keypair.clone(),
        network_knowledge,
        None,
        used_space.clone(),
        root_storage_dir.to_path_buf(),
        fault_cmds_sender,
    )
    .await?;
    info!("{} Joined the network!", node.info().name());
    info!("Our AGE: {}", node.info().age());
    Ok(node)
}
