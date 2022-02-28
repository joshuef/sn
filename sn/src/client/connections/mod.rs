// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod listeners;
mod messaging;

use sn_interface::messaging::{
    data::{CmdError, OperationId, QueryResponse},
    MsgId,
};
use sn_interface::network_knowledge::prefix_map::NetworkPrefixMap;
use sn_interface::types::PeerLinks;

use dashmap::DashMap;
use qp2p::Endpoint;
use std::sync::Arc;
use tokio::{
    sync::{mpsc::Sender, RwLock},
    time::Duration,
};

// Here we dont track the msg_id across the network, but just use it as a local identifier to remove the correct listener
type PendingQueryResponses = Arc<DashMap<OperationId, Vec<(MsgId, QueryResponseSender)>>>;
type QueryResponseSender = Sender<QueryResponse>;

type CmdResponse = (std::net::SocketAddr, Option<CmdError>);
type PendingCmdAcks = Arc<DashMap<MsgId, Sender<CmdResponse>>>;

#[derive(Debug)]
pub struct QueryResult {
    pub response: QueryResponse,
    pub operation_id: OperationId,
}

#[derive(Clone, Debug)]
pub(super) struct Session {
    // Session endpoint.
    endpoint: Endpoint,
    // Channels for sending responses to upper layers
    pending_queries: PendingQueryResponses,
    // Channels for sending errors to upper layer
    #[allow(dead_code)]
    incoming_err_sender: Arc<Sender<CmdError>>,
    // Channels for sending CmdAck to upper layers
    pending_cmds: PendingCmdAcks,
    /// All elders we know about from AE messages
    network: Arc<NetworkPrefixMap>,
    /// Network's genesis key
    genesis_key: bls::PublicKey,
    /// Initial network comms MsgId
    initial_connection_check_msg_id: Arc<RwLock<Option<MsgId>>>,
    /// Standard time to await potential AE messages:
    cmd_ack_wait: Duration,
    /// Links to nodes
    peer_links: PeerLinks,
}
