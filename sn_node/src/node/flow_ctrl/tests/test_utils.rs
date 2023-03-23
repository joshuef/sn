// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::network_builder::TestCommRx;
use crate::{
    node::{
        cfg::create_test_capacity_and_root_storage,
        error::Result as NodeResult,
        flow_ctrl::{process_cmd_non_blocking, FlowCtrlCmd},
        messaging::Recipients,
        Cmd, MyNode, NodeEventsChannel,
    },
    UsedSpace,
};
use sn_comms::{Comm, CommEvent, MsgReceived};
use sn_interface::{
    messaging::{
        data::ClientMsg, serialisation::WireMsg, AuthorityProof, ClientAuth, Dst, MsgId, MsgKind,
    },
    network_knowledge::{MyNodeInfo, NetworkKnowledge},
    test_utils::expand_age_pattern,
    types::{keys::ed25519::gen_keypair, Keypair},
};

use bytes::Bytes;
use eyre::{eyre, Context, Result};
use qp2p::Endpoint;
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque},
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    mpsc::{self, error::TryRecvError, Receiver, Sender},
    RwLock,
};
use xor_name::{Prefix, XorName};

// Process commands, allowing the user to inspect each and all of the intermediate
// commands that are being returned by the Cmd node.
// All commands that are meant to send msgs over the wire are inspected but not processed further.
pub(crate) struct ProcessAndInspectCmds {
    pending_cmds: VecDeque<Cmd>,
    index_inspected: usize,
    flow_ctrl_sender: Sender<FlowCtrlCmd>,
    flow_ctrl_receiver: Receiver<FlowCtrlCmd>,
}

impl ProcessAndInspectCmds {
    pub(crate) fn new(cmd: Cmd) -> Self {
        Self::from(vec![cmd])
    }

    fn from(cmds: Vec<Cmd>) -> Self {
        // We initialise `index_inspected` with MAX value, it will wraparound to 0 upon the first
        // call to `next()` method, thus making sure the first cmd is inspected in first iteration.
        let index_inspected = usize::MAX;
        let (sender, receiver) = mpsc::channel(1000);
        Self {
            pending_cmds: VecDeque::from(cmds),
            index_inspected,
            flow_ctrl_sender: sender,
            flow_ctrl_receiver: receiver,
        }
    }

    // This constructor invokes `MyNode::handle_valid_client_msg` using the
    // provided ClientMsg, and it uses the outcome (commands) as the
    // starting set of cmds to process by the ProcessAndInspectCmds instance herein created.
    // TODO: the client recv-stream created could be returned for the caller to use if necessary,
    // at this point it's useless since `Cmd::SendDataResponse` is not processed but only inspected.
    pub(crate) async fn new_from_client_msg(
        msg: ClientMsg,
        node: &mut MyNode,
        mut comm_rx: Receiver<CommEvent>,
    ) -> crate::node::Result<ProcessAndInspectCmds> {
        let context = node.context();
        let (msg_id, serialised_payload, msg_kind, _auth) =
            get_client_msg_parts_for_handling(&msg)?;

        let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
        let client_endpoint = Endpoint::builder()
            .addr(client_addr)
            .idle_timeout(70_000)
            .client()
            .expect("failed to create new client endpoint");

        let node_id = context.info.id();
        let node_addr = node_id.addr();
        let (client_conn, _) = client_endpoint
            .connect_to(&node_addr)
            .await
            .unwrap_or_else(|err| panic!("failed to connect to node at {node_addr:?}: {err:?}"));
        let (mut send_stream, _recv_stream) = client_conn
            .open_bi()
            .await
            .expect("failed to open bi-stream from new client endpoint");

        let dst = Dst {
            name: node_id.name(),
            section_key: context.network_knowledge.section_key(),
        };
        let wire_msg = WireMsg::new_msg(msg_id, serialised_payload, msg_kind, dst);
        let user_msg = wire_msg.serialize()?;

        // move send msg off thread so send / receive can both complete
        let _handle = tokio::spawn(async move {
            let _ = send_stream
                .send_user_msg(user_msg)
                .await
                .context("Could not send user msg");
        });

        match comm_rx.recv().await {
            Some(CommEvent::Msg(MsgReceived {
                send_stream: Some(send_stream),
                ..
            })) => {
                let cmds = MyNode::handle_msg(
                    &node.network_knowledge,
                    node.name(),
                    node.is_elder(),
                    sn_interface::types::Participant::from_node(node_id),
                    wire_msg,
                    Some(send_stream),
                )
                .await?;
                Ok(Self::from(cmds))
            }
            _ => Err(crate::node::Error::NoClientResponseStream),
        }
    }

    pub(crate) async fn next(&mut self, node: &mut MyNode) -> crate::node::Result<Option<&Cmd>> {
        let mut next_index = self.index_inspected.wrapping_add(1);
        if next_index < self.pending_cmds.len() {
            let cmd = self.pending_cmds.get(next_index);
            assert!(cmd.is_some());
            self.index_inspected = next_index;
            return Ok(cmd);
        }

        while let Some(cmd) = self.pending_cmds.pop_front() {
            next_index -= 1;
            if !matches!(
                cmd,
                Cmd::SendMsg { .. }
                    | Cmd::SendDataResponse { .. }
                    | Cmd::SendAndForwardResponseToClient { .. }
            ) {
                let new_cmds = MyNode::test_process_cmd(
                    cmd,
                    node,
                    &self.flow_ctrl_sender,
                    &mut self.flow_ctrl_receiver,
                )
                .await?;
                self.pending_cmds.extend(new_cmds);

                if next_index < self.pending_cmds.len() {
                    let cmd = self.pending_cmds.get(next_index);
                    assert!(cmd.is_some());
                    self.index_inspected = next_index;
                    return Ok(cmd);
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn process_all(
        &mut self,
        node: &mut MyNode,
    ) -> crate::node::error::Result<()> {
        while self.next(node).await?.is_some() { /* we just process all cmds */ }
        Ok(())
    }
}

pub(crate) fn get_client_msg_parts_for_handling(
    msg: &ClientMsg,
) -> crate::node::error::Result<(MsgId, Bytes, MsgKind, AuthorityProof<ClientAuth>)> {
    let payload = WireMsg::serialize_msg_payload(msg)?;
    let src_client_keypair = Keypair::new_ed25519();
    let auth = ClientAuth {
        public_key: src_client_keypair.public_key(),
        signature: src_client_keypair.sign(&payload),
    };
    let auth_proof = AuthorityProof::verify(auth.clone(), &payload)?;
    let kind = MsgKind::Client {
        auth,
        is_spend: false,
        query_index: None,
    };

    Ok((MsgId::new(), payload, kind, auth_proof))
}

/// A wrapper around `MyNode` to enable some it to function without having to initialize FlowCtrl
pub(crate) struct TestNode {
    pub(crate) node: MyNode,
    pub(crate) msg_tracker: Arc<RwLock<TestMsgTracker>>,
    pub(crate) flow_ctrl_sender: Sender<FlowCtrlCmd>,
    pub(crate) flow_ctrl_receiver: Receiver<FlowCtrlCmd>,
}

impl TestNode {
    pub(crate) fn new(node: MyNode, msg_tracker: Arc<RwLock<TestMsgTracker>>) -> Self {
        let (flow_ctrl_sender, flow_ctrl_receiver) = mpsc::channel(1000);
        Self {
            node,
            msg_tracker,
            flow_ctrl_sender,
            flow_ctrl_receiver,
        }
    }

    /// Processes the commands and also tracks any SendMsg* cmds.
    pub(crate) async fn process_cmd(&mut self, cmd: Cmd) -> Result<Vec<Cmd>> {
        self.msg_tracker.write().await.track(&cmd);
        let cmd_string = cmd.to_string();
        MyNode::test_process_cmd(
            cmd,
            &mut self.node,
            &self.flow_ctrl_sender,
            &mut self.flow_ctrl_receiver,
        )
        .await
        .wrap_err(format!("Error while processing cmd {cmd_string:?}"))
    }

    /// Handle and keep track of msgs from clients and nodes.
    /// Contains optional relocation_old_name to deal with name change during relocation.
    pub(crate) async fn handle_msg(
        &mut self,
        msg: MsgReceived,
        relocation_old_name: Option<XorName>,
    ) -> Result<Vec<Cmd>> {
        let msg_id = msg.wire_msg.msg_id();

        // check if we have successfully untracked the msg
        let mut untracked = false;
        if let Some(old_name) = relocation_old_name {
            untracked = untracked || self.msg_tracker.write().await.untrack(msg_id, &old_name);
        }
        let our_name = self.node.name();
        untracked = untracked || self.msg_tracker.write().await.untrack(msg_id, &our_name);
        if !untracked {
            return Err(eyre!(
                "Trying to untrack {msg_id:?} at node {our_name:?}
                \nThe msg was not tracked for this node.
                \nPlease check Cmd::SendMsg* to debug the issue"
            ));
        }

        let handle_node_msg_cmd = Cmd::HandleMsg {
            sender: msg.sender,
            wire_msg: msg.wire_msg,
            send_stream: msg.send_stream,
        };

        let process_node_msg = self
            .process_cmd(handle_node_msg_cmd)
            .await
            .wrap_err("Error while handling node_msg, Cmd::HandleMsg")?;
        // The `handle_msg()` which processes the above command might lead to several other Cmds like `ProcessNodeMsg`,
        // `ProcessClientMsg` etc. These are handled here instead of handling them in every test case.
        let mut new_cmds = Vec::new();
        for cmd in process_node_msg {
            let new_cmd = self
                .process_cmd(cmd)
                .await
                .wrap_err("Error while handling node_msg, Cmd::HandleMsg")?;
            new_cmds.extend(new_cmd);
        }

        Ok(new_cmds)
    }
}

/// Test utility to keep track of the msgs that has been sent.
/// When the msg has been received, it is removed from the tracker.
/// Used to terminate tests.
#[derive(Debug, Default)]
pub(crate) struct TestMsgTracker {
    pub(crate) tracker: BTreeMap<MsgId, BTreeSet<XorName>>,
}

impl TestMsgTracker {
    /// Tracks the msgs during SendMsg* Cmd
    pub(crate) fn track(&mut self, cmd: &Cmd) {
        if let Cmd::SendMsg {
            msg_id, recipients, ..
        } = cmd
        {
            let recp = recipients.clone().into_iter().map(|p| p.name()).collect();
            info!("Tracking {msg_id:?} for {recp:?}, cmd {cmd}");
            let _ = self.tracker.insert(*msg_id, recp);
        } else if let Cmd::SendMsgEnqueueAnyResponse {
            msg_id, recipients, ..
        } = cmd
        {
            let recp = recipients.iter().map(|p| p.name()).collect();
            info!("Tracking {msg_id:?} for {recp:?}, cmd {cmd}");
            let _ = self.tracker.insert(*msg_id, recp);
        } else if let Cmd::SendNodeMsgResponse {
            msg_id, node_id, ..
        } = cmd
        {
            info!("Tracking {msg_id:?} for {node_id:?}, cmd {cmd}");
            let _ = self
                .tracker
                .insert(*msg_id, BTreeSet::from([node_id.name()]));
        } else if let Cmd::UpdateCallerOnStream { caller, msg_id, .. } = cmd {
            info!("Tracking {msg_id:?} for {caller:?}, cmd {cmd}");
            let _ = self
                .tracker
                .insert(*msg_id, BTreeSet::from([caller.name()]));
        }
    }

    // Untrack the msg when we receive a MsgReceived
    pub(crate) fn untrack(&mut self, msg_id: MsgId, our_name: &XorName) -> bool {
        info!("Untracking {msg_id:?} for {our_name:?}");
        let removed;
        if let Entry::Occupied(mut entry) = self.tracker.entry(msg_id) {
            let nodes = entry.get_mut();
            removed = nodes.remove(our_name);
            if nodes.is_empty() {
                let _ = entry.remove();
            }
        } else {
            removed = false;
        }
        removed
    }

    /// When the counter is empty we are sure that all the msgs are processed
    pub(crate) fn is_empty(&self) -> bool {
        self.tracker.is_empty()
    }
}

/// Extend the `Cmd` enum with some utilities for testing.
///
/// Since this is in a module marked as #[test], this functionality will only be present in the
/// testing context.
impl Cmd {
    // Filters the list of recipients in a `SendCmd`
    pub(crate) fn filter_recipients(&mut self, filter_list: BTreeSet<XorName>) -> Result<()> {
        if let Cmd::SendMsg {
            ref mut recipients, ..
        } = self
        {
            let new_recipients = match recipients {
                Recipients::Single(dst) => {
                    if filter_list.contains(&dst.name()) {
                        Recipients::Multiple(BTreeSet::new())
                    } else {
                        Recipients::Single(*dst)
                    }
                }
                Recipients::Multiple(nodes) => {
                    let nodes = nodes
                        .iter()
                        .filter(|node| !filter_list.contains(&node.name()))
                        .cloned()
                        .collect();
                    Recipients::Multiple(nodes)
                }
            };
            *recipients = new_recipients;
        } else {
            return Err(eyre!("Expected a Cmd::SendMsg* to filter the recipients"));
        };
        Ok(())
    }
}

/// Extend the `MyNode` struct with some utilities for testing.
///
/// Since this is in a module marked as #[test], this functionality will only be present in the
/// testing context.
impl MyNode {
    /// Helper to process both non-blocking and blocking cmds and return the new cmds.
    pub(crate) async fn test_process_cmd(
        cmd: Cmd,
        node: &mut MyNode,
        flow_ctrl_cmd_sender: &Sender<FlowCtrlCmd>,
        flow_ctrl_receiver: &mut Receiver<FlowCtrlCmd>,
    ) -> NodeResult<Vec<Cmd>> {
        // process non blocking cmd
        let (join_handle, blocking_cmd) =
            process_cmd_non_blocking(cmd, &node.context(), flow_ctrl_cmd_sender.clone()).await?;
        if let Some(blocking_cmd) = blocking_cmd {
            // process blocking cmd
            return MyNode::process_cmd(blocking_cmd, node).await;
        }
        // If any tasks were spawned while handling the non_blocking cmds, then we make sure the tasks are completed.
        // Now any command in the flow_ctrl_chanel should be the new_cmds produced by our original cmd.
        // Note: This is not a guarantee as the tasks might themselves produce sub-task on their own, and we might not
        // get their JoinHandles. In that case, any new_cmds produced by the sub-task might be received at a later
        // point in time, breaking our assumption.
        if let Some(join_handle) = join_handle {
            loop {
                if join_handle.is_finished() {
                    let mut new_cmds = Vec::new();
                    while let Some(cmd) = get_next_cmd(flow_ctrl_receiver).await {
                        new_cmds.push(cmd);
                    }
                    return Ok(new_cmds);
                }
                debug!("join handle is still in progress");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            Ok(vec![])
        }
    }
}

// Receive the next `MsgReceived` if the buffer is not empty. Returns None if the buffer is currently empty
pub(crate) async fn get_next_msg(comm_rx: &mut Receiver<CommEvent>) -> Option<MsgReceived> {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    match comm_rx.try_recv() {
        Ok(CommEvent::Msg(msg)) => Some(msg),
        Ok(_) => None,
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => panic!("the comm_rx channel is closed"),
    }
}

// Receive the next `Cmd` if the buffer is not empty. Returns None if the buffer is currently empty
// Ignores `FlowCtrlCmd::UpdateContext`
pub(crate) async fn get_next_cmd(flow_ctrl_receiver: &mut Receiver<FlowCtrlCmd>) -> Option<Cmd> {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    match flow_ctrl_receiver.try_recv() {
        Ok(FlowCtrlCmd::Handle(cmd)) => Some(cmd),
        Ok(_) => None,
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => panic!("the comm_rx channel is closed"),
    }
}

/// Create set of elder, adults nodes
///
/// Optionally provide `age_pattern` to create elders with specific ages.
/// If None = elder's age is set to `MIN_ADULT_AGE`
/// If age_pattern.len() == elder, then apply the respective ages to each node
/// If age_pattern.len() < elder, then the last element's value is taken as the age for the remaining nodes.
/// If age_pattern.len() > elder, then the extra elements after `count` are ignored.
#[allow(clippy::type_complexity)]
pub(crate) fn gen_node_infos_with_comm(
    prefix: &Prefix,
    elders: usize,
    adults: usize,
    elder_age_pattern: Option<&[u8]>,
    adult_age_pattern: Option<&[u8]>,
) -> (Vec<(MyNodeInfo, Comm)>, Vec<(MyNodeInfo, Comm)>, TestCommRx) {
    let elder_age_pattern = expand_age_pattern(elder_age_pattern, elders);
    let adult_age_pattern = expand_age_pattern(adult_age_pattern, adults);
    let mut comm_rx = BTreeMap::new();
    let elder_nodes = (0..elders)
        .map(|idx| {
            let (node, comm, rx) = gen_info_with_comm(elder_age_pattern[idx], Some(*prefix));
            let _ = comm_rx.insert(node.public_key(), Some(rx));
            (node, comm)
        })
        .collect();
    let adult_nodes = (0..adults)
        .map(|idx| {
            let (node, comm, rx) = gen_info_with_comm(adult_age_pattern[idx], Some(*prefix));
            let _ = comm_rx.insert(node.public_key(), Some(rx));
            (node, comm)
        })
        .collect();
    (elder_nodes, adult_nodes, comm_rx)
}

/// Generate `MyNodeInfo` and `Comm`
pub(crate) fn gen_info_with_comm(
    age: u8,
    prefix: Option<Prefix>,
) -> (MyNodeInfo, Comm, Receiver<CommEvent>) {
    let socket_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
    let (comm, rx) = Comm::new(socket_addr, None).expect("failed  to create comm");
    let info = MyNodeInfo::new(
        gen_keypair(&prefix.unwrap_or_default().range_inclusive(), age),
        comm.socket_addr(),
    );
    (info, comm, rx)
}

/// Creates a single `MyNode` instance
pub(crate) fn build_a_node_instance(
    info: &MyNodeInfo,
    comm: &Comm,
    network_knowledge: &NetworkKnowledge,
) -> Result<MyNode> {
    // enter the current tokio runtime
    let (min_capacity, max_capacity, root_storage_dir) =
        create_test_capacity_and_root_storage().wrap_err("Failed to create root storage")?;

    let node = MyNode::new(
        comm.clone(),
        info.keypair.clone(),
        bls::SecretKey::random().public_key(),
        network_knowledge.clone(),
        None,
        UsedSpace::new(min_capacity, max_capacity),
        root_storage_dir,
        mpsc::channel(10).0,
        NodeEventsChannel::default(),
    )
    .wrap_err("Failed to create MyNode")?;
    Ok(node)
}
