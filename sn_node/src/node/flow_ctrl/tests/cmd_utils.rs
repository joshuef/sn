// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::dispatcher::Dispatcher, messaging::Peers, Cmd, MyNode};
use sn_comms::{CommEvent, MsgFromPeer};
use sn_interface::{
    messaging::{
        data::ClientMsg,
        serialisation::WireMsg,
        system::{JoinResponse, NodeMsg},
        AuthorityProof, ClientAuth, Dst, MsgId, MsgKind, MsgType,
    },
    network_knowledge::{test_utils::*, NodeState},
    types::{Keypair, Peer},
};

use assert_matches::assert_matches;
use bytes::Bytes;
use eyre::Result;
use qp2p::Endpoint;
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque},
    net::{Ipv4Addr, SocketAddr},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};
use xor_name::XorName;

pub(crate) struct JoinApprovalSent(pub(crate) bool);

pub(crate) async fn handle_online_cmd(
    peer: &Peer,
    sk_set: &bls::SecretKeySet,
    dispatcher: &Dispatcher,
) -> Result<JoinApprovalSent> {
    let node_state = NodeState::joined(*peer, None);
    let membership_decision = section_decision(sk_set, node_state);

    let mut all_cmds = ProcessAndInspectCmds::new(
        Cmd::HandleMembershipDecision(membership_decision),
        dispatcher,
    );

    let mut approval_sent = JoinApprovalSent(false);

    while let Some(cmd) = all_cmds.next().await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                recipients, msg, ..
            } => (msg, recipients),
            _ => continue,
        };

        match msg {
            NodeMsg::JoinResponse(JoinResponse::Approved { .. }) => {
                assert_matches!(recipients, Peers::Multiple(peers) => {
                    assert_eq!(peers, &BTreeSet::from([*peer]));
                });
                approval_sent.0 = true;
            }
            _ => continue,
        }
    }

    Ok(approval_sent)
}

// Process commands, allowing the user to inspect each and all of the intermediate
// commands that are being returned by the Cmd dispatcher.
// All commands that are meant to send msgs over the wire are inspected but not processed further.
pub(crate) struct ProcessAndInspectCmds<'a> {
    pending_cmds: VecDeque<Cmd>,
    index_inspected: usize,
    dispatcher: &'a Dispatcher,
}

impl<'a> ProcessAndInspectCmds<'a> {
    pub(crate) fn new(cmd: Cmd, dispatcher: &'a Dispatcher) -> Self {
        Self::from(vec![cmd], dispatcher)
    }

    fn from(cmds: Vec<Cmd>, dispatcher: &'a Dispatcher) -> Self {
        // We initialise `index_inspected` with MAX value, it will wraparound to 0 upon the first
        // call to `next()` method, thus making sure the first cmd is inspected in first iteration.
        let index_inspected = usize::MAX;

        Self {
            pending_cmds: VecDeque::from(cmds),
            index_inspected,
            dispatcher,
        }
    }

    // This constructor invokes `MyNode::handle_valid_client_msg` using the
    // provided ClientMsg, and it uses the outcome (commands) as the
    // starting set of cmds to process by the ProcessAndInspectCmds instance herein created.
    // TODO: the client recv-stream created could be returned for the caller to use if necessary,
    // at this point it's useless since `Cmd::SendClientResponse` is not processed but only inspected.
    pub(crate) async fn new_from_client_msg(
        msg: ClientMsg,
        dispatcher: &'a Dispatcher,
        mut comm_rx: Receiver<CommEvent>,
    ) -> crate::node::error::Result<ProcessAndInspectCmds> {
        let context = dispatcher.node().read().await.context();
        let (msg_id, serialised_payload, msg_kind, _auth) =
            get_client_msg_parts_for_handling(&msg)?;

        let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
        let client_endpoint = Endpoint::builder()
            .addr(client_addr)
            // .idle_timeout(70_000)
            .client()
            .expect("failed to create new client endpoint");

        let peer = context.info.peer();
        let node_addr = peer.addr();
        let (client_conn, _) = client_endpoint
            .connect_to(&node_addr)
            .await
            .unwrap_or_else(|err| panic!("failed to connect to node at {node_addr:?}: {err:?}"));
        let (mut send_stream, _recv_stream) = client_conn
            .open_bi()
            .await
            .expect("failed to open bi-stream from new client endpoint");

        let dst = Dst {
            name: peer.name(),
            section_key: context.network_knowledge.section_key(),
        };
        let wire_msg = WireMsg::new_msg(msg_id, serialised_payload, msg_kind, dst);
        let user_msg = wire_msg.serialize()?;
        send_stream.send_user_msg(user_msg).await?;

        match comm_rx.recv().await {
            Some(CommEvent::Msg(MsgFromPeer {
                send_stream: Some(send_stream),
                ..
            })) => {
                let cmds = MyNode::handle_msg(dispatcher.node(), peer, wire_msg, Some(send_stream))
                    .await?;
                Ok(Self::from(cmds, dispatcher))
            }
            _ => Err(crate::node::error::Error::NoClientResponseStream),
        }
    }

    pub(crate) async fn next(&mut self) -> crate::node::error::Result<Option<&Cmd>> {
        let mut next_index = self.index_inspected + 1;
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
                    | Cmd::SendAndForwardResponseToClient { .. }
                    | Cmd::SendClientResponse { .. }
                    | Cmd::SendNodeMsgResponse { .. }
            ) {
                let new_cmds = self.dispatcher.process_cmd(cmd).await?;
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

    pub(crate) async fn process_all(&mut self) -> crate::node::error::Result<()> {
        while self.next().await?.is_some() { /* we just process all cmds */ }
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

// A test utility to keep track of the Msgs that has been sent and received
#[derive(Debug, Default)]
pub(crate) struct TestMsgCounter {
    pub(crate) counter: BTreeMap<MsgId, BTreeSet<XorName>>,
}

impl TestMsgCounter {
    /// Tracks the msgs during SendMsg Cmd
    pub(crate) fn track(&mut self, cmd: &Cmd) {
        if let Cmd::SendMsg {
            msg_id, recipients, ..
        } = cmd
        {
            let recp = recipients.get().into_iter().map(|p| p.name()).collect();
            info!("Tracking {msg_id:?} for {recp:?}");
            let _ = self.counter.insert(*msg_id, recp);
        } else if let Cmd::SendMsgEnqueueAnyResponse {
            msg_id, recipients, ..
        } = cmd
        {
            let recp = recipients.iter().map(|p| p.name()).collect();
            info!("Tracking {msg_id:?} for {recp:?}");
            let _ = self.counter.insert(*msg_id, recp);
        } else {
            panic!("A Cmd::SendMsg variant was expected")
        }
    }

    // Untrack the msg when we receive a MsgFromPeer
    pub(crate) fn untrack(&mut self, msg_id: MsgId, our_name: &XorName) -> bool {
        info!("Untracking {msg_id:?} for {our_name:?}");
        let removed;
        if let Entry::Occupied(mut entry) = self.counter.entry(msg_id) {
            let peers = entry.get_mut();
            removed = peers.remove(our_name);
            if peers.is_empty() {
                let _ = entry.remove();
            }
        } else {
            panic!("msg_id {msg_id:?} is not found")
        }
        removed
    }

    /// When the counter is empty we are sure that all the msgs are processed
    pub(crate) fn is_empty(&self) -> bool {
        self.counter.is_empty()
    }
}

/// Extend the `Cmd` enum with some utilities for testing.
///
/// Since this is in a module marked as #[test], this functionality will only be present in the
/// testing context.
impl Cmd {
    // Filters the list of recipients in a `SendCmd`
    pub(crate) fn filter_recipients(&mut self, filter_list: BTreeSet<XorName>) {
        if let Cmd::SendMsg {
            ref mut recipients, ..
        } = self
        {
            let new_recipients = match recipients {
                Peers::Single(peer) => {
                    if filter_list.contains(&peer.name()) {
                        Peers::Multiple(BTreeSet::new())
                    } else {
                        Peers::Single(*peer)
                    }
                }
                Peers::Multiple(peers) => {
                    let peers = peers
                        .iter()
                        .filter(|peer| !filter_list.contains(&peer.name()))
                        .cloned()
                        .collect();
                    Peers::Multiple(peers)
                }
            };
            *recipients = new_recipients;
        } else {
            panic!("A Cmd::SendMsg variant was expected")
        };
    }
}

/// Extend the `Dispatcher` with some utilities for testing.
///
/// Since this is in a module marked as #[test], this functionality will only be present in the
/// testing context.
impl Dispatcher {
    // Handle and keep track of Msg from Peers
    // Contains optional relocation_old_name to deal with name change during relocation
    pub(crate) async fn test_handle_msg_from_peer(
        &self,
        msg: MsgFromPeer,
        msg_counter: &mut TestMsgCounter,
        relocation_old_name: Option<XorName>,
    ) -> Vec<Cmd> {
        let msg_id = {
            let msg_type = msg
                .wire_msg
                .into_msg()
                .expect("Failed to convert wire_msg to MsgType");
            match msg_type {
                MsgType::Client { msg_id, .. } => msg_id,
                MsgType::ClientDataResponse { msg_id, .. } => msg_id,
                MsgType::Node { msg_id, .. } => msg_id,
            }
        };

        // check if we have successfully untracked the msg
        let mut untracked = false;
        if let Some(old_name) = relocation_old_name {
            untracked = untracked || msg_counter.untrack(msg_id, &old_name);
        }
        let our_name = self.node().read().await.name();
        untracked = untracked || msg_counter.untrack(msg_id, &our_name);
        assert!(untracked);

        let handle_node_msg_cmd = Cmd::HandleMsg {
            origin: msg.sender,
            wire_msg: msg.wire_msg,
            send_stream: msg.send_stream,
        };
        self.process_cmd(handle_node_msg_cmd)
            .await
            .expect("Error while handling node msg")
    }
}

// Receive the next `MsgFromPeer` if the buffer is not empty. Returns None if the buffer is currently empty
pub(crate) async fn get_next_msg(comm_rx: &mut Receiver<CommEvent>) -> Option<MsgFromPeer> {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    match comm_rx.try_recv() {
        Ok(CommEvent::Msg(msg)) => Some(msg),
        Ok(_) => None,
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => panic!("the comm_rx channel is closed"),
    }
}
