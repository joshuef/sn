// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::dispatcher::Dispatcher,
    messaging::{node_msgs::into_msg_bytes, Peers},
    Cmd, MyNode,
};

use sn_comms::MsgFromPeer;
use sn_interface::{
    messaging::{
        data::ClientMsg,
        serialisation::WireMsg,
        system::{JoinResponse, NodeMsg},
        AuthorityProof, ClientAuth, Dst, MsgId, MsgKind,
    },
    network_knowledge::{test_utils::*, NodeState},
    types::{Keypair, Peer},
};

use assert_matches::assert_matches;
use bytes::Bytes;
use eyre::{eyre, Result};
use qp2p::{Config, Endpoint};
use std::{
    collections::{BTreeSet, VecDeque},
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
        mut comm_rx: Receiver<MsgFromPeer>,
    ) -> crate::node::error::Result<ProcessAndInspectCmds> {
        let context = dispatcher.node().read().await.context();
        let (msg_id, serialised_payload, msg_kind, auth) = get_client_msg_parts_for_handling(&msg)?;

        let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
        let client_endpoint = Endpoint::new_client(client_addr, Config::default())
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
        let user_msg = WireMsg::new_msg(msg_id, serialised_payload, msg_kind, dst).serialize()?;
        send_stream.send_user_msg(user_msg).await?;

        match comm_rx.recv().await {
            Some(MsgFromPeer {
                send_stream: Some(send_stream),
                ..
            }) => {
                let cmds =
                    MyNode::handle_valid_client_msg(context, msg_id, msg, auth, peer, send_stream)
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
                    | Cmd::SendMsgAwaitResponseAndRespondToClient { .. }
                    | Cmd::SendClientResponse { .. }
                    | Cmd::SendNodeDataResponse { .. }
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
    let msg_kind = MsgKind::Client(auth);

    Ok((MsgId::new(), payload, msg_kind, auth_proof))
}

/// Extend the `Cmd` enum with some utilities for testing.
///
/// Since this is in a module marked as #[test], this functionality will only be present in the
/// testing context.
impl Cmd {
    /// Get the recipients for a `SendMsg` command.
    pub(crate) fn recipients(&self) -> Result<BTreeSet<Peer>> {
        match self {
            Cmd::SendMsg { recipients, .. } => match recipients {
                Peers::Single(peer) => Ok(BTreeSet::from([*peer])),
                Peers::Multiple(peers) => Ok(peers.clone()),
            },
            _ => Err(eyre!("A Cmd::SendMsg variant was expected")),
        }
    }
}

impl Dispatcher {
    // Sends out `NodeMsgs` to others synchronously, (process_cmd() spawns tasks to do it).
    // Optionally drop the msgs to the provided set of peers.
    pub(crate) async fn mock_send_msg(&self, cmd: Cmd, filter_recp: Option<BTreeSet<XorName>>) {
        if let Cmd::SendMsg {
            msg,
            msg_id,
            recipients,
            context,
        } = cmd
        {
            let peer_msgs = into_msg_bytes(
                &context.network_knowledge,
                context.name,
                msg.clone(),
                msg_id,
                recipients,
            )
            .expect("cannot convert msg into bytes");

            for (peer, msg_bytes) in peer_msgs {
                if let Some(filter) = &filter_recp {
                    if filter.contains(&peer.name()) {
                        continue;
                    }
                }

                if let Err(err) = context.comm.send_out_bytes(peer, msg_id, msg_bytes).await {
                    info!("Failed to send {msg} to {}: {err:?}", peer.name());
                }
            }
        } else {
            panic!("mock_send_msg expects Cmd::SendMsg, got {cmd:?}");
        }
    }
}

// Receive the next `MsgFromPeer` if the buffer is not empty. Returns None if the buffer is currently empty
pub(crate) fn get_next_msg(comm_rx: &mut Receiver<MsgFromPeer>) -> Option<MsgFromPeer> {
    match comm_rx.try_recv() {
        Ok(msg) => Some(msg),
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => panic!("the comm_rx channel is closed"),
    }
}
