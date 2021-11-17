// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Command, Event};
use crate::messaging::{
    system::{Proposal, SystemMsg},
    DstLocation, EndUser, MsgKind, WireMsg,
};
use crate::routing::{
    core::ChunkStore,
    core::{Core, SendStatus},
    error::Result,
    log_markers::LogMarker,
    messages::WireMsgUtils,
    network_knowledge::NetworkKnowledge,
    node::Node,
    Error, Peer,
};
use itertools::Itertools;
use std::{sync::Arc, time::Duration};
use tokio::time::MissedTickBehavior;
use tokio::{sync::watch, time};
use tracing::Instrument;

const PROBE_INTERVAL: Duration = Duration::from_secs(30);

// `Command` Dispatcher.
pub(super) struct Dispatcher {
    pub(super) core: Core,

    cancel_timer_tx: watch::Sender<bool>,
    cancel_timer_rx: watch::Receiver<bool>,
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        // Cancel all scheduled timers including any future ones.
        let _res = self.cancel_timer_tx.send(true);
    }
}

impl Dispatcher {
    pub(super) fn new(core: Core) -> Self {
        let (cancel_timer_tx, cancel_timer_rx) = watch::channel(false);
        Self {
            core,
            cancel_timer_tx,
            cancel_timer_rx,
        }
    }

    pub(super) async fn get_chunk_storage(&self) -> ChunkStore {
        self.core.chunk_storage.clone()
    }


    /// Handles the given command and transitively any new commands that are
    /// produced during its handling. Trace logs will include the provided command id,
    /// and any sub-commands produced will have it as a common root cmd id.
    /// If a command id string is not provided a random one will be generated.
    pub(super) async fn handle_commands(
        self: Arc<Self>,
        command: Command,
        cmd_id: Option<String>,
    ) -> Result<()> {
        let cmd_id = cmd_id.unwrap_or_else(|| rand::random::<u32>().to_string());
        let cmd_id_clone = cmd_id.clone();
        let command_display = command.to_string();
        let _ = tokio::spawn(async move {
            if let Ok(commands) = self.handle_command(command, &cmd_id).await {
                for (sub_cmd_count, command) in commands.into_iter().enumerate() {
                    let sub_cmd_id = format!("{}.{}", cmd_id, sub_cmd_count);
                    self.clone().spawn_handle_commands(command, sub_cmd_id);
                }
            }
        });

        trace!(
            "{:?} {} cmd_id={}",
            LogMarker::CommandHandleSpawned,
            command_display,
            cmd_id_clone
        );
        Ok(())
    }

    // Note: this indirecton is needed. Trying to call `spawn(self.handle_commands(...))` directly
    // inside `handle_commands` causes compile error about type check cycle.
    fn spawn_handle_commands(self: Arc<Self>, command: Command, cmd_id: String) {
        let _ = tokio::spawn(self.handle_commands(command, Some(cmd_id)));
    }

    pub(super) async fn start_network_probing(self: Arc<Self>) {
        info!("Starting to probe network");
        let _handle = tokio::spawn(async move {
            let dispatcher = self.clone();
            let mut interval = tokio::time::interval(PROBE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                let _instant = interval.tick().await;

                // Send a probe message if we are an elder
                let core = &dispatcher.core;
                if core.is_elder().await && !core.network_knowledge().prefix().await.is_empty() {
                    match core.generate_probe_message().await {
                        Ok(command) => {
                            info!("Sending ProbeMessage");
                            if let Err(e) = dispatcher.clone().handle_commands(command, None).await
                            {
                                error!("Error sending a Probe message to the network: {:?}", e);
                            }
                        }
                        Err(error) => error!("Problem generating probe message: {:?}", error),
                    }
                }
            }
        });
    }

    pub(super) async fn write_prefixmap_to_disk(self: Arc<Self>) {
        info!("Writing our PrefixMap to disk");
        self.clone().core.write_prefix_map().await
    }

    /// Handles a single command.
    pub(super) async fn handle_command(
        &self,
        command: Command,
        cmd_id: &str,
    ) -> Result<Vec<Command>> {
        // Create a tracing span containing info about the current node. This is very useful when
        // analyzing logs produced by running multiple nodes within the same process, for example
        // from integration tests.
        let span = {
            let core = &self.core;

            let prefix = core.network_knowledge().prefix().await;
            let is_elder = core.is_elder().await;
            let section_key = core.network_knowledge().section_key().await;
            let age = core.node.read().await.age();
            trace_span!(
                "handle_command",
                name = %core.node.read().await.name(),
                prefix = format_args!("({:b})", prefix),
                age,
                elder = is_elder,
                cmd_id = %cmd_id,
                section_key = ?section_key,
                %command,
            )
        };

        async {
            trace!("{:?}", LogMarker::CommandHandleStart);

            let command_display = command.to_string();
            match self.try_handle_command(command).await {
                Ok(outcome) => {
                    trace!("{:?} {}", LogMarker::CommandHandleEnd, command_display);
                    Ok(outcome)
                }
                Err(error) => {
                    error!(
                        "Error encountered when handling command (cmd_id {}): {:?}",
                        cmd_id, error
                    );
                    trace!(
                        "{:?} {}: {:?}",
                        LogMarker::CommandHandleError,
                        command_display,
                        error
                    );
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn try_handle_command(&self, command: Command) -> Result<Vec<Command>> {
        match command {
            Command::HandleSystemMessage {
                sender,
                msg_id,
                msg_authority,
                dst_location,
                msg,
                payload,
                known_keys,
            } => {
                self.core
                    .handle_system_message(
                        sender,
                        msg_id,
                        msg_authority,
                        dst_location,
                        msg,
                        payload,
                        known_keys,
                    )
                    .await
            }
            Command::PrepareNodeMsgToSend { msg, dst } => {
                self.core.prepare_node_msg(msg, dst).await
            }
            Command::HandleMessage {
                sender_addr,
                wire_msg,
                original_bytes,
            } => {
                self.core
                    .handle_message(sender_addr, wire_msg, original_bytes)
                    .await
            }
            Command::HandleTimeout(token) => self.core.handle_timeout(token).await,
            Command::HandleAgreement { proposal, sig } => {
                self.core.handle_non_elder_agreement(proposal, sig).await
            }
            Command::HandleElderAgreement { proposal, sig } => match proposal {
                Proposal::OurElders(section_auth) => {
                    self.core
                        .handle_our_elders_agreement(section_auth.into_authed_state(), sig)
                        .await
                }
                _ => {
                    error!("Other agreement messages should be handled in `HandleAgreement`, which is non-blocking ");
                    Ok(vec![])
                }
            },
            Command::HandlePeerLost(peer) => self.core.handle_peer_lost(&peer.addr()).await,
            Command::HandleDkgOutcome {
                section_auth,
                outcome,
            } => self.core.handle_dkg_outcome(section_auth, outcome).await,
            Command::HandleDkgFailure(signeds) => self
                .core
                .handle_dkg_failure(signeds)
                .await
                .map(|command| vec![command]),
            Command::SendMessage {
                recipients,
                wire_msg,
            } => {
                self.send_message(&recipients, recipients.len(), wire_msg)
                    .await
            }
            Command::SendMessageDeliveryGroup {
                recipients,
                delivery_group_size,
                wire_msg,
            } => {
                self.send_message(&recipients, delivery_group_size, wire_msg)
                    .await
            }
            Command::ParseAndSendWireMsg(wire_msg) => self.send_wire_message(wire_msg).await,
            Command::ScheduleTimeout { duration, token } => Ok(self
                .handle_schedule_timeout(duration, token)
                .await
                .into_iter()
                .collect()),
            Command::HandleRelocationComplete { node, section } => {
                self.handle_relocation_complete(node, section).await?;
                Ok(vec![])
            }
            Command::SetJoinsAllowed(joins_allowed) => {
                self.core.set_joins_allowed(joins_allowed).await
            }
            Command::ProposeOnline {
                peer,
                previous_name,
                dst_key,
            } => {
                self.core
                    .make_online_proposal(&peer, previous_name, dst_key)
                    .await
            }
            Command::ProposeOffline(name) => self.core.propose_offline(name).await,
            Command::StartConnectivityTest(name) => {
                let msg = {
                    let core = &self.core;
                    let node = core.node.read().await.clone();
                    let section_pk = core.network_knowledge().section_key().await;
                    WireMsg::single_src(
                        &node,
                        DstLocation::Section {
                            name: node.name(),
                            section_pk,
                        },
                        SystemMsg::StartConnectivityTest(name),
                        section_pk,
                    )?
                };
                let our_name = self.core.node.read().await.name();
                let peers = self
                    .core
                    .network_knowledge()
                    .active_members()
                    .await
                    .iter()
                    .filter(|peer| peer.name() != name && peer.name() != our_name)
                    .cloned()
                    .collect_vec();
                Ok(self.core.send_or_handle(msg, peers).await)
            }
            Command::TestConnectivity(name) => {
                let mut commands = vec![];
                if let Some(member_info) = self.core.network_knowledge().members().get(&name) {
                    if self
                        .core
                        .comm
                        .is_reachable(&member_info.addr())
                        .await
                        .is_err()
                    {
                        commands.push(Command::ProposeOffline(member_info.name()));
                    }
                }
                Ok(commands)
            }
        }
    }

    async fn send_message(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Command>> {
        let cmds = match wire_msg.msg_kind() {
            MsgKind::NodeAuthMsg(_) | MsgKind::NodeBlsShareAuthMsg(_) => {
                self.deliver_messages(recipients, delivery_group_size, wire_msg)
                    .await?
            }
            MsgKind::ServiceMsg(_) => {
                let _res = self
                    .core
                    .comm
                    .send_on_existing_connection(recipients, wire_msg)
                    .await;

                vec![]
            }
        };

        Ok(cmds)
    }

    async fn deliver_messages(
        &self,
        recipients: &[Peer],
        delivery_group_size: usize,
        wire_msg: WireMsg,
    ) -> Result<Vec<Command>> {
        let status = self
            .core
            .comm
            .send(recipients, delivery_group_size, wire_msg)
            .await?;

        match status {
            SendStatus::MinDeliveryGroupSizeReached(failed_recipients)
            | SendStatus::MinDeliveryGroupSizeFailed(failed_recipients) => Ok(failed_recipients
                .into_iter()
                .map(Command::HandlePeerLost)
                .collect()),
            _ => Ok(vec![]),
        }
        .map_err(|e: Error| e)
    }

    /// Send a message, either section to section, node to node, or to an end user.
    pub(super) async fn send_wire_message(&self, mut wire_msg: WireMsg) -> Result<Vec<Command>> {
        if let DstLocation::EndUser(EndUser(name)) = wire_msg.dst_location() {
            let addr = self.core.comm.get_peer_address(name).await;

            if let Some(socket_addr) = addr {
                // Send a message to a client peer.
                // Messages sent to a client are not signed
                // or validated as part of the routing library.
                debug!("Sending client msg to {:?}: {:?}", socket_addr, wire_msg);

                let recipients = vec![Peer::new(*name, socket_addr)];
                wire_msg.set_dst_section_pk(self.core.network_knowledge().section_key().await);

                let command = Command::SendMessage {
                    recipients,
                    wire_msg,
                };

                Ok(vec![command])
            } else {
                error!(
                        "End user msg dropped at send. Could not find socketaddr corresponding to xorname {:?}: {:?}",
                        name, wire_msg
                    );
                Ok(vec![])
            }
        } else {
            // This message is not for an end user, then send it to peer/s over the network
            let cmd = self.core.send_msg_to_peers(wire_msg).await?;
            Ok(vec![cmd])
        }
    }

    async fn handle_schedule_timeout(&self, duration: Duration, token: u64) -> Option<Command> {
        let mut cancel_rx = self.cancel_timer_rx.clone();

        if *cancel_rx.borrow() {
            // Timers are already cancelled, do nothing.
            return None;
        }

        tokio::select! {
            _ = time::sleep(duration) => Some(Command::HandleTimeout(token)),
            _ = cancel_rx.changed() => None,
        }
    }

    async fn handle_relocation_complete(
        &self,
        new_node: Node,
        new_section: NetworkKnowledge,
    ) -> Result<()> {
        let previous_name = self.core.node.read().await.name();
        let new_keypair = new_node.keypair.clone();

        self.core.relocate(new_node, new_section).await?;

        self.core
            .send_event(Event::Relocated {
                previous_name,
                new_keypair,
            })
            .await;

        Ok(())
    }
}
