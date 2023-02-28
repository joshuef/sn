// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::FlowCtrl;

use crate::node::{
    core::NodeContext, flow_ctrl::cmds::Cmd, membership::Membership, node_starter::CmdChannel,
    MyNode,
};

use sn_interface::{
    messaging::system::NodeMsg, network_knowledge::RelocationState, types::log_markers::LogMarker,
};

use std::{collections::BTreeSet, time::Duration};
use tokio::time::Instant;

const PROBE_INTERVAL: Duration = Duration::from_secs(300);
const REQUEST_TO_RELOCATE_TIMEOUT_SEC: Duration = Duration::from_secs(1);
const JOIN_AS_RELOCATED_TIMEOUT_SEC: Duration = Duration::from_secs(1);
const MISSING_VOTE_INTERVAL: Duration = Duration::from_secs(5);
const MISSING_DKG_MSG_INTERVAL: Duration = Duration::from_secs(5);
// const SECTION_PROBE_INTERVAL: Duration = Duration::from_secs(300);
const FAULT_CHECK_INTERVAL: Duration = Duration::from_secs(5);
// 30 adult nodes checked per minute., so each node should be queried 10x in 10 mins
// Which should hopefully trigger fault if we're not getting responses back
// const ADULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(2);
const ELDER_PROBE_INTERVAL: Duration = Duration::from_secs(10);

pub(super) struct PeriodicChecksTimestamps {
    last_probe: Instant,
    // last_section_probe: Instant,
    // last_adult_health_check: Instant,
    last_elder_health_check: Instant,
    last_vote_check: Instant,
    last_dkg_msg_check: Instant,
    last_fault_check: Instant,
    request_to_relocate_check: Instant,
    join_as_relocated_check: Instant,
}

impl PeriodicChecksTimestamps {
    pub(super) fn now() -> Self {
        Self {
            last_probe: Instant::now(),
            // last_section_probe: Instant::now(),
            // last_adult_health_check: Instant::now(),
            last_elder_health_check: Instant::now(),
            last_vote_check: Instant::now(),
            last_dkg_msg_check: Instant::now(),
            last_fault_check: Instant::now(),
            request_to_relocate_check: Instant::now(),
            join_as_relocated_check: Instant::now(),
        }
    }

    /// Check if any of the periodic checks have elapsed
    pub(crate) fn something_expired(&self) -> bool {
        self.last_vote_check.elapsed() > MISSING_VOTE_INTERVAL
            || self.last_probe.elapsed() > PROBE_INTERVAL
            || self.last_dkg_msg_check.elapsed() > MISSING_DKG_MSG_INTERVAL
            || self.last_fault_check.elapsed() > FAULT_CHECK_INTERVAL
            || self.request_to_relocate_check.elapsed() > REQUEST_TO_RELOCATE_TIMEOUT_SEC
            || self.join_as_relocated_check.elapsed() > JOIN_AS_RELOCATED_TIMEOUT_SEC
    }
}

impl FlowCtrl {
    /// Generate and fire commands for all types of periodic checks
    pub(super) async fn perform_periodic_checks(&mut self, node: &mut MyNode) {
        if !self.timestamps.something_expired() {
            return;
        }
        let context = node.context();
        let mut cmds = vec![];

        cmds.extend(self.enqueue_cmds_for_node_periodic_checks(&context, node));
        if context.is_elder {
            cmds.extend(
                self.enqueue_cmds_for_elder_periodic_checks(&context, node)
                    .await,
            );
        }

        // move cmd spawn off thread to not block
        let sender_channel = self.cmd_sender_channel.clone();
        let _handle = tokio::spawn(async move {
            for cmd in cmds {
                if let Err(error) = sender_channel.send((cmd, vec![])).await {
                    error!("Error queuing periodic check: {error:?}");
                }
            }
        });
    }

    /// Periodic tasks run for both elders and adults
    fn enqueue_cmds_for_node_periodic_checks(
        &mut self,
        context: &NodeContext,
        node: &mut MyNode,
    ) -> Vec<Cmd> {
        let mut cmds = vec![];

        // here we specifically ask for AE prob msgs and manually
        // track faults for missing AE, and theerefore hopefully keep members updated...
        if self.timestamps.last_elder_health_check.elapsed() > ELDER_PROBE_INTERVAL {
            self.timestamps.last_elder_health_check = Instant::now();
            for cmd in Self::ae_probe_to_elders_in_section(context).await {
                cmds.push(cmd);
            }
        }

        // check if we can request for relocation
        // The relocation_state will be changed into `JoinAsRelocated` once the request has been
        // approved by the section
        if let Some(RelocationState::PreparingToRelocate(dst)) = &context.relocation_state {
            if self.timestamps.request_to_relocate_check.elapsed() > REQUEST_TO_RELOCATE_TIMEOUT_SEC
            {
                info!(
                    "Periodic check: sending request to relocate our node to {:?}",
                    dst
                );

                if self.started_relocating.is_none() {
                    self.started_relocating = Some(context.name);
                    debug!(
                        "Started trying to relocate our node {:?}",
                        self.started_relocating
                    );
                }

                self.timestamps.request_to_relocate_check = Instant::now();
                cmds.push(MyNode::send_to_elders(
                    context,
                    NodeMsg::ProceedRelocation(*dst),
                ));
            }
        }

        // check if we can join the dst section
        if let Some(RelocationState::ReadyToJoinNewSection(proof)) = &context.relocation_state {
            if self.timestamps.join_as_relocated_check.elapsed() > JOIN_AS_RELOCATED_TIMEOUT_SEC {
                self.timestamps.join_as_relocated_check = Instant::now();
                if !context.network_knowledge.is_section_member(&context.name) {
                    info!(
                        "Periodic check: sending request to join the section as a relocated node"
                    );
                    cmds.push(MyNode::send_to_elders_await_responses(
                        context.clone(),
                        NodeMsg::TryJoin(Some(proof.clone())),
                    ));
                } else {
                    info!(
                        "{} for node: {:?}: Age is: {:?}",
                        LogMarker::RelocateEnd,
                        context.name,
                        context.info.age()
                    );
                    info!("We've joined a section, dropping the relocation proof.");
                    node.relocation_state = None;
                }
            }
        }
        cmds
    }

    /// Periodic tasks run for adults only
    // async fn enqueue_cmds_for_adult_periodic_checks(&mut self, context: &NodeContext) -> Vec<Cmd> {
    //     let mut cmds = vec![];

    // // if we've passed enough time, section probe
    // if self.timestamps.last_section_probe.elapsed() > SECTION_PROBE_INTERVAL {
    //     self.timestamps.last_section_probe = Instant::now();
    //     cmds.push(Self::probe_the_section(context).await);
    // }

    //     cmds
    // }

    /// Periodic tasks run for elders only
    async fn enqueue_cmds_for_elder_periodic_checks(
        &mut self,
        context: &NodeContext,
        node: &MyNode,
    ) -> Vec<Cmd> {
        let now = Instant::now();
        let mut cmds = vec![];

        if self.timestamps.last_probe.elapsed() > PROBE_INTERVAL {
            self.timestamps.last_probe = now;
            if let Some(cmd) = Self::probe_the_network(context) {
                cmds.push(cmd);
            }
        }

        // TODO: reevaluate both adult and elder health checks. Does this make sense mocking a client req?
        // how do we handle the need for a bidi stream (if using the whole client flow?)

        // if self.timestamps.last_adult_health_check.elapsed() > ADULT_HEALTH_CHECK_INTERVAL {
        //     debug!(" ----> adult health periodics sgtart");
        //     self.timestamps.last_adult_health_check = now;
        //     let health_cmds = match Self::perform_health_checks(self.node.clone()).await {
        //         Ok(cmds) => cmds,
        //         Err(error) => {
        //             error!("Error handling client msg to perform health check: {error:?}");
        //             vec![]
        //         }
        //     };
        //     cmds.extend(health_cmds);
        //     debug!(" ----> adult health periodics done");
        // }

        if self.timestamps.last_vote_check.elapsed() > MISSING_VOTE_INTERVAL {
            self.timestamps.last_vote_check = now;
            trace!(" ----> vote periodics start");
            cmds.extend(self.check_for_missed_votes(context, &node.membership));
            trace!(" ----> vote periodics done");
        }

        if self.timestamps.last_dkg_msg_check.elapsed() > MISSING_DKG_MSG_INTERVAL {
            trace!(" ----> dkg msg periodics start");
            self.timestamps.last_dkg_msg_check = now;
            Self::check_for_missed_dkg_messages(node, self.cmd_sender_channel.clone());
            trace!(" ----> dkg msg periodics done");
        }

        if self.timestamps.last_fault_check.elapsed() > FAULT_CHECK_INTERVAL {
            self.timestamps.last_fault_check = now;
            cmds.extend(self.vote_out_faulty_nodes().await);
        }

        cmds
    }

    // /// Initiates and generates all the subsequent Cmds to perform a healthcheck
    // async fn perform_health_checks(node: &MyNode) -> Result<Vec<Cmd>> {
    //     info!("Starting to check the section's health");
    //     let node = node.read().await;
    //     let our_prefix = node.network_knowledge.prefix();

    //     // random chunk addr will be sent to relevant nodes in the section.
    //     let chunk_addr = xor_name::rand::random();

    //     let chunk_addr = our_prefix.substituted_in(chunk_addr);

    //     let msg = ClientMsg::Query(DataQuery {
    //         variant: DataQueryVariant::GetChunk(ChunkAddress(chunk_addr)),
    //         adult_index: 0,
    //     });

    //     let msg_id = MsgId::new();
    //     let our_info = node.info();
    //     let origin = our_info.peer();

    //     let auth = auth(&node, &msg)?;

    //     // generate the cmds, and ensure we go through fault tracking
    //     node.handle_valid_client_msg(
    //         msg_id,
    //         msg,
    //         auth,
    //         origin,
    //     )
    //     .await
    // }

    /// Generates a probe msg, which goes to up to three random sections in order to
    /// passively maintain network knowledge over time.
    fn probe_the_network(context: &NodeContext) -> Option<Cmd> {
        let prefix = context.network_knowledge.prefix();

        // Send a probe message if we are an elder
        // but dont bother if we're the first section
        if !prefix.is_empty() {
            info!("Probing network");
            match MyNode::generate_probe_msg(context) {
                Ok(cmd) => Some(cmd),
                Err(error) => {
                    error!("Could not generate probe msg: {error:?}");
                    None
                }
            }
        } else {
            None
        }
    }

    /// Generates a probe msg, which goes to all section elders in order to
    /// passively maintain network knowledge over time and track faults.
    /// Tracking faults while awaiting a response.
    async fn ae_probe_to_elders_in_section(context: &NodeContext) -> Vec<Cmd> {
        let mut cmds = vec![];

        // Send a probe message to an elder
        debug!("Going to health check elders");

        let elders = context.network_knowledge.elders();
        for elder in elders {
            // we track a knowledge issue
            // which is countered when an AE-Update is
            cmds.push(Cmd::TrackNodeIssue {
                name: elder.name(),
                issue: sn_fault_detection::IssueType::AeProbeMsg,
            });
        }

        // Send a probe message to an elder
        cmds.push(MyNode::generate_section_probe_msg(context));

        cmds
    }

    /// Checks the interval since last vote received during a generation
    fn check_for_missed_votes(
        &self,
        context: &NodeContext,
        membership_context: &Option<Membership>,
    ) -> Vec<Cmd> {
        info!("Checking for missed votes");
        let mut cmds = vec![];
        if let Some(membership) = &membership_context {
            let last_received_vote_time = membership.last_received_vote_time();

            if let Some(time) = last_received_vote_time {
                // we want to resend the prev vote
                if time.elapsed() >= MISSING_VOTE_INTERVAL {
                    debug!("Vote consensus appears stalled...");
                    if let Some(cmd) = MyNode::membership_gossip_votes(context, membership_context)
                    {
                        trace!("Vote resending cmd: {cmd:?}");

                        cmds.push(cmd);
                    }
                    // we may also be behind, so lets request AE incase that is the case!
                    let msg = NodeMsg::MembershipAE(membership.generation());
                    cmds.push(MyNode::send_to_elders(context, msg));
                }
            }
        }
        cmds
    }

    /// Checks the interval since last dkg vote received
    fn check_for_missed_dkg_messages(node: &MyNode, sender_channel: CmdChannel) {
        info!("Checking for DKG missed messages");

        let dkg_voter = &node.dkg_voter;
        let last_received_dkg_message = dkg_voter.last_received_dkg_message();

        if let Some(time) = last_received_dkg_message {
            if time.elapsed() >= MISSING_DKG_MSG_INTERVAL {
                let cmds = node.dkg_gossip_msgs();
                if !cmds.is_empty() {
                    debug!("Dkg msg resending cmd, as Dkg voting appears stalled...");
                }

                // move cmd spawn off thread to not block
                let _handle = tokio::spawn(async move {
                    for cmd in cmds {
                        if let Err(error) = sender_channel.send((cmd, vec![])).await {
                            error!("Error sending DKG gossip msgs {error:?}");
                        }
                    }
                });
            }
        }
    }

    async fn vote_out_faulty_nodes(&mut self) -> Vec<Cmd> {
        info!("Voting out faulty nodes");
        let mut cmds = vec![];
        let faulty_nodes = self.get_faulty_node_names().await;

        if !faulty_nodes.is_empty() {
            info!("{:?} : {faulty_nodes:?}", LogMarker::ProposeOffline);
            let mut fault_set = BTreeSet::new();
            for node in faulty_nodes {
                let _prev = fault_set.insert(node);
            }
            cmds.push(Cmd::ProposeVoteNodesOffline(fault_set))
        }

        cmds
    }
}

// fn auth(node: &MyNode, msg: &ClientMsg) -> Result<AuthorityProof<ClientAuth>> {
//     let keypair = node.keypair.clone();
//     let payload = WireMsg::serialize_msg_payload(&msg)?;
//     let signature = keypair.sign(&payload);

//     let auth = ClientAuth {
//         public_key: PublicKey::Ed25519(keypair.public),
//         signature: Signature::Ed25519(signature),
//     };

//     Ok(AuthorityProof::verify(auth, payload)?)
// }
