// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::cmds::Cmd, messaging::Recipients, relocation::find_nodes_to_relocate, MyNode,
    NodeContext, NodeEvent, Result,
};

use sn_consensus::Decision;
use sn_interface::{
    elder_count,
    messaging::system::{NodeMsg, SectionSigned},
    network_knowledge::{
        node_state::RelocationInfo, Error, MembershipState, NodeState, RelocationProof,
    },
    network_knowledge::{node_state::RelocationTrigger, RelocationState},
    types::{keys::ed25519, log_markers::LogMarker, Participant},
};

use std::collections::BTreeSet;
use xor_name::XorName;

// Relocation
impl MyNode {
    pub(crate) fn try_relocate_nodes(
        &mut self,
        trigger: RelocationTrigger,
        excluded: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        info!("Try to find relocate nodes, excluded {excluded:?}");
        // Do not carry out relocation when there is not enough elder nodes.
        if self.network_knowledge.section_auth().elder_count() < elder_count() {
            warn!(
                "Not enough elders current {:?} vs. expected {:?}",
                self.network_knowledge.section_auth().elder_count(),
                elder_count()
            );
            return Ok(vec![]);
        }

        let mut cmds = vec![];

        for node_state in find_nodes_to_relocate(&self.network_knowledge, &trigger, excluded) {
            info!(
                "Begin relocation flow for {:?} to {:?} (on churn of {})",
                node_state.node_id(),
                trigger.dst_section(node_state.node_id().name()),
                trigger.churn_id(),
            );
            let cmd = Cmd::send_msg(
                NodeMsg::PrepareToRelocate(trigger.clone()),
                Recipients::Single(Participant::from_node(*node_state.node_id())),
            );
            cmds.push(cmd);
        }

        Ok(cmds)
    }

    /// On receiving the relocation trigger from the elders, the relocating node can
    /// request the section to do the relocation membership change.
    pub(crate) fn prepare_to_relocate(&mut self, trigger: RelocationTrigger) -> Vec<Cmd> {
        // store the `RelocationDst` to start polling the elders
        if let RelocationState::NoRelocation = self.relocation_state {
            debug!(
                "Started trying to relocate our node. (on churn of {})",
                trigger.churn_id(),
            );

            // store the `RelocationState` locally to periodically request the elders
            self.relocation_state = RelocationState::PreparingToRelocate(trigger.clone());
        } else {
            warn!(
                "Already trying to init relocation, so ignoring new relocation msg to trigger: {trigger:?}"
            );
            return vec![];
        }

        self.node_events_sender.broadcast(NodeEvent::RelocateStart);
        info!("{}", LogMarker::RelocateStart);
        info!(
            "Sending request to relocate our node. (on churn of {})",
            trigger.churn_id(),
        );

        vec![MyNode::send_to_elders(
            &self.network_knowledge,
            NodeMsg::ProceedRelocation(trigger),
        )]
    }

    /// The elder proposes a relocation membership change on receiving the
    /// proceed request from a node that was previously asked to prepare for relocation.
    pub(crate) fn proceed_relocation(
        &mut self,
        relocation_node: XorName,
        relocation_trigger: RelocationTrigger,
    ) -> Result<Vec<Cmd>> {
        // Todo: Verify the relocation trigger here
        let node_state = self
            .network_knowledge()
            .get_section_member(&relocation_node)
            .ok_or(Error::NotAMember)?;

        Ok(self
            .propose_membership_change(node_state.relocate(relocation_trigger))
            .map_or_else(Vec::new, |cmd| vec![cmd]))
    }

    /// Join the destination section as a relocated node
    pub(crate) fn relocate(
        &mut self,
        signed_relocation: SectionSigned<NodeState>,
    ) -> Result<Option<Cmd>> {
        // should be unreachable, but a sanity check
        let serialized = bincode::serialize(&signed_relocation.value)?;
        if !signed_relocation.sig.verify(&serialized) {
            warn!("Relocate: Could not verify section signature of our relocation");
            return Err(super::Error::InvalidSignature);
        }
        if self.name() != signed_relocation.node_id().name() {
            // not for us, drop it
            warn!("Relocate: The received section signed relocation is not for us.");
            return Ok(None);
        }

        let dst_section =
            if let MembershipState::Relocated(relocation_trigger) = signed_relocation.state() {
                relocation_trigger.dst_section(self.name())
            } else {
                warn!(
                    "Relocate: Ignoring msg containing invalid NodeState: {:?}",
                    signed_relocation.state()
                );
                return Ok(None);
            };

        debug!("Relocate: Received decision to relocate to other section at {dst_section}");

        let original_info = self.info();

        let dst_sap = self
            .network_knowledge
            .closest_signed_sap(&dst_section)
            .ok_or(super::Error::NoMatchingSection)?;
        let new_keypair = ed25519::gen_keypair(
            &dst_sap.prefix().range_inclusive(),
            original_info.age().saturating_add(1),
        );
        let new_name = ed25519::name(&new_keypair.public);

        let info = RelocationInfo::new(signed_relocation, new_name);
        let serialized_info = bincode::serialize(&info)?;
        // we verify that this new name was actually created by the old name
        let node_sig = ed25519::sign(&serialized_info, &original_info.keypair);
        let new_prefix = dst_sap.prefix();

        // we switch to the new section
        self.switch_section(dst_sap, new_keypair)?;
        // update the comms
        MyNode::update_comm_target_list(
            &self.comm,
            &self.network_knowledge.archived_members(),
            self.network_knowledge().members(),
        );

        info!(
            "Relocation of us as {}: switched section to {new_prefix:?} with new name {new_name}. Now trying to join..",
            original_info.name(),
        );

        let proof = RelocationProof::new(info, node_sig, original_info.keypair.public);
        // we cache the proof so that we can retry if the join times out
        self.relocation_state = RelocationState::ReadyToJoinNewSection(proof.clone());

        Ok(MyNode::try_join_section(self.context(), Some(proof)))
    }

    /// Finalise an ongoing relocation process, by updating the node's state,
    /// if we confirm we've already joined the new section.
    pub(crate) fn finalise_relocation(
        &mut self,
        context: &NodeContext,
        decision: Decision<NodeState>,
    ) {
        info!(
            "{} for node: {:?}: Age is: {:?}",
            LogMarker::RelocateEnd,
            context.name,
            context.info.age()
        );

        info!("We've joined a section, dropping the relocation proof.");
        self.relocation_state = RelocationState::NoRelocation;

        // Update our members list making sure we don't exist in it with our old name.
        let new_states: BTreeSet<_> = decision.proposals.keys().cloned().collect();
        match self.network_knowledge.try_update_member(decision) {
            Ok(true) => trace!(
                "Section members list updated due to relocation: {new_states:?}. \
                    New members list: {:?}",
                self.network_knowledge.members()
            ),
            Ok(false) => warn!(
                "Relocation: {new_states:?} doesn't trigger an update \
                    among current knowledge {:?}",
                self.network_knowledge.members()
            ),
            Err(err) => error!(
                "Failed to update relocation: {new_states:?} among \
                    current knowledge {:?} with error {:?}",
                self.network_knowledge.members(),
                err
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::flow_ctrl::{
        cmds::Cmd,
        tests::{
            network_builder::TestNetworkBuilder,
            test_utils::{
                build_a_node_instance, gen_info_with_comm, get_next_msg, ProcessAndInspectCmds,
                TestMsgTracker, TestNode,
            },
        },
    };
    use sn_comms::{Comm, CommEvent};
    use sn_consensus::Decision;
    use sn_interface::{
        elder_count, init_logger,
        messaging::{
            system::{JoinResponse, NodeMsg},
            AntiEntropyKind, AntiEntropyMsg, NetworkMsg,
        },
        network_knowledge::{recommended_section_size, MyNodeInfo, NodeState, MIN_ADULT_AGE},
        test_utils::{
            gen_node_id_in_prefix, prefix, section_decision, try_create_relocation_trigger,
            TestSapBuilder,
        },
    };

    use assert_matches::assert_matches;
    use eyre::{eyre, Result};
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::{
        sync::{mpsc::Receiver, RwLock},
        task::JoinHandle,
    };
    use xor_name::{Prefix, XorName};

    /// Creates a RelocationTrigger for the provided age.
    ///
    /// NOTE: recommended to call this with low `age` (4 or 5), otherwise it might take very long time
    /// to complete because it needs to generate a signature with the number of trailing zeroes equal
    /// to (or greater that) `age`.
    fn create_relocation_trigger(
        sk_set: &bls::SecretKeySet,
        gen: u64,
        age: u8,
        prefix: Prefix,
    ) -> Result<(Decision<NodeState>, MyNodeInfo, Comm, Receiver<CommEvent>)> {
        loop {
            let (info, comm, comm_rx) = gen_info_with_comm(MIN_ADULT_AGE, Some(prefix));
            if let Some((_trigger, decision)) =
                try_create_relocation_trigger(info.id(), sk_set, gen, age)?
            {
                return Ok((decision, info, comm, comm_rx));
            }
        }
    }

    #[tokio::test]
    async fn relocation_trigger_is_sent() -> Result<()> {
        init_logger();

        let prefix: Prefix = prefix("0");
        let adults = recommended_section_size() - elder_count();
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(TestSapBuilder::new(prefix).adult_count(adults))
            .build()?;
        let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
        let mut section = env.get_network_knowledge(prefix, None)?;
        let sk_set = env.get_secret_key_set(prefix, None)?;

        let relocated_node = gen_node_id_in_prefix(MIN_ADULT_AGE - 1, prefix);
        let node_state = NodeState::joined(relocated_node, None);
        let node_state = section_decision(&sk_set, 1, node_state)?;
        assert!(section.try_update_member(node_state)?);
        // update our node with the new network_knowledge
        node.network_knowledge = section.clone();

        let (membership_decision, ..) =
            create_relocation_trigger(&sk_set, 2, relocated_node.age(), prefix)?;

        let mut cmds =
            ProcessAndInspectCmds::new(Cmd::HandleMembershipDecision(membership_decision));

        let mut trigger_is_sent = false;
        while let Some(cmd) = cmds.next(&mut node).await? {
            let msg = match cmd {
                Cmd::SendMsg {
                    msg: NetworkMsg::Node(msg),
                    ..
                } => msg,
                _ => continue,
            };

            // Verify that the node has been asked to prepare for relocation.
            if let NodeMsg::PrepareToRelocate(_dst) = msg {
                trigger_is_sent = true;
            }
        }

        assert!(trigger_is_sent);
        Ok(())
    }

    #[tokio::test]
    async fn relocate_adults_to_the_same_section() -> Result<()> {
        init_logger();
        let elder_count = 7;
        let adult_count = 7;
        let prefix = Prefix::default();
        let relocation_node_age = 2;

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(
                TestSapBuilder::new(prefix)
                    .elder_count(elder_count)
                    .adult_count(adult_count)
                    .elder_age_pattern(vec![50])
                    .adult_age_pattern(vec![relocation_node_age, 20]),
            )
            .build()?;
        let mut node_instances = env
            .get_nodes(Prefix::default(), elder_count, adult_count, None)?
            .into_iter()
            .map(|node| {
                let prefix = node.network_knowledge().prefix();
                let name = node.name();
                let test_node = Arc::new(RwLock::new(TestNode::new(node, msg_tracker.clone())));
                ((prefix, name), test_node)
            })
            .collect::<BTreeMap<(Prefix, XorName), Arc<RwLock<TestNode>>>>();
        let mut comm_receivers = BTreeMap::new();
        for (name, node) in node_instances.iter() {
            let pk = node.read().await.node.info().public_key();
            let comm = env.take_comm_rx(pk);
            let _ = comm_receivers.insert(*name, comm);
        }
        // allow time to create the nodes and write section tree to disk
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let relocation_node = env
            .get_node_ids(Prefix::default(), 0, adult_count, None)?
            .into_iter()
            .find(|p| p.age() == relocation_node_age)
            .ok_or_else(|| eyre!("a node of age {relocation_node_age} must be present"))?;
        let relocation_node_old_name = relocation_node.name();
        let network_knowledge = env.get_network_knowledge(Prefix::default(), None)?;
        let sk_set = env.get_secret_key_set(Prefix::default(), None)?;

        // Find a node, that when joined will trigger the relocation of our relocation_node
        let (_, trig_info, trig_comm, trig_comm_rx) =
            create_relocation_trigger(&sk_set, 1, relocation_node_age, Prefix::default())?;
        let trig_node = build_a_node_instance(&trig_info, &trig_comm, &network_knowledge)?;
        let trig_node = Arc::new(RwLock::new(TestNode::new(trig_node, msg_tracker.clone())));
        let _ = node_instances.insert((Prefix::default(), trig_info.name()), trig_node);
        let _ = comm_receivers.insert((Prefix::default(), trig_info.name()), trig_comm_rx);

        // now let the tirggering node join the network
        for node in node_instances.values() {
            if node.read().await.node.is_elder() {
                let join = node
                    .write()
                    .await
                    .node
                    .propose_membership_change(NodeState::joined(trig_info.id(), None))
                    .ok_or_else(|| eyre!("Failed to propose membership change"))?;

                assert!(node.write().await.process_cmd(join).await?.is_empty());
            }
        }

        relocation_loop(&node_instances, &mut comm_receivers, msg_tracker.clone()).await?;

        // Validate the membership changes
        let relocation_node_new_name = node_instances
            .get(&(Prefix::default(), relocation_node_old_name))
            .expect("Node should be present")
            .read()
            .await
            .node
            .name();
        for node in node_instances.values() {
            let network_knowledge = node.read().await.node.network_knowledge().clone();
            // Make sure the relocation_node (new_name) is part of the elder's network_knowledge
            if !network_knowledge.is_adult(&relocation_node_new_name) {
                panic!("The relocation node should've joined with a new name");
            }

            // Make sure the relocation_node's old_name is removed
            // The membership changes are actively monitored by the elders, so skip this check
            // for the adult nodes
            if node.read().await.node.is_elder()
                && network_knowledge.is_adult(&relocation_node_old_name)
            {
                panic!("The relocation node's old name should've been removed");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn relocate_adults_to_different_section() -> Result<()> {
        init_logger();
        let elder_count_0 = 7;
        let adult_count_0 = 7;
        let elder_count_1 = 7;
        let adult_count_1 = 0;
        let prefix0 = prefix("0");
        let prefix1 = prefix("1");
        let relocation_node_age = 2;

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(
                TestSapBuilder::new(prefix0)
                    .elder_count(elder_count_0)
                    .adult_count(adult_count_0)
                    .elder_age_pattern(vec![50])
                    .adult_age_pattern(vec![relocation_node_age, 20]),
            )
            .sap(
                TestSapBuilder::new(prefix1)
                    .elder_count(elder_count_1)
                    .adult_count(adult_count_1),
            )
            .build()?;
        let network_knowledge_0 = env.get_network_knowledge(prefix0, None)?;
        let st_update_0 = network_knowledge_0
            .section_tree()
            .generate_section_tree_update(&prefix0)?;
        let network_knowledge_1 = env.get_network_knowledge(prefix1, None)?;
        let st_update_1 = network_knowledge_1
            .section_tree()
            .generate_section_tree_update(&prefix1)?;

        let mut node_instances = env.get_nodes(prefix0, elder_count_0, adult_count_0, None)?;
        node_instances.extend(env.get_nodes(prefix1, elder_count_1, adult_count_1, None)?);
        let mut node_instances = node_instances
            .into_iter()
            .map(|mut node| {
                let name = node.name();
                let prefix = node.network_knowledge().prefix();
                // the nodes obtained from TestNetworkBuilder do not have knowledge about the other
                // sections, hence update their SectionTree
                if prefix == prefix0 {
                    let _ = node
                        .network_knowledge
                        .section_tree_mut()
                        .update_the_section_tree(st_update_1.clone())
                        .expect("Section tree update failed");
                } else if prefix == prefix1 {
                    let _ = node
                        .network_knowledge
                        .section_tree_mut()
                        .update_the_section_tree(st_update_0.clone())
                        .expect("Section tree update failed");
                }
                let node = Arc::new(RwLock::new(TestNode::new(node, msg_tracker.clone())));
                ((prefix, name), node)
            })
            .collect::<BTreeMap<(Prefix, XorName), Arc<RwLock<TestNode>>>>();
        let mut comm_receivers = BTreeMap::new();
        for (name, node) in node_instances.iter() {
            let pk = node.read().await.node.info().public_key();
            let comm = env.take_comm_rx(pk);
            let _ = comm_receivers.insert(*name, comm);
        }
        // allow time to create the nodes and write section tree to disk
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let relocation_node = env
            .get_node_ids(prefix0, 0, adult_count_0, None)?
            .into_iter()
            .find(|p| p.age() == relocation_node_age)
            .ok_or_else(|| eyre!("a node of age {relocation_node_age} must be present"))?;
        let relocation_node_old_name = relocation_node.name();
        let network_knowledge_0 = env.get_network_knowledge(prefix0, None)?;
        let sk_set = env.get_secret_key_set(prefix0, None)?;

        // Find a node, that when joined will trigger the relocation of our relocation_node
        let (_, trig_info, trig_comm, trig_comm_rx) =
            create_relocation_trigger(&sk_set, 1, relocation_node_age, prefix0)?;
        let trig_node = build_a_node_instance(&trig_info, &trig_comm, &network_knowledge_0)?;
        let trig_node = Arc::new(RwLock::new(TestNode::new(trig_node, msg_tracker.clone())));
        let _ = node_instances.insert((prefix0, trig_info.name()), trig_node);
        let _ = comm_receivers.insert((prefix0, trig_info.name()), trig_comm_rx);

        // now let the tirggering node join the network
        for node in node_instances.values() {
            let is_elder = node.read().await.node.is_elder();
            let is_prefix0 = node.read().await.node.network_knowledge().prefix() == prefix0;
            if is_elder && is_prefix0 {
                let join = node
                    .write()
                    .await
                    .node
                    .propose_membership_change(NodeState::joined(trig_info.id(), None))
                    .ok_or_else(|| eyre!("Failed to propose membership change"))?;

                assert!(node.write().await.process_cmd(join).await?.is_empty());
            }
        }

        relocation_loop(&node_instances, &mut comm_receivers, msg_tracker.clone()).await?;

        // Validate the membership changes
        let relocation_node_new_name = node_instances
            .get(&(prefix0, relocation_node_old_name))
            .expect("Node should be present")
            .read()
            .await
            .node
            .name();
        for ((pref, node_name), node) in node_instances.iter() {
            let network_knowledge = node.read().await.node.network_knowledge().clone();
            // the test_node for the relocation_node is still under the old name
            if node_name == &relocation_node_old_name {
                // the relocation node should be part of prefix1
                assert_eq!(network_knowledge.prefix(), prefix1);
                continue;
            }

            // Make sure the relocation_node with the new_name is part of prefix1
            if pref == &prefix1 {
                if !network_knowledge.is_adult(&relocation_node_new_name) {
                    panic!("The relocation node should've joined Prefix1 with the new name");
                }

                if network_knowledge.is_adult(&relocation_node_old_name) {
                    panic!("The relocation node should not have joined Prefix1 with its old name")
                }
            }

            // Make sure the relocation_node's old_name is removed from prefix0. Also make sure the
            // new_name is not a part of prefix0
            if pref == &prefix0 {
                if node.read().await.node.is_elder()
                    && network_knowledge.is_adult(&relocation_node_old_name)
                {
                    panic!("The relocation node's old name should've been removed from Prefix0");
                }

                if network_knowledge.is_adult(&relocation_node_new_name) {
                    panic!("The relocation node should not have joined Prefix0 with its new name")
                }
            }
        }

        Ok(())
    }

    /// Main loop that sends and processes Cmds
    async fn relocation_loop(
        node_instances: &BTreeMap<(Prefix, XorName), Arc<RwLock<TestNode>>>,
        comm_receivers: &mut BTreeMap<(Prefix, XorName), Receiver<CommEvent>>,
        msg_tracker: Arc<RwLock<TestMsgTracker>>,
    ) -> Result<()> {
        // Handler for the bidi stream task
        let mut join_handle_for_relocating_node: Option<(XorName, JoinHandle<Result<Vec<Cmd>>>)> =
            None;
        // terminate if there are no more msgs to process
        let mut done = false;
        while !done {
            for (key, test_node) in node_instances.iter() {
                let mut node = test_node.write().await;
                let name = key.1;
                info!("\n\n NODE: {}", name);
                let comm_rx = comm_receivers
                    .get_mut(key)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;

                if let Some((relocating_node_name, handle_ref)) = &join_handle_for_relocating_node {
                    if *relocating_node_name == name && handle_ref.is_finished() {
                        let (_, handle) = join_handle_for_relocating_node
                            .take()
                            .expect("join_handle is present");
                        assert!(handle.await??.is_empty());
                    }
                }

                // Allow the node to receive msgs from others
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                while let Some(msg) = get_next_msg(comm_rx).await {
                    for cmd in node.test_handle_msg(msg, Some(name)).await? {
                        info!("Got cmd {}", cmd);
                        if let Cmd::SendMsg { .. } = &cmd {
                            assert!(node.process_cmd(cmd).await?.is_empty());
                        } else if let Cmd::SendMsgEnqueueAnyResponse { .. } = &cmd {
                            // The relocating node waits for the elders to allow it to join the
                            // section. It happens through a bidi stream and hence spawn it as a
                            // separate task.
                            let test_node = test_node.clone();
                            join_handle_for_relocating_node = Some((
                                name,
                                tokio::spawn(async move {
                                    test_node.write().await.process_cmd(cmd).await
                                }),
                            ));
                        } else if let Cmd::HandleNodeOffAgreement { .. } = &cmd {
                            let mut send_cmd = node.process_cmd(cmd).await?;
                            assert_eq!(send_cmd.len(), 1);
                            let send_cmd = send_cmd.remove(0);
                            assert_matches!(&send_cmd, Cmd::SendMsg { msg: NetworkMsg::Node(msg), .. } => {
                                assert_matches!(msg, NodeMsg::MembershipVotes(_));
                            });
                            assert!(node.process_cmd(send_cmd).await?.is_empty());
                        } else if let Cmd::HandleMembershipDecision(_) = &cmd {
                            let send_cmds = node.process_cmd(cmd).await?;
                            for cmd in send_cmds {
                                assert!(node.process_cmd(cmd).await?.is_empty());
                            }
                        } else if let Cmd::SendNodeMsgResponse {
                            msg: NodeMsg::JoinResponse(JoinResponse::UnderConsideration),
                            ..
                        } = &cmd
                        {
                            // Send out the `UnderConsideration` as stream response.
                            let _ = node.process_cmd(cmd).await?;
                        } else if let Cmd::UpdateCaller {
                            kind: AntiEntropyKind::Redirect { .. },
                            ..
                        } = &cmd
                        {
                            let mut send_cmds = node.process_cmd(cmd).await?;

                            let send_cmd = send_cmds.remove(0);
                            assert_matches!(
                                &send_cmd,
                                Cmd::SendMsg {
                                    msg: NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                                        kind: AntiEntropyKind::Redirect { .. },
                                        ..
                                    }),
                                    ..
                                }
                            );
                            assert!(node.process_cmd(send_cmd).await?.is_empty());
                        } else if let Cmd::TrackNodeIssue { .. } = &cmd {
                            // skip
                        } else {
                            panic!("got a different cmd {cmd:?}");
                        }
                    }
                }
            }

            if msg_tracker.read().await.is_empty() {
                done = true;
            } else {
                debug!(
                    "remaining msgs {:?}",
                    msg_tracker.read().await.tracker.keys().collect::<Vec<_>>()
                );
            }
        }
        Ok(())
    }
}
