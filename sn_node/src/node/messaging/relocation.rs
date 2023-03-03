// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::cmds::Cmd,
    messaging::Peers,
    relocation::{find_nodes_to_relocate, ChurnId},
    MyNode, Result,
};

use sn_interface::{
    elder_count,
    messaging::system::{NodeMsg, SectionSigned},
    network_knowledge::{
        node_state::{RelocationDst, RelocationInfo},
        Error, MembershipState, NodeState, RelocationProof, RelocationState,
    },
    types::{keys::ed25519, log_markers::LogMarker},
};

use std::collections::BTreeSet;
use xor_name::XorName;

// Relocation
impl MyNode {
    pub(crate) fn try_relocate_peers(
        &mut self,
        churn_id: ChurnId,
        excluded: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        info!("Try to find relocate peers, excluded {excluded:?}");
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

        for (node_state, relocation_dst) in
            find_nodes_to_relocate(&self.network_knowledge, &churn_id, excluded)
        {
            info!(
                "Begin relocation flow for {:?} to {relocation_dst:?} (on churn of {churn_id})",
                node_state.peer(),
            );

            let cmd = Cmd::send_msg(
                NodeMsg::PrepareToRelocate(RelocationDst::new(relocation_dst)),
                Peers::Single(*node_state.peer()),
            );
            cmds.push(cmd);
        }

        Ok(cmds)
    }

    /// On receiving the relocation trigger from the elders, the relocating node can
    /// request the section to do the relocation membership change.
    pub(crate) fn prepare_to_relocate(
        &mut self,
        dst: RelocationDst,
        current_name: XorName,
    ) -> Vec<Cmd> {
        // store the `RelocationDst` to start polling the elders
        if self.relocation_state.is_none() {
            debug!(
                "Started trying to relocate our node {:?} to {:?}",
                current_name, dst
            );

            // store the `RelocationState` locally to periodically request the elders
            self.relocation_state = Some(RelocationState::PreparingToRelocate(dst));
        } else {
            warn!(
                "Already trying to init relocation, so ignoring new relocation msg to dst: {dst:?}"
            );
            return vec![];
        }

        info!("{}", LogMarker::RelocateStart);
        info!("Sending request to proceed relocation of us to {:?}", dst);
        vec![MyNode::send_to_elders(
            &self.context(),
            NodeMsg::ProceedRelocation(dst),
        )]
    }

    /// The elder proposes a relocation membership change on receiving the
    /// proceed request from a node that was previously asked to prepare for relocation.
    pub(crate) fn proceed_relocation(
        &mut self,
        relocation_node: XorName,
        relocation_dst: RelocationDst,
    ) -> Result<Vec<Cmd>> {
        // Todo: Verify the relocation trigger here
        let node_state = self
            .network_knowledge()
            .get_section_member(&relocation_node)
            .ok_or(Error::NotAMember)?;

        Ok(self
            .propose_membership_change(node_state.relocate(relocation_dst))
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
        if self.name() != signed_relocation.peer().name() {
            // not for us, drop it
            warn!("Relocate: The received section signed relocation is not for us.");
            return Ok(None);
        }

        let dst_section =
            if let MembershipState::Relocated(relocation_dst) = signed_relocation.state() {
                *relocation_dst.name()
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
        MyNode::update_comm_target_list(&self.context());

        info!(
            "Relocation of us as {}: switched section to {new_prefix:?} with new name {new_name}. Now trying to join..",
            original_info.name(),
        );

        let proof = RelocationProof::new(info, node_sig, original_info.keypair.public);
        // we cache the proof so that we can retry if the join times out
        self.relocation_state = Some(RelocationState::ReadyToJoinNewSection(proof.clone()));

        Ok(MyNode::try_join_section(self.context(), Some(proof)))
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{
        flow_ctrl::{
            cmds::Cmd,
            tests::{
                cmd_utils::{get_next_msg, ProcessAndInspectCmds, TestMsgTracker, TestNode},
                network_builder::TestNetworkBuilder,
            },
        },
        relocation_check, ChurnId,
    };
    use sn_comms::CommEvent;
    use sn_consensus::Decision;
    use sn_interface::{
        elder_count, init_logger,
        messaging::{
            system::{JoinResponse, NodeDataCmd, NodeMsg},
            AntiEntropyKind, AntiEntropyMsg, NetworkMsg,
        },
        network_knowledge::{recommended_section_size, NodeState, RelocationDst, MIN_ADULT_AGE},
        test_utils::{gen_peer, gen_peer_in_prefix, prefix, section_decision, TestKeys},
    };

    use assert_matches::assert_matches;
    use eyre::{eyre, Result};
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };
    use tokio::{
        sync::{mpsc::Receiver, RwLock},
        task::JoinHandle,
    };
    use xor_name::{Prefix, XorName};

    /// Create a `SectionStateVote::Online` whose agreement handling triggers relocation of a node with the
    /// given age.
    ///
    /// NOTE: recommended to call this with low `age` (4 or 5), otherwise it might take very long time
    /// to complete because it needs to generate a signature with the number of trailing zeroes equal
    /// to (or greater that) `age`.
    fn create_relocation_trigger(
        sk_set: &bls::SecretKeySet,
        age: u8,
    ) -> Result<Decision<NodeState>> {
        loop {
            let node_state = NodeState::joined(gen_peer(MIN_ADULT_AGE), None);
            let decision = section_decision(sk_set, node_state.clone())?;

            let sig: bls::Signature = decision.proposals[&node_state].clone();
            let churn_id = ChurnId(sig.to_bytes());

            if relocation_check(age, &churn_id) && !relocation_check(age + 1, &churn_id) {
                return Ok(decision);
            }
        }
    }

    #[tokio::test]
    async fn relocation_trigger_is_sent() -> Result<()> {
        init_logger();

        let prefix: Prefix = prefix("0");
        let adults = recommended_section_size() - elder_count();
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(prefix, elder_count(), adults, None, None)
            .build()?;
        let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
        let mut section = env.get_network_knowledge(prefix, None)?;
        let sk_set = env.get_secret_key_set(prefix, None)?;

        let relocated_peer = gen_peer_in_prefix(MIN_ADULT_AGE - 1, prefix);
        let node_state = NodeState::joined(relocated_peer, None);
        let node_state = TestKeys::get_section_signed(&sk_set.secret_key(), node_state)?;
        assert!(section.update_member(node_state));
        // update our node with the new network_knowledge
        node.network_knowledge = section.clone();

        let membership_decision = create_relocation_trigger(&sk_set, relocated_peer.age())?;

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
        let adult_count = 1;

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(Prefix::default(), elder_count, adult_count, None, None)
            .build()?;
        let node_instances = env
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

        // Initialize relocation manually without using ChurnIds
        let relocation_node_old_name = env
            .get_peers(Prefix::default(), 0, 1, None)?
            .remove(0)
            .name();
        for node in node_instances.iter().filter_map(|((_, name), node)| {
            if name != &relocation_node_old_name {
                Some(node)
            } else {
                None
            }
        }) {
            initialize_relocation(node.clone(), relocation_node_old_name, Prefix::default())
                .await?;
        }

        relocation_loop(
            &node_instances,
            &mut comm_receivers,
            elder_count,
            msg_tracker.clone(),
        )
        .await?;

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
        let adult_count_0 = 1;
        let elder_count_1 = 7;
        let adult_count_1 = 0;
        let prefix0 = prefix("0");
        let prefix1 = prefix("1");

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(prefix0, elder_count_0, adult_count_0, None, None)
            .sap(prefix1, elder_count_1, adult_count_1, None, None)
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
        let node_instances = node_instances
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

        // Initialize relocation manually without using ChurnIds. Here, the adult from prefix0 is
        // to be relocated to prefix1
        let relocation_node_old_name = env.get_peers(prefix0, 0, 1, None)?.remove(0).name();

        // Only the elders in prefix0 should propose the SectionStateVote
        for test_node in node_instances
            .iter()
            .filter_map(|((pre, name), test_node)| {
                if name != &relocation_node_old_name && pre == &prefix0 {
                    Some(test_node)
                } else {
                    None
                }
            })
        {
            initialize_relocation(test_node.clone(), relocation_node_old_name, prefix1).await?;
        }

        relocation_loop(
            &node_instances,
            &mut comm_receivers,
            elder_count_0,
            msg_tracker.clone(),
        )
        .await?;

        // Validate the membership changes
        let relocation_node_new_name = node_instances
            .get(&(prefix0, relocation_node_old_name))
            .expect("Node should be present")
            .read()
            .await
            .node
            .name();
        for ((pref, node_name), test_node) in node_instances.iter() {
            let network_knowledge = test_node.read().await.node.network_knowledge().clone();
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
                if network_knowledge.is_adult(&relocation_node_old_name) {
                    panic!("The relocation node's old name should've been removed from Prefix0");
                }

                if network_knowledge.is_adult(&relocation_node_new_name) {
                    panic!("The relocation node should not have joined Prefix0 with its new name")
                }
            }
        }

        Ok(())
    }

    // Propose NodeIsOffline(relocation) for the provided node
    async fn initialize_relocation(
        node: Arc<RwLock<TestNode>>,
        relocation_node_name: XorName,
        dst_prefix: Prefix,
    ) -> Result<()> {
        info!(
            "Initialize relocation from {:?}",
            node.read().await.node.name()
        );
        let relocation_node_state = node
            .read()
            .await
            .node
            .network_knowledge()
            .get_section_member(&relocation_node_name)
            .expect("relocation node should be present");

        let elders = node
            .read()
            .await
            .node
            .network_knowledge()
            .section_auth()
            .elders_vec();
        let relocation_dst = RelocationDst::new(dst_prefix.name());
        let relocation_node_state = relocation_node_state.relocate(relocation_dst);
        let mut relocation_send_msg = node
            .write()
            .await
            .node
            .send_node_off_proposal(elders, relocation_node_state.clone())?;
        assert_eq!(relocation_send_msg.len(), 1);
        let relocation_send_msg = relocation_send_msg.remove(0);
        assert!(node
            .write()
            .await
            .process_cmd(relocation_send_msg)
            .await?
            .is_empty());

        Ok(())
    }

    /// Main loop that sends and processes Cmds
    async fn relocation_loop(
        node_instances: &BTreeMap<(Prefix, XorName), Arc<RwLock<TestNode>>>,
        comm_receivers: &mut BTreeMap<(Prefix, XorName), Receiver<CommEvent>>,
        from_section_n_elders: usize,
        msg_tracker: Arc<RwLock<TestMsgTracker>>,
    ) -> Result<()> {
        let mut relocation_membership_decision_done = BTreeSet::new();
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
                    for cmd in node.test_handle_msg_from_peer(msg, Some(name)).await? {
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
                        }
                        // There are 2 Membership changes here, one to relocate the
                        // node and the other one to allow the node to join as adult after
                        // being relocated
                        else if let Cmd::HandleMembershipDecision(_) = &cmd {
                            let mut send_cmds = node.process_cmd(cmd).await?;

                            if relocation_membership_decision_done.len() != from_section_n_elders {
                                // the first membership change contains an extra step to send the
                                // relocate msg to the relocation_node
                                assert_eq!(send_cmds.len(), 3);
                                let send_cmd = send_cmds.remove(0);
                                assert_matches!(&send_cmd, Cmd::SendMsg { msg: NetworkMsg::Node(msg), .. } => {
                                    assert_matches!(msg, NodeMsg::CompleteRelocation(_));
                                });
                                assert!(node.process_cmd(send_cmd).await?.is_empty());

                                let _ = relocation_membership_decision_done.insert(name);
                            } else {
                                assert_eq!(send_cmds.len(), 3);
                                let send_cmd = send_cmds.remove(0);
                                assert_matches!(&send_cmd, Cmd::SendMsg { msg: NetworkMsg::Node(msg), .. } => {
                                    assert_matches!(msg,  NodeMsg::JoinResponse(JoinResponse::Approved { .. }));
                                });
                                assert!(node.process_cmd(send_cmd).await?.is_empty());
                            }

                            // Common for both the cases
                            let send_cmd = send_cmds.remove(0);
                            assert_matches!(
                                &send_cmd,
                                Cmd::SendMsg {
                                    msg: NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                                        kind: AntiEntropyKind::Update { .. },
                                        ..
                                    }),
                                    ..
                                }
                            );
                            assert!(node.process_cmd(send_cmd).await?.is_empty());

                            // Skip NodeDataCmd as we don't have any data
                            let send_cmd = send_cmds.remove(0);
                            assert_matches!(&send_cmd, Cmd::SendMsg { msg: NetworkMsg::Node(msg), .. } => {
                                assert_matches!(msg,  NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data)) => {
                                    assert!(data.is_empty());
                                });
                            });
                        } else if let Cmd::TrackNodeIssue { .. } = &cmd {
                            // skip
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
