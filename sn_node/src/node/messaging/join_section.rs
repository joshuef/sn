// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{flow_ctrl::cmds::Cmd, MyNode, NodeContext};

use sn_interface::{messaging::system::NodeMsg, network_knowledge::node_state::RelocationProof};

impl MyNode {
    /// Join a section.
    pub(crate) fn try_join_section(
        context: NodeContext,
        relocation: Option<RelocationProof>,
    ) -> Option<Cmd> {
        debug!("trying to join...");
        if context.network_knowledge.is_section_member(&context.name) {
            info!("We joined the network");
            None
        } else {
            Some(MyNode::send_to_elders_await_responses(
                context.network_knowledge.section_auth(),
                NodeMsg::TryJoin(relocation),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{
        flow_ctrl::tests::{
            network_builder::TestNetworkBuilder,
            test_utils::{
                build_a_node_instance, gen_info_with_comm, get_next_msg, TestMsgTracker, TestNode,
            },
        },
        messaging::Recipients,
        MIN_ADULT_AGE,
    };

    use sn_comms::CommEvent;
    use sn_interface::{
        elder_count, init_logger,
        messaging::{system::JoinResponse, MsgId, NetworkMsg},
        network_knowledge::{MembershipState, NetworkKnowledge},
        test_utils::TestSapBuilder,
        types::Participant,
    };

    use assert_matches::assert_matches;
    use eyre::{eyre, Result};
    use rand::thread_rng;
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::{
        sync::{mpsc::Receiver, RwLock},
        task::JoinHandle,
    };
    use xor_name::{Prefix, XorName};

    #[tokio::test]
    async fn join_happy_path_completes() -> Result<()> {
        init_logger();
        let prefix = Prefix::default();
        let elder_count = elder_count();
        let adult_count = 0;

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(thread_rng())
            .sap(
                TestSapBuilder::new(prefix)
                    .elder_count(elder_count)
                    .adult_count(adult_count),
            )
            .build()?;
        let mut node_instances = env
            .get_nodes(prefix, elder_count, adult_count, None)?
            .into_iter()
            .map(|node| {
                let name = node.name();
                let node = TestNode::new(node, msg_tracker.clone());
                (name, Arc::new(RwLock::new(node)))
            })
            .collect::<BTreeMap<XorName, Arc<RwLock<TestNode>>>>();
        let mut comm_receivers = BTreeMap::new();
        for (name, node) in node_instances.iter() {
            let pk = node.read().await.node.info().public_key();
            let comm = env.take_comm_rx(pk);
            let _ = comm_receivers.insert(*name, comm);
        }
        let network_knowledge = env.get_network_knowledge(prefix, None)?;

        let (joining_node_name, mut joining_node_handle) = initialize_join(
            prefix,
            &network_knowledge,
            &mut node_instances,
            &mut comm_receivers,
            msg_tracker.clone(),
        )
        .await?;

        join_loop(
            &node_instances,
            &mut comm_receivers,
            &mut joining_node_handle,
            msg_tracker,
        )
        .await?;

        // Check if the node has joined
        for node in node_instances.values() {
            let network_knowledge = node.read().await.node.network_knowledge().clone();
            if !network_knowledge.is_adult(&joining_node_name) {
                panic!("The node should've joined");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn joining_leads_to_vote_for_joined() -> Result<()> {
        init_logger();

        let prefix = Prefix::default();
        let env = TestNetworkBuilder::new(thread_rng())
            .sap(TestSapBuilder::new(prefix).elder_count(1))
            .build()?;

        let network_knowledge = env.get_network_knowledge(prefix, None)?;

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                gen_info_with_comm(MIN_ADULT_AGE, Some(prefix));
            build_a_node_instance(&info, &comm, &network_knowledge)?
        };

        let mut nodes = env.get_nodes(prefix, 1, 0, None)?;
        let mut elder = nodes.pop().expect("One elder should exist.");
        let elder_node_id = elder.info().id();

        let elder_context = elder.context();

        let joiner_node_id = joining_node.info().id();
        let some_cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_node_id,
            MsgId::new(),
            None,
            None,
        )
        .expect("An error was not expected.");

        let some_cmd = some_cmd
            .iter()
            .find(|cmd| matches!(cmd, Cmd::SendMsg { .. }));
        assert_matches!(some_cmd, Some(Cmd::SendMsg {
            msg: NetworkMsg::Node(msg),
            recipients,
            ..
        }) => {
            // verify the msg
            assert_matches!(msg, NodeMsg::MembershipVotes(votes) => {
                let vote = votes.first().expect("A vote should exist.");
                let proposals = vote.proposals();
                let node_state = proposals.first().expect("A proposal should exist.");
                let node_id = node_state.node_id();
                let state = node_state.state();
                let previous_name = node_state.previous_name();
                let age = node_state.age();
                assert_eq!(node_id, &joiner_node_id);
                assert_matches!(state, MembershipState::Joined);
                assert_matches!(previous_name, None);
                assert_eq!(age, MIN_ADULT_AGE);
            });
            // verify the recipients
            assert_matches!(recipients, Recipients::Multiple(recipients) => {
                let recipient = recipients.first().expect("A recipient should exist.");
                // the only elder sent a msg to itself
                assert_eq!(recipient, &elder_node_id);
            });
        });

        Ok(())
    }

    #[tokio::test]
    async fn adults_dont_handle_joins() -> Result<()> {
        init_logger();

        let prefix = Prefix::default();
        let env = TestNetworkBuilder::new(thread_rng())
            .sap(TestSapBuilder::new(prefix).elder_count(1).adult_count(1))
            .build()?;

        let network_knowledge = env.get_network_knowledge(prefix, None)?;

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                gen_info_with_comm(MIN_ADULT_AGE, Some(prefix));
            build_a_node_instance(&info, &comm, &network_knowledge)?
        };

        let mut nodes = env.get_nodes(prefix, 0, 1, None)?;
        let mut adult = nodes.pop().expect("One adult should exist.");

        assert!(adult.is_not_elder());

        let adult_context = adult.context();

        let joiner_node_id = joining_node.info().id();
        let cmd = MyNode::handle_join(
            &mut adult,
            &adult_context,
            joiner_node_id,
            MsgId::new(),
            None,
            None,
        )
        .expect("An error was not expected.");

        let cmd = cmd.iter().find(|cmd| matches!(cmd, Cmd::SendMsg { .. }));
        assert_matches!(cmd, None);

        Ok(())
    }

    #[tokio::test]
    async fn join_to_wrong_prefix_is_dropped() -> Result<()> {
        init_logger();

        let section_prefix = Prefix::default().pushed(false);
        let wrong_prefix = section_prefix.sibling();
        assert_ne!(section_prefix, wrong_prefix);

        let env = TestNetworkBuilder::new(thread_rng())
            .sap(TestSapBuilder::new(section_prefix).elder_count(1))
            .build()?;

        let network_knowledge = env.get_network_knowledge(section_prefix, None)?;

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                gen_info_with_comm(MIN_ADULT_AGE, Some(wrong_prefix));
            build_a_node_instance(&info, &comm, &network_knowledge)?
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None)?;
        let mut elder = nodes.pop().expect("One elder should exist.");

        let elder_context = elder.context();

        let joiner_node_id = joining_node.info().id();
        let cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_node_id,
            MsgId::new(),
            None,
            None,
        )
        .expect("An error was not expected.");

        let cmd = cmd.iter().find(|cmd| matches!(cmd, Cmd::SendMsg { .. }));
        assert_matches!(cmd, None);

        Ok(())
    }

    #[tokio::test]
    async fn join_with_wrong_age_is_dropped() -> Result<()> {
        init_logger();

        let section_prefix = Prefix::default();

        let env = TestNetworkBuilder::new(thread_rng())
            .sap(TestSapBuilder::new(section_prefix).elder_count(1))
            .build()?;

        let network_knowledge = env.get_network_knowledge(section_prefix, None)?;

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                gen_info_with_comm(MIN_ADULT_AGE + 1, Some(section_prefix));
            build_a_node_instance(&info, &comm, &network_knowledge)?
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None)?;
        let mut elder = nodes.pop().expect("One elder should exist.");

        let elder_context = elder.context();

        let joiner_node_id = joining_node.info().id();
        let cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_node_id,
            MsgId::new(),
            None,
            None,
        )
        .expect("An error was not expected.");

        let cmd = cmd.iter().find(|cmd| matches!(cmd, Cmd::SendMsg { .. }));
        assert_matches!(cmd, None);

        Ok(())
    }

    #[tokio::test]
    async fn join_when_disallowed_is_rejected() -> Result<()> {
        init_logger();

        let section_prefix = Prefix::default();

        let env = TestNetworkBuilder::new(thread_rng())
            .sap(TestSapBuilder::new(section_prefix).elder_count(1))
            .build()?;

        let network_knowledge = env.get_network_knowledge(section_prefix, None)?;

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                gen_info_with_comm(MIN_ADULT_AGE, Some(section_prefix));
            build_a_node_instance(&info, &comm, &network_knowledge)?
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None)?;
        let mut elder = nodes.pop().expect("One elder should exist.");

        // disallow joins
        elder.joins_allowed = false;

        let elder_context = elder.context();

        let joiner_node_id = joining_node.info().id();
        let some_cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_node_id,
            MsgId::new(),
            None,
            None,
        )
        .expect("An error was not expected.");

        let some_cmd = some_cmd
            .iter()
            .find(|cmd| matches!(cmd, Cmd::SendMsg { .. }));

        assert_matches!(some_cmd, Some(Cmd::SendMsg {
            msg: NetworkMsg::Node(msg),
            recipients,
            ..
        }) => {
            // the msg should be a rejection for joins disallowed
            assert_matches!(msg, NodeMsg::JoinResponse(JoinResponse::JoinsDisallowed));
            // the recipient should be the joining node
            assert_matches!(recipients, Recipients::Single(recipient) => {
                assert_eq!(recipient, &Participant::from_node(joiner_node_id));
            });
        });

        Ok(())
    }

    #[tokio::test]
    async fn join_with_old_sap_succeeds() -> Result<()> {
        init_logger();
        let prefix = Prefix::default();
        let elder_count = elder_count() - 1;
        let adult_count = 0;

        // Test environment setup
        let msg_tracker = Arc::new(RwLock::new(TestMsgTracker::default()));
        let mut env = TestNetworkBuilder::new(thread_rng())
            .sap(
                TestSapBuilder::new(prefix)
                    .elder_count(elder_count)
                    .adult_count(adult_count),
            )
            .build()?;
        let mut node_instances = env
            .get_nodes(prefix, elder_count, adult_count, None)?
            .into_iter()
            .map(|node| {
                let name = node.name();
                let node = TestNode::new(node, msg_tracker.clone());
                (name, Arc::new(RwLock::new(node)))
            })
            .collect::<BTreeMap<XorName, Arc<RwLock<TestNode>>>>();
        let mut comm_receivers = BTreeMap::new();
        for (name, node) in node_instances.iter() {
            let pk = node.read().await.node.info().public_key();
            let comm = env.take_comm_rx(pk);
            let _ = comm_receivers.insert(*name, comm);
        }
        let network_knowledge = env.get_network_knowledge(prefix, None)?;

        // elder joins the network
        let (joining_node_name, mut joining_node_handle) = initialize_join(
            prefix,
            &network_knowledge,
            &mut node_instances,
            &mut comm_receivers,
            msg_tracker.clone(),
        )
        .await?;
        join_loop(
            &node_instances,
            &mut comm_receivers,
            &mut joining_node_handle,
            msg_tracker.clone(),
        )
        .await?;
        // Check if the node has joined
        for node in node_instances.values() {
            let network_knowledge = node.read().await.node.network_knowledge().clone();
            if !network_knowledge.is_elder(&joining_node_name) {
                panic!("The node should've joined as an elder");
            }
        }

        // adult joins the network with the old network_knowledge, it will go through ae steps within the join process
        let (joining_node_name, mut joining_node_handle) = initialize_join(
            prefix,
            &network_knowledge,
            &mut node_instances,
            &mut comm_receivers,
            msg_tracker.clone(),
        )
        .await?;

        join_loop(
            &node_instances,
            &mut comm_receivers,
            &mut joining_node_handle,
            msg_tracker,
        )
        .await?;

        // Check if the node has joined
        for node in node_instances.values() {
            let network_knowledge = node.read().await.node.network_knowledge().clone();
            if !network_knowledge.is_adult(&joining_node_name) {
                panic!("The node should've joined as an adult");
            }
        }
        Ok(())
    }

    // Create a new adult and send the TryJoinNetwork to the section
    async fn initialize_join(
        prefix: Prefix,
        network_knowledge: &NetworkKnowledge,
        node_instances: &mut BTreeMap<XorName, Arc<RwLock<TestNode>>>,
        comm_receivers: &mut BTreeMap<XorName, Receiver<CommEvent>>,
        msg_tracker: Arc<RwLock<TestMsgTracker>>,
    ) -> Result<(XorName, Option<(XorName, JoinHandle<Result<Vec<Cmd>>>)>)> {
        // create the new adult
        let (info, comm, incoming_msg_receiver) = gen_info_with_comm(MIN_ADULT_AGE, Some(prefix));
        let node = build_a_node_instance(&info, &comm, network_knowledge)?;
        let name = node.info().name();
        let node = Arc::new(RwLock::new(TestNode::new(node, msg_tracker.clone())));

        // spawn a separate task for the joining node as it awaits for responses from the other
        // nodes
        let mut send_cmd = node.write().await.process_cmd(Cmd::TryJoinNetwork).await?;
        assert_eq!(send_cmd.len(), 1);
        let send_cmd = send_cmd.remove(0);
        assert_matches!(&send_cmd, Cmd::SendMsgEnqueueAnyResponse { .. });
        let node_clone = node.clone();
        let joining_node_handle =
            tokio::spawn(async move { node_clone.write().await.process_cmd(send_cmd).await });
        let joining_node_handle = Some((name, joining_node_handle));

        // add the joiner to our set
        let _ = node_instances.insert(name, node);
        let _ = comm_receivers.insert(name, incoming_msg_receiver);
        Ok((name, joining_node_handle))
    }

    /// Main loop that sends and processes Cmds
    async fn join_loop(
        node_instances: &BTreeMap<XorName, Arc<RwLock<TestNode>>>,
        comm_receivers: &mut BTreeMap<XorName, Receiver<CommEvent>>,
        joining_node_handle: &mut Option<(XorName, JoinHandle<Result<Vec<Cmd>>>)>,
        msg_tracker: Arc<RwLock<TestMsgTracker>>,
    ) -> Result<()> {
        // terminate if there are no more msgs to process
        let mut done = false;
        while !done {
            for (name, rw_test_node) in node_instances.iter() {
                let mut test_node = rw_test_node.write().await;
                info!("\n\n NODE: {}", name);
                let comm_rx = comm_receivers
                    .get_mut(name)
                    .ok_or_else(|| eyre!("comm_rx should be present"))?;

                if let Some((joining_node_name, handle_ref)) = &joining_node_handle {
                    if joining_node_name == name && handle_ref.is_finished() {
                        let (_, handle) =
                            joining_node_handle.take().expect("join_handle is present");
                        assert!(handle.await??.is_empty());
                    }
                }

                // Allow the node to receive msgs from others
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                while let Some(msg) = get_next_msg(comm_rx).await {
                    let mut cmds = match test_node.handle_msg(msg, None).await {
                        Err(err) => {
                            warn!("Error while processing cmd inside join_loop {err:?}");
                            continue;
                        }
                        Ok(cmds) => cmds,
                    };
                    while !cmds.is_empty() {
                        match test_node.process_cmd(cmds.remove(0)).await {
                            Ok(new_cmds) => cmds.extend(new_cmds),
                            Err(err) => {
                                warn!("Error while processing cmd inside join_loop {err:?}")
                            }
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
