// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, MyNode};

use sn_interface::{messaging::system::NodeMsg, network_knowledge::node_state::RelocationProof};

impl MyNode {
    /// Join a section.
    pub(crate) fn try_join_section(
        context: NodeContext,
        relocation: Option<RelocationProof>,
    ) -> Option<Cmd> {
        debug!("tyring to join...");
        if context.network_knowledge.is_section_member(&context.name) {
            debug!("tyring to join...WE JOINED?!");
            None
        } else {
            Some(MyNode::send_to_elders_await_responses(
                context,
                NodeMsg::TryJoin(relocation),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{
        flow_ctrl::{
            tests::network_builder::{TestNetwork, TestNetworkBuilder},
            CmdCtrl, FlowCtrl, RejoinReason,
        },
        messaging::Peers,
        MIN_ADULT_AGE,
    };

    use sn_comms::CommEvent;
    use sn_interface::{
        elder_count, init_logger,
        messaging::{
            MsgId,
            {
                system::{JoinRejectReason, JoinResponse},
                NetworkMsg,
            },
        },
        network_knowledge::{MembershipState, NetworkKnowledge},
    };

    use assert_matches::assert_matches;
    use eyre::Result;
    use futures::future::join_all;
    use rand::thread_rng;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::{
        mpsc::{self, error::TryRecvError, Receiver, Sender},
        RwLock,
    };
    use xor_name::Prefix;

    const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn join_happy_path_completes() -> Result<()> {
        init_logger();

        // setup section and a joiner
        let env = setup(elder_count()).await;

        // join the section and return resulting section and next joiner
        let _new_env = env.join().await?;

        Ok(())
    }

    #[tokio::test]
    async fn joining_leads_to_vote_for_joined() -> Result<()> {
        init_logger();

        let prefix = Prefix::default();
        let env = TestNetworkBuilder::new(thread_rng())
            .sap(prefix, 1, 0, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(prefix, None);

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                TestNetwork::gen_info(MIN_ADULT_AGE, Some(prefix));
            TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge)
        };

        let mut nodes = env.get_nodes(prefix, 1, 0, None);
        let elder = nodes.pop().expect("One elder should exist.");
        let elder_peer = elder.info().peer();

        let elder_context = elder.context();

        let joiner_peer = joining_node.info().peer();
        let some_cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_peer,
            MsgId::new(),
            None,
            None,
        )
        .await
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
                let peer = node_state.peer();
                let state = node_state.state();
                let previous_name = node_state.previous_name();
                let age = node_state.age();
                assert_eq!(peer, &joiner_peer);
                assert_matches!(state, MembershipState::Joined);
                assert_matches!(previous_name, None);
                assert_eq!(age, MIN_ADULT_AGE);
            });
            // verify the recipients
            assert_matches!(recipients, Peers::Multiple(recipients) => {
                let recipient = recipients.first().expect("A recipient should exist.");
                // the only elder sent a msg to itself
                assert_eq!(recipient, &elder_peer);
            });
        });

        Ok(())
    }

    #[tokio::test]
    async fn adults_dont_handle_joins() -> Result<()> {
        init_logger();

        let prefix = Prefix::default();
        let env = TestNetworkBuilder::new(thread_rng())
            .sap(prefix, 1, 1, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(prefix, None);

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                TestNetwork::gen_info(MIN_ADULT_AGE, Some(prefix));
            TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge)
        };

        let mut nodes = env.get_nodes(prefix, 0, 1, None);
        let adult = nodes.pop().expect("One adult should exist.");

        assert!(adult.is_not_elder());

        let adult_context = adult.context();

        let joiner_peer = joining_node.info().peer();
        let cmd = MyNode::handle_join(
            &mut adult,
            &adult_context,
            joiner_peer,
            MsgId::new(),
            None,
            None,
        )
        .await
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
            .sap(section_prefix, 1, 0, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(section_prefix, None);

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                TestNetwork::gen_info(MIN_ADULT_AGE, Some(wrong_prefix));
            TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge)
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None);
        let elder = nodes.pop().expect("One elder should exist.");

        let elder_context = elder.context();

        let joiner_peer = joining_node.info().peer();
        let cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_peer,
            MsgId::new(),
            None,
            None,
        )
        .await
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
            .sap(section_prefix, 1, 0, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(section_prefix, None);

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                TestNetwork::gen_info(MIN_ADULT_AGE + 1, Some(section_prefix));
            TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge)
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None);
        let elder = nodes.pop().expect("One elder should exist.");

        let elder_context = elder.context();

        let joiner_peer = joining_node.info().peer();
        let cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_peer,
            MsgId::new(),
            None,
            None,
        )
        .await
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
            .sap(section_prefix, 1, 0, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(section_prefix, None);

        let joining_node = {
            let (info, comm, _incoming_msg_receiver) =
                TestNetwork::gen_info(MIN_ADULT_AGE, Some(section_prefix));
            TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge)
        };

        let mut nodes = env.get_nodes(section_prefix, 1, 0, None);
        let mut elder = nodes.pop().expect("One elder should exist.");

        // disallow joins
        elder.joins_allowed = false;

        let elder_context = elder.context();

        let joiner_peer = joining_node.info().peer();
        let some_cmd = MyNode::handle_join(
            &mut elder,
            &elder_context,
            joiner_peer,
            MsgId::new(),
            None,
            None,
        )
        .await
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
            assert_matches!(msg, NodeMsg::JoinResponse(JoinResponse::Rejected(JoinRejectReason::JoinsDisallowed)));
            // the recipient should be the joining node
            assert_matches!(recipients, Peers::Single(recipient) => {
                assert_eq!(recipient, &joiner_peer);
            });
        });

        Ok(())
    }

    #[tokio::test]
    async fn join_with_old_sap_succeeds() -> Result<()> {
        init_logger();

        let future_elders = 1;
        let start_elder_count = elder_count() - future_elders;
        // setup section and a joiner
        let mut env = setup(start_elder_count).await;
        // as an old knowledge is used for next joiner, it will go through ae steps within the join process
        let old_network_knowledge = env.joiner.node.read().await.network_knowledge().clone();

        assert_eq!(env.section.len(), start_elder_count);

        for _ in 0..future_elders {
            // join the section and return resulting section and next joiner
            env = env.join().await?;
        }

        assert_eq!(env.section.len(), elder_count());

        assert_eq!(old_network_knowledge.elders().len(), start_elder_count);

        // replace the joiner with a new one, with old network knowledge
        env.joiner = joiner(old_network_knowledge).await;

        let final_env = env.join().await?;

        assert_eq!(
            final_env.section.len(),
            start_elder_count + future_elders + 1
        );

        Ok(())
    }

    // =========================================================================
    // ========================== Test helpers ===============================
    // =========================================================================

    struct JoinEnv {
        joiner: TestNode,
        section: Vec<TestNode>,
    }

    struct TestNode {
        node: Arc<RwLock<MyNode>>,
        cmd_channel: Sender<(Cmd, Vec<usize>)>,
        rejoin_rx: Receiver<RejoinReason>,
    }

    impl JoinEnv {
        async fn join(mut self) -> Result<JoinEnv> {
            self.joiner
                .cmd_channel
                .send((Cmd::TryJoinNetwork, vec![]))
                .await
                .map_err(|e| {
                    error!("Failed join: {:?}", e);
                    crate::node::Error::JoinTimeout
                })?;

            tokio::time::timeout(JOIN_TIMEOUT, await_join(self.joiner.node.clone()))
                .await
                .map_err(|e| {
                    error!("Failed join: {:?}", e);
                    crate::node::Error::JoinTimeout
                })?;

            assert_matches!(self.joiner.rejoin_rx.try_recv(), Err(TryRecvError::Empty));

            let network_knowledge = self.joiner.node.read().await.network_knowledge().clone();

            self.section.push(self.joiner);

            Ok(JoinEnv {
                joiner: joiner(network_knowledge).await,
                section: self.section,
            })
        }
    }

    async fn setup(elders: usize) -> JoinEnv {
        let (network_knowledge, section) = section(elders).await;
        JoinEnv {
            joiner: joiner(network_knowledge).await,
            section,
        }
    }

    async fn joiner(network_knowledge: NetworkKnowledge) -> TestNode {
        let prefix = network_knowledge.prefix();
        let (info, comm, incoming_msg_receiver) =
            TestNetwork::gen_info(MIN_ADULT_AGE, Some(prefix));
        let node = TestNetwork::build_a_node_instance(&info, &comm, &network_knowledge);
        connect_flows(node, incoming_msg_receiver).await
    }

    async fn section(elders: usize) -> (NetworkKnowledge, Vec<TestNode>) {
        let prefix = Prefix::default();
        let mut env = TestNetworkBuilder::new(thread_rng())
            .sap(prefix, elders, 0, None, None)
            .build();

        let network_knowledge = env.get_network_knowledge(prefix, None);

        let section = join_all(
            env.get_nodes(prefix, elders, 0, None)
                .into_iter()
                .map(|node| {
                    let public_key = node.info().public_key();
                    connect_flows(node, env.take_comm_rx(public_key))
                }),
        )
        .await;

        (network_knowledge, section)
    }

    async fn connect_flows(node: MyNode, incoming_msg_receiver: Receiver<CommEvent>) -> TestNode {
        let node = Arc::new(RwLock::new(node));
        let (dispatcher, data_replication_receiver) = Dispatcher::new();
        let cmd_ctrl = CmdCtrl::new(dispatcher);
        let (cmd_channel, rejoin_rx) = FlowCtrl::start(
            node,
            cmd_ctrl,
            JOIN_TIMEOUT,
            incoming_msg_receiver,
            data_replication_receiver,
            mpsc::channel(10),
        )
        .await;

        TestNode {
            node,
            cmd_channel,
            rejoin_rx,
        }
    }

    async fn await_join(node: Arc<RwLock<MyNode>>) {
        let mut is_member = false;
        while !is_member {
            let read_only = node.read().await;
            let our_name = read_only.name();
            is_member = read_only.network_knowledge.is_section_member(&our_name);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
