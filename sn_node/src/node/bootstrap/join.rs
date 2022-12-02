// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::comm::{Comm, MsgFromPeer};
use crate::log_sleep;
use crate::node::{messages::WireMsgUtils, Error, Result, STANDARD_CHANNEL_SIZE};

use sn_interface::{
    messaging::{
        system::{JoinRejectionReason, JoinRequest, JoinResponse, NodeMsg},
        Dst, MsgType, WireMsg,
    },
    network_knowledge::{
        MembershipState, MyNodeInfo, NetworkKnowledge, SectionTree, SectionTreeUpdate,
    },
    types::{keys::ed25519, log_markers::LogMarker, Peer},
    SectionAuthorityProvider,
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use futures::future;

use std::collections::BTreeSet;
use tokio::{sync::mpsc, time::Duration};
use tracing::Instrument;

/// Join the network as new node.
///
/// NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
/// lost in transit or other reasons. It's the responsibility of the caller to handle this case,
/// for example by using a timeout.
pub(crate) async fn join_network(
    node: MyNodeInfo,
    comm: &Comm,
    incoming_msgs: &mut mpsc::Receiver<MsgFromPeer>,
    section_tree: SectionTree,
    join_timeout: Duration,
) -> Result<(MyNodeInfo, NetworkKnowledge)> {
    let (outgoing_msgs_sender, outgoing_msgs_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);

    let span = trace_span!("bootstrap");
    let joiner = Joiner::new(node, outgoing_msgs_sender, incoming_msgs, section_tree);

    let (res, _) = future::join(
        joiner.try_join(join_timeout),
        send_messages(outgoing_msgs_receiver, comm),
    )
    .instrument(span)
    .await;

    match res {
        Ok(node) => Ok(node),
        Err(error) => {
            // We need to manually closing endpoint or listeners will persist
            comm.our_endpoint.close();
            Err(error)
        }
    }
}

struct Joiner<'a> {
    // Sender for outgoing messages.
    outgoing_msgs: mpsc::Sender<(WireMsg, Vec<Peer>)>,
    // Receiver for incoming messages.
    incoming_msgs: &'a mut mpsc::Receiver<MsgFromPeer>,
    node: MyNodeInfo,
    section_tree: SectionTree,
    backoff: ExponentialBackoff,
    /// cache of retry response sending peers. When we exceed 1/3rd responses we retry
    /// (the rety_response cache makes sure we retry only once per name/sap)
    retry_responses_cache: BTreeSet<Peer>,
}

impl<'a> Joiner<'a> {
    fn new(
        node: MyNodeInfo,
        outgoing_msgs: mpsc::Sender<(WireMsg, Vec<Peer>)>,
        incoming_msgs: &'a mut mpsc::Receiver<MsgFromPeer>,
        section_tree: SectionTree,
    ) -> Self {
        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(50),
            max_interval: Duration::from_millis(750),
            max_elapsed_time: Some(Duration::from_secs(60)),
            ..Default::default()
        };

        // this seems needed for custom settings to take effect
        backoff.reset();

        Self {
            outgoing_msgs,
            incoming_msgs,
            node,
            section_tree,
            backoff,
            retry_responses_cache: Default::default(),
        }
    }

    // Send `JoinRequest` and wait for the response. If the response is:
    // - `Retry`: repeat with the new info.
    // - `Redirect`: repeat with the new set of addresses.
    // - `Approval`: returns the decision proving the node was approved.
    async fn try_join(self, join_timeout: Duration) -> Result<(MyNodeInfo, NetworkKnowledge)> {
        trace!(
            "Bootstrap run, section tree as we have it: {:?}",
            self.section_tree
        );

        tokio::time::timeout(join_timeout, self.join(join_timeout / 10))
            .await
            .map_err(|e| {
                error!("Failed join: {:?}", e);
                Error::JoinTimeout
            })?
    }

    fn join_target_sap(&self) -> Result<SectionAuthorityProvider> {
        let our_name = self.node.name();
        let sap = self.section_tree.section_by_name(&our_name)?;
        Ok(sap)
    }

    #[tracing::instrument(skip(self))]
    async fn join(mut self, response_timeout: Duration) -> Result<(MyNodeInfo, NetworkKnowledge)> {
        self.bootstrap_section_tree(self.join_target_sap()?, response_timeout)
            .await?;

        let mut target_sap = self.join_target_sap()?;
        let section_key = target_sap.section_key();
        let msg = NodeMsg::JoinRequest(JoinRequest { section_key });
        self.send(msg, &target_sap.elders_vec(), section_key, false)
            .await?;

        loop {
            let (response, sender) =
                tokio::time::timeout(response_timeout, self.receive_join_response())
                    .await
                    .map_err(|e| {
                        error!("Failed to receive join response: {:?}", e);
                        Error::JoinTimeout
                    })??;

            match response {
                JoinResponse::Approved { decision } => {
                    info!("{}", LogMarker::ReceivedJoinApproval);
                    if let Err(e) = decision.validate(&target_sap.public_key_set()) {
                        error!("Dropping invalid join decision: {e:?}");
                        continue;
                    }

                    // Ensure this decision includes us as a joining node
                    if decision
                        .proposals
                        .keys()
                        .filter(|n| n.state() == MembershipState::Joined)
                        .all(|n| n.name() != self.node.name())
                    {
                        trace!("MyNode named: {:?} Ignore join approval decision not for us: {decision:?}", self.node.name());
                        continue;
                    }

                    trace!(
                        "=========>> This node has been approved to join the network at {:?}!",
                        target_sap.prefix(),
                    );

                    // TODO: NetworkKnowledge::new(..) should not be taking the section_tree_update
                    let section_tree_update = self
                        .section_tree
                        .generate_section_tree_update(&target_sap.prefix())?;

                    let network_knowledge =
                        NetworkKnowledge::new(self.section_tree, section_tree_update)?;

                    return Ok((self.node, network_knowledge));
                }
                JoinResponse::Retry => {
                    trace!(
                        "My joining node with {:?} - name: {:?} ; received a Retry from {}",
                        target_sap.prefix(),
                        self.node.name(),
                        sender.name(),
                    );

                    if self.should_retry_after_response(sender, target_sap.elders_set()) {
                        self.retry_responses_cache = Default::default();

                        trace!("Bootstrapping section tree for retry");
                        self.bootstrap_section_tree(target_sap, response_timeout)
                            .await?;
                        target_sap = self.join_target_sap()?;

                        let new_keypair = ed25519::gen_keypair(
                            &target_sap.prefix().range_inclusive(),
                            self.node.age(),
                        );
                        self.node = MyNodeInfo::new(new_keypair, self.node.addr);

                        info!("Retrying with new name: {}", self.node.name());

                        let section_key = target_sap.section_key();
                        let msg = NodeMsg::JoinRequest(JoinRequest { section_key });
                        self.send(msg, &target_sap.elders_vec(), section_key, true)
                            .await?;
                    }
                }
                JoinResponse::Redirect(section_auth) => {
                    // TODO: Replace Redirect with a Retry + AEProbe.
                    trace!("Received a redirect JoinResponse from {}. Sending request to the latest contacts", sender);
                    if section_auth.elders().next().is_none() {
                        error!(
                            "Invalid JoinResponse::Redirect, empty list of Elders: {:?}",
                            section_auth
                        );
                        continue;
                    }

                    if !section_auth.prefix().matches(&self.node.name()) {
                        warn!(
                            "Ignoring newer JoinResponse::Redirect response not for us {:?}, SAP {:?} from {:?}",
                            self.node.name(),
                            section_auth,
                            sender,
                        );
                        continue;
                    }

                    info!(
                        "Newer JoinResponse::Redirect for us {:?}, SAP {:?} from {:?}",
                        self.node.name(),
                        section_auth,
                        sender
                    );

                    self.bootstrap_section_tree(section_auth, response_timeout)
                        .await?;

                    let target_sap = self.join_target_sap()?;
                    let section_key = target_sap.section_key();

                    let msg = NodeMsg::JoinRequest(JoinRequest { section_key });
                    self.send(msg, &target_sap.elders_vec(), section_key, true)
                        .await?;
                }
                JoinResponse::Rejected(JoinRejectionReason::JoinsDisallowed) => {
                    error!("Network is set to not taking any new joining node, try join later.");
                    return Err(Error::TryJoinLater);
                }
                JoinResponse::Rejected(JoinRejectionReason::NodeNotReachable(addr)) => {
                    error!("Join rejected since node is not externally reachable: {addr}");
                    return Err(Error::NodeNotReachable(addr));
                }
            }
        }
    }

    async fn bootstrap_section_tree(
        &mut self,
        initial_target_sap: SectionAuthorityProvider,
        response_timeout: Duration,
    ) -> Result<()> {
        let mut target_sap = initial_target_sap;
        loop {
            self.send(
                NodeMsg::AntiEntropyProbe(target_sap.section_key()),
                &target_sap.elders_vec(),
                target_sap.section_key(),
                true,
            )
            .await?;

            // We wait till we receive a threshold of updates.

            let mut any_new_information = false;

            for _ in 0..=target_sap.public_key_set().threshold() {
                let update =
                    tokio::time::timeout(response_timeout, self.receive_section_tree_update())
                        .await
                        .map_err(|_| Error::JoinTimeout)??;

                info!("Received section tree update: {update:?}");

                any_new_information = self.section_tree.update(update)?;

                if any_new_information {
                    break;
                }
            }

            if any_new_information {
                // Update the target sap since we've received new information and try again.
                target_sap = self.join_target_sap()?;
                info!("Received new information in last AEProbe, updating target sap to {target_sap:?}");
            } else {
                // We are up to date with these nodes so we can end the bootstrap
                info!("No new information was learned in last AEProbe, ending tree bootstrap");
                return Ok(());
            }
        }
    }

    // We'll restart the join process once we receive Retry responses from >1/3 of elders
    fn should_retry_after_response(&mut self, sender: Peer, elders: BTreeSet<Peer>) -> bool {
        if !elders.contains(&sender) {
            error!("Sender {sender:?} of the retry-response is not part of the elders {elders:?}");
            return false;
        }
        let _ = self.retry_responses_cache.insert(sender);

        self.retry_responses_cache.len() > elders.len() / 3
    }

    #[tracing::instrument(skip(self))]
    async fn send(
        &mut self,
        msg: NodeMsg,
        recipients: &[Peer],
        section_key: BlsPublicKey,
        should_backoff: bool,
    ) -> Result<()> {
        if should_backoff {
            // use exponential backoff here to delay our responses and avoid any intensive join reqs
            let next_wait = self.backoff.next_backoff();

            if let Some(wait) = next_wait {
                log_sleep!(Duration::from_millis(wait.as_millis() as u64));
            } else {
                error!("Waiting before attempting to join again");
                log_sleep!(Duration::from_millis(
                    self.backoff.max_interval.as_millis() as u64
                ));
                self.backoff.reset();
            }
        }

        info!(
            "Sending {msg:?} to {:?}",
            Vec::from_iter(recipients.iter().map(Peer::name))
        );

        let wire_msg = WireMsg::single_src(
            &self.node,
            Dst {
                name: self.node.name(), // we want to target a section where our name fits
                section_key,
            },
            msg,
        )?;

        let _res = self
            .outgoing_msgs
            .send((wire_msg, recipients.to_vec()))
            .await;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn receive_join_response(&mut self) -> Result<(JoinResponse, Peer)> {
        loop {
            let (msg, sender) = self.receive_node_msg().await?;
            match msg {
                NodeMsg::JoinResponse(resp) => return Ok((resp, sender)),
                _ => {
                    trace!(
                        "Non-JoinResponse message received and discarded: sender: {sender:?} msg: {msg:?}"
                    )
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn receive_section_tree_update(&mut self) -> Result<SectionTreeUpdate> {
        loop {
            let (msg, sender) = self.receive_node_msg().await?;
            match msg {
                NodeMsg::AntiEntropy {
                    section_tree_update,
                    ..
                } => return Ok(section_tree_update),
                _ => {
                    trace!(
                        "Non-SectionTreeUpdate message discarded: sender: {sender:?} msg: {msg:?}"
                    )
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn receive_node_msg(&mut self) -> Result<(NodeMsg, Peer)> {
        while let Some(MsgFromPeer {
            sender, wire_msg, ..
        }) = self.incoming_msgs.recv().await
        {
            // We are interested only in `Node` type of messages
            match wire_msg.into_msg()? {
                MsgType::Node { msg, .. } => return Ok((msg, sender)),
                MsgType::Client { msg_id, .. }
                | MsgType::ClientDataResponse { msg_id, .. }
                | MsgType::NodeDataResponse { msg_id, .. } => {
                    trace!("Non-NodeMsg bootstrap message discarded: sender: {sender:?} msg_id: {msg_id:?}")
                }
            };
        }

        error!("NodeMsg sender unexpectedly closed");
        Err(Error::BootstrapConnectionClosed)
    }
}

// Keep reading messages from `rx` and send them using `comm`.
async fn send_messages(
    mut outgoing_msgs: mpsc::Receiver<(WireMsg, Vec<Peer>)>,
    comm: &Comm,
) -> Result<()> {
    while let Some((msg, peers)) = outgoing_msgs.recv().await {
        for peer in peers {
            let dst = *msg.dst();
            let msg_id = msg.msg_id();

            let bytes = msg.serialize()?;
            match comm.send_out_bytes(peer, msg_id, bytes, None).await {
                Ok(()) => trace!("Msg {msg_id:?} sent on {dst:?}"),
                Err(error) => {
                    warn!("Error in comms when sending msg {msg_id:?} to peer {peer:?}: {error}")
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{messages::WireMsgUtils, Error as RoutingError, MIN_ADULT_AGE};
    use assert_matches::assert_matches;
    use eyre::{eyre, Result};
    use futures::{
        future::{self, Either},
        pin_mut,
    };
    use sn_interface::{
        init_logger,
        messaging::system::AntiEntropyKind,
        network_knowledge::{NodeState, SectionAuthorityProvider, SectionsDAG},
        test_utils::*,
        types::PublicKey,
    };
    use tokio::task;
    use xor_name::{Prefix, XorName};

    const JOIN_TIMEOUT_SEC: u64 = 15;

    #[tokio::test]
    async fn join_as_adult() {
        init_logger();

        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(10);

        let (genesis_sap, genesis_sk_set, genesis_elders, ..) =
            TestSapBuilder::new(Prefix::default()).build();
        let genesis_sk = genesis_sk_set.secret_key();
        let genesis_pk = genesis_sk.public_key();

        let node = MyNodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let signed_genesis_sap = TestKeys::get_section_signed(&genesis_sk, genesis_sap.clone());
        let mut tree = SectionTree::new(genesis_pk);
        assert!(tree.insert_without_chain(signed_genesis_sap));

        let state = Joiner::new(node.clone(), send_tx, &mut recv_rx, tree);

        // Create the bootstrap task, but don't run it yet.
        let bootstrap = async { state.try_join(join_timeout).await.expect("Failed to join") };

        let (next_sap, next_sk_set, next_elders, _) =
            TestSapBuilder::new(Prefix::default()).build();

        let next_section_key = next_sk_set.public_keys().public_key();
        let section_tree_update = TestSectionTree::get_section_tree_update(
            &TestKeys::get_section_signed(&next_sk_set.secret_key(), next_sap.clone()),
            &SectionsDAG::new(genesis_pk),
            &genesis_sk,
        );

        // Create the task that executes the body of the test, but don't run it either.
        let others = async {
            // First the joining node bootstraps it's network knowledge.
            // We expect two probes, one to the genesis elders, then another to the next sap elders.

            for expected_elders in [genesis_elders, next_elders.clone()] {
                let (node_msg, _, recipients) = recv_node_msg(&mut send_rx).await;

                assert_eq!(
                    BTreeSet::from_iter(recipients),
                    BTreeSet::from_iter(expected_elders.iter().map(|e| e.peer())),
                );
                assert_matches!(node_msg, NodeMsg::AntiEntropyProbe(_));

                info!("Received anti-entropy probe");

                let ae_update_msg = NodeMsg::AntiEntropy {
                    section_tree_update: section_tree_update.clone(),
                    kind: AntiEntropyKind::Update {
                        members: Default::default(),
                    },
                };

                for elder in expected_elders.iter() {
                    send_node_msg(&recv_tx, ae_update_msg.clone(), elder, next_section_key);
                }
            }

            info!("Waiting on Join Request");

            // Receive the second JoinRequest with correct section info
            let (node_msg, dst, recipients) = recv_node_msg(&mut send_rx).await;

            itertools::assert_equal(recipients, next_sap.elders());
            assert_eq!(dst.section_key, next_section_key);
            assert_matches!(node_msg, NodeMsg::JoinRequest(JoinRequest{ section_key }) => {
                assert_eq!(section_key, next_section_key);
            });

            // Name changed
            let new_peer = Peer::new(dst.name, node.peer().addr());
            // Send JoinResponse::Approved
            let decision = section_decision(&next_sk_set, NodeState::joined(new_peer, None));
            send_join_response(
                &recv_tx,
                JoinResponse::Approved { decision },
                &next_elders[0],
                next_sap.section_key(),
            );
        };

        // Drive both tasks to completion concurrently (but on the same thread).
        let ((node, section), _) = future::join(bootstrap, others).await;

        assert_eq!(section.section_auth(), next_sap);
        assert_eq!(section.section_key(), next_section_key);
        assert_eq!(node.age(), MIN_ADULT_AGE);
    }

    #[tokio::test]
    async fn join_receive_redirect_response() -> Result<()> {
        init_logger();

        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(10);
        let (recv_tx, mut recv_rx) = mpsc::channel(10);

        let (genesis_sap, genesis_sk_set, genesis_nodes, _) =
            TestSapBuilder::new(Prefix::default()).build();
        let genesis_sk = genesis_sk_set.secret_key();
        let genesis_pk = genesis_sk.public_key();

        let node = MyNodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let signed_genesis_sap = TestKeys::get_section_signed(&genesis_sk, genesis_sap.clone());
        let mut tree = SectionTree::new(genesis_pk);
        assert!(tree.insert_without_chain(signed_genesis_sap));

        let state = Joiner::new(node, send_tx, &mut recv_rx, tree.clone());

        let bootstrap_task = state.try_join(join_timeout);
        let test_task = async move {
            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::AntiEntropyProbe { .. });

            let section_tree_update = tree
                .generate_section_tree_update(&prefix(""))
                .expect("Failed to create update");

            let ae_msg = NodeMsg::AntiEntropy {
                section_tree_update,
                kind: AntiEntropyKind::Update {
                    members: Default::default(),
                },
            };

            for node in genesis_nodes.iter() {
                send_node_msg(&recv_tx, ae_msg.clone(), node, genesis_sap.section_key());
            }

            // Receive JoinRequest
            let (node_msg, _, recipients) = recv_node_msg(&mut send_rx).await;
            itertools::assert_equal(recipients, genesis_sap.elders());

            assert_matches!(node_msg, NodeMsg::JoinRequest { .. });

            // Send JoinResponse::Redirect
            let (new_sap, sks, ..) = TestSapBuilder::new(Prefix::default()).build();

            send_join_response(
                &recv_tx,
                JoinResponse::Redirect(new_sap.clone()),
                &genesis_nodes[0],
                new_sap.section_key(),
            );

            task::yield_now().await;

            let mut proof_chain = SectionsDAG::new(genesis_pk);

            proof_chain
                .insert(
                    &genesis_pk,
                    new_sap.section_key(),
                    TestKeys::get_section_signed(&genesis_sk, new_sap.section_key())
                        .sig
                        .signature,
                )
                .expect("Bad proof chain insert");

            let section_tree_update = SectionTreeUpdate {
                signed_sap: TestKeys::get_section_signed(&sks.secret_key(), new_sap.clone()),
                proof_chain,
            };

            let ae_msg = NodeMsg::AntiEntropy {
                section_tree_update,
                kind: AntiEntropyKind::Update {
                    members: Default::default(),
                },
            };

            for _ in 0..2 {
                let (node_msg, dst, recipients) = recv_node_msg(&mut send_rx).await;
                itertools::assert_equal(recipients, new_sap.elders());
                assert_eq!(dst.section_key, new_sap.section_key());

                assert_matches!(node_msg, NodeMsg::AntiEntropyProbe { .. });

                for node in genesis_nodes.iter() {
                    send_node_msg(&recv_tx, ae_msg.clone(), node, genesis_sap.section_key());
                }
            }

            // Receive new JoinRequest with redirected bootstrap contacts
            let (node_msg, dst, recipients) = recv_node_msg(&mut send_rx).await;

            itertools::assert_equal(recipients, new_sap.elders());

            assert_eq!(dst.section_key, new_sap.section_key());
            assert_matches!(node_msg, NodeMsg::JoinRequest(JoinRequest{ section_key }) => {
                assert_eq!(section_key, new_sap.section_key());
            });

            Ok(())
        };

        pin_mut!(bootstrap_task);
        pin_mut!(test_task);

        match future::select(bootstrap_task, test_task).await {
            Either::Left((res, _)) => panic!("Bootstrap should not have finished: {res:?}"),
            Either::Right((output, _)) => output,
        }
    }

    #[tokio::test]
    async fn join_invalid_redirect_response() -> Result<()> {
        init_logger();
        let _span = tracing::info_span!("join_invalid_redirect_response").entered();

        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(10);
        let (recv_tx, mut recv_rx) = mpsc::channel(10);

        let (genesis_sap, genesis_sk_set, genesis_nodes, _) =
            TestSapBuilder::new(Prefix::default()).build();
        let genesis_sk = genesis_sk_set.secret_key();
        let genesis_pk = genesis_sk.public_key();

        let node = MyNodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let signed_genesis_sap = TestKeys::get_section_signed(&genesis_sk, genesis_sap.clone());
        let mut tree = SectionTree::new(genesis_pk);
        assert!(tree.insert_without_chain(signed_genesis_sap));

        let state = Joiner::new(node, send_tx, &mut recv_rx, tree.clone());

        let bootstrap_task = state.try_join(join_timeout);
        let test_task = async {
            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::AntiEntropyProbe { .. });

            let section_tree_update = tree
                .generate_section_tree_update(&prefix(""))
                .expect("Failed to create update");

            let ae_msg = NodeMsg::AntiEntropy {
                section_tree_update,
                kind: AntiEntropyKind::Update {
                    members: Default::default(),
                },
            };

            for node in genesis_nodes.iter() {
                send_node_msg(&recv_tx, ae_msg.clone(), node, genesis_sap.section_key());
            }

            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;

            assert_matches!(node_msg, NodeMsg::JoinRequest { .. });

            let (new_sap, new_sk_set, ..) = TestSapBuilder::new(Prefix::default()).build();
            let new_pk_set = new_sk_set.public_keys();

            send_join_response(
                &recv_tx,
                JoinResponse::Redirect(SectionAuthorityProvider::new(
                    BTreeSet::new(),
                    Prefix::default(),
                    BTreeSet::new(),
                    new_pk_set.clone(),
                    0,
                )),
                &genesis_nodes[0],
                new_sap.section_key(),
            );
            task::yield_now().await;

            send_join_response(
                &recv_tx,
                JoinResponse::Redirect(new_sap.clone()),
                &genesis_nodes[0],
                new_sap.section_key(),
            );
            task::yield_now().await;

            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::AntiEntropyProbe { .. });

            for node in genesis_nodes.iter() {
                send_node_msg(&recv_tx, ae_msg.clone(), node, genesis_sap.section_key());
            }

            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::JoinRequest { .. });

            Ok(())
        };

        pin_mut!(bootstrap_task);
        pin_mut!(test_task);

        match future::select(bootstrap_task, test_task).await {
            Either::Left((res, _)) => panic!("Bootstrap should not have finished {res:?}"),
            Either::Right((output, _)) => output,
        }
    }

    #[tokio::test]
    async fn join_disallowed_response() -> Result<()> {
        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(10);
        let (recv_tx, mut recv_rx) = mpsc::channel(10);

        let (genesis_sap, genesis_sk_set, genesis_nodes, _) =
            TestSapBuilder::new(Prefix::default()).build();
        let genesis_sk = genesis_sk_set.secret_key();
        let genesis_pk = genesis_sk.public_key();

        let node = MyNodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let signed_genesis_sap = TestKeys::get_section_signed(&genesis_sk, genesis_sap.clone());
        let mut tree = SectionTree::new(genesis_pk);
        assert!(tree.insert_without_chain(signed_genesis_sap));

        let state = Joiner::new(node, send_tx, &mut recv_rx, tree.clone());

        let bootstrap_task = state.try_join(join_timeout);
        let test_task = async {
            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::AntiEntropyProbe { .. });

            let section_tree_update = tree
                .generate_section_tree_update(&prefix(""))
                .expect("Failed to create update");

            let ae_msg = NodeMsg::AntiEntropy {
                section_tree_update,
                kind: AntiEntropyKind::Update {
                    members: Default::default(),
                },
            };

            for node in genesis_nodes.iter() {
                send_node_msg(&recv_tx, ae_msg.clone(), node, genesis_sap.section_key());
            }

            let (node_msg, _, _) = recv_node_msg(&mut send_rx).await;
            assert_matches!(node_msg, NodeMsg::JoinRequest { .. });

            send_join_response(
                &recv_tx,
                JoinResponse::Rejected(JoinRejectionReason::JoinsDisallowed),
                &genesis_nodes[0],
                genesis_sap.section_key(),
            );

            Ok(())
        };

        let (join_result, test_result) = future::join(bootstrap_task, test_task).await;

        if let Err(RoutingError::TryJoinLater) = join_result {
        } else {
            return Err(eyre!("Not getting an execpted network rejection."));
        }

        test_result
    }

    async fn recv_node_msg(
        channel: &mut mpsc::Receiver<(WireMsg, Vec<Peer>)>,
    ) -> (NodeMsg, Dst, Vec<Peer>) {
        let (wire_msg, recipients) = channel.recv().await.expect("Should have received node msg");
        let msg = wire_msg.into_msg().expect("Failed to decode msg");
        let (node_msg, dst) = assert_matches!(msg, MsgType::Node { msg, dst,.. } => (msg, dst));
        (node_msg, dst, recipients)
    }

    fn send_join_response(
        recv_tx: &mpsc::Sender<MsgFromPeer>,
        resp: JoinResponse,
        sender: &MyNodeInfo,
        section_pk: BlsPublicKey,
    ) {
        let node_msg = NodeMsg::JoinResponse(resp);
        send_node_msg(recv_tx, node_msg, sender, section_pk);
    }

    // test helper
    #[instrument]
    fn send_node_msg(
        recv_tx: &mpsc::Sender<MsgFromPeer>,
        msg: NodeMsg,
        sender: &MyNodeInfo,
        section_pk: BlsPublicKey,
    ) {
        let wire_msg = WireMsg::single_src(
            sender,
            Dst {
                name: XorName::from(PublicKey::Bls(section_pk)),
                section_key: section_pk,
            },
            msg,
        )
        .expect("Failed to build wire msg");

        recv_tx
            .try_send(MsgFromPeer {
                sender: sender.peer(),
                wire_msg,
                send_stream: None,
            })
            .expect("Failed to send message");
    }
}
