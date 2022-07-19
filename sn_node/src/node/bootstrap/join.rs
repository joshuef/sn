// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::UsedRecipientSaps;
use crate::comm::{Comm, DeliveryStatus, MsgEvent};

use crate::node::{messages::WireMsgUtils, Error, Result};

use sn_interface::{
    messaging::{
        system::{
            JoinRejectionReason, JoinRequest, JoinResponse, ResourceProofResponse, SectionAuth,
            SystemMsg,
        },
        AuthKind, DstLocation, MsgType, NodeAuth, WireMsg,
    },
    network_knowledge::{
        prefix_map::NetworkPrefixMap, NetworkKnowledge, NodeInfo, SectionAuthUtils, MIN_ADULT_AGE,
    },
    types::{keys::ed25519, log_markers::LogMarker, Peer},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use futures::future;
use resource_proof::ResourceProof;
use std::net::SocketAddr;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration, Instant},
};
use tracing::Instrument;
use xor_name::Prefix;

/// Join the network as new node.
///
/// NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
/// lost in transit or other reasons. It's the responsibility of the caller to handle this case,
/// for example by using a timeout.
pub(crate) async fn join_network(
    node: NodeInfo,
    comm: &Comm,
    incoming_msgs: &mut mpsc::Receiver<MsgEvent>,
    bootstrap_addr: SocketAddr,
    prefix_map: NetworkPrefixMap,
    join_timeout: Duration,
) -> Result<(NodeInfo, NetworkKnowledge)> {
    let (outgoing_msgs_sender, outgoing_msgs_receiver) = mpsc::channel(1);

    let span = trace_span!("bootstrap");
    let joiner = Joiner::new(node, outgoing_msgs_sender, incoming_msgs, prefix_map);

    debug!("=========> attempting bootstrap to {bootstrap_addr}");
    future::join(
        joiner.try_join(bootstrap_addr, join_timeout),
        send_messages(outgoing_msgs_receiver, comm),
    )
    .instrument(span)
    .await
    .0
}

struct Joiner<'a> {
    // Sender for outgoing messages.
    outgoing_msgs: mpsc::Sender<(WireMsg, Vec<Peer>)>,
    // Receiver for incoming messages.
    incoming_msgs: &'a mut mpsc::Receiver<MsgEvent>,
    node: NodeInfo,
    prefix: Prefix,
    prefix_map: NetworkPrefixMap,
    backoff: ExponentialBackoff,
    aggregated: bool,
}

impl<'a> Joiner<'a> {
    fn new(
        node: NodeInfo,
        outgoing_msgs: mpsc::Sender<(WireMsg, Vec<Peer>)>,
        incoming_msgs: &'a mut mpsc::Receiver<MsgEvent>,
        prefix_map: NetworkPrefixMap,
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
            prefix: Prefix::default(),
            prefix_map,
            backoff,
            aggregated: false,
        }
    }

    // Send `JoinRequest` and wait for the response. If the response is:
    // - `Retry`: repeat with the new info.
    // - `Redirect`: repeat with the new set of addresses.
    // - `ResourceChallenge`: carry out resource proof calculation.
    // - `Approval`: returns the initial `Section` value to use by this node,
    //    completing the bootstrap.
    async fn try_join(
        self,
        bootstrap_addr: SocketAddr,
        join_timeout: Duration,
    ) -> Result<(NodeInfo, NetworkKnowledge)> {
        // Use our XorName as we do not know their name or section key yet.
        let bootstrap_peer = Peer::new(self.node.name(), bootstrap_addr);

        trace!(
            "Bootstrap run, prefixmap as we have it: {:?}",
            self.prefix_map
        );
        let genesis_key = self.prefix_map.genesis_key();

        let (target_section_key, recipients) =
            if let Ok(sap) = self.prefix_map.section_by_name(&bootstrap_peer.name()) {
                (sap.section_key(), sap.elders_vec())
            } else {
                (genesis_key, vec![bootstrap_peer])
            };

        self.join(genesis_key, target_section_key, recipients, join_timeout)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn join(
        mut self,
        network_genesis_key: BlsPublicKey,
        target_section_key: BlsPublicKey,
        recipients: Vec<Peer>,
        mut join_timeout: Duration,
    ) -> Result<(NodeInfo, NetworkKnowledge)> {
        // We first use genesis key as the target section key, we'll be getting
        // a response with the latest section key for us to retry with.
        // Once we are approved to join, we will make sure the SAP we receive can
        // be validated with the received proof chain and the 'network_genesis_key'.
        let mut section_key = target_section_key;

        // We send a first join request to obtain the resource challenge, which
        // we will then use to generate the challenge proof and send the
        // `JoinRequest` again with it.
        let join_request = JoinRequest {
            section_key,
            resource_proof_response: None,
        };

        let mut timer = Instant::now(); // start right before sending msgs

        self.send_join_requests(join_request.clone(), &recipients, section_key, false)
            .await?;

        // Avoid sending more than one duplicated request (with same SectionKey) to the same peer.
        let mut used_recipient_saps = UsedRecipientSaps::new();

        loop {
            // Breaks the loop raising an error, if join_timeout time elapses.
            join_timeout = join_timeout
                .checked_sub(timer.elapsed())
                .ok_or(Error::JoinTimeout)?;
            timer = Instant::now(); // reset timer

            let (response, sender) = self.receive_join_response(join_timeout).await?;

            match response {
                JoinResponse::Rejected(JoinRejectionReason::NodeNotReachable(addr)) => {
                    error!(
                        "Node cannot join the network since it is not externally reachable: {}",
                        addr
                    );
                    return Err(Error::NodeNotReachable(addr));
                }
                JoinResponse::Rejected(JoinRejectionReason::JoinsDisallowed) => {
                    error!("Network is set to not taking any new joining node, try join later.");
                    return Err(Error::TryJoinLater);
                }
                JoinResponse::Approval {
                    section_auth,
                    genesis_key,
                    section_chain,
                    node_state,
                } => {
                    info!("{}", LogMarker::ReceivedJoinApproval);
                    if node_state.name != self.node.name() {
                        trace!("Ignore NodeApproval not for us: {:?}", node_state);
                        continue;
                    }

                    if !node_state.verify(&section_chain) {
                        error!(
                            "Verification of node state in JoinResponse failed: {:?}",
                            node_state
                        );
                        continue;
                    }

                    trace!(
                        "This node has been approved to join the network at {:?}!",
                        section_auth.prefix,
                    );

                    // Building our network knowledge instance will validate SAP and section chain.
                    let section_auth = section_auth.into_authed_state();

                    let network_knowledge = NetworkKnowledge::new(
                        genesis_key,
                        section_chain,
                        section_auth,
                        Some(self.prefix_map),
                    )?;

                    return Ok((self.node, network_knowledge));
                }
                JoinResponse::Retry {
                    section_auth,
                    section_signed,
                    proof_chain,
                    expected_age,
                } => {
                    let section_auth = section_auth.into_state();

                    trace!(
                        "Joining node {:?} - {:?}/{:?} received a Retry from {} with SAP {:?}, expected_age: {}, our age: {}",
                        self.prefix,
                        self.node.name(),
                        self.node.age(),sender,
                        section_auth,
                        expected_age,
                        self.node.age()
                    );

                    let prefix = section_auth.prefix();
                    if !prefix.matches(&self.node.name()) {
                        warn!(
                            "Ignoring newer JoinResponse::Retry response not for us {:?}, SAP {:?} from {:?}",
                            self.node.name(),
                            section_auth,
                            sender,
                        );
                        continue;
                    }

                    let signed_sap = SectionAuth {
                        value: section_auth.clone(),
                        sig: section_signed,
                    };

                    // make sure we received a valid and trusted new SAP
                    let is_new_sap = match self.prefix_map.update(signed_sap, &proof_chain) {
                        Ok(updated) => updated,
                        Err(err) => {
                            debug!(
                                "Ignoring JoinResponse::Retry with an invalid SAP: {:?}",
                                err
                            );
                            continue;
                        }
                    };

                    // if it's not a new SAP, ignore response unless the expected age is different.
                    if self.node.age() != expected_age
                        && (self.node.age() > expected_age || self.node.age() <= MIN_ADULT_AGE)
                        && !self.aggregated
                    {
                        // adjust our joining age to the expected by the network
                        trace!(
                            "Re-generating name due to mis-matched age, current {} vs. expected {}",
                            self.node.age(),
                            expected_age
                        );
                        // The expected_age is a sequence of 98, 96, 94, 92, ...
                        // The prefix is deduced from the age's bits.
                        let mut cur_age = expected_age / 2;
                        let mut new_prefix = Prefix::default();
                        while cur_age > 0 {
                            let push_prefix_0 = cur_age % 2 == 1;
                            new_prefix = new_prefix.pushed(push_prefix_0);
                            cur_age /= 2;
                        }
                        trace!("Name shall have the prefix of {:?}", new_prefix);

                        let new_keypair =
                            ed25519::gen_keypair(&new_prefix.range_inclusive(), expected_age);
                        let new_name = ed25519::name(&new_keypair.public);

                        info!("Setting Node name to {} (age {})", new_name, expected_age);
                        self.node = NodeInfo::new(new_keypair, self.node.addr);
                    } else if !is_new_sap {
                        debug!("Ignoring JoinResponse::Retry with same SAP as we previously sent to: {:?}", section_auth);
                        continue;
                    }

                    info!(
                        "Newer Join response for us {:?}, SAP {:?} from {:?}",
                        self.node.name(),
                        section_auth,
                        sender
                    );

                    section_key = section_auth.section_key();
                    let join_request = JoinRequest {
                        section_key,
                        resource_proof_response: None,
                    };

                    let new_recipients = section_auth.elders_vec();
                    self.send_join_requests(join_request, &new_recipients, section_key, true)
                        .await?;
                }
                JoinResponse::Redirect(section_auth) => {
                    trace!("Received a redirect/retry JoinResponse from {}. Sending request to the latest contacts", sender);
                    if section_auth.elders.is_empty() {
                        error!(
                            "Invalid JoinResponse::Redirect, empty list of Elders: {:?}",
                            section_auth
                        );
                        continue;
                    }

                    let section_auth = section_auth.into_state();
                    if !section_auth.prefix().matches(&self.node.name()) {
                        warn!(
                            "Ignoring newer JoinResponse::Redirect response not for us {:?}, SAP {:?} from {:?}",
                            self.node.name(),
                            section_auth,
                            sender,
                        );
                        continue;
                    }

                    let new_section_key = section_auth.section_key();
                    let new_recipients: Vec<_> = section_auth
                        .elders()
                        .filter(|peer| used_recipient_saps.insert((peer.addr(), new_section_key)))
                        .cloned()
                        .collect();

                    if new_recipients.is_empty() {
                        debug!(
                            "Ignoring JoinResponse::Redirect with old SAP that has been sent to: {:?}",
                            section_auth
                        );
                        continue;
                    }

                    info!(
                        "Newer JoinResponse::Redirect for us {:?}, SAP {:?} from {:?}",
                        self.node.name(),
                        section_auth,
                        sender
                    );

                    section_key = new_section_key;
                    self.prefix = section_auth.prefix();

                    let join_request = JoinRequest {
                        section_key,
                        resource_proof_response: None,
                    };

                    self.send_join_requests(join_request, &new_recipients, section_key, true)
                        .await?;
                }
                JoinResponse::ResourceChallenge {
                    data_size,
                    difficulty,
                    nonce,
                    nonce_signature,
                } => {
                    trace!("Received a ResourceChallenge from {}", sender);
                    let rp = ResourceProof::new(data_size, difficulty);
                    let data = rp.create_proof_data(&nonce);
                    let mut prover = rp.create_prover(data.clone());
                    let solution = prover.solve();

                    let join_request = JoinRequest {
                        section_key,
                        resource_proof_response: Some(ResourceProofResponse {
                            solution,
                            data,
                            nonce,
                            nonce_signature,
                        }),
                    };
                    let recipients = &[sender];
                    self.send_join_requests(join_request, recipients, section_key, false)
                        .await?;
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn send_join_requests(
        &mut self,
        join_request: JoinRequest,
        recipients: &[Peer],
        section_key: BlsPublicKey,
        should_backoff: bool,
    ) -> Result<()> {
        if should_backoff {
            // use exponential backoff here to delay our responses and avoid any intensive join reqs
            let next_wait = self.backoff.next_backoff();

            if let Some(wait) = next_wait {
                sleep(wait).await;
            } else {
                error!("Waiting before attempting to join again");

                sleep(self.backoff.max_interval).await;
                self.backoff.reset();
            }
        }

        info!("Sending {:?} to {:?}", join_request, recipients);

        let node_msg = SystemMsg::JoinRequest(Box::new(join_request));
        let wire_msg = WireMsg::single_src(
            &self.node,
            DstLocation::Section {
                name: self.node.name(),
                section_pk: section_key,
            },
            node_msg,
            section_key,
        )?;

        let _res = self
            .outgoing_msgs
            .send((wire_msg, recipients.to_vec()))
            .await;

        Ok(())
    }

    // TODO: receive JoinResponse from the JoinResponse handler directly,
    // analogous to the JoinAsRelocated flow.
    #[tracing::instrument(skip(self))]
    async fn receive_join_response(
        &mut self,
        mut join_timeout: Duration,
    ) -> Result<(JoinResponse, Peer)> {
        let mut timer = Instant::now();

        // Awaits at most the time left of join_timeout.
        while let Some(event) = tokio::time::timeout(join_timeout, self.incoming_msgs.recv())
            .await
            .map_err(|_| Error::JoinTimeout)?
        {
            // Breaks the loop raising an error, if join_timeout time elapses.
            join_timeout = join_timeout
                .checked_sub(timer.elapsed())
                .ok_or(Error::JoinTimeout)?;
            timer = Instant::now(); // reset timer

            // We are interested only in `JoinResponse` type of messages
            let (join_response, sender) = match event {
                MsgEvent::Received {
                    sender, wire_msg, ..
                } => match wire_msg.auth_kind() {
                    AuthKind::Service(_) => continue,
                    AuthKind::NodeBlsShare(_) => {
                        trace!(
                            "Bootstrap message discarded: sender: {:?} wire_msg: {:?}",
                            sender,
                            wire_msg
                        );
                        continue;
                    }
                    AuthKind::Node(NodeAuth { .. }) => match wire_msg.into_msg() {
                        Ok(MsgType::System {
                            msg: SystemMsg::JoinResponse(resp),
                            ..
                        }) => (*resp, sender),
                        Ok(MsgType::Service { msg_id, .. } | MsgType::System { msg_id, .. }) => {
                            trace!(
                                "Bootstrap message discarded: sender: {:?} msg_id: {:?}",
                                sender,
                                msg_id
                            );
                            continue;
                        }
                        Err(err) => {
                            debug!("Failed to deserialize message payload: {:?}", err);
                            continue;
                        }
                    },
                },
            };

            return Ok((join_response, sender));
        }

        error!("NodeMsg sender unexpectedly closed");
        // TODO: consider more specific error here (e.g. `BootstrapInterrupted`)
        Err(Error::InvalidState)
    }
}

// Keep reading messages from `rx` and send them using `comm`.
async fn send_messages(
    mut outgoing_msgs: mpsc::Receiver<(WireMsg, Vec<Peer>)>,
    comm: &Comm,
) -> Result<()> {
    while let Some((wire_msg, recipients)) = outgoing_msgs.recv().await {
        match comm.send(&recipients, wire_msg.clone()).await {
            Ok(DeliveryStatus::AllRecipients) | Ok(DeliveryStatus::DeliveredToAll(_)) => {}
            Ok(DeliveryStatus::FailedToDeliverAll(recipients)) => {
                error!("Failed to send message {:?} to {:?}", wire_msg, recipients)
            }
            Err(err) => {
                error!(
                    "Failed to send message {:?} to {:?}: {:?}",
                    wire_msg, recipients, err
                )
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::node::{messages::WireMsgUtils, Error as RoutingError, MIN_ADULT_AGE};

    use sn_interface::{
        elder_count, init_logger,
        messaging::SectionAuthorityProvider as SectionAuthorityProviderMsg,
        network_knowledge::{test_utils::*, NodeState},
        types::PublicKey,
    };

    use assert_matches::assert_matches;
    use eyre::{eyre, Error, Result};
    use futures::{
        future::{self, Either},
        pin_mut,
    };
    use secured_linked_list::SecuredLinkedList;
    use std::{collections::BTreeMap, net::SocketAddr};
    use tokio::task;
    use xor_name::XorName;

    const JOIN_TIMEOUT_SEC: u64 = 15;

    #[tokio::test]
    async fn join_as_adult() -> Result<()> {
        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);

        let (section_auth, mut nodes, sk_set) =
            gen_section_authority_provider(Prefix::default(), elder_count());
        let bootstrap_node = nodes.remove(0);
        let bootstrap_addr = bootstrap_node.addr;
        let sk = sk_set.secret_key();
        let section_key = sk.public_key();

        // Node in first section has to have a stepped age,
        // Otherwise during the bootstrap process, node will change its id and age.
        let node_age = MIN_ADULT_AGE;
        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), node_age),
            gen_addr(),
        );
        let peer = node.peer();
        let state = Joiner::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        // Create the bootstrap task, but don't run it yet.
        let bootstrap = async move {
            state
                .try_join(bootstrap_addr, join_timeout)
                .await
                .map_err(Error::from)
        };

        // Create the task that executes the body of the test, but don't run it either.
        let others = async {
            // Receive JoinRequest
            let (wire_msg, recipients) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            let bootstrap_addrs: Vec<SocketAddr> = recipients
                .iter()
                .map(|recipient| recipient.addr())
                .collect();
            assert_eq!(bootstrap_addrs, [bootstrap_addr]);

            let node_msg = assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, .. }) =>
                msg);

            assert_matches!(node_msg, SystemMsg::JoinRequest(request) => {
                assert!(request.resource_proof_response.is_none());
            });

            // Send JoinResponse::Retry with section auth provider info
            let section_chain = SecuredLinkedList::new(section_key);
            let signed_sap = section_signed(sk, section_auth.clone())?;

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Retry {
                    section_auth: section_auth.to_msg(),
                    section_signed: signed_sap.sig,
                    proof_chain: section_chain,
                    expected_age: MIN_ADULT_AGE,
                })),
                &bootstrap_node,
                section_auth.section_key(),
            )?;

            // Receive the second JoinRequest with correct section info
            let (wire_msg, recipients) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;
            let (node_msg, dst_location) = assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, dst_location,.. }) =>
                (msg, dst_location));

            assert_eq!(dst_location.section_pk(), Some(section_key));
            itertools::assert_equal(recipients, section_auth.elders());
            assert_matches!(node_msg, SystemMsg::JoinRequest(request) => {
                assert_eq!(request.section_key, section_key);
            });

            // Send JoinResponse::Approval
            let section_auth = section_signed(sk, section_auth.clone())?;
            let node_state = section_signed(sk, NodeState::joined(peer, None))?;
            let proof_chain = SecuredLinkedList::new(section_key);
            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Approval {
                    genesis_key: section_key,
                    section_auth: section_auth.clone().into_authed_msg(),
                    node_state: node_state.into_authed_msg(),
                    section_chain: proof_chain,
                })),
                &bootstrap_node,
                section_auth.section_key(),
            )?;

            Ok(())
        };

        // Drive both tasks to completion concurrently (but on the same thread).
        let ((node, section), _) = future::try_join(bootstrap, others).await?;

        assert_eq!(section.authority_provider(), section_auth);
        assert_eq!(section.section_key(), section_key);
        assert_eq!(node.age(), node_age);

        Ok(())
    }

    #[tokio::test]
    async fn join_receive_redirect_response() -> Result<()> {
        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);

        let (_, mut nodes, sk_set) =
            gen_section_authority_provider(Prefix::default(), elder_count());
        let bootstrap_node = nodes.remove(0);
        let genesis_key = sk_set.secret_key().public_key();

        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );
        let state = Joiner::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(genesis_key),
        );

        let bootstrap_task = state.try_join(bootstrap_node.addr, join_timeout);
        let test_task = async move {
            // Receive JoinRequest
            let (wire_msg, recipients) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            assert_eq!(
                recipients
                    .into_iter()
                    .map(|peer| peer.addr())
                    .collect::<Vec<_>>(),
                vec![bootstrap_node.addr]
            );

            assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, .. }) =>
                    assert_matches!(msg, SystemMsg::JoinRequest{..}));

            // Send JoinResponse::Redirect
            let new_bootstrap_addrs: BTreeMap<_, _> = (0..elder_count())
                .map(|_| (xor_name::rand::random(), gen_addr()))
                .collect();

            let (new_section_auth, _, new_sk_set) =
                gen_section_authority_provider(Prefix::default(), elder_count());
            let new_pk_set = new_sk_set.public_keys();

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(
                    SectionAuthorityProviderMsg {
                        prefix: Prefix::default(),
                        public_key_set: new_pk_set.clone(),
                        elders: new_bootstrap_addrs.clone(),
                        members: BTreeMap::new(),
                        membership_gen: 0,
                    },
                ))),
                &bootstrap_node,
                new_section_auth.section_key(),
            )?;
            task::yield_now().await;

            // Receive new JoinRequest with redirected bootstrap contacts
            let (wire_msg, recipients) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            assert_eq!(
                recipients
                    .into_iter()
                    .map(|peer| peer.addr())
                    .collect::<Vec<_>>(),
                new_bootstrap_addrs
                    .iter()
                    .map(|(_, addr)| *addr)
                    .collect::<Vec<_>>()
            );

            let (node_msg, dst_location) = assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, dst_location,.. }) =>
                    (msg, dst_location));

            assert_eq!(dst_location.section_pk(), Some(new_pk_set.public_key()));
            assert_matches!(node_msg, SystemMsg::JoinRequest(req) => {
                assert_eq!(req.section_key, new_pk_set.public_key());
            });

            Ok(())
        };

        pin_mut!(bootstrap_task);
        pin_mut!(test_task);

        match future::select(bootstrap_task, test_task).await {
            Either::Left(_) => unreachable!(),
            Either::Right((output, _)) => output,
        }
    }

    #[tokio::test]
    async fn join_invalid_redirect_response() -> Result<()> {
        init_logger();
        let _span = tracing::info_span!("join_invalid_redirect_response").entered();

        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);

        let (_, mut nodes, sk_set) =
            gen_section_authority_provider(Prefix::default(), elder_count());
        let bootstrap_node = nodes.remove(0);

        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );
        let section_key = sk_set.secret_key().public_key();
        let state = Joiner::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let bootstrap_task = state.try_join(bootstrap_node.addr, join_timeout);
        let test_task = async {
            let (wire_msg, _) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, .. }) =>
            assert_matches!(msg, SystemMsg::JoinRequest{..}));

            let (new_section_auth, _, new_sk_set) =
                gen_section_authority_provider(Prefix::default(), elder_count());
            let new_pk_set = new_sk_set.public_keys();

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(
                    SectionAuthorityProviderMsg {
                        prefix: Prefix::default(),
                        public_key_set: new_pk_set.clone(),
                        elders: BTreeMap::new(),
                        members: BTreeMap::new(),
                        membership_gen: 0,
                    },
                ))),
                &bootstrap_node,
                new_section_auth.section_key(),
            )?;
            task::yield_now().await;

            let addrs: BTreeMap<XorName, SocketAddr> = (0..elder_count())
                .map(|_| (xor_name::rand::random(), gen_addr()))
                .collect();

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(
                    SectionAuthorityProviderMsg {
                        prefix: Prefix::default(),
                        public_key_set: new_pk_set.clone(),
                        elders: addrs.clone(),
                        members: BTreeMap::new(),
                        membership_gen: 0,
                    },
                ))),
                &bootstrap_node,
                new_section_auth.section_key(),
            )?;
            task::yield_now().await;

            let (wire_msg, _) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, .. }) =>
            assert_matches!(msg, SystemMsg::JoinRequest{..}));

            Ok(())
        };

        pin_mut!(bootstrap_task);
        pin_mut!(test_task);

        match future::select(bootstrap_task, test_task).await {
            Either::Left(_) => unreachable!(),
            Either::Right((output, _)) => output,
        }
    }

    #[tokio::test]
    async fn join_disallowed_response() -> Result<()> {
        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);

        let (section_auth, mut nodes, sk_set) =
            gen_section_authority_provider(Prefix::default(), elder_count());
        let bootstrap_node = nodes.remove(0);

        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let section_key = sk_set.secret_key().public_key();
        let state = Joiner::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let bootstrap_task = state.try_join(bootstrap_node.addr, join_timeout);
        let test_task = async {
            let (wire_msg, _) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("JoinRequest was not received"))?;

            assert_matches!(wire_msg.into_msg(), Ok(MsgType::System { msg, .. }) =>
                                assert_matches!(msg, SystemMsg::JoinRequest{..}));

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Rejected(
                    JoinRejectionReason::JoinsDisallowed,
                ))),
                &bootstrap_node,
                section_auth.section_key(),
            )?;

            Ok(())
        };

        let (join_result, test_result) = future::join(bootstrap_task, test_task).await;

        if let Err(RoutingError::TryJoinLater) = join_result {
        } else {
            return Err(eyre!("Not getting an execpted network rejection."));
        }

        test_result
    }

    #[tokio::test]
    async fn join_invalid_retry_prefix_response() -> Result<()> {
        init_logger();
        let _span = tracing::info_span!("join_invalid_retry_prefix_response").entered();

        let join_timeout = Duration::from_secs(JOIN_TIMEOUT_SEC);
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);

        let bootstrap_node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        let (good_prefix, bad_prefix) = {
            let p0 = Prefix::default().pushed(false);
            let p1 = Prefix::default().pushed(true);

            if node.name().bit(0) {
                (p1, p0)
            } else {
                (p0, p1)
            }
        };

        let (section_auth, _, sk_set) = gen_section_authority_provider(good_prefix, elder_count());
        let section_key = sk_set.public_keys().public_key();

        let state = Joiner::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let elders = (0..elder_count())
            .map(|_| {
                Peer::new(
                    good_prefix.substituted_in(xor_name::rand::random()),
                    gen_addr(),
                )
            })
            .collect();
        let join_task = state.join(section_key, section_key, elders, join_timeout);

        let test_task = async {
            let (wire_msg, _) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("NodeMsg was not received"))?;

            let node_msg =
                assert_matches!(wire_msg.into_msg(), Ok(MsgType::System{ msg, .. }) => msg);
            assert_matches!(node_msg, SystemMsg::JoinRequest(_));

            let section_chain = SecuredLinkedList::new(section_key);
            let signed_sap = section_signed(sk_set.secret_key(), section_auth.clone())?;

            // Send `Retry` with bad prefix
            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Retry {
                    section_auth: gen_section_authority_provider(bad_prefix, elder_count())
                        .0
                        .to_msg(),
                    section_signed: signed_sap.sig.clone(),
                    proof_chain: section_chain.clone(),
                    expected_age: MIN_ADULT_AGE,
                })),
                &bootstrap_node,
                section_key,
            )?;
            task::yield_now().await;

            // Send `Retry` with good prefix
            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Retry {
                    section_auth: section_auth.to_msg(),
                    section_signed: signed_sap.sig,
                    proof_chain: section_chain,
                    expected_age: MIN_ADULT_AGE,
                })),
                &bootstrap_node,
                section_key,
            )?;

            let (wire_msg, _) = send_rx
                .recv()
                .await
                .ok_or_else(|| eyre!("NodeMsg was not received"))?;

            let node_msg =
                assert_matches!(wire_msg.into_msg(), Ok(MsgType::System{ msg, .. }) => msg);
            assert_matches!(node_msg, SystemMsg::JoinRequest(_));

            Ok(())
        };

        pin_mut!(join_task);
        pin_mut!(test_task);

        match future::select(join_task, test_task).await {
            Either::Left(_) => unreachable!(),
            Either::Right((output, _)) => output,
        }
    }

    // test helper
    #[instrument]
    fn send_response(
        recv_tx: &mpsc::Sender<MsgEvent>,
        node_msg: SystemMsg,
        bootstrap_node: &NodeInfo,
        section_pk: BlsPublicKey,
    ) -> Result<()> {
        let wire_msg = WireMsg::single_src(
            bootstrap_node,
            DstLocation::Section {
                name: XorName::from(PublicKey::Bls(section_pk)),
                section_pk,
            },
            node_msg,
            section_pk,
        )?;

        debug!("wire msg built");

        let original_bytes = wire_msg.serialize()?;

        recv_tx.try_send(MsgEvent::Received {
            sender: bootstrap_node.peer(),
            wire_msg,
            original_bytes,
        })?;

        Ok(())
    }
}
