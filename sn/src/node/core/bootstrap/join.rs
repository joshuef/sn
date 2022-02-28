// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{read_prefix_map_from_disk, UsedRecipientSaps};
use crate::node::{
    core::{Comm, ConnectionEvent, SendStatus},
    ed25519,
    messages::WireMsgUtils,
    Error, NodeInfo, Result,
};
use sn_interface::messaging::{
    signature_aggregator::{Error as AggregatorError, SignatureAggregator},
    system::{
        JoinRejectionReason, JoinRequest, JoinResponse, ResourceProofResponse, SectionAuth,
        SystemMsg,
    },
    DstLocation, MsgKind, MsgType, NodeAuth, WireMsg,
};
use sn_interface::network_knowledge::{
    prefix_map::NetworkPrefixMap, NetworkKnowledge, SectionAuthUtils, MIN_ADULT_AGE,
};

use sn_interface::types::{log_markers::LogMarker, Peer};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use futures::future;
use resource_proof::ResourceProof;
use std::{collections::BTreeMap, net::SocketAddr};
use tokio::{sync::mpsc, time::sleep, time::Duration};
use tracing::Instrument;
use xor_name::Prefix;

// arbitrarily long. No join in a non splitting section should fail to get signature shares in anything like a few minutes
const JOIN_SHARE_EXPIRATION_DURATION: Duration = Duration::from_secs(900);

/// Join the network as new node.
///
/// NOTE: It's not guaranteed this function ever returns. This can happen due to messages being
/// lost in transit or other reasons. It's the responsibility of the caller to handle this case,
/// for example by using a timeout.
pub(crate) async fn join_network(
    node: NodeInfo,
    comm: &Comm,
    incoming_conns: &mut mpsc::Receiver<ConnectionEvent>,
    bootstrap_addr: SocketAddr,
    genesis_key: BlsPublicKey,
) -> Result<(NodeInfo, NetworkKnowledge)> {
    let (send_tx, send_rx) = mpsc::channel(1);

    let span = trace_span!("bootstrap");

    // Read prefix map from cache if available
    let prefix_map = read_prefix_map_from_disk(genesis_key).await?;

    let state = Join::new(node, send_tx, incoming_conns, prefix_map);

    future::join(state.run(bootstrap_addr), send_messages(send_rx, comm))
        .instrument(span)
        .await
        .0
}

struct Join<'a> {
    // Sender for outgoing messages.
    send_tx: mpsc::Sender<(WireMsg, Vec<Peer>)>,
    // Receiver for incoming messages.
    recv_rx: &'a mut mpsc::Receiver<ConnectionEvent>,
    node: NodeInfo,
    prefix: Prefix,
    prefix_map: NetworkPrefixMap,
    signature_aggregators: BTreeMap<BlsPublicKey, SignatureAggregator>,
    node_state_serialized: Option<Vec<u8>>,
    backoff: ExponentialBackoff,
    aggregated: bool,
}

impl<'a> Join<'a> {
    fn new(
        node: NodeInfo,
        send_tx: mpsc::Sender<(WireMsg, Vec<Peer>)>,
        recv_rx: &'a mut mpsc::Receiver<ConnectionEvent>,
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
            send_tx,
            recv_rx,
            node,
            prefix: Prefix::default(),
            prefix_map,
            signature_aggregators: BTreeMap::default(),
            node_state_serialized: None,
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
    async fn run(self, bootstrap_addr: SocketAddr) -> Result<(NodeInfo, NetworkKnowledge)> {
        // Use our XorName as we do not know their name or section key yet.
        let bootstrap_peer = Peer::new(self.node.name(), bootstrap_addr);
        let genesis_key = self.prefix_map.genesis_key();

        let (target_section_key, recipients) =
            if let Ok(sap) = self.prefix_map.section_by_name(&bootstrap_peer.name()) {
                (sap.section_key(), sap.elders_vec())
            } else {
                (genesis_key, vec![bootstrap_peer])
            };

        self.join(genesis_key, target_section_key, recipients).await
    }

    #[tracing::instrument(skip(self))]
    async fn join(
        mut self,
        network_genesis_key: BlsPublicKey,
        target_section_key: BlsPublicKey,
        recipients: Vec<Peer>,
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
            aggregated: None,
        };

        self.send_join_requests(join_request.clone(), &recipients, section_key, false)
            .await?;

        // Avoid sending more than one duplicated request (with same SectionKey) to the same peer.
        let mut used_recipient_saps = UsedRecipientSaps::new();

        loop {
            let (response, sender) = self.receive_join_response().await?;
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
                JoinResponse::ApprovalShare {
                    node_state,
                    sig_share,
                    section_auth,
                    section_signed,
                    section_chain,
                    ..
                } => {
                    // The JoinResponse::Redirect doesn't contains the proof_chain of the target
                    // section. Hence self.prefix_map didn't get updated on receiving it.
                    // In such case, we have to update self.prefix_map based on the received infos
                    // within JoinResponse::ApprovalShare
                    let section_auth = section_auth.into_state();
                    let signed_sap = SectionAuth {
                        value: section_auth,
                        sig: section_signed,
                    };
                    match self.prefix_map.update(signed_sap, &section_chain) {
                        Ok(updated) => {
                            debug!(
                                "Update prefix_map via JoinResponse::ApprovalShare: {:?}",
                                updated
                            );
                        }
                        Err(err) => {
                            debug!(
                                "Failed to update prefix_map via JoinResponse::ApprovalShare: {:?}",
                                err
                            );
                        }
                    }

                    let serialized_details =
                        if let Some(node_state_serialized) = &self.node_state_serialized {
                            node_state_serialized.clone()
                        } else {
                            let node_state_serialized = bincode::serialize(&node_state)?;
                            self.node_state_serialized = Some(node_state_serialized.clone());
                            node_state_serialized
                        };

                    let sig_pk = sig_share.public_key_set.public_key();

                    // get the aggregator or make a new one for this new section public key
                    let aggregator =
                        self.signature_aggregators.entry(sig_pk).or_insert_with(|| {
                            SignatureAggregator::with_expiration(JOIN_SHARE_EXPIRATION_DURATION)
                        });

                    info!("Aggregating received ApprovalShare from {:?}", sender);
                    match aggregator.add(&serialized_details, sig_share.clone()).await {
                        Ok(sig) => {
                            info!("Successfully aggregated ApprovalShares for joining the network");
                            self.aggregated = true;

                            let section_key = sig_share.public_key_set.public_key();
                            let auth = SectionAuth {
                                value: node_state,
                                sig,
                            };
                            let join_req = JoinRequest {
                                section_key,
                                resource_proof_response: None,
                                aggregated: Some(auth),
                            };
                            let name = self.node.name();
                            let recipients: Vec<Peer> = if let Some(signed_sap) =
                                self.prefix_map.closest_or_opposite(&name, None)
                            {
                                signed_sap.value.elders().cloned().collect()
                            } else {
                                warn!("cannot find recipients to send aggregated JoinApproval");
                                continue;
                            };
                            trace!("Sending aggregated JoinRequest to {:?}", recipients);
                            // Resend the JoinRequest now that we have collected enough ApprovalShares from the Elders
                            self.send_join_requests(join_req, &recipients, section_key, false)
                                .await?;
                            continue;
                        }
                        Err(AggregatorError::NotEnoughShares) => continue,
                        error => {
                            warn!(
                                "Error received as part of signature aggregation during join: {:?}",
                                error
                            );

                            if sig_pk != section_key {
                                // if we've have aggregation errors, we attempt a fresh join as there's likely been a key change
                                let join_request = JoinRequest {
                                    section_key,
                                    resource_proof_response: None,
                                    aggregated: None,
                                };

                                self.send_join_requests(
                                    join_request,
                                    &recipients,
                                    section_key,
                                    true,
                                )
                                .await?;
                            }

                            continue;
                        }
                    }
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
                        aggregated: None,
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
                        aggregated: None,
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
                        aggregated: None,
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

        let _res = self.send_tx.send((wire_msg, recipients.to_vec())).await;

        Ok(())
    }

    // TODO: receive JoinResponse from the JoinResponse handler directly,
    // analogous to the JoinAsRelocated flow.
    #[tracing::instrument(skip(self))]
    async fn receive_join_response(&mut self) -> Result<(JoinResponse, Peer)> {
        while let Some(event) = self.recv_rx.recv().await {
            // We are interested only in `JoinResponse` type of messages
            let (join_response, sender) = match event {
                ConnectionEvent::Received {
                    sender, wire_msg, ..
                } => match wire_msg.msg_kind() {
                    MsgKind::ServiceMsg(_) => continue,
                    MsgKind::NodeBlsShareAuthMsg(_) => {
                        trace!(
                            "Bootstrap message discarded: sender: {:?} wire_msg: {:?}",
                            sender,
                            wire_msg
                        );
                        continue;
                    }
                    MsgKind::NodeAuthMsg(NodeAuth { .. }) => match wire_msg.into_msg() {
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
async fn send_messages(mut rx: mpsc::Receiver<(WireMsg, Vec<Peer>)>, comm: &Comm) -> Result<()> {
    while let Some((wire_msg, recipients)) = rx.recv().await {
        match comm
            .send(&recipients, recipients.len(), wire_msg.clone())
            .await
        {
            Ok(SendStatus::AllRecipients) | Ok(SendStatus::MinDeliveryGroupSizeReached(_)) => {}
            Ok(SendStatus::MinDeliveryGroupSizeFailed(recipients)) => {
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

    use crate::node::{
        dkg::test_utils::*,
        messages::WireMsgUtils,
        sn_interface::network_knowledge::{test_utils::*, NodeState},
        Error as RoutingError, MIN_ADULT_AGE,
    };
    use crate::{elder_count, init_test_logger};
    use sn_interface::messaging::SectionAuthorityProvider as SectionAuthorityProviderMsg;
    use sn_interface::types::PublicKey;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn join_as_adult() -> Result<()> {
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
        let state = Join::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        // Create the bootstrap task, but don't run it yet.
        let bootstrap = async move { state.run(bootstrap_addr).await.map_err(Error::from) };

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

        assert_eq!(section.authority_provider().await, section_auth);
        assert_eq!(section.section_key().await, section_key);
        assert_eq!(node.age(), node_age);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn join_receive_redirect_response() -> Result<()> {
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
        let state = Join::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(genesis_key),
        );

        let bootstrap_task = state.run(bootstrap_node.addr);
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
                .map(|_| (XorName::random(), gen_addr()))
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

    #[tokio::test(flavor = "multi_thread")]
    async fn join_invalid_redirect_response() -> Result<()> {
        init_test_logger();
        let _span = tracing::info_span!("join_invalid_redirect_response").entered();

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
        let state = Join::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let bootstrap_task = state.run(bootstrap_node.addr);
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
                    },
                ))),
                &bootstrap_node,
                new_section_auth.section_key(),
            )?;
            task::yield_now().await;

            let addrs = (0..elder_count())
                .map(|_| (XorName::random(), gen_addr()))
                .collect();

            send_response(
                &recv_tx,
                SystemMsg::JoinResponse(Box::new(JoinResponse::Redirect(
                    SectionAuthorityProviderMsg {
                        prefix: Prefix::default(),
                        public_key_set: new_pk_set.clone(),
                        elders: addrs,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn join_disallowed_response() -> Result<()> {
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
        let state = Join::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let bootstrap_task = state.run(bootstrap_node.addr);
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

    #[tokio::test(flavor = "multi_thread")]
    async fn join_invalid_retry_prefix_response() -> Result<()> {
        init_test_logger();
        let _span = tracing::info_span!("join_invalid_retry_prefix_response").entered();

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

        let state = Join::new(
            node,
            send_tx,
            &mut recv_rx,
            NetworkPrefixMap::new(section_key),
        );

        let elders = (0..elder_count())
            .map(|_| Peer::new(good_prefix.substituted_in(rand::random()), gen_addr()))
            .collect();
        let join_task = state.join(section_key, section_key, elders);

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
        recv_tx: &mpsc::Sender<ConnectionEvent>,
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

        recv_tx.try_send(ConnectionEvent::Received {
            sender: bootstrap_node.peer(),
            wire_msg,
            original_bytes,
        })?;

        Ok(())
    }
}
