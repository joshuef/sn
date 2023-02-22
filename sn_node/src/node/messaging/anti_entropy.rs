// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    core::NodeContext,
    flow_ctrl::{cmds::Cmd, RejoinReason},
    messaging::Peers,
    Error, MyNode, Result,
};

use sn_fault_detection::IssueType;
use sn_interface::{
    messaging::{AntiEntropyKind, AntiEntropyMsg, MsgId, MsgKind, NetworkMsg, WireMsg},
    network_knowledge::{NetworkKnowledge, SectionTreeUpdate},
    types::{log_markers::LogMarker, Peer, PublicKey},
};

use bls::PublicKey as BlsPublicKey;
use itertools::Itertools;
use qp2p::SendStream;
use std::collections::BTreeSet;

use xor_name::XorName;

impl MyNode {
    /// Send `AntiEntropy` update message to all nodes in our own section.
    pub(crate) fn send_ae_update_to_our_section(&self) -> Result<Option<Cmd>> {
        let our_name = self.info().name();
        let context = &self.context();
        let recipients: BTreeSet<_> = self
            .network_knowledge
            .section_members()
            .into_iter()
            .filter(|info| info.name() != our_name)
            .map(|info| *info.peer())
            .collect();

        if recipients.is_empty() {
            warn!("No peers of our section found in our network knowledge to send AE-Update");
            return Ok(None);
        }

        let leaf = self.section_chain().last_key()?;
        // The previous PK which is likely what adults know
        match self.section_chain().get_parent_key(&leaf) {
            Ok(prev_pk) => {
                let prev_pk = prev_pk.unwrap_or(*self.section_chain().genesis_key());
                Ok(Some(MyNode::send_ae_update_to_nodes(
                    context,
                    Peers::Multiple(recipients),
                    prev_pk,
                )))
            }
            Err(_) => {
                error!("SectionsDAG fields went out of sync");
                Ok(None)
            }
        }
    }

    /// Send `AntiEntropy` update message to the specified nodes.
    pub(crate) fn send_ae_update_to_nodes(
        context: &NodeContext,
        recipients: Peers,
        section_pk: BlsPublicKey,
    ) -> Cmd {
        let members = context.network_knowledge.section_members_with_decision();

        let ae_msg = NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
            section_tree_update: MyNode::generate_ae_section_tree_update(context, Some(section_pk)),
            kind: AntiEntropyKind::Update { members },
        });

        Cmd::send_network_msg(ae_msg, recipients)
    }

    #[instrument(skip_all)]
    /// Send AntiEntropy update message to the nodes in our sibling section.
    pub(crate) fn send_updates_to_sibling_section(
        &self,
        prev_context: &NodeContext,
    ) -> Result<Vec<Cmd>> {
        info!("{}", LogMarker::AeSendUpdateToSiblings);
        let sibling_prefix = prev_context.network_knowledge.prefix().sibling();
        if let Some(sibling_sap) = prev_context
            .network_knowledge
            .section_tree()
            .get_signed(&sibling_prefix)
        {
            let promoted_sibling_elders: BTreeSet<_> = sibling_sap
                .elders()
                .filter(|peer| !prev_context.network_knowledge.elders().contains(peer))
                .cloned()
                .collect();

            if promoted_sibling_elders.is_empty() {
                debug!("No promoted siblings found in our network knowledge to send AE-Update");
                return Ok(vec![]);
            }

            // Using previous_key as dst_section_key as newly promoted
            // sibling Elders shall still in the state of pre-split.
            let previous_section_key = prev_context.network_knowledge.section_key();

            // Send AE update to sibling section's new Elders
            Ok(vec![MyNode::send_ae_update_to_nodes(
                prev_context,
                Peers::Multiple(promoted_sibling_elders),
                previous_section_key,
            )])
        } else {
            error!("Failed to get sibling SAP during split.");
            Ok(vec![])
        }
    }

    // Private helper to generate a SectionTreeUpdate to update
    // a peer abot our SAP, with proof_chain and members list.
    fn generate_ae_section_tree_update(
        context: &NodeContext,
        dst_section_key: Option<BlsPublicKey>,
    ) -> SectionTreeUpdate {
        let signed_sap = context.network_knowledge.signed_sap();

        let proof_chain = dst_section_key
            .and_then(|key| {
                context
                    .network_knowledge
                    .get_proof_chain_to_current_section(&key)
                    .ok()
            })
            .unwrap_or_else(|| context.network_knowledge.section_chain());

        SectionTreeUpdate::new(signed_sap, proof_chain)
    }

    #[instrument(skip_all)]
    pub(crate) async fn handle_anti_entropy_msg(
        node: &mut MyNode,
        starting_context: NodeContext,
        section_tree_update: SectionTreeUpdate,
        kind: AntiEntropyKind,
        sender: Peer,
    ) -> Result<Vec<Cmd>> {
        let sap = section_tree_update.signed_sap.value.clone();

        let members = if let AntiEntropyKind::Update { members } = &kind {
            Some(members.clone())
        } else {
            None
        };

        let mut cmds = vec![];

        // block off the write lock
        let updated = {
            let should_update = starting_context
                .clone()
                .network_knowledge
                .update_knowledge_if_valid(
                    section_tree_update.clone(),
                    members.clone(),
                    &starting_context.name,
                )?;

            if should_update {
                let updated = node.network_knowledge.update_knowledge_if_valid(
                    section_tree_update,
                    members,
                    &starting_context.name,
                )?;
                debug!("net knowledge updated");
                // always run this, only changes will trigger events
                cmds.extend(node.update_on_section_change(&starting_context).await?);
                trace!("updated for section change");
                updated
            } else {
                false
            }
        };

        // mut here to update comms
        let latest_context = node.context();

        // Only trigger reorganize data when there is a membership change happens.
        if updated {
            MyNode::update_comm_target_list(&latest_context);
            // write latest section tree before potential rejoin of network
            MyNode::write_section_tree(&latest_context);
            let prefix = sap.prefix();
            info!("SectionTree written to disk with update for prefix {prefix:?}");

            // check if we've been kicked out of the section
            if starting_context
                .network_knowledge
                .members()
                .iter()
                .map(|m| m.name())
                .contains(&latest_context.name)
                && !latest_context
                    .network_knowledge
                    .members()
                    .iter()
                    .map(|m| m.name())
                    .contains(&latest_context.name)
            {
                error!("We've been removed from the section");
                return Err(Error::RejoinRequired(RejoinReason::RemovedFromSection));
            }

            // we're still in the section, only now we start asking others for data
            cmds.push(MyNode::ask_for_any_new_data_from_whole_section(&latest_context).await);
        } else {
            debug!("No update to network knowledge");
        }

        // Check if we need to resend any messages and who should we send it to.
        let (bounced_msg, target_peer) = match kind {
            AntiEntropyKind::Update { .. } => {
                // log the msg as received. Elders track this for other elders in fault detection
                latest_context.untrack_node_issue(sender.name(), IssueType::AeProbeMsg);
                return Ok(cmds);
            } // Nope, bail early
            AntiEntropyKind::Retry { bounced_msg } => {
                trace!("{}", LogMarker::AeResendAfterRetry);
                (bounced_msg, sender)
            }
            AntiEntropyKind::Redirect { bounced_msg } => {
                // We choose the Elder closest to the dst section key,
                // just to pick one of them in an arbitrary but deterministic fashion.
                let target_name = XorName::from(PublicKey::Bls(sap.section_key()));

                let chosen_dst_elder = if let Some(dst) = sap
                    .elders()
                    .max_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()))
                {
                    *dst
                } else {
                    error!("Failed to find closest Elder to resend msg upon AE-Redirect response.");
                    return Ok(cmds);
                };

                trace!("{}", LogMarker::AeResendAfterRedirect);

                (bounced_msg, chosen_dst_elder)
            }
        };

        let wire_msg = WireMsg::from(bounced_msg)?;
        let dst = wire_msg.dst;
        let msg_id = wire_msg.msg_id();
        let msg_to_resend = match wire_msg.into_msg()? {
            NetworkMsg::Node(msg) => msg,
            _ => {
                warn!("Not a Node NetworkMsg received in AE response. We do not handle any other type in AE msgs yet.");
                return Ok(cmds);
            }
        };

        // If the new SAP's section key is the same as the section key set when the
        // bounced message was originally sent, we just drop it.
        if dst.section_key == sap.section_key() {
            error!("Dropping bounced msg ({sender:?}) received in AE-Retry from {msg_id:?} as suggested new dst section key is the same as previously sent: {:?}", sap.section_key());
            return Ok(cmds);
        }

        trace!("Resending original {msg_id:?} to {target_peer:?} with {msg_to_resend:?}");
        trace!("{}", LogMarker::AeResendAfterRedirect);

        cmds.push(Cmd::send_msg(msg_to_resend, Peers::Single(target_peer)));
        Ok(cmds)
    }

    /// Generate and return AE commands for a given wire_msg and section_tree_update
    pub(crate) fn generate_anti_entropy_cmds(
        wire_msg: &WireMsg,
        origin: Peer,
        section_tree_update: SectionTreeUpdate,
        kind: AntiEntropyKind,
        send_stream: Option<SendStream>,
    ) -> Result<Vec<Cmd>> {
        if matches!(
            wire_msg.kind(),
            MsgKind::AntiEntropy(_) | MsgKind::DataResponse(_)
        ) {
            // TODO: error
            error!("Should be unreachable. Dropping message.");
            return Ok(vec![]);
        }

        let msg_id = wire_msg.msg_id();
        let mut cmds = vec![];
        if matches!(wire_msg.kind(), MsgKind::Node { .. }) {
            cmds.push(Cmd::TrackNodeIssue {
                name: origin.name(),
                issue: sn_fault_detection::IssueType::NetworkKnowledge,
            });
        }

        if let Some(stream) = send_stream {
            cmds.push(Cmd::UpdateCallerOnStream {
                caller: origin,
                msg_id: MsgId::new(),
                correlation_id: msg_id,
                kind,
                section_tree_update,
                stream,
            });
            return Ok(cmds);
        } else if matches!(wire_msg.kind(), MsgKind::Client { .. }) {
            // TODO: error
            error!("No response stream from client. Dropping message");
            return Ok(vec![]);
        }

        trace!("Attempting to send AE response over fresh conn for {msg_id:?}");
        cmds.push(Cmd::UpdateCaller {
            caller: origin,
            correlation_id: msg_id,
            kind,
            section_tree_update,
        });
        Ok(cmds)
    }

    // If entropy is found, determine the `SectionTreeUpdate` and kind of AE response
    // to send in order to bring the sender's knowledge about us up to date.
    pub(crate) fn check_for_entropy(
        wire_msg: &WireMsg,
        network_knowledge: &NetworkKnowledge,
        sender: &Peer,
    ) -> Result<Option<(SectionTreeUpdate, AntiEntropyKind)>> {
        let msg_id = wire_msg.msg_id();
        let dst = wire_msg.dst();

        // Check if the message has reached the correct section,
        // if not, we'll need to respond with AE
        let our_prefix = network_knowledge.prefix();
        // Let's try to find a section closer to the destination, if it's not for us.
        if !our_prefix.matches(&dst.name) {
            trace!(
                "AE: {msg_id:?} prefix not matching. We are: {our_prefix:?}, they sent to: {:?}",
                dst.name
            );
            let closest_sap = network_knowledge.closest_signed_sap_with_chain(&dst.name);
            return match closest_sap {
                Some((signed_sap, proof_chain)) => {
                    debug!(
                        "{msg_id:?} Found a better matching prefix {:?}: {signed_sap:?}",
                        signed_sap.prefix()
                    );
                    // Redirect to the closest section
                    trace!(
                        "{} {msg_id:?} entropy found. {sender:?} should be updated",
                        LogMarker::AeSendRedirect
                    );
                    let section_tree_update = SectionTreeUpdate::new(signed_sap, proof_chain);
                    let bounced_msg = wire_msg.serialize()?;
                    let kind = AntiEntropyKind::Redirect { bounced_msg };

                    Ok(Some((section_tree_update, kind)))
                }
                None => {
                    // TODO: instead of just dropping the message, don't we actually need
                    // to get up to date info from other Elders in our section as it may be
                    // a section key we are not aware of yet?
                    // ...and once we acquired new key/s we attempt AE check again?
                    warn!(
                        "Anti-Entropy: cannot reply with redirect msg for dst_name {:?} and \
                        key {:?} to a closest section. Our SectionTree is empty.",
                        dst.name, dst.section_key
                    );
                    Err(Error::NoMatchingSection)
                }
            };
        }

        let our_section_key = network_knowledge.section_key();
        trace!(
            "Performing AE checks on {msg_id:?}, provided pk was: {:?} ours is: {our_section_key:?}",
            dst.section_key
        );

        if dst.section_key == our_section_key {
            // Destination section key matches our current section key
            return Ok(None);
        }

        let section_tree_update =
            generate_ae_section_tree_update(network_knowledge, Some(dst.section_key));

        trace!("Sending AE-Retry message to {sender:?} with {section_tree_update:?}");
        let bounced_msg = wire_msg.serialize()?;
        let kind = AntiEntropyKind::Retry { bounced_msg };

        Ok(Some((section_tree_update, kind)))
    }
}

// Private helper to generate a SectionTreeUpdate to update
// a peer about our SAP, with proof_chain and members list.
fn generate_ae_section_tree_update(
    network_knowledge: &NetworkKnowledge,
    dst_section_key: Option<BlsPublicKey>,
) -> SectionTreeUpdate {
    let signed_sap = network_knowledge.signed_sap();

    let proof_chain = dst_section_key
        .and_then(|key| {
            network_knowledge
                .get_proof_chain_to_current_section(&key)
                .ok()
        })
        .unwrap_or_else(|| network_knowledge.section_chain());

    SectionTreeUpdate::new(signed_sap, proof_chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{flow_ctrl::tests::network_builder::TestNetworkBuilder, MIN_ADULT_AGE};
    use sn_interface::{
        elder_count,
        messaging::{AntiEntropyMsg, Dst, MsgId, MsgKind},
        network_knowledge::MyNodeInfo,
        test_utils::{gen_addr, prefix},
        types::keys::ed25519,
    };

    use bls::SecretKey;
    use eyre::{ContextCompat, Result};
    use xor_name::Prefix;

    #[tokio::test]
    async fn ae_everything_up_to_date() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let dst_section_key = node.network_knowledge().section_key();
        let mut msg = create_msg(&our_prefix, dst_section_key)?;
        msg.dst = Dst {
            name: our_prefix.substituted_in(xor_name::rand::random()),
            section_key: dst_section_key,
        };

        let context = node.context();
        let sender = node.info().peer();

        let ae_msg = MyNode::check_for_entropy(&msg, &context.network_knowledge, &sender)?;

        assert!(ae_msg.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn ae_redirect_to_other_section() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        let other_section = env.get_network_knowledge(other_prefix, None);
        let other_sap = other_section.signed_sap();

        // get node from the latest section of our_prefix
        let mut node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let other_sk = bls::SecretKey::random();
        let other_pk = other_sk.public_key();

        let mut wire_msg = create_msg(&other_prefix, other_pk)?;

        // set our target dst
        // since it's not aware of the other prefix, it will redirect to self
        wire_msg.dst = Dst {
            section_key: other_pk,
            name: other_sap.prefix().name(),
        };

        let context = node.context();
        let sender = node.info().peer();

        let (section_tree_update, _kind) =
            MyNode::check_for_entropy(&wire_msg, &context.network_knowledge, &sender)?
                .context("no entropy found")?;

        assert_eq!(
            section_tree_update.signed_sap,
            node.network_knowledge().signed_sap()
        );

        // now let's insert the other SAP to make it aware of the other prefix
        let section_tree_update =
            SectionTreeUpdate::new(other_sap.clone(), other_section.section_chain());
        assert!(node.network_knowledge.update_knowledge_if_valid(
            section_tree_update,
            None,
            &context.name,
        )?);

        let new_context = node.context();
        // and it now shall give us an AE redirect msg
        // with the SAP we inserted for other prefix
        let (section_tree_update, _kind) =
            MyNode::check_for_entropy(&wire_msg, &new_context.network_knowledge, &sender)?
                .context("no entropy found")?;

        assert_eq!(section_tree_update.signed_sap, other_sap);
        Ok(())
    }

    #[tokio::test]
    async fn ae_outdated_dst_key_of_our_section() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let network_knowledge = node.network_knowledge();
        let mut msg = create_msg(&our_prefix, network_knowledge.section_key())?;
        msg.dst = Dst {
            section_key: *network_knowledge.genesis_key(),
            name: our_prefix.substituted_in(xor_name::rand::random()),
        };

        let sender = node.info().peer();
        let (section_tree_update, _kind) =
            MyNode::check_for_entropy(&msg, network_knowledge, &sender)?
                .context("no entropy found")?;

        assert_eq!(
            section_tree_update.signed_sap,
            network_knowledge.signed_sap()
        );
        assert_eq!(section_tree_update.proof_chain, node.section_chain());
        Ok(())
    }

    #[tokio::test]
    async fn ae_wrong_dst_key_of_our_section_returns_retry() -> Result<()> {
        // create an env with 3 churns in prefix0. And a single churn in prefix1
        let our_prefix = prefix("0");
        let other_prefix = prefix("1");
        let env = TestNetworkBuilder::new(rand::thread_rng())
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(our_prefix, elder_count(), 0, None, None)
            .sap(other_prefix, elder_count(), 0, None, None)
            .build();
        // get node from the latest section of our_prefix
        let node = env.get_nodes(our_prefix, 1, 0, None).remove(0);

        let mut msg = create_msg(&our_prefix, node.network_knowledge().section_key())?;
        let bogus_network_gen = bls::SecretKey::random();
        msg.dst = Dst {
            section_key: bogus_network_gen.public_key(),
            name: our_prefix.substituted_in(xor_name::rand::random()),
        };

        let context = node.context();
        let sender = node.info().peer();

        let (section_tree_update, _kind) =
            MyNode::check_for_entropy(&msg, &context.network_knowledge, &sender)?
                .context("no entropy found")?;

        assert_eq!(
            section_tree_update.signed_sap,
            node.network_knowledge().signed_sap()
        );
        assert_eq!(section_tree_update.proof_chain, node.section_chain());

        Ok(())
    }

    fn create_msg(src_section_prefix: &Prefix, src_section_pk: BlsPublicKey) -> Result<WireMsg> {
        let sender = MyNodeInfo::new(
            ed25519::gen_keypair(&src_section_prefix.range_inclusive(), MIN_ADULT_AGE),
            gen_addr(),
        );

        // just some message we can construct easily
        let payload_msg = AntiEntropyMsg::Probe(src_section_pk);
        let payload = WireMsg::serialize_msg_payload(&payload_msg)?;

        let dst = Dst {
            name: xor_name::rand::random(),
            section_key: SecretKey::random().public_key(),
        };
        Ok(WireMsg::new_msg(
            MsgId::new(),
            payload,
            MsgKind::AntiEntropy(sender.name()),
            dst,
        ))
    }
}
