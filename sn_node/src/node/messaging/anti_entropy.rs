// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::log_sleep;
use crate::node::{
    flow_ctrl::cmds::Cmd,
    messaging::{OutgoingMsg, Peers},
    Error, Event, MembershipEvent, Node, Result, StateSnapshot,
};

use qp2p::UsrMsgBytes;
use sn_interface::messaging::system::AntiEntropyKind;
#[cfg(feature = "traceroute")]
use sn_interface::messaging::Traceroute;
use sn_interface::{
    messaging::{
        system::{KeyedSig, NodeCmd, SectionAuth, SystemMsg},
        MsgType, WireMsg,
    },
    network_knowledge::SectionAuthorityProvider,
    types::{log_markers::LogMarker, Peer, PublicKey},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use secured_linked_list::SecuredLinkedList;
use std::{collections::BTreeSet, time::Duration};
use xor_name::{Prefix, XorName};

impl Node {
    /// Send `AntiEntropy` update message to all nodes in our own section.
    pub(crate) fn send_ae_update_to_our_section(&self) -> Option<Cmd> {
        let our_name = self.info().name();
        let recipients: BTreeSet<_> = self
            .network_knowledge
            .section_members()
            .into_iter()
            .filter(|info| info.name() != our_name)
            .map(|info| *info.peer())
            .collect();

        if recipients.is_empty() {
            warn!("No peers of our section found in our network knowledge to send AE-Update");
            return None;
        }

        // The previous PK which is likely what adults know
        let previous_pk = *self.our_section_dag().prev_key();
        Some(self.send_ae_update_to_nodes(recipients, previous_pk))
    }

    /// Send `AntiEntropy` update message to the specified nodes.
    pub(crate) fn send_ae_update_to_nodes(
        &self,
        recipients: BTreeSet<Peer>,
        section_pk: BlsPublicKey,
    ) -> Cmd {
        let members = self
            .network_knowledge
            .section_signed_members()
            .iter()
            .map(|state| state.clone().into_authed_msg())
            .collect();

        let ae_msg = self.generate_ae_msg(Some(section_pk), AntiEntropyKind::Update { members });

        self.send_system_msg(ae_msg, Peers::Multiple(recipients))
    }

    /// Send `MetadataExchange` packet to the specified nodes
    pub(crate) fn send_metadata_updates(&self, recipients: BTreeSet<Peer>, prefix: &Prefix) -> Cmd {
        let metadata = self.get_metadata_of(prefix);
        self.send_system_msg(
            SystemMsg::NodeCmd(NodeCmd::ReceiveMetadata { metadata }),
            Peers::Multiple(recipients),
        )
    }

    #[instrument(skip_all)]
    /// Send AntiEntropy update message to the nodes in our sibling section.
    pub(crate) fn send_updates_to_sibling_section(
        &self,
        our_prev_state: &StateSnapshot,
    ) -> Result<Vec<Cmd>> {
        debug!("{}", LogMarker::AeSendUpdateToSiblings);
        let sibling_prefix = self.network_knowledge.prefix().sibling();
        if let Some(sibling_sap) = self
            .network_knowledge
            .section_tree()
            .get_signed(&sibling_prefix)
        {
            let promoted_sibling_elders: BTreeSet<_> = sibling_sap
                .elders()
                .filter(|peer| !our_prev_state.elders.contains(&peer.name()))
                .cloned()
                .collect();

            if promoted_sibling_elders.is_empty() {
                debug!("No promoted siblings found in our network knowledge to send AE-Update");
                return Ok(vec![]);
            }

            // Using previous_key as dst_section_key as newly promoted
            // sibling Elders shall still in the state of pre-split.
            let previous_section_key = our_prev_state.section_key;
            let sibling_prefix = sibling_sap.prefix();

            let mut cmds =
                vec![self.send_metadata_updates(promoted_sibling_elders.clone(), &sibling_prefix)];

            // Also send AE update to sibling section's new Elders
            cmds.push(self.send_ae_update_to_nodes(promoted_sibling_elders, previous_section_key));

            Ok(cmds)
        } else {
            error!("Failed to get sibling SAP during split.");
            Ok(vec![])
        }
    }

    // Private helper to generate AntiEntropy message to update
    // a peer abot our SAP, with proof_chain and members list.
    fn generate_ae_msg(
        &self,
        dst_section_key: Option<BlsPublicKey>,
        kind: AntiEntropyKind,
    ) -> SystemMsg {
        let signed_sap = self.network_knowledge.section_signed_authority_provider();

        let proof_chain = dst_section_key
            .and_then(|key| self.network_knowledge.get_proof_chain_to_current(&key).ok())
            .unwrap_or_else(|| self.network_knowledge.our_section_dag());

        SystemMsg::AntiEntropy {
            section_auth: signed_sap.value.to_msg(),
            section_signed: signed_sap.sig,
            proof_chain,
            kind,
        }
    }

    #[instrument(skip_all)]
    pub(crate) async fn handle_anti_entropy_msg(
        &mut self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        kind: AntiEntropyKind,
        sender: Peer,
        #[cfg(feature = "traceroute")] traceroute: Traceroute,
    ) -> Result<Vec<Cmd>> {
        let snapshot = self.state_snapshot();

        let our_name = self.info().name();
        let signed_sap = SectionAuth {
            value: section_auth.clone(),
            sig: section_signed.clone(),
        };

        let members = if let AntiEntropyKind::Update { members } = kind.clone() {
            Some(members)
        } else {
            None
        };

        let updated = self.network_knowledge.update_knowledge_if_valid(
            signed_sap.clone(),
            &proof_chain,
            members,
            &our_name,
            &self.section_keys_provider,
        )?;

        // always run this, only changes will trigger events
        let mut cmds = self.update_on_elder_change(&snapshot).await?;

        // Only trigger reorganize data when there is a membership change happens.
        if updated && self.is_not_elder() {
            // only done if adult, since as an elder we dont want to get any more
            // data for our name (elders will eventually be caching data in general)
            cmds.push(self.ask_for_any_new_data().await);
        }

        if updated {
            self.write_section_tree().await;
            let prefix = section_auth.prefix();
            info!("SectionTree written to disk with update for prefix {prefix:?}");

            // check if we've been kicked out of the section
            if snapshot.members.contains(&self.name())
                && !self.state_snapshot().members.contains(&self.name())
            {
                error!("Detected that we've been removed from the section");
                self.send_event(Event::Membership(MembershipEvent::RemovedFromSection))
                    .await;
                return Err(Error::RemovedFromSection);
            }
        }

        // Check if we need to resend any messsages and who should we send it to.
        let (bounced_msg, response_peer) = match kind {
            AntiEntropyKind::Update { .. } => {
                // log the msg as received. Elders track this for other elders in dysfunction
                self.dysfunction_tracking
                    .ae_update_msg_received(&sender.name());
                return Ok(cmds);
            } // Nope, bail early
            AntiEntropyKind::Retry { bounced_msg } => (bounced_msg, sender),
            AntiEntropyKind::Redirect { bounced_msg } => {
                // We choose the Elder closest to the dst section key,
                // just to pick one of them in an arbitrary but deterministic fashion.
                let target_name = XorName::from(PublicKey::Bls(section_auth.section_key()));

                let chosen_dst_elder = if let Some(dst) = section_auth
                    .elders()
                    .max_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()))
                {
                    *dst
                } else {
                    error!("Failed to find closest Elder to resend msg upon AE-Redirect response.");
                    return Ok(cmds);
                };

                (bounced_msg, chosen_dst_elder)
            }
        };

        let (header, dst, payload) = bounced_msg;

        let (msg_to_resend, msg_id, dst) = match WireMsg::deserialize(header, dst, payload)? {
            MsgType::System {
                msg, msg_id, dst, ..
            } => (msg, msg_id, dst),
            _ => {
                warn!("Non System MsgType received in AE response. We do not handle any other type in AE msgs yet.");
                return Ok(cmds);
            }
        };

        // If the new SAP's section key is the same as the section key set when the
        // bounced message was originally sent, we just drop it.
        if dst.section_key == section_auth.section_key() {
            error!("Dropping bounced msg ({sender:?}) received in AE-Retry from {msg_id:?} as suggested new dst section key is the same as previously sent: {:?}", section_auth.section_key());
            return Ok(cmds);
        }

        self.create_or_wait_for_backoff(&response_peer).await;

        trace!("{}", LogMarker::AeResendAfterAeRedirect);

        if cfg!(feature = "traceroute") {
            cmds.push(self.trace_system_msg(
                msg_to_resend,
                Peers::Single(response_peer),
                #[cfg(feature = "traceroute")]
                traceroute,
            ))
        } else {
            cmds.push(self.send_system_msg(msg_to_resend, Peers::Single(response_peer)))
        }

        Ok(cmds)
    }

    /// Checks AE-BackoffCache for backoff, or creates a new instance
    /// waits for any required backoff duration
    async fn create_or_wait_for_backoff(&mut self, peer: &Peer) {
        let our_backoff = self
            .ae_backoff_cache
            .find(|(node, _)| node == peer)
            .map(|(_, backoff)| backoff);

        if let Some(backoff) = our_backoff {
            let next_backoff = backoff.next_backoff();
            let sleep_time = if let Some(mut next_wait) = next_backoff {
                // The default setup start with around 400ms
                // then increases to around 50s after 25 calls.
                // with `next_wait /= 100`, the sleep_time still rise to over 800ms quickly.
                next_wait /= 200;
                if next_wait > Duration::from_millis(500) {
                    backoff.reset();
                }
                Some(next_wait)
            } else {
                // TODO: we've done all backoffs and are _still_ getting messages?
                // we should probably penalise the node here.
                None
            };

            if let Some(sleep_time) = sleep_time {
                log_sleep!(Duration::from_millis(sleep_time.as_millis() as u64));
            }
        } else {
            let _res = self
                .ae_backoff_cache
                .insert((*peer, ExponentialBackoff::default()));
        }
    }

    // If entropy is found, determine the msg to send in order to
    // bring the sender's knowledge about us up to date.
    pub(crate) fn check_for_entropy(
        &self,
        wire_msg: &WireMsg,
        dst_section_key: &BlsPublicKey,
        dst_name: XorName,
        sender: &Peer,
    ) -> Result<Option<Cmd>> {
        // Check if the message has reached the correct section,
        // if not, we'll need to respond with AE
        let our_prefix = self.network_knowledge.prefix();

        // Let's try to find a section closer to the destination, if it's not for us.
        if !self.network_knowledge.prefix().matches(&dst_name) {
            debug!(
                "AE: prefix not matching. We are: {:?}, they sent to: {:?}",
                our_prefix, dst_name
            );
            return match self.network_knowledge.closest_signed_sap(&dst_name) {
                Some((signed_sap, section_dag)) => {
                    info!("Found a better matching prefix {:?}", signed_sap.prefix());
                    let bounced_msg = wire_msg.serialize()?;
                    // Redirect to the closest section
                    let ae_msg = SystemMsg::AntiEntropy {
                        section_auth: signed_sap.value.to_msg(),
                        section_signed: signed_sap.sig.clone(),
                        proof_chain: section_dag,
                        kind: AntiEntropyKind::Redirect { bounced_msg },
                    };

                    trace!("{}", LogMarker::AeSendRedirect);

                    return Ok(Some(Cmd::send_msg(
                        OutgoingMsg::System(ae_msg),
                        Peers::Single(*sender),
                    )));
                }
                None => {
                    warn!("Our SectionTree is empty");
                    // TODO: instead of just dropping the message, don't we actually need
                    // to get up to date info from other Elders in our section as it may be
                    // a section key we are not aware of yet?
                    // ...and once we acquired new key/s we attempt AE check again?
                    warn!(
                        "Anti-Entropy: cannot reply with redirect msg for dst_name {:?} and key {:?} to a closest section.",
                        dst_name, dst_section_key
                    );

                    Err(Error::NoMatchingSection)
                }
            };
        }

        let section_key = self.network_knowledge.section_key();
        trace!(
            "Performing AE checks, provided pk was: {:?} ours is: {:?}",
            dst_section_key,
            section_key
        );

        if dst_section_key == &section_key {
            // Destination section key matches our current section key
            return Ok(None);
        }

        let bounced_msg = wire_msg.serialize()?;

        let ae_msg = self.generate_ae_msg(
            Some(*dst_section_key),
            AntiEntropyKind::Retry { bounced_msg },
        );

        Ok(Some(Cmd::send_msg(
            OutgoingMsg::System(ae_msg),
            Peers::Single(*sender),
        )))
    }

    // Generate an AE redirect cmd for the given message
    pub(crate) fn ae_redirect_to_our_elders(
        &self,
        sender: Peer,
        bounced_msg: UsrMsgBytes,
    ) -> Result<Cmd> {
        trace!("{} in ae_redirect ", LogMarker::AeSendRedirect);

        let ae_msg = self.generate_ae_msg(None, AntiEntropyKind::Redirect { bounced_msg });

        Ok(Cmd::send_msg(
            OutgoingMsg::System(ae_msg),
            Peers::Single(sender),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::node::{
        cfg::create_test_max_capacity_and_root_storage,
        flow_ctrl::{event_channel, tests::network_utils::create_comm},
        MIN_ADULT_AGE,
    };
    use crate::UsedSpace;

    use sn_interface::{
        elder_count,
        messaging::{
            AuthKind, AuthorityProof, Dst, MsgId, NodeAuth, NodeMsgAuthority,
            SectionAuth as SectionAuthMsg,
        },
        network_knowledge::{
            test_utils::{gen_addr, random_sap, section_signed},
            NetworkKnowledge, NodeInfo, SectionKeyShare, SectionKeysProvider,
        },
        types::keys::ed25519,
    };

    use assert_matches::assert_matches;
    use bls::SecretKey;
    use eyre::{Context, Result};
    use secured_linked_list::SecuredLinkedList;
    use std::collections::BTreeSet;
    use xor_name::Prefix;

    #[tokio::test]
    async fn ae_everything_up_to_date() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let env = Env::new().await?;
                let our_prefix = env.node.network_knowledge().prefix();
                let msg =
                    env.create_msg(&our_prefix, env.node.network_knowledge().section_key())?;
                let sender = env.node.info().peer();
                let dst_name = our_prefix.substituted_in(xor_name::rand::random());
                let dst_section_key = env.node.network_knowledge().section_key();

                let cmd = env.node.check_for_entropy(
                    msg.serialize_and_cache_bytes()?,
                    &dst_section_key,
                    dst_name,
                    &sender,
                )?;

                assert!(cmd.is_none());
                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn ae_update_msg_to_be_trusted() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                let env = Env::new().await?;

                let known_keys = vec![*env.node.network_knowledge().genesis_key()];

                // This proof_chain already contains other_pk
                let proof_chain = env.proof_chain.clone();
                let (msg, msg_authority) = env.create_update_msg(proof_chain)?;

                // AeUpdate message shall get pass through.
                assert!(NetworkKnowledge::verify_node_msg_can_be_trusted(
                    &msg_authority,
                    &msg,
                    &known_keys
                ));

                // AeUpdate message contains corrupted proof_chain shall get rejected.
                let other_env = Env::new().await?;
                let (corrupted_msg, _msg_authority) =
                    env.create_update_msg(other_env.proof_chain)?;
                assert!(!NetworkKnowledge::verify_node_msg_can_be_trusted(
                    &msg_authority,
                    &corrupted_msg,
                    &known_keys
                ));

                // Other messages shall get rejected.
                let other_msg = SystemMsg::AntiEntropyProbe(known_keys[0]);
                assert!(!NetworkKnowledge::verify_node_msg_can_be_trusted(
                    &msg_authority,
                    &other_msg,
                    &known_keys
                ));
                Result::<()>::Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn ae_redirect_to_other_section() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local.run_until(async move {
            let mut env = Env::new().await?;

            let other_sk = bls::SecretKey::random();
            let other_pk = other_sk.public_key();

            let msg = env.create_msg(&env.other_sap.prefix(), other_pk)?;
            let sender = env.node.info().peer();

            // since it's not aware of the other prefix, it will redirect to self
            let dst_section_key = other_pk;
            let dst_name = env.other_sap.prefix().name();

            let original_bytes = msg.serialize_and_cache_bytes()?;
            let cmd = env
                .node
                .check_for_entropy(
                    original_bytes.clone(),
                    &dst_section_key,
                    dst_name,
                    &sender,
                );

            let msg = assert_matches!(cmd, Ok(Some(Cmd::SendMsg { msg: OutgoingMsg::System(msg), .. })) => {
                msg
            });

            assert_matches!(msg, SystemMsg::AntiEntropy { section_auth, kind: AntiEntropyKind::Redirect {..}, .. } => {
                assert_eq!(section_auth, env.node.network_knowledge().authority_provider().to_msg());
            });

            // now let's insert the other SAP to make it aware of the other prefix
            assert!(
                env.node
                    .network_knowledge
                    .update_knowledge_if_valid(
                        env.other_sap.clone(),
                        &env.proof_chain,
                        None,
                        &env.node.info().name(),
                        &env.node.section_keys_provider
                    )?
            );

            // and it now shall give us an AE redirect msg
            // with the SAP we inserted for other prefix
            let cmd = env
                .node
                .check_for_entropy(
                    original_bytes,
                    &dst_section_key,
                    dst_name,
                    &sender,
                );

            let msg = assert_matches!(cmd, Ok(Some(Cmd::SendMsg { msg: OutgoingMsg::System(msg), .. })) => {
                msg
            });

            assert_matches!(msg, SystemMsg::AntiEntropy { section_auth, kind: AntiEntropyKind::Redirect {..}, .. } => {
                assert_eq!(section_auth, env.other_sap.value.to_msg());
            });
            Result::<()>::Ok(())
        }).await
    }

    #[tokio::test]
    async fn ae_outdated_dst_key_of_our_section() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local.run_until(async move {


            let env = Env::new().await?;
            let our_prefix = env.node.network_knowledge().prefix();

            let msg = env.create_msg(
                &our_prefix,
                env.node.network_knowledge().section_key(),
            )?;
            let sender = env.node.info().peer();
            let dst_name = our_prefix.substituted_in(xor_name::rand::random());
            let dst_section_key = env.node.network_knowledge().genesis_key();

            let cmd = env
                .node
                .check_for_entropy(
                    msg.serialize_and_cache_bytes()?,
                    dst_section_key,
                    dst_name,
                    &sender,
                )?;

            let msg = assert_matches!(cmd, Some(Cmd::SendMsg { msg: OutgoingMsg::System(msg), .. }) => {
                msg
            });

            assert_matches!(&msg, SystemMsg::AntiEntropy { section_auth, proof_chain, kind: AntiEntropyKind::Retry{..}, .. } => {
                assert_eq!(section_auth, &env.node.network_knowledge().authority_provider().to_msg());
                assert_eq!(proof_chain, &env.node.our_section_dag());
            });
            Ok(())
        }).await
    }

    #[tokio::test]
    async fn ae_wrong_dst_key_of_our_section_returns_retry() -> Result<()> {
        // Construct a local task set that can run `!Send` futures.
        let local = tokio::task::LocalSet::new();

        // Run the local task set.
        local.run_until(async move {

            let env = Env::new().await?;
            let our_prefix = env.node.network_knowledge().prefix();

            let msg = env.create_msg(
                &our_prefix,
                env.node.network_knowledge().section_key(),
            )?;
            let sender = env.node.info().peer();
            let dst_name = our_prefix.substituted_in(xor_name::rand::random());

            let bogus_env = Env::new().await?;
            let dst_section_key = bogus_env.node.network_knowledge().genesis_key();

            let cmd = env
                .node
                .check_for_entropy(
                    msg.serialize_and_cache_bytes()?,
                    dst_section_key,
                    dst_name,
                    &sender,
                )?;

            let msg = assert_matches!(cmd, Some(Cmd::SendMsg { msg: OutgoingMsg::System(msg), .. }) => {
                msg
            });

            assert_matches!(&msg, SystemMsg::AntiEntropy { section_auth, proof_chain, kind: AntiEntropyKind::Retry {..}, .. } => {
                assert_eq!(*section_auth, env.node.network_knowledge().authority_provider().to_msg());
                assert_eq!(*proof_chain, env.node.our_section_dag());
            });
            Ok(())
        }).await
    }

    struct Env {
        node: Node,
        other_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: SecuredLinkedList,
    }

    impl Env {
        async fn new() -> Result<Self> {
            let prefix0 = Prefix::default().pushed(false);
            let prefix1 = Prefix::default().pushed(true);

            // generate a SAP for prefix0
            let (section_auth, mut nodes, secret_key_set) =
                random_sap(prefix0, elder_count(), 0, None);
            let info = nodes.remove(0);
            let sap_sk = secret_key_set.secret_key();
            let signed_sap = section_signed(sap_sk, section_auth)?;

            let (chain, genesis_sk_set) = create_chain(sap_sk, signed_sap.section_key())
                .context("failed to create section chain")?;
            let genesis_pk = genesis_sk_set.public_keys().public_key();
            assert_eq!(genesis_pk, *chain.root_key());

            let (max_capacity, root_storage_dir) = create_test_max_capacity_and_root_storage()?;
            let (mut node, _) = Node::first_node(
                create_comm().await?.socket_addr(),
                info.keypair.clone(),
                event_channel::new(1).0,
                UsedSpace::new(max_capacity),
                root_storage_dir,
                genesis_sk_set.clone(),
            )
            .await?;

            let section_key_share = SectionKeyShare {
                public_key_set: secret_key_set.public_keys(),
                index: 0,
                secret_key_share: secret_key_set.secret_key_share(0),
            };

            node.section_keys_provider = SectionKeysProvider::new(Some(section_key_share));

            // get our Node to now be in prefix(0)
            let _ = node.network_knowledge.update_knowledge_if_valid(
                signed_sap.clone(),
                &chain,
                None,
                &info.name(),
                &node.section_keys_provider,
            );

            // generate other SAP for prefix1
            let (other_sap, _, secret_key_set) = random_sap(prefix1, elder_count(), 0, None);
            let other_sap_sk = secret_key_set.secret_key();
            let other_sap = section_signed(other_sap_sk, other_sap)?;
            // generate a proof chain for this other SAP
            let mut proof_chain = SecuredLinkedList::new(genesis_pk);
            let signature = bincode::serialize(&other_sap_sk.public_key())
                .map(|bytes| genesis_sk_set.secret_key().sign(&bytes))?;
            proof_chain.insert(&genesis_pk, other_sap_sk.public_key(), signature)?;

            Ok(Self {
                node,
                other_sap,
                proof_chain,
            })
        }

        fn create_msg(
            &self,
            src_section_prefix: &Prefix,
            src_section_pk: BlsPublicKey,
        ) -> Result<WireMsg> {
            let sender = NodeInfo::new(
                ed25519::gen_keypair(&src_section_prefix.range_inclusive(), MIN_ADULT_AGE),
                gen_addr(),
            );

            // just some message we can construct easily
            let payload_msg = SystemMsg::AntiEntropyProbe(src_section_pk);

            let payload = WireMsg::serialize_msg_payload(&payload_msg)?;

            let dst = Dst {
                name: xor_name::rand::random(),
                section_key: SecretKey::random().public_key(),
            };

            let msg_id = MsgId::new();
            let node_auth = NodeAuth::authorize(src_section_pk, &sender.keypair, &payload);
            let auth = AuthKind::Node(node_auth.into_inner());

            Ok(WireMsg::new_msg(msg_id, payload, auth, dst))
        }

        fn create_update_msg(
            &self,
            proof_chain: SecuredLinkedList,
        ) -> Result<(SystemMsg, NodeMsgAuthority)> {
            let payload_msg = SystemMsg::AntiEntropy {
                section_auth: self.other_sap.value.to_msg(),
                section_signed: self.other_sap.sig.clone(),
                proof_chain,
                kind: AntiEntropyKind::Update {
                    members: BTreeSet::new(),
                },
            };

            let auth_proof = AuthorityProof(SectionAuthMsg {
                src_name: self.other_sap.value.prefix().name(),
                sig: self.other_sap.sig.clone(),
            });
            let node_auth = NodeMsgAuthority::Section(auth_proof);

            Ok((payload_msg, node_auth))
        }
    }

    // Creates a section chain with three blocks
    fn create_chain(
        sap_sk: &SecretKey,
        last_key: BlsPublicKey,
    ) -> Result<(SecuredLinkedList, bls::SecretKeySet)> {
        // create chain with random genesis key
        let genesis_sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());
        let genesis_pk = genesis_sk_set.public_keys().public_key();
        let mut chain = SecuredLinkedList::new(genesis_pk);

        // insert second key which is the PK derived from SAP's SK
        let sap_pk = sap_sk.public_key();
        let sig = genesis_sk_set
            .secret_key()
            .sign(&bincode::serialize(&sap_pk)?);
        chain.insert(&genesis_pk, sap_pk, sig)?;

        // insert third key which is provided `last_key`
        let last_sig = sap_sk.sign(&bincode::serialize(&last_key)?);
        chain.insert(&sap_pk, last_key, last_sig)?;

        Ok((chain, genesis_sk_set))
    }
}
