// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Core;
use crate::messaging::{
    system::{KeyedSig, SectionAuth, SectionPeers, SystemMsg},
    MessageType, SectionAuthorityProvider, SrcLocation, WireMsg,
};
use crate::routing::{
    dkg::SectionAuthUtils,
    error::{Error, Result},
    log_markers::LogMarker,
    messages::WireMsgUtils,
    routing_api::command::Command,
};
use crate::types::PublicKey;
use backoff::{backoff::Backoff, ExponentialBackoff};
use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use itertools::Itertools;
use secured_linked_list::SecuredLinkedList;
use std::net::SocketAddr;
use xor_name::XorName;

impl Core {
    pub(crate) async fn handle_anti_entropy_update_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        members: Option<SectionPeers>,
        sender: SocketAddr,
    ) -> Result<Vec<Command>> {
        let snapshot = self.state_snapshot().await;

        let signed_section_auth = SectionAuth {
            value: section_auth.clone(),
            sig: section_signed,
        };

        match self.network.verify_with_chain_and_update(
            signed_section_auth.clone(),
            &proof_chain,
            &self.section.chain().await,
        ) {
            Ok(updated) => {
                if updated {
                    info!(
                        "Anti-Entropy: updated remote section SAP updated for {:?}",
                        section_auth.prefix
                    );

                    // FIXME: perhaps we should update our section chain
                    // only upon successful DKG round we participated on??
                    self.section
                        .merge_chain(&signed_section_auth, proof_chain)
                        .await?;
                } else {
                    debug!(
                        "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
                        section_auth.prefix, section_auth
                    );
                }

                if let Some(peers) = members {
                    self.section.merge_members(peers).await?;
                }
            }
            Err(err) => {
                warn!(
                    "Anti-Entropy: Did not update remote section SAP provided by {:?}: {:?}",
                    sender, err
                );
                return Err(err);
            }
        }

        self.fire_node_event_for_any_new_adults().await?;

        // always run this, only changes will trigger events
        self.update_self_for_new_node_state_and_fire_events(snapshot)
            .await
    }
    pub(crate) async fn handle_anti_entropy_retry_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        proof_chain: SecuredLinkedList,
        bounced_msg: Bytes,
        sender: SocketAddr,
        src_name: XorName,
    ) -> Result<Vec<Command>> {
        let bounced_msg = match WireMsg::deserialize(bounced_msg)? {
            MessageType::System { msg, .. } => msg,
            _ => {
                warn!("Non System MessageType received at Node in AE response. We do not handle any other type yet");
                return Ok(vec![]);
            }
        };

        info!(
            "Anti-Entropy: retry message received from peer: {} ({})",
            src_name, sender
        );

        match self.network.verify_with_chain_and_update(
            SectionAuth {
                value: section_auth.clone(),
                sig: section_signed,
            },
            &proof_chain,
            &self.section.chain().await,
        ) {
            Ok(updated) => {
                if updated {
                    // Update the Prefixmap written on our disk
                    info!(
                        "Anti-Entropy: updated remote section SAP updated for {:?}",
                        section_auth.prefix
                    );
                    self.write_prefix_map().await;
                    info!("PrefixMap written to disk");
                } else {
                    debug!(
                        "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
                        section_auth.prefix, section_auth
                    );
                }

                // Regardless if the SAP already existed, as long as it was valid
                // (it may have been just updated by a concurrent handler of another bounced msg),
                //we still resend this message.
                //
                // TODO: we may need to check if 'bounced_msg' dest section pk is different
                // from the received new SAP key, to prevent from endlessly resending a msg
                // if a sybil/corrupt peer keeps sending us the same AE msg.
                let dst_section_pk = section_auth.public_key_set.public_key();
                trace!("{}", LogMarker::AeResendAfterRetry);

                self.create_or_wait_for_backoff(&src_name, &sender).await;

                let cmd = self
                    .send_direct_message((src_name, sender), bounced_msg, dst_section_pk)
                    .await?;
                Ok(vec![cmd])
            }
            Err(err) => {
                warn!("Anti-Entropy: failed to update remote section SAP, bounced msg from {:?} dropped: {:?}, {:?}", sender, bounced_msg, err);
                Ok(vec![])
            }
        }
    }

    pub(crate) async fn handle_anti_entropy_redirect_msg(
        &self,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        bounced_msg: Bytes,
        sender: SocketAddr,
    ) -> Result<Vec<Command>> {
        debug!(
            "Anti-Entropy: redirect message received from peer: {}",
            sender
        );

        let (bounced_msg, msg_id) = match WireMsg::deserialize(bounced_msg)? {
            MessageType::System { msg_id, msg, .. } => (msg, msg_id),
            _ => {
                warn!("Non System MessageType received at Node in AE response. We do not handle any other type yet");
                return Ok(vec![]);
            }
        };

        // Check if SAP signature is valid
        let section_signed = SectionAuth {
            value: section_auth.clone(),
            sig: section_signed,
        };
        if !section_signed.self_verify() {
            warn!(
                "Anti-Entropy: failed to verify signature of SAP received in a redirect msg, bounced msg ({:?}) dropped: {:?}",
                msg_id,bounced_msg
            );
            return Ok(vec![]);
        }

        // Try to find Elders we have in our local records matching the prefix of the
        // provided SAP, or just use the Elders contained in the provided SAP.
        // The chosen dst_section_pk will either be the latest we are aware of
        // if we find a matching prefix in our records, or the genesis_key otherwise.
        let (dst_elders, dst_section_pk) =
            match self.network.section_by_prefix(&section_auth.prefix) {
                Ok(trusted_sap) => (trusted_sap.elders, trusted_sap.public_key_set.public_key()),
                Err(_) => {
                    // In case we don't have the knowledge of that neighbour locally,
                    // let's take the Elders from the provided SAP and genesis key.
                    (section_signed.value.elders, *self.section.genesis_key())
                }
            };

        // We choose the Elder closest to the dest section key,
        // just to pick one of them in a random but deterministic fashion.
        let name = XorName::from(PublicKey::Bls(dst_section_pk));
        let chosen_dst_elder = dst_elders
            .iter()
            .filter(|(elder, _)| section_auth.elders.contains_key(elder))
            .sorted_by(|lhs, rhs| name.cmp_distance(lhs.0, rhs.0))
            .next();

        if let Some((name, addr)) = chosen_dst_elder {
            if addr == &sender {
                error!(
                    "Failed to find an alternative Elder to resend msg ({:?}) upon AE-Redirect response.",msg_id
                );
                Ok(vec![])
            } else {
                trace!("{}", LogMarker::AeResendAfterAeRedirect);

                self.create_or_wait_for_backoff(name, addr).await;

                let cmd = self
                    .send_direct_message((*name, *addr), bounced_msg, dst_section_pk)
                    .await?;
                Ok(vec![cmd])
            }
        } else {
            warn!(
                "Anti-Entropy: no trust-worthy elder among incoming SAP {:?} and locally known elders.",
                section_auth
            );

            // For the situation non-elder exists in both incoming and local SAP, send to one of
            // the incoming elder with the geneis key to trigger AE.
            if let Some((name, addr)) = section_auth.elders.iter().next() {
                trace!("{}", LogMarker::BounceAfterNewElderNotKnownLocally);

                let cmd = self
                    .send_direct_message((*name, *addr), bounced_msg, *self.section.genesis_key())
                    .await?;
                Ok(vec![cmd])
            } else {
                error!(
                    "Anti-Entropy: incoming SAP in {:?} doesn't contain any elder! {:?}",
                    msg_id, section_auth
                );
                Ok(vec![])
            }
        }
    }

    /// Checks AeBackoffCache for backoff, or creates a new instance
    /// waits for any required backoff duration
    async fn create_or_wait_for_backoff(&self, name: &XorName, addr: &SocketAddr) {
        let mut ae_backoff_guard = self.ae_backoff_cache.write().await;

        if let Some(backoff) = ae_backoff_guard
            .find(|(node_name, socket, _)| node_name == name && socket == addr)
            .map(|(_, _, backoff)| backoff)
        {
            if let Some(next_wait) = backoff.next_backoff() {
                tokio::time::sleep(next_wait).await;
            } else {
                // TODO: we've done all backoffs and are _still_ getting messages?
                // we should probably penalise the node here.
            }
        } else {
            let _res = ae_backoff_guard.insert((*name, *addr, ExponentialBackoff::default()));
        }
    }

    // If entropy is found, determine the msg to send in order to
    // bring the sender's knowledge about us up to date.
    pub(crate) async fn check_for_entropy(
        &self,
        original_bytes: Bytes,
        src_location: &SrcLocation,
        dst_section_pk: &BlsPublicKey,
        dst_name: XorName,
        sender: SocketAddr,
    ) -> Result<Option<Command>> {
        trace!("Checking for entropy");
        // Check if the message has reached the correct section,
        // if not, we'll need to respond with AE

        // Let's try to find a section closer to the destination, if it's not for us.
        if !self.section.prefix().await.matches(&dst_name) {
            debug!("AE: prefix not matching");
            match self.network.closest_or_opposite(&dst_name) {
                Some(section_auth) => {
                    info!("Found a better matching section {:?}", section_auth);
                    let bounced_msg = original_bytes;
                    // Redirect to the closest section
                    let ae_msg = SystemMsg::AntiEntropyRedirect {
                        section_auth: section_auth.value.clone(),
                        section_signed: section_auth.sig,
                        bounced_msg,
                    };
                    let wire_msg = WireMsg::single_src(
                        &self.node.read().await.clone(),
                        src_location.to_dst(),
                        ae_msg,
                        self.section.section_key().await,
                    )?;
                    trace!("{}", LogMarker::AeSendRedirect);

                    return Ok(Some(Command::SendMessage {
                        recipients: vec![(src_location.name(), sender)],
                        wire_msg,
                    }));
                }
                None => {
                    error!("Our PrefixMap is empty");
                    // TODO: do we want to reroute some data messages to another seciton here using check_for_better_section_sap_for_data ?
                    // if not we can remove that function.

                    // TODO: instead of just dropping the message, don't we actually need
                    // to get up to date info from other Elders in our section as it may be
                    // a section key we are not aware of yet?
                    // ...and once we acquired new key/s we attempt AE check again?
                    error!(
                            "Anti-Entropy: cannot reply with redirect msg for dst_name {:?} and key {:?} to a closest section.",
                            dst_name, dst_section_pk
                        );

                    return Err(Error::NoMatchingSection);
                }
            }
        }

        trace!(
            "Performing AE checks, provided pk was: {:?} ours is: {:?}",
            dst_section_pk,
            self.section.section_key().await
        );

        if dst_section_pk == &self.section.section_key().await {
            trace!("Provided Section PK matching our latest. All AE checks passed!");
            // Destination section key matches our current section key
            return Ok(None);
        }

        let ae_msg = match self
            .section
            .chain()
            .await
            .get_proof_chain_to_current(dst_section_pk)
        {
            Ok(proof_chain) => {
                debug!(
                    ">> the proof chain: len: {:?}, chain: {:?}",
                    proof_chain.len(),
                    proof_chain
                );
                info!("Anti-Entropy: sender's ({}) knowledge of our SAP is outdated, bounce msg for AE-Retry with up to date SAP info.", sender);

                let section_signed_auth = self.section.section_signed_authority_provider().await;
                let section_auth = section_signed_auth.value;
                let section_signed = section_signed_auth.sig;
                let bounced_msg = original_bytes;

                trace!("{}", LogMarker::AeSendRetryAsOutdated);

                SystemMsg::AntiEntropyRetry {
                    section_auth,
                    section_signed,
                    proof_chain,
                    bounced_msg,
                }
            }
            Err(_) => {
                trace!(
                    "Anti-Entropy: cannot find dst_section_pk {:?} sent by {} in our chain",
                    dst_section_pk,
                    sender
                );

                let proof_chain = self.section.chain().await;

                let section_signed_auth = self.section.section_signed_authority_provider().await;
                let section_auth = section_signed_auth.value;
                let section_signed = section_signed_auth.sig;
                let bounced_msg = original_bytes;

                trace!("{}", LogMarker::AeSendRetryDstPkFail);

                SystemMsg::AntiEntropyRetry {
                    section_auth,
                    section_signed,
                    proof_chain,
                    bounced_msg,
                }
            }
        };

        let wire_msg = WireMsg::single_src(
            &self.node.read().await.clone(),
            src_location.to_dst(),
            ae_msg,
            self.section.section_key().await,
        )?;

        Ok(Some(Command::SendMessage {
            recipients: vec![(src_location.name(), sender)],
            wire_msg,
        }))
    }

    // generate an AE redirect command for the given message
    pub(crate) async fn ae_redirect(
        &self,
        sender: SocketAddr,
        src_location: &SrcLocation,
        original_wire_msg: &WireMsg,
    ) -> Result<Command> {
        let section_signed_auth = self
            .section
            .section_signed_authority_provider()
            .await
            .clone();
        let section_auth = section_signed_auth.value;
        let section_signed = section_signed_auth.sig;

        let ae_msg = SystemMsg::AntiEntropyRedirect {
            section_auth,
            section_signed,
            bounced_msg: original_wire_msg.serialize()?,
        };

        let wire_msg = WireMsg::single_src(
            &self.node.read().await.clone(),
            src_location.to_dst(),
            ae_msg,
            self.section.section_key().await,
        )?;

        trace!("{} in ae_redirect", LogMarker::AeSendRedirect);

        Ok(Command::SendMessage {
            recipients: vec![(src_location.name(), sender)],
            wire_msg,
        })
    }

    // checks to see if we're actually in the ideal section for this data
    #[allow(dead_code)]
    pub(crate) async fn check_for_better_section_sap_for_data(
        &self,
        data_name: Option<XorName>,
    ) -> Option<SectionAuth<SectionAuthorityProvider>> {
        if let Some(data_name) = data_name {
            let our_sap = self.section.section_signed_authority_provider().await;
            trace!("Our SAP: {:?}", our_sap);

            match self.network.closest_or_opposite(&data_name) {
                Some(better_sap) => {
                    // Update the client of the actual destination section
                    trace!(
                        "We have a better matched section for the data name {:?}",
                        data_name
                    );
                    Some(better_sap)
                }
                None => {
                    trace!(
                        "We don't have a better matching section for data name {:?}m our SAP: {:?}",
                        data_name,
                        our_sap
                    );
                    None
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::{DstLocation, MessageId, MessageType, MsgKind, NodeAuth};
    use crate::routing::{
        create_test_used_space_and_root_storage,
        dkg::test_utils::section_signed,
        ed25519,
        node::Node,
        routing_api::tests::create_comm,
        section::{
            test_utils::{gen_addr, gen_section_authority_provider},
            Section,
        },
        XorName, ELDER_SIZE, MIN_ADULT_AGE,
    };
    use assert_matches::assert_matches;
    use bls::SecretKey;
    use eyre::{eyre, Context, Result};
    use rand::Rng;
    use secured_linked_list::SecuredLinkedList;
    use tokio::sync::mpsc;
    use xor_name::Prefix;

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_everything_up_to_date() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.section().prefix().await;
        let (msg, src_location) =
            env.create_message(&our_prefix, *env.core.section_chain().await.last_key())?;
        let sender = env.core.node.read().await.addr;
        let dst_name = our_prefix.substituted_in(rng.gen());
        let dst_section_pk = *env.core.section_chain().await.last_key();

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_pk,
                dst_name,
                sender,
            )
            .await?;

        assert!(command.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_redirect_to_other_section() -> Result<()> {
        let env = Env::new().await?;

        let other_sk = bls::SecretKey::random();
        let other_pk = other_sk.public_key();

        let (msg, src_location) = env.create_message(&env.other_sap.value.prefix, other_pk)?;
        let sender = env.core.node.read().await.addr;

        // since it's not aware of the other prefix, it shall fail with NoMatchingSection
        let dst_section_pk = other_pk;
        let dst_name = env.other_sap.value.prefix.name();
        match env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_pk,
                dst_name,
                sender,
            )
            .await
        {
            Err(Error::NoMatchingSection) => {}
            _ => return Err(eyre!("expected Error::NoMatchingSection")),
        }

        // now let's insert a SAP to make it aware of the other prefix
        assert!(env
            .core
            .network
            .update(env.other_sap.clone(), &env.proof_chain)?);

        // and it now shall give us an AE redirect msg
        // with the SAP we inserted for other prefix
        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_pk,
                dst_name,
                sender,
            )
            .await?;

        let msg_type = assert_matches!(command, Some(Command::SendMessage { wire_msg, .. }) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRedirect { section_auth, .. } => {
                assert_eq!(section_auth, env.other_sap.value);
            });
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_outdated_dst_key_of_our_section() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.section().prefix().await;

        let (msg, src_location) =
            env.create_message(&our_prefix, *env.core.section_chain().await.last_key())?;
        let sender = env.core.node.read().await.addr;
        let dst_name = our_prefix.substituted_in(rng.gen());
        let dst_section_pk = *env.core.section_chain().await.root_key();

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_pk,
                dst_name,
                sender,
            )
            .await?;

        let msg_type = assert_matches!(command, Some(Command::SendMessage { wire_msg, .. }) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRetry { ref section_auth, ref proof_chain, .. } => {
                assert_eq!(section_auth, &env.core.section().authority_provider().await);
                assert_eq!(proof_chain, &env.core.section_chain().await);
            });
        });

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_wrong_dst_key_of_our_section_returns_retry() -> Result<()> {
        let mut rng = rand::thread_rng();
        let env = Env::new().await?;
        let our_prefix = env.core.section().prefix().await;

        let (msg, src_location) =
            env.create_message(&our_prefix, *env.core.section_chain().await.last_key())?;
        let sender = env.core.node.read().await.addr;
        let dst_name = our_prefix.substituted_in(rng.gen());

        let bogus_env = Env::new().await?;
        let dst_section_pk = *bogus_env.core.section_chain().await.root_key();

        let command = env
            .core
            .check_for_entropy(
                msg.serialize()?,
                &src_location,
                &dst_section_pk,
                dst_name,
                sender,
            )
            .await?;

        let msg_type = assert_matches!(command, Some(Command::SendMessage { wire_msg, .. }) => {
            wire_msg
                .into_message()
                .context("failed to deserialised anti-entropy message")?
        });

        assert_matches!(msg_type, MessageType::System{ msg, .. } => {
            assert_matches!(msg, SystemMsg::AntiEntropyRetry { ref section_auth, ref proof_chain, .. } => {
                assert_eq!(*section_auth, env.core.section().authority_provider().await);
                assert_eq!(*proof_chain, env.core.section_chain().await);
            });
        });

        Ok(())
    }

    struct Env {
        core: Core,
        other_sap: SectionAuth<SectionAuthorityProvider>,
        proof_chain: SecuredLinkedList,
    }

    impl Env {
        async fn new() -> Result<Self> {
            let prefix0 = Prefix::default().pushed(false);
            let prefix1 = Prefix::default().pushed(true);

            // generate a SAP for prefix0
            let (section_auth, mut nodes, secret_key_set) =
                gen_section_authority_provider(prefix0, ELDER_SIZE);
            let node = nodes.remove(0);
            let sap_sk = secret_key_set.secret_key();
            let signed_section_auth = section_signed(sap_sk, section_auth)?;

            let (chain, genesis_sk_set) = create_chain(
                sap_sk,
                signed_section_auth.value.public_key_set.public_key(),
            )
            .context("failed to create section chain")?;
            let genesis_pk = genesis_sk_set.public_keys().public_key();
            assert_eq!(genesis_pk, *chain.root_key());

            let section = Section::new(genesis_pk, chain, signed_section_auth)
                .context("failed to create section")?;

            let (used_space, root_storage_dir) = create_test_used_space_and_root_storage()?;
            let core = Core::first_node(
                create_comm().await?,
                node.clone(),
                mpsc::channel(1).0,
                used_space,
                root_storage_dir,
                genesis_sk_set.clone(),
            )
            .await?;

            core.relocate(node, section).await?;

            // generate other SAP for prefix1
            let (other_sap, _, secret_key_set) =
                gen_section_authority_provider(prefix1, ELDER_SIZE);
            let other_sap_sk = secret_key_set.secret_key();
            let other_sap = section_signed(other_sap_sk, other_sap)?;
            // generate a proof chain for this other SAP
            let mut proof_chain = SecuredLinkedList::new(genesis_pk);
            let signature = bincode::serialize(&other_sap_sk.public_key())
                .map(|bytes| genesis_sk_set.secret_key().sign(&bytes))?;
            proof_chain.insert(&genesis_pk, other_sap_sk.public_key(), signature)?;

            Ok(Self {
                core,
                other_sap,
                proof_chain,
            })
        }

        fn create_message(
            &self,
            src_section_prefix: &Prefix,
            src_section_pk: BlsPublicKey,
        ) -> Result<(WireMsg, SrcLocation)> {
            let sender = Node::new(
                ed25519::gen_keypair(&src_section_prefix.range_inclusive(), MIN_ADULT_AGE),
                gen_addr(),
            );

            let sender_name = sender.name();
            let src_node_keypair = sender.keypair;

            let payload_msg = SystemMsg::StartConnectivityTest(XorName::random());
            let payload = WireMsg::serialize_msg_payload(&payload_msg)?;

            let dst_name = XorName::random();
            let dst_section_pk = SecretKey::random().public_key();
            let dst_location = DstLocation::Node {
                name: dst_name,
                section_pk: dst_section_pk,
            };

            let msg_id = MessageId::new();

            let node_auth = NodeAuth::authorize(src_section_pk, &src_node_keypair, &payload);

            let msg_kind = MsgKind::NodeAuthMsg(node_auth.into_inner());

            let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;

            let src_location = SrcLocation::Node {
                name: sender_name,
                section_pk: src_section_pk,
            };

            Ok((wire_msg, src_location))
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
