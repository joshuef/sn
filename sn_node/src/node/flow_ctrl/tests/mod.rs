// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![allow(dead_code)]
pub(crate) mod dbc_utils;
pub(crate) mod network_builder;
pub(crate) mod test_utils;

use crate::node::{
    flow_ctrl::tests::{
        network_builder::TestNetworkBuilder,
        test_utils::{gen_info_with_comm, gen_node_infos_with_comm},
    },
    messaging::Recipients,
    Cmd, Error, MyNode,
};

use sn_comms::{CommEvent, MsgReceived};
use sn_dbc::Hash;
use sn_interface::{
    dbcs::{gen_genesis_dbc, DbcReason},
    elder_count, init_logger,
    messaging::{
        data::{
            ClientMsg, CmdResponse, DataCmd, DataResponse, Error as MessagingDataError,
            SpentbookCmd,
        },
        system::{JoinResponse, NodeDataCmd, NodeMsg},
        AntiEntropyKind, AntiEntropyMsg, Dst, NetworkMsg, WireMsg,
    },
    network_knowledge::{
        section_keys::SectionKeysProvider, Error as NetworkKnowledgeError, MyNodeInfo, NodeState,
        RelocationInfo, RelocationProof, SectionTreeUpdate, SectionsDAG, MIN_ADULT_AGE,
    },
    test_utils::*,
    types::{keys::ed25519, Participant, PublicKey},
};

use assert_matches::assert_matches;
use eyre::{bail, eyre, Result};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use std::{
    collections::{BTreeSet, HashSet},
    iter,
};
use test_utils::ProcessAndInspectCmds;
use tokio::sync::mpsc;
use xor_name::{Prefix, XorName};

#[tokio::test]
async fn membership_churn_starts_on_join_request_from_relocated_node() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("receive_join_request_from_relocated_node").entered();

    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix).adult_count(1))
        .build()?;

    let sk_set = env.get_secret_key_set(prefix, None)?;
    let section_key = sk_set.public_keys().public_key();

    let adult_node = env.get_nodes(prefix, 0, 1, None)?.remove(0);
    let old_info = adult_node.info();
    let old_name = old_info.name();
    let old_keypair = old_info.keypair.clone();

    let new_info = MyNodeInfo::new(
        ed25519::gen_keypair(&prefix.range_inclusive(), old_info.age() + 1),
        gen_addr(),
    );

    let (relocation_trigger, _) = create_relocation_trigger(&sk_set, 1, old_info.age() + 1)?;
    let relocated_state = NodeState::relocated(old_info.id(), Some(old_name), relocation_trigger);
    let section_signed_state = TestKeys::get_section_signed(&sk_set.secret_key(), relocated_state)?;

    let info = RelocationInfo::new(section_signed_state, new_info.name());
    let serialized_info = bincode::serialize(&info)?;
    let signature_over_new_name = ed25519::sign(&serialized_info, &old_keypair);

    let proof = RelocationProof::new(info, signature_over_new_name, old_keypair.public);

    let wire_msg = single_src_node(
        new_info.name(),
        Dst {
            name: XorName::from(PublicKey::Bls(section_key)),
            section_key,
        },
        NodeMsg::TryJoin(Some(proof)),
    )?;

    let mut elder_node = env.get_nodes(prefix, 1, 0, None)?.remove(0);

    ProcessAndInspectCmds::new(Cmd::HandleMsg {
        sender: Participant::from_node(new_info.id()),
        wire_msg,
        send_stream: None,
    })
    .process_all(&mut elder_node)
    .await?;

    assert!(elder_node
        .membership
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());

    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_online() -> Result<()> {
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix))
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let sk_set = env.get_secret_key_set(prefix, None)?;
    let new_node_id = gen_node_id(MIN_ADULT_AGE);
    let mut approval_sent = false;
    let new_node_state = NodeState::joined(new_node_id, None);

    let membership_decision = section_decision(&sk_set, 1, new_node_state.clone())?;
    let mut all_cmds =
        ProcessAndInspectCmds::new(Cmd::HandleMembershipDecision(membership_decision));

    while let Some(cmd) = all_cmds.next(&mut node).await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                recipients,
                msg: NetworkMsg::Node(msg),
                ..
            } => (msg, recipients),
            _ => continue,
        };

        match msg {
            NodeMsg::JoinResponse(JoinResponse::Approved { .. }) => {
                assert_matches!(recipients, Recipients::Multiple(nodes) => {
                    assert_eq!(nodes, &BTreeSet::from([new_node_id]));
                });
                approval_sent = true;
            }
            _ => continue,
        }
    }
    assert!(approval_sent);
    assert!(node.network_knowledge().is_adult(&new_node_state.name()));

    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_online_of_elder_candidate() -> Result<()> {
    init_logger();
    // Creates nodes where everybody has age 6 except one has 5.
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix).elder_age_pattern(vec![MIN_ADULT_AGE, MIN_ADULT_AGE + 1]))
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let node_name = node.name();
    let section = env.get_network_knowledge(prefix, None)?;
    let sk_set = env.get_secret_key_set(prefix, None)?;

    let mut expected_new_elders = section
        .elders()
        .into_iter()
        .filter(|node_id| node_id.age() == MIN_ADULT_AGE + 1)
        .collect::<BTreeSet<_>>();

    // Handle agreement on Online of a node that is older than the youngest
    // current elder - that means this node is going to be promoted.
    let new_node = gen_node_id(MIN_ADULT_AGE + 1);
    let node_state = NodeState::joined(new_node, None);

    let membership_decision = section_decision(&sk_set, 1, node_state.clone())?;

    // Force this node to join
    node.membership
        .as_mut()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .force_bootstrap(node_state);

    let mut cmds = ProcessAndInspectCmds::new(Cmd::HandleMembershipDecision(membership_decision));

    // Verify we sent a `DkgStart` message with the expected participants.
    let mut dkg_start_sent = false;
    let _changed = expected_new_elders.insert(new_node);

    while let Some(cmd) = cmds.next(&mut node).await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                recipients,
                msg: NetworkMsg::Node(msg),
                ..
            } => (msg, recipients),
            _ => continue,
        };

        let actual_elder_candidates = match msg {
            NodeMsg::DkgStart(session, _) => session.elders.clone(),
            _ => continue,
        };

        itertools::assert_equal(
            actual_elder_candidates,
            expected_new_elders.iter().map(|p| (p.name(), p.addr())),
        );

        let expected_dkg_start_recipients: BTreeSet<_> = expected_new_elders
            .iter()
            .filter(|node_id| node_id.name() != node_name)
            .cloned()
            .collect();

        assert_matches!(recipients, Recipients::Multiple(nodes) => {
            assert_eq!(nodes, &expected_dkg_start_recipients);
        });

        dkg_start_sent = true;
    }

    assert!(dkg_start_sent);
    Ok(())
}

#[tokio::test]
async fn handle_join_request_of_rejoined_node() -> Result<()> {
    init_logger();
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix))
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);

    // Make a left node.
    let node_id = gen_node_id_in_prefix(MIN_ADULT_AGE, prefix);
    node.membership
        .as_mut()
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .force_bootstrap(NodeState::left(node_id, None));

    // Simulate the same node rejoining
    let node_state = NodeState::joined(node_id, None);
    let join_cmd = node.propose_membership_change(node_state);

    // A rejoining node will always be rejected
    assert!(join_cmd.is_none()); // no cmd signals this membership proposal was dropped.
    assert!(!node
        .membership
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());
    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_offline_of_non_elder() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_agreement_on_offline_of_non_elder").entered();
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix).adult_count(1))
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let sk_set = env.get_secret_key_set(prefix, None)?;

    // get the node state of the non_elder node
    let node_state = env.get_nodes(prefix, 0, 1, None)?.remove(0).info().id();
    let node_state = NodeState::left(node_state, None);

    let proposal = node_state.clone();
    let sig = TestKeys::get_section_sig_bytes(&sk_set.secret_key(), &get_single_sig(&proposal));

    ProcessAndInspectCmds::new(Cmd::HandleNodeOffAgreement { proposal, sig })
        .process_all(&mut node)
        .await?;

    assert!(!node
        .network_knowledge()
        .section_members()
        .contains(&node_state));
    Ok(())
}

#[tokio::test]
async fn handle_agreement_on_offline_of_elder() -> Result<()> {
    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix))
        .build()?;
    let mut elders = env.get_nodes(prefix, 2, 0, None)?;
    let mut node = elders.remove(0);
    let sk_set = env.get_secret_key_set(prefix, None)?;

    let elder = elders.remove(0);
    let elder_id = elder.info().id();
    let offline_elder = NodeState::left(elder_id, None);

    // Handle agreement on the Offline proposal
    let proposal = offline_elder.clone();
    let sig = TestKeys::get_section_sig_bytes(&sk_set.secret_key(), &get_single_sig(&proposal));

    ProcessAndInspectCmds::new(Cmd::HandleNodeOffAgreement { proposal, sig })
        .process_all(&mut node)
        .await?;

    // Verify we initiated a membership churn
    assert!(node
        .membership
        .ok_or_else(|| eyre!("Membership for the node must be set"))?
        .is_churn_in_progress());
    Ok(())
}

#[tokio::test]
async fn ae_msg_from_the_future_is_handled() -> Result<()> {
    init_logger();
    let _span = info_span!("ae_msg_from_the_future_is_handled").entered();

    let prefix = Prefix::default();
    let (elders0, ..) = gen_node_infos_with_comm(&prefix, elder_count(), 0, Some(&[6]), None);
    let new_elder = gen_info_with_comm(MIN_ADULT_AGE, Some(prefix));
    let elders1 = elders0
        .clone()
        .into_iter()
        .take(elder_count() - 1)
        .chain(vec![(new_elder.0, new_elder.1)])
        .collect::<Vec<_>>();

    // SAP0 is succeeded by SAP1 with a change in elder list
    let env = TestNetworkBuilder::new(thread_rng())
        .sap_with_members(prefix, elders0.clone(), elders0)
        .sap_with_members(prefix, elders1.clone(), elders1)
        .build()?;
    let sk_set0 = env.get_secret_key_set(prefix, Some(0))?;
    let sap1 = env.get_sap(prefix, Some(1))?;
    let sk_set1 = env.get_secret_key_set(prefix, Some(1))?;
    let pk_0 = sk_set0.public_keys().public_key();

    // Our node does not know about SAP1
    let mut node = env.get_nodes(prefix, 1, 0, Some(0))?.remove(0);

    let new_section_elders: BTreeSet<_> = sap1.elders_set();
    let section_tree_update = TestSectionTree::get_section_tree_update(
        &sap1,
        &node.section_chain(),
        &sk_set0.secret_key(),
    )?;

    // Create the `Sync` message containing the new `Section`.
    let sender = gen_info(MIN_ADULT_AGE, None);
    let wire_msg = ae_msg(
        sender.name(),
        Dst {
            name: XorName::from(PublicKey::Bls(pk_0)),
            section_key: pk_0,
        },
        AntiEntropyMsg::AntiEntropy {
            section_tree_update,
            kind: AntiEntropyKind::Update {
                section_decisions: Default::default(),
            },
        },
    )?;

    // Simulate DKG round finished succesfully by adding
    // the new section key share to our cache
    node.section_keys_provider
        .insert(TestKeys::get_section_key_share(&sk_set1, 0));

    ProcessAndInspectCmds::new(Cmd::HandleMsg {
        sender: Participant::from_node(sender.id()),
        wire_msg,
        send_stream: None,
    })
    .process_all(&mut node)
    .await?;

    // Verify our `Section` got updated.
    assert_lists(node.network_knowledge().elders(), new_section_elders);
    Ok(())
}

/// Checking when we send AE info to a section from untrusted section, we do not handle it and
/// error out.
#[tokio::test]
async fn untrusted_ae_msg_errors() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("untrusted_ae_msg_errors").entered();

    let prefix = Prefix::default();
    let env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix))
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let section = env.get_network_knowledge(prefix, None)?;
    let signed_sap = section.signed_sap();
    let sk_set = env.get_secret_key_set(prefix, None)?;
    let pk = sk_set.secret_key().public_key();

    // a valid AE msg but with a non-verifiable SAP...
    let bogus_env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix))
        .build()?;
    let bogus_sap = bogus_env.get_network_knowledge(prefix, None)?.signed_sap();
    let bogus_section_pk = bls::SecretKey::random().public_key();
    let bogus_section_tree_update =
        SectionTreeUpdate::new(bogus_sap, SectionsDAG::new(bogus_section_pk));

    let sender = gen_info(MIN_ADULT_AGE, None);

    let wire_msg = ae_msg(
        sender.name(),
        Dst {
            name: XorName::from(PublicKey::Bls(bogus_section_pk)),
            section_key: bogus_section_pk,
        },
        // we use the nonsense here
        AntiEntropyMsg::AntiEntropy {
            section_tree_update: bogus_section_tree_update,
            kind: AntiEntropyKind::Update {
                section_decisions: Default::default(),
            },
        },
    )?;

    assert!(matches!(
        ProcessAndInspectCmds::new(Cmd::HandleMsg {
            sender: Participant::from_node(sender.id()),
            wire_msg,
            send_stream: None,
        },)
        .process_all(&mut node)
        .await,
        Err(Error::NetworkKnowledge(
            NetworkKnowledgeError::SAPKeyNotCoveredByProofChain(_)
        ))
    ));

    assert_eq!(node.network_knowledge().genesis_key(), &pk);
    assert_eq!(
        node.network_knowledge()
            .section_tree()
            .all()
            .collect::<Vec<_>>(),
        vec![&signed_sap.value]
    );
    Ok(())
}

#[tokio::test]
async fn msg_to_self() -> Result<()> {
    let prefix = Prefix::default();
    let mut env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(prefix).elder_count(1))
        .build()?;

    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let mut comm_rx = env.take_comm_rx(node.info().public_key());
    let info = node.info();

    let node_msg = NodeMsg::NodeDataCmd(NodeDataCmd::ReplicateDataBatch(vec![]));

    // don't use the cmd collection fn, as it skips Cmd::SendMsg
    let (sender, mut receiver) = mpsc::channel(100);
    let cmds = MyNode::test_process_cmd(
        Cmd::send_msg(
            node_msg.clone(),
            Recipients::Single(Participant::from_node(info.id())),
        ),
        &mut node,
        &sender,
        &mut receiver,
    )
    .await?;

    assert!(cmds.is_empty());

    let msg_type = assert_matches!(comm_rx.recv().await, Some(CommEvent::Msg(MsgReceived { sender, wire_msg, .. })) => {
        assert_eq!(sender.addr(), info.addr);
        assert_matches!(wire_msg.into_msg(), Ok(msg_type) => msg_type)
    });

    assert_matches!(msg_type, NetworkMsg::Node(msg) => {
        assert_eq!(
            msg,
            node_msg
        );
    });
    Ok(())
}

#[tokio::test]
async fn handle_elders_update() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_elders_update").entered();

    let prefix = Prefix::default();
    // Start with section that has `elder_count()` elders with age 6, 1 non-elder with age 5 and one
    // to-be-elder with age 7
    let (elders0, ..) = gen_node_infos_with_comm(&prefix, elder_count(), 1, Some(&[6]), None);
    let mut elders1 = elders0.clone();
    let promoted_node = {
        let (promoted_node, promoted_comm, _) = gen_info_with_comm(MIN_ADULT_AGE + 2, None);
        (promoted_node, promoted_comm)
    };
    // members list remain the same for the two SAPs
    let members = elders1
        .clone()
        .into_iter()
        .chain(vec![promoted_node.clone()]);

    let demoted_node = elders1.remove(elders1.len() - 1);
    elders1.push(promoted_node.clone());

    let env = TestNetworkBuilder::new(StdRng::seed_from_u64(123))
        .sap_with_members(prefix, elders0, members.clone())
        .sap_with_members(prefix, elders1, members)
        .build()?;
    let section0 = env.get_network_knowledge(prefix, Some(0))?;
    let sk_set0 = env.get_secret_key_set(prefix, Some(0))?;
    let sap1 = env.get_sap(prefix, Some(1))?;
    let sk_set1 = env.get_secret_key_set(prefix, Some(1))?;

    // node from sap0 will process `HandleNewEldersAgreement` to update its knowledge about sap1
    let mut node = env.get_nodes(prefix, 1, 0, Some(0))?.remove(0);
    let info = node.info();
    // Simulate DKG round finished successfully by adding
    // the new section key share to our cache
    node.section_keys_provider
        .insert(TestKeys::get_section_key_share(&sk_set1, 0));

    // Create `HandleNewEldersAgreement` cmd. This will demote one of the
    // current elders and promote the oldest node.
    let elders_1: BTreeSet<_> = sap1.elders_set();
    let bytes = bincode::serialize(&sap1.sig.public_key).expect("Failed to serialize");
    let sig = TestKeys::get_section_sig_bytes(&sk_set0.secret_key(), &bytes);

    let mut cmds = ProcessAndInspectCmds::new(Cmd::HandleNewEldersAgreement {
        new_elders: sap1,
        sig,
    });

    let mut update_actual_recipients = HashSet::new();
    while let Some(cmd) = cmds.next(&mut node).await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                msg,
                recipients: Recipients::Multiple(recipients),
                ..
            } => (msg, recipients),
            _ => continue,
        };

        let section_tree_update = match msg {
            NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
                kind: AntiEntropyKind::Update { .. },
                section_tree_update,
            }) => section_tree_update.clone(),
            _ => continue,
        };

        assert_eq!(
            section_tree_update.proof_chain.last_key()?,
            sk_set1.public_keys().public_key()
        );
        // Merging the section contained in the message with the original section succeeds.
        assert!(section0
            .clone()
            .update_sap_knowledge_if_valid(section_tree_update, &info.name())
            .is_ok());

        update_actual_recipients.extend(recipients);
    }

    let update_expected_recipients: HashSet<_> = env
        .get_node_ids(prefix, elder_count(), 1, Some(0))?
        .into_iter()
        .filter(|node_id| *node_id != info.id())
        .chain(iter::once(promoted_node.0.id()))
        .chain(iter::once(demoted_node.0.id()))
        .collect();

    assert_eq!(update_actual_recipients, update_expected_recipients);

    assert_lists(node.network_knowledge().elders(), elders_1);

    Ok(())
}

/// Test that demoted node still sends `Sync` messages on split.
#[tokio::test]
async fn handle_demote_during_split() -> Result<()> {
    init_logger();
    let _span = tracing::info_span!("handle_demote_during_split").entered();
    let prefix0 = prefix("0");
    let prefix1 = prefix("1");

    // `nodes_a` + `info` are pre-split elders.
    // `nodes_a` + `node_c` are prefix-0 post-split elders.
    let (mut nodes_a, ..) =
        gen_node_infos_with_comm(&prefix0, elder_count(), 0, Some(&[MIN_ADULT_AGE]), None);

    let info = nodes_a
        .pop()
        .unwrap_or_else(|| panic!("No nodes generated!"));
    let node_name = info.0.name();

    // `nodes_b` are prefix-1 post-split elders.
    let (nodes_b, ..) =
        gen_node_infos_with_comm(&prefix1, elder_count(), 0, Some(&[MIN_ADULT_AGE]), None);
    // `node_c` is a prefix-0 post-split elder.
    let node_c = {
        let (node_c, comm, _) = gen_info_with_comm(MIN_ADULT_AGE, Some(prefix0));
        (node_c, comm)
    };
    // all members
    let members = nodes_a
        .iter()
        .chain(nodes_b.iter())
        .cloned()
        .chain([info.clone(), node_c.clone()]);

    let env = TestNetworkBuilder::new(thread_rng())
        // pre-split section
        .sap_with_members(
            Prefix::default(),
            nodes_a.iter().cloned().chain(iter::once(info.clone())),
            members.clone(),
        )
        // post-split prefix-0
        .sap_with_members(
            prefix0,
            nodes_a.iter().cloned().chain(iter::once(node_c.clone())),
            nodes_a
                .iter()
                .cloned()
                .chain([info.clone(), node_c.clone()]),
        )
        // post-split prefix-1
        .sap_with_members(prefix1, nodes_b.clone(), nodes_b.clone())
        .build()?;

    let sk_set_gen = env.get_secret_key_set(Prefix::default(), None)?;
    let sap0 = env.get_sap(prefix0, None)?.value;
    let sk_set0 = env.get_secret_key_set(prefix0, None)?;
    let sap1 = env.get_sap(prefix1, None)?.value;
    let sk_set1 = env.get_secret_key_set(prefix1, None)?;

    // get the `info` node from pre-split section
    let mut node = env.get_node_by_key(Prefix::default(), info.0.public_key(), None)?;

    // Simulate DKG round finished successfully by adding the new section
    // key share to our cache (according to which split section we'll belong to).
    if prefix0.matches(&node_name) {
        node.section_keys_provider
            .insert(TestKeys::get_section_key_share(&sk_set0, 0));
    } else {
        node.section_keys_provider
            .insert(TestKeys::get_section_key_share(&sk_set1, 0));
    }

    let cmd = {
        // Sign the saps.
        let sap0 = TestKeys::get_section_signed(&sk_set0.secret_key(), sap0)?;
        let sap1 = TestKeys::get_section_signed(&sk_set1.secret_key(), sap1)?;

        let bytes0 = bincode::serialize(&sap0.sig.public_key).expect("Failed to serialize");
        let bytes1 = bincode::serialize(&sap1.sig.public_key).expect("Failed to serialize");

        Cmd::HandleNewSectionsAgreement {
            sap1: sap0,
            sig1: TestKeys::get_section_sig_bytes(&sk_set_gen.secret_key(), &bytes0),
            sap2: sap1,
            sig2: TestKeys::get_section_sig_bytes(&sk_set_gen.secret_key(), &bytes1),
        }
    };

    let mut cmds = ProcessAndInspectCmds::new(cmd);

    let mut update_recipients = BTreeSet::new();
    while let Some(cmd) = cmds.next(&mut node).await? {
        let (msg, recipients) = match cmd {
            Cmd::SendMsg {
                msg, recipients, ..
            } => (msg, recipients.clone()),
            _ => continue,
        };

        if let NetworkMsg::AntiEntropy(AntiEntropyMsg::AntiEntropy {
            kind: AntiEntropyKind::Update { .. },
            ..
        }) = msg
        {
            update_recipients.extend(recipients.into_iter().map(|r| r.name()))
        }
    }

    // our node's whole section
    assert_eq!(update_recipients.len(), elder_count());
    Ok(())
}

#[tokio::test]
async fn spentbook_spend_client_message_should_replicate_to_adults_and_send_ack() -> Result<()> {
    init_logger();
    let prefix = Prefix::default();
    let replication_count = 5;
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let mut env = TestNetworkBuilder::new(thread_rng())
        .sap(
            TestSapBuilder::new(prefix)
                .adult_count(6)
                .sk_threshold_size(0),
        )
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let sk_set = env.get_secret_key_set(prefix, None)?;

    let (public_key, tx, spent_proofs, spent_transactions) =
        dbc_utils::get_genesis_dbc_spend_info(&sk_set)?;

    let comm_rx = env.take_comm_rx(node.info().public_key());
    let mut cmds = ProcessAndInspectCmds::new_from_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            public_key,
            tx: tx.clone(),
            reason: DbcReason::none(),
            spent_proofs,
            spent_transactions,
            network_knowledge: None,
        })),
        &mut node,
        comm_rx,
    )
    .await?;

    while let Some(cmd) = cmds.next(&mut node).await? {
        if let Cmd::SendAndForwardResponseToClient {
            wire_msg, targets, ..
        } = cmd
        {
            let msg = wire_msg.into_msg()?;
            match msg {
                NetworkMsg::Node(msg) => match msg {
                    NodeMsg::NodeDataCmd(NodeDataCmd::StoreData(data)) => {
                        assert_eq!(targets.len(), replication_count);
                        let spent_proof_share =
                            dbc_utils::get_spent_proof_share_from_replicated_data(data)?;
                        assert_eq!(public_key.to_hex(), spent_proof_share.public_key().to_hex());
                        assert_eq!(Hash::from(tx.hash()), spent_proof_share.transaction_hash());
                        assert_eq!(
                            sk_set.public_keys().public_key().to_hex(),
                            spent_proof_share.spentbook_pks().public_key().to_hex()
                        );
                    }
                    _ => {
                        bail!("Unexpected msg type when processing cmd")
                    }
                },
                _ => {
                    bail!("Unexpected Cmd type when processing cmd")
                }
            }
            return Ok(());
        }
    }

    bail!("No cmd msg was generate to replicate the data to node holders");
}

#[tokio::test]
async fn spentbook_spend_transaction_with_no_inputs_should_return_spentbook_error() -> Result<()> {
    init_logger();
    let prefix = prefix("1");
    let replication_count = 5;
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let mut env = TestNetworkBuilder::new(thread_rng())
        .sap(
            TestSapBuilder::new(prefix)
                .adult_count(6)
                .sk_threshold_size(0),
        )
        .build()?;
    let mut node = env.get_nodes(prefix, 1, 0, None)?.remove(0);
    let section = env.get_network_knowledge(prefix, None)?;
    let sk_set = env.get_secret_key_set(prefix, None)?;

    // These conditions will produce a failure on `tx.verify` in the message handler.
    let sap = section.section_auth();
    let keys_provider = node.section_keys_provider.clone();
    let genesis_dbc = gen_genesis_dbc(&sk_set, &sk_set.secret_key())?;
    let new_dbc = reissue_dbc(
        &genesis_dbc,
        10,
        &bls::SecretKey::random(),
        &sap,
        &keys_provider,
    )?;
    let new_dbc2_sk = bls::SecretKey::random();
    let new_dbc2 = dbc_utils::reissue_invalid_dbc_with_no_inputs(&new_dbc, 5, &new_dbc2_sk)?;

    let comm_rx = env.take_comm_rx(node.info().public_key());
    let mut cmds = ProcessAndInspectCmds::new_from_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            public_key: new_dbc2_sk.public_key(),
            tx: new_dbc2.transaction,
            reason: DbcReason::none(),
            spent_proofs: new_dbc.inputs_spent_proofs.clone(),
            spent_transactions: new_dbc.inputs_spent_transactions,
            network_knowledge: None,
        })),
        &mut node,
        comm_rx,
    )
    .await?;

    while let Some(cmd) = cmds.next(&mut node).await? {
        if let Cmd::SendDataResponse {
            msg:
                DataResponse::CmdResponse {
                    response: CmdResponse::SpendKey(Err(error)),
                    ..
                },
            ..
        } = cmd
        {
            assert_eq!(
                error,
                &MessagingDataError::from(Error::SpentbookError(
                    "The DBC transaction must have at least one input".to_string()
                )),
                "A different error was expected for this case: {error:?}"
            );
            return Ok(());
        }
    }

    bail!("We expected an error to be returned");
}

/// This could potentially be the start of a case for the updated proof chain and SAP being sent
/// with the spend request, but I don't know exactly what the conditions are for getting the
/// network knowledge to update correctly.
#[tokio::test]
async fn spentbook_spend_with_updated_network_knowledge_should_update_the_node() -> Result<()> {
    init_logger();
    let replication_count = 5;
    let prefix0 = prefix("0");
    let prefix1 = prefix("1");
    std::env::set_var("SN_DATA_COPY_COUNT", replication_count.to_string());

    let mut env = TestNetworkBuilder::new(thread_rng())
        .sap(TestSapBuilder::new(Prefix::default()).sk_threshold_size(0))
        .sap(TestSapBuilder::new(prefix0).sk_threshold_size(0))
        .sap(TestSapBuilder::new(prefix1).sk_threshold_size(0))
        .build()?;

    let mut node = env.get_nodes(Prefix::default(), 1, 0, None)?.remove(0);
    let info = node.info();
    let genesis_sk_set = env.get_secret_key_set(Prefix::default(), None)?;

    let other_node = env.get_nodes(prefix1, 1, 0, None)?.remove(0);
    let other_node_info = other_node.info();
    let other_section_key_share =
        env.get_section_key_share(prefix1, other_node_info.public_key(), None)?;
    let other_section = env.get_network_knowledge(prefix1, None)?;
    let other_section_key = env.get_secret_key_set(prefix1, None)?;

    // At this point, only the genesis key should be in the proof chain on this node.
    let tree = node.network_knowledge().section_tree().clone();
    let proof_chain = tree.get_sections_dag().clone();
    assert_eq!(proof_chain.keys().count(), 1);

    // The key share also needs to be added to the section keys provider, which is stored
    // on the node.
    node.section_keys_provider
        .insert(other_section_key_share.clone());

    // Reissue a couple of DBC from genesis. They will be reissued using the section keys
    // provider and SAP from the other section, hence the spent proofs will be signed with
    // the unknown section key.
    // The owners of the DBCs here don't really matter, so we just use random keys.
    let skp = SectionKeysProvider::new(Some(other_section_key_share.clone()));
    let sap = other_section.signed_sap();
    let genesis_dbc = gen_genesis_dbc(&genesis_sk_set, &genesis_sk_set.secret_key())?;
    let new_dbc = reissue_dbc(&genesis_dbc, 10, &bls::SecretKey::random(), &sap, &skp)?;
    let new_dbc2 = reissue_dbc(&new_dbc, 5, &bls::SecretKey::random(), &sap, &skp)?;
    let new_dbc2_spent_proof = new_dbc2
        .inputs_spent_proofs
        .iter()
        .next()
        .ok_or_else(|| eyre!("This DBC should have been reissued with a spent proof"))?;
    assert_eq!(
        new_dbc2_spent_proof.spentbook_pub_key,
        other_section_key.secret_key().public_key()
    );

    // Finally, spend new_dbc2 as part of the input for another reissue.
    // It needs to be associated with a valid transaction, which is why the util function
    // is used. Again, the owner of the output DBCs don't really matter, so a random key is
    // used.
    let proof_chain = other_section.section_chain();
    let (public_key, tx) = get_input_dbc_spend_info(&new_dbc2, 2, &bls::SecretKey::random())?;

    let comm_rx = env.take_comm_rx(info.public_key());

    let mut cmds = ProcessAndInspectCmds::new_from_client_msg(
        ClientMsg::Cmd(DataCmd::Spentbook(SpentbookCmd::Spend {
            public_key,
            tx,
            reason: DbcReason::none(),
            spent_proofs: new_dbc2.inputs_spent_proofs,
            spent_transactions: new_dbc2.inputs_spent_transactions,
            network_knowledge: Some((proof_chain, sap)),
        })),
        &mut node,
        comm_rx,
    )
    .await?;

    // // The commands returned here should include the new command to update the network
    // // knowledge and also the other two commands to replicate the spent proof shares and
    // // the ack command, but we've already validated the other two as part of another test.
    let mut found = false;
    while let Some(cmd) = cmds.next(&mut node).await? {
        if let Cmd::UpdateNetworkAndHandleValidClientMsg { .. } = cmd {
            found = true;
        }
    }
    assert!(found, "UpdateNetworkAndHandleValidClientMsg was not generated to update the node's network knowledge");

    // Now the proof chain should have the other section key.
    let tree = node.network_knowledge().section_tree().clone();
    let proof_chain = tree.get_sections_dag().clone();
    assert_eq!(proof_chain.keys().count(), 2);
    let mut proof_chain_iter = proof_chain.keys();
    let genesis_key = genesis_sk_set.public_keys().public_key();
    assert_eq!(
        genesis_key,
        proof_chain_iter
            .next()
            .ok_or_else(|| eyre!("The proof chain should include the genesis key"))?
    );
    assert_eq!(
        other_section_key.secret_key().public_key(),
        proof_chain_iter
            .next()
            .ok_or_else(|| eyre!("The proof chain should include the other section key"))?
    );

    Ok(())
}

fn get_single_sig(proposal: &NodeState) -> Vec<u8> {
    bincode::serialize(proposal).expect("Failed to serialize")
}

fn ae_msg(name: XorName, dst: Dst, msg: AntiEntropyMsg) -> Result<WireMsg> {
    use sn_interface::messaging::{MsgId, MsgKind};
    Ok(WireMsg::new_msg(
        MsgId::new(),
        WireMsg::serialize_msg_payload(&msg)?,
        MsgKind::AntiEntropy(name),
        dst,
    ))
}

fn single_src_node(name: XorName, dst: Dst, msg: NodeMsg) -> Result<WireMsg> {
    use sn_interface::messaging::{MsgId, MsgKind};
    let msg_payload = WireMsg::serialize_msg_payload(&msg)?;

    let wire_msg = WireMsg::new_msg(
        MsgId::new(),
        msg_payload,
        MsgKind::Node {
            name,
            is_join: msg.is_join(),
        },
        dst,
    );

    Ok(wire_msg)
}
