// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Error, Result};

use sn_interface::{
    elder_count,
    messaging::DstLocation,
    network_knowledge::{supermajority, NetworkKnowledge},
    types::Peer,
};

use itertools::Itertools;
use std::{cmp, iter};
use xor_name::XorName;

/// Returns a set of nodes and their section PublicKey to which a message for the given
/// `DstLocation` could be sent onwards, sorted by priority, along with the number of targets the
/// message should be sent to. If the total number of targets returned is larger than this number,
/// the spare targets can be used if the message can't be delivered to some of the initial ones.
///
/// * If the destination is a `DstLocation::Section` OR `DstLocation::EndUser`:
///     - if our section is the closest on the network (i.e. our section's prefix is a prefix of
///       the dst), returns all other members of our section; otherwise
///     - returns the `N/3` closest members to the target
///
/// * If the destination is an individual node:
///     - if our name *is* the dst, returns an empty set; otherwise
///     - if the destination name is an entry in the routing table, returns it; otherwise
///     - returns the `N/3` closest members of the RT to the target
pub(crate) async fn delivery_targets(
    dst: &DstLocation,
    our_name: &XorName,
    network_knowledge: &NetworkKnowledge,
) -> Result<(Vec<Peer>, usize)> {
    // Adult now having the knowledge of other adults within the own section.
    // Functions of `section_candidates` and `candidates` only take section elder into account.

    match dst {
        DstLocation::Section { name, .. } => {
            section_candidates(name, our_name, network_knowledge).await
        }
        DstLocation::EndUser(user) => {
            section_candidates(&user.0, our_name, network_knowledge).await
        }
        DstLocation::Node { name, .. } => {
            if name == our_name {
                return Ok((Vec::new(), 0));
            }
            if let Some(node) = get_peer(name, network_knowledge).await {
                return Ok((vec![node], 1));
            }

            if !network_knowledge.is_elder(our_name) {
                // We are not Elder - return all the elders of our section,
                // so the message can be properly relayed through them.
                let targets: Vec<_> = network_knowledge.authority_provider().elders_vec();
                let dg_size = targets.len();
                Ok((targets, dg_size))
            } else {
                candidates(name, our_name, network_knowledge).await
            }
        }
    }
}

async fn section_candidates(
    target_name: &XorName,
    our_name: &XorName,
    network_knowledge: &NetworkKnowledge,
) -> Result<(Vec<Peer>, usize)> {
    let default_sap = network_knowledge.authority_provider();
    // Find closest section to `target_name` out of the ones we know (including our own)
    let network_sections = network_knowledge.prefix_map().all();
    let info = iter::once(default_sap.clone())
        .chain(network_sections)
        .min_by(|lhs, rhs| lhs.prefix().cmp_distance(&rhs.prefix(), target_name))
        .unwrap_or(default_sap);

    if info.prefix() == network_knowledge.prefix() {
        // Exclude our name since we don't need to send to ourself
        let chosen_section: Vec<_> = info
            .elders()
            .filter(|node| node.name() != *our_name)
            .cloned()
            .collect();
        let dg_size = chosen_section.len();
        return Ok((chosen_section, dg_size));
    }

    candidates(target_name, our_name, network_knowledge).await
}

// Obtain the delivery group candidates for this target
async fn candidates(
    target_name: &XorName,
    our_name: &XorName,
    network_knowledge: &NetworkKnowledge,
) -> Result<(Vec<Peer>, usize)> {
    // All sections we know (including our own), sorted by distance to `target_name`.
    let sections = network_knowledge.prefix_map().all();

    let sections = sections
        .iter()
        .sorted_by(|lhs, rhs| lhs.prefix().cmp_distance(&rhs.prefix(), target_name))
        .map(|info| (info.prefix(), info.elder_count(), info.elders_vec()))
        .collect_vec();

    // let sections = iter::once(&sap)
    // sections.chain(network_sections.iter())
    // .sorted_by(|lhs, rhs| lhs.prefix.cmp_distance(&rhs.prefix, target_name))
    // .map(|info| (&info.prefix, info.elder_count(), info.peers()))
    // .collect_vec();

    // gives at least 1 honest target among recipients.
    let min_dg_size = 1 + elder_count() - supermajority(elder_count());
    let mut dg_size = min_dg_size;
    let mut candidates = Vec::new();
    for (idx, (prefix, len, connected)) in sections.iter().enumerate() {
        candidates.extend(connected.clone());
        if prefix.matches(target_name) {
            // If we are last hop before final dst, send to all candidates.
            dg_size = *len;
        } else {
            // If we don't have enough contacts send to as many as possible
            // up to dg_size of Elders
            dg_size = cmp::min(*len, dg_size);
        }
        if len < &min_dg_size {
            warn!(
                "Delivery group only {:?} when it should be {:?}",
                len, min_dg_size
            )
        }

        if *prefix == network_knowledge.prefix() {
            // Send to all connected targets so they can forward the message
            candidates.retain(|node| node.name() != *our_name);
            dg_size = candidates.len();
            break;
        }
        if idx == 0 && candidates.len() >= dg_size {
            // can deliver to enough of the closest section
            break;
        }
    }
    candidates.sort_by(|lhs, rhs| target_name.cmp_distance(&lhs.name(), &rhs.name()));

    if dg_size > 0 && candidates.len() >= dg_size {
        Ok((candidates, dg_size))
    } else {
        Err(Error::CannotRoute(dg_size, candidates.len()))
    }
}

// Returns a `Peer` for a known node.
async fn get_peer(name: &XorName, network_knowledge: &NetworkKnowledge) -> Option<Peer> {
    match network_knowledge.get_section_member(name) {
        Some(info) => Some(*info.peer()),
        None => network_knowledge
            .section_by_name(name)
            .ok()?
            .get_elder(name)
            .cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sn_interface::{
        network_knowledge::{
            test_utils::section_signed,
            test_utils::{gen_addr, gen_section_authority_provider},
            NodeState, SectionAuthorityProvider, MIN_ADULT_AGE,
        },
        types::keys::ed25519,
    };

    use eyre::{ContextCompat, Result};
    use rand::seq::IteratorRandom;
    use secured_linked_list::SecuredLinkedList;
    use xor_name::Prefix;

    #[tokio::test]
    async fn delivery_targets_elder_to_our_elder() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let dst_name = *network_knowledge
            .authority_provider()
            .names()
            .iter()
            .filter(|&&name| name != our_name)
            .choose(&mut rand::thread_rng())
            .context("too few elders")?;

        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_our_adult() -> Result<()> {
        let (our_name, network_knowledge, sk) = setup_elder().await?;

        let name = ed25519::gen_name_with_age(MIN_ADULT_AGE);
        let dst_name = network_knowledge.prefix().substituted_in(name);
        let peer = Peer::new(dst_name, gen_addr());
        let node_state = NodeState::joined(peer, None);
        let node_state = section_signed(&sk, node_state)?;
        assert!(network_knowledge.update_member(node_state));

        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_our_section() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let dst_name = network_knowledge
            .prefix()
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all our elders except us.
        let expected_recipients: Vec<_> = network_knowledge
            .authority_provider()
            .elders()
            .filter(|elder| elder.name() != our_name)
            .cloned()
            .collect();

        assert_eq!(dg_size, expected_recipients.len());
        assert_eq!(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_known_remote_peer() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let section_auth1 = network_knowledge
            .prefix_map()
            .get(&Prefix::default().pushed(true))
            .context("unknown section")?;

        let dst_name = choose_elder_name(&section_auth1)?;
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send only to the dst node.
        assert_eq!(dg_size, 1);
        assert_eq!(recipients[0].name(), dst_name);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_to_final_hop_unknown_remote_peer() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let section_auth1 = network_knowledge
            .prefix_map()
            .get(&Prefix::default().pushed(true))
            .context("unknown section")?;

        let dst_name = section_auth1
            .prefix()
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders in the dst section
        let expected_recipients = section_auth1
            .elders()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(&lhs.name(), &rhs.name()));
        assert_eq!(dg_size, section_auth1.elder_count());
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Need to setup network so that we do not locate final dst, as to trigger correct outcome."]
    async fn delivery_targets_elder_to_intermediary_hop_unknown_remote_peer() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let elders_info1 = network_knowledge
            .prefix_map()
            .get(&Prefix::default().pushed(true))
            .context("unknown section")?;

        let dst_name = elders_info1
            .prefix()
            .pushed(false)
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders in the dst section
        let expected_recipients = elders_info1
            .elders()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(&lhs.name(), &rhs.name()));
        let min_dg_size =
            1 + elders_info1.elder_count() - supermajority(elders_info1.elder_count());
        assert_eq!(dg_size, min_dg_size);
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_elder_final_hop_to_remote_section() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let section_auth1 = network_knowledge
            .prefix_map()
            .get(&Prefix::default().pushed(true))
            .context("unknown section")?;

        let dst_name = section_auth1
            .prefix()
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders in the final dst section
        let expected_recipients = section_auth1
            .elders()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(&lhs.name(), &rhs.name()));
        assert_eq!(dg_size, section_auth1.elder_count());
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Need to setup network so that we do not locate final dst, as to trigger correct outcome."]
    async fn delivery_targets_elder_intermediary_hop_to_remote_section() -> Result<()> {
        let (our_name, network_knowledge, _) = setup_elder().await?;

        let elders_info1 = network_knowledge
            .prefix_map()
            .get(&Prefix::default().pushed(true))
            .context("unknown section")?;

        let dst_name = elders_info1
            .prefix()
            .pushed(false)
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to a subset of elders in the intermediary dst section
        let min_dg_size =
            1 + elders_info1.elder_count() - supermajority(elders_info1.elder_count());
        let expected_recipients = elders_info1
            .elders()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(&lhs.name(), &rhs.name()))
            .take(min_dg_size);

        assert_eq!(dg_size, min_dg_size);
        itertools::assert_equal(recipients, expected_recipients);

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_elder() -> Result<()> {
        let (our_name, network_knowledge) = setup_adult().await?;

        let dst_name = choose_elder_name(&network_knowledge.authority_provider())?;
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to chosen elder
        assert_eq!(dg_size, 1);
        assert_eq!(
            Some(&recipients[0]),
            network_knowledge.authority_provider().get_elder(&dst_name),
        );

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_adult() -> Result<()> {
        let (our_name, network_knowledge) = setup_adult().await?;

        let dst_name = network_knowledge
            .prefix()
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders
        assert_eq!(
            dg_size,
            network_knowledge.authority_provider().elder_count()
        );
        itertools::assert_equal(recipients, network_knowledge.authority_provider().elders());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_our_section() -> Result<()> {
        let (our_name, network_knowledge) = setup_adult().await?;

        let dst_name = network_knowledge
            .prefix()
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders
        assert_eq!(
            dg_size,
            network_knowledge.authority_provider().elder_count()
        );
        itertools::assert_equal(recipients, network_knowledge.authority_provider().elders());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_remote_peer() -> Result<()> {
        let (our_name, network_knowledge) = setup_adult().await?;

        let dst_name = Prefix::default()
            .pushed(true)
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Node {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders
        assert_eq!(
            dg_size,
            network_knowledge.authority_provider().elder_count()
        );
        itertools::assert_equal(recipients, network_knowledge.authority_provider().elders());

        Ok(())
    }

    #[tokio::test]
    async fn delivery_targets_adult_to_remote_section() -> Result<()> {
        let (our_name, network_knowledge) = setup_adult().await?;

        let dst_name = Prefix::default()
            .pushed(true)
            .substituted_in(xor_name::rand::random());
        let section_pk = network_knowledge.authority_provider().section_key();
        let dst = DstLocation::Section {
            name: dst_name,
            section_pk,
        };
        let (recipients, dg_size) = delivery_targets(&dst, &our_name, &network_knowledge).await?;

        // Send to all elders
        assert_eq!(
            dg_size,
            network_knowledge.authority_provider().elder_count()
        );
        itertools::assert_equal(recipients, network_knowledge.authority_provider().elders());

        Ok(())
    }

    async fn setup_elder() -> Result<(XorName, NetworkKnowledge, bls::SecretKey)> {
        let prefix0 = Prefix::default().pushed(false);
        let prefix1 = Prefix::default().pushed(true);

        let (section_auth0, _, secret_key_set) =
            gen_section_authority_provider(prefix0, elder_count());
        let genesis_sk = secret_key_set.secret_key();
        let genesis_pk = genesis_sk.public_key();

        let elders0 = section_auth0.elders_vec();
        let section_auth0 = section_signed(genesis_sk, section_auth0)?;

        let chain = SecuredLinkedList::new(genesis_pk);

        let network_knowledge = NetworkKnowledge::new(genesis_pk, chain, section_auth0, None)?;

        for peer in elders0 {
            let node_state = NodeState::joined(peer, None);
            let node_state = section_signed(genesis_sk, node_state)?;
            assert!(network_knowledge.update_member(node_state));
        }

        let (section_auth1, _, secret_key_set) =
            gen_section_authority_provider(prefix1, elder_count());
        let sk1 = secret_key_set.secret_key();
        let pk1 = sk1.public_key();

        let section_auth1 = section_signed(sk1, section_auth1)?;

        // create a section chain branched out from same genesis pk
        let mut proof_chain = SecuredLinkedList::new(genesis_pk);
        // second key is the PK derived from SAP's SK
        let sig1 = bincode::serialize(&pk1).map(|bytes| genesis_sk.sign(&bytes))?;
        proof_chain.insert(&genesis_pk, pk1, sig1)?;

        // 3rd key is the section key in SAP
        let pk2 = section_auth1.section_key();
        let sig2 = bincode::serialize(&pk2).map(|bytes| sk1.sign(&bytes))?;
        proof_chain.insert(&pk1, pk2, sig2)?;

        assert!(network_knowledge
            .prefix_map()
            .verify_with_chain_and_update(
                section_auth1,
                &proof_chain,
                &network_knowledge.section_chain()
            )
            .is_ok(),);

        let our_name = choose_elder_name(&network_knowledge.authority_provider())?;

        Ok((our_name, network_knowledge, genesis_sk.clone()))
    }

    async fn setup_adult() -> Result<(XorName, NetworkKnowledge)> {
        let prefix0 = Prefix::default().pushed(false);

        let (section_auth, _, secret_key_set) =
            gen_section_authority_provider(prefix0, elder_count());
        let genesis_sk = secret_key_set.secret_key();
        let genesis_pk = genesis_sk.public_key();
        let section_auth = section_signed(genesis_sk, section_auth)?;
        let chain = SecuredLinkedList::new(genesis_pk);
        let network_knowledge = NetworkKnowledge::new(genesis_pk, chain, section_auth, None)?;

        let our_name = network_knowledge
            .prefix()
            .substituted_in(xor_name::rand::random());

        Ok((our_name, network_knowledge))
    }

    fn choose_elder_name(section_auth: &SectionAuthorityProvider) -> Result<XorName> {
        section_auth
            .names()
            .into_iter()
            .choose(&mut rand::thread_rng())
            .context("no elders")
    }
}
