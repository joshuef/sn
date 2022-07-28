// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::UsedRecipientSaps;

use crate::node::{
    flow_ctrl::cmds::Cmd,
    messaging::{OutgoingMsg, Peers},
    Error, Result,
};
// use crate::node::{messages::WireMsgUtils,};

use sn_interface::{
    messaging::system::{
        JoinAsRelocatedRequest, JoinAsRelocatedResponse, NodeState, SectionAuth, SystemMsg,
    },
    network_knowledge::{NodeInfo, SectionAuthorityProvider},
    types::{keys::ed25519, Peer, PublicKey},
};

use bls::PublicKey as BlsPublicKey;
use ed25519_dalek::{Keypair, Signature};
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};
use xor_name::{Prefix, XorName};

/// Re-join as a relocated node.
pub(crate) struct JoiningAsRelocated {
    pub(crate) node: NodeInfo,
    relocate_proof: SectionAuth<NodeState>,
    // Avoid sending more than one duplicated request (with same SectionKey) to the same peer.
    used_recipient_saps: UsedRecipientSaps,
    dst_xorname: XorName,
    dst_section_key: BlsPublicKey,
    new_age: u8,
    old_keypair: Arc<Keypair>,
}

impl JoiningAsRelocated {
    // Generates the first cmd to send a `JoinAsRelocatedRequest`, responses
    // shall be fed back with `handle_join_response` function.
    pub(crate) fn start(
        node: NodeInfo,
        relocate_proof: SectionAuth<NodeState>,
        bootstrap_addrs: Vec<SocketAddr>,
        dst_xorname: XorName,
        dst_section_key: BlsPublicKey,
        new_age: u8,
    ) -> Result<(Self, Cmd)> {
        let recipients: BTreeSet<_> = bootstrap_addrs
            .iter()
            .map(|addr| Peer::new(dst_xorname, *addr))
            .collect();

        let used_recipient_saps = bootstrap_addrs
            .iter()
            .map(|addr| (*addr, dst_section_key))
            .collect();

        // We send a first join request to obtain the section prefix, which
        // we will then used calculate our new name and send the `JoinAsRelocatedRequest` again.
        // This time we just send a dummy signature for the name.
        // TODO: include the section Prefix in the RelocationDetails so we save one request.
        let old_keypair = node.keypair.clone();
        let dummy_signature = ed25519::sign(&node.name().0, &old_keypair);

        let relocating = Self {
            node,
            relocate_proof,
            used_recipient_saps,
            dst_xorname,
            dst_section_key,
            new_age,
            old_keypair,
        };
        let cmd = relocating.build_join_request_cmd(recipients, dummy_signature)?;

        Ok((relocating, cmd))
    }

    // Handles a `JoinAsRelocatedResponse`, if it's a:
    // - `Retry`: repeat join request with the new info, which shall include the relocation payload.
    // - `Redirect`: repeat join request with the new set of addresses.
    // - `Approval`: returns the `Section` to use by this node, completing the relocation.
    // - `NodeNotReachable`: returns an error, completing the relocation attempt.
    pub(crate) fn handle_join_response(
        &mut self,
        join_response: JoinAsRelocatedResponse,
        sender: SocketAddr,
    ) -> Result<Option<Cmd>> {
        trace!("Hanlde JoinResponse {:?}", join_response);
        match join_response {
            JoinAsRelocatedResponse::Retry(section_auth) => {
                let section_auth = section_auth.into_state();
                if !self.check_autority_provider(&section_auth, &self.dst_xorname) {
                    trace!("failed to check authority");
                    return Ok(None);
                }

                if section_auth.section_key() == self.dst_section_key {
                    trace!("equal destination section key");
                    return Ok(None);
                }

                let new_section_key = section_auth.section_key();
                let new_recipients: BTreeSet<_> = section_auth
                    .elders()
                    .filter(|peer| {
                        self.used_recipient_saps
                            .insert((peer.addr(), new_section_key))
                    })
                    .cloned()
                    .collect();

                if new_recipients.is_empty() {
                    debug!(
                        "Ignore JoinAsRelocatedResponse::Retry with old SAP that has been sent to: {:?}",
                        section_auth
                    );
                    return Ok(None);
                }

                info!(
                    "Newer Join response for our prefix {:?} from {:?}",
                    section_auth, sender
                );
                self.dst_section_key = section_auth.section_key();

                let new_name_sig = self.build_relocation_name(&section_auth.prefix());
                let cmd = self.build_join_request_cmd(new_recipients, new_name_sig)?;

                Ok(Some(cmd))
            }
            JoinAsRelocatedResponse::Redirect(section_auth) => {
                let section_auth = section_auth.into_state();

                if !self.check_autority_provider(&section_auth, &self.dst_xorname) {
                    return Ok(None);
                }

                if section_auth.section_key() == self.dst_section_key {
                    return Ok(None);
                }

                let new_section_key = section_auth.section_key();
                let new_recipients: BTreeSet<_> = section_auth
                    .elders()
                    .filter(|peer| {
                        self.used_recipient_saps
                            .insert((peer.addr(), new_section_key))
                    })
                    .cloned()
                    .collect();

                if new_recipients.is_empty() {
                    debug!(
                        "Ignore JoinAsRelocatedResponse::Redirect with old SAP that has been sent to: {:?}",
                        section_auth
                    );
                    return Ok(None);
                }

                info!(
                    "Newer Join response for our prefix {:?} from {:?}",
                    section_auth, sender
                );
                self.dst_section_key = section_auth.section_key();

                let new_name_sig = self.build_relocation_name(&section_auth.prefix());
                let cmd = self.build_join_request_cmd(new_recipients, new_name_sig)?;

                Ok(Some(cmd))
            }
            JoinAsRelocatedResponse::NodeNotReachable(addr) => {
                error!(
                    "Node cannot join as relocated since it is not externally reachable: {}",
                    addr
                );
                Err(Error::NodeNotReachable(addr))
            }
        }
    }

    // Change our name to fit the destination section and apply the new age.
    fn build_relocation_name(&mut self, prefix: &Prefix) -> Signature {
        // We are relocating so we need to change our name.
        // Use a name that will match the destination even after multiple splits
        let extra_split_count = 3;
        let name_prefix = Prefix::new(prefix.bit_count() + extra_split_count, self.dst_xorname);

        let new_keypair = ed25519::gen_keypair(&name_prefix.range_inclusive(), self.new_age);
        let new_name = XorName::from(PublicKey::from(new_keypair.public));

        // Sign new_name with our old keypair
        let signature_over_new_name = ed25519::sign(&new_name.0, &self.old_keypair);

        info!("Changing name to {}", new_name);
        self.node = NodeInfo::new(new_keypair, self.node.addr);

        signature_over_new_name
    }

    fn build_join_request_cmd(
        &self,
        recipients: BTreeSet<Peer>,
        new_name_sig: Signature,
    ) -> Result<Cmd> {
        let join_request = JoinAsRelocatedRequest {
            section_key: self.dst_section_key,
            relocate_proof: self.relocate_proof.clone(),
            signature_over_new_name: new_name_sig,
        };

        info!("Sending {:?} to {:?}", join_request, recipients);

        let msg = SystemMsg::JoinAsRelocatedRequest(Box::new(join_request));
        let cmd = Cmd::send_msg(OutgoingMsg::System(msg), Peers::Multiple(recipients));

        Ok(cmd)
    }

    fn check_autority_provider(
        &self,
        section_auth: &SectionAuthorityProvider,
        dst: &XorName,
    ) -> bool {
        if !section_auth.prefix().matches(dst) {
            error!("Invalid JoinResponse bad prefix: {:?}", section_auth);
            false
        } else if section_auth.elder_count() == 0 {
            error!(
                "Invalid JoinResponse, empty list of Elders: {:?}",
                section_auth
            );
            false
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JoiningAsRelocated;
    use crate::node::{
        messaging::{OutgoingMsg, Peers},
        Cmd,
    };
    use color_eyre::Result;
    use eyre::eyre;
    use sn_interface::{
        elder_count,
        messaging::{
            system::{JoinAsRelocatedResponse, NodeState, SystemMsg},
            SectionAuthorityProvider,
        },
        network_knowledge::{
            test_utils::{gen_addr, section_signed},
            NodeInfo, MIN_ADULT_AGE,
        },
        types::keys::ed25519,
    };
    use std::{collections::BTreeMap, net::SocketAddr};
    use tokio;
    use xor_name::{Prefix, XorName};

    #[tokio::test]
    async fn should_send_join_as_relocated_request_system_msg() -> Result<()> {
        // from_sap
        let (mut from_sap, from_sk_set) = generate_sap();
        let node = NodeInfo::new(
            ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE + 1),
            gen_addr(),
        );
        let node_state = NodeState::joined(node.name(), node.addr, None);
        let _ = from_sap.members.insert(node.name(), node_state.clone());
        let signed_node_state = section_signed(&from_sk_set.secret_key(), node_state)?;
        // to_sap
        let (to_sap, _) = generate_sap();

        let bootstrap: Vec<SocketAddr> = to_sap.elders.iter().map(|(_, addr)| *addr).collect();
        let (_, cmd) = JoiningAsRelocated::start(
            node.clone(),
            signed_node_state,
            bootstrap.clone(),
            node.name(),
            bls::SecretKey::random().public_key(),
            node.age() + 1,
        )?;

        match cmd {
            Cmd::SendMsg {
                msg, recipients, ..
            } => {
                match recipients {
                    Peers::Single(_) => assert_eq!(bootstrap.len(), 1),
                    Peers::Multiple(recipients) => assert_eq!(bootstrap.len(), recipients.len()),
                }
                if !matches!(
                    msg,
                    OutgoingMsg::System(SystemMsg::JoinAsRelocatedRequest(_))
                ) {
                    return Err(eyre!("Should be JoinAsRelocatedRequest"));
                }
            }
            _ => return Err(eyre!("Should be SendMsg")),
        };
        Ok(())
    }

    #[tokio::test]
    async fn retry_and_redirect_should_update_the_node_with_latest_sap_of_the_destination_section(
    ) -> Result<()> {
        // destination section sends its updated SAP
        relocate(Response::Retry)?;
        // incorrect section sends the SAP of the correct section
        relocate(Response::Redirect)?;

        fn relocate(response: Response) -> Result<()> {
            // from_sap
            let (mut from_sap, from_sk_set) = generate_sap();
            let node = NodeInfo::new(
                ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE + 1),
                gen_addr(),
            );
            let node_state = NodeState::joined(node.name(), node.addr, None);
            let _ = from_sap.members.insert(node.name(), node_state.clone());
            let signed_node_state = section_signed(&from_sk_set.secret_key(), node_state)?;
            // to_sap
            let (to_sap, to_sk_set) = generate_sap();

            let bootstrap: Vec<SocketAddr> = to_sap.elders.iter().map(|(_, addr)| *addr).collect();
            let (mut joining, _) = JoiningAsRelocated::start(
                node.clone(),
                signed_node_state,
                bootstrap,
                // initially with our own xorname
                node.name(),
                // initially we have outdated destination section key
                bls::SecretKey::random().public_key(),
                node.age() + 1,
            )?;

            // destination section sends Retry/Redirect response with its latest SAP
            let response = match response {
                Response::Retry => JoinAsRelocatedResponse::Retry(to_sap.clone()),
                Response::Redirect => JoinAsRelocatedResponse::Redirect(to_sap.clone()),
            };
            let (_, sender) = to_sap
                .elders
                .iter()
                .next()
                .ok_or_else(|| eyre!("Elder will be present"))?;
            let handled = joining.handle_join_response(response.clone(), *sender);

            // updated with section's latest key
            assert_eq!(joining.dst_section_key, to_sk_set.secret_key().public_key());
            // new name is set
            assert_ne!(joining.node.name(), node.name());
            if let Ok(Some(Cmd::SendMsg { recipients, .. })) = handled {
                match recipients {
                    Peers::Single(_) => assert_eq!(elder_count(), 1),
                    Peers::Multiple(recipients) => assert_eq!(elder_count(), recipients.len()),
                }
            }

            // The to_sap's elder list is exhausted after the first handle_join_response
            // Thus it should return None when new_recipients is empty and with outdated section key
            joining.dst_section_key = bls::SecretKey::random().public_key();
            let handled = joining.handle_join_response(response, *sender);
            if let Ok(Some(_)) = handled {
                return Err(eyre!("Should return Ok(None)"));
            }

            Ok(())
        }
        Ok(())
    }

    #[tokio::test]
    async fn retry_and_redirect_should_return_none_on_latest_sap() -> Result<()> {
        relocate(Response::Retry)?;
        relocate(Response::Redirect)?;

        fn relocate(response: Response) -> Result<()> {
            // from_sap
            let (mut from_sap, from_sk_set) = generate_sap();
            let node = NodeInfo::new(
                ed25519::gen_keypair(&Prefix::default().range_inclusive(), MIN_ADULT_AGE + 1),
                gen_addr(),
            );
            let node_state = NodeState::joined(node.name(), node.addr, None);
            let _ = from_sap.members.insert(node.name(), node_state.clone());
            let signed_node_state = section_signed(&from_sk_set.secret_key(), node_state)?;
            // to_sap
            let (to_sap, to_sk_set) = generate_sap();

            let bootstrap: Vec<SocketAddr> = to_sap.elders.iter().map(|(_, addr)| *addr).collect();
            let (mut joining, _) = JoiningAsRelocated::start(
                node.clone(),
                signed_node_state,
                bootstrap,
                node.name(),
                // latest section key of the destination
                to_sk_set.secret_key().public_key(),
                node.age() + 1,
            )?;

            // destination section sends Retry/Redirect response with its latest SAP
            let response = match response {
                Response::Retry => JoinAsRelocatedResponse::Retry(to_sap.clone()),
                Response::Redirect => JoinAsRelocatedResponse::Redirect(to_sap.clone()),
            };
            let (_, sender) = to_sap
                .elders
                .iter()
                .next()
                .ok_or_else(|| eyre!("Elder will be present"))?;
            let handled = joining.handle_join_response(response, *sender);

            if let Ok(Some(_)) = handled {
                return Err(eyre!("Should return Ok(None)"));
            }
            Ok(())
        }
        Ok(())
    }

    enum Response {
        Retry,
        Redirect,
    }

    fn generate_sap() -> (SectionAuthorityProvider, bls::SecretKeySet) {
        let sk_set = bls::SecretKeySet::random(0, &mut rand::thread_rng());

        let mut elders: BTreeMap<XorName, SocketAddr> = BTreeMap::new();
        let mut members: BTreeMap<XorName, NodeState> = BTreeMap::new();

        for _ in 0..elder_count() {
            let xor_name = xor_name::rand::random();
            let socket_addr = gen_addr();
            let _ = elders.insert(xor_name, socket_addr);
            let _ = members.insert(xor_name, NodeState::joined(xor_name, socket_addr, None));
        }
        let sap = SectionAuthorityProvider {
            prefix: Prefix::default(),
            public_key_set: sk_set.public_keys(),
            elders,
            members,
            membership_gen: 0,
        };
        (sap, sk_set)
    }
}
