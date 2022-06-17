// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::UsedRecipientSaps;

use crate::node::{api::cmds::Cmd, messages::WireMsgUtils, Error, Result};

use sn_interface::{
    messaging::{
        system::{
            JoinAsRelocatedRequest, JoinAsRelocatedResponse, NodeState, SectionAuth, SystemMsg,
        },
        DstLocation, WireMsg,
    },
    network_knowledge::{NodeInfo, SectionAuthorityProvider},
    types::{keys::ed25519, Peer, PublicKey},
};

use bls::PublicKey as BlsPublicKey;
use ed25519_dalek::{Keypair, Signature};
use std::{net::SocketAddr, sync::Arc};
use xor_name::{Prefix, XorName};

/// Re-join as a relocated node.
pub(crate) struct JoiningAsRelocated {
    pub(crate) node: NodeInfo,
    genesis_key: BlsPublicKey,
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
        genesis_key: BlsPublicKey,
        relocate_proof: SectionAuth<NodeState>,
        bootstrap_addrs: Vec<SocketAddr>,
        dst_xorname: XorName,
        dst_section_key: BlsPublicKey,
        new_age: u8,
    ) -> Result<(Self, Cmd)> {
        let recipients: Vec<_> = bootstrap_addrs
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
            genesis_key,
            relocate_proof,
            used_recipient_saps,
            dst_xorname,
            dst_section_key,
            new_age,
            old_keypair,
        };
        let cmd = relocating.build_join_request_cmd(&recipients, dst_xorname, dummy_signature)?;

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
                let new_recipients: Vec<_> = section_auth
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
                let cmd = self.build_join_request_cmd(
                    &new_recipients,
                    section_auth.prefix().name(),
                    new_name_sig,
                )?;

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
                let new_recipients: Vec<_> = section_auth
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
                let cmd = self.build_join_request_cmd(
                    &new_recipients,
                    section_auth.prefix().name(),
                    new_name_sig,
                )?;

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
        recipients: &[Peer],
        dst_name: XorName,
        new_name_sig: Signature,
    ) -> Result<Cmd> {
        let join_request = JoinAsRelocatedRequest {
            section_key: self.dst_section_key,
            relocate_proof: self.relocate_proof.clone(),
            signature_over_new_name: new_name_sig,
        };

        info!("Sending {:?} to {:?}", join_request, recipients);

        let node_msg = SystemMsg::JoinAsRelocatedRequest(Box::new(join_request));
        let wire_msg = WireMsg::single_src(
            &self.node,
            DstLocation::Section {
                name: dst_name,
                section_pk: self.dst_section_key,
            },
            node_msg,
            self.genesis_key,
        )?;

        let cmd = Cmd::SendMsg {
            recipients: recipients.to_vec(),
            wire_msg,
        };

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
