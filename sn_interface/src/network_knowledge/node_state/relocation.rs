// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::NodeState;
use crate::messaging::system::SectionSigned;
use crate::network_knowledge::{Error, Result};
use crate::types::utils::calc_age;

use ed25519_dalek::{PublicKey, Signature, Verifier};
use hex_fmt::HexFmt;
use serde::{Deserialize, Serialize};
use sn_consensus::Decision;
use xor_name::XorName;

use std::fmt::{self, Display, Formatter};

// Unique identifier for a churn event, which is used to select nodes to relocate.
pub struct ChurnId(pub XorName);

impl Display for ChurnId {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(
            fmt,
            "Churn-{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2]
        )
    }
}

/// The relocation trigger is sent by the elder nodes to the relocating nodes.
/// This is then used by the relocating nodes to request the Section to propose a relocation membership change.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct RelocationTrigger(Decision<NodeState>);

impl RelocationTrigger {
    /// We are relocating to the section that matches `dst_name`.
    pub fn new(decision: Decision<NodeState>) -> Self {
        Self(decision)
    }

    /// calculates the destination section for the given `peer_name`.
    pub fn dst_section(&self, peer_name: XorName) -> XorName {
        let mut content_parts = Vec::new();
        content_parts.push(peer_name.0.to_vec());
        for sig in self.0.proposals.values() {
            content_parts.push(sig.to_bytes().to_vec());
        }

        XorName::from_content_parts(
            Vec::from_iter(content_parts.iter().map(|v| v.as_slice())).as_slice(),
        )
    }

    /// calculates the churn_id for the given proposals.
    pub fn churn_id(&self) -> ChurnId {
        let mut content_parts = Vec::new();
        for sig in self.0.proposals.values() {
            content_parts.push(sig.to_bytes().to_vec());
        }

        ChurnId(XorName::from_content_parts(
            Vec::from_iter(content_parts.iter().map(|v| v.as_slice())).as_slice(),
        ))
    }
}

/// The relocation info contains the dst (in `NodeState`),
/// the old name, the new name and the source section signature
/// over the fact that the section considered the node to be relocated.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct RelocationInfo {
    signed_relocation: SectionSigned<NodeState>,
    new_name: XorName,
}

/// A relocation proof proves that a section started a relocation
/// of one of its nodes, and that the new name provided was created by that node.
///
/// NB: Upper layers will need to verify that said section is also a known section,
/// only then is the relocation fully valid.
#[derive(Clone, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
pub struct RelocationProof {
    info: RelocationInfo,
    // This sig proves that the new name was actually created by the node holding the old keys.
    #[serde(with = "serde_bytes")]
    #[debug(with = "Self::fmt_ed25519")]
    self_sig: Signature,
    /// The old key that identified the node in the source section.
    self_old_key: PublicKey,
}

impl RelocationInfo {
    pub fn new(signed_relocation: SectionSigned<NodeState>, new_name: XorName) -> Self {
        Self {
            signed_relocation,
            new_name,
        }
    }
}

impl RelocationProof {
    pub fn new(info: RelocationInfo, self_sig: Signature, self_old_key: PublicKey) -> Self {
        Self {
            info,
            self_sig,
            self_old_key,
        }
    }

    /// The key of the section that the node is relocating from.
    pub fn signed_by(&self) -> &bls::PublicKey {
        &self.info.signed_relocation.sig.public_key
    }

    /// This verifies that the new name was actually created by the node holding the old name,
    /// and that the section signature is signed by the provided section key.
    /// Calling context will need to verify that said section key is also a known section.
    pub fn verify(&self) -> Result<()> {
        // the key that we use to verify the sig over the new name, must match the name of the relocated node
        if self.old_key_name() != self.info.signed_relocation.name() {
            return Err(Error::InvalidRelocationProof);
        }
        let serialized_info =
            bincode::serialize(&self.info).map_err(|_err| Error::InvalidRelocationProof)?;
        self.self_old_key
            .verify(&serialized_info, &self.self_sig)
            .map_err(|_err| Error::InvalidRelocationProof)?;
        let serialized_state = bincode::serialize(&self.info.signed_relocation.value)
            .map_err(|_err| Error::InvalidRelocationProof)?;
        if !self.info.signed_relocation.sig.verify(&serialized_state) {
            Err(Error::InvalidRelocationProof)
        } else {
            Ok(())
        }
    }

    /// New name of the relocating node.
    pub fn new_name(&self) -> XorName {
        self.info.new_name
    }
    /// New age of the relocating node.
    pub fn new_age(&self) -> u8 {
        calc_age(&self.new_name())
    }

    /// Previous name of the relocating node.
    pub fn previous_name(&self) -> XorName {
        self.info.signed_relocation.name()
    }
    /// Previous age of the relocating node.
    pub fn previous_age(&self) -> u8 {
        self.info.signed_relocation.age()
    }

    pub fn signed_relocation(&self) -> &SectionSigned<NodeState> {
        &self.info.signed_relocation
    }

    // ed25519_dalek::Signature has overly verbose debug output, so we provide our own
    fn fmt_ed25519(sig: &Signature, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({:0.10})", HexFmt(sig))
    }

    fn old_key_name(&self) -> XorName {
        use crate::types::PublicKey::Ed25519;
        XorName::from(Ed25519(self.self_old_key))
    }
}

/// The current state of a relocating node
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum RelocationState {
    /// The node is not peforming a relocation
    NoRelocation,
    /// A relocation dst is sent from a section to one of its members, based upon
    /// the node matching the trigger.
    /// This is the elders asking the node to start polling
    /// them for the decision to remove it from members as being relocated.
    PreparingToRelocate(RelocationTrigger),
    /// When the node has a `RelocationProof` it can join the dst section with the provided proof.
    ReadyToJoinNewSection(RelocationProof),
}

impl RelocationState {
    pub fn proof(&self) -> Option<&RelocationProof> {
        match self {
            Self::ReadyToJoinNewSection(proof) => Some(proof),
            _ => None,
        }
    }
}
