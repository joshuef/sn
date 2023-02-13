// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::fmt::{self, Formatter};

use super::NodeState;
use crate::messaging::system::SectionSigned;
use crate::network_knowledge::{Error, Result};

use ed25519_dalek::{PublicKey, Signature, Verifier};
use hex_fmt::HexFmt;
use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// We are relocating to the section that matches the contained XorName.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct RelocationDst(XorName);

impl RelocationDst {
    /// We are relocating to the section that matches `dst_name`.
    pub fn new(dst_name: XorName) -> Self {
        Self(dst_name)
    }

    pub fn name(&self) -> &XorName {
        &self.0
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

impl RelocationInfo {
    pub fn new(signed_relocation: SectionSigned<NodeState>, new_name: XorName) -> Self {
        Self {
            signed_relocation,
            new_name,
        }
    }
}

/// The relocation trigger is sent by the elder nodes to the relocating nodes.
/// This is then used by the relocating nodes to request the Section to propose a relocation membership change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RelocationTrigger {
    pub dst: RelocationDst,
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

    /// Previous name of the relocating node.
    pub fn previous_name(&self) -> XorName {
        self.info.signed_relocation.name()
    }

    /// Previous age of the relocating node.
    pub fn previous_age(&self) -> u8 {
        self.info.signed_relocation.age()
    }

    // ed25519_dalek::Signature has overly verbose debug output, so we provide our own
    pub fn fmt_ed25519(sig: &Signature, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({:0.10})", HexFmt(sig))
    }

    fn old_key_name(&self) -> XorName {
        use crate::types::PublicKey::Ed25519;
        XorName::from(Ed25519(self.self_old_key))
    }
}

/// The current state of a relocating node
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum RelocationState {
    /// If the node has the `RelocationTrigger` then it can request the section to relocate it.
    RequestToRelocate(RelocationTrigger),
    /// If the node has the `RelocationProof` then it can join the destination with the provided proof.
    JoinAsRelocated(RelocationProof),
}
