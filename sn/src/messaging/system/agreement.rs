// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{section::NodeState, signed::KeyedSig};
use crate::messaging::SectionAuthorityProvider;
use ed25519_dalek::{PublicKey, Signature};
use hex_fmt::HexFmt;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, collections::BTreeSet, fmt, ops::Deref};
use xor_name::{Prefix, XorName};

/// SHA3-256 hash digest.
type Digest256 = [u8; 32];

/// Unique identifier of a DKG session.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, custom_debug::Debug)]
pub struct DkgSessionId {
    /// A hash of the peers and prefix of the specific session.
    #[debug(with = "Self::fmt_hash")]
    pub hash: Digest256,
    /// The generation, as in the length of the section chain main branch.
    pub generation: u64,
}

impl DkgSessionId {
    fn fmt_hash(hash: &Digest256, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:0.10}", HexFmt(hash))
    }
}

/// One signed failure for a DKG round by a given PublicKey
#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, custom_debug::Debug)]
pub struct DkgFailureSig {
    #[allow(missing_docs)]
    #[debug(with = "crate::types::PublicKey::fmt_ed25519")]
    pub public_key: PublicKey,
    #[allow(missing_docs)]
    #[debug(with = "crate::types::Signature::fmt_ed25519")]
    pub signature: Signature,
}

/// Dkg failure info for a round
#[derive(Default, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct DkgFailureSigSet {
    #[allow(missing_docs)]
    pub sigs: Vec<DkgFailureSig>,
    #[allow(missing_docs)]
    pub failed_participants: BTreeSet<XorName>,
}

/// A value together with the signature that it was agreed on by the majority of the section elders.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct SectionAuth<T: Serialize> {
    /// some value to be agreed upon by elders
    pub value: T,
    /// signature over the value
    pub sig: KeyedSig,
}

impl<T> Borrow<Prefix> for SectionAuth<T>
where
    T: Borrow<Prefix> + Serialize,
{
    fn borrow(&self) -> &Prefix {
        self.value.borrow()
    }
}

impl<T: Serialize> Deref for SectionAuth<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// A step in the Propose-Broadcast-Aggregate-Execute workflow.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
/// A proposal about the state of the network
/// This can be a result of seeing a node come online, go offline, changes to section info etc.
/// Anything where we need section authority before action can be taken
pub enum Proposal {
    /// Proposal to remove a node from our section
    Offline(NodeState),
    /// Proposal to update info about a section.
    ///
    /// It signals the completion of a DKG by the elder candidates to the current elders.
    /// This proposal is then signed by the newly generated section key.
    SectionInfo(SectionAuthorityProvider),
    /// Proposal to change the elders (and possibly the prefix) of our section.
    /// NOTE: the `SectionAuthorityProvider` is already signed with the new key. This proposal is only to signs the
    /// new key with the current key. That way, when it aggregates, we obtain all the following
    /// pieces of information at the same time:
    ///   1. the new section authority provider
    ///   2. the new key
    ///   3. the signature of the new section authority provider using the new key
    ///   4. the signature of the new key using the current key
    /// Which we can use to update the section section authority provider and the section chain at
    /// the same time as a single atomic operation without needing to cache anything.
    OurElders(SectionAuth<SectionAuthorityProvider>),
    /// Proposal to change whether new nodes are allowed to join our section.
    JoinsAllowed(bool),
}
