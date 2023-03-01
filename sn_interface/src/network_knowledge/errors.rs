// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use thiserror::Error;

/// The type returned by the `sn_routing` message handling methods.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Internal error.
#[derive(Debug, Error)]
pub enum Error {
    /// Error reading/writing a file
    #[error("File read/write error: {0}")]
    FileHandling(String),
    /// Error creating Directory
    #[error("Directory creation error: {0}")]
    DirectoryHandling(String),
    /// Serialization error
    #[error("Failed to serialise section tree: {0}")]
    Serialisation(String),
    /// Failed to deserialise a section tree.
    #[error("Failed to deserialise section tree: {0}")]
    Deserialisation(String),
    #[error("The provided SAP must belong to the genesis prefix")]
    NonGenesisSap,
    #[error("The provided signature cannot be verified while inserting into the SectionsDAG")]
    InvalidSignature,
    #[error("Key not found in the SectionsDAG: {0:?}")]
    KeyNotFound(bls::PublicKey),
    #[error("The 'to' or 'from' key is not present in the same branch of the SectionsDAG")]
    InvalidBranch,
    #[error("The SectionsDAG should contain a single branch")]
    MultipleBranchError,
    #[error("Proof chain cannot be trusted: {0}")]
    UntrustedProofChain(String),
    #[error("Provided proof_chain doesn't cover the SAP's key we currently know: {0}")]
    SAPKeyNotCoveredByProofChain(String),
    #[error("Section authority provider cannot be trusted: {0}")]
    UntrustedSectionAuthProvider(String),
    #[error("The genesis key of the provided SectionTree is invalid: {0:?}")]
    InvalidGenesisKey(bls::PublicKey),
    #[error("A relocating node provided invalid relocation proof.")]
    InvalidRelocationProof,
    #[error("A signature share is invalid.")]
    InvalidSignatureShare,
    #[error("The secret key share is missing for public key {0:?}")]
    MissingSecretKeyShare(bls::PublicKey),
    #[error("Invalid payload")]
    InvalidPayload,
    #[error("The section is currently set to not allow taking any new node")]
    TryJoinLater,
    #[error("No matching Section")]
    NoMatchingSection,
    #[error(
        "A JoinResponse was received after we've already had a node on this socket join the the network. It has been ignored."
    )]
    ExistingMemberSocketAddrConflict,
    #[error(
        "A JoinResponse was received after we've a node with this name has joined the network. It has been ignored."
    )]
    ExistingMemberNameConflict,
    #[error("The node in question is not a member of the section")]
    NotAMember,
    #[error("Request does not match the section prefix")]
    WrongSection,
    #[error("Consensus({0:?})")]
    Consensus(#[from] sn_consensus::Error),
    #[error("An archived node attempted to rejoin the section")]
    ArchivedNodeRejoined,
}
