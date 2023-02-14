// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{flow_ctrl::RejoinReason, Prefix};

use crate::node::handover::Error as HandoverError;

use sn_dbc::Error as DbcError;
use sn_interface::{
    dbcs::Error as GenesisError,
    messaging::{data::Error as ErrorMsg, system::DkgSessionId},
};

use ed25519::Signature;
use std::{io, net::SocketAddr};
use thiserror::Error;
use xor_name::XorName;

/// The type returned by the `sn_routing` message handling methods.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Internal error.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum Error {
    /// Client messages always need a response, so should come in on bidi streams
    #[error("No client response stream. Client message came in on a unidirectional stream. All client messages should be bidirectional.")]
    NoClientResponseStream,
    #[error("The bootstrap connection unexpectedly closed")]
    BootstrapConnectionClosed,
    /// SendChannel error for the data replication flow. This is a critical error and the node no longer functions.
    #[error("Data replication channel could not be sent to. This means the receiver has been dropped, the node can no longer replicate data and must shut down.")]
    DataReplicationChannel,
    /// This peer has no connections, and none will be created
    #[error("Peer link has no connections ")]
    NoConnectionsForPeer,
    /// This should not be possible as the channel is stored in node, and used to process child commands
    #[error("No more Cmds will be received or processed. CmdChannel senders have been dropped. ")]
    CmdCtrlChannelDropped,
    /// This should not be possible as the channel is stored in node, and used to process incoming msgs
    #[error("No more Msgs will be received or processed. MsgSender has been dropped. ")]
    MsgChannelDropped,
    /// We shortcircuit and ignore older len prefixes to avoid that
    /// during a split DKG messages are still ongoing post-split
    /// and are sent to the neighbouring section, which causes an AE loop as
    /// section keys are not in chain.
    #[error("Dkg prefix is shorter than our prefix, so dropping the message.")]
    InvalidDkgPrefix,
    #[error("No membership data exists when it is needed.")]
    NoMembershipFound,
    #[cfg(feature = "chaos")]
    #[error("[Chaos] feature flag induced a crash at startup")]
    ChaoticStartupCrash,
    #[error("Section authority provider cannot be trusted: {0}")]
    UntrustedSectionAuthProvider(String),
    #[error("Invalid dkg participant; not part of our section.")]
    InvalidDkgParticipant,
    #[error("Content of a received message is inconsistent.")]
    InvalidMessage,
    #[error("A signature share is invalid.")]
    InvalidSignatureShare,
    #[error("The secret key share is missing for public key {0:?}")]
    MissingSecretKeyShare(bls::PublicKey),
    #[error("Messaging protocol error: {0}")]
    Messaging(#[from] sn_interface::messaging::Error),
    #[error("Membership error: {0}")]
    Membership(#[from] crate::node::membership::Error),
    #[error("No matching Section")]
    NoMatchingSection,
    #[error("Node cannot join the network since it is not externally reachable: {0}")]
    NodeNotReachable(SocketAddr),
    #[error("A node has invalid relocation details")]
    InvalidRelocationDetails,
    /// Need to re-join the network
    #[error("Node needs to rejoin the network due to {0:?}")]
    RejoinRequired(RejoinReason),
    /// Comms error.
    #[error("Comms error: {0}")]
    Comms(#[from] sn_comms::Error),
    /// Database error.
    #[error("Database error:: {0}")]
    Database(#[from] crate::storage::Error),
    /// Insufficient number of Nodes found to perform data operation
    #[error(
        "Not enough Nodes available at section {prefix:?}. Expected {expected}, found {found}."
    )]
    InsufficientNodeCount {
        /// The prefix of the section.
        prefix: Prefix,
        /// Expected number of Nodes for minimum replication.
        expected: u8,
        /// Actual number of Nodes found to hold the data.
        found: u8,
    },
    /// Received a dkg message from an invalid Xorname
    #[error("Invalid Xorname: {0:?}")]
    InvalidXorname(XorName),
    /// Already generated our Dkg ephemeral key
    #[error("Already generated our Dkg ephemeral key")]
    DkgEphemeralKeyAlreadyGenerated,
    /// Received a dkg message from a node that is not in dkg session
    #[error("Node not in dkg session: {0:?}")]
    NodeNotInDkgSession(XorName),
    /// We don't have a bls key for this dkg session id yet
    #[error("We don't have a bls key for this dkg session id yet: {0:?}")]
    NoDkgKeysForSession(DkgSessionId),
    /// Double Key Attack Detected, node is faulty, error contains proof: sigs and signed bls keys
    #[error("Double Key Attack Detected from: {0:?}")]
    DoubleKeyAttackDetected(
        XorName,
        Box<bls::PublicKey>,
        Box<Signature>,
        Box<bls::PublicKey>,
        Box<Signature>,
    ),
    /// Dkg error
    #[error("DKG error: {0}")]
    DkgError(#[from] sn_sdkg::Error),
    /// We don't have a dkg state for this dkg session id
    #[error("No dkg state for session: {0:?}")]
    NoDkgStateForSession(DkgSessionId),
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// JSON serialisation error.
    #[error("JSON serialisation error:: {0}")]
    JsonSerialisation(#[from] serde_json::Error),
    /// Bincode error.
    #[error("Bincode error:: {0}")]
    Bincode(#[from] bincode::Error),
    /// Network client message error.
    #[error("Network client message error:: {0}")]
    ClientMsg(#[from] Box<sn_interface::messaging::data::Error>),
    /// Network data error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] sn_interface::types::Error),
    /// Error Sending Cmd in to node for processing
    #[error("Error sending Cmd on node channel for processing.")]
    CmdChannelSendError,
    /// Network Knowledge error.
    #[error("Network knowledge error:: {0}")]
    NetworkKnowledge(#[from] sn_interface::network_knowledge::Error),
    /// Signature verification failed
    #[error("Invalid signature")]
    InvalidSignature,
    /// Key share is not from the expected section
    #[error("Invalid key share section key")]
    InvalidKeyShareSectionKey,
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),
    /// Invalid node authority for a query response.
    #[error("Invalid node authority received for a QueryResponse message")]
    InvalidQueryResponseAuthority,
    /// Elder Handover related errors
    #[error("Handover Error:: {0}")]
    HandoverError(#[from] HandoverError),
    /// Request Handover Anti Entropy
    #[error("RequestHandoverAntiEntropy {0}")]
    RequestHandoverAntiEntropy(u64),
    /// Missing Membership Instance when checking a Handover vote's SAP
    #[error("MissingMembershipInstance")]
    MissingMembershipInstance,
    /// Failed to get parent SAP in chain when checking a Handover vote's SAP
    #[error("FailedToGetSAPforPrefix {0:?}")]
    FailedToGetSAPforPrefix(Prefix),
    /// Invalid Elder Candidates in Handover vote's SAP
    #[error("InvalidElderCandidates")]
    InvalidElderCandidates,
    /// Invalid Split Candidates in Handover vote's SAP
    #[error("InvalidSplitCandidates")]
    InvalidSplitCandidates,
    /// Received an invalid section prefix when checking handover candidate's section prefix
    /// The candidate's section prefix is supposed to be identical to ours for a handover
    #[error("Invalid Section Prefix For Handover Candidate")]
    InvalidSectionPrefixForCandidate,
    /// Received invalid split sections candidates
    /// The candidates are supposed to be 2 distinct children prefixes for our section:
    /// Example: for section 10, the candidates are supposed to be 101, 100
    #[error("Invalid Section Prefix For Handover Split Candidate")]
    InvalidSectionPrefixForSplitCandidates,
    /// Spentbook error
    #[error("Spentbook Error: {0}")]
    SpentbookError(String),
    /// A DBC spend request could not be processed because the processing section was unaware of
    /// the section that signed one of the input spent proofs.
    #[error("Spent proof is signed by section key {0:?} that is unknown to the current section")]
    SpentProofUnknownSectionKey(bls::PublicKey),
    /// Error occurred when minting the Genesis DBC.
    #[error("Genesis DBC error:: {0}")]
    GenesisDbcError(#[from] GenesisError),
    /// Error thrown by DBC public API
    #[error("DbcError: {0}")]
    DbcError(#[from] DbcError),
    #[error("BLS error: {0}")]
    BlsError(#[from] bls::Error),
    #[cfg(feature = "otlp")]
    #[error("OpenTelemetry Tracing error: {0}")]
    OpenTelemetryTracing(#[from] opentelemetry::trace::TraceError),
}

impl From<qp2p::SendError> for Error {
    fn from(error: qp2p::SendError) -> Self {
        Self::Comms(sn_comms::Error::AddressNotReachable(qp2p::RpcError::Send(
            error,
        )))
    }
}

// Convert node error to messaging error message for sending over the network.
impl From<Error> for ErrorMsg {
    fn from(error: Error) -> ErrorMsg {
        match error {
            Error::InsufficientNodeCount {
                prefix,
                expected,
                found,
            } => ErrorMsg::InsufficientNodeCount {
                prefix,
                expected,
                found,
            },
            Error::SpentProofUnknownSectionKey(unknown_section_key) => {
                ErrorMsg::SpentProofUnknownSectionKey(unknown_section_key)
            }
            Error::NetworkData(error) => error.into(),
            other => ErrorMsg::InvalidOperation(format!("Failed to perform operation: {other:?}")),
        }
    }
}

impl From<sn_interface::messaging::data::Error> for Error {
    fn from(error: sn_interface::messaging::data::Error) -> Error {
        Error::ClientMsg(Box::new(error))
    }
}
