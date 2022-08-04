// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Prefix;

use crate::node::handover::Error as HandoverError;

use sn_interface::{
    messaging::data::Error as ErrorMsg,
    types::{convert_dt_error_to_error_msg, Peer, PublicKey, ReplicatedDataAddress as DataAddress},
};

use secured_linked_list::error::Error as SecuredLinkedListError;
use std::{io, net::SocketAddr};
use thiserror::Error;
use xor_name::XorName;

/// The type returned by the `sn_routing` message handling methods.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Internal error.
#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error("CmdCtrl has stopped.")]
    CmdCtrlStopped,
    // We shortcircuit and ignore older len prefixes to avoid that
    // during a split DKG messages are still ongoing post-split
    // and are sent to the neighbouring section, which causes an AE loop as
    // section keys are not in chain.
    #[error("Dkg prefix is shorter than our prefix, so dropping the message.")]
    InvalidDkgPrefix,
    #[error("No membership data exists when it is needed.")]
    NoMembershipFound,
    #[error("Max retries reached for processing cmd, cmd dropped.")]
    MaxCmdRetriesReached(usize),
    #[error("Cmd job watcher dropped for some unknown reason.")]
    CmdJobWatcherDropped,
    #[error("Permit was not retrieved in 500 loops")]
    CouldNotGetPermitInTime,
    #[cfg(feature = "chaos")]
    #[error("[Chaos] feature flag induced a crash at startup")]
    ChaoticStartupCrash,
    #[error("Node prioritisation semaphore was closed early.")]
    SemaphoreClosed,
    #[error("There was a problem during acquisition of a tokio::sync::semaphore permit.")]
    PermitAcquisitionFailed,
    #[error("Section authority provider cannot be trusted: {0}")]
    UntrustedSectionAuthProvider(String),
    #[error("Proof chain cannot be trusted: {0}")]
    UntrustedProofChain(String),
    #[error("Invalid genesis key of provided prefix map: {}", hex::encode(_0.to_bytes()))]
    InvalidGenesisKey(bls::PublicKey),
    #[error("Cannot route. Delivery group size: {}, candidates: {}.", _0, _1)]
    CannotRoute(usize, usize),
    #[error("Empty recipient list")]
    EmptyRecipientList,
    #[error("Could not connect to any bootstrap contact")]
    BootstrapFailed,
    #[error("Cannot connect to the endpoint: {0}")]
    CannotConnectEndpoint(#[from] qp2p::EndpointError),
    #[error("Address not reachable: {0}")]
    AddressNotReachable(#[from] qp2p::RpcError),
    #[error("The node is not in a state to handle the action.")]
    InvalidState,
    #[error("Invalid dkg participant; not part of our section.")]
    InvalidDkgParticipant,
    #[error("Content of a received message is inconsistent.")]
    InvalidMessage,
    #[error("A signature share is invalid.")]
    InvalidSignatureShare,
    #[error("The secret key share is missing for public key {0:?}")]
    MissingSecretKeyShare(bls::PublicKey),
    #[error("Failed to send a message to {0}")]
    FailedSend(Peer),
    #[error("Link to peer has been dropped {0}")]
    PeerLinkDropped(Peer),
    #[error("Invalid section chain: {0}")]
    InvalidSectionChain(#[from] SecuredLinkedListError),
    #[error("Messaging protocol error: {0}")]
    Messaging(#[from] sn_interface::messaging::Error),
    #[error("Membership error: {0}")]
    Membership(#[from] crate::node::membership::Error),
    #[error("Invalid payload")]
    InvalidPayload,
    #[error("The section is currently set to not allow taking any new node")]
    TryJoinLater,
    #[error("No matching Section")]
    NoMatchingSection,
    #[error(
        "A JoinResponse was reeived after we've already joined the network. It has been ignored."
    )]
    AlreadyJoinedTheNetwork,
    #[error("No matching Elder")]
    NoMatchingElder,
    #[error("Node cannot join the network since it is not externally reachable: {0}")]
    NodeNotReachable(SocketAddr),
    /// Timeout when trying to join the network
    #[error("Timeout when trying to join the network")]
    JoinTimeout,
    /// Join occured during section churn and new elders missed it, need to re-join the network
    #[error("Node was removed from the section")]
    RemovedFromSection,
    /// Database error.
    #[error("Database error:: {0}")]
    Database(#[from] crate::dbs::Error),
    /// Not enough in the section to perform Chunk operation
    #[error("Not enough Adults available in Section({0:?}) to perform operation")]
    NoAdults(Prefix),
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing")]
    NoSectionPublicKey,
    /// Not Section PublicKeySet.
    #[error("Not section public key set returned from routing")]
    NoSectionPublicKeySet,
    /// Not Section PublicKey.
    #[error("Not section public key returned from routing for xorname {0}")]
    NoSectionPublicKeyKnown(XorName),
    /// Key, Value pair not found.
    #[error("No such data: {0:?}")]
    NoSuchData(DataAddress),
    /// Chunk already exists for this node
    #[error("Data already exists at this node")]
    DataExists,
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// JSON serialisation error.
    #[error("JSON serialisation error:: {0}")]
    JsonSerialisation(#[from] serde_json::Error),
    /// Bincode error.
    #[error("Bincode error:: {0}")]
    Bincode(#[from] bincode::Error),
    /// Network service message error.
    #[error("Network service message error:: {0}")]
    ServiceMsg(#[from] sn_interface::messaging::data::Error),
    /// Network data error.
    #[error("Network data error:: {0}")]
    NetworkData(#[from] sn_interface::types::Error),
    /// Error Sending Cmd in to node for processing
    #[error("Error Sending Cmd in to node for processing.")]
    CmdSendError,
    /// Network Knowledge error.
    #[error("Network data error:: {0}")]
    NetworkKnowledge(#[from] sn_interface::network_knowledge::Error),
    /// Data owner provided is invalid.
    #[error("Provided PublicKey is not a valid owner. Provided PublicKey: {0}")]
    InvalidOwner(PublicKey),
    /// Signature verification failed
    #[error("Invalid signature")]
    InvalidSignature,
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),
    /// Invalid node authority for a query response.
    #[error("Invalid node authority received for a QueryResponse message")]
    InvalidQueryResponseAuthority,
    /// DysfunctionDetection error.
    #[error("DysfunctionDetection error:: {0}")]
    DysfunctionDetection(#[from] sn_dysfunction::Error),
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
    /// Error occurred when minting the Genesis DBC.
    #[error("Genesis DBC error:: {0}")]
    GenesisDbcError(String),
}

impl From<qp2p::ClientEndpointError> for Error {
    fn from(error: qp2p::ClientEndpointError) -> Self {
        let endpoint_err = match error {
            qp2p::ClientEndpointError::Config(error) => qp2p::EndpointError::Config(error),
            qp2p::ClientEndpointError::Socket(error) => qp2p::EndpointError::Socket(error),
            qp2p::ClientEndpointError::Io(error) => qp2p::EndpointError::IoError(error),
        };

        Self::CannotConnectEndpoint(endpoint_err)
    }
}

impl From<qp2p::SendError> for Error {
    fn from(error: qp2p::SendError) -> Self {
        Self::AddressNotReachable(qp2p::RpcError::Send(error))
    }
}

pub(crate) fn convert_to_error_msg(error: Error) -> ErrorMsg {
    match error {
        Error::InvalidOwner(key) => ErrorMsg::InvalidOwner(key),
        Error::NoSuchData(address) => ErrorMsg::DataNotFound(address),
        Error::DataExists => ErrorMsg::DataExists,
        Error::NetworkData(error) => convert_dt_error_to_error_msg(error),
        other => ErrorMsg::InvalidOperation(format!("Failed to perform operation: {:?}", other)),
    }
}
