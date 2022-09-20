// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::{
    messaging::{
        data::{DataQuery, DataQueryVariant, Error as ErrorMsg, QueryResponse},
        Error as MessagingError, MsgId,
    },
    types::{Error as DtError, Peer},
};

use bls::PublicKey;
use std::io;
use thiserror::Error;
use xor_name::XorName;

/// Specialisation of `std::Result` for Client.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Client Errors
#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to obtain network contacts to bootstrap to
    #[error("Failed to obtain network contacts to bootstrap to: {0}")]
    NetworkContacts(String),
    /// InsufficientAcksReceived
    #[error("Did not receive sufficient ACK messages from elders to be sure this cmd passed.")]
    InsufficientAcksReceived,
    /// Message auth checks failed
    #[error("Message's authority could not be trusted, unknown section key: {0:?}.")]
    UntrustedMessage(PublicKey),
    /// Initial network contact failed
    #[error("Initial network contact probe failed. Attempted contacts: {0:?}")]
    NetworkContact(Vec<Peer>),
    /// Client has not gone through qp2p bootstrap process yet
    #[error("Client has not yet acquired enough/any network knowledge for destination xorname {0}, so anything sent is guaranteed to have a lengthy AE process")]
    NoNetworkKnowledge(XorName),
    /// Could not connect to sufficient elder to retrieve reliable responses.
    #[error(
        "Problem connecting to sufficient elders. A supermajority of responses is unobtainable. {connections} were connected to, {required} needed."
    )]
    InsufficientElderConnections {
        /// Number of existing connections to Elders
        connections: usize,
        /// Minimum number of connections to Elders required for the operation
        required: usize,
    },
    /// Did not know of sufficient elders in the desired section to get supermajority of responses.
    #[error(
        "Problem finding sufficient elders. A supermajority of responses is unobtainable. {connections} were known in this section, {required} needed. Section pk: {section_pk:?}"
    )]
    InsufficientElderKnowledge {
        /// Number of existing connections to Elders
        connections: usize,
        /// Minimum number of connections to Elders required for the operation
        required: usize,
        /// Public key of the target section
        section_pk: PublicKey,
    },
    /// Cannot store empty file..
    #[error("Cannot store empty file.")]
    EmptyFileProvided,
    /// Not enough bytes for self-encryption.
    #[error("Not enough bytes ({size}) for self-encryption, at least {minimum} bytes needed. Try storing it as a SmallFile.")]
    TooSmallForSelfEncryption {
        /// Number of bytes
        size: usize,
        /// Minimum number of bytes for self-encryption
        minimum: usize,
    },
    /// Encryption oversized the SmallFile, so it cannot be stored as a SmallFile and be encrypted
    #[error("You might need to pad the `SmallFile` contents and then store it as a `LargeFile`, as the encryption has made it slightly too big ({0} bytes)")]
    SmallFilePaddingNeeded(usize),
    /// The provided bytes is too large to store as a `SmallFile`.
    #[error(
        "The provided bytes ({size}) is too large to store as a `SmallFile` which maximum can be {maximum}. Store as a LargeFile instead."
    )]
    TooLargeAsSmallFile {
        /// Number of bytes
        size: usize,
        /// Maximum number of bytes for a `SmallFile`
        maximum: usize,
    },
    /// Failed to obtain any response
    #[error("No responses were returned for file upload validation")]
    NoResponsesForUploadValidation,
    /// Failed to obtain a response from Elders.
    #[error("Failed to obtain any response from: {0:?}")]
    NoResponse(Vec<Peer>),
    /// Failed to obtain a response from Elders even after retrying.
    #[error("Failed to obtain any response, even after {attempts} attempts, for query: {query:?}. Error in last attempt: {last_error}")]
    NoResponseAfterRetrying {
        /// Number of attempts made
        attempts: usize,
        /// Query sent to Elders
        query: DataQuery,
        /// Source of error in last attempt
        last_error: Box<Self>,
    },
    /// No operation Id could be found
    #[error("Could not retrieve the operation id of a query or response")]
    UnknownOperationId,
    /// Unexpected query response received
    #[error("Unexpected response received for {query:?}. Received: {response:?}")]
    UnexpectedQueryResponse {
        /// Query sent to Elders
        query: DataQueryVariant,
        /// Unexpected response received
        response: QueryResponse,
    },
    /// Other types errors
    #[error(transparent)]
    NetworkDataError(#[from] DtError),
    /// Errors received from the network via sn_messaging
    #[error("Error received from the network: {source:?}")]
    ErrorMsg {
        /// The source of an error msg
        source: ErrorMsg,
    },
    /// Error response received for a client cmd sent to the network
    #[error("Error received from the network: {source:?} for cmd: {msg_id:?}")]
    ErrorCmd {
        /// The source of an error msg
        source: ErrorMsg,
        /// MsgId of the cmd
        msg_id: MsgId,
    },
    /// Errors occurred when serialising or deserialising msgs
    #[error(transparent)]
    MessagingProtocol(#[from] MessagingError),
    /// Self-Enryption errors
    #[error(transparent)]
    SelfEncryption(#[from] self_encryption::Error),
    /// Io error.
    #[error(transparent)]
    IoError(#[from] io::Error),
    /// Endpoint setup error.
    #[error(transparent)]
    EndpointSetup(#[from] qp2p::ClientEndpointError),
    /// QuicP2p Recv error.
    #[error(transparent)]
    QuicP2p(#[from] qp2p::RecvError),
    /// QuicP2p Connection error.
    #[error("Failed to stablish a connection with node {peer:?}: {error}.")]
    QuicP2pConnection {
        /// Node the connection was attempted to be stablished with
        peer: Peer,
        /// The error encountered when attempting to stablish the connection
        error: qp2p::ConnectionError,
    },
    /// QuicP2p Send error.
    #[error("Failed to send a message to node {peer:?}: {error}.")]
    QuicP2pSend {
        /// Node the message was attempted to be sent to
        peer: Peer,
        /// The error encountered when attempting to send the message
        error: qp2p::SendError,
    },
    /// Bincode error
    #[error(transparent)]
    Serialisation(#[from] Box<bincode::ErrorKind>),
    /// Could not retrieve all chunks required to decrypt the data. (expected, error)
    #[error("Not all chunks were retrieved, expected {expected}, retrieved {retrieved}.")]
    NotEnoughChunksRetrieved {
        /// Number of Chunks expected to be retrieved
        expected: usize,
        /// Number of Chunks retrieved
        retrieved: usize,
    },
    /// Could not chunk all the data required to encrypt the data. (Expected, Actual)
    #[error("Not all data was chunked, expected {expected}, but we have {chunked}.)")]
    NotAllDataWasChunked {
        /// Number of Chunks expected to be generated
        expected: usize,
        /// Number of Chunks generated
        chunked: usize,
    },
}
