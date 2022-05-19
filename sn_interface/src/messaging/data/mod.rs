// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Data messages and their possible responses.

mod cmd;
mod data_exchange;
mod errors;
mod query;
mod register;

pub use self::{
    cmd::DataCmd,
    data_exchange::{MetadataExchange, RegisterStoreExport, ReplicatedRegisterLog, StorageLevel},
    errors::{Error, Result},
    query::DataQuery,
    register::{
        CreateRegister, DeleteRegister, EditRegister, ExtendRegister, RegisterCmd, RegisterQuery,
        SignedRegisterCreate, SignedRegisterDelete, SignedRegisterEdit, SignedRegisterExtend,
    },
};

use crate::types::{
    register::{Entry, EntryHash, Permissions, Policy, Register, User},
    Chunk, ChunkAddress, DataAddress,
};
use crate::{
    messaging::{data::Error as ErrorMsg, MsgId},
    types::utils,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Formatter};
use std::{collections::BTreeSet, convert::TryFrom};
use tiny_keccak::{Hasher, Sha3};
use xor_name::XorName;

/// Derivable Id of an operation. Query/Response should return the same id for simple tracking purposes.
/// TODO: make uniquer per requester for some operations
#[derive(Deserialize, Serialize, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct OperationId(pub(crate) [u8; 32]);

impl Display for OperationId {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(
            fmt,
            "OpId-{:02x}{:02x}{:02x}..",
            self.0[0], self.0[1], self.0[2]
        )
    }
}

impl Debug for OperationId {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

/// Return operation Id of a chunk
pub fn chunk_operation_id(address: &ChunkAddress) -> Result<OperationId> {
    let bytes = utils::encode(address).map_err(|_| Error::NoOperationId)?;
    let mut hasher = Sha3::v256();
    let mut output = [0; 32];
    hasher.update(bytes.as_bytes());
    hasher.finalize(&mut output);

    Ok(OperationId(output))
}

/// A message indicating that an error occurred as a node was handling a client's message.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct ServiceError {
    /// Optional reason for the error.
    ///
    /// This can be used to handle the error.
    pub reason: Option<Error>,
    /// Message that triggered this error.
    ///
    /// This could be used to retry the message if the error could be handled.
    pub source_message: Option<Bytes>,
}

/// Network service messages that clients or nodes send in order to use the services,
/// communicate and carry out the tasks.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum ServiceMsg {
    /// Messages that lead to mutation.
    ///
    /// There will be no response to these messages on success, only if something went wrong. Due to
    /// the eventually consistent nature of the network, it may be necessary to continually retry
    /// operations that depend on the effects of mutations.
    Cmd(DataCmd),
    /// A read-only operation.
    ///
    /// Senders should eventually receive either a corresponding [`QueryResponse`] or an error in
    /// reply.
    /// [`QueryResponse`]: Self::QueryResponse
    Query(DataQuery),
    /// The response to a query, containing the query result.
    QueryResponse {
        /// The result of the query.
        response: QueryResponse,
        /// ID of the query message.
        correlation_id: MsgId,
    },
    /// An error response to a [`Cmd`].
    ///
    /// [`Cmd`]: Self::Cmd
    CmdError {
        /// The error.
        error: CmdError,
        /// ID of causing [`Cmd`] message.
        ///
        /// [`Cmd`]: Self::Cmd
        correlation_id: MsgId,
    },
    /// A message indicating that an error occurred as a node was handling a client's message.
    ServiceError(ServiceError),
    /// CmdAck will be sent back to the client when the handling on the
    /// receiving Elder has been succeeded.
    CmdAck {
        /// ID of causing [`Cmd`] message.
        ///
        /// [`Cmd`]: Self::Cmd
        correlation_id: MsgId,
    },
}

impl ServiceMsg {
    /// Returns the destination address for cmds and Queries only.
    pub fn dst_address(&self) -> Option<XorName> {
        match self {
            Self::Cmd(cmd) => Some(cmd.dst_name()),
            Self::Query(query) => Some(query.dst_name()),
            _ => None,
        }
    }
}

/// An error response to a [`Cmd`].
///
/// [`Cmd`]: ServiceMsg::Cmd
#[derive(Debug, Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum CmdError {
    /// An error response to a [`DataCmd`].
    // FIXME: `Cmd` is not an enum, so should this be?
    Data(Error), // DataError enum for better differentiation?
}

/// The response to a query, containing the query result.
/// Response operation id should match query operation_id
#[allow(clippy::large_enum_variant, clippy::type_complexity)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum QueryResponse {
    //
    // ===== Chunk =====
    //
    /// Response to [`ChunkRead::Get`].
    GetChunk(Result<Chunk>),
    //
    // ===== Register Data =====
    //
    /// Response to [`RegisterQuery::Get`].
    GetRegister((Result<Register>, OperationId)),
    /// Response to [`RegisterQuery::GetEntry`].
    GetRegisterEntry((Result<Entry>, OperationId)),
    /// Response to [`RegisterQuery::GetOwner`].
    GetRegisterOwner((Result<User>, OperationId)),
    /// Response to [`RegisterQuery::Read`].
    ReadRegister((Result<BTreeSet<(EntryHash, Entry)>>, OperationId)),
    /// Response to [`RegisterQuery::GetPolicy`].
    GetRegisterPolicy((Result<Policy>, OperationId)),
    /// Response to [`RegisterQuery::GetUserPermissions`].
    GetRegisterUserPermissions((Result<Permissions>, OperationId)),
    //
    // ===== Other =====
    //
    /// Failed to create id generation
    FailedToCreateOperationId,
}

impl QueryResponse {
    /// Returns true if the result returned is a success or not
    pub fn is_success(&self) -> bool {
        use QueryResponse::*;
        match self {
            GetChunk(result) => result.is_ok(),
            GetRegister((result, _op_id)) => result.is_ok(),
            GetRegisterEntry((result, _op_id)) => result.is_ok(),
            GetRegisterOwner((result, _op_id)) => result.is_ok(),
            ReadRegister((result, _op_id)) => result.is_ok(),
            GetRegisterPolicy((result, _op_id)) => result.is_ok(),
            GetRegisterUserPermissions((result, _op_id)) => result.is_ok(),
            FailedToCreateOperationId => false,
        }
    }

    /// Returns true if data was not found
    pub fn failed_with_data_not_found(&self) -> bool {
        use QueryResponse::*;

        match self {
            GetChunk(result) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::ChunkNotFound(_)),
            },
            GetRegister((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            GetRegisterEntry((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            GetRegisterOwner((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            ReadRegister((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            GetRegisterPolicy((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            GetRegisterUserPermissions((result, _op_id)) => match result {
                Ok(_) => false,
                Err(error) => matches!(*error, ErrorMsg::DataNotFound(_)),
            },
            FailedToCreateOperationId => false,
        }
    }

    /// Retrieves the operation identifier for this response, use in tracking node liveness
    /// and responses at clients.
    pub fn operation_id(&self) -> Result<OperationId> {
        use QueryResponse::*;

        // TODO: Operation Id should eventually encompass _who_ the op is for.
        match self {
            GetChunk(result) => match result {
                Ok(chunk) => chunk_operation_id(chunk.address()),
                Err(ErrorMsg::ChunkNotFound(name)) => chunk_operation_id(&ChunkAddress(*name)),
                Err(ErrorMsg::DataNotFound(DataAddress::Bytes(address))) => {
                    chunk_operation_id(&ChunkAddress(*address.name()))
                }
                Err(ErrorMsg::DataNotFound(another_address)) => {
                    error!(
                        "{:?} address returned when we were expecting a ChunkAddress",
                        another_address
                    );
                    Err(Error::NoOperationId)
                }
                Err(another_error) => {
                    error!("Could not form operation id: {:?}", another_error);
                    Err(Error::InvalidQueryResponseErrorForOperationId)
                }
            },
            GetRegister((_, operation_id))
            | GetRegisterEntry((_, operation_id))
            | GetRegisterOwner((_, operation_id))
            | ReadRegister((_, operation_id))
            | GetRegisterPolicy((_, operation_id))
            | GetRegisterUserPermissions((_, operation_id)) => Ok(*operation_id),
            FailedToCreateOperationId => Err(Error::NoOperationId),
        }
    }
}

/// Error type for an attempted conversion from a [`QueryResponse`] variant to an expected wrapped
/// value.
#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum TryFromError {
    /// Wrong variant found in `QueryResponse`.
    WrongType,
    /// The `QueryResponse` contained an error.
    Response(Error),
}

macro_rules! try_from {
    ($ok_type:ty, $($variant:ident),*) => {
        impl TryFrom<QueryResponse> for $ok_type {
            type Error = TryFromError;
            fn try_from(response: QueryResponse) -> std::result::Result<Self, Self::Error> {
                match response {
                    $(
                        QueryResponse::$variant((Ok(data), _op_id)) => Ok(data),
                        QueryResponse::$variant((Err(error), _op_id)) => Err(TryFromError::Response(error)),
                    )*
                    _ => Err(TryFromError::WrongType),
                }
            }
        }
    };
}

// try_from!(Chunk, GetChunk);

impl TryFrom<QueryResponse> for Chunk {
    type Error = TryFromError;
    fn try_from(response: QueryResponse) -> std::result::Result<Self, Self::Error> {
        match response {
            QueryResponse::GetChunk(Ok(data)) => Ok(data),
            QueryResponse::GetChunk(Err(error)) => Err(TryFromError::Response(error)),
            _ => Err(TryFromError::WrongType),
        }
    }
}

try_from!(Register, GetRegister);
try_from!(User, GetRegisterOwner);
try_from!(BTreeSet<(EntryHash, Entry)>, ReadRegister);
try_from!(Policy, GetRegisterPolicy);
try_from!(Permissions, GetRegisterUserPermissions);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{utils::random_bytes, Chunk, Keypair, PublicKey};
    use bytes::Bytes;
    use eyre::{eyre, Result};
    use std::convert::{TryFrom, TryInto};

    fn gen_keypairs() -> Vec<Keypair> {
        let mut rng = rand::thread_rng();
        let bls_secret_key = bls::SecretKeySet::random(1, &mut rng);
        vec![
            Keypair::new_ed25519(),
            Keypair::new_bls_share(
                0,
                bls_secret_key.secret_key_share(0),
                bls_secret_key.public_keys(),
            ),
        ]
    }

    pub fn gen_keys() -> Vec<PublicKey> {
        gen_keypairs().iter().map(PublicKey::from).collect()
    }

    #[test]
    fn debug_format_functional() -> Result<()> {
        if let Some(key) = gen_keys().first() {
            let errored_response = QueryResponse::GetRegister((
                Err(Error::AccessDenied(User::Key(*key))),
                [
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5,
                    6, 7, 8, 9, 1, 2,
                ], // some op id
            ));
            assert!(format!("{:?}", errored_response).contains("GetRegister((Err(AccessDenied("));
            Ok(())
        } else {
            Err(eyre!("Could not generate public key"))
        }
    }

    #[test]
    fn try_from() -> Result<()> {
        use QueryResponse::*;
        let key = match gen_keys().first() {
            Some(key) => User::Key(*key),
            None => return Err(eyre!("Could not generate public key")),
        };

        let i_data = Chunk::new(Bytes::from(vec![1, 3, 1, 4]));
        let e = Error::AccessDenied(key);
        assert_eq!(
            i_data,
            GetChunk(Ok(i_data.clone()))
                .try_into()
                .map_err(|_| eyre!("Mismatched types".to_string()))?
        );
        assert_eq!(
            Err(TryFromError::Response(e.clone())),
            Chunk::try_from(GetChunk(Err(e)))
        );

        Ok(())
    }

    #[test]
    fn wire_msg_payload() -> Result<()> {
        use crate::messaging::data::DataCmd;
        use crate::messaging::data::ServiceMsg;
        use crate::messaging::WireMsg;

        let chunks = (0..10).map(|_| Chunk::new(random_bytes(3072)));

        for chunk in chunks {
            let (original_msg, serialised_cmd) = {
                let msg = ServiceMsg::Cmd(DataCmd::StoreChunk(chunk));
                let bytes = WireMsg::serialize_msg_payload(&msg)?;
                (msg, bytes)
            };
            let deserialized_msg: ServiceMsg =
                rmp_serde::from_slice(&serialised_cmd).map_err(|err| {
                    crate::messaging::Error::FailedToParse(format!(
                        "Data message payload as Msgpack: {}",
                        err
                    ))
                })?;
            assert_eq!(original_msg, deserialized_msg);
        }

        Ok(())
    }
}
