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
mod spentbook;

pub use self::{
    cmd::DataCmd,
    data_exchange::{MetadataExchange, StorageLevel},
    errors::{Error, Result},
    query::DataQuery,
    query::DataQueryVariant,
    register::{
        CreateRegister, EditRegister, RegisterCmd, RegisterQuery, SignedRegisterCreate,
        SignedRegisterEdit,
    },
    spentbook::{SpentbookCmd, SpentbookQuery},
};

use crate::messaging::MsgId;
use crate::types::{
    register::{Entry, EntryHash, Permissions, Policy, Register, User},
    Chunk,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sn_dbc::SpentProofShare;
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    fmt::{self, Debug, Display, Formatter},
};
use xor_name::XorName;

/// Network service messages exchanged between clients
/// and nodes in order for the clients to use the network services.
/// NB: These are not used for node-to-node comms (see [`SystemMsg`] for those).
///
/// [`SystemMsg`]: crate::messaging::system::SystemMsg
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
        error: Error,
        /// ID of causing [`Cmd`] message.
        ///
        /// [`Cmd`]: Self::Cmd
        correlation_id: MsgId,
    },
    /// A message indicating that an error occurred as a node was handling a client's message.
    ServiceError {
        /// Optional reason for the error.
        ///
        /// This can be used to handle the error.
        reason: Option<Error>,
        /// Message that triggered this error.
        ///
        /// This could be used to retry the message if the error could be handled.
        source_message: Option<Bytes>,
    },
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
            Self::Query(query) => Some(query.variant.dst_name()),
            _ => None,
        }
    }

    #[cfg(any(feature = "chunks", feature = "registers"))]
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        use super::msg_type::{SERVICE_CMD_PRIORITY, SERVICE_QUERY_PRIORITY};

        match self {
            // Client <-> node service comms
            Self::Cmd(_) => SERVICE_CMD_PRIORITY,
            _ => SERVICE_QUERY_PRIORITY,
        }
    }
}

impl Display for ServiceMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cmd(cmd) => write!(f, "ServiceMsg::Cmd({:?})", cmd),
            Self::CmdAck { correlation_id } => {
                write!(f, "ServiceMsg::CmdAck({:?})", correlation_id)
            }
            Self::CmdError { error, .. } => write!(f, "ServiceMsg::CmdError({:?})", error),
            Self::Query(query) => write!(f, "ServiceMsg::Query({:?})", query),
            Self::QueryResponse { response, .. } => {
                write!(f, "ServiceMsg::QueryResponse({:?})", response)
            }
            Self::ServiceError { reason, .. } => {
                write!(f, "ServiceMsg::ServiceError({:?})", reason)
            }
        }
    }
}

/// The response to a query, containing the query result.
/// Response operation id should match query `operation_id`
#[allow(clippy::large_enum_variant, clippy::type_complexity)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Hash)]
pub enum QueryResponse {
    //
    // ===== Chunk =====
    //
    /// Response to [`GetChunk`]
    ///
    /// [`GetChunk`]: crate::messaging::data::DataQueryVariant::GetChunk
    GetChunk(Result<Chunk>),
    //
    // ===== Register Data =====
    //
    /// Response to [`RegisterQuery::Get`].
    GetRegister(Result<Register>),
    /// Response to [`RegisterQuery::GetEntry`].
    GetRegisterEntry(Result<Entry>),
    /// Response to [`RegisterQuery::GetOwner`].
    GetRegisterOwner(Result<User>),
    /// Response to [`RegisterQuery::Read`].
    ReadRegister(Result<BTreeSet<(EntryHash, Entry)>>),
    /// Response to [`RegisterQuery::GetPolicy`].
    GetRegisterPolicy(Result<Policy>),
    /// Response to [`RegisterQuery::GetUserPermissions`].
    GetRegisterUserPermissions(Result<Permissions>),
    //
    // ===== Spentbook Data =====
    //
    /// Response to [`SpentbookQuery::SpentProofShares`].
    SpentProofShares(Result<Vec<SpentProofShare>>),
}

impl QueryResponse {
    /// Returns true if the result returned is a success or not
    pub fn is_success(&self) -> bool {
        use QueryResponse::*;
        matches!(
            self,
            GetChunk(Ok(_))
                | GetRegister(Ok(_))
                | GetRegisterEntry(Ok(_))
                | GetRegisterOwner(Ok(_))
                | ReadRegister(Ok(_))
                | GetRegisterPolicy(Ok(_))
                | GetRegisterUserPermissions(Ok(_))
                | SpentProofShares(Ok(_))
        )
    }

    /// Returns true if the result returned is DataNotFound
    pub fn is_data_not_found(&self) -> bool {
        use QueryResponse::*;
        if let SpentProofShares(Ok(vec)) = self {
            return vec.len() < 5;
        }

        matches!(
            self,
            GetChunk(Err(Error::DataNotFound(_)))
                | GetRegister(Err(Error::DataNotFound(_)))
                | GetRegisterEntry(Err(Error::DataNotFound(_)))
                | GetRegisterEntry(Err(Error::NoSuchEntry))
                | GetRegisterOwner(Err(Error::DataNotFound(_)))
                | GetRegisterOwner(Err(Error::NoSuchEntry))
                | ReadRegister(Err(Error::DataNotFound(_)))
                | GetRegisterPolicy(Err(Error::DataNotFound(_)))
                | GetRegisterPolicy(Err(Error::NoSuchEntry))
                | GetRegisterUserPermissions(Err(Error::DataNotFound(_)))
                | GetRegisterUserPermissions(Err(Error::NoSuchEntry))
                | SpentProofShares(Err(Error::DataNotFound(_)))
        )
    }
}

/// Error type for an attempted conversion from a [`QueryResponse`] variant
/// to an expected wrapped value.
#[derive(Debug, Eq, PartialEq)]
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
                        QueryResponse::$variant(Ok(data)) => Ok(data),
                        QueryResponse::$variant(Err(error)) => Err(TryFromError::Response(error)),
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

    fn gen_keys() -> Vec<PublicKey> {
        gen_keypairs().iter().map(PublicKey::from).collect()
    }

    #[test]
    fn debug_format_functional() -> Result<()> {
        if let Some(key) = gen_keys().first() {
            let errored_response =
                QueryResponse::GetRegister(Err(Error::AccessDenied(User::Key(*key))));
            assert!(format!("{:?}", errored_response).contains("GetRegister(Err(AccessDenied("));
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
