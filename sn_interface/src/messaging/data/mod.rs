// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Data messages and their possible responses.

mod cmd;
mod errors;
mod query;
mod register;
mod spentbook;

pub use self::{
    cmd::DataCmd,
    errors::{Error, Result},
    query::{DataQuery, DataQueryVariant},
    register::{
        CreateRegister, EditRegister, RegisterCmd, RegisterQuery, SignedRegisterCreate,
        SignedRegisterEdit,
    },
    spentbook::{SpentbookCmd, SpentbookQuery},
};

use crate::network_knowledge::SectionTreeUpdate;
use crate::types::{
    register::{Entry, EntryHash, Permissions, Policy, Register, User},
    Chunk,
};
use crate::{
    messaging::{
        msg_type::{CLIENT_CMD_PRIORITY, CLIENT_QUERY_PRIORITY},
        MsgId,
    },
    types::ReplicatedData,
};

use qp2p::UsrMsgBytes;
use serde::{Deserialize, Serialize};
use sn_dbc::SpentProofShare;
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    fmt::{self, Debug, Display, Formatter},
};

/// Network service messages exchanged between clients
/// and nodes in order for the clients to use the network services.
/// NB: These are not used for node-to-node comms (see [`NodeMsg`] for those).
///
/// [`NodeMsg`]: crate::messaging::system::NodeMsg
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum ClientMsg {
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
}

impl ClientMsg {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        match self {
            Self::Cmd(_) => CLIENT_CMD_PRIORITY,
            Self::Query(_) => CLIENT_QUERY_PRIORITY,
        }
    }
}

impl Display for ClientMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cmd(cmd) => write!(f, "ClientMsg::Cmd({:?})", cmd),
            Self::Query(query) => write!(f, "ClientMsg::Query({:?})", query),
        }
    }
}

/// Messages sent from the nodes to the clients in response to queries or commands
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, custom_debug::Debug)]
pub enum ClientDataResponse {
    /// The response to a query, containing the query result.
    QueryResponse {
        /// The result of the query.
        response: QueryResponse,
        /// ID of the [`Query`] message.
        ///
        /// [`Query`]: self::ClientMsg::Query
        correlation_id: MsgId,
    },
    /// The response will be sent back to the client when the handling on the
    /// receiving Elder has been finished.
    CmdResponse {
        /// The result of the command.
        response: CmdResponse,
        /// ID of causing [`Cmd`] message.
        ///
        /// [`Cmd`]: self::ClientMsg::Cmd
        correlation_id: MsgId,
    },
    AntiEntropy {
        /// The update to our NetworkKnowledge containing the current `SectionAuthorityProvider`
        /// and the section chain truncated from the triggering msg's dst section_key or genesis_key
        /// if the the dst section_key is not a direct ancestor to our section_key
        section_tree_update: SectionTreeUpdate,
        /// This AE message is sent to a client when a message with outdated section
        /// information was received, attaching the bounced message so
        /// the client can resend it with up to date destination information.
        #[debug(skip)]
        bounced_msg: UsrMsgBytes,
    },
}

impl ClientDataResponse {
    /// The priority of the message, when handled by lower level comms.
    pub fn priority(&self) -> i32 {
        CLIENT_QUERY_PRIORITY
    }
}

impl Display for ClientDataResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryResponse { response, .. } => {
                write!(f, "ClientDataResponse::QueryResponse({response:?})")
            }
            Self::CmdResponse { response, .. } => {
                write!(f, "ClientDataResponse::CmdResponse({response:?})")
            }
            Self::AntiEntropy { .. } => {
                write!(f, "ClientDataResponse::AntiEntropy")
            }
        }
    }
}

/// The response to a query, containing the query result.
#[allow(clippy::large_enum_variant, clippy::type_complexity)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
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
        matches!(
            self,
            GetChunk(Err(Error::DataNotFound(_)))
                | GetRegister(Err(Error::DataNotFound(_)))
                | GetRegisterEntry(Err(Error::DataNotFound(_)))
                | GetRegisterEntry(Err(Error::NoSuchEntry(_)))
                | GetRegisterOwner(Err(Error::DataNotFound(_)))
                | GetRegisterOwner(Err(Error::NoSuchUser(_)))
                | ReadRegister(Err(Error::DataNotFound(_)))
                | GetRegisterPolicy(Err(Error::DataNotFound(_)))
                | GetRegisterPolicy(Err(Error::NoSuchUser(_)))
                | GetRegisterUserPermissions(Err(Error::DataNotFound(_)))
                | GetRegisterUserPermissions(Err(Error::NoSuchUser(_)))
                | SpentProofShares(Err(Error::DataNotFound(_)))
        )
    }
}

/// The response to a Cmd, containing the query result.
#[allow(clippy::large_enum_variant, clippy::type_complexity)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum CmdResponse {
    //
    // ===== Chunk =====
    //
    /// Response to DataCmd::StoreChunk
    StoreChunk(Result<()>),
    //
    // ===== Register Data =====
    //
    /// Response to RegisterCmd::Create.
    CreateRegister(Result<()>),
    /// Response to RegisterCmd::Edit.
    EditRegister(Result<()>),
    //
    // ===== Spentbook Data =====
    //
    /// Response to SpentbookCmd::Spend.
    SpendKey(Result<()>),
}

impl CmdResponse {
    #[allow(clippy::result_large_err)]
    pub fn ok(data: ReplicatedData) -> Result<CmdResponse> {
        let res = match &data {
            ReplicatedData::Chunk(_) => CmdResponse::StoreChunk(Ok(())),
            ReplicatedData::RegisterWrite(RegisterCmd::Create { .. }) => {
                CmdResponse::CreateRegister(Ok(()))
            }
            ReplicatedData::RegisterWrite(RegisterCmd::Edit { .. }) => {
                CmdResponse::EditRegister(Ok(()))
            }
            ReplicatedData::SpentbookWrite(_) => CmdResponse::SpendKey(Ok(())),
            ReplicatedData::RegisterLog(_) => return Err(Error::NoCorrespondingCmdError), // this should be unreachable, since `RegisterLog` is not resulting from a cmd.
            ReplicatedData::SpentbookLog(_) => return Err(Error::NoCorrespondingCmdError), // this should be unreachable, since `SpentbookLog` is not resulting from a cmd.
        };
        Ok(res)
    }

    #[allow(clippy::result_large_err)]
    pub fn err(data: ReplicatedData, err: Error) -> Result<CmdResponse> {
        let res = match &data {
            ReplicatedData::Chunk(_) => CmdResponse::StoreChunk(Err(err)),
            ReplicatedData::RegisterWrite(RegisterCmd::Create { .. }) => {
                CmdResponse::CreateRegister(Err(err))
            }
            ReplicatedData::RegisterWrite(RegisterCmd::Edit { .. }) => {
                CmdResponse::EditRegister(Err(err))
            }
            ReplicatedData::SpentbookWrite(_) => CmdResponse::SpendKey(Err(err)),
            ReplicatedData::RegisterLog(_) => return Err(Error::NoCorrespondingCmdError), // this should be unreachable, since `RegisterLog` is not resulting from a cmd.
            ReplicatedData::SpentbookLog(_) => return Err(Error::NoCorrespondingCmdError), // this should be unreachable, since `SpentbookLog` is not resulting from a cmd.
        };
        Ok(res)
    }

    /// Returns true if the result returned is a success or not
    pub fn is_success(&self) -> bool {
        self.result().is_ok()
    }

    /// Returns the result
    pub fn result(&self) -> &Result<()> {
        use CmdResponse::*;
        match self {
            StoreChunk(result)
            | CreateRegister(result)
            | EditRegister(result)
            | SpendKey(result) => result,
        }
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
        use crate::messaging::data::ClientMsg;
        use crate::messaging::data::DataCmd;
        use crate::messaging::WireMsg;

        let chunks = (0..10).map(|_| Chunk::new(random_bytes(3072)));

        for chunk in chunks {
            let (original_msg, serialised_cmd) = {
                let msg = ClientMsg::Cmd(DataCmd::StoreChunk(chunk));
                let bytes = WireMsg::serialize_msg_payload(&msg)?;
                (msg, bytes)
            };
            let deserialized_msg: ClientMsg =
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
