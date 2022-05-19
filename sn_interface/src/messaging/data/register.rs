// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{CmdError, Error, QueryResponse, Result};

use crate::messaging::{data::OperationId, SectionAuth};
use crate::types::register::{EntryHash, Register};
use crate::types::{
    register::{Entry, Policy, RegisterOp, User},
    RegisterAddress as Address,
};
use tiny_keccak::{Hasher, Sha3};

use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// [`Register`] read operations.
#[allow(clippy::large_enum_variant)]
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub enum RegisterQuery {
    /// Retrieve the [`Register`] at the given address.
    ///
    /// This should eventually lead to a [`GetRegister`] response.
    ///
    /// [`GetRegister`]: QueryResponse::GetRegister
    Get(Address),
    /// Retrieve the current entries from the [`Register`] at the given address.
    ///
    /// Multiple entries occur on concurrent writes. This should eventually lead to a
    /// [`ReadRegister`] response.
    ///
    /// [`ReadRegister`]: QueryResponse::ReadRegister
    Read(Address),
    /// Get an entry from a [`Register`] on the Network by its hash
    ///
    /// This should eventually lead to a [`GetRegisterEntry`] response.
    ///
    /// [`GetEntry`]: QueryResponse::GetRegisterEntry
    GetEntry {
        /// Register address.
        address: Address,
        /// The hash of the entry.
        hash: EntryHash,
    },
    /// Retrieve the policy of the [`Register`] at the given address.
    ///
    /// This should eventually lead to a [`GetRegisterPolicy`] response.
    ///
    /// [`GetRegisterPolicy`]: QueryResponse::GetRegisterPolicy
    GetPolicy(Address),
    /// Retrieve the permissions of a given user for the [`Register`] at the given address.
    ///
    /// This should eventually lead to a [`GetRegisterUserPermissions`] response.
    ///
    /// [`GetRegisterUserPermissions`]: QueryResponse::GetRegisterUserPermissions
    GetUserPermissions {
        /// Register address.
        address: Address,
        /// User to get permissions for.
        user: User,
    },
    /// Retrieve the owner of the [`Register`] at the given address.
    ///
    /// This should eventually lead to a [`GetRegisterOwner`] response.
    ///
    /// [`GetRegisterOwner`]: QueryResponse::GetRegisterOwner
    GetOwner(Address),
}

/// A [`Register`] cmd that is stored in a log on Adults.
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum RegisterCmd {
    /// Create a new [`Register`] on the network.
    Create {
        /// The user signed op.
        cmd: SignedRegisterCreate,
        /// Section signature over the operation,
        /// verifying that it was paid for.
        section_auth: SectionAuth,
    },
    /// Edit the [`Register`].
    Edit(SignedRegisterEdit),
    /// Delete the [`Register`].
    Delete(SignedRegisterDelete),
    /// Extend the size of the [`Register`].
    Extend {
        /// The user signed op.
        cmd: SignedRegisterExtend,
        /// Section signature over the operation,
        /// verifying that it was paid for.
        section_auth: SectionAuth,
    },
}

///
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum CreateRegister {
    /// Populated with entries
    Populated(Register),
    /// Without entries
    Empty {
        /// The name of the [`Register`].
        name: XorName,
        /// The tag on the [`Register`].
        tag: u64,
        /// The initial size of the [`Register`].
        size: u16,
        /// The policy of the [`Register`].
        policy: Policy,
    },
}

impl CreateRegister {
    ///
    pub fn owner(&self) -> User {
        use CreateRegister::*;
        match self {
            Populated(reg) => reg.owner(),
            Empty { policy, .. } => *policy.owner(),
        }
    }

    ///
    pub fn size(&self) -> u16 {
        use CreateRegister::*;
        match self {
            Populated(reg) => reg.size() as u16,
            Empty { size, .. } => *size,
        }
    }

    ///
    pub fn address(&self) -> Address {
        use CreateRegister::*;
        match self {
            Populated(reg) => *reg.address(),
            Empty {
                policy, name, tag, ..
            } => {
                if let Policy::Public { .. } = policy {
                    Address::Public {
                        name: *name,
                        tag: *tag,
                    }
                } else {
                    Address::Private {
                        name: *name,
                        tag: *tag,
                    }
                }
            }
        }
    }
}

///
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct ExtendRegister {
    /// The address of the [`Register`] to extend.
    pub address: Address,
    /// The size to extend the [`Register`] with.
    pub extend_with: u16,
}

///
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct DeleteRegister(pub Address);

///
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct EditRegister {
    /// The address of the [`Register`] to edit.
    pub address: Address,
    /// The operation to perform.
    pub edit: RegisterOp<Entry>,
}

/// A signed cmd to create a [`Register`].
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SignedRegisterCreate {
    /// Create a [`Register`].
    pub op: CreateRegister,
    /// A signature carrying authority to perform the operation.
    ///
    /// This will be verified against the register's owner and permissions.
    pub auth: crate::messaging::ServiceAuth,
}

/// A signed cmd to create a [`Register`].
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SignedRegisterExtend {
    /// Extend a [`Register`].
    pub op: ExtendRegister,
    /// A signature carrying authority to perform the operation.
    ///
    /// This will be verified against the register's owner and permissions.
    pub auth: crate::messaging::ServiceAuth,
}

/// A [`Register`] write operation signed by the requester.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct SignedRegisterEdit {
    /// The operation to perform.
    pub op: EditRegister,
    /// A signature carrying authority to perform the operation.
    ///
    /// This will be verified against the register's owner and permissions.
    pub auth: crate::messaging::ServiceAuth,
}

/// A [`Register`] write operation.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct SignedRegisterDelete {
    /// Delete a private [`Register`].
    ///
    /// This operation will result in an error if applied to a public register. Only private
    /// registers can be deleted, and only by their current owner(s).
    pub op: DeleteRegister,
    /// A signature carrying authority to perform the operation.
    ///
    /// This will be verified against the register's owner and permissions.
    pub auth: crate::messaging::ServiceAuth,
}

impl SignedRegisterCreate {
    /// Returns the dst address of the register.
    pub fn dst_address(&self) -> Address {
        self.op.address()
    }
}

impl SignedRegisterEdit {
    /// Returns the dst address of the register.
    pub fn dst_address(&self) -> &Address {
        &self.op.address
    }
}

impl SignedRegisterDelete {
    /// Returns the dst address of the register.
    pub fn dst_address(&self) -> &Address {
        &self.op.0
    }
}

impl SignedRegisterExtend {
    /// Returns the dst address of the register.
    pub fn dst_address(&self) -> &Address {
        &self.op.address
    }
}

impl RegisterQuery {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> Result<QueryResponse> {
        match *self {
            RegisterQuery::Get(_) => Ok(QueryResponse::GetRegister((
                Err(error),
                self.operation_id()?,
            ))),
            RegisterQuery::Read(_) => Ok(QueryResponse::ReadRegister((
                Err(error),
                self.operation_id()?,
            ))),
            RegisterQuery::GetPolicy(_) => Ok(QueryResponse::GetRegisterPolicy((
                Err(error),
                self.operation_id()?,
            ))),
            RegisterQuery::GetUserPermissions { .. } => Ok(
                QueryResponse::GetRegisterUserPermissions((Err(error), self.operation_id()?)),
            ),
            RegisterQuery::GetEntry { .. } => Ok(QueryResponse::GetRegisterEntry((
                Err(error),
                self.operation_id()?,
            ))),
            RegisterQuery::GetOwner(_) => Ok(QueryResponse::GetRegisterOwner((
                Err(error),
                self.operation_id()?,
            ))),
        }
    }

    /// Returns the dst address for the request. (Scoped to Private/Public)
    pub fn dst_address(&self) -> Address {
        match self {
            RegisterQuery::Get(ref address)
            | RegisterQuery::Read(ref address)
            | RegisterQuery::GetPolicy(ref address)
            | RegisterQuery::GetUserPermissions { ref address, .. }
            | RegisterQuery::GetEntry { ref address, .. }
            | RegisterQuery::GetOwner(ref address) => *address,
        }
    }

    /// Returns the xorname of the data for request.
    pub fn dst_name(&self) -> XorName {
        match self {
            RegisterQuery::Get(ref address)
            | RegisterQuery::Read(ref address)
            | RegisterQuery::GetPolicy(ref address)
            | RegisterQuery::GetUserPermissions { ref address, .. }
            | RegisterQuery::GetEntry { ref address, .. }
            | RegisterQuery::GetOwner(ref address) => *address.name(),
        }
    }

    /// Retrieves the operation identifier for this response, use in tracking node liveness
    /// and responses at clients.
    /// Must be the same as the query response
    pub fn operation_id(&self) -> Result<OperationId> {
        let bytes = crate::types::utils::encode(&self).map_err(|_| Error::NoOperationId)?;
        let mut hasher = Sha3::v256();
        let mut output = [0; 32];
        hasher.update(bytes.as_bytes());
        hasher.finalize(&mut output);
        Ok(OperationId(output))
    }
}

impl RegisterCmd {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> CmdError {
        CmdError::Data(error)
    }

    /// Returns the name of the register.
    /// This is not a unique identifier.
    pub fn name(&self) -> XorName {
        *self.dst_address().name()
    }

    // /// Returns the id of the register.
    // /// This is a unique identifier, used
    // /// in order to not co-locate private and public
    // /// and different tags of same register name.
    // pub fn dst_id(&self) -> Result<XorName> {
    //     self.dst_address().id()
    // }

    /// Returns the dst address of the register.
    pub fn dst_address(&self) -> Address {
        match self {
            Self::Create { cmd, .. } => cmd.dst_address(),
            Self::Edit(cmd) => *cmd.dst_address(),
            Self::Delete(cmd) => *cmd.dst_address(),
            Self::Extend { cmd, .. } => *cmd.dst_address(),
        }
    }

    /// Owner of the Register
    pub fn owner(&self) -> Option<User> {
        match self {
            Self::Create {
                cmd: SignedRegisterCreate { op, .. },
                ..
            } => Some(op.owner()),
            _ => None,
        }
    }
}
