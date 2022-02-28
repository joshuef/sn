// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Scope;
use xor_name::XorName;

/// Address of Bytes data type.
#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize, Debug,
)]
pub enum BytesAddress {
    /// Private namespace.
    Private(XorName),
    /// Public namespace.
    Public(XorName),
}

impl BytesAddress {
    /// Creates a new BytesAddress
    pub fn new(name: XorName, scope: Scope) -> BytesAddress {
        match scope {
            Scope::Public => Self::Public(name),
            Scope::Private => Self::Private(name),
        }
    }

    /// The xorname.
    pub fn name(&self) -> &XorName {
        match self {
            Self::Public(name) | Self::Private(name) => name,
        }
    }

    /// The address scope
    pub fn scope(&self) -> Scope {
        match self {
            Self::Public(_) => Scope::Public,
            Self::Private(_) => Scope::Private,
        }
    }

    /// Returns true if public.
    pub fn is_public(self) -> bool {
        matches!(self.scope(), Scope::Public)
    }

    /// Returns true if private.
    pub fn is_private(self) -> bool {
        !self.is_public()
    }
}
