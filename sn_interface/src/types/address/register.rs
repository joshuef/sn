// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::super::{Result, XorName};
use super::{DataAddress, Scope};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// Address of a Register, different from
/// a ChunkAddress in that it also includes a tag.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub enum RegisterAddress {
    /// Public namespace.
    Public {
        /// Name.
        name: XorName,
        /// Tag.
        tag: u64,
    },
    /// Private namespace.
    Private {
        /// Name.
        name: XorName,
        /// Tag.
        tag: u64,
    },
}

impl RegisterAddress {
    /// Constructs a new `RegisterAddress` given `name`, `scope`, and `tag`.
    pub fn new(name: XorName, scope: Scope, tag: u64) -> Self {
        match scope {
            Scope::Public => Self::Public { name, tag },
            Scope::Private => Self::Private { name, tag },
        }
    }

    /// Returns the scope.
    pub fn scope(&self) -> Scope {
        match self {
            Self::Public { .. } => Scope::Public,
            Self::Private { .. } => Scope::Private,
        }
    }

    /// This is a unique identifier of the Register,
    /// since it also encodes the Public | Private scope,
    /// as well as the tag of the Address.
    pub fn id(&self) -> Result<XorName> {
        Ok(XorName::from_content(self.encode_to_zbase32()?.as_bytes()))
    }

    /// Returns the name.
    /// This is not a unique identifier.
    pub fn name(&self) -> &XorName {
        match self {
            Self::Public { ref name, .. } | Self::Private { ref name, .. } => name,
        }
    }

    /// Returns the tag.
    pub fn tag(&self) -> u64 {
        match self {
            Self::Public { tag, .. } | Self::Private { tag, .. } => *tag,
        }
    }

    /// Returns true if public.
    pub fn is_public(&self) -> bool {
        matches!(self.scope(), Scope::Public)
    }

    /// Returns true if private.
    pub fn is_private(&self) -> bool {
        !self.is_public()
    }

    /// Returns the Address serialised and encoded in z-base-32.
    pub fn encode_to_zbase32(&self) -> Result<String> {
        DataAddress::Register(*self).encode_to_zbase32()
    }
}
