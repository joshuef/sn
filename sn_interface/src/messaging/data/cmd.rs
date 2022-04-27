// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{CmdError, Error};

#[cfg(feature = "registers")]
use super::RegisterCmd;
#[cfg(feature = "chunks")]
use crate::types::Chunk;

use serde::{Deserialize, Serialize};
use xor_name::XorName;

/// Data cmds - creating, updating, or removing data.
///
/// See the [`types`] module documentation for more details of the types supported by the Safe
/// Network, and their semantics.
///
/// [`types`]: crate::types
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum DataCmd {
    #[cfg(feature = "chunks")]
    /// [`Chunk`] write operation.
    ///
    /// [`Chunk`]: crate::types::Chunk
    StoreChunk(Chunk),
    #[cfg(feature = "registers")]
    /// [`Register`] write operation.
    ///
    /// [`Register`]: crate::types::register::Register
    Register(RegisterCmd),
}

impl DataCmd {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// cmd variant.
    pub fn error(&self, error: Error) -> CmdError {
        use DataCmd::*;
        match self {
            #[cfg(feature = "chunks")]
            StoreChunk(_) => CmdError::Data(error),
            #[cfg(feature = "registers")]
            Register(c) => c.error(error),
        }
    }

    /// Returns the xorname of the data for this cmd.
    pub fn dst_name(&self) -> XorName {
        use DataCmd::*;
        match self {
            #[cfg(feature = "chunks")]
            StoreChunk(c) => *c.name(),
            #[cfg(feature = "registers")]
            Register(c) => c.name(), // TODO: c.dst_id(), as to not co-locate private and public and different tags of same name.
        }
    }
}
