// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Error, QueryResponse};

use crate::types::SpentbookAddress;

use serde::{Deserialize, Serialize};
use sn_dbc::{KeyImage, RingCtTransaction, SpentProof};
use std::collections::BTreeSet;
use xor_name::XorName;

/// Spentbook read operations.
#[allow(clippy::large_enum_variant)]
#[derive(Hash, Eq, PartialEq, PartialOrd, Clone, Serialize, Deserialize, Debug)]
pub enum SpentbookQuery {
    /// Query the set of spent proofs if the provided key image has already been spent with a Tx
    SpentProofShares(SpentbookAddress),
}

/// A Spentbook cmd.
#[allow(clippy::large_enum_variant)]
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum SpentbookCmd {
    /// Request sent to Elders to log a key image (and associated Tx) to be logged as spent,
    /// returning the spent proof signature share to the sender once all verifications were successfull.
    Spend {
        key_image: KeyImage,
        tx: RingCtTransaction,
        spent_proofs: BTreeSet<SpentProof>,
        spent_transactions: BTreeSet<RingCtTransaction>,
    },
}

impl SpentbookQuery {
    /// Creates a Response containing an error, with the Response variant corresponding to the
    /// Request variant.
    pub fn error(&self, error: Error) -> QueryResponse {
        match self {
            Self::SpentProofShares(_) => QueryResponse::SpentProofShares(Err(error)),
        }
    }

    /// Returns the dst address for the request.
    pub fn dst_address(&self) -> SpentbookAddress {
        match self {
            Self::SpentProofShares(address) => *address,
        }
    }

    /// Returns the xorname of the data for request.
    pub fn dst_name(&self) -> XorName {
        *self.dst_address().name()
    }
}

impl SpentbookCmd {
    /// Returns the name of the register.
    /// This is not a unique identifier.
    pub fn name(&self) -> XorName {
        *self.dst_address().name()
    }

    /// Returns the dst address of the spentbook.
    pub fn dst_address(&self) -> SpentbookAddress {
        match self {
            Self::Spend { key_image, .. } => {
                SpentbookAddress::new(XorName::from_content(&key_image.to_bytes()))
            }
        }
    }
}
