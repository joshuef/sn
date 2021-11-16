// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod elder_stores;

use crate::messaging::data::{DataExchange, StorageLevel};
use crate::node::{network::Network, Result};
use crate::types::PublicKey;
use elder_stores::ElderStores;
use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
};
use xor_name::XorName;

/// This module is called `Metadata`
/// as a preparation for the responsibilities
/// it will have eventually, after `Data Hierarchy Refinement`
/// has been implemented; where the data types are all simply
/// the structures + their metadata - handled at `Elders` - with
/// all underlying data being chunks stored at `Adults`.
pub(crate) struct Metadata {
    elder_stores: ElderStores,
}

impl Metadata {
    pub(crate) async fn new(network: Network) -> Result<Self> {
        let elder_stores = ElderStores::new(network);
        Ok(Self { elder_stores })
    }

    pub(crate) async fn retain_members_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        self.elder_stores
            .network
            .retain_members_only(members)
            .await?;
        Ok(())
    }

    /// Sets the storage level of a given node.
    /// Returns whether the level changed or not.
    pub(crate) async fn set_storage_level(&self, node_id: PublicKey, level: StorageLevel) -> bool {
        self.elder_stores
            .network
            .set_storage_level(&node_id, level)
            .await
    }
}

impl Display for Metadata {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "Metadata")
    }
}
