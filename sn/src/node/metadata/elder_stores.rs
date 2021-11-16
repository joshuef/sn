// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::data::DataExchange;
use crate::node::{network::Network, Error, Result};

/// The various data type stores,
/// that are only managed at Elders.
pub(super) struct ElderStores {
    // TODO: this is needed to access RegsiterStorage for DataExchange flows
    // This should be removed once chunks are all in routing.
    pub(crate) network: Network,
}

impl ElderStores {
    pub(super) fn new(network: Network) -> Self {
        Self { network }
    }

    // TODO: This should be moved into routing
    pub(super) async fn update(&mut self, data: DataExchange) -> Result<(), Error> {
        // todo: all this can be done in parallel
        // let register_storage = self.network.get_register_storage().await;

        // register_storage.update(data.reg_data)?;
        // let _chunks = self.network.update_chunks(data.chunk_data).await;
        Ok(())
    }
}
