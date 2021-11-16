// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::role::{ElderRole, Role};
use crate::messaging::data::DataExchange;
use crate::node::{metadata::Metadata, node_ops::NodeDuty, Node, Result};
use tracing::info;

impl Node {
    /// Level up a newbie to an oldie on promotion
    pub async fn level_up(&self) -> Result<()> {
        // start handling metadata
        let meta_data = Metadata::new(self.network_api.clone()).await?;

        *self.role.write().await = Role::Elder(ElderRole::new(meta_data, false));

        Ok(())
    }

    /// Continue the level up and handle more responsibilities.
    pub(crate) async fn synch_state(elder: &ElderRole, metadata: DataExchange) -> Result<NodeDuty> {
        if *elder.received_initial_sync.read().await {
            info!("We are already received the initial sync from our section. Ignoring update");
            return Ok(NodeDuty::NoOp);
        }
        // --------- merge in provided metadata ---------
        // elder.meta_data.write().await.update(metadata).await?;

        *elder.received_initial_sync.write().await = true;

        Ok(NodeDuty::NoOp)
    }
}
