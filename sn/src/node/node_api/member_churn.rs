// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::role::{ElderRole, Role};
use crate::node::{metadata::Metadata, Node, Result};

impl Node {
    /// Level up a newbie to an oldie on promotion
    pub async fn level_up(&self) -> Result<()> {
        // start handling metadata
        let meta_data = Metadata::new(self.network_api.clone()).await?;

        *self.role.write().await = Role::Elder(ElderRole::new(meta_data, false));

        Ok(())
    }
}
