// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Result};
use sn_interface::types::log_markers::LogMarker;
use std::collections::BTreeSet;
use xor_name::XorName;

impl Node {
    /// Will reorganize data if we are an adult,
    /// and there were changes to adults (any added or removed).
    #[cfg(feature = "service-msgs")]
    pub(crate) async fn try_reorganize_data(
        &self,
        old_adults: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        if self.is_elder().await {
            // only adults carry out the ops in this method
            return Ok(vec![]);
        }

        let current_adults: BTreeSet<_> = self
            .network_knowledge
            .adults()
            .await
            .iter()
            .map(|p| p.name())
            .collect();
        let added: BTreeSet<_> = current_adults.difference(&old_adults).copied().collect();
        let removed: BTreeSet<_> = old_adults.difference(&current_adults).copied().collect();

        if added.is_empty() && removed.is_empty() {
            // no adults added or removed, so nothing to do
            return Ok(vec![]);
        }

        trace!("{:?}", LogMarker::DataReorganisationUnderway);
        // we are an adult, and there were changes to adults
        // so we reorganise the data stored in this section..:
        let remaining = old_adults.intersection(&current_adults).copied().collect();
        self.reorganize_data(added, removed, remaining)
            .await
            .map_err(super::Error::from)
    }
}
