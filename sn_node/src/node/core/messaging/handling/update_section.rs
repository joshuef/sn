// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Error, Result};
use sn_interface::messaging::{
    system::{NodeCmd, SystemMsg},
    DstLocation,
};
use sn_interface::types::log_markers::LogMarker;
use std::collections::BTreeSet;
use xor_name::XorName;

impl Node {
    /// Will send a list of currently known/owned data to relevant nodes.
    /// These nodes should send back anything missing (in batches).
    /// Relevant nodes should be all _prior_ neighbours + _new_ elders.
    #[allow(dead_code)]
    pub(crate) async fn ask_for_any_new_data(&self) -> Result<Vec<Cmd>> {
        let data_i_have = self.data_storage.keys().await?;
        let membership_guard = self.membership.read().await;
        if let Some(membership) = &*membership_guard {
            let mut cmds = vec![];
            let gen = membership.generation();

            // TODO: should we refine this to more relevant nodes?
            // Here we use the previous membership state, as changes after a churn means we'll likely be missing out on data as those nodes are reorganising
            let mut target_members: BTreeSet<_> = membership
                .section_members(gen - 1)
                .map_err(Error::from)?
                .iter()
                .map(|(_, state)| state.peer())
                .collect();
            let current_elders = self.network_knowledge.elders().await;
            for elder in current_elders {
                let _existed = target_members.insert(elder);
            }
            let section_pk = self.network_knowledge.section_key().await;

            for peer in target_members.into_iter() {
                trace!("Sending replicated data list to: {:?}", peer);
                cmds.push(Cmd::SignOutgoingSystemMsg {
                    msg: SystemMsg::NodeCmd(
                        NodeCmd::EnsureReplicationOfDataAddress(data_i_have.clone()).clone(),
                    ),
                    dst: DstLocation::Node {
                        name: peer.name(),
                        section_pk,
                    },
                })
            }

            Ok(cmds)
        } else {
            // nothing to do
            Ok(vec![])
        }
    }

    /// Will reorganize data if we are an adult,
    /// and there were changes to adults (any added or removed).
    pub(crate) async fn try_reorganize_data(
        &self,
        old_adults: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        // if self.is_elder().await {
        //     // only adults carry out the ops in this method
        //     return Ok(vec![]);
        // }

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
