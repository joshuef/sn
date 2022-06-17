// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Result};

use sn_interface::{
    data_copy_count,
    messaging::{
        system::{NodeCmd, SystemMsg},
        DstLocation,
    },
    types::{log_markers::LogMarker, Peer, ReplicatedDataAddress},
};

use itertools::Itertools;
use std::collections::BTreeSet;

impl Node {
    /// Given what data the peer has, we shall calculate what data the peer is missing that
    /// we have, and send such data to the peer.
    #[instrument(skip(self, data_sender_has))]
    pub(crate) async fn get_missing_data_for_node(
        &self,
        sender: Peer,
        data_sender_has: Vec<ReplicatedDataAddress>,
    ) -> Result<Vec<Cmd>> {
        trace!("Getting missing data for node");
        // Collection of data addresses that we do not have

        // TODO: can we cache this data stored per churn event?
        let data_i_have = self.data_storage.keys().await?;
        trace!("Our data got");

        if data_i_have.is_empty() {
            trace!("We have no data");
            return Ok(vec![]);
        }

        let adults = self.network_knowledge.adults();
        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let mut data_for_sender = vec![];
        for data in data_i_have {
            if data_sender_has.contains(&data) {
                continue;
            }

            let holder_adult_list: BTreeSet<_> = adults_names
                .clone()
                .sorted_by(|lhs, rhs| data.name().cmp_distance(lhs, rhs))
                .take(data_copy_count())
                .collect();

            if holder_adult_list.contains(&sender.name()) {
                debug!("Our requester should hold: {:?}", data);
                let _existed = data_for_sender.push(data);
            }
        }

        if data_for_sender.is_empty() {
            trace!("We have no data worth sending");
            return Ok(vec![]);
        }

        debug!(
            "{:?} batch to: {:?} ",
            LogMarker::QueuingMissingReplicatedData,
            sender
        );

        let cmd = Cmd::EnqueueDataForReplication {
            recipient: sender,
            data_batch: data_for_sender,
        };

        Ok(vec![cmd])
    }

    /// Will send a list of currently known/owned data to relevant nodes.
    /// These nodes should send back anything missing (in batches).
    /// Relevant nodes should be all _prior_ neighbours + _new_ elders.
    #[instrument(skip(self))]
    pub(crate) async fn ask_for_any_new_data(&self) -> Result<Vec<Cmd>> {
        debug!("Querying section for any new data");
        let data_i_have = self.data_storage.keys().await?;
        let mut cmds = vec![];

        let adults = self.network_knowledge.adults();
        let adults_names = adults.iter().map(|p2p_node| p2p_node.name()).collect_vec();

        let elders = self.network_knowledge.elders();
        let my_name = self.info.borrow().name();

        // find data targets that are not us.
        let mut target_member_names = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| my_name.cmp_distance(lhs, rhs))
            .filter(|peer| peer != &my_name)
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "nearest neighbours for data req: {}: {:?}",
            target_member_names.len(),
            target_member_names
        );

        // also send to our elders in case they are holding but were just promoted
        for elder in elders {
            let _existed = target_member_names.insert(elder.name());
        }

        let section_pk = self.network_knowledge.section_key();

        for name in target_member_names {
            trace!("Sending our data list to: {:?}", name);
            cmds.push(Cmd::SignOutgoingSystemMsg {
                msg: SystemMsg::NodeCmd(
                    NodeCmd::SendAnyMissingRelevantData(data_i_have.clone()).clone(),
                ),
                dst: DstLocation::Node { name, section_pk },
            })
        }

        Ok(cmds)
    }

    /// Will reorganize data if we are an adult,
    /// and there were changes to adults (any added or removed).
    pub(crate) async fn try_reorganize_data(&self) -> Result<Vec<Cmd>> {
        // as an elder we dont want to get any more data for our name
        // (elders will eventually be caching data in general)
        if self.is_elder() {
            return Ok(vec![]);
        }

        trace!("{:?}", LogMarker::DataReorganisationUnderway);

        self.ask_for_any_new_data().await
    }
}
