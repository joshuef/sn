// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Result};
use itertools::Itertools;
use sn_interface::data_copy_count;
use sn_interface::types::{log_markers::LogMarker, Peer};
use sn_interface::{
    messaging::{
        system::{NodeCmd, SystemMsg},
        DstLocation,
    },
    types::ReplicatedDataAddress,
};
use std::collections::BTreeSet;
use xor_name::XorName;

impl Node {
    /// Gets data we hold, that is relevant to a specific sender, it returns a Vec of Vecs,
    /// each inner vec holding ReplicatedDatAddresses of lessening proximity to the holder
    /// This assumes that the requester has asked us a valid question...
    async fn get_data_for_holder(
        &self,
        data_i_have: Vec<ReplicatedDataAddress>,
        sender: Peer,
    ) -> Result<Vec<Vec<ReplicatedDataAddress>>> {
        // You sort by xorname for all data keys
        // you take adults known / replica count's worth of names.
        // those are then prioritised into batches...

        // let known_adults = self.network_knowledge.adults().await.len();
        let data_copies = data_copy_count();

        // how much of the held data should any one node be responsible for
        let data_per_holder = data_i_have.len().checked_div(data_copies);
        // if we're responsible for any of it...
        if let Some(data_per_holder) = data_per_holder {
            debug!("{sender} is responsible for {data_per_holder} of data");

            // let data_prio_1: Vec<_> =

            let sorted_data = data_i_have
                .into_iter()
                .sorted_by(|lhs, rhs| sender.name().cmp_distance(lhs.name(), rhs.name()))
                .collect::<Vec<_>>();

            let data_chunks = sorted_data
                .chunks(data_per_holder)
                .map(|a| {
                    a.iter().cloned().collect_vec()
                })
                // .collect::<Vec<_>>();
                // .collect_vec()
                .collect_vec();

            Ok(data_chunks)
            // debug!("len of direct data responsibility: {:?}", data_prio_1.len());
        } else {
            Ok(vec![])
        }
    }

    /// Given a peer and a `greed level`, we check if a peer should be holding certain data.
    /// A greed_level of 0 will result in true being returned only if the peer is within
    /// `data_copy_count` nodes of the data.
    async fn check_if_node_should_hold_data_in_section(
        &self,
        node_name: &XorName,
        data: &ReplicatedDataAddress,
        greed_level: usize,
    ) -> bool {
        let adults = self.network_knowledge.adults().await;
        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let holder_breadth = data_copy_count() + greed_level;
        let holder_adult_list: BTreeSet<_> = adults_names
            .clone()
            .sorted_by(|lhs, rhs| data.name().cmp_distance(lhs, rhs))
            .take(holder_breadth)
            .collect();

        holder_adult_list.contains(node_name)
    }

    /// Given a set of known data, we can calculate what more from what we have a
    /// given node should be responsible for
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

        let data_chunked_per_holder_quantity =
            self.get_data_for_holder(data_i_have, sender).await?;

        if data_chunked_per_holder_quantity.is_empty() {
            trace!("We have no data for sender");
            return Ok(vec![]);
        }

        let mut cmds = vec![];
        // TODO: parallelise
        for (i, dataset) in data_chunked_per_holder_quantity.into_iter().enumerate() {
            let mut data_for_sender = vec![];

            for data in dataset {
                if data_sender_has.contains(&data) {
                    continue;
                }

                // Right now we just want data_copy but, this could be increased easily via
                // sender node eg.
                let greed_level = 0;
                let should_hold_data = self
                    .check_if_node_should_hold_data_in_section(&sender.name(), &data, 0)
                    .await;

                if should_hold_data {
                    debug!("Our requester should hold: {:?}", data);
                    let _existed = data_for_sender.push(data.clone());
                }
            }

            if data_for_sender.is_empty() {
                trace!("We have no data worth sending");
                continue;
            }
            debug!(
                "{:?} batch to: {:?} ",
                LogMarker::QueuingMissingReplicatedData,
                sender
            );

            cmds.push(Cmd::EnqueueDataForReplication {
                recipient: sender,
                data_batch: data_for_sender,
            });
        }

        Ok(cmds)
    }

    /// Will send a list of currently known/owned data to relevant nodes.
    /// These nodes should send back anything missing (in batches).
    /// Relevant nodes should be all _prior_ neighbours + _new_ elders.
    #[instrument(skip(self))]
    pub(crate) async fn ask_for_any_new_data(&self) -> Result<Vec<Cmd>> {
        debug!("Querying section for any new data");
        let data_i_have = self.data_storage.keys().await?;
        let mut cmds = vec![];

        let adults = self.network_knowledge.adults().await;
        let adults_names = adults.iter().map(|p2p_node| p2p_node.name()).collect_vec();

        let elders = self.network_knowledge.elders().await;
        let my_name = self.info.read().await.name();

        // find data targets that are not us.
        // TODO: handle edge case Within an edge case that for a section of prefix(), if our_name is: 0011xxx , other nodes are 0101xxx, 0101xxx, 0101xxx, 0100xxx, 0100xxx .
        // Then for a data of 0000xxx, the closest 3 nodes to the data will be 0011xxx, 0100xxx, 0100xxx .
        // But the closest 3 nodes to us will be 0101xxx, 0101xxx, 0101xxx, which missed that data.
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

        let section_pk = self.network_knowledge.section_key().await;

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
        if self.is_elder().await {
            return Ok(vec![]);
        }

        trace!("{:?}", LogMarker::DataReorganisationUnderway);

        self.ask_for_any_new_data().await
    }
}
