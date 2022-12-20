// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, MyNode};

use sn_interface::{
    data_copy_count,
    messaging::system::{NodeDataCmd, NodeMsg},
    types::{log_markers::LogMarker, DataAddress, Peer},
};

use itertools::Itertools;
use std::collections::BTreeSet;

impl MyNode {
    /// Given what data the peer has, we shall calculate what data the peer is missing that
    /// we have, and send such data to the peer.
    #[instrument(skip(context, data_sender_has))]
    pub(crate) async fn get_missing_data_for_node(
        context: &NodeContext,
        sender: Peer,
        data_sender_has: Vec<DataAddress>,
    ) -> Option<Cmd> {
        trace!("Getting missing data for node");
        // Collection of data addresses that we do not have

        // TODO: can cache this data stored per churn event?
        let data_i_have = context.data_storage.data_addrs().await;
        trace!("Our data got");

        if data_i_have.is_empty() {
            trace!("We have no data");
            return None;
        }

        let adults = context.network_knowledge.adults();
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
                data_for_sender.push(data);
            }
        }

        if data_for_sender.is_empty() {
            trace!("We have no data worth sending");
            return None;
        }

        debug!(
            "{:?} batching data of len {:?} to: {:?} ",
            LogMarker::QueuingMissingReplicatedData,
            data_for_sender.len(),
            sender
        );

        Some(Cmd::EnqueueDataForReplication {
            recipient: sender,
            data_batch: data_for_sender,
        })
    }

    /// Will send a list of currently known/owned data to relevant nodes.
    /// These nodes should send back anything missing (in batches).
    /// Relevant nodes should be all _prior_ neighbours + _new_ elders.
    #[instrument(skip(context))]
    pub(crate) async fn ask_for_any_new_data(context: &NodeContext) -> Cmd {
        trace!("{:?}", LogMarker::DataReorganisationUnderway);
        debug!("Querying section for any new data");
        let data_i_have = context.data_storage.data_addrs().await;

        let my_name = context.name;
        let adults = context.network_knowledge.adults();
        let elders = context.network_knowledge.elders();

        // find data targets that are not us.
        let mut target_members = adults
            .into_iter()
            .sorted_by(|lhs, rhs| my_name.cmp_distance(&lhs.name(), &rhs.name()))
            .filter(|peer| peer.name() != my_name)
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "nearest neighbours for data req: {}: {:?}",
            target_members.len(),
            target_members
        );

        // also send to our elders in case they are holding but were just promoted
        for elder in elders {
            let _existed = target_members.insert(elder);
        }

        if target_members.is_empty() {
            warn!("We have no peers to ask for data!");
        } else {
            trace!("Sending our data list to: {:?}", target_members);
        }

        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
        MyNode::send_system_msg(msg, Peers::Multiple(target_members), context.clone())
    }
}
