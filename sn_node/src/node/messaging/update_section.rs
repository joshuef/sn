// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{core::NodeContext, flow_ctrl::cmds::Cmd, messaging::Peers, MyNode};
use rand::{rngs::OsRng, seq::SliceRandom};
use sn_interface::{
    data_copy_count,
    messaging::system::{NodeDataCmd, NodeMsg},
    types::{log_markers::LogMarker, DataAddress, Peer},
};

use itertools::Itertools;
use std::collections::BTreeSet;

// This data will all be replicated into one message, as such we want to keep size
// low to ensure that serialisation sin't too heavy
static MAX_REPLICATION_BATCH_SIZE: usize = 25;

impl MyNode {
    /// Given what data the peer has, we shall calculate what data the peer is missing that
    /// we have, and send such data to the peer.
    #[instrument(skip(context, data_sender_has))]
    pub(crate) async fn get_missing_data_for_node(
        context: &NodeContext,
        sender: Peer,
        data_sender_has: Vec<DataAddress>,
    ) -> Option<Cmd> {
        debug!("Getting missing data for node");
        // TODO: can cache this data stored per churn event?
        let mut data_i_have = context.data_storage.data_addrs().await;
        trace!("Our data got");

        if data_i_have.is_empty() {
            trace!("We have no data");
            return None;
        }
        // To make each data storage node reply with different copies, so that the
        // overall queries can be reduced, the data names are scrambled.
        data_i_have.shuffle(&mut OsRng);

        let members = context.network_knowledge.members();
        let members_names = members.iter().map(|p2p_node| p2p_node.name());

        let mut data_for_sender = vec![];
        for data in data_i_have {
            if data_sender_has.contains(&data) {
                continue;
            }

            let holder_list: BTreeSet<_> = members_names
                .clone()
                .sorted_by(|lhs, rhs| data.name().cmp_distance(lhs, rhs))
                .take(data_copy_count())
                .collect();

            if holder_list.contains(&sender.name()) {
                debug!(
                    "{:?} batch data {:?} to: {:?} ",
                    LogMarker::QueuingMissingReplicatedData,
                    data,
                    sender
                );
                data_for_sender.push(data);
                // To avoid bundle too many data into one response,
                // Only reply with MAX_REPLICATION_BATCH_SIZE data for each query round,
                if data_for_sender.len() == MAX_REPLICATION_BATCH_SIZE {
                    break;
                }
            }
        }

        if data_for_sender.is_empty() {
            trace!("We have no data worth sending");
            return None;
        }

        Some(Cmd::EnqueueDataForReplication {
            recipient: sender,
            data_batch: data_for_sender,
        })
    }

    /// Will send a list of currently known/owned data to relevant nodes.
    /// These nodes should send back anything missing (in batches).
    /// Relevant nodes should be all _prior_ neighbours + _new_ elders.
    #[instrument(skip(context))]
    pub(crate) async fn ask_for_any_new_data_from_whole_section(context: &NodeContext) -> Cmd {
        trace!("{:?}", LogMarker::DataReorganisationUnderway);
        debug!("Querying section for any new data");
        let data_i_have = context.data_storage.data_addrs().await;

        // ask the entire section.
        // they will only send over relevant things they have, in small + randomized batches
        let members = context.network_knowledge.members();

        trace!(
            "section neighbours for data req: {}: {:?}",
            members.len(),
            members
        );

        if members.is_empty() {
            warn!("We have no peers to ask for data!");
        } else {
            trace!("Sending our data list to: {:?}", members);
        }

        let msg = NodeMsg::NodeDataCmd(NodeDataCmd::SendAnyMissingRelevantData(data_i_have));
        Cmd::send_msg(msg, Peers::Multiple(members))
    }
}
