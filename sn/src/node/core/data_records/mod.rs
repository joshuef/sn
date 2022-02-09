// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Cmd, Core, Prefix};

use crate::{
    data_copy_count,
    messaging::{
        data::{CmdError, DataQuery, MetadataExchange, StorageLevel},
        system::{NodeCmd, NodeQuery, SystemMsg},
        AuthorityProof, EndUser, MsgId, ServiceAuth,
    },
    node::{error::convert_to_error_msg, Error, Result},
    peer::Peer,
    types::{log_markers::LogMarker, PublicKey, ReplicatedData, ReplicatedDataAddress},
};

use itertools::Itertools;
use std::collections::BTreeSet;
use tracing::info;
use xor_name::XorName;

impl Core {
    // Locate ideal holders for this data, line up wiremsgs for those to instruct them to store the data
    pub(crate) async fn replicate_data(&self, data: ReplicatedData) -> Result<Vec<Cmd>> {
        trace!("{:?}: {:?}", LogMarker::DataStoreReceivedAtElder, data);
        if self.is_elder().await {
            let targets = self.get_adults_who_should_store_data(data.name()).await;

            let msg = SystemMsg::NodeCmd(NodeCmd::ReplicateData(data));
            self.send_node_msg_to_nodes(msg, targets, false).await
        } else {
            Err(Error::InvalidState)
        }
    }

    pub(crate) async fn read_data_from_adults(
        &self,
        query: DataQuery,
        msg_id: MsgId,
        auth: AuthorityProof<ServiceAuth>,
        origin: Peer,
    ) -> Result<Vec<Cmd>> {
        let address = query.address();
        let operation_id = query.operation_id()?;
        trace!(
            "{:?} preparing to query adults for data at {:?} with op_id: {:?}",
            LogMarker::DataQueryReceviedAtElder,
            address,
            operation_id
        );

        let targets = self.get_adults_holding_data(address.name()).await;

        if targets.is_empty() {
            return self
                .send_error(
                    Error::NoAdults(self.network_knowledge().prefix().await),
                    msg_id,
                    origin,
                )
                .await;
        }

        let mut fresh_targets = BTreeSet::new();
        for target in targets {
            self.liveness
                .add_a_pending_request_operation(target, operation_id)
                .await;
            let _existed = fresh_targets.insert(target);
        }

        let overwrote = self
            .pending_data_queries
            .set(operation_id, origin.clone(), None)
            .await;
        if let Some(overwrote) = overwrote {
            // Since `XorName` is a 256 bit value, we consider the probability negligible, but warn
            // anyway so we're not totally lost if it does happen.
            warn!(
                "Overwrote an existing pending data query for {:?} from {} - what are the chances?",
                operation_id, overwrote
            );
        }

        let msg = SystemMsg::NodeQuery(NodeQuery::Data {
            query,
            auth: auth.into_inner(),
            origin: EndUser(origin.name()),
            correlation_id: MsgId::from_xor_name(*address.name()),
        });
        let aggregation = false;

        self.send_node_msg_to_nodes(msg, fresh_targets, aggregation)
            .await
    }

    pub(crate) async fn get_metadata_of(&self, prefix: &Prefix) -> MetadataExchange {
        // Load tracked adult_levels
        let adult_levels = self.capacity.levels_matching(*prefix).await;
        MetadataExchange { adult_levels }
    }

    pub(crate) async fn set_adult_levels(&self, adult_levels: MetadataExchange) {
        let MetadataExchange { adult_levels } = adult_levels;
        self.capacity.set_adult_levels(adult_levels).await
    }

    /// Registered holders not present in provided list of members
    /// will be removed from adult_storage_info and no longer tracked for liveness.
    pub(crate) async fn liveness_retain_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        // full adults
        self.capacity.retain_members_only(&members).await;

        // stop tracking liveness of absent holders
        self.liveness.retain_members_only(members);

        Ok(())
    }

    /// Adds the new adult to the Capacity and Liveness trackers.
    pub(crate) async fn add_new_adult_to_trackers(&self, adult: XorName) {
        info!("Adding new Adult: {adult} to trackers");
        self.capacity.add_new_adult(adult).await;

        self.liveness.add_new_adult(adult);
    }

    /// Set storage level of a given node.
    /// Returns whether the level changed or not.
    pub(crate) async fn set_storage_level(&self, node_id: &PublicKey, level: StorageLevel) -> bool {
        info!("Setting new storage level..");
        let changed = self
            .capacity
            .set_adult_level(XorName::from(*node_id), level)
            .await;
        let avg_usage = self.capacity.avg_usage().await;
        info!(
            "Avg storage usage among Adults is between {}-{} %",
            avg_usage * 10,
            (avg_usage + 1) * 10
        );
        changed
    }

    pub(crate) async fn full_adults(&self) -> BTreeSet<XorName> {
        self.capacity.full_adults().await
    }

    pub(crate) async fn send_error(
        &self,
        error: Error,
        msg_id: MsgId,
        origin: Peer,
    ) -> Result<Vec<Cmd>> {
        let error = convert_to_error_msg(error);
        let error = CmdError::Data(error);

        self.send_cmd_error_response(error, origin, msg_id).await
    }

    pub(crate) fn compute_holders(
        &self,
        addr: &ReplicatedDataAddress,
        adult_list: &BTreeSet<XorName>,
    ) -> BTreeSet<XorName> {
        adult_list
            .iter()
            .sorted_by(|lhs, rhs| addr.name().cmp_distance(lhs, rhs))
            .take(data_copy_count())
            .cloned()
            .collect()
    }
}
