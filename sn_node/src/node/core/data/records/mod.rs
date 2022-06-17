// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod capacity;

pub(crate) use self::capacity::{Capacity, MIN_LEVEL_WHEN_FULL};

use crate::node::{
    core::{Cmd, Node, Prefix, MAX_WAITING_PEERS_PER_QUERY},
    error::convert_to_error_msg,
    messages::WireMsgUtils,
    Error, Result,
};

use sn_dysfunction::IssueType;
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{CmdError, DataQuery, MetadataExchange, StorageLevel},
        system::{NodeCmd, NodeQuery, SystemMsg},
        AuthorityProof, DstLocation, EndUser, MsgId, ServiceAuth, WireMsg,
    },
    types::{log_markers::LogMarker, Peer, PublicKey, ReplicatedData},
};

use dashmap::DashSet;
use itertools::Itertools;
use std::{cmp::Ordering, collections::BTreeSet, rc::Rc};
use tracing::info;
use xor_name::XorName;

impl Node {
    // Locate ideal holders for this data, line up wiremsgs for those to instruct them to store the data
    pub(crate) async fn replicate_data(&self, data: ReplicatedData) -> Result<Vec<Cmd>> {
        trace!("{:?}: {:?}", LogMarker::DataStoreReceivedAtElder, data);
        if self.is_elder() {
            let targets = self.get_adults_who_should_store_data(data.name()).await;

            info!(
                "Replicating data {:?} to holders {:?}",
                data.name(),
                &targets,
            );

            let msg = SystemMsg::NodeCmd(NodeCmd::ReplicateData(vec![data]));
            self.send_node_msg_to_nodes(msg, targets).await
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

        let targets = self
            .get_adults_holding_data_including_full(address.name())
            .await;

        if targets.is_empty() {
            let error = convert_to_error_msg(Error::NoAdults(self.network_knowledge().prefix()));

            debug!("No targets found for {msg_id:?}");
            return self
                .send_cmd_error_response(CmdError::Data(error), origin, msg_id)
                .await;
        }

        let mut op_was_already_underway = false;
        let waiting_peers = if let Some(peers) = self.pending_data_queries.get(&operation_id).await
        {
            op_was_already_underway = peers.insert(origin);

            peers
        } else {
            let peers = DashSet::new();
            let _false_as_fresh = peers.insert(origin);
            Rc::new(peers)
        };

        // drop if we exceed
        if waiting_peers.len() > MAX_WAITING_PEERS_PER_QUERY {
            warn!("Dropping query from {origin:?}, there are more than {MAX_WAITING_PEERS_PER_QUERY} waiting already");
            return Ok(vec![]);
        }

        // only set pending data query cache if non existed.
        // otherwise we've appended to the Peers above
        // we rely on the data query cache timeout to decide as/when we'll be re-sending a query to adults
        if !op_was_already_underway {
            // ensure we only add a pending request when we're actually sending out requests.
            for target in &targets {
                trace!("adding pending req for {target:?} in dysfunction tracking");
                self.dysfunction_tracking
                    .track_issue(
                        *target,
                        IssueType::PendingRequestOperation(Some(operation_id)),
                    )
                    .await?;
            }

            trace!(
                "Adding to pending data queries for op id: {:?}",
                operation_id
            );

            let _prior_value = self
                .pending_data_queries
                .set(operation_id, waiting_peers, None)
                .await;

            let msg = SystemMsg::NodeQuery(NodeQuery::Data {
                query,
                auth: auth.into_inner(),
                origin: EndUser(origin.name()),
                correlation_id: MsgId::from_xor_name(*address.name()),
            });

            self.send_node_msg_to_nodes(msg, targets).await
        } else {
            // we don't do anything as we're still within data query timeout
            Ok(vec![])
        }
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
        let _ = self.dysfunction_tracking.retain_members_only(members).await;

        Ok(())
    }

    /// Adds the new adult to the Capacity and Liveness trackers.
    pub(crate) async fn add_new_adult_to_trackers(&self, adult: XorName) {
        info!("Adding new Adult: {adult} to trackers");
        self.capacity.add_new_adult(adult).await;

        let _ = self.dysfunction_tracking.add_new_node(adult).await;
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

    /// Used to fetch the list of holders for given data name. Includes full nodes
    async fn get_adults_holding_data_including_full(&self, target: &XorName) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults();

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let mut candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "Chunk holders of {:?} are empty adults: {:?} and full adults: {:?}",
            target,
            candidates,
            full_adults
        );

        // Full adults that are close to the chunk, shall still be considered as candidates
        // to allow chunks stored to empty adults can be queried when nodes become full.
        let close_full_adults = if let Some(closest_empty) = candidates.iter().next() {
            full_adults
                .iter()
                .filter_map(|name| {
                    if target.cmp_distance(name, closest_empty) == Ordering::Less {
                        Some(*name)
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>()
        } else {
            // In case there is no empty candidates, query all full_adults
            full_adults
        };

        candidates.extend(close_full_adults);
        candidates
    }

    /// Used to fetch the list of holders for given name of data. Excludes full nodes
    async fn get_adults_who_should_store_data(&self, target: XorName) -> BTreeSet<XorName> {
        let full_adults = self.full_adults().await;
        // TODO: reuse our_adults_sorted_by_distance_to API when core is merged into upper layer
        let adults = self.network_knowledge().adults();

        trace!("Total adults known about: {:?}", adults.len());

        let adults_names = adults.iter().map(|p2p_node| p2p_node.name());

        let candidates = adults_names
            .into_iter()
            .sorted_by(|lhs, rhs| target.cmp_distance(lhs, rhs))
            .filter(|peer| !full_adults.contains(peer))
            .take(data_copy_count())
            .collect::<BTreeSet<_>>();

        trace!(
            "Target holders of {:?} are empty adults: {:?} and full adults that were ignored: {:?}",
            target,
            candidates,
            full_adults
        );

        candidates
    }

    // Takes a message for specified targets, and builds internal send cmds
    // for sending to each of the targets.
    // Targets are XorName specified so must be within the section
    async fn send_node_msg_to_nodes(
        &self,
        msg: SystemMsg,
        targets: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>> {
        // we create a dummy/random dst location,
        // we will set it correctly for each msg and target
        let section_pk = self.network_knowledge().section_key();
        let our_name = self.info.borrow().name();
        let dummy_dst_location = DstLocation::Node {
            name: our_name,
            section_pk,
        };

        // separate this into form_wire_msg based on agg
        let wire_msg = WireMsg::single_src(
            &self.info.borrow().clone(),
            dummy_dst_location,
            msg,
            section_pk,
        )?;

        let mut cmds = vec![];

        for target in targets {
            debug!("Sending {:?} to {:?}", wire_msg, target);
            let mut wire_msg = wire_msg.clone();
            let dst_section_pk = self.section_key_by_name(&target);
            wire_msg.set_dst_section_pk(dst_section_pk);
            wire_msg.set_dst_xorname(target);

            cmds.extend(self.send_msg_to_nodes(wire_msg).await?);
        }

        Ok(cmds)
    }
}
