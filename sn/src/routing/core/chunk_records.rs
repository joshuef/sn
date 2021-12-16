// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{capacity::CHUNK_COPY_COUNT, Command, Core, Prefix, Result};
use crate::peer::Peer;
use crate::routing::log_markers::LogMarker;
use crate::routing::{error::convert_to_error_message, Error};
use crate::types::{Chunk, PublicKey};
use crate::{
    messaging::{
        data::{operation_id, ChunkDataExchange, CmdError, Error as ErrorMessage, StorageLevel},
        system::{NodeCmd, NodeQuery, SystemMsg},
        AuthorityProof, EndUser, MessageId, ServiceAuth,
    },
    types::ChunkAddress,
};
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use tracing::info;
use xor_name::XorName;
impl Core {
    pub(crate) fn get_copy_count(&self) -> usize {
        CHUNK_COPY_COUNT
    }

    pub(crate) async fn get_data_of(&self, prefix: &Prefix) -> ChunkDataExchange {
        // Prepare full_adult details
        let adult_levels = self.capacity.levels_matching(*prefix).await;
        ChunkDataExchange { adult_levels }
    }

    pub(crate) async fn update_chunks(&self, chunk_data: ChunkDataExchange) {
        let ChunkDataExchange { adult_levels } = chunk_data;
        self.capacity.set_adult_levels(adult_levels).await
    }

    /// Registered holders not present in provided list of members
    /// will be removed from adult_storage_info and no longer tracked for liveness.
    pub(crate) async fn retain_members_only(&self, members: BTreeSet<XorName>) -> Result<()> {
        // full adults
        self.capacity.retain_members_only(&members).await;

        // stop tracking liveness of absent holders
        self.liveness.retain_members_only(members);

        Ok(())
    }

    /// Set storage level of a given node.
    /// Returns whether the level changed or not.
    pub(crate) async fn set_storage_level(&self, node_id: &PublicKey, level: StorageLevel) -> bool {
        let node_name = XorName::from(*node_id);
        info!("Setting new storage level for {:?} to {:?}", node_name, level);
        let changed = self
            .capacity
            .set_adult_level(node_name, level)
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

    pub(super) async fn send_chunk_to_adults(
        &self,
        chunk: Chunk,
        msg_id: MessageId,
        auth: AuthorityProof<ServiceAuth>,
        origin: Peer,
    ) -> Result<Vec<Command>> {
        trace!("{:?}: {:?}", LogMarker::ChunkStoreReceivedAtElder, chunk);

        let target = *chunk.name();

        let msg = SystemMsg::NodeCmd(NodeCmd::StoreChunk {
            chunk,
            auth: auth.into_inner(),
            origin: EndUser(origin.name()),
        });

        let targets = self.get_adults_holding_chunk(&target).await;

        let aggregation = false;

        if self.get_copy_count() > targets.len() {
            let error = CmdError::Data(ErrorMessage::InsufficientAdults(
                self.network_knowledge().prefix().await,
            ));
            return self.send_cmd_error_response(error, origin, msg_id);
        }

        self.send_node_msg_to_targets(msg, targets, aggregation)
            .await
    }

    pub(crate) async fn send_error(
        &self,
        error: Error,
        msg_id: MessageId,
        origin: Peer,
    ) -> Result<Vec<Command>> {
        let error = convert_to_error_message(error);
        let error = CmdError::Data(error);

        self.send_cmd_error_response(error, origin, msg_id)
    }

    pub(super) async fn read_chunk_from_adults(
        &self,
        address: ChunkAddress,
        msg_id: MessageId,
        origin: Peer,
    ) -> Result<Vec<Command>> {
        let operation_id = operation_id(&address)?;
        trace!(
            "{:?} preparing to query adults for chunk at {:?} with op_id: {:?}",
            LogMarker::ChunkQueryReceviedAtElder,
            address,
            operation_id
        );

        let targets = self.get_adults_holding_chunk(address.name()).await;

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
                .add_a_pending_request_operation(target, operation_id.clone())
                .await;
            let _existed = fresh_targets.insert(target);
        }

        let correlation_id = XorName::random();
        let overwrote = self
            .pending_chunk_queries
            .set(correlation_id, origin, None)
            .await;
        if let Some(overwrote) = overwrote {
            // Since `XorName` is a 256 bit value, we consider the probability negligible, but warn
            // anyway so we're not totally lost if it does happen.
            warn!(
                "Overwrote an existing pending chunk query for {} from {} - what are the chances?",
                correlation_id, overwrote
            );
        }

        let msg = SystemMsg::NodeQuery(NodeQuery::GetChunk {
            address,
            origin: EndUser(correlation_id),
        });
        let aggregation = false;

        self.send_node_msg_to_targets(msg, fresh_targets, aggregation)
            .await
    }

    #[allow(clippy::mutable_key_type)]
    pub(crate) async fn reorganize_chunks(
        &self,
        our_name: XorName,
        new_adults: BTreeSet<XorName>,
        lost_adults: BTreeSet<XorName>,
        remaining: BTreeSet<XorName>,
    ) -> Result<Vec<Command>> {
        let chunks = self.chunk_storage.clone();
        let keys = chunks.keys()?;
        let mut data_for_replication = BTreeMap::new();
        for addr in keys.iter() {
            if let Some((data, holders)) = self
                .republish_and_cache(addr, &our_name, &new_adults, &lost_adults, &remaining)
                .await
            {
                let _prev = data_for_replication.insert(data, holders);
            }
        }

        let mut commands = vec![];
        let section_pk = self.network_knowledge.section_key().await;
        for (chunk, targets) in data_for_replication {
            for name in targets {
                commands.push(Command::PrepareNodeMsgToSend {
                    msg: SystemMsg::NodeCmd(NodeCmd::ReplicateChunk(chunk.clone())),
                    dst: crate::messaging::DstLocation::Node { name, section_pk },
                })
            }
        }

        Ok(commands)
    }

    async fn republish_and_cache(
        &self,
        address: &ChunkAddress,
        our_name: &XorName,
        new_adults: &BTreeSet<XorName>,
        lost_adults: &BTreeSet<XorName>,
        remaining: &BTreeSet<XorName>,
    ) -> Option<(Chunk, BTreeSet<XorName>)> {
        let chunks = self.chunk_storage.clone();

        let old_adult_list = remaining.union(lost_adults).copied().collect();
        let new_adult_list = remaining.union(new_adults).copied().collect();
        let new_holders = self.compute_holders(address, &new_adult_list);
        let old_holders = self.compute_holders(address, &old_adult_list);

        let we_are_not_holder_anymore = !new_holders.contains(our_name);
        let new_adult_is_holder = !new_holders.is_disjoint(new_adults);
        let lost_old_holder = !old_holders.is_disjoint(lost_adults);

        if we_are_not_holder_anymore || new_adult_is_holder || lost_old_holder {
            info!("Republishing chunk at {:?}", address);
            trace!("We are not a holder anymore? {}, New Adult is Holder? {}, Lost Adult was holder? {}", we_are_not_holder_anymore, new_adult_is_holder, lost_old_holder);
            let chunk = chunks.get_chunk(address).ok()?;
            if we_are_not_holder_anymore {
                if let Err(err) = chunks.remove_chunk(address) {
                    warn!("Error deleting chunk during republish: {:?}", err);
                }
            }
            // TODO: Push to LRU cache
            Some((chunk, new_holders))
        } else {
            None
        }
    }

    fn compute_holders(
        &self,
        addr: &ChunkAddress,
        adult_list: &BTreeSet<XorName>,
    ) -> BTreeSet<XorName> {
        adult_list
            .iter()
            .sorted_by(|lhs, rhs| addr.name().cmp_distance(lhs, rhs))
            .take(CHUNK_COPY_COUNT)
            .cloned()
            .collect()
    }
}
