// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod chunks;
mod registers;

use crate::{
    dbs::Result,
    node::core::{Cmd, Node},
    UsedSpace,
};

use sn_interface::messaging::{
    data::{DataQuery, RegisterStoreExport, StorageLevel},
    system::{NodeCmd, NodeQueryResponse, SystemMsg},
    DstLocation,
};
use sn_interface::types::{register::User, ReplicatedData, ReplicatedDataAddress as DataAddress};

pub(crate) use chunks::ChunkStorage;
pub(crate) use registers::RegisterStorage;

use sn_interface::types::ReplicatedDataAddress;
use std::collections::btree_map::Entry;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::info;
use xor_name::XorName;

/// Operations on data.
#[derive(Clone)]
pub(crate) struct DataStorage {
    #[cfg(feature="chunks")]
    chunks: ChunkStorage,
    registers: RegisterStorage,
    used_space: UsedSpace,
    last_recorded_level: Arc<RwLock<StorageLevel>>,
}

impl DataStorage {
    pub(crate) fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        Ok(Self {
            #[cfg(feature="chunks")]
            chunks: ChunkStorage::new(path, used_space.clone())?,
            registers: RegisterStorage::new(path, used_space.clone())?,
            used_space,
            last_recorded_level: Arc::new(RwLock::new(StorageLevel::zero())),
        })
    }

    /// Store data in the local store
    #[instrument(skip(self))]
    pub(crate) async fn store(&self, data: &ReplicatedData) -> Result<Option<StorageLevel>> {
        debug!("Replicating {data:?}");
        match data.clone() {
            #[cfg(feature="chunks")]
            ReplicatedData::Chunk(chunk) => self.chunks.store(&chunk).await?,
            ReplicatedData::RegisterLog(data) => {
                self.registers
                    .update(RegisterStoreExport(vec![data]))
                    .await?
            }
            ReplicatedData::RegisterWrite(cmd) => self.registers.write(cmd).await?,
        };

        // check if we've filled another approx. 10%-points of our storage
        // if so, update the recorded level
        let last_recorded_level = { *self.last_recorded_level.read().await };
        if let Ok(next_level) = last_recorded_level.next() {
            // used_space_ratio is a heavy task that's why we don't do it all the time
            let used_space_ratio = self.used_space.ratio();
            let used_space_level = 10.0 * used_space_ratio;
            // every level represents 10 percentage points
            if used_space_level as u8 >= next_level.value() {
                debug!("Next level for storage has been reached");
                *self.last_recorded_level.write().await = next_level;
                return Ok(Some(next_level));
            }
        }

        Ok(None)
    }

    // Query the local store and return NodeQueryResponse
    pub(crate) async fn query(&self, query: &DataQuery, requester: User) -> NodeQueryResponse {
        match query {
            #[cfg(feature="chunks")]
            DataQuery::GetChunk(addr) => self.chunks.get(addr).await,
            #[cfg(feature="registers")]
            DataQuery::Register(read) => self.registers.read(read, requester).await,
        }
    }

    /// --- System calls ---

    // Read data from local store
    #[cfg(any(feature="chunks", feature="registers"))]
    pub(crate) async fn get_from_local_store(
        &self,
        address: &DataAddress,
    ) -> Result<ReplicatedData> {

        match address {
            #[cfg(feature="chunks")]
            DataAddress::Chunk(addr) => {
                self.chunks.get_chunk(addr).await.map(ReplicatedData::Chunk)
            }
            #[cfg(feature="registers")]
            DataAddress::Register(addr) => self
                .registers
                .get_register_replica(addr)
                .await
                .map(ReplicatedData::RegisterLog),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn remove(&self, address: &DataAddress) -> Result<()> {
        match address {
            #[cfg(feature="chunks")]
            DataAddress::Chunk(addr) => self.chunks.remove_chunk(addr).await,
            #[cfg(feature="registers")]
            DataAddress::Register(addr) => self.registers.remove_register(addr).await,
        }
    }

    async fn keys(&self) -> Result<Vec<DataAddress>> {
        let mut keys = vec![];
        #[cfg(feature="chunks")]
        {
            let chunk_keys: Vec<_> = self.chunks.keys()?.into_iter().map(DataAddress::Chunk).collect();
            keys.extend(chunk_keys)
            // keys = keys.chain(chunk_keys).into_iter();
        }

        #[cfg(feature="registers")]
        {
            let reg_keys: Vec<_> = self
                .registers
                .keys()
                .await?
                .into_iter()
                .map(DataAddress::Register).collect();
                keys.extend(reg_keys)

            // keys = keys.chain(reg_keys).into_iter();
        }
        Ok(keys)
    }

    pub(crate) async fn get_for_replication(
        &self,
        data_address: ReplicatedDataAddress,
    ) -> Result<ReplicatedData> {
        self.get_from_local_store(&data_address).await
    }
}

impl Node {
    #[allow(clippy::mutable_key_type)]
    #[cfg(any(feature = "chunks", feature = "registers"))]
    pub(crate) async fn reorganize_data(
        &self,
        new_adults: BTreeSet<XorName>,
        lost_adults: BTreeSet<XorName>,
        remaining: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>, crate::node::Error> {
        let data = self.data_storage.clone();
        let keys = data.keys().await?;
        let mut data_for_replication = BTreeMap::new();
        for addr in keys.iter() {
            if let Some((data, holders)) = self
                .get_replica_targets(addr, &new_adults, &lost_adults, &remaining)
                .await
            {
                let _prev = data_for_replication.insert(data.address(), (data, holders));
            }
        }

        let mut cmds = vec![];
        let section_pk = self.network_knowledge.section_key().await;
        let mut send_list: BTreeMap<XorName, Vec<ReplicatedDataAddress>> = BTreeMap::new();

        for (data_address, (_data, targets)) in data_for_replication {
            for target in targets {
                let entry = send_list.entry(target);
                match entry {
                    Entry::Occupied(mut present_entries) => {
                        let addresses = present_entries.get_mut();
                        addresses.push(data_address);
                    }
                    Entry::Vacant(e) => {
                        let _ = e.insert(vec![data_address]);
                    }
                }
            }
        }

        for (target, data_addresses) in send_list.into_iter() {
            cmds.push(Cmd::SignOutgoingSystemMsg {
                msg: SystemMsg::NodeCmd(NodeCmd::SendReplicateDataAddress(data_addresses).clone()),
                dst: DstLocation::Node {
                    name: target,
                    section_pk,
                },
            })
        }

        Ok(cmds)
    }

    // on adults
    #[cfg(any(feature = "chunks", feature = "registers"))]
    async fn get_replica_targets(
        &self,
        address: &DataAddress,
        new_adults: &BTreeSet<XorName>,
        lost_adults: &BTreeSet<XorName>,
        remaining: &BTreeSet<XorName>,
    ) -> Option<(ReplicatedData, BTreeSet<XorName>)> {
        let storage = self.data_storage.clone();

        let old_adult_list = remaining.union(lost_adults).copied().collect();
        let new_adult_list = remaining.union(new_adults).copied().collect();
        let new_holders = self.compute_holders(address, &new_adult_list);

        debug!("New holders len: {:?}", new_holders.len());
        let old_holders = self.compute_holders(address, &old_adult_list);

        let new_adult_is_holder = !new_holders.is_disjoint(new_adults);
        let lost_old_holder = !old_holders.is_disjoint(lost_adults);

        if new_adult_is_holder || lost_old_holder {
            info!("Replicating data at {:?}", address);
            trace!(
                "New Adult is Holder? {}, Lost Adult was holder? {}",
                new_adult_is_holder,
                lost_old_holder
            );
            let data = match storage.get_from_local_store(address).await {
                Ok(data) => {
                    info!("Data found for replication: {address:?}");
                    Ok(data)
                }
                Err(error) => {
                    warn!("Error finding {address:?} for replication: {error:?}");
                    Err(error)
                }
            }
            .ok()?;

            Some((data, new_holders))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dbs::Error;
    use crate::node::core::data::DataStorage;
    use crate::UsedSpace;
    use sn_interface::messaging::data::DataQuery;
    use sn_interface::messaging::system::NodeQueryResponse;
    use sn_interface::types::register::User;
    use sn_interface::types::utils::random_bytes;
    use sn_interface::types::{Chunk, ReplicatedData};
    use tempfile::tempdir;

    #[tokio::test]
    async fn data_storage_basics() -> Result<(), Error> {
        // Generate temp path for storage
        // Cleaned up automatically after test completes
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path();
        let used_space = UsedSpace::new(usize::MAX);

        // Create instance
        let storage = DataStorage::new(path, used_space)?;

        // 5mb random data chunk
        let bytes = random_bytes(5 * 1024 * 1024);
        let chunk = Chunk::new(bytes);
        let replicated_data = ReplicatedData::Chunk(chunk.clone());

        // Store the chunk
        let _ = storage.store(&replicated_data).await?;

        // Test local fetch
        let fetched_data = storage
            .get_from_local_store(&replicated_data.address())
            .await?;

        assert_eq!(replicated_data, fetched_data);

        // Test client fetch
        let query = DataQuery::GetChunk(*chunk.address());
        let user = User::Anyone;

        let query_response = storage.query(&query, user).await;

        assert_eq!(query_response, NodeQueryResponse::GetChunk(Ok(chunk)));

        // Remove from storage
        storage.remove(&replicated_data.address()).await?;

        // Assert data is not found after storage
        match storage
            .get_from_local_store(&replicated_data.address())
            .await
        {
            Err(Error::ChunkNotFound(address)) => assert_eq!(address, replicated_data.name()),
            _ => panic!("Unexpected data found"),
        }

        Ok(())
    }
}
