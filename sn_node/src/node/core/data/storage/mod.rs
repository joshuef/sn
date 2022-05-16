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
#[derive(Clone, Debug)]
// exposed as pub due to benches
pub struct DataStorage {
    chunks: ChunkStorage,
    registers: RegisterStorage,
    used_space: UsedSpace,
    last_recorded_level: Arc<RwLock<StorageLevel>>,
}

impl DataStorage {
    /// Set up a new DataStorage instance
    pub fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        Ok(Self {
            chunks: ChunkStorage::new(path, used_space.clone())?,
            registers: RegisterStorage::new(path, used_space.clone())?,
            used_space,
            last_recorded_level: Arc::new(RwLock::new(StorageLevel::zero())),
        })
    }

    /// Store data in the local store
    #[instrument(skip(self))]
    pub async fn store(&self, data: &ReplicatedData) -> Result<Option<StorageLevel>> {
        debug!("Replicating {data:?}");
        match data.clone() {
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
            DataQuery::GetChunk(addr) => self.chunks.get(addr).await,
            DataQuery::Register(read) => self.registers.read(read, requester).await,
        }
    }

    /// --- System calls ---

    // Read data from local store
    pub(crate) async fn get_from_local_store(
        &self,
        address: &DataAddress,
    ) -> Result<ReplicatedData> {
        match address {
            DataAddress::Chunk(addr) => {
                self.chunks.get_chunk(addr).await.map(ReplicatedData::Chunk)
            }
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
            DataAddress::Chunk(addr) => self.chunks.remove_chunk(addr).await,
            DataAddress::Register(addr) => self.registers.remove_register(addr).await,
        }
    }

    /// Retrieve all keys/ReplicatedDataAddresses of stored data
    pub async fn keys(&self) -> Result<Vec<DataAddress>> {
        let chunk_keys = self.chunks.keys()?.into_iter().map(DataAddress::Chunk);
        let reg_keys = self
            .registers
            .keys()
            .await?
            .into_iter()
            .map(DataAddress::Register);
        Ok(reg_keys.chain(chunk_keys).collect())
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
    pub(crate) async fn reorganize_data(
        &self,
        new_adults: BTreeSet<XorName>,
        _lost_adults: BTreeSet<XorName>,
        remaining: BTreeSet<XorName>,
    ) -> Result<Vec<Cmd>, crate::node::Error> {
        let data = self.data_storage.clone();
        let keys = data.keys().await?;

        trace!("We have {:?} pieces of data stored locally", keys.len());
        let mut data_for_replication = BTreeMap::new();
        for addr in keys.iter() {
            if let Some((data, holders)) = self
                .get_data_and_nodes_responsible(addr, &new_adults, &remaining)
                .await
            {
                let _prev = data_for_replication.insert(data.address(), (data, holders));
            }
        }

        let mut cmds = vec![];
        let section_pk = self.network_knowledge.section_key().await;
        let mut send_list: BTreeMap<XorName, Vec<ReplicatedDataAddress>> = BTreeMap::new();

        trace!("data_for_replication: {:?}", data_for_replication.len());
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
            trace!("Sending replicated data list to: {:?}", target);
            cmds.push(Cmd::SignOutgoingSystemMsg {
                msg: SystemMsg::NodeCmd(
                    NodeCmd::EnsureReplicationOfDataAddress(data_addresses).clone(),
                ),
                dst: DstLocation::Node {
                    name: target,
                    section_pk,
                },
            })
        }

        Ok(cmds)
    }

    // on adults, find out who else is supposed to be responsible for any of the data,
    // this can be used to send a message indicating all stored data we have that may be of interest
    async fn get_data_and_nodes_responsible(
        &self,
        address: &DataAddress,
        new_adults: &BTreeSet<XorName>,
        // lost_adults: &BTreeSet<XorName>,
        remaining: &BTreeSet<XorName>,
    ) -> Option<(ReplicatedData, BTreeSet<XorName>)> {
        let storage = self.data_storage.clone();

        // let old_adult_list = remaining.union(lost_adults).copied().collect();
        let new_adult_list = remaining.union(new_adults).copied().collect();
        // let new_holders = self.compute_holders(address, &new_adult_list);

        // debug!("New holders len: {:?}", new_holders.len());
        // let old_holders = self.compute_holders(address, &old_adult_list);

        // let new_adult_is_holder = !new_holders.is_disjoint(new_adults);
        // let lost_old_holder = !old_holders.is_disjoint(lost_adults);

        // if new_adult_is_holder || lost_old_holder {
        info!("Informing nodes we have data at {:?}", address);
        // trace!(
        //     "New Adult is Holder? {}, Lost Adult was holder? {}",
        //     new_adult_is_holder,
        //     lost_old_holder
        // );
        let data = match storage.get_from_local_store(address).await {
            Ok(data) => {
                // info!("Data found for replication: {address:?}");
                Ok(data)
            }
            Err(error) => {
                warn!("Error finding {address:?} for replication: {error:?}");
                Err(error)
            }
        }
        .ok()?;

        Some((data, new_adult_list))
        // } else {
        //     None
        // }
    }
}

#[cfg(test)]
mod tests {
    use crate::dbs::Error;
    use crate::node::core::data::DataStorage;
    use crate::UsedSpace;
    use eyre::Result;
    use proptest::{
        collection::SizeRange,
        prelude::{any, prop_oneof, proptest},
        strategy::Strategy,
    };
    use sn_interface::messaging::data::DataQuery;
    use sn_interface::messaging::system::NodeQueryResponse;
    use sn_interface::types::register::User;
    use sn_interface::types::utils::random_bytes;
    use sn_interface::types::{Chunk, ChunkAddress, ReplicatedData, ReplicatedDataAddress};
    use std::cmp::max;
    use std::collections::BTreeMap;
    use std::{thread, time::Duration};
    use tempfile::tempdir;
    use tokio::runtime::Runtime;
    use xor_name::XorName;

    const MAX_N_OPS: usize = 100;
    const CHUNK_MIN: usize = 1;
    const CHUNK_MAX: usize = 5;

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

    // Model-based testing where random sets of Operations are performed on the Storage module and
    // a hashmap. The behaviour of both the models should be identical.
    proptest! {
        #[test]
        fn model_based_test(ops in arbitrary_ops(0..MAX_N_OPS)){
            model_based_test_imp(ops).unwrap();
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Store(usize, usize),
        Query(usize),
        Get(usize),
        Remove(usize),
    }

    fn arbitrary_single_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<usize>(), CHUNK_MIN..CHUNK_MAX)
                .prop_map(|(flag, chunk_size)| Op::Store(flag, chunk_size)),
            any::<usize>().prop_map(Op::Query),
            any::<usize>().prop_map(Op::Get),
            any::<usize>().prop_map(Op::Remove),
        ]
    }
    fn arbitrary_ops(count: impl Into<SizeRange>) -> impl Strategy<Value = Vec<Op>> {
        proptest::collection::vec(arbitrary_single_op(), count)
    }

    fn get_xor_name(model: &BTreeMap<XorName, ReplicatedData>, idx: usize) -> XorName {
        match model.iter().nth(idx) {
            Some((xor_name, _)) => *xor_name,
            None => xor_name::rand::random(),
        }
    }

    fn model_based_test_imp(ops: Vec<Op>) -> Result<(), Error> {
        let mut model: BTreeMap<XorName, ReplicatedData> = BTreeMap::new();
        let temp_dir = tempdir()?;
        let path = temp_dir.path();
        let used_space = UsedSpace::new(usize::MAX);
        let runtime = Runtime::new()?;
        let storage = DataStorage::new(path, used_space)?;
        for op in ops.into_iter() {
            match op {
                Op::Store(flag, chunk_size) => {
                    let data = match flag.rem_euclid(2) {
                        // get bytes from hashmap
                        0 => {
                            let size = max(model.len(), 1);
                            match model.get(&get_xor_name(&model, flag % size)) {
                                Some(data) => data.clone(),
                                // when hashmap is empty, xor_name is random, get random_bytes
                                None => {
                                    let chunk = Chunk::new(random_bytes(chunk_size * 1024 * 1024));
                                    ReplicatedData::Chunk(chunk)
                                }
                            }
                        }
                        // random bytes
                        _ => {
                            let chunk = Chunk::new(random_bytes(chunk_size * 1024 * 1024));
                            ReplicatedData::Chunk(chunk)
                        }
                    };
                    let _ = runtime.block_on(storage.store(&data))?;
                    // If Get/Query is performed just after a Store op, half-written data is returned
                    // Adding some delay fixes it
                    thread::sleep(Duration::from_millis(15));

                    if let ReplicatedData::Chunk(chunk) = &data {
                        let _ = model.insert(*chunk.name(), data);
                    }
                }
                Op::Query(idx) => {
                    // +1 for a chance to get random xor_name
                    let key = get_xor_name(&model, idx % (model.len() + 1));
                    let query = DataQuery::GetChunk(ChunkAddress(key));
                    let user = User::Anyone;
                    let stored_res = runtime.block_on(storage.query(&query, user));
                    let model_res = model.get(&key);

                    match model_res {
                        Some(m_res) => {
                            if let NodeQueryResponse::GetChunk(Ok(s_chunk)) = stored_res {
                                if let ReplicatedData::Chunk(m_chunk) = m_res {
                                    assert_eq!(*m_chunk, s_chunk);
                                }
                            } else {
                                return Err(Error::ChunkNotFound(key));
                            }
                        }
                        None => {
                            if let NodeQueryResponse::GetChunk(Ok(_)) = stored_res {
                                return Err(Error::DataExists);
                            }
                        }
                    }
                }
                Op::Get(idx) => {
                    let key = get_xor_name(&model, idx % (model.len() + 1));
                    let addr = ReplicatedDataAddress::Chunk(ChunkAddress(key));
                    let stored_data = runtime.block_on(storage.get_from_local_store(&addr));
                    let model_data = model.get(&key);

                    match model_data {
                        Some(m_data) => {
                            if let Ok(s_data) = stored_data {
                                assert_eq!(*m_data, s_data);
                            } else {
                                return Err(Error::ChunkNotFound(key));
                            }
                        }
                        None => {
                            if stored_data.is_ok() {
                                return Err(Error::DataExists);
                            }
                        }
                    }
                }
                Op::Remove(idx) => {
                    let key = get_xor_name(&model, idx % (model.len() + 1));
                    let addr = ReplicatedDataAddress::Chunk(ChunkAddress(key));

                    let storage_res = runtime.block_on(storage.remove(&addr));
                    match model.remove(&key) {
                        Some(_) => {
                            if let Err(err) = storage_res {
                                return Err(err);
                            }
                        }
                        None => {
                            if storage_res.is_ok() {
                                return Err(Error::DataExists);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
