// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod chunks;
mod registers;

pub(crate) use chunks::ChunkStorage;
pub(crate) use registers::RegisterStorage;

use crate::{dbs::Result, UsedSpace};

use sn_dbc::SpentProofShare;
use sn_interface::{
    messaging::{
        data::{DataQuery, Error, RegisterQuery, RegisterStoreExport, StorageLevel},
        system::NodeQueryResponse,
    },
    types::{
        register::User, RegisterAddress, ReplicatedData, ReplicatedDataAddress, SPENTBOOK_TYPE_TAG,
    },
};

use std::{path::Path, sync::Arc};
use tokio::sync::RwLock;

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
                self.registers.update(RegisterStoreExport(vec![data]))?
            }
            ReplicatedData::RegisterWrite(cmd) => self.registers.write(cmd)?,
            ReplicatedData::SpentbookWrite(cmd) => {
                // FIMXE: this is temporay logic to create a spentbook to make sure it exists.
                // Spentbooks shall always exist, and the section nodes shall create them by default.
                self.registers
                    .create_spentbook_register(&cmd.dst_address())?;

                // We now write the cmd received
                self.registers.write(cmd)?
            }
            ReplicatedData::SpentbookLog(data) => {
                self.registers.update(RegisterStoreExport(vec![data]))?
            }
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
            DataQuery::Register(read) => self.registers.read(read, requester),
            DataQuery::Spentbook(read) => {
                // TODO: this is temporary till spentbook native data type is implemented,
                // we read from the Register where we store the spentbook data
                let spentbook_op_id = match read.operation_id() {
                    Ok(id) => id,
                    Err(_e) => {
                        return NodeQueryResponse::FailedToCreateOperationId;
                    }
                };

                let reg_addr = RegisterAddress::new(read.dst_name(), SPENTBOOK_TYPE_TAG);

                match self
                    .registers
                    .read(&RegisterQuery::Get(reg_addr), requester)
                {
                    NodeQueryResponse::GetRegister((Err(Error::DataNotFound(_)), _)) => {
                        NodeQueryResponse::SpentProofShares((Ok(Vec::new()), spentbook_op_id))
                    }
                    NodeQueryResponse::GetRegister((result, _)) => {
                        let proof_shares_result = result.map(|reg| {
                            let mut proof_shares = Vec::new();
                            let entries = reg.read();
                            for (_, entry) in entries {
                                // Deserialise spent proof share from the entry
                                let spent_proof_share: SpentProofShare = match rmp_serde::from_slice(&entry) {
                                    Ok(proof) => proof,
                                    Err(err) => {
                                        warn!("Ignoring entry found in Spentbook since it cannot be deserialised as a valid SpentProofShare: {:?}", err);
                                        continue;
                                    }
                                };

                                proof_shares.push(spent_proof_share);
                            }
                            proof_shares
                        });

                        NodeQueryResponse::SpentProofShares((proof_shares_result, spentbook_op_id))
                    }
                    other => {
                        // TODO: this is temporary till spentbook native data type is implemented,
                        // for now we just return the response even that it's a Register query response.
                        other
                    }
                }
            }
        }
    }

    /// --- System calls ---

    // Read data from local store
    pub(crate) async fn get_from_local_store(
        &self,
        address: &ReplicatedDataAddress,
    ) -> Result<ReplicatedData> {
        match address {
            ReplicatedDataAddress::Chunk(addr) => {
                self.chunks.get_chunk(addr).await.map(ReplicatedData::Chunk)
            }
            ReplicatedDataAddress::Register(addr) => self
                .registers
                .get_register_replica(addr)
                .map(ReplicatedData::RegisterLog),
            ReplicatedDataAddress::Spentbook(addr) => {
                let reg_addr = RegisterAddress::new(*addr.name(), SPENTBOOK_TYPE_TAG);
                self.registers
                    .get_register_replica(&reg_addr)
                    .map(ReplicatedData::SpentbookLog)
            }
        }
    }

    /// Retrieve all keys/ReplicatedDataAddresses of stored data
    pub fn keys(&self) -> Result<Vec<ReplicatedDataAddress>> {
        let chunk_keys = self
            .chunks
            .keys()?
            .into_iter()
            .map(ReplicatedDataAddress::Chunk);
        let reg_keys = self
            .registers
            .keys()?
            .into_iter()
            .map(ReplicatedDataAddress::Register);
        Ok(reg_keys.chain(chunk_keys).collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::dbs::Error;
    use crate::node::core::data::DataStorage;
    use crate::UsedSpace;

    use sn_interface::{
        messaging::{data::DataQuery, system::NodeQueryResponse},
        types::{
            register::User, utils::random_bytes, Chunk, ChunkAddress, ReplicatedData,
            ReplicatedDataAddress,
        },
    };

    use eyre::Result;
    use proptest::{
        collection::SizeRange,
        prelude::{any, prop_oneof, proptest},
        strategy::Strategy,
    };
    use std::{cmp::max, collections::BTreeMap, thread, time::Duration};
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
