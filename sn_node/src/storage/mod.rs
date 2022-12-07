// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod chunks;
mod errors;
mod register_store;
mod registers;
mod used_space;

pub use used_space::UsedSpace;

pub(crate) use errors::{Error, Result};

use chunks::ChunkStorage;
use registers::RegisterStorage;

use sn_dbc::SpentProofShare;
use sn_interface::{
    messaging::{
        data::{DataQueryVariant, Error as MessagingError, RegisterQuery, StorageThreshold},
        system::NodeQueryResponse,
    },
    types::{
        register::User, DataAddress, Keypair, PublicKey, RegisterAddress, ReplicatedData,
        SPENTBOOK_TYPE_TAG,
    },
};

use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use xor_name::XorName;

const BIT_TREE_DEPTH: usize = 20;

/// Operations on data stored to disk.
/// As data the storage struct may be cloned throughoout the node
/// Operations here must be persisted to disk.
#[derive(Debug, Clone)]
// exposed as pub due to benches
pub struct DataStorage {
    chunks: ChunkStorage,
    registers: RegisterStorage,
    used_space: UsedSpace,
    reported_threshold: Option<StorageThreshold>,
}

impl DataStorage {
    /// Set up a new `DataStorage` instance
    pub fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        Ok(Self {
            chunks: ChunkStorage::new(path, used_space.clone())?,
            registers: RegisterStorage::new(path, used_space.clone())?,
            used_space,
            reported_threshold: None,
        })
    }

    /// Returns whether the storage threshold has been reached or not.
    pub(crate) fn is_threshold_reached(&self) -> bool {
        self.reported_threshold.is_some()
    }

    /// Set the storage threshold reached flag
    /// (To avoid needing a write lock for general storage ops, we separate out this state operation)
    pub fn set_threshold_reached(&mut self, threshold: StorageThreshold) {
        if self.reported_threshold.is_none() {
            self.reported_threshold = Some(threshold);
        }
    }

    /// Clears the storage threshold reached flag
    /// (To avoid needing a write lock for general storage ops, we separate out this state operation)
    pub fn clear_threshold_reached(&mut self) {
        self.reported_threshold = None;
    }

    /// Store data in the local store
    #[instrument(skip(self))]
    pub async fn store(
        &self,
        data: &ReplicatedData,
        section_pk: PublicKey,
        node_keypair: Keypair,
    ) -> Result<Option<StorageThreshold>> {
        debug!("Replicating {data:?}");
        match data {
            ReplicatedData::Chunk(chunk) => self.chunks.store(chunk).await?,
            ReplicatedData::RegisterLog(data) => {
                info!("Updating register: {:?}", data.address);
                self.registers.update(data).await?
            }
            ReplicatedData::RegisterWrite(cmd) => self.registers.write(cmd).await?,
            ReplicatedData::SpentbookWrite(cmd) => {
                // FIMXE: this is temporary logic to have spentbooks as Registers.
                // Spentbooks shall always exist, and the section nodes shall create them by default.
                self.registers
                    .write_spentbook_register(cmd, section_pk, node_keypair)
                    .await?;
            }
            ReplicatedData::SpentbookLog(data) => self.registers.update(data).await?,
        };

        // check if we've reached the threshold of our storage
        // if so, we will set the reported threshold
        if self.reported_threshold.is_none()
            && 100 * self.used_space.ratio() as u8 >= StorageThreshold::THRESHOLD
        {
            debug!("Adult storage threshold level has been reached");
            return Ok(Some(StorageThreshold::new()));
        }

        Ok(None)
    }

    // Query the local store and return NodeQueryResponse
    pub(crate) async fn query(
        &self,
        query: &DataQueryVariant,
        requester: User,
    ) -> NodeQueryResponse {
        match query {
            DataQueryVariant::GetChunk(addr) => self.chunks.get(addr).await,
            DataQueryVariant::Register(read) => self.registers.read(read, requester).await,
            DataQueryVariant::Spentbook(read) => {
                // TODO: this is temporary till spentbook native data type is implemented,
                // we read from the Register where we store the spentbook data
                let reg_addr = RegisterAddress::new(read.dst_name(), SPENTBOOK_TYPE_TAG);

                match self
                    .registers
                    .read(&RegisterQuery::Get(reg_addr), requester)
                    .await
                {
                    NodeQueryResponse::GetRegister(Err(MessagingError::DataNotFound(_))) => {
                        NodeQueryResponse::SpentProofShares(Ok(Vec::new()))
                    }
                    NodeQueryResponse::GetRegister(result) => {
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

                        NodeQueryResponse::SpentProofShares(proof_shares_result)
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
        address: &DataAddress,
    ) -> Result<ReplicatedData> {
        match address {
            DataAddress::Bytes(addr) => {
                self.chunks.get_chunk(addr).await.map(ReplicatedData::Chunk)
            }
            DataAddress::Register(addr) => self
                .registers
                .get_register_replica(addr)
                .await
                .map(ReplicatedData::RegisterLog),
            DataAddress::Spentbook(addr) => {
                let reg_addr = RegisterAddress::new(*addr.name(), SPENTBOOK_TYPE_TAG);
                self.registers
                    .get_register_replica(&reg_addr)
                    .await
                    .map(ReplicatedData::SpentbookLog)
            }
            other => Err(Error::UnsupportedDataType(*other)),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn remove(&mut self, address: &DataAddress) -> Result<()> {
        match address {
            DataAddress::Bytes(addr) => self.chunks.remove_chunk(addr).await,
            DataAddress::Register(addr) => self.registers.remove_register(addr).await,
            DataAddress::Spentbook(addr) => {
                let reg_addr = RegisterAddress::new(*addr.name(), SPENTBOOK_TYPE_TAG);
                self.registers.remove_register(&reg_addr).await
            }
            other => Err(Error::UnsupportedDataType(*other)),
        }
    }

    /// Retrieve all ReplicatedDataAddresses of stored data
    pub async fn data_addrs(&self) -> Vec<DataAddress> {
        // TODO: Parallelize this below loops
        self.chunks
            .addrs()
            .into_iter()
            .map(DataAddress::Bytes)
            .chain(
                self.registers
                    .addrs()
                    .await
                    .into_iter()
                    .map(DataAddress::Register),
            )
            .collect()
    }
}

// Helper that returns the prefix tree path of depth BIT_TREE_DEPTH for a given xorname
// Example:
// - with a xorname with starting bits `010001110110....`
// - and a BIT_TREE_DEPTH of `6`
// returns the path `ROOT_PATH/0/1/0/0/0/1`
fn prefix_tree_path(root: &Path, xorname: XorName) -> PathBuf {
    let bin = format!("{:b}", xorname);
    let prefix_dir_path: PathBuf = bin.chars().take(BIT_TREE_DEPTH).map(String::from).collect();
    root.join(prefix_dir_path)
}

fn list_files_in(path: &Path) -> Vec<PathBuf> {
    if !path.exists() {
        return vec![];
    }

    WalkDir::new(path)
        .into_iter()
        .filter_map(|e| match e {
            Ok(direntry) => Some(direntry),
            Err(err) => {
                warn!("Store: failed to process filesystem entry: {}", err);
                None
            }
        })
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.path().to_path_buf())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DataStorage, Error, UsedSpace};
    use sn_interface::{
        init_logger,
        messaging::{
            data::{CreateRegister, DataQueryVariant, SignedRegisterCreate},
            system::NodeQueryResponse,
        },
        test_utils::TestKeys,
        types::{
            register::{Policy, User},
            utils::random_bytes,
            Chunk, ChunkAddress, DataAddress, Keypair, PublicKey, RegisterCmd, ReplicatedData,
            SectionSig,
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
        let mut storage = DataStorage::new(path, used_space)?;

        // 5mb random data chunk
        let bytes = random_bytes(5 * 1024 * 1024);
        let chunk = Chunk::new(bytes);
        let replicated_data = ReplicatedData::Chunk(chunk.clone());

        let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
        let keypair = Keypair::new_ed25519();

        // Store the chunk
        let _ = storage.store(&replicated_data, pk, keypair).await?;

        // Test local fetch
        let fetched_data = storage
            .get_from_local_store(&replicated_data.address())
            .await?;

        assert_eq!(replicated_data, fetched_data);

        // Test client fetch
        let query = DataQueryVariant::GetChunk(*chunk.address());
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

    #[tokio::test]
    async fn data_storage_chunk_keys_returned() -> Result<(), Error> {
        init_logger();
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
        let replicated_chunk = ReplicatedData::Chunk(chunk.clone());

        let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
        let keypair = Keypair::new_ed25519();

        // Store the chunk
        let _ = storage.store(&replicated_chunk, pk, keypair).await?;

        let keys = storage.data_addrs().await;

        let expected_key = replicated_chunk.address();
        assert!(
            keys.contains(&expected_key),
            "data storage does not contain our keys: {expected_key:?}"
        );

        Ok(())
    }

    fn section_sig() -> SectionSig {
        let sk = bls::SecretKey::random();
        TestKeys::get_section_sig_bytes(&sk, "hello".as_bytes())
    }

    #[tokio::test]
    async fn data_storage_register_keys_returned() -> Result<(), Error> {
        init_logger();
        // Generate temp path for storage
        // Cleaned up automatically after test completes
        let tmp_dir = tempdir()?;
        let path = tmp_dir.path();
        let used_space = UsedSpace::new(usize::MAX);

        // Create instance
        let storage = DataStorage::new(path, used_space)?;

        // create reg cmd

        let keypair = Keypair::new_ed25519();

        let name = xor_name::rand::random();
        let tag = 15000;
        let owner = User::Key(keypair.public_key());

        fn public_policy(owner: User) -> Policy {
            let permissions = BTreeMap::new();
            Policy { owner, permissions }
        }

        let policy = public_policy(owner);

        let op = CreateRegister { name, tag, policy };
        let signature = keypair.sign(&bincode::serialize(&op).expect("could not serialize op"));
        let section_auth = section_sig();
        let cmd = RegisterCmd::Create {
            cmd: SignedRegisterCreate {
                op,
                auth: sn_interface::messaging::ClientAuth {
                    public_key: keypair.public_key(),
                    signature,
                },
            },
            section_sig: section_auth.clone(), // obtained after presenting a valid payment to the network
        };

        // ReplicatedData::RegisterWrite(reg_cmd)

        let replicated_register = ReplicatedData::RegisterWrite(cmd);

        let pk = PublicKey::Bls(bls::SecretKey::random().public_key());
        let keypair = Keypair::new_ed25519();

        // Store the chunk
        let _ = storage.store(&replicated_register, pk, keypair).await?;

        let keys = storage.data_addrs().await;

        let expected_key = replicated_register.address();
        assert!(
            keys.contains(&expected_key),
            "data storage does not contain our keys: {expected_key:?}"
        );

        Ok(())
    }

    // Model-based testing where random sets of Operations are performed on the Storage module and
    // a hashmap. The behaviour of both the models should be identical.
    proptest! {
        #[test]
        #[allow(clippy::unwrap_used)]
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
        let mut storage = DataStorage::new(path, used_space)?;
        let owner_pk = PublicKey::Bls(bls::SecretKey::random().public_key());
        let owner_keypair = Keypair::new_ed25519();
        for op in ops {
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
                    runtime.block_on(async {
                        match storage.store(&data, owner_pk, owner_keypair.clone()).await {
                            Ok(_) => {
                                // do nothing
                                Ok(())
                            }
                            Err(Error::DataExists(_)) => {
                                // also do nothing
                                Ok(())
                            }
                            Err(other_error) => Err(other_error),
                        }
                    })?;
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
                    let addr = ChunkAddress(key);
                    let query = DataQueryVariant::GetChunk(addr);
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
                                return Err(Error::DataExists(DataAddress::Bytes(addr)));
                            }
                        }
                    }
                }
                Op::Get(idx) => {
                    let key = get_xor_name(&model, idx % (model.len() + 1));
                    let addr = DataAddress::Bytes(ChunkAddress(key));
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
                                return Err(Error::DataExists(addr));
                            }
                        }
                    }
                }
                Op::Remove(idx) => {
                    let key = get_xor_name(&model, idx % (model.len() + 1));
                    let addr = DataAddress::Bytes(ChunkAddress(key));

                    let storage_res = runtime.block_on(storage.remove(&addr));
                    match model.remove(&key) {
                        Some(_) => storage_res?,
                        None => {
                            if storage_res.is_ok() {
                                return Err(Error::DataExists(addr));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
