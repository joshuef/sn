// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    data::{encrypt_large, to_chunk, LargeFile, SmallFile},
    Client,
};
use crate::client::{client_api::data::DataMapLevel, utils::encryption, Error, Result};
use sn_interface::messaging::data::{DataCmd, DataQuery, QueryResponse};
use sn_interface::types::{BytesAddress, Chunk, ChunkAddress, Encryption, PublicKey, Scope};

use bincode::deserialize;
use bytes::Bytes;
use futures::future::join_all;
use itertools::Itertools;
use self_encryption::{self, ChunkInfo, DataMap, EncryptedChunk};
use tokio::task;
use tracing::trace;
use xor_name::XorName;

struct HeadChunk {
    chunk: Chunk,
    address: BytesAddress,
}

impl Client {
    #[instrument(skip(self), level = "debug")]
    /// Reads [`Bytes`] from the network, whose contents are contained within on or more chunks.
    pub async fn read_bytes(&self, address: BytesAddress) -> Result<Bytes> {
        let chunk = self.get_chunk(address.name()).await?;

        // first try to deserialize a LargeFile, if it works, we go and seek it
        if let Ok(data_map) = self
            .unpack_head_chunk(HeadChunk {
                chunk: chunk.clone(),
                address,
            })
            .await
        {
            self.read_all(data_map).await
        } else {
            // if an error occurs, we assume it's a SmallFile
            self.get_bytes(chunk, address.scope())
        }
    }

    /// Read bytes from the network. The contents are spread across
    /// multiple chunks in the network. This function invokes the self-encryptor and returns
    /// the data that was initially stored.
    ///
    /// Takes `position` and `length` arguments which specify the start position
    /// and the length of bytes to be read.
    /// Passing `0` to position reads the data from the beginning,
    /// and the `length` is just an upper limit.
    ///
    /// # Examples
    ///
    /// TODO: update once data types are crdt compliant
    ///
    #[instrument(skip_all, level = "trace")]
    pub async fn read_from(
        &self,
        address: BytesAddress,
        position: usize,
        length: usize,
    ) -> Result<Bytes>
    where
        Self: Sized,
    {
        trace!(
            "Reading {:?} bytes at: {:?}, starting from position: {:?}",
            &length,
            &address,
            &position,
        );

        let chunk = self.get_chunk(address.name()).await?;

        // First try to deserialize a LargeFile, if it works, we go and seek it.
        // If an error occurs, we consider it to be a SmallFile.
        if let Ok(data_map) = self
            .unpack_head_chunk(HeadChunk {
                chunk: chunk.clone(),
                address,
            })
            .await
        {
            return self.seek(data_map, position, length).await;
        }

        // The error above is ignored to avoid leaking the storage format detail of SmallFiles and LargeFiles.
        // The basic idea is that we're trying to deserialize as one, and then the other.
        // The cost of it is that some errors will not be seen without a refactor.
        let mut bytes = self.get_bytes(chunk, address.scope())?;

        let _ = bytes.split_to(position);
        bytes.truncate(length);

        Ok(bytes)
    }

    #[instrument(skip(self), level = "trace")]
    pub(crate) async fn get_chunk(&self, name: &XorName) -> Result<Chunk> {
        // first check it's not already in our Chunks' cache
        if let Some(chunk) = self
            .chunks_cache
            .write()
            .await
            .find(|c| c.address().name() == name)
        {
            trace!("Chunk retrieved from local cache: {:?}", name);
            return Ok(chunk.clone());
        }

        let res = self
            .send_query(DataQuery::GetChunk(ChunkAddress(*name)))
            .await?;

        let operation_id = res.operation_id;
        let chunk: Chunk = match res.response {
            QueryResponse::GetChunk(result) => {
                result.map_err(|err| Error::from((err, operation_id)))
            }
            response => return Err(Error::UnexpectedQueryResponse(response)),
        }?;

        let _ = self.chunks_cache.write().await.insert(chunk.clone());

        Ok(chunk)
    }

    /// Tries to chunk the bytes, returning an address and chunks, without storing anything to network.
    #[instrument(skip_all, level = "trace")]
    pub fn chunk_bytes(&self, bytes: Bytes, scope: Scope) -> Result<(BytesAddress, Vec<Chunk>)> {
        if let Ok(file) = LargeFile::new(bytes.clone()) {
            Self::encrypt_large(file, scope, self.public_key())
        } else {
            let file = SmallFile::new(bytes)?;
            let (address, chunk) = Self::package_small(file, scope, self.public_key())?;
            Ok((address, vec![chunk]))
        }
    }

    /// Encrypts a [`LargeFile`] and returns the resulting address and all chunks.
    /// Does not store anything to the network.
    #[instrument(skip(file), level = "trace")]
    fn encrypt_large(
        file: LargeFile,
        scope: Scope,
        public_key: PublicKey,
    ) -> Result<(BytesAddress, Vec<Chunk>)> {
        let owner = encryption(scope, public_key);
        encrypt_large(file.bytes(), owner.as_ref())
    }

    /// Packages a [`SmallFile`] and returns the resulting address and the chunk.
    /// The chunk content will be in plain text if it has public scope, or encrypted if it is instead private.
    /// Does not store anything to the network.
    fn package_small(
        file: SmallFile,
        scope: Scope,
        public_key: PublicKey,
    ) -> Result<(BytesAddress, Chunk)> {
        let encryption = encryption(scope, public_key);
        let chunk = to_chunk(file.bytes(), encryption.as_ref())?;
        if chunk.value().len() >= self_encryption::MIN_ENCRYPTABLE_BYTES {
            return Err(Error::SmallFilePaddingNeeded);
        }
        let name = *chunk.name();
        let address = if encryption.is_some() {
            BytesAddress::Private(name)
        } else {
            BytesAddress::Public(name)
        };
        Ok((address, chunk))
    }

    /// Directly writes [`Bytes`] to the network in the
    /// form of immutable chunks, without any batching.
    #[instrument(skip(self, bytes), level = "debug")]
    pub async fn upload(&self, bytes: Bytes, scope: Scope) -> Result<BytesAddress> {
        if let Ok(file) = LargeFile::new(bytes.clone()) {
            self.upload_large(file, scope).await
        } else {
            let file = SmallFile::new(bytes)?;
            self.upload_small(file, scope).await
        }
    }

    /// Directly writes [`Bytes`] to the network in the
    /// form of immutable chunks, without any batching.
    /// It also attempts to verify that all the data was uploaded to the network before returning.
    #[instrument(skip_all, level = "trace")]
    pub async fn upload_and_verify(
        &self,
        bytes: Bytes,
        scope: Scope,
    ) -> Result<(BytesAddress, Bytes)> {
        let address = self.upload(bytes, scope).await?;

        // let's now try to retrieve it
        let bytes = self.read_bytes(address).await?;

        Ok((address, bytes))
    }

    /// Calculates a LargeFile's/SmallFile's address from self encrypted chunks,
    /// without storing them onto the network.
    #[instrument(skip(bytes), level = "debug")]
    pub fn calculate_address(bytes: Bytes, scope: Scope) -> Result<BytesAddress> {
        // we use just a random BLS public key as the owner
        let public_key = PublicKey::Bls(bls::SecretKey::random().public_key());
        if let Ok(file) = LargeFile::new(bytes.clone()) {
            let (head_address, _all_chunks) = Self::encrypt_large(file, scope, public_key)?;
            Ok(head_address)
        } else {
            let file = SmallFile::new(bytes)?;
            let (address, _chunk) = Self::package_small(file, scope, public_key)?;
            Ok(address)
        }
    }

    /// Directly writes a [`LargeFile`] to the network in the
    /// form of immutable self encrypted chunks, without any batching.
    #[instrument(skip_all, level = "trace")]
    async fn upload_large(&self, large: LargeFile, scope: Scope) -> Result<BytesAddress> {
        let (head_address, all_chunks) = Self::encrypt_large(large, scope, self.public_key())?;

        let tasks = all_chunks.into_iter().map(|chunk| {
            let writer = self.clone();
            task::spawn(async move { writer.send_cmd(DataCmd::StoreChunk(chunk)).await })
        });

        let respones = join_all(tasks)
            .await
            .into_iter()
            .flatten() // swallows errors
            .collect_vec();

        for res in respones {
            // fail with any issue here
            let _ = res?;
        }

        Ok(head_address)
    }

    /// Directly writes a [`SmallFile`] to the network in the
    /// form of a single chunk, without any batching.
    #[instrument(skip_all, level = "trace")]
    async fn upload_small(&self, small: SmallFile, scope: Scope) -> Result<BytesAddress> {
        let (address, chunk) = Self::package_small(small, scope, self.public_key())?;
        self.send_cmd(DataCmd::StoreChunk(chunk)).await?;
        Ok(address)
    }

    // --------------------------------------------
    // ---------- Private helpers -----------------
    // --------------------------------------------

    // Gets and decrypts chunks from the network using nothing else but the data map,
    // then returns the raw data.
    async fn read_all(&self, data_map: DataMap) -> Result<Bytes> {
        let encrypted_chunks = Self::try_get_chunks(self, data_map.infos()).await?;
        let bytes = self_encryption::decrypt_full_set(&data_map, &encrypted_chunks)?;
        Ok(bytes)
    }

    // Gets a subset of chunks from the network, decrypts and
    // reads `len` bytes of the data starting at given `pos` of original file.
    #[instrument(skip_all, level = "trace")]
    async fn seek(&self, data_map: DataMap, pos: usize, len: usize) -> Result<Bytes> {
        let info = self_encryption::seek_info(data_map.file_size(), pos, len);
        let range = &info.index_range;
        let all_infos = data_map.infos();

        let encrypted_chunks = Self::try_get_chunks(
            self,
            (range.start..range.end + 1)
                .clone()
                .map(|i| all_infos[i].clone())
                .collect_vec(),
        )
        .await?;

        let bytes =
            self_encryption::decrypt_range(&data_map, &encrypted_chunks, info.relative_pos, len)?;

        Ok(bytes)
    }

    #[instrument(skip_all, level = "trace")]
    async fn try_get_chunks(
        client: &Client,
        chunks_info: Vec<ChunkInfo>,
    ) -> Result<Vec<EncryptedChunk>> {
        let expected_count = chunks_info.len();

        let tasks = chunks_info.into_iter().map(|chunk_info| {
            let client = client.clone();
            task::spawn(async move {
                match client.get_chunk(&chunk_info.dst_hash).await {
                    Ok(chunk) => Ok(EncryptedChunk {
                        index: chunk_info.index,
                        content: chunk.value().clone(),
                    }),
                    Err(err) => {
                        warn!(
                            "Reading chunk {} from network, resulted in error {:?}.",
                            chunk_info.dst_hash, err
                        );
                        Err(err)
                    }
                }
            })
        });

        // This swallowing of errors
        // is basically a compaction into a single
        // error saying "didn't get all chunks".
        let encrypted_chunks = join_all(tasks)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect_vec();

        if expected_count > encrypted_chunks.len() {
            Err(Error::NotEnoughChunksRetrieved {
                expected: expected_count,
                retrieved: encrypted_chunks.len(),
            })
        } else {
            Ok(encrypted_chunks)
        }
    }

    /// Extracts a file DataMapLevel from a head chunk.
    /// If the DataMapLevel is not the first level mapping directly to the user's contents,
    /// the process repeats itself until it obtains the first level DataMapLevel.
    #[instrument(skip_all, level = "trace")]
    async fn unpack_head_chunk(&self, chunk: HeadChunk) -> Result<DataMap> {
        let HeadChunk { mut chunk, address } = chunk;
        loop {
            let bytes = self.get_bytes(chunk, address.scope())?;

            match deserialize(&bytes)? {
                DataMapLevel::First(data_map) => {
                    return Ok(data_map);
                }
                DataMapLevel::Additional(data_map) => {
                    let serialized_chunk = self.read_all(data_map).await?;
                    chunk = deserialize(&serialized_chunk)?;
                }
            }
        }
    }

    /// If scope == Scope::Private, decrypts contents with the client encryption keys.
    /// Else returns the content bytes.
    #[instrument(skip_all, level = "trace")]
    fn get_bytes(&self, chunk: Chunk, scope: Scope) -> Result<Bytes> {
        if matches!(scope, Scope::Public) {
            Ok(chunk.value().clone())
        } else {
            let owner = encryption(scope, self.public_key()).ok_or(Error::NoEncryptionObject)?;
            Ok(owner.decrypt(chunk.value().clone())?)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::utils::test_utils::create_test_client_with;
    use crate::client::{
        client_api::file_apis::LargeFile,
        utils::test_utils::{create_test_client, init_test_logger},
        Client,
    };
    use sn_interface::types::log_markers::LogMarker;
    use sn_interface::types::{utils::random_bytes, BytesAddress, Keypair, Scope};

    use bytes::Bytes;
    use eyre::Result;
    use futures::future::join_all;
    use rand::rngs::OsRng;
    use std::time::Duration;
    use tokio::time::Instant;
    use tracing::{instrument::Instrumented, Instrument};

    const LARGE_FILE_SIZE_MIN: usize = self_encryption::MIN_ENCRYPTABLE_BYTES;

    #[test]
    fn deterministic_chunking() -> Result<()> {
        init_test_logger();
        let keypair = Keypair::new_ed25519(&mut OsRng);
        let file = random_bytes(LARGE_FILE_SIZE_MIN);

        use crate::client::client_api::data::encrypt_large;
        use crate::client::utils::encryption;
        let owner = encryption(Scope::Private, keypair.public_key());
        let (first_address, mut first_chunks) = encrypt_large(file.clone(), owner.as_ref())?;

        first_chunks.sort();

        for _ in 0..100 {
            let owner = encryption(Scope::Private, keypair.public_key());
            let (head_address, mut all_chunks) = encrypt_large(file.clone(), owner.as_ref())?;
            assert_eq!(first_address, head_address);
            all_chunks.sort();
            assert_eq!(first_chunks, all_chunks);
        }

        Ok(())
    }

    // Test storing and reading min sized LargeFile.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_3kb() -> Result<()> {
        init_test_logger();
        let _start_span = tracing::info_span!("store_and_read_3kb").entered();

        let client = create_test_client().await?;

        let file = LargeFile::new(random_bytes(LARGE_FILE_SIZE_MIN))?;

        // Store private file (also verifies that the file is stored)
        let (private_address, read_data) = client
            .upload_and_verify(file.bytes(), Scope::Private)
            .await?;

        compare(file.bytes(), read_data)?;

        // Test storing private file with the same value.
        // Should not conflict and return same address
        let address = client
            .upload_large(file.clone(), Scope::Private)
            .instrument(tracing::info_span!(
                "checking no conflict on same private upload"
            ))
            .await?;
        assert_eq!(address, private_address);

        // Test storing public file with the same value. Should not conflict.
        let public_address = client
            .upload_large(file.clone(), Scope::Public)
            .instrument(tracing::info_span!("checking no conflict on public upload"))
            .await?;

        assert_ne!(public_address, private_address);

        // Assert that the public file is stored.
        let read_data = client
            .read_bytes(public_address)
            .instrument(tracing::info_span!("reading_public"))
            .await?;

        compare(file.bytes(), read_data)?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seek_with_unknown_length() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("seek_with_unknown_length").entered();
        let client = create_test_client().await?;

        // create content which is stored as LargeFile, i.e. its size is larger than LARGE_FILE_SIZE_MIN
        let size = 2 * LARGE_FILE_SIZE_MIN;
        let file = LargeFile::new(random_bytes(size))?;

        let (address, _) = client
            .upload_and_verify(file.bytes(), Scope::Public)
            .await?;

        let pos = 512;
        let read_data = read_from_pos(&file, address, pos, usize::MAX, &client).await?;

        assert_eq!(read_data.len(), size - pos);
        compare(file.bytes().split_off(pos), read_data)?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seek_in_data() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("seek_in_data").entered();
        let client = create_test_client().await?;

        for i in 1..5 {
            let size = i * LARGE_FILE_SIZE_MIN;
            let _outer_span = tracing::info_span!("size:", size).entered();
            for divisor in 2..5 {
                let _outer_span = tracing::info_span!("divisor", divisor).entered();
                let len = size / divisor;
                let file = LargeFile::new(random_bytes(size))?;

                let (address, _) = client
                    .upload_and_verify(file.bytes(), Scope::Public)
                    .await?;

                // Read first part
                let read_data_1 = {
                    let pos = 0;
                    read_from_pos(&file, address, pos, len, &client).await?
                };

                // Read second part
                let read_data_2 = {
                    let pos = len;
                    read_from_pos(&file, address, pos, len, &client).await?
                };

                // Join parts
                let read_data: Bytes = [read_data_1, read_data_2]
                    .iter()
                    .flat_map(|bytes| bytes.clone())
                    .collect();

                compare(file.bytes().slice(0..(2 * len)), read_data)?;
            }
        }

        Ok(())
    }

    // Test storing and reading 5mb file. Try and read from many clients and ensure we do not overwelm nodes.
    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_5mb_from_many_clients() -> Result<()> {
        init_test_logger();
        let _start_span = tracing::info_span!("store_and_read_5mb_from_many_clients").entered();

        let uploader = create_test_client().await?;
        // create file with random bytes 5mb
        let bytes = random_bytes(5 * 1024 * 1024);
        let file = LargeFile::new(bytes)?;

        // Store file (also verifies that the file is stored)
        let (address, read_data) = uploader
            .upload_and_verify(file.bytes(), Scope::Public)
            .await?;

        compare(file.bytes(), read_data)?;

        debug!("======> Data uploaded");

        let reader_count = 25;
        let clients = create_clients(reader_count).await;
        assert_eq!(reader_count, clients.len());

        let mut tasks = vec![];

        for client in clients {
            let handle: Instrumented<tokio::task::JoinHandle<Result<()>>> =
                tokio::spawn(async move {
                    let mut last_try = true;
                    // get the data with many retries
                    for i in 0..250 {
                        match client.read_bytes(address).await {
                            Ok(_data) => {
                                last_try = false;
                                break;
                            }
                            Err(_) => {
                                debug!(
                                    "client #{:?} failed the data, sleeping..",
                                    client.public_key()
                                );
                                tokio::time::sleep(Duration::from_millis(i * 100)).await;
                            }
                        };
                    }
                    if last_try {
                        let _ = client.read_bytes(address).await?;
                    }

                    debug!("client #{:?} got the data", client.public_key());

                    Ok(())
                })
                .in_current_span();

            tasks.push(handle);
        }

        let results = join_all(tasks).await;

        for res in results {
            let _ok = res??;
        }

        // TODO: we need to use the node log analysis to check the mem usage across nodes does not exceed X
        Ok(())
    }

    async fn create_clients(count: usize) -> Vec<Client> {
        let mut tasks = vec![];

        for i in 0..count {
            debug!("starting client on thread #{:?}", i);
            let handle: Instrumented<tokio::task::JoinHandle<Result<Client>>> =
                tokio::spawn(async move {
                    debug!("starting client #{:?}..", i);
                    // use a fresh client
                    let client = create_test_client().await?;
                    debug!("client #{:?} started", i);
                    Ok(client)
                })
                .in_current_span();
            tasks.push(handle);
        }

        join_all(tasks)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Testnet network_assert_ tests should be excluded from normal tests runs, they need to be run in sequence to ensure validity of checks"]
    async fn file_network_assert_expected_log_counts() -> Result<()> {
        init_test_logger();

        let _outer_span = tracing::info_span!("file_network_assert").entered();

        let network_assert_delay: u64 = std::env::var("NETWORK_ASSERT_DELAY")
            .unwrap_or_else(|_| "3".to_string())
            .parse()?;

        let delay = tokio::time::Duration::from_secs(network_assert_delay);
        debug!("Running network asserts with delay of {:?}", delay);

        let bytes = random_bytes(LARGE_FILE_SIZE_MIN / 3);
        let client = create_test_client().await?;

        let mut the_logs = crate::testnet_grep::NetworkLogState::new()?;

        let _address = client
            .upload_and_verify(bytes.clone(), Scope::Public)
            .await?;

        // small delay to ensure all node's logs have written
        tokio::time::sleep(delay).await;

        // 3 elders were chosen by the client (should only be 3 as even if client chooses adults, AE should kick in prior to them attempting any of this)
        the_logs
            .assert_count(LogMarker::DataStoreReceivedAtElder, 3)
            .await?;

        // 4 adults * reqs from 3 elders storing the chunk
        the_logs.assert_count(LogMarker::StoringChunk, 12).await?;

        // Here we can see that each write thinks it's new, so there's 12... but we let Sled handle this later.
        // 4 adults storing the chunk * 3 messages, so we'll still see this due to the rapid/ concurrent nature here...
        the_logs.assert_count(LogMarker::StoredNewChunk, 12).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_1kb() -> Result<()> {
        init_test_logger();
        let size = LARGE_FILE_SIZE_MIN / 3;
        let _outer_span = tracing::info_span!("store_and_read_1kb", size).entered();
        let client = create_test_client().await?;
        store_and_read(&client, size, Scope::Public).await?;
        store_and_read(&client, size, Scope::Private).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_1mb() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("store_and_read_1mb").entered();
        let client = create_test_client().await?;
        store_and_read(&client, 1024 * 1024, Scope::Public).await?;
        store_and_read(&client, 1024 * 1024, Scope::Private).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ae_checks_file_test() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("ae_checks_file_test").entered();
        let client = create_test_client_with(None, None, false).await?;
        store_and_read(&client, 10 * 1024 * 1024, Scope::Private).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_10mb() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("store_and_read_10mb").entered();
        let client = create_test_client().await?;
        store_and_read(&client, 10 * 1024 * 1024, Scope::Private).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_read_20mb() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("store_and_read_20mb").entered();
        let client = create_test_client().await?;
        store_and_read(&client, 20 * 1024 * 1024, Scope::Private).await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "too heavy for CI"]
    async fn store_and_read_40mb() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("store_and_read_40mb").entered();
        let client = create_test_client().await?;
        store_and_read(&client, 40 * 1024 * 1024, Scope::Private).await
    }

    // Essentially a load test, seeing how much parallel batting the nodes can take.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "too heavy for CI"]
    async fn parallel_timings() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("parallel_timings").entered();

        let client = create_test_client().await?;

        let handles = (0..1000_usize)
            .map(|i| (i, client.clone()))
            .map(|(i, client)| {
                tokio::spawn(async move {
                    let file = LargeFile::new(random_bytes(LARGE_FILE_SIZE_MIN))?;
                    let _ = client.upload_large(file, Scope::Public).await?;
                    println!("Iter: {}", i);
                    let res: Result<()> = Ok(());
                    res
                })
            });

        let results = join_all(handles).await;

        for res1 in results {
            if let Ok(res2) = res1 {
                if res2.is_err() {
                    println!("Error: {:?}", res2);
                }
            } else {
                println!("Error: {:?}", res1);
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "too heavy for CI"]
    async fn one_by_one_timings() -> Result<()> {
        init_test_logger();
        let _outer_span = tracing::info_span!("test__one_by_one_timings").entered();

        let client = create_test_client().await?;

        for i in 0..1000_usize {
            let file = LargeFile::new(random_bytes(LARGE_FILE_SIZE_MIN))?;
            let now = Instant::now();
            let _ = client.upload_large(file, Scope::Public).await?;
            let elapsed = now.elapsed();
            println!("Iter: {}, in {} millis", i, elapsed.as_millis());
        }

        Ok(())
    }

    async fn store_and_read(client: &Client, size: usize, scope: Scope) -> Result<()> {
        // cannot use scope as var w/ macro
        let _outer_span = if scope == Scope::Public {
            tracing::info_span!("store_and_read_public_bytes", size).entered()
        } else {
            tracing::info_span!("store_and_read_private_bytes", size).entered()
        };

        // random bytes of requested size
        let bytes = random_bytes(size);

        // we'll also test we can calculate address offline using `calculate_address` API
        let expected_address = Client::calculate_address(bytes.clone(), scope)?;

        // we use upload_and_verify since it uploads and also confirms it was uploaded
        let (address, read_data) = client.upload_and_verify(bytes.clone(), scope).await?;
        assert_eq!(address, expected_address);

        // then the content should be what we stored
        compare(bytes, read_data)?;

        Ok(())
    }

    async fn read_from_pos(
        file: &LargeFile,
        address: BytesAddress,
        pos: usize,
        len: usize,
        client: &Client,
    ) -> Result<Bytes> {
        let read_data = client.read_from(address, pos, len).await?;
        let mut expected = file.bytes();
        let _ = expected.split_to(pos);
        expected.truncate(len);
        compare(expected, read_data.clone())?;
        Ok(read_data)
    }

    fn compare(original: Bytes, result: Bytes) -> Result<()> {
        assert_eq!(original.len(), result.len());

        for (counter, (a, b)) in original.into_iter().zip(result).enumerate() {
            if a != b {
                return Err(eyre::eyre!(format!("Not equal! Counter: {}", counter)));
            }
        }
        Ok(())
    }
}
