// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Error, Result};

use crate::UsedSpace;
use sn_interface::types::{Chunk, ChunkAddress};

use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use walkdir::WalkDir;
use xor_name::{Prefix, XorName};

const BIT_TREE_DEPTH: usize = 20;
const CHUNK_DB_DIR: &str = "chunkdb";

/// A disk store for chunks
#[derive(Clone, Debug)]
pub(crate) struct ChunkStore {
    bit_tree_depth: usize,
    chunk_store_path: PathBuf,
    used_space: UsedSpace,
}

impl ChunkStore {
    /// Creates a new `ChunkStore` at location `root/CHUNK_DB_DIR`
    ///
    /// If the location specified already contains a ChunkStore, it is simply used
    ///
    /// Used space of the dir is tracked
    pub(crate) fn new<P: AsRef<Path>>(root: P, used_space: UsedSpace) -> Result<Self> {
        let chunk_store_path = root.as_ref().join(CHUNK_DB_DIR);
        // let mut file = tokio::fs::File::create_dir_all(chunk_store_path).await?;

        Ok(ChunkStore {
            bit_tree_depth: BIT_TREE_DEPTH,
            chunk_store_path,
            used_space,
        })
    }

    // ---------------------- helper methods ----------------------

    // Helper that returns the prefix tree path of depth `bit_count` for a given xorname
    // Example:
    // - with a xorname with starting bits `010001110110....`
    // - and a bit_count of `6`
    // returns the path `CHUNK_STORE_PATH/0/1/0/0/0/1`
    // If the provided bit count is larger than `self.bit_tree_depth`, uses `self.bit_tree_depth`
    // to stay within the prefix tree path
    fn prefix_tree_path(&self, xorname: XorName, bit_count: usize) -> PathBuf {
        let bin = format!("{:b}", xorname);
        let prefix_dir_path: PathBuf = bin
            .chars()
            .take(std::cmp::min(bit_count, self.bit_tree_depth))
            .map(|c| format!("{}", c))
            .collect();

        let mut path = self.chunk_store_path.clone();
        path.push(prefix_dir_path);
        path
    }

    fn address_to_filepath(&self, addr: &ChunkAddress) -> Result<PathBuf> {
        let xorname = *addr.name();
        let filename = addr.encode_to_zbase32()?;
        let mut path = self.prefix_tree_path(xorname, self.bit_tree_depth);
        path.push(filename);
        Ok(path)
    }

    fn filepath_to_address(&self, path: &str) -> Result<ChunkAddress> {
        let filename = Path::new(path)
            .file_name()
            .ok_or(Error::NoFilename)?
            .to_str()
            .ok_or(Error::InvalidFilename)?;
        Ok(ChunkAddress::decode_from_zbase32(filename)?)
    }

    // ---------------------- api methods ----------------------

    pub(crate) fn can_add(&self, size: usize) -> bool {
        self.used_space.can_add(size)
    }

    pub(crate) async fn write_chunk(&self, data: &Chunk) -> Result<ChunkAddress> {
        let addr = data.address();
        let filepath = self.address_to_filepath(addr)?;
        if let Some(dirs) = filepath.parent() {
            tokio::fs::create_dir_all(dirs).await?;
        }

        let mut file = tokio::fs::File::create(filepath).await?;
        file.write_all(data.value()).await?;

        self.used_space.increase(data.value().len());

        Ok(*addr)
    }

    #[allow(dead_code)]
    pub(crate) async fn delete_chunk(&self, addr: &ChunkAddress) -> Result<()> {
        let filepath = self.address_to_filepath(addr)?;
        let meta = tokio::fs::metadata(filepath.clone()).await?;
        tokio::fs::remove_file(filepath).await?;
        self.used_space.decrease(meta.len() as usize);
        Ok(())
    }

    pub(crate) async fn read_chunk(&self, addr: &ChunkAddress) -> Result<Chunk> {
        let file_path = self.address_to_filepath(addr)?;
        let bytes = Bytes::from(tokio::fs::read(file_path).await?);
        let chunk = Chunk::new(bytes);
        Ok(chunk)
    }

    pub(crate) fn chunk_file_exists(&self, addr: &ChunkAddress) -> Result<bool> {
        let filepath = self.address_to_filepath(addr)?;
        Ok(filepath.exists())
    }

    pub(crate) fn list_all_files(&self) -> Result<Vec<String>> {
        list_files_in(&self.chunk_store_path)
    }

    pub(crate) fn list_all_chunk_addresses(&self) -> Result<Vec<ChunkAddress>> {
        let all_files = self.list_all_files()?;
        let all_addrs = all_files
            .iter()
            .map(|filepath| self.filepath_to_address(filepath))
            .collect();
        all_addrs
    }

    #[allow(unused)]
    /// quickly find chunks related or not to a section, might be useful when adults change sections
    /// not used yet
    pub(crate) fn list_files_without_prefix(&self, prefix: Prefix) -> Result<Vec<String>> {
        let all_files = self.list_all_files()?;
        let prefix_path = self.prefix_tree_path(prefix.name(), prefix.bit_count());
        let outside_prefix = all_files
            .into_iter()
            .filter(|p| !Path::new(&p).starts_with(&prefix_path.as_path()))
            .collect();
        Ok(outside_prefix)
    }

    #[allow(unused)]
    /// quickly find chunks related or not to a section, might be useful when adults change sections
    /// not used yet
    pub(crate) fn list_files_with_prefix(&self, prefix: Prefix) -> Result<Vec<String>> {
        let prefix_path = self.prefix_tree_path(prefix.name(), prefix.bit_count());
        list_files_in(prefix_path.as_path())
    }
}

fn list_files_in(path: &Path) -> Result<Vec<String>> {
    let files = WalkDir::new(path)
        .into_iter()
        .filter_map(|e| match e {
            Ok(direntry) => Some(direntry),
            Err(err) => {
                warn!("ChunkStore: failed to process file entry: {}", err);
                None
            }
        })
        .filter(|e| e.file_type().is_file())
        .map(|e| e.path().display().to_string())
        .collect();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use sn_interface::types::utils::random_bytes;

    use super::*;
    use futures::future::join_all;
    use rayon::prelude::*;
    use tempfile::tempdir;

    fn init_chunk_disk_store() -> ChunkStore {
        let root = tempdir().expect("Failed to create temporary directory for chunk disk store");
        ChunkStore::new(root.path(), UsedSpace::new(usize::MAX))
            .expect("Failed to create chunk disk store")
    }

    #[tokio::test]
    #[ignore]
    async fn test_write_read_chunk() {
        let store = init_chunk_disk_store();
        // test that a range of different chunks return the written chunk
        for _ in 0..10 {
            let chunk = Chunk::new(random_bytes(100));

            let addr = store
                .write_chunk(&chunk)
                .await
                .expect("Failed to write chunk.");

            let read_chunk = store
                .read_chunk(&addr)
                .await
                .expect("Failed to read chunk.");

            assert_eq!(chunk.value(), read_chunk.value());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_read_async_multiple_chunks() {
        let store = init_chunk_disk_store();
        let size = 100;
        let chunks: Vec<Chunk> = std::iter::repeat_with(|| Chunk::new(random_bytes(size)))
            .take(7)
            .collect();
        write_and_read_chunks(&chunks, store).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_read_async_multiple_identical_chunks() {
        let store = init_chunk_disk_store();
        let chunks: Vec<Chunk> = std::iter::repeat(Chunk::new(Bytes::from("test_concurrent")))
            .take(7)
            .collect();
        write_and_read_chunks(&chunks, store).await;
    }

    async fn write_and_read_chunks(chunks: &[Chunk], store: ChunkStore) {
        // write all chunks
        let tasks = chunks.iter().map(|c| store.write_chunk(c));
        let results = join_all(tasks).await;

        // read all chunks
        let tasks = results.iter().flatten().map(|addr| store.read_chunk(addr));
        let results = join_all(tasks).await;
        let read_chunks: Vec<&Chunk> = results.iter().flatten().collect();

        // verify all written were read
        assert!(chunks
            .par_iter()
            .all(|c| read_chunks.iter().any(|r| r.value() == c.value())))
    }
}
