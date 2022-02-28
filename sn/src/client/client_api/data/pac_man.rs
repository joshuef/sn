// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::client::{Error, Result};
use bincode::serialize;
use bytes::Bytes;
use rayon::prelude::*;
use self_encryption::{DataMap, EncryptedChunk};
use serde::{Deserialize, Serialize};
use sn_interface::types::{BytesAddress, Chunk, Encryption};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub(crate) enum DataMapLevel {
    // Holds the data map to the source data.
    First(DataMap),
    // Holds the data map of an _additional_ level of chunks
    // resulting from chunking up a previous level data map.
    // This happens when that previous level data map was too big to fit in a chunk itself.
    Additional(DataMap),
}

#[allow(unused)]
pub(crate) fn encrypt_from_path(
    path: &Path,
    encryption: Option<&impl Encryption>,
) -> Result<(BytesAddress, Vec<Chunk>)> {
    let (data_map, encrypted_chunks) = encrypt_file(path)?;
    pack(data_map, encrypted_chunks, encryption)
}

pub(crate) fn encrypt_large(
    data: Bytes,
    encryption: Option<&impl Encryption>,
) -> Result<(BytesAddress, Vec<Chunk>)> {
    let (data_map, encrypted_chunks) = encrypt_data(data)?;
    pack(data_map, encrypted_chunks, encryption)
}

/// Returns the top-most chunk address through which the entire
/// data tree can be accessed, and all the other encrypted chunks.
/// If encryption is provided, the additional secret key level chunks are encrypted with it.
/// This is necessary if the data is meant to be private, since a `DataMap` is used to find and decrypt the original file.
pub(crate) fn pack(
    data_map: DataMap,
    encrypted_chunks: Vec<EncryptedChunk>,
    encryption: Option<&impl Encryption>,
) -> Result<(BytesAddress, Vec<Chunk>)> {
    // Produces a chunk out of the first secret key, which is validated for its size.
    // If the chunk is too big, it is self-encrypted and the resulting (additional level) secret key is put into a chunk.
    // The above step is repeated as many times as required until the chunk size is valid.
    // In other words: If the chunk content is too big, it will be
    // self encrypted into additional chunks, and now we have a new secret key
    // which points to all of those additional chunks.. and so on.
    let mut chunks = vec![];
    let mut chunk_content = pack_data_map(DataMapLevel::First(data_map))?;

    // appeasing of compiler inference shenanigans..
    // no need to encrypt what is self-encrypted, thus we pass in `None` for those cases
    // (however, the compiler could not infer type from the `None`)
    let mut no_encryption = encryption; // copy the original variable
    let _value = no_encryption.take(); // make it None

    let (address, additional_chunks) = loop {
        let chunk = to_chunk(chunk_content, encryption)?;
        // If datamap chunk is less that 1MB return it so it can be directly sent to the network
        if chunk.validate_size() {
            let name = *chunk.name();
            chunks.reverse();
            chunks.push(chunk);
            // returns the address of the last datamap, and all the chunks produced
            let address = if encryption.is_some() {
                BytesAddress::Private(name)
            } else {
                BytesAddress::Public(name)
            };
            break (address, chunks);
        } else {
            let serialized_chunk = Bytes::from(serialize(&chunk)?);
            let (data_map, next_encrypted_chunks) =
                self_encryption::encrypt(serialized_chunk).map_err(Error::SelfEncryption)?;
            let expected_total = chunks.len() + next_encrypted_chunks.len();
            chunks = next_encrypted_chunks
                .par_iter()
                .map(|c| to_chunk(c.content.clone(), no_encryption)) // no need to encrypt what is self-encrypted
                .flatten()
                .chain(chunks)
                .collect();
            if expected_total > chunks.len() {
                // as we flatten above, we need to check outcome here
                return Err(Error::NotAllDataWasChunked {
                    expected: expected_total,
                    chunked: chunks.len(),
                });
            }
            chunk_content = pack_data_map(DataMapLevel::Additional(data_map))?;
        }
    };

    let expected_total = encrypted_chunks.len() + additional_chunks.len();
    let all_chunks: Vec<_> = encrypted_chunks
        .par_iter()
        .map(|c| to_chunk(c.content.clone(), no_encryption)) // no need to encrypt what is self-encrypted
        .flatten() // swallows errors!
        .chain(additional_chunks)
        .collect();

    if expected_total > all_chunks.len() {
        // as we flatten above, we need to check outcome here
        return Err(Error::NotAllDataWasChunked {
            expected: expected_total,
            chunked: all_chunks.len(),
        });
    }

    Ok((address, all_chunks))
}

pub(crate) fn to_chunk(
    chunk_content: Bytes,
    encryption: Option<&impl Encryption>,
) -> Result<Chunk> {
    let chunk: Chunk = if let Some(encryption) = encryption {
        // If this is a DataMapLevel: strictly, we do not need to encrypt this if it's not going to be the
        // last level, since it will then instead be self-encrypted.
        // But we can just as well do it, for now.. (which also lets us avoid some edge case handling).
        let encrypted_content = encryption.encrypt(chunk_content)?;
        Chunk::new(encrypted_content)
    } else {
        Chunk::new(chunk_content)
    };

    Ok(chunk)
}

fn pack_data_map(data_map: DataMapLevel) -> Result<Bytes> {
    Ok(Bytes::from(serialize(&data_map)?))
}

fn encrypt_file(file: &Path) -> Result<(DataMap, Vec<EncryptedChunk>)> {
    let bytes = Bytes::from(std::fs::read(file).map_err(Error::IoError)?);
    self_encryption::encrypt(bytes).map_err(Error::SelfEncryption)
}

fn encrypt_data(bytes: Bytes) -> Result<(DataMap, Vec<EncryptedChunk>)> {
    self_encryption::encrypt(bytes).map_err(Error::SelfEncryption)
}
