// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::errors::convert_bincode_error;
// use sn_interface::network_knowledge::prefix_map::NetworkPrefixMap;
use super::{Error, Result};
use bytes::Bytes;
use multibase::{self, Base};
use rand::rngs::OsRng;
use rand::Rng;
use rayon::current_num_threads;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Wrapper for raw bincode::serialise.
pub fn serialise<T: Serialize>(data: &T) -> Result<Vec<u8>> {
    bincode::serialize(data).map_err(convert_bincode_error)
}

/// Wrapper for bincode::deserialize.
pub fn deserialise<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    bincode::deserialize(bytes).map_err(convert_bincode_error)
}

/// Wrapper for z-Base-32 multibase::encode.
pub fn encode<T: Serialize>(data: &T) -> Result<String> {
    let bytes = serialise(&data)?;
    Ok(multibase::encode(Base::Base32Z, &bytes))
}

/// Wrapper for z-Base-32 multibase::decode.
pub fn decode<I: AsRef<str>, O: DeserializeOwned>(encoded: I) -> Result<O> {
    let (base, decoded) =
        multibase::decode(encoded).map_err(|e| Error::FailedToParse(e.to_string()))?;
    if base != Base::Base32Z {
        return Err(Error::FailedToParse(format!(
            "Expected z-base-32 encoding, but got {:?}",
            base
        )));
    }
    deserialise(&decoded).map_err(|e| Error::FailedToParse(e.to_string()))
}


/// Generates a random vector using provided `length`.
pub fn random_bytes(length: usize) -> Bytes {
    use rayon::prelude::*;
    let threads = current_num_threads();

    if threads > length {
        let mut rng = OsRng;
        return ::std::iter::repeat(())
            .map(|()| rng.gen::<u8>())
            .take(length)
            .collect();
    }

    let per_thread = length / threads;
    let remainder = length % threads;

    let mut bytes: Vec<u8> = (0..threads)
        .par_bridge()
        .map(|_| vec![0u8; per_thread])
        .map(|mut bytes| {
            let bytes = bytes.as_mut_slice();
            rand::thread_rng().fill(bytes);
            bytes.to_owned()
        })
        .flatten()
        .collect();

    bytes.extend(vec![0u8; remainder]);

    Bytes::from(bytes)
}
