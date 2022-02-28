// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{ChunkAddress, XorName};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum allowed size for a serialised Chunk to grow to.
pub const MAX_CHUNK_SIZE_IN_BYTES: usize = 1024 * 1024 + 10 * 1024;

/// Chunk, an immutable chunk of data
#[derive(Hash, Eq, PartialEq, PartialOrd, Ord, Clone, custom_debug::Debug)]
pub struct Chunk {
    /// Network address. Omitted when serialising and
    /// calculated from the `value` when deserialising.
    address: ChunkAddress,
    /// Contained data.
    #[debug(skip)]
    value: Bytes,
}

impl Chunk {
    /// Creates a new instance of `Chunk`.
    pub fn new(value: Bytes) -> Self {
        Self {
            address: ChunkAddress(XorName::from_content(value.as_ref())),
            value,
        }
    }

    /// Returns the value.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Returns the address.
    pub fn address(&self) -> &ChunkAddress {
        &self.address
    }

    /// Returns the name.
    pub fn name(&self) -> &XorName {
        self.address.name()
    }

    /// Returns size of contained value.
    pub fn payload_size(&self) -> usize {
        self.value.len()
    }

    /// Returns size of this chunk after serialisation.
    pub fn serialised_size(&self) -> usize {
        self.value.len()
    }

    /// Returns `true` if the size is valid.
    pub fn validate_size(&self) -> bool {
        self.serialised_size() <= MAX_CHUNK_SIZE_IN_BYTES
    }
}

impl Serialize for Chunk {
    fn serialize<S: Serializer>(&self, serialiser: S) -> Result<S::Ok, S::Error> {
        // Address is omitted since it's derived from value
        self.value.serialize(serialiser)
    }
}

impl<'de> Deserialize<'de> for Chunk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Deserialize::deserialize(deserializer)?;
        Ok(Self::new(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{utils, Chunk, Error, Result};
    use bytes::Bytes;
    use hex::encode;
    use rand::{self, Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::{env, iter, thread};

    #[test]
    fn deterministic_name() {
        let bytes1 = Bytes::from(b"Hello".to_vec());
        let bytes2 = Bytes::from(b"Goodbye".to_vec());

        let chunk1 = Chunk::new(bytes1.clone());
        let chunk2 = Chunk::new(bytes2.clone());
        let chunk3 = Chunk::new(bytes1);
        let chunk4 = Chunk::new(bytes2);

        assert_eq!(chunk1.name(), chunk3.name());
        assert_eq!(chunk2.name(), chunk4.name());

        assert_ne!(chunk1.name(), chunk2.name());
        assert_ne!(chunk3.name(), chunk4.name());
    }

    #[test]
    fn deterministic_address() -> Result<()> {
        let bytes1 = Bytes::from(b"Hello".to_vec());
        let bytes2 = Bytes::from(b"Goodbye".to_vec());

        let chunk1 = Chunk::new(bytes1.clone());
        let chunk2 = Chunk::new(bytes2.clone());
        let chunk3 = Chunk::new(bytes1);
        let chunk4 = Chunk::new(bytes2);

        assert_eq!(chunk1.address(), chunk3.address());
        assert_eq!(chunk2.address(), chunk4.address());

        assert_ne!(chunk1.address(), chunk2.address());
        assert_ne!(chunk3.address(), chunk4.address());

        assert_ne!(
            bincode::serialize(chunk1.address()).map_err(|_| Error::Serialisation(
                "Test address serialisation failed".to_string()
            ))?,
            bincode::serialize(chunk2.address()).map_err(|_| Error::Serialisation(
                "Test address serialisation failed".to_string()
            ))?
        );

        Ok(())
    }

    #[test]
    fn deterministic_test() {
        let value = Bytes::from(b"immutable chunk value".to_vec());
        let chunk = Chunk::new(value);
        let chunk_name = encode(chunk.name().0.as_ref());
        let expected_name = "920f9a03bc90af3a7bfaf50c03abd5ff5b1579bd4006ba28eebcf240d4922519";

        assert_eq!(&expected_name, &chunk_name);
    }

    #[test]
    fn serialisation() -> Result<()> {
        let mut rng = get_rng();
        let len = rng.gen_range(1, 10_000);
        let value = iter::repeat_with(|| rng.gen()).take(len).collect();
        let chunk = Chunk::new(value);
        let serialised = utils::serialise(&chunk)?;
        let parsed = utils::deserialise(&serialised)?;
        assert_eq!(chunk, parsed);
        Ok(())
    }

    fn get_rng() -> XorShiftRng {
        let env_var_name = "RANDOM_SEED";
        let seed = env::var(env_var_name)
            .map(|res| res.parse::<u64>().unwrap_or_else(|_| rand::random()))
            .unwrap_or_else(|_| rand::random());
        println!(
            "To replay this '{}', set env var {}={}",
            thread::current().name().unwrap_or(""),
            env_var_name,
            seed
        );
        XorShiftRng::seed_from_u64(seed)
    }
}
