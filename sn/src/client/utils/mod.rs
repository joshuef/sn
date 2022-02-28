// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

/// Common utility functions for writing test cases.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use bytes::Bytes;
use rand::{self, distributions::Alphanumeric, rngs::OsRng, Rng};
use sn_interface::types::{Encryption, PublicKey, Result as TypesResult, Scope};

struct DummyEncryption {
    public_key: PublicKey,
}

impl DummyEncryption {
    pub(crate) fn new(public_key: PublicKey) -> Self {
        Self { public_key }
    }
}

impl Encryption for DummyEncryption {
    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
    fn encrypt(&self, data: Bytes) -> TypesResult<Bytes> {
        Ok(data)
    }
    fn decrypt(&self, encrypted_data: Bytes) -> TypesResult<Bytes> {
        Ok(encrypted_data)
    }
}

///
pub fn encryption(scope: Scope, public_key: PublicKey) -> Option<impl Encryption> {
    match scope {
        Scope::Public => None,
        Scope::Private => Some(DummyEncryption::new(public_key)),
    }
}

/// Generates a `String` from `length` random UTF-8 `char`s.  Note that the NULL character will be
/// excluded to allow conversion to a `CString` if required, and that the actual `len()` of the
/// returned `String` will likely be around `4 * length` as most of the randomly-generated `char`s
/// will consume 4 elements of the `String`.
pub fn generate_random_string(length: usize) -> String {
    let mut rng = OsRng;
    ::std::iter::repeat(())
        .map(|()| rng.gen::<char>())
        .filter(|c| *c != '\u{0}')
        .take(length)
        .collect()
}

/// Generates a readable `String` using provided `length` and only ASCII characters.
pub fn generate_readable_string(length: usize) -> String {
    let mut rng = OsRng;
    ::std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(length)
        .collect()
}

/// Convert binary data to a displayable format
#[inline]
pub fn bin_data_format(data: &[u8]) -> String {
    let len = data.len();
    if len < 8 {
        return format!("[ {:?} ]", data);
    }

    format!(
        "[ {:02x} {:02x} {:02x} {:02x}..{:02x} {:02x} {:02x} {:02x} ]",
        data[0],
        data[1],
        data[2],
        data[3],
        data[len - 4],
        data[len - 3],
        data[len - 2],
        data[len - 1]
    )
}

#[cfg(test)]
mod tests {
    use sn_interface::types::utils::random_bytes;

    use super::*;

    const SIZE: usize = 10;

    // Test `generate_random_string` and that the results are not repeated.
    #[test]
    fn random_string() {
        let str0 = generate_random_string(SIZE);
        let str1 = generate_random_string(SIZE);
        let str2 = generate_random_string(SIZE);

        assert_ne!(str0, str1);
        assert_ne!(str0, str2);
        assert_ne!(str1, str2);

        assert_eq!(str0.chars().count(), SIZE);
        assert_eq!(str1.chars().count(), SIZE);
        assert_eq!(str2.chars().count(), SIZE);
    }

    // Test `random_bytes` and that the results are not repeated.
    #[test]
    fn random_vector() {
        let vec0 = random_bytes(SIZE);
        let vec1 = random_bytes(SIZE);
        let vec2 = random_bytes(SIZE);

        assert_ne!(vec0, vec1);
        assert_ne!(vec0, vec2);
        assert_ne!(vec1, vec2);

        assert_eq!(vec0.len(), SIZE);
        assert_eq!(vec1.len(), SIZE);
        assert_eq!(vec2.len(), SIZE);
    }
}
