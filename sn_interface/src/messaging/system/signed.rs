// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use serde::{Deserialize, Serialize};

/// Signature created when a quorum of the section elders has agreed on something.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyedSig {
    /// The BLS public key.
    pub public_key: bls::PublicKey,
    /// The BLS signature corresponding to the public key.
    pub signature: bls::Signature,
}

impl KeyedSig {
    /// Verifies this signature against the payload.
    pub fn verify(&self, payload: &[u8]) -> bool {
        self.public_key.verify(&self.signature, payload)
    }
}

/// Single share of `KeyedSig`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SigShare {
    /// BLS public key set.
    pub public_key_set: bls::PublicKeySet,
    /// Index of the node that created this signature share.
    pub index: usize,
    /// BLS signature share corresponding to the `index`-th public key share of the public key set.
    pub signature_share: bls::SignatureShare,
}

impl SigShare {
    /// Creates new signature share.
    pub fn new(
        public_key_set: bls::PublicKeySet,
        index: usize,
        secret_key_share: &bls::SecretKeyShare,
        payload: &[u8],
    ) -> Self {
        Self {
            public_key_set,
            index,
            signature_share: secret_key_share.sign(payload),
        }
    }

    /// Verifies this signature share against the payload.
    pub fn verify(&self, payload: &[u8]) -> bool {
        self.public_key_set
            .public_key_share(self.index)
            .verify(&self.signature_share, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bls::SecretKey;

    #[test]
    fn verify_keyed_sig() {
        let sk = SecretKey::random();
        let public_key = sk.public_key();
        let data = "hello".to_string();
        let signature = sk.sign(&data);
        let sig = KeyedSig {
            public_key,
            signature,
        };
        assert!(sig.verify(data.as_bytes()));
    }
}
