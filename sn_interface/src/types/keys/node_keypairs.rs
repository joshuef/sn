// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::super::{PublicKey, Signature};
use crate::types::{BlsKeypairShare, SignatureShare};
use bls::{serde_impl::SerdeSecret, PublicKeySet, SecretKeyShare as BlsSecretKeyShare};
use ed25519_dalek::Keypair as Ed25519Keypair;
use rand::{CryptoRng, Rng};
use serde::{Deserialize, Serialize};
use signature::Signer;
use xor_name::XorName;

/// This is used at a network node for holding the
/// obligatory Ed25519 keypair needed as Adult, and
/// then a BLS keypair share when being promoted to Elder.
/// (Also the corresponding public keys).
/// The Ed25519 is kept as Elder, in case it is demoted.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeKeypairs {
    ed25519: Ed25519Keypair,
    bls: Option<BlsKeypairShare>,
}

impl NodeKeypairs {
    /// Constructs a `NodeKeypairs` with a random Ed25519 keypair and no BLS keys.
    pub fn new<T: CryptoRng + Rng>(rng: &mut T) -> Self {
        let ed25519 = Ed25519Keypair::generate(rng);

        Self { ed25519, bls: None }
    }

    /// Constructs a `NodeKeypairs` whose name is in the interval [start, end] (both endpoints inclusive).
    pub fn within_range<T: CryptoRng + Rng>(start: &XorName, end: &XorName, rng: &mut T) -> Self {
        let mut ed25519 = Ed25519Keypair::generate(rng);
        loop {
            let name: XorName = PublicKey::Ed25519(ed25519.public).into();
            if name >= *start && name <= *end {
                return Self { ed25519, bls: None };
            }
            ed25519 = Ed25519Keypair::generate(rng);
        }
    }

    /// Returns the BLS if any, else the Ed25519.
    pub fn public_key(&self) -> PublicKey {
        if let Some(keys) = &self.bls {
            PublicKey::BlsShare(keys.public)
        } else {
            PublicKey::Ed25519(self.ed25519.public)
        }
    }

    /// Returns the BLS public key set if any.
    pub fn public_key_set(&self) -> Option<&PublicKeySet> {
        self.bls.as_ref().map(|s| &s.public_key_set)
    }

    /// Signs with the BLS if any, else the Ed25519.
    pub fn sign(&self, data: &[u8]) -> Signature {
        if let Some(sig) = self.sign_using_bls(data) {
            sig
        } else {
            self.sign_using_ed25519(data)
        }
    }

    /// Creates a detached Ed25519 signature of `data`.
    pub fn sign_using_ed25519<T: AsRef<[u8]>>(&self, data: T) -> Signature {
        Signature::Ed25519(self.ed25519.sign(data.as_ref()))
    }

    /// Creates a detached BLS signature share of `data` if the `self` holds a BLS keypair share.
    pub fn sign_using_bls<T: AsRef<[u8]>>(&self, data: T) -> Option<Signature> {
        self.bls.as_ref().map(|keys| {
            Signature::BlsShare(SignatureShare {
                index: keys.index,
                share: keys.secret.inner().sign(data),
            })
        })
    }

    /// Sets the `NodeKeypairs`'s BLS keypair share using the provided BLS secret key share.
    pub fn set_bls_keys(
        &mut self,
        index: usize,
        secret_share: BlsSecretKeyShare,
        public_set: PublicKeySet,
    ) {
        let public = secret_share.public_key_share();
        let secret = SerdeSecret(secret_share);
        self.bls = Some(BlsKeypairShare {
            index,
            secret,
            public,
            public_key_set: public_set,
        });
    }

    /// Clears the `NodeKeypairs`'s BLS keypair share, i.e. sets it to `None`.
    pub fn clear_bls_keys(&mut self) {
        self.bls = None;
    }
}
