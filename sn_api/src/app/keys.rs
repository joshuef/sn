// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Safe;
use crate::{Error, Result};
use hex::encode;
use rand::rngs::OsRng;
use sn_interface::types::{Keypair, SecretKey};
use xor_name::XorName;

impl Safe {
    // Generate a key pair
    pub fn generate_random_ed_keypair(&self) -> Keypair {
        let mut rng = OsRng;
        Keypair::new_ed25519(&mut rng)
    }

    // Check that the XOR/NRS-URL corresponds to the public key derived from the provided client id
    pub async fn validate_sk_for_url(&self, secret_key: &SecretKey, url: &str) -> Result<String> {
        let derived_xorname = match secret_key {
            SecretKey::Ed25519(sk) => {
                let pk: ed25519_dalek::PublicKey = sk.into();
                XorName(pk.to_bytes())
            }
            _ => {
                return Err(Error::InvalidInput(
                    "Cannot form a keypair from a BlsKeyShare at this time.".to_string(),
                ))
            }
        };

        let safeurl = self.parse_and_resolve_url(url).await?;
        if safeurl.xorname() != derived_xorname {
            Err(Error::InvalidInput(
                "The URL doesn't correspond to the public key derived from the provided secret key"
                    .to_string(),
            ))
        } else {
            Ok(encode(&derived_xorname))
        }
    }
}
