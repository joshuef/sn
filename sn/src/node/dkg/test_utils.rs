// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::KeyedSig;

use crate::node::{Error, Result};
use sn_interface::messaging::system::SectionAuth;

use serde::Serialize;

// Create signature for the given payload using the given secret key.
pub(crate) fn prove<T: Serialize>(secret_key: &bls::SecretKey, payload: &T) -> Result<KeyedSig> {
    let bytes = bincode::serialize(payload).map_err(|_| Error::InvalidPayload)?;
    Ok(KeyedSig {
        public_key: secret_key.public_key(),
        signature: secret_key.sign(&bytes),
    })
}

// Wrap the given payload in `SectionAuth`
pub(crate) fn section_signed<T: Serialize>(
    secret_key: &bls::SecretKey,
    payload: T,
) -> Result<SectionAuth<T>> {
    let sig = prove(secret_key, &payload)?;
    Ok(SectionAuth::new(payload, sig))
}
