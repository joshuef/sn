// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! SAFE network data types.

/// Standardised messaging interface
pub mod messaging;
/// Knowledge of the safe network
pub mod network_knowledge;
/// Types on the safe network
pub mod types;

#[macro_use]
extern crate tracing;

pub use network_knowledge::{elder_count, SectionAuthorityProvider};

/// Number of copies of a chunk
/// Should be almost as much as elders, in case many are promoted at onces in one data space.
const DEFAULT_DATA_COPY_COUNT: usize = 6;

// const SN_ELDER_COUNT: &str = "SN_ELDER_COUNT";
const SN_DATA_COPY_COUNT: &str = "SN_DATA_COPY_COUNT";

/// Max number of faulty Elders is assumed to be less than 1/3.
/// So it's no more than 2 with 7 Elders.
pub fn max_num_faulty_elders() -> usize {
    elder_count() / 3
}

/// Max number of faulty Elders is assumed to be less than 1/3.
/// So it's no more than 2 with 7 Elders.
pub fn max_num_faulty_elders_for_sap(sap: SectionAuthorityProvider) -> usize {
    sap.elder_count() / 3
}

/// The least number of Elders to select, to be "guaranteed" one correctly functioning Elder.
/// This number will be 3 with 7 Elders.
pub fn at_least_one_correct_elder() -> usize {
    max_num_faulty_elders() + 1
}

/// The least number of Elders to select, to be "guaranteed" one correctly functioning Elder.
/// This number will be 3 with 7 Elders. And is relative to the provided SAP.
pub fn at_least_one_correct_elder_for_sap(sap: Option<SectionAuthorityProvider>) -> usize {
    if let Some(sap) = sap {
        max_num_faulty_elders_for_sap(sap) + 1
    } else {
        max_num_faulty_elders() + 1
    }
}

/// Get the expected chunk copy count for our network.
/// Defaults to DEFAULT_DATA_COPY_COUNT, but can be overridden by the env var SN_DATA_COPY_COUNT.
pub fn data_copy_count() -> usize {
    // if we have an env var for this, lets override
    match std::env::var(SN_DATA_COPY_COUNT) {
        Ok(count) => match count.parse() {
            Ok(count) => {
                warn!(
                    "data_copy_count countout set from env var SN_DATA_COPY_COUNT: {:?}",
                    SN_DATA_COPY_COUNT
                );
                count
            }
            Err(error) => {
                warn!("There was an error parsing {:?} env var. DEFAULT_DATA_COPY_COUNT will be used: {:?}", SN_DATA_COPY_COUNT, error);
                DEFAULT_DATA_COPY_COUNT
            }
        },
        Err(_) => DEFAULT_DATA_COPY_COUNT,
    }
}
