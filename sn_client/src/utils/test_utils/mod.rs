// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(any(test, feature = "test-utils"))]
/// Utility functions for testing clients
pub mod test_client;

#[cfg(test)]
pub use test_client::{
    create_test_client, create_test_client_with, get_dbc_owner_from_secret_key_hex,
};

#[cfg(any(test, feature = "test-utils"))]
pub use test_client::read_genesis_dbc_from_first_node;

#[cfg(test)]
pub use sn_interface::init_logger;

#[cfg(test)]
#[macro_export]
/// Helper for tests to retry an operation awaiting for a successful response result
macro_rules! retry_loop {
    ($async_func:expr) => {
        loop {
            match $async_func.await {
                Ok(val) => break val,
                Err(_) => tokio::time::sleep(std::time::Duration::from_secs(2)).await,
            }
        }
    };
}

#[cfg(test)]
#[macro_export]
/// Helper for tests to retry an operation awaiting for a specific result
macro_rules! retry_loop_for_pattern {
    ($async_func:expr, $pattern:pat $(if $cond:expr)?) => {
        loop {
            let result = $async_func.await;
            match &result {
                $pattern $(if $cond)? => break result,
                Ok(_) | Err(_) => {
                    debug!("waiting before retying.....");
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await}
                    ,
            }
        }
    };
}
