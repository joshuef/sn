// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{constants::SN_AUTHD_CONNECTION_IDLE_TIMEOUT, Error, Result};
use log::info;
use qjsonrpc::ClientEndpoint;
use serde::de::DeserializeOwned;
use tokio::runtime;

pub mod auth_types {
    use crate::ipc::req::IpcReq;
    use serde::{Deserialize, Serialize};

    pub type SafeAuthReq = IpcReq;
    pub type SafeAuthReqId = u32;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct AuthedApp {
        /// The App ID. It must be unique.
        pub id: String,
        /// The application friendly-name.
        pub name: String,
        /// The application provider/vendor (e.g. MaidSafe)
        pub vendor: String,
    }

    // Type of the list of authorised applications in a Safe
    pub type AuthedAppsList = Vec<AuthedApp>;
}

pub fn parse_hex(hex_str: &str) -> Vec<u8> {
    let mut hex_bytes = hex_str
        .as_bytes()
        .iter()
        .filter_map(|b| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        })
        .fuse();

    let mut bytes = Vec::new();
    while let (Some(h), Some(l)) = (hex_bytes.next(), hex_bytes.next()) {
        bytes.push(h << 4 | l)
    }
    bytes
}

pub fn ed_sk_from_hex(hex_str: &str) -> Result<ed25519_dalek::SecretKey> {
    let sk_bytes = parse_hex(hex_str);
    ed25519_dalek::SecretKey::from_bytes(&sk_bytes).map_err(|_| {
        Error::InvalidInput("Failed to deserialize provided Ed25519 secret key".to_string())
    })
}

// Get hex string of a SecretKey
pub fn sk_to_hex(sk: sn_interface::types::SecretKey) -> String {
    match sk {
        sn_interface::types::SecretKey::Ed25519(sk) => {
            sk.to_bytes().iter().map(|b| format!("{:02x}", b)).collect()
        }
        //SecretKey::Bls(sk) => sk.inner().reveal(), // FIXME: it includes bls in the text
        sn_interface::types::SecretKey::BlsShare(sk) => sk.inner().reveal(), // FIXME: it includes bls in the text
    }
}

// Send a request to authd using JSON-RPC over QUIC
pub async fn send_authd_request<T>(
    dst_endpoint: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<T>
where
    T: DeserializeOwned,
{
    info!(
        "Sending '{}' request to SAFE Authenticator on {} ...",
        method, dst_endpoint
    );

    match dirs_next::home_dir() {
        None => Err(Error::AuthdClientError(
            "Failed to obtain local project directory where to read certificate from".to_string(),
        )),
        Some(mut paths) => {
            paths.push(".safe");
            paths.push("authd");

            let cert_base_path = paths.display().to_string();

            let qjsonrpc_client = ClientEndpoint::new(
                &cert_base_path,
                Some(SN_AUTHD_CONNECTION_IDLE_TIMEOUT),
                false,
            )
            .map_err(|err| {
                Error::AuthdClientError(format!("Failed to create client endpoint: {}", err))
            })?;

            // We try to obtain current runtime or create a new one if there is none
            let runtime = match runtime::Handle::try_current() {
                Ok(r) => r,
                Err(_) => runtime::Runtime::new()
                    .map_err(|err| {
                        Error::AuthdClientError(format!("Failed to create runtime: {}", err))
                    })?
                    .handle()
                    .clone(),
            };

            let mut outgoing_conn = {
                let _ = runtime.enter();
                qjsonrpc_client.bind().map_err(|err| {
                    Error::AuthdClientError(format!("Failed to bind endpoint: {}", err))
                })?
            };

            // Establish a new connection
            outgoing_conn
                .connect(dst_endpoint, None)
                .await
                .map_err(|err| {
                    Error::AuthdClientError(format!(
                        "Failed to establish connection with authd: {}",
                        err
                    ))
                })?
                // Send request and await for response
                .send(method, params)
                .await
                .map_err(|err| match err {
                    qjsonrpc::Error::RemoteEndpointError(msg) => Error::AuthdError(msg),
                    other => Error::AuthdClientError(other.to_string()),
                })
        }
    }
}
