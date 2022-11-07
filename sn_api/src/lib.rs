// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#![warn(clippy::unwrap_used)]
#![recursion_limit = "256"]

#[cfg(feature = "app")]
mod app;
#[cfg(any(feature = "app", feature = "authd_client"))]
mod ipc;

#[cfg(feature = "authd_client")]
mod authd_client;
#[cfg(feature = "authenticator")]
mod authenticator;
mod common;
mod constants;
mod errors;
mod safeurl;

// re-export these useful types from sn_data_types
pub use sn_interface::types::{DataAddress, Keypair, PublicKey, RegisterAddress, SecretKey};

#[cfg(feature = "app")]
pub use app::*;
#[cfg(any(feature = "app", feature = "authd_client"))]
pub use ipc::*;

#[cfg(feature = "app")]
pub use xor_name::XorName;

#[cfg(feature = "authenticator")]
pub use authenticator::*;

#[cfg(feature = "authd_client")]
pub use authd_client::*;

#[cfg(any(feature = "authenticator", feature = "authd_client"))]
pub use common::auth_types::*;

pub use errors::{Error, Result};

pub use safeurl::*;
