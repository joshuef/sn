// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! The Safe Network Client.
//!
//! In order to connect to The Safe Network you'll need to send messages back and forth to network nodes.
//! The [Client] has everything needed to perform this communication, with APIs to enable
//! working with data.
//!
//! With these APIs you can easily:
//! - Connect to The Safe Network
//! - Read Public data from the network
//! - Write data to the network (assuming you have a SafeCoin balance)
//!
//! ## Basic Usage
//!
//! Setting up a random client for read only access:
//!
//! # Examples
//!
//! TODO: update once data types are crdt compliant
//!
mod config_handler;
mod connections;
mod errors;

// Export public API.

pub use client_api::{Client, RegisterWriteAheadLog};
pub use config_handler::{ClientConfig, DEFAULT_AE_WAIT, DEFAULT_OPERATION_TIMEOUT};
pub use errors::ErrorMessage;
pub use errors::{Error, Result};
pub use qp2p::Config as QuicP2pConfig;

/// Client trait and related constants.
pub mod client_api;

/// Utility functions.
pub mod utils;
