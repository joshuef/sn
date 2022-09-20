// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Implementation of the "Node" node for the SAFE Network.

// For quick_error
#![recursion_limit = "256"]
#![doc(
    html_logo_url = "https://github.com/maidsafe/QA/raw/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// Forbid some very bad patterns. Forbid is stronger than `deny`, preventing us from suppressing the
// lint with `#[allow(...)]` et-all.
#![forbid(
    arithmetic_overflow,
    mutable_transmutes,
    no_mangle_const_items,
    unknown_crate_types,
    unsafe_code
)]
// Turn on some additional warnings to encourage good style.
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    clippy::unicode_not_nfc,
    clippy::unwrap_used
)]

#[macro_use]
extern crate tracing;

pub mod comm;

mod storage;

pub use storage::UsedSpace;

pub mod node;

#[macro_export]
/// Help macro to log a sleep with unique id, meanwhile retain the info of LOC.
/// Pass in a Duration constructor as an argument.
macro_rules! log_sleep {
    ($duration_expr:expr) => {
        let duration = $duration_expr;
        let mut rng = rand::thread_rng();
        let sleep_id = xor_name::XorName::random(&mut rng);
        // To reduce the logs, only log long time sleeps.
        if duration > Duration::from_millis(500) {
            debug!("Sleep ID {sleep_id} going to sleep for {:?}", duration);
        }
        tokio::time::sleep(duration).await;
        if duration > Duration::from_millis(500) {
            debug!("Sleep ID {sleep_id} completed");
        }
    };
}

#[cfg(test)]
#[ctor::ctor]
fn test_setup() {
    // If you look down the call stack for `color_eyre::install`, the only error can come from
    // `OnceCell::set` if it's called twice. We could ignore the error, but it would be better to
    // ensure we only call it once.
    color_eyre::install().expect("color_eyre::install can only be called once");
}
