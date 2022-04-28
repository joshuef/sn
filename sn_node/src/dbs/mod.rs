// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

#[cfg(feature="chunks")]
mod chunk_store;
#[cfg(any(feature="chunks", feature="registers"))]
mod encoding;
#[cfg(any(feature="chunks", feature="registers"))]
mod errors;
#[cfg(any(feature="chunks", feature="registers"))]
mod event_store;
mod lru_cache;
mod used_space;

#[cfg(feature="chunks")]
pub(crate) use chunk_store::ChunkStore;

#[cfg(any(feature="chunks", feature="registers"))]
pub(crate) use encoding::{deserialise, serialise};
#[cfg(any(feature="chunks", feature="registers"))]
pub(crate) use errors::{convert_to_error_msg, Error, Result};
#[cfg(any(feature="chunks", feature="registers"))]
pub(crate) use event_store::EventStore;
pub(crate) use lru_cache::LruCache;
use std::path::Path;
pub use used_space::UsedSpace;

pub(crate) const SLED_FLUSH_TIME_MS: Option<u64> = Some(10000);

pub(crate) trait Subdir {
    fn subdir() -> &'static Path;
}
