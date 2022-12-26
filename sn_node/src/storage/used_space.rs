// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::info;

#[derive(Clone, Debug)]
/// Tracking used space
pub struct UsedSpace {
    /// the maximum (inclusive) allocated space for storage
    max_capacity: usize,
    used_space: Arc<AtomicUsize>,
}

impl UsedSpace {
    /// Create new `UsedSpace` tracker
    pub fn new(max_capacity: usize) -> Self {
        Self {
            max_capacity,
            used_space: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn increase(&self, size: usize) {
        let _ = self.used_space.fetch_add(size, Ordering::Relaxed);
    }

    pub(crate) fn decrease(&self, size: usize) {
        let _ = self.used_space.fetch_sub(size, Ordering::Relaxed);
    }

    pub(crate) fn can_add(&self, size: usize) -> bool {
        let current_used_space = self.used_space.load(Ordering::Relaxed);
        current_used_space + size <= self.max_capacity
    }

    /// Checks if we've reached the limit of our storage.
    pub(crate) fn has_reached_limit(&self) -> bool {
        let current_used_space = self.used_space.load(Ordering::Relaxed);
        current_used_space >= self.max_capacity
    }

    #[allow(unused)]
    pub(crate) fn ratio(&self) -> f64 {
        let used = self.used_space.load(Ordering::Relaxed);
        let max_capacity = self.max_capacity;
        let used_space_ratio = used as f64 / max_capacity as f64;
        info!("Used space: {:?}", used);
        info!("Max capacity: {:?}", max_capacity);
        info!("Used space ratio: {:?}", used_space_ratio);
        used_space_ratio
    }
}
