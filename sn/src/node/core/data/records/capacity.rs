// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{Prefix, XorName};
use itertools::Itertools;
use sn_interface::messaging::data::StorageLevel;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::RwLock;

// The number of separate copies of a chunk which should be maintained.
pub(crate) const MIN_LEVEL_WHEN_FULL: u8 = 9; // considered full when >= 90 %.

/// A util for sharing the
/// info on data capacity among the
/// chunk storing nodes in the section.
#[derive(Clone, Default)]
pub(crate) struct Capacity {
    adult_levels: Arc<RwLock<BTreeMap<XorName, Arc<RwLock<StorageLevel>>>>>,
}

impl Capacity {
    /// Whether the adult is considered full.
    /// This happens when it has reported at least `MIN_LEVEL_WHEN_FULL`.
    pub(crate) async fn is_full(&self, adult: &XorName) -> Option<bool> {
        let adult_levels = self.adult_levels.read().await;
        let level = adult_levels.get(adult)?.read().await.value();
        Some(level >= MIN_LEVEL_WHEN_FULL)
    }

    pub(super) async fn add_new_adult(&self, adult: XorName) {
        info!("Adding new adult:{adult} to Capacity tracker");

        if let Some(old_entry) = self
            .adult_levels
            .write()
            .await
            .insert(adult, Arc::new(RwLock::new(StorageLevel::zero())))
        {
            let _level = old_entry.read().await.value();
            warn!("Throwing old storage level for Adult {adult}:{_level}");
        }
    }

    /// Avg usage by nodes in the section, a value between 0 and 10.
    pub(super) async fn avg_usage(&self) -> u8 {
        let mut total = 0_usize;
        let levels = self.adult_levels.read().await;
        // not sure if necessary, but now we'll be working with an isolated snapshot:
        let levels = levels.values().collect_vec();
        let num_adults = levels.len();
        if num_adults == 0 {
            return 0; // avoid divide by zero
        }
        for v in levels {
            total += v.read().await.value() as usize;
        }
        (total / num_adults) as u8
    }

    /// Storage levels of nodes in the section.
    pub(super) async fn levels(&self) -> BTreeMap<XorName, StorageLevel> {
        let mut map = BTreeMap::new();
        for (name, level) in self.adult_levels.read().await.iter() {
            let _prev = map.insert(*name, *level.read().await);
        }
        map
    }

    /// Nodes and storage levels of nodes matching the prefix.
    pub(super) async fn levels_matching(&self, prefix: Prefix) -> BTreeMap<XorName, StorageLevel> {
        self.levels()
            .await
            .iter()
            .filter(|(name, _)| prefix.matches(name))
            .map(|(name, level)| (*name, *level))
            .collect()
    }

    /// Full chunk storing nodes in the section (considered full when at >= `MIN_LEVEL_WHEN_FULL`).
    pub(super) async fn full_adults(&self) -> BTreeSet<XorName> {
        let mut set = BTreeSet::new();
        for (name, level) in self.adult_levels.read().await.iter() {
            if level.read().await.value() >= MIN_LEVEL_WHEN_FULL {
                let _changed = set.insert(*name);
            }
        }
        set
    }

    pub(super) async fn set_adult_levels(&self, levels: BTreeMap<XorName, StorageLevel>) {
        for (name, level) in levels {
            let _changed = self.set_adult_level(name, level).await;
        }
    }

    /// Returns whether the level changed or not.
    pub(super) async fn set_adult_level(&self, adult: XorName, new_level: StorageLevel) -> bool {
        {
            let all_levels = self.adult_levels.read().await;
            if let Some(level) = all_levels.get(&adult) {
                let current_level = { level.read().await.value() };
                info!("Current level: {}", current_level);
                if new_level.value() > current_level {
                    *level.write().await = new_level;
                    info!("Old value overwritten.");
                    return true; // value changed
                }
                return false; // no change
            }
        }

        info!("No current level, aqcuiring top level write lock..");
        // locks to prevent racing
        let mut all_levels = self.adult_levels.write().await;
        info!("Top level write lock acquired.");
        // checking the value again, if there was a concurrent insert..
        if let Some(level) = all_levels.get(&adult) {
            info!("Oh wait, a value was just recorded..");
            let current_level = { level.read().await.value() };
            info!("Current level: {}", current_level);
            if new_level.value() > current_level {
                *level.write().await = new_level;
                info!("Old value overwritten.");
                return true; // value changed
            }
            false // no change
        } else {
            let _level = all_levels.insert(adult, Arc::new(RwLock::new(new_level)));
            info!("New value inserted.");
            true // value changed
        }
    }

    /// Registered holders not present in provided list of members
    /// will be removed from adult_levels and no longer tracked for liveness.
    pub(super) async fn retain_members_only(&self, members: &BTreeSet<XorName>) {
        let mut adult_levels = self.adult_levels.write().await;
        let absent_adults: Vec<_> = adult_levels
            .iter()
            .filter(|(key, _)| !members.contains(key))
            .map(|(key, _)| *key)
            .collect();

        for adult in &absent_adults {
            let _level = adult_levels.remove(adult);
        }
    }
}
