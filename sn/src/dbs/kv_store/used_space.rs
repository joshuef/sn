// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Error;
use async_recursion::async_recursion;
use dashmap::DashSet;
use futures::future::join_all;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;

#[derive(Clone, Debug)]
/// Tracking used space in supplied dirs, and providing checks to ensure max capacity isn't exceeded
pub struct UsedSpace {
    /// the maximum (inclusive) allocated space for storage
    max_capacity: u64,
    dirs: Arc<DashSet<PathBuf>>,
}

impl UsedSpace {
    /// Create new UsedSpace tracker
    pub fn new(max_capacity: u64) -> Self {

        debug!(">>>> NEW USED SPACE, max capacity: {:?}", max_capacity);
        Self {
            max_capacity,
            dirs: Arc::new(DashSet::new()),
        }
    }

    // The dir shall not be added when its ancestor exists.
    // Remove existing decendant dirs when the dir is ancestor to them.
    pub(crate) fn add_dir(&self, dir: &Path) {
        let dir_str = if let Some(str) = dir.to_str() {
            str
        } else {
            error!("Path {:?} cannot be coverted to str.", dir);
            return;
        };

        let ancestor_strs: HashSet<_> = dir.ancestors().filter_map(|anc| anc.to_str()).collect();

        if self.dirs.iter().any(|dir| {
            let cur_dir_str = if let Some(str) = dir.to_str() {
                str
            } else {
                return false;
            };
            ancestor_strs.contains(cur_dir_str)
        }) {
            // Ancestor exists
            trace!("Path {:?} is a descendant of an existing entry, it will not be added for storage calcuation checks.", dir);
            return;
        }

        // Remove descendants
        self.dirs.retain(|cur_dir| {
            let cur_ancestor_strs: HashSet<_> =
                cur_dir.ancestors().filter_map(|anc| anc.to_str()).collect();
            !cur_ancestor_strs.contains(dir_str)
        });

        let _ = self.dirs.insert(dir.to_path_buf());
    }

    // pub(crate) fn remove_dir(&self, dir: &Path) {
    //     let _ = self.dirs.remove(&dir.to_path_buf());
    // }

    pub(crate) fn max_capacity(&self) -> u64 {
        self.max_capacity
    }

    pub(crate) async fn can_consume(&self, space: u64) -> bool {
        let total_used_right_now = self.total().await;
        debug!(">>>> Checking if we can consume {:?} of space", space);
        debug!(">>>> current total: {:?}",  total_used_right_now);
        total_used_right_now
            .checked_add(space)
            .map_or(false, |new_total| self.max_capacity >= new_total)
    }

    pub(crate) async fn total(&self) -> u64 {
        // todo: handle the error
        let handles = self
            .dirs
            .iter()
            .map(|d| d.clone())
            .map(|path| tokio::spawn(async move { get_size(path).await.map_err(Error::from) }));
        join_all(handles).await.iter().flatten().flatten().sum()
    }
}

/// Returns size in bytes of a file or directory. For directories, it recursively gets all of the
/// sizes of its children.
#[async_recursion]
async fn get_size(path: PathBuf) -> tokio::io::Result<u64> {

    debug!(">>> getting size of {:?}", path);

    let metadata = fs::metadata(&path).await?;
    let mut size = metadata.len();
    if metadata.is_dir() {
        let mut dir = fs::read_dir(&path).await?;
        while let Some(entry) = dir.next_entry().await? {
            size += get_size(entry.path()).await?;
        }
    }

    debug!("size found of {:?} is {:?}", path, size);
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn used_space_add_dir() {
        let used_space = UsedSpace::new(u64::MAX);
        let path_desc_1 = Path::new("/x/y/a");
        let path_desc_2 = Path::new("/x/y/b");
        let path_anc = Path::new("/x");

        used_space.add_dir(path_desc_1);
        used_space.add_dir(path_desc_2);

        assert_eq!(2, used_space.dirs.len());

        used_space.add_dir(path_anc);
        assert_eq!(1, used_space.dirs.len());
        assert!(used_space.dirs.contains(path_anc));

        used_space.add_dir(path_desc_1);
        assert_eq!(1, used_space.dirs.len());
        assert!(used_space.dirs.contains(path_anc));
    }
}
