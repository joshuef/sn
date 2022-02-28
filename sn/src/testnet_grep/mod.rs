// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use sn_interface::types::log_markers::LogMarker;

use grep::matcher::Matcher;
use grep::regex::RegexMatcher;
use grep::searcher::sinks::UTF8;
use grep::searcher::Searcher;
use std::path::PathBuf;

use std::collections::BTreeMap;
use walkdir::WalkDir;

use std::string::ToString;

use eyre::{bail, Error, Result};

use eyre::eyre;

use dirs_next::home_dir;

use strum::IntoEnumIterator;

// line number and the match
pub(crate) type Matches = Vec<(u64, String)>;

fn get_testnet_path() -> Result<PathBuf> {
    let mut home_dirs = home_dir().ok_or_else(|| eyre!("Failed to obtain user's home path"))?;

    home_dirs.push(".safe");
    home_dirs.push("node");
    home_dirs.push("local-test-network");
    Ok(home_dirs)
}

// Handler for searching log state
pub(crate) struct NetworkLogState {
    initial_logs: BTreeMap<LogMarker, usize>,
}

impl NetworkLogState {
    /// Set the baseline for the log state, return a
    pub(crate) fn new() -> Result<Self> {
        let mut initial_logs = BTreeMap::default();

        // let's get the initial state for each log marker
        for marker in LogMarker::iter() {
            let start_count = search_testnet(&marker)?;
            let _old = initial_logs.insert(marker, start_count.len());
        }

        Ok(Self { initial_logs })
    }

    /// Search for a marker, get changed count, and update log state
    /// Returns an optional new message count, None signifies no new messages
    pub(crate) fn get_additional_marker_count(
        &mut self,
        marker: LogMarker,
    ) -> Result<Option<usize>> {
        let new_markers = search_testnet(&marker)?.len();

        let old_markers = self.initial_logs.insert(marker, new_markers);

        if let Some(old_markers_count) = old_markers {
            if old_markers_count == new_markers {
                // no change
                return Ok(None);
            }
            Ok(Some(new_markers - old_markers_count))
        } else {
            Ok(Some(new_markers))
        }
    }

    /// assert new log marker counts
    pub(crate) async fn assert_count(
        &mut self,
        marker: LogMarker,
        target_count: usize,
    ) -> Result<()> {
        let new_messages = self.get_additional_marker_count(marker.clone())?;

        if let Some(count) = new_messages {
            if count != target_count {
                return Err(eyre!(
                    "The {} new {:?} log markers found did not match the target count of {:?}",
                    count,
                    &marker,
                    &target_count
                ));
            }

            Ok(())
        } else {
            bail!("No new {:?} were received at nodes", marker)
        }
    }
}

/// Search the local-test-network dir for matches.
pub(crate) fn search_testnet(pattern: &LogMarker) -> Result<Matches, Error> {
    let the_path = get_testnet_path()?;
    let paths = [the_path];
    let matcher = RegexMatcher::new_line_matcher(&pattern.to_string())?;
    let mut matches: Matches = vec![];

    for path in paths {
        for result in WalkDir::new(path) {
            let dent = match result {
                Ok(dent) => dent,
                Err(err) => {
                    bail!(err)
                }
            };

            let is_log_file = format!("{:?}", dent.file_name()).contains("log");

            if !is_log_file && !dent.file_type().is_file() {
                continue;
            }

            Searcher::new().search_path(
                &matcher,
                dent.path(),
                UTF8(|lnum, line| {
                    // We are guaranteed to find a match, so the unwrap is OK.
                    let mymatch = matcher.find(line.as_bytes())?.unwrap();
                    matches.push((lnum, line[mymatch].to_string()));
                    Ok(true)
                }),
            )?;
        }
    }

    Ok(matches)
}

/// Search the local-test-network dir for matches.
pub(crate) fn search_testnet_results_per_node(
    pattern: String,
) -> Result<BTreeMap<String, Matches>, Error> {
    let the_path = get_testnet_path()?;
    let paths = [the_path];
    let matcher = RegexMatcher::new_line_matcher(&pattern)?;
    let mut matches: BTreeMap<String, Matches> = BTreeMap::default();

    for path in paths {
        for result in WalkDir::new(path) {
            let dent = match result {
                Ok(dent) => dent,
                Err(err) => {
                    bail!(err)
                }
            };

            let is_log_file = format!("{:?}", dent.file_name()).contains("log");

            if !dent.file_type().is_file() && !is_log_file {
                continue;
            }

            Searcher::new().search_path(
                &matcher,
                dent.path(),
                UTF8(|lnum, line| {
                    // Now per node
                    let mut node_file_path = dent.path().to_path_buf();

                    // get the containing dir
                    let _result = node_file_path.pop();
                    let node_name = node_file_path
                        .file_name()
                        .expect("node dir name can be parsed")
                        .to_str()
                        .expect("node dir name can be parsed to str")
                        .to_string();

                    // We are guaranteed to find a match, so the unwrap is OK.
                    let mymatch = matcher.find(line.as_bytes())?.unwrap();

                    let file_matches = matches.entry(node_name).or_insert_with(Vec::new);

                    // let file_matches = matches.remove(node_name).ok_or_else(vec![])?;
                    file_matches.push((lnum, line[mymatch].to_string()));

                    Ok(true)
                }),
            )?;
        }
    }

    Ok(matches)
}
