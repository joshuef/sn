// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use eyre::{bail, Error, Result};
use grep::matcher::Matcher;
use grep::regex::RegexMatcher;
use grep::searcher::sinks::UTF8;
use grep::searcher::Searcher;
use sn_interface::types::log_markers::LogMarker;
use std::str::FromStr;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use structopt::{clap::AppSettings::ColoredHelp, StructOpt};
use strum::IntoEnumIterator;
use walkdir::WalkDir;

#[derive(StructOpt, Debug)]
/// Inspect Safe Network local testnet logs
#[structopt(global_settings(&[ColoredHelp]))]
struct CmdArgs {
    /// sub cmds
    #[structopt(subcommand)]
    pub cmd: Option<SubCmds>,
    /// Path to the testnet logs folder, e.g. ~/.safe/node/local-test-network
    pub logs_path: PathBuf,
    /// Show stats per node? (this is slower)
    #[structopt(short)]
    pub nodes: bool,
}

#[derive(StructOpt, Debug)]
enum SubCmds {
    /// Generate a report of cmds and corresponding sub-cmds
    // TODO: make the cmd-id optional, to report all cmds
    Cmds {
        /// ID of the cmd to obtain a report for, e.g. 924678512
        cmd_id: String,
    },
    /// Generate a report of cmds dispatched to process incoming msgs
    // TODO: make the msg-id optional, to report all msgs
    Msgs {
        /// ID of the msg to obtain a report for, e.g. c971..cfb5
        msg_id: String,
    },
    /// Generate a report of cmds dispatched, and which were started but not completed
    IncompleteCmds,
}

fn main() -> Result<()> {
    let args = CmdArgs::from_args();
    let report = inspect_log_files(&args)?;

    println!();
    if let Some(cmd) = args.cmd {
        match cmd {
            SubCmds::Cmds { cmd_id } => {
                if report.is_empty() {
                    println!("** No cmds were found for cmd id {} **", cmd_id);
                } else {
                    println!(
                        "*** REPORT: The following cmds were found for cmd id {} ***",
                        cmd_id
                    );
                    for (cmd_id, log_entries) in report.iter() {
                        println!("==> Log entries for sub-cmd {}:", cmd_id);
                        for log_entry in log_entries.iter() {
                            println!("{}", log_entry);
                        }
                        println!();
                    }
                }
            }
            SubCmds::Msgs { msg_id } => {
                if report.is_empty() {
                    println!("** No cmds were found for msg id {} **", msg_id);
                } else {
                    println!(
                        "*** REPORT: The following cmds were found for msg id {} ***",
                        msg_id
                    );
                    for (cmd_id, log_entries) in report.iter() {
                        println!("==> Log entries for sub-cmd {}:", cmd_id);
                        for log_entry in log_entries.iter() {
                            println!("{}", log_entry);
                        }
                        println!();
                    }
                }
            }
            SubCmds::IncompleteCmds => {
                if report.is_empty() {
                    println!("** No errors detected in any of the logs scanned! **");
                } else {
                    println!("*** REPORT: The following issues were detected ***");
                    for (logfile, log_entries) in report.iter() {
                        println!("Cmds not completed in log {}:", logfile);
                        for log_entry in log_entries.iter() {
                            println!("{}", log_entry);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// A cmd/sub-cmd id e.g. "963111461", "963111461.0"
type CmdId = String;
// Entire log entry as read from log files
type LogEntry = String;
//  A msg id, e.g. "68fe..b776"
type MsgId = String;
//  A nodes id, as oer log folder, eg sn-node-14
type NodeId = String;

#[derive(Default, Clone)]
// Tuple with log filepath, and the list of cmds/sub-cmds and corresponding log entry.
pub struct SubCmdsInfo {
    pub logfile: PathBuf,
    pub cmd_logs: BTreeMap<CmdId, LogEntry>,
}

impl SubCmdsInfo {
    pub fn new(logfile: PathBuf) -> Self {
        Self {
            logfile,
            cmd_logs: BTreeMap::default(),
        }
    }

    pub fn insert(&mut self, cmd_id: CmdId, log_entry: LogEntry) {
        self.cmd_logs.insert(cmd_id, log_entry);
    }
}

// Container for all info per each cmd root id
type CmdsInfoList = BTreeMap<CmdId, SubCmdsInfo>;

#[derive(Default, Clone)]
pub struct ScannedInfo {
    // markers per node node
    pub by_node: BTreeMap<NodeId, BTreeMap<LogMarker, CmdsInfoList>>,
    // msgs handled per node
    pub msgs_per_node: BTreeMap<NodeId, Vec<MsgId>>,
    // log marker to cmd info
    pub by_marker: BTreeMap<LogMarker, CmdsInfoList>,
    // msg id to cmd root ids (can be same msg id received many times)
    pub cmd_by_msg_id: BTreeMap<MsgId, Vec<CmdId>>,
    // msg id tied to a cmd that it spawned
    pub counts: BTreeMap<LogMarker, usize>,
}

/// Search the local-test-network log file and return count
pub fn update_cmds_info_for_markers(
    path: &Path,
    stats_per_node: bool,
) -> Result<ScannedInfo, Error> {
    let paths = [path];
    let mut info = ScannedInfo {
        by_marker: BTreeMap::default(),
        counts: BTreeMap::default(),
        cmd_by_msg_id: BTreeMap::default(),
        by_node: BTreeMap::default(),
        msgs_per_node: BTreeMap::default(),
    };

    let mut pattern = r"".to_owned();

    for (i, marker) in LogMarker::iter().enumerate() {
        if i > 0 {
            // lets add an OR
            pattern.push('|');
        }
        pattern.push_str(&marker.to_string())
    }

    let matcher = RegexMatcher::new_line_matcher(&pattern)?;
    let cmd_id_regex = RegexMatcher::new_line_matcher(r".*cmd_id=([^\s-]*)")?;
    let msg_id_regex = RegexMatcher::new_line_matcher(r".*MsgId\((.*)\)")?;

    for path in paths {
        for result in WalkDir::new(path) {
            let dent = match result {
                Ok(dent) => dent,
                Err(err) => {
                    bail!(err)
                }
            };

            if !dent.file_type().is_file() {
                continue;
            }

            Searcher::new().search_path(
                &matcher,
                dent.path(),
                UTF8(|_lnum, line| {
                    let path = dent.path().to_path_buf();
                    let the_match = matcher.find(line.as_bytes())?.unwrap();
                    let matched_marker = &line[the_match].to_string();
                    let matched_marker =
                        LogMarker::from_str(matched_marker).expect("match to be log marker");

                    // update the total count for this match
                    let count = info.counts.entry(matched_marker.clone()).or_insert(0);
                    *count += 1;

                    if let Some(cmd_id_match) = cmd_id_regex.find(line.as_bytes())? {
                        let cmd_id = &line[cmd_id_match].to_string();
                        let root_cmd_id = get_root_cmd_id(cmd_id);

                        // update the specifics of this marker
                        let marker_map = info
                            .by_marker
                            .entry(matched_marker.clone())
                            .or_insert_with(BTreeMap::default);

                        let sub_cmd_info = marker_map
                            .entry(root_cmd_id.clone())
                            .or_insert_with(|| SubCmdsInfo::new(path.clone()));

                        // track the entry
                        sub_cmd_info.insert(cmd_id.clone(), line.to_string());

                        // Now per node
                        let mut node_file_path = dent.path().to_path_buf();
                        // get the containing dir
                        node_file_path.pop();

                        let node_name = node_file_path
                            .file_name()
                            .expect("node dir name can be parsed")
                            .to_str()
                            .expect("node dir name can be parsed to str")
                            .to_string();

                        if stats_per_node {
                            let nodes_map = info
                                .by_node
                                .entry(node_name.clone())
                                .or_insert_with(BTreeMap::default);

                            let nodes_markers_map = nodes_map
                                .entry(matched_marker)
                                .or_insert_with(BTreeMap::default);

                            // nodes_markers_map.insert(root_cmd_id.clone(), value)
                            let per_node_sub_cmd_info = nodes_markers_map
                                .entry(root_cmd_id.clone())
                                .or_insert_with(|| SubCmdsInfo::new(path.clone()));

                            per_node_sub_cmd_info.insert(cmd_id.clone(), line.to_string());
                        }

                        // And messaging related tracking...
                        if let Some(msg_id_match) = msg_id_regex.find(line.as_bytes())? {
                            let msg_id = &line[msg_id_match].to_string();

                            info.cmd_by_msg_id
                                .entry(msg_id.to_string())
                                .or_insert_with(Vec::new)
                                .push(root_cmd_id);

                            info.msgs_per_node
                                .entry(node_name)
                                .or_insert_with(Vec::new)
                                .push(msg_id.to_string());
                        }
                    }

                    Ok(true)
                }),
            )?;
        }
    }

    Ok(info)
}

fn inspect_log_files(args: &CmdArgs) -> Result<BTreeMap<String, Vec<String>>> {
    println!(
        "Inspecting testnet logs folder: {}",
        args.logs_path.display()
    );

    let mut report = BTreeMap::<String, Vec<String>>::new();

    let info = update_cmds_info_for_markers(&args.logs_path, args.nodes)?;

    println!("-------------------------");
    println!("LogMarker Stats: (markers found across all log files): ");
    for (marker, count) in info.clone().counts.into_iter() {
        println!("{} found: {} times", marker, count);
    }
    println!("-------------------------");

    let default_map = BTreeMap::default();
    let spawned = info
        .by_marker
        .get(&LogMarker::CmdHandlingSpawned)
        .unwrap_or(&default_map);

    let started = info
        .by_marker
        .get(&LogMarker::CmdProcessStart)
        .unwrap_or(&default_map);
    let succeeded = info
        .by_marker
        .get(&LogMarker::CmdProcessEnd)
        .unwrap_or(&default_map);
    let failed = info
        .by_marker
        .get(&LogMarker::CmdProcessingError)
        .unwrap_or(&default_map);

    if let Some(cmd) = &args.cmd {
        match cmd {
            SubCmds::Cmds { ref cmd_id } => {
                populate_cmds_tree(spawned, started, succeeded, failed, &mut report, cmd_id);
            }
            SubCmds::Msgs { ref msg_id } => {
                populate_cmds_tree_for_msgs(
                    &info,
                    spawned,
                    started,
                    succeeded,
                    failed,
                    &mut report,
                    msg_id,
                );
            }
            SubCmds::IncompleteCmds => {
                check_completed_cmds(&info, spawned, started, succeeded, failed, &mut report);
            }
        }
    }

    for (node_name, markers) in info.by_node {
        println!("///////////////////////");
        println!("Node {:?}", node_name);
        println!("///////////////////////");

        if markers.get(&LogMarker::PromotedToElder).is_some() {
            println!("** Has been an elder **");
        }
        println!();
        if let Some(cmds) = markers.get(&LogMarker::CmdHandlingSpawned) {
            println!("Spawned cmds: {:?}", cmds.len());
        }
        if let Some(cmds) = markers.get(&LogMarker::CmdProcessStart) {
            println!("Started cmds: {:?}", cmds.len());
        }
        if let Some(cmds) = markers.get(&LogMarker::CmdProcessEnd) {
            println!("Succeeded cmds: {:?}", cmds.len());
        }
        if let Some(cmds) = markers.get(&LogMarker::CmdProcessingError) {
            println!("Errored cmds: {:?}", cmds.len());
        }

        if let Some(msgs) = info.msgs_per_node.get(&node_name) {
            println!("Messages handled by the node: {:?}", msgs.len());
        }
        println!();
        println!();
    }

    Ok(report)
}

// Try to find inconsistencies among the cmds,
// trying to find those which were started but not completed
fn check_completed_cmds(
    info: &ScannedInfo,
    spawned: &CmdsInfoList,
    started: &CmdsInfoList,
    succeeded: &CmdsInfoList,
    failed: &CmdsInfoList,
    report: &mut BTreeMap<String, Vec<String>>,
) {
    let mut cmds_with_error = vec![];
    let mut cmds_not_completed = vec![];
    let mut cmds_with_end = 0;

    println!("Checking completed cmds...");
    println!("-------------------------");

    for (cmd_id, log_entry, logfile) in spawned.iter().flat_map(|(_, sub_cmds)| {
        let mut cmds = vec![];
        for (cmd_id, log_entry) in sub_cmds.cmd_logs.iter() {
            cmds.push((cmd_id, log_entry, sub_cmds.logfile.clone()))
        }

        cmds
    }) {
        let logfile = logfile.display().to_string();

        let root_cmd_id = get_root_cmd_id(cmd_id);

        if started
            .get(&root_cmd_id)
            .and_then(|sub_cmds| sub_cmds.cmd_logs.get(cmd_id))
            .is_none()
        {
            println!(
                "Cmd with id {} spawned but not started: {}",
                cmd_id, log_entry
            );

            cmds_not_completed.push((cmd_id, logfile.clone(), log_entry));
            report
                .entry(logfile)
                .or_insert_with(Vec::new)
                .push(log_entry.clone());
        } else {
            // cmd spawned and started, let's see if it completed...
            if succeeded
                .get(&root_cmd_id)
                .and_then(|sub_cmds| sub_cmds.cmd_logs.get(cmd_id))
                .is_none()
            {
                if failed
                    .get(&root_cmd_id)
                    .and_then(|sub_cmds| sub_cmds.cmd_logs.get(cmd_id))
                    .is_none()
                {
                    cmds_with_error.push((cmd_id, logfile, log_entry));
                } else {
                    println!(
                        "Cmd with id {} spawned and started, but not completed: {}",
                        cmd_id, log_entry
                    );
                    cmds_not_completed.push((cmd_id, logfile.clone(), log_entry));
                    report
                        .entry(logfile)
                        .or_insert_with(Vec::new)
                        .push(log_entry.clone());
                }
            } else {
                // cmd completed
                cmds_with_end += 1;
            }
        }
    }

    println!(
        "Cmds handled which Failed: {}, Succeeded: {}, not Completed: {}",
        cmds_with_error.len(),
        cmds_with_end,
        cmds_not_completed.len()
    );
    println!("Incoming msgs handled: {}", info.cmd_by_msg_id.len());

    if !cmds_not_completed.is_empty() {
        println!("\n!!! ERROR !!!: Some cmd/s were not completed in log:");
        for (id, file, line) in cmds_not_completed {
            println!("{}{}{}", id, file, line);
        }
    }
    println!();

    if !cmds_with_error.is_empty() {
        println!("\n!!! ERROR !!!: Some cmds produced errors:");
        for (id, file, line) in cmds_with_error {
            println!("{} {} {}", id, file, line);
        }
    }

    println!("-------------------------");
}

// Populate the report with the list of cmds/sub-cmds correlated to the provided cmd id.
fn populate_cmds_tree(
    spawned: &CmdsInfoList,
    started: &CmdsInfoList,
    succeeded: &CmdsInfoList,
    failed: &CmdsInfoList,
    report: &mut BTreeMap<String, Vec<String>>,
    cmd_id: &str,
) {
    println!("Looking for cmds spawned from cmd id {}", cmd_id);
    let root_cmd_id = get_root_cmd_id(cmd_id);

    populate_tree_for(spawned, &root_cmd_id, report);
    populate_tree_for(started, &root_cmd_id, report);
    populate_tree_for(succeeded, &root_cmd_id, report);
    populate_tree_for(failed, &root_cmd_id, report);
}

fn populate_tree_for(
    cmd_list: &CmdsInfoList,
    root_cmd_id: &str,
    report: &mut BTreeMap<String, Vec<String>>,
) {
    if let Some(matching_cmds) = cmd_list.get(root_cmd_id) {
        matching_cmds
            .cmd_logs
            .iter()
            .for_each(|(cmd_id, log_entry)| {
                report
                    .entry(cmd_id.clone())
                    .or_insert_with(Vec::new)
                    .push(format!(
                        "{}: {}",
                        matching_cmds.logfile.display(),
                        log_entry
                    ));
            });
    }
}

// Populate the report with the list of message ids and their correlated cmds.
fn populate_cmds_tree_for_msgs(
    info: &ScannedInfo,
    spawned: &CmdsInfoList,
    started: &CmdsInfoList,
    succeeded: &CmdsInfoList,
    failed: &CmdsInfoList,
    report: &mut BTreeMap<String, Vec<String>>,
    msg_id: &str,
) {
    println!("Looking for cmds spawned from msg id {}", msg_id);

    if let Some(ids) = info.cmd_by_msg_id.get(msg_id) {
        ids.iter().for_each(|root_cmd_id| {
            populate_cmds_tree(spawned, started, succeeded, failed, report, root_cmd_id);
        });
    }
}

// Given a cmd id, return the root id, e.g. the root cmd id of 'abc.1.0.2' is 'a.b.c'.
fn get_root_cmd_id(cmd_id: &str) -> CmdId {
    let mut root_cmd_id = cmd_id.to_string();
    root_cmd_id.truncate(cmd_id.find('.').unwrap_or(cmd_id.len()));
    root_cmd_id
}
