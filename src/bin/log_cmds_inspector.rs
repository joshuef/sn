use eyre::{bail, Error, Result};
use grep::matcher::Matcher;
use grep::regex::RegexMatcher;
use grep::searcher::sinks::UTF8;
use grep::searcher::Searcher;
use safe_network::routing::log_markers::LogMarker;
use std::str::FromStr;
use std::{collections::BTreeMap, path::PathBuf};
use structopt::{clap::AppSettings::ColoredHelp, StructOpt};
use strum::IntoEnumIterator;
use walkdir::WalkDir;

#[derive(StructOpt, Debug)]
/// Inspect Safe Network local testnet logs
#[structopt(global_settings(&[ColoredHelp]))]
struct CmdArgs {
    /// subcommands
    #[structopt(subcommand)]
    pub cmd: SubCommands,
    /// Path to the testnet logs folder, e.g. ~/.safe/node/local-test-network
    // #[structopt(default_value="$HOME/.safe/node/local-test-network/")]
    pub logs_path: String,
}

#[derive(StructOpt, Debug)]
enum SubCommands {
    /// Generate a report of commands and corresponding sub-commands
    // TODO: make the cmd-id optional, to report all commands
    Commands {
        /// ID of the command to obtain a report for, e.g. 924678512
        cmd_id: String,
    },
    /// Generate a report of commands dispatched to process incoming messages
    // TODO: make the msg-id optional, to report all messages
    Messages {
        /// ID of the message to obtain a report for, e.g. c971..cfb5
        msg_id: String,
    },
    /// Generate a report of commands dispatched, and which were started but not completed
    IncompleteCmds,
}

fn main() -> Result<()> {
    let args = CmdArgs::from_args();
    let report = inspect_log_files(&args)?;

    println!();
    match args.cmd {
        SubCommands::Commands { cmd_id } => {
            println!(
                "*** REPORT: The following commands were found for cmd id {} ***",
                cmd_id
            );
            for (cmd_id, log_entries) in report.iter() {
                println!("==> Log entries for sub-command {}:", cmd_id);
                for log_entry in log_entries.iter() {
                    println!("{}", log_entry);
                }
                println!();
            }
        }
        SubCommands::Messages { msg_id } => {
            println!(
                "*** REPORT: The following commands were found for msg id {} ***",
                msg_id
            );
            for (cmd_id, log_entries) in report.iter() {
                println!("==> Log entries for sub-command {}:", cmd_id);
                for log_entry in log_entries.iter() {
                    println!("{}", log_entry);
                }
                println!();
            }
        }
        SubCommands::IncompleteCmds => {
            if report.is_empty() {
                println!("** No errors detected in any of the logs scanned! **");
            } else {
                println!("*** REPORT: The following issues were detected ***");
                for (logfile, log_entries) in report.iter() {
                    println!("Commands not completed in log {}:", logfile);
                    for log_entry in log_entries.iter() {
                        println!("{}", log_entry);
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Default, Clone)]
// Tuple with log filepath, and the list of sommands/sub-commands and corresponding log entry.
pub struct SubCommandsInfo {
    pub logfile: PathBuf,
    pub cmds_logs: BTreeMap<String, String>,
}

impl SubCommandsInfo {
    pub fn new(logfile: PathBuf) -> Self {
        Self {
            logfile,
            cmds_logs: BTreeMap::default(),
        }
    }

    pub fn insert(&mut self, cmd_id: String, log_entry: String) {
        self.cmds_logs.insert(cmd_id, log_entry);
    }
}

#[derive(Default, Clone)]
pub struct ScannedInfo {
    // TODO: by node
    pub by_marker: BTreeMap<LogMarker, BTreeMap<String, SubCommandsInfo>>,
    // msg id to cmd root ids (can be same msg id received many times)
    pub cmd_by_msg_id: BTreeMap<String, Vec<String>>,
    // msg id tied to a command that it spawned
    // pub msg_id_to_cmd_id: BTreeMap<String, String>,
    // pub by_file: BTreeMap<String, BTreeMap<String, SubCommandsInfo>>,
    pub counts: BTreeMap<LogMarker, usize>,
}

/// Search the local-test-network log file and return count
pub fn update_commands_info_for_markers(path: &PathBuf) -> Result<ScannedInfo, Error> {
    let paths = [path];
    let mut info = ScannedInfo {
        // by_file: BTreeMap::default(),
        by_marker: BTreeMap::default(),
        counts: BTreeMap::default(),
        cmd_by_msg_id: BTreeMap::default(),
        // msg_id_to_cmd_id: BTreeMap::default(),
    };

    let mut pattern = r"".to_owned();

    for (i, marker) in LogMarker::iter().enumerate() {
        if i > 0 {
            // lets add an OR
            pattern.push_str(r"|");
        }
        pattern.push_str(&marker.to_string())
    }

    let matcher = RegexMatcher::new_line_matcher(&pattern)?;
    let cmd_id_regex = RegexMatcher::new_line_matcher(r".*cmd_id=([^\s-]*)")?;
    let msg_id_regex = RegexMatcher::new_line_matcher(r".*MessageId\((.*)\)")?;

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
                        LogMarker::from_str(&matched_marker).expect("match to be log marker");

                    // update the total count for this match
                    let count = info.counts.entry(matched_marker.clone()).or_insert(0);
                    *count += 1;

                    if let Some(cmd_id_match) = cmd_id_regex.find(line.as_bytes())? {
                        let cmd_id = &line[cmd_id_match].to_string();
                        let root_cmd_id = get_root_cmd_id(&cmd_id);

                        // update the specifics of this marker
                        let marker_map = info
                            .by_marker
                            .entry(matched_marker.clone())
                            .or_insert(BTreeMap::default());

                        let subcommand_info = marker_map
                            .entry(root_cmd_id.clone())
                            .or_insert_with(|| SubCommandsInfo::new(path.clone()));

                        // track the entry
                        subcommand_info.insert(cmd_id.clone(), line.to_string());

                        if let Some(msg_id_match) = msg_id_regex.find(line.as_bytes())? {
                            let msg_id = &line[msg_id_match].to_string();

                            info.cmd_by_msg_id
                                .entry(msg_id.to_string())
                                .or_insert_with(Vec::new)
                                .push(root_cmd_id);

                            // info.
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
    println!("Inspecting testnet logs folder: {}", args.logs_path);
    let logs_dir = std::path::PathBuf::from(args.logs_path.clone());

    let mut report = BTreeMap::<String, Vec<String>>::new();

    let info = update_commands_info_for_markers(&logs_dir)?;

    println!("-------------------------");
    println!("LogMarker Stats: (markers found across all log files): ");
    for (marker, count) in info.clone().counts.into_iter() {
        println!("{} found: {} times", marker, count);
    }
    println!("-------------------------");

    //     if is_elder {
    //         println!("************************");
    //         println!("Node is Elder");
    //         println!("************************");

    //     }
    let default_map = BTreeMap::default();
    let spawned = info
        .by_marker
        .get(&LogMarker::CommandHandleSpawned)
        .unwrap_or(&default_map);
    let started = info
        .by_marker
        .get(&LogMarker::CommandHandleStart)
        .unwrap_or(&default_map);
    let succeeded = info
        .by_marker
        .get(&LogMarker::CommandHandleEnd)
        .unwrap_or(&default_map);
    let failed = info
        .by_marker
        .get(&LogMarker::CommandHandleError)
        .unwrap_or(&default_map);

    println!(
        "Commands Spawned: {}, Started: {}, Succeeded: {}, Produced an error: {}",
        spawned.len(),
        started.len(),
        succeeded.len(),
        failed.len()
    );
    println!("-------------------------");

    match args.cmd {
        SubCommands::Commands { ref cmd_id } => {
            populate_commands_tree(spawned, started, succeeded, failed, &mut report, cmd_id);
        }
        SubCommands::Messages { ref msg_id } => {
            populate_commands_tree_for_msgs(
                &info,
                spawned,
                started,
                succeeded,
                failed,
                &mut report,
                msg_id,
            );
        }
        SubCommands::IncompleteCmds => {
            check_completed_cmds(&info, spawned, started, succeeded, failed, &mut report);
        }
    }

    Ok(report)
}

// Try to find inconsistencies among the commands,
// trying to find those which were started but not completed
fn check_completed_cmds(
    info: &ScannedInfo,
    spawned: &BTreeMap<String, SubCommandsInfo>,
    started: &BTreeMap<String, SubCommandsInfo>,
    succeeded: &BTreeMap<String, SubCommandsInfo>,
    failed: &BTreeMap<String, SubCommandsInfo>,
    report: &mut BTreeMap<String, Vec<String>>,
    // node_log_filepath: &Path,
) {
    let mut cmds_with_error = vec![];
    let mut cmds_not_completed = vec![];
    let mut cmds_with_end = 0;

    println!("Checking completed commands...");
    println!("-------------------------");

    for (cmd_id, log_entry, logfile) in spawned.iter().flat_map(|(_, subcommands)| {
        let mut commands = vec![];
        for (cmd_id, log_entry) in subcommands.cmds_logs.iter() {
            commands.push((cmd_id, log_entry, subcommands.logfile.clone()))
        }

        commands
    }) {
        let logfile = logfile.display().to_string();

        let root_cmd_id = get_root_cmd_id(cmd_id);

        if started
            .get(&root_cmd_id)
            .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
            .is_none()
        {
            println!(
                "Command with id {} spawned but not started: {}",
                cmd_id, log_entry
            );

            cmds_not_completed.push((cmd_id, logfile.clone(), log_entry));
            report
                .entry(logfile)
                .or_insert_with(Vec::new)
                .push(log_entry.clone());
        } else {
            // Command spwned and started, let's see if it completed...
            if succeeded
                .get(&root_cmd_id)
                .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
                .is_none()
            {
                if failed
                    .get(&root_cmd_id)
                    .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
                    .is_none()
                {
                    cmds_with_error.push((cmd_id, logfile, log_entry));
                } else {
                    println!(
                        "Command with id {} spawned and started, but not completed: {}",
                        cmd_id, log_entry
                    );
                    cmds_not_completed.push((cmd_id, logfile.clone(), log_entry));
                    report
                        .entry(logfile)
                        .or_insert_with(Vec::new)
                        .push(log_entry.clone());
                }
            } else {
                // command completed
                cmds_with_end += 1;
            }
        }
    }

    println!(
        "Commands handled which Failed: {}, Succeeded: {}, not Completed: {}",
        cmds_with_error.len(),
        cmds_with_end,
        cmds_not_completed.len()
    );
    println!("Incoming messages handled: {}", info.cmd_by_msg_id.len());

    if cmds_not_completed.len() > 0 {
        println!("\n!!! ERROR !!!: Some command/s were not completed in log:");
        for (id, file, line) in cmds_not_completed {
            println!("{}{}{}", id, file, line);
        }
    }
    println!("-------------------------");

    if cmds_with_error.len() > 0 {
        println!("\n!!! ERROR !!!: Some commands errored but were not handled?:");
        for (id, file, line) in cmds_with_error {
            println!("{}{}{}", id, file, line);
        }
    }
    println!("-------------------------");
}

// Populate the report with the list of commands/sub-commands correlated to the provided cmd id.
fn populate_commands_tree(
    spawned: &BTreeMap<String, SubCommandsInfo>,
    started: &BTreeMap<String, SubCommandsInfo>,
    succeeded: &BTreeMap<String, SubCommandsInfo>,
    failed: &BTreeMap<String, SubCommandsInfo>,
    report: &mut BTreeMap<String, Vec<String>>,
    cmd_id: &str,
) {
    println!("Looking for commands spawned from cmd id {}", cmd_id);
    let root_cmd_id = get_root_cmd_id(cmd_id);

    if let Some(matching_cmds) = spawned.get(&root_cmd_id) {
        matching_cmds
            .cmds_logs
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

    if let Some(matching_cmds) = started.get(&root_cmd_id) {
        matching_cmds
            .cmds_logs
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

    if let Some(matching_cmds) = succeeded.get(&root_cmd_id) {
        matching_cmds
            .cmds_logs
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

    if let Some(matching_cmds) = failed.get(&root_cmd_id) {
        matching_cmds
            .cmds_logs
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

// Populate the report with the list of message ids and their correlated commands.
fn populate_commands_tree_for_msgs(
    info: &ScannedInfo,
    spawned: &BTreeMap<String, SubCommandsInfo>,
    started: &BTreeMap<String, SubCommandsInfo>,
    succeeded: &BTreeMap<String, SubCommandsInfo>,
    failed: &BTreeMap<String, SubCommandsInfo>,
    report: &mut BTreeMap<String, Vec<String>>,
    msg_id: &str,
) {
    println!("Looking for commands spawned from msg id {}", msg_id);

    if let Some(ids) = info.cmd_by_msg_id.get(msg_id) {
        ids.iter().for_each(|root_cmd_id| {
            populate_commands_tree(spawned, started, succeeded, failed, report, root_cmd_id);
        });
    }
}

// Given a command id, return the root id, e.g. the root cmd id of 'abc.1.0.2' is 'a.b.c'.
fn get_root_cmd_id(cmd_id: &str) -> String {
    let mut root_cmd_id = cmd_id.to_string();
    root_cmd_id.truncate(cmd_id.find('.').unwrap_or(cmd_id.len()));
    root_cmd_id
}
