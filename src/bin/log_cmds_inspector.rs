use eyre::{eyre, Result};
use regex::Regex;
use safe_network::routing::log_markers::LogMarker;
use safe_network::testnet_grep::{get_count_in_logfile, search_logfile_get_whole_line};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use structopt::{clap::AppSettings::ColoredHelp, StructOpt};

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

#[derive(Default)]
// Tuple with log filepath, and the list of sommands/sub-commands and corresponding log entry.
struct SubCommandsInfo {
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

#[derive(Default)]
// Maps from cmd_id to a sub-commands info.
struct ScannedCommands {
    pub spawned: BTreeMap<String, SubCommandsInfo>,
    pub started: BTreeMap<String, SubCommandsInfo>,
    pub succeeded: BTreeMap<String, SubCommandsInfo>,
    pub failed: BTreeMap<String, SubCommandsInfo>,
    pub handle_msg: BTreeMap<String, (PathBuf, String)>,
    // Map from msg_id to cmd_ids
    pub msg_id_to_cmds: BTreeMap<String, Vec<String>>,
}

/// Update passed command tree with data for a specific LogMarker from a specific path
fn update_command_info_for_marker(
    path: &PathBuf,
    marker: &LogMarker,
    commands: &mut ScannedCommands,
) -> Result<()> {
    let cmd_id_regex = Regex::new(r".*cmd_id=([^\s-]*)")?;
    let msg_id_regex = Regex::new(r".*MessageId\((.*)\)")?;

    let regex_send_msg =
        Regex::new(r".*CommandHandleSpawned SendMessage MessageId\((.*)\) cmd_id=(.*)$")?;

    let results = search_logfile_get_whole_line(path, marker)?;

    for (_line, text, file) in results {
        if let Some(cap) = cmd_id_regex.captures_iter(&text).next() {
            let cmd_id = cap[1].to_string();
            let root_cmd_id = get_root_cmd_id(&cmd_id);

            if marker == &LogMarker::DispatchHandleMsgCmd {
                // track msg_id if we can find it
                if let Some(cap) = msg_id_regex.captures_iter(&text).next() {
                    let msg_id = cap[1].to_string();
                    commands
                        .handle_msg
                        .insert(msg_id.clone(), (file.clone(), text.clone()));

                    commands
                        .msg_id_to_cmds
                        .entry(msg_id)
                        .or_insert_with(Vec::new)
                        .push(root_cmd_id);
                }
            } else {
                let tracker = match marker {
                    LogMarker::CommandHandleSpawned => &mut commands.spawned,
                    LogMarker::CommandHandleStart => &mut commands.started,
                    LogMarker::CommandHandleEnd => &mut commands.succeeded,
                    LogMarker::CommandHandleError => &mut commands.failed,
                    _ => return Err(eyre!("No tracker set up for LogMarker {:?}", marker)),
                };

                tracker
                    .entry(root_cmd_id.clone())
                    .or_insert_with(|| SubCommandsInfo::new(path.clone()))
                    .insert(cmd_id.clone(), text.clone());

                // track msg_id if we can find it
                if let Some(cap) = msg_id_regex.captures_iter(&text).next() {
                    let msg_id = cap[1].to_string();

                    commands
                        .msg_id_to_cmds
                        .entry(msg_id)
                        .or_insert_with(Vec::new)
                        .push(root_cmd_id);
                }
            }
        }

        if let Some(cap) = regex_send_msg.captures_iter(&text).next() {
            let msg_id = cap[1].to_string();
            let cmd_id = cap[2].to_string();
            let root_cmd_id = get_root_cmd_id(&cmd_id);
            commands
                .msg_id_to_cmds
                .entry(msg_id)
                .or_insert_with(Vec::new)
                .push(root_cmd_id);
        }
    }

    Ok(())
}

fn inspect_log_files(args: &CmdArgs) -> Result<BTreeMap<String, Vec<String>>> {
    println!("Inspecting testnet logs folder: {}", args.logs_path);

    let logs_dir = std::fs::read_dir(args.logs_path.clone())?;

    let mut report = BTreeMap::<String, Vec<String>>::new();

    // Iterate over each of the testnet nodes log files
    for node_log_path in logs_dir {
        let node_log_filepath = node_log_path?.path();
        println!();
        println!("-------------------------");
        println!("Inspecting node's folder: {}", node_log_filepath.display());
        println!("-------------------------");

        let mut commands = ScannedCommands::default();

        let is_elder =
            !search_logfile_get_whole_line(&node_log_filepath, &LogMarker::PromotedToElder)?
                .is_empty();

        // TODO: Could the regex just be against _any_ command and return all those?
        let _res = update_command_info_for_marker(
            &node_log_filepath,
            &LogMarker::CommandHandleSpawned,
            &mut commands,
        );
        let _res = update_command_info_for_marker(
            &node_log_filepath,
            &LogMarker::CommandHandleStart,
            &mut commands,
        );
        let _res = update_command_info_for_marker(
            &node_log_filepath,
            &LogMarker::CommandHandleEnd,
            &mut commands,
        );
        let _res = update_command_info_for_marker(
            &node_log_filepath,
            &LogMarker::CommandHandleError,
            &mut commands,
        );

        let _res = update_command_info_for_marker(
            &node_log_filepath,
            &LogMarker::DispatchHandleMsgCmd,
            &mut commands,
        );

        // TODO: how can we get a list of all routing commands programatically?
        // TODO: add anything new here
        let routing_commands_to_check = vec![
            "HandlePeerLost",
            "ScheduleTimeout",
            "HandleSystemMessage",
            "HandleMessage",
            "HandleBlockingMessage",
            "HandleNonBlockingMessage",
            "HandlePeerLost",
            "HandleAgreement",
            "HandleDkgOutcome",
            "HandleDkgFailure",
            "SendMessage",
            "ParseAndSendWireMsg",
            "PrepareNodeMsgToSend",
            "SendMessageDeliveryGroup",
            "HandleRelocationComplete",
            "SetJoinsAllowed",
            "ProposeOnline",
            "ProposeOffline",
            "StartConnectivityTest",
            "TestConnectivity",
        ];

        if is_elder {
            println!("Node is Elder");
        }
        println!("-------------------------");

        println!(
            "Commands Spawned: {}, Started: {}, Succeeded: {}, Failed: {}",
            commands.spawned.len(),
            commands.started.len(),
            commands.succeeded.len(),
            commands.failed.len()
        );
        println!("-------------------------");

        for command in routing_commands_to_check {
            let count = get_count_in_logfile(&node_log_filepath, command)?;
            println!("{} found {:?} times", command, count);
        }

        match args.cmd {
            SubCommands::Commands { ref cmd_id } => {
                populate_commands_tree(&commands, &mut report, cmd_id);
            }
            SubCommands::Messages { ref msg_id } => {
                populate_commands_tree_for_msgs(&commands, &mut report, msg_id);
            }
            SubCommands::IncompleteCmds => {
                check_completed_cmds(&commands, &mut report, &node_log_filepath);
            }
        }
    }

    Ok(report)
}

// Try to find inconsistencies among the commands,
// trying to find those which were started but not completed
fn check_completed_cmds(
    commands: &ScannedCommands,
    report: &mut BTreeMap<String, Vec<String>>,
    node_log_filepath: &Path,
) {
    let mut cmds_with_error = 0;
    let mut cmds_not_completed = 0;
    let mut cmds_with_end = 0;
    for (cmd_id, log_entry) in commands
        .spawned
        .iter()
        .flat_map(|(_, v)| v.cmds_logs.iter())
    {
        let root_cmd_id = get_root_cmd_id(cmd_id);
        if commands
            .started
            .get(&root_cmd_id)
            .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
            .is_none()
        {
            println!(
                "Command with id {} spawned but not started: {}",
                cmd_id, log_entry
            );
            cmds_not_completed += 1;
            let logfile = node_log_filepath.display().to_string();
            report
                .entry(logfile)
                .or_insert_with(Vec::new)
                .push(log_entry.clone());
        } else {
            // Command spwned and started, let's see if it completed...
            if commands
                .succeeded
                .get(&root_cmd_id)
                .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
                .is_none()
            {
                // it didn't finish succesfully, did it fail?....
                if commands
                    .failed
                    .get(&root_cmd_id)
                    .map(|subcmds| subcmds.cmds_logs.get(cmd_id))
                    .is_none()
                {
                    cmds_with_error += 1;
                } else {
                    println!(
                        "Command with id {} spawned and started, but not completed: {}",
                        cmd_id, log_entry
                    );
                    cmds_not_completed += 1;
                    let logfile = node_log_filepath.display().to_string();
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
        cmds_with_error, cmds_with_end, cmds_not_completed
    );
    println!(
        "Incoming messages handled: {}",
        commands.msg_id_to_cmds.len()
    );

    if cmds_not_completed > 0 {
        println!(
            "\n!!! ERROR !!!: Some command/s were not completed in log: {}",
            node_log_filepath.display()
        );
    }
}

// Populate the report with the list of commands/sub-commands correlated to the provided cmd id.
fn populate_commands_tree(
    commands: &ScannedCommands,
    report: &mut BTreeMap<String, Vec<String>>,
    cmd_id: &str,
) {
    println!("Looking for commands spawned from cmd id {}", cmd_id);
    let root_cmd_id = get_root_cmd_id(cmd_id);
    if let Some(matching_cmds) = commands.spawned.get(&root_cmd_id) {
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

    if let Some(matching_cmds) = commands.started.get(&root_cmd_id) {
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

    if let Some(matching_cmds) = commands.succeeded.get(&root_cmd_id) {
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

    if let Some(matching_cmds) = commands.failed.get(&root_cmd_id) {
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
    commands: &ScannedCommands,
    report: &mut BTreeMap<String, Vec<String>>,
    msg_id: &str,
) {
    println!("Looking for commands spawned from msg id {}", msg_id);
    let incoming_msg_log_entry = commands.handle_msg.get(msg_id);

    if let Some(ids) = commands.msg_id_to_cmds.get(msg_id) {
        ids.iter().for_each(|root_cmd_id| {
            if let Some((logfile, log_entry)) = incoming_msg_log_entry {
                report
                    .entry(root_cmd_id.clone())
                    .or_insert_with(Vec::new)
                    .push(format!("{}: {}", logfile.display(), log_entry));
            }

            populate_commands_tree(commands, report, root_cmd_id);
        });
    }
}

// Given a command id, return the root id, e.g. the root cmd id of 'abc.1.0.2' is 'a.b.c'.
fn get_root_cmd_id(cmd_id: &str) -> String {
    let mut root_cmd_id = cmd_id.to_string();
    root_cmd_id.truncate(cmd_id.find('.').unwrap_or(cmd_id.len()));
    root_cmd_id
}
