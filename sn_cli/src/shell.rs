// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::cli;
use async_std::task;
use color_eyre::Result;
use shrust::{Shell, ShellIO};
use sn_api::SafeAuthdClient;
use std::io::Write;

pub fn shell_run() -> Result<()> {
    let sn_authd_client = SafeAuthdClient::new(None);
    let mut shell = Shell::new(sn_authd_client);
    shell.set_default(|io, _, cmd| {
        writeln!(
            io,
            "Command '{}' is unknown or not supported yet in interactive mode",
            cmd
        )?;
        writeln!(io, "Type 'help' for a list of currently supported top level commands")?;
        writeln!(io, "Pass '--help' flag to any top level command for a complete list of supported subcommands and arguments")?;
        Ok(())
    });
    shell.new_command(
        "cat",
        "Read data on the Safe Network",
        0,
        |io, _sn_authd_client, args| call_cli("cat", args, io),
    );
    shell.new_command(
        "config",
        "CLI config settings",
        0,
        |io, _sn_authd_client, args| call_cli("config", args, io),
    );
    shell.new_command(
        "dog",
        "Inspect data on the Safe Network providing only metadata information about the content",
        0,
        |io, _sn_authd_client, args| call_cli("dog", args, io),
    );
    shell.new_command(
        "files",
        "Manage files on the Safe Network",
        0,
        |io, _sn_authd_client, args| call_cli("files", args, io),
    );
    shell.new_command(
        "seq",
        "Manage Sequences on the Safe Network",
        0,
        |io, _sn_authd_client, args| call_cli("seq", args, io),
    );
    shell.new_command(
        "keypair",
        "Generate a key pair without creating and/or storing a SafeKey on the network",
        0,
        |io, _sn_authd_client, args| call_cli("keypair", args, io),
    );
    shell.new_command(
        "keys",
        "Manage keys on the Safe Network",
        0,
        |io, _sn_authd_client, args| call_cli("keys", args, io),
    );
    shell.new_command(
        "networks",
        "Switch between Safe networks",
        0,
        |io, _sn_authd_client, args| call_cli("networks", args, io),
    );
    shell.new_command(
        "nrs",
        "Manage public names on the Safe Network",
        0,
        |io, _sn_authd_client, args| call_cli("nrs", args, io),
    );
    shell.new_command(
        "setup",
        "Perform setup tasks",
        0,
        |io, _sn_authd_client, args| call_cli("setup", args, io),
    );
    shell.new_command(
        "update",
        "Update the application to the latest available version",
        0,
        |io, _sn_authd_client, args| call_cli("update", args, io),
    );
    shell.new_command(
        "node",
        "Commands to manage Safe Network Nodes",
        0,
        |io, _sn_authd_client, args| call_cli("node", args, io),
    );
    shell.new_command(
        "xorurl",
        "Obtain the XOR-URL of data without uploading it to the network, or decode XOR-URLs",
        0,
        |io, _sn_authd_client, args| call_cli("xorurl", args, io),
    );

    println!();
    println!("Welcome to Safe CLI interactive shell!");
    println!("Type 'help' for a list of supported commands");
    println!("Pass '--help' flag to any top level command for a complete list of supported subcommands and arguments");
    println!("Type 'quit' to exit this shell. Enjoy it!");
    println!();

    // Run the shell loop to process user commands
    shell.run_loop(&mut ShellIO::default());

    Ok(())
}

fn call_cli(
    subcommand: &str,
    args: &[&str],
    io: &mut shrust::ShellIO,
) -> Result<(), shrust::ExecError> {
    // Let's create an args array to mimic the one we'd receive when passed to CLI
    let mut mimic_cli_args = vec!["safe", subcommand];
    mimic_cli_args.extend(args.iter());

    // We can now pass this args array to the CLI
    match task::block_on(cli::run_with(Some(&mimic_cli_args), None)) {
        Ok(()) => Ok(()),
        Err(err) => {
            writeln!(io, "{}", err)?;
            Ok(())
        }
    }
}

// #[allow(dead_code)]
// fn prompt_to_allow_auth(auth_req: AuthReq) -> Option<bool> {
//     println!();
//     println!("A new application authorisation request was received:");
//     let req_id = auth_req.req_id;
//     pretty_print_auth_reqs(vec![auth_req], None);

//     println!("You can use 'auth allow'/'auth deny' commands to allow/deny the request respectively, e.g.: auth allow {}", req_id);
//     println!("Press Enter to continue");
//     let _ = stdout().flush();
//     None
// }
