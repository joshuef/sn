// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::OutputFmt;
use ansi_term::Style;
use color_eyre::{eyre::bail, eyre::eyre, eyre::WrapErr, Result};
use comfy_table::{Cell, CellAlignment, Table};
use num_traits::Float;
use serde::ser::Serialize;
use sn_api::{
    files::{FilesMapChange, ProcessedFiles},
    multimap::Multimap,
    nrs::NrsMap,
    wallet::Dbc,
    Safe, SafeUrl,
};
use std::io::{stdin, stdout, Read, Write};
use tracing::{debug, warn};
use xor_name::XorName;

// Warn the user about a dry-run being performed
pub fn notice_dry_run() {
    println!("NOTE the operation is being performed in dry-run mode, therefore no changes are committed to the network.");
}

// Converts the XOR name bytes into a hex encoded string
pub fn xorname_to_hex(xorname: &XorName) -> String {
    xorname.0.iter().map(|b| format!("{:02x}", b)).collect()
}

// Read the argument string from the STDIN if is not an arg provided
pub fn get_from_arg_or_stdin(arg: Option<String>, message: Option<&str>) -> Result<String> {
    match arg {
        Some(ref t) if t.is_empty() => {
            let val = get_from_stdin(message)?;
            Ok(String::from_utf8(val).map_err(|err| {
                eyre!(
                    "String read from STDIN contains invalid UTF-8 characters: {}",
                    err
                )
            })?)
        }
        Some(t) => Ok(t),
        None => {
            let val = get_from_stdin(message)?;
            Ok(String::from_utf8(val).map_err(|err| {
                eyre!(
                    "String read from STDIN contains invalid UTF-8 characters: {}",
                    err
                )
            })?)
        }
    }
}

pub fn read_stdin_response() -> Result<String> {
    let mut user_input = String::new();
    stdin()
        .read_line(&mut user_input)
        .with_context(|| "Error occurred when attempting to get input from stdin".to_string())?;
    if user_input.ends_with('\n') {
        user_input.pop();
    }
    if user_input.ends_with('\r') {
        user_input.pop();
    }
    Ok(user_input)
}

// Outputs a message and then reads from stdin
pub fn get_from_stdin(message: Option<&str>) -> Result<Vec<u8>> {
    let the_message = message.unwrap_or("...awaiting data from STDIN stream...");
    println!("{}", &the_message);
    let mut buffer = Vec::new();
    match std::io::stdin().read_to_end(&mut buffer) {
        Ok(size) => {
            debug!("Read ({} bytes) from STDIN", size);
            Ok(buffer)
        }
        Err(_) => bail!("Failed to read from STDIN stream".to_string()),
    }
}

// Prompt the user with the message provided
pub fn prompt_user(prompt_msg: &str, error_msg: &str) -> Result<String> {
    print!("{}", prompt_msg);
    let _ = stdout().flush();
    let buf = read_stdin_response()?;
    if buf.is_empty() {
        Err(eyre!(error_msg.to_string()))
    } else {
        Ok(buf)
    }
}

#[allow(dead_code)]
// Unwrap secret key string provided, otherwise prompt user to provide it
pub fn get_secret_key(key_xorurl: &str, sk: Option<String>, msg: &str) -> Result<String> {
    let mut sk = sk.unwrap_or_else(|| String::from(""));

    if sk.is_empty() {
        let msg = if key_xorurl.is_empty() {
            format!("Enter secret key corresponding to {}: ", msg)
        } else {
            format!(
                "Enter secret key corresponding to public key at \"{}\": ",
                key_xorurl
            )
        };
        sk = prompt_user(&msg, "Invalid input")?;
    }

    Ok(sk)
}

pub fn processed_files_err_report<T: std::fmt::Display>(err: &T) -> (String, String) {
    ("E".to_string(), format!("<{}>", err))
}

pub fn gen_processed_files_table(
    processed_files: &ProcessedFiles,
    show_change_sign: bool,
) -> (Table, u64) {
    let mut table = Table::new();
    let mut success_count = 0;
    for (file_name, change) in processed_files.iter() {
        if change.is_success() {
            success_count += 1;
        }

        let (change_sign, link) = match change {
            FilesMapChange::Failed(err) => processed_files_err_report(&err),
            FilesMapChange::Added(link) => ("+".to_string(), link.clone()),
            FilesMapChange::Updated(link) => ("*".to_string(), link.clone()),
            FilesMapChange::Removed(link) => ("-".to_string(), link.clone()),
        };

        if show_change_sign {
            table.add_row(&vec![change_sign, file_name.display().to_string(), link]);
        } else {
            table.add_row(&vec![file_name.display().to_string(), link]);
        }
    }
    (table, success_count)
}

// Reads a Multimap, deserialises it as a Wallet, fetching and listing
// each of the contained spendable balances (DBCs), returning a Table ready to print out.
pub async fn gen_wallet_table(safe: &mut Safe, multimap: &Multimap) -> Result<Table> {
    let mut table = Table::new();
    table.add_row(&vec![
        "Spendable balance name",
        "Balance",
        "Owner",
        "DBC Data",
    ]);

    for (_, (key, value)) in multimap.iter() {
        let xorurl_str = std::str::from_utf8(value)?;
        let dbc_bytes = safe.files_get(xorurl_str, None).await?;

        let dbc: Dbc = match rmp_serde::from_slice(&dbc_bytes) {
            Ok(dbc) => dbc,
            Err(err) => {
                warn!("Ignoring entry found in wallet since it cannot be deserialised as a valid DBC: {:?}", err);
                continue;
            }
        };

        let balance = match dbc.amount_secrets_bearer() {
            Ok(amount_secrets) => amount_secrets.amount().to_string(),
            Err(err) => {
                warn!("Ignoring amount from DBC found in wallet due to error in revealing secret amount: {:?}", err);
                "unknown".to_string()
            }
        };

        let spendable_name = std::str::from_utf8(key)?;
        let hex_dbc = dbc.to_hex()?;
        let hex_dbc = format!("{}...{}", &hex_dbc[..8], &hex_dbc[hex_dbc.len() - 8..]);
        let hex_owner = dbc.owner_base().public_key().to_hex();
        let owner = format!(
            "{}...{}",
            &hex_owner[..6],
            &hex_owner[hex_owner.len() - 6..]
        );

        let mut row = comfy_table::Row::new();
        row.add_cell(spendable_name.into());
        row.add_cell(Cell::new(balance).set_alignment(CellAlignment::Right));
        row.add_cell(owner.into());
        row.add_cell(hex_dbc.into());

        table.add_row(row);
    }

    Ok(table)
}

// converts "-" to "", both of which mean to read from stdin.
pub fn parse_stdin_arg(src: &str) -> String {
    if src.is_empty() || src == "-" {
        "".to_string()
    } else {
        src.to_string()
    }
}

// serialize structured value using any format from OutputFmt
// except OutputFmt::Pretty, which must be handled by caller.
pub fn serialise_output<T: ?Sized>(value: &T, fmt: OutputFmt) -> String
where
    T: Serialize,
{
    match fmt {
        OutputFmt::Yaml => serde_yaml::to_string(&value)
            .unwrap_or_else(|_| "Failed to serialise output to yaml".to_string()),
        OutputFmt::Json => serde_json::to_string_pretty(&value)
            .unwrap_or_else(|_| "Failed to serialise output to json".to_string()),
        OutputFmt::JsonCompact => serde_json::to_string(&value)
            .unwrap_or_else(|_| "Failed to serialise output to compact json".to_string()),
        OutputFmt::Pretty => {
            "OutputFmt::Pretty' not handled by caller, in serialise_output()".to_string()
        }
    }
}

pub fn print_nrs_map(nrs_map: &NrsMap) {
    println!("Listing NRS map contents:");
    let summary = nrs_map.get_map_summary();
    for (pub_name, link) in summary.iter() {
        println!("{}: {}", pub_name, link);
    }
}

// returns singular or plural version of string, based on count.
pub fn pluralize<'a>(singular: &'a str, plural: &'a str, count: u64) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

// if stdout is a TTY, then it returns a string with ansi codes according to
// style.  Otherwise, it returns the original string.
pub fn if_tty(s: &str, style: Style) -> String {
    if atty::is(atty::Stream::Stdout) {
        style.paint(s).to_string()
    } else {
        s.to_string()
    }
}

pub fn div_or<X: Float>(num: X, den: X, default: X) -> X {
    if (!num.is_normal() && !num.is_zero()) || !den.is_normal() {
        default
    } else {
        num / den
    }
}

/// Get the target URL from the link as a string.
///
/// If the user hasn't prefixed the link with `safe://`, we'll do that for them here.
pub fn get_target_url(link: &str) -> Result<SafeUrl> {
    if !link.starts_with("safe://") {
        return Ok(SafeUrl::from_url(&format!("safe://{}", link))?);
    }
    Ok(SafeUrl::from_url(link)?)
}
