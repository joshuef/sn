// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    helpers::{
        gen_wallet_table, get_from_arg_or_stdin, get_target_url, print_nrs_map, serialise_output,
    },
    OutputFmt,
};
use clap::Args;
use color_eyre::{eyre::WrapErr, Result};
use comfy_table::Table;
use sn_api::{resolver::SafeData, ContentType, Safe, SafeUrl};
use std::io::{self, Write};
use tracing::debug;

#[derive(Args, Debug)]
pub struct CatCommands {
    /// The safe:// location to retrieve
    location: Option<String>,
    /// Renders file output as hex
    #[clap(short = 'x', long = "hexdump")]
    hexdump: bool,
}

pub async fn cat_commander(cmd: CatCommands, output_fmt: OutputFmt, safe: &mut Safe) -> Result<()> {
    let link = get_from_arg_or_stdin(cmd.location, None)?;
    let url = get_target_url(&link)?;
    debug!("Running cat for: {}", &url.to_string());

    match safe.fetch(&url.to_string(), None).await? {
        SafeData::FilesContainer {
            version, files_map, ..
        } => {
            // Render FilesContainer
            if OutputFmt::Pretty == output_fmt {
                println!(
                    "Files of FilesContainer ({}) at \"{}\":",
                    version.map_or("empty".to_string(), |v| format!("version {}", v)),
                    url
                );
                let mut table = Table::new();
                table.add_row(&vec!["Name", "Type", "Size", "Created", "Modified", "Link"]);
                for (name, file_item) in files_map.iter() {
                    table.add_row(&vec![
                        name,
                        &file_item["type"],
                        &file_item["size"],
                        &file_item["created"],
                        &file_item["modified"],
                        file_item.get("link").unwrap_or(&String::default()),
                    ]);
                }
                println!("{table}");
            } else {
                println!(
                    "{}",
                    serialise_output(&(url.to_string(), files_map), output_fmt)
                );
            }
        }
        SafeData::PublicFile { data, .. } => {
            if cmd.hexdump {
                // Render hex representation of file
                println!("{}", pretty_hex::pretty_hex(&data));
            } else {
                // Render file
                io::stdout()
                    .write_all(&data)
                    .context("Failed to print out the content of the file")?;
            }
        }
        SafeData::NrsMapContainer { nrs_map, .. } => {
            if OutputFmt::Pretty == output_fmt {
                println!("NRS Map Container at {}", url);
                print_nrs_map(&nrs_map);
            } else {
                println!(
                    "{}",
                    serialise_output(&(url.to_string(), nrs_map), output_fmt)
                );
            }
        }
        SafeData::SafeKey { .. } => {
            println!("No content to show since the URL targets a SafeKey. Use the 'dog' command to obtain additional information about the targeted SafeKey.");
        }
        SafeData::Multimap { xorurl, data, .. } => {
            let safeurl = SafeUrl::from_xorurl(&xorurl)?;
            if safeurl.content_type() == ContentType::Wallet {
                if OutputFmt::Pretty == output_fmt {
                    println!("Spendable balances of wallet at \"{}\":", xorurl);
                    let table = gen_wallet_table(safe, &data).await?;
                    println!("{table}");
                } else {
                    println!("{}", serialise_output(&(url.to_string(), data), output_fmt));
                }
            } else {
                println!("Type of content not supported yet by 'cat' command.");
            }
        }
        SafeData::NrsEntry { .. } | SafeData::Register { .. } => {
            println!("Type of content not supported yet by 'cat' command.");
        }
    }

    Ok(())
}
