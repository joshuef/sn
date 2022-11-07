// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    helpers::{gen_processed_files_table, get_from_arg_or_stdin, serialise_output, xorname_to_hex},
    OutputFmt,
};
use clap::Subcommand;
use color_eyre::{eyre::eyre, Result};
use sn_api::{files::FilesMapChange, PublicKey, Safe, SafeUrl, XorName, XorUrlBase};

// Defines subcommands of 'xorurl'
#[derive(Subcommand, Debug)]
pub enum XorurlSubCommands {
    #[clap(name = "decode")]
    /// Decode a XOR-URL extracting all the information encoded it in
    Decode {
        /// The XOR-URL to decode
        xorurl: Option<String>,
    },
    /// Generate the SafeKey XOR-URL for a Public Key
    Pk {
        /// The Public Key to generate the SafeKey XOR-URL for
        pk: String,
    },
}

pub fn xorurl_commander(
    cmd: XorurlSubCommands,
    output_fmt: OutputFmt,
    xorurl_base: XorUrlBase,
) -> Result<()> {
    match cmd {
        XorurlSubCommands::Decode { xorurl } => {
            let url = get_from_arg_or_stdin(xorurl, Some("...awaiting XOR-URL from stdin"))?;
            let safeurl = SafeUrl::from_url(&url)?;
            if OutputFmt::Pretty == output_fmt {
                let (urltype, public_name) = if safeurl.is_nrsurl() {
                    ("NRS-URL", safeurl.public_name())
                } else {
                    ("XOR-URL", "<unknown>")
                };
                println!("Information decoded from SafeUrl: {}", url);
                println!("UrlType: {}", urltype);
                println!("Xorname: {}", xorname_to_hex(&safeurl.xorname()));
                println!("Public Name: {}", public_name);
                if safeurl.is_nrsurl() {
                    println!("Top Name: {}", safeurl.top_name());
                }
                println!("Sub names: {}", safeurl.sub_names());
                println!("Type tag: {}", safeurl.type_tag());
                println!("Native data type: {}", safeurl.data_type());
                println!("Path: {}", safeurl.path_decoded()?);
                println!("QueryString: {}", safeurl.query_string());
                println!("QueryPairs: {:?}", safeurl.query_pairs());
                println!("Fragment: {}", safeurl.fragment());
                println!(
                    "Content version: {}",
                    safeurl
                        .content_version()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "latest".to_string())
                );
            } else {
                println!("{}", serialise_output(&safeurl, output_fmt));
            }
        }
        XorurlSubCommands::Pk { pk } => {
            let public_key = PublicKey::ed25519_from_hex(&pk)
                .or_else(|_| PublicKey::bls_from_hex(&pk))
                .map_err(|_| eyre!("Invalid (Ed25519/BLS) public key bytes: {}", pk))?;

            let xorname = XorName::from(public_key);
            let xorurl = SafeUrl::from_safekey(xorname)?.encode(xorurl_base);

            // Now let's just print out the SafeKey xorurl
            if OutputFmt::Pretty == output_fmt {
                println!("SafeKey XOR-URL: {}", xorurl);
            } else {
                println!("{}", xorurl);
            }
        }
    }
    Ok(())
}

pub async fn xorurl_of_files(
    location: Option<String>,
    recursive: bool,
    follow_symlinks: bool,
    output_fmt: OutputFmt,
    xorurl_base: XorUrlBase,
) -> Result<()> {
    // Do a dry-run on the location
    let mut safe = Safe::dry_runner(Some(xorurl_base));

    let location = get_from_arg_or_stdin(location, Some("...awaiting location path from stdin"))?;
    let (_, processed_files, _) = safe
        .files_container_create_from(&location, None, recursive, follow_symlinks)
        .await?;

    // Now let's just print out a list of the xorurls
    if OutputFmt::Pretty == output_fmt {
        if processed_files.is_empty() {
            println!("No files were processed");
        } else {
            let (table, success_count) = gen_processed_files_table(&processed_files, false);
            println!("{} file/s processed:", success_count);
            println!("{table}");
        }
    } else {
        let mut list = Vec::<(String, String)>::new();
        for (file_name, change) in processed_files {
            let link = match change {
                FilesMapChange::Failed(err) => format!("<{}>", err),
                FilesMapChange::Added(link)
                | FilesMapChange::Updated(link)
                | FilesMapChange::Removed(link) => link,
            };

            list.push((file_name.display().to_string(), link));
        }
        println!("{}", serialise_output(&list, output_fmt));
    }

    Ok(())
}
