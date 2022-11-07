// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    helpers::{
        get_from_arg_or_stdin, get_target_url, print_nrs_map, serialise_output, xorname_to_hex,
    },
    OutputFmt,
};
use clap::Args;
use color_eyre::Result;
use sn_api::{
    resolver::{ContentType, SafeData},
    Safe, SafeUrl,
};
use tracing::debug;

#[derive(Args, Debug)]
pub struct DogCommands {
    /// The safe:// location to inspect
    location: Option<String>,
}

pub async fn dog_commander(cmd: DogCommands, output_fmt: OutputFmt, safe: &mut Safe) -> Result<()> {
    let link = get_from_arg_or_stdin(cmd.location, None)?;
    let url = get_target_url(&link)?;
    debug!("Running dog for: {}", &url);

    let resolved_content = safe.inspect(&url.to_string()).await?;
    if OutputFmt::Pretty != output_fmt {
        println!(
            "{}",
            serialise_output(&(url.to_string(), resolved_content), output_fmt)
        );
    } else {
        for (i, ref content) in resolved_content.iter().enumerate() {
            println!();
            println!("== URL resolution step {} ==", i + 1);
            match content {
                SafeData::NrsMapContainer {
                    xorurl,
                    xorname,
                    type_tag,
                    nrs_map,
                    data_type,
                    ..
                } => {
                    println!("= NRS Map Container =");
                    println!("XOR-URL: {}", xorurl);
                    println!("Type tag: {}", type_tag);
                    println!("XOR name: 0x{}", xorname_to_hex(xorname));
                    println!("Native data type: {}", data_type);
                    let mut safeurl = SafeUrl::from_url(xorurl)?;
                    safeurl.set_content_type(ContentType::Raw)?;
                    println!("Native data XOR-URL: {}", safeurl);
                    print_nrs_map(nrs_map);
                }
                SafeData::NrsEntry {
                    xorurl,
                    public_name,
                    data_type,
                    resolves_into,
                    resolved_from,
                    version,
                } => {
                    println!("Resolved from: {}", resolved_from);
                    println!("= NrsEntry =");
                    println!("Public name: {}", public_name);
                    println!("Target XOR-URL: {}", xorurl);
                    println!("Target native data type: {}", data_type);
                    println!("Resolves into: {}", resolves_into);
                    println!(
                        "Version: {}",
                        version.map_or("none".to_string(), |v| v.to_string())
                    );
                }
                SafeData::FilesContainer {
                    xorurl,
                    xorname,
                    type_tag,
                    version,
                    data_type,
                    resolved_from,
                    ..
                } => {
                    println!("Resolved from: {}", resolved_from);
                    println!("= FilesContainer =");
                    println!("XOR-URL: {}", xorurl);
                    println!(
                        "Version: {}",
                        version.map_or("none".to_string(), |v| v.to_string())
                    );
                    println!("Type tag: {}", type_tag);
                    println!("XOR name: 0x{}", xorname_to_hex(xorname));
                    println!("Native data type: {}", data_type);
                    let mut safeurl = SafeUrl::from_url(xorurl)?;
                    safeurl.set_content_type(ContentType::Raw)?;
                    println!("Native data XOR-URL: {}", safeurl);
                }
                SafeData::PublicFile {
                    xorurl,
                    xorname,
                    media_type,
                    resolved_from,
                    ..
                } => {
                    println!("Resolved from: {}", resolved_from);
                    println!("= File =");
                    println!("XOR-URL: {}", xorurl);
                    println!("XOR name: 0x{}", xorname_to_hex(xorname));
                    println!("Native data type: PublicFile");
                    println!(
                        "Media type: {}",
                        media_type.clone().unwrap_or_else(|| "Unknown".to_string())
                    );
                }
                SafeData::SafeKey { .. } => {
                    println!("The SafeKey data type is not supported at the moment");
                }
                SafeData::Multimap {
                    xorurl,
                    resolved_from,
                    xorname,
                    type_tag,
                    ..
                } => {
                    let safeurl = SafeUrl::from_xorurl(xorurl)?;
                    println!("Resolved from: {}", resolved_from);
                    if safeurl.content_type() == ContentType::Wallet {
                        println!("= Wallet =");
                    } else {
                        println!("= Multimap =");
                    }
                    println!("XOR-URL: {}", xorurl);
                    println!("Type tag: {}", type_tag);
                    println!("XOR name: 0x{}", xorname_to_hex(xorname));
                    println!("Native data type: Register");
                }
                SafeData::Register {
                    xorurl,
                    resolved_from,
                    xorname,
                    type_tag,
                    ..
                } => {
                    println!("Resolved from: {}", resolved_from);
                    println!("= Register =");
                    println!("XOR-URL: {}", xorurl);
                    println!("Type tag: {}", type_tag);
                    println!("XOR name: 0x{}", xorname_to_hex(xorname));
                    println!("Native data type: Register");
                }
            }
        }
        println!();
    }

    Ok(())
}
