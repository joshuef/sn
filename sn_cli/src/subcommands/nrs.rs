// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::fmt::Write as _;

use super::{
    helpers::{get_from_arg_or_stdin, get_target_url, serialise_output},
    OutputFmt,
};
use clap::Subcommand;
use color_eyre::{eyre::eyre, Help, Result};
use comfy_table::Table;
use sn_api::Error::{InvalidInput, NetDataError, NrsNameAlreadyExists, UnversionedContentError};
use sn_api::{Safe, SafeUrl};

#[derive(Subcommand, Debug)]
pub enum NrsSubCommands {
    #[clap(name = "add")]
    /// Add a subname to a registered NRS name and link it to some content, or update an existing
    /// subname with a new link.
    Add {
        /// Specify the public name, which is the subname you wish to use, and the registered
        /// topname. For example, "new.topname". If the topname has not already been registered
        /// with the `nrs register` command, use the `--register-top-name` flag to register it here.
        public_name: String,
        /// The safe:// URL to link to. Usually a FilesContainer for a website. This should be
        /// wrapped in double quotes on bash based systems. A link must be provided for a subname.
        /// If you don't provide it with this argument, you will be prompted to provide it
        /// interactively.
        #[clap(short = 'l', long = "link")]
        link: Option<String>,
        /// Set this flag to register the topname if it hasn't already been registered.
        #[clap(short = 'y', long = "register-top-name")]
        register_top_name: bool,
        /// Set this flag to register this link as default for the topname when no subname is
        /// specified.
        #[clap(long = "default")]
        default: bool,
    },
    #[clap(name = "register")]
    /// Register a new top name in Safe NRS
    Register {
        /// The name of the new topname to register
        name: String,
        /// Optional safe:// URL to link the topname to. Usually a FilesContainer for a website.
        /// This should be wrapped in double quotes on bash based systems.
        #[clap(short = 'l', long = "link")]
        link: Option<String>,
    },
    #[clap(name = "remove")]
    /// Remove a subname from an NRS name
    Remove {
        /// The name to remove
        name: String,
    },
}

pub async fn nrs_commander(
    cmd: NrsSubCommands,
    output_fmt: OutputFmt,
    safe: &mut Safe,
) -> Result<()> {
    match cmd {
        NrsSubCommands::Register { name, link } => {
            run_register_subcommand(name, link, safe, output_fmt).await
        }
        NrsSubCommands::Add {
            public_name: name,
            link,
            register_top_name,
            default,
        } => run_add_subcommand(name, link, register_top_name, default, safe, output_fmt).await,
        NrsSubCommands::Remove { name } => run_remove_subcommand(name, safe, output_fmt).await,
    }
}

async fn run_register_subcommand(
    name: String,
    link: Option<String>,
    safe: &mut Safe,
    output_fmt: OutputFmt,
) -> Result<()> {
    match safe.nrs_create(&name).await {
        Ok(topname_url) => {
            let mut summary = String::new();
            write!(
                summary,
                "The container for the map is located at {}",
                topname_url.to_xorurl_string()
            )?;
            if let Some(ref link) = link {
                let url = get_target_url(link)?;
                let _ = associate_url_with_public_name(&name, safe, &url).await?;
                write!(summary, "\nThe entry points to {link}")?;
            }
            print_summary(
                output_fmt,
                &format!(
                    "New NRS Map created for \"safe://{}\"",
                    name.replace("safe://", "")
                ),
                summary,
                &topname_url.to_xorurl_string(),
                &topname_url,
                ("+", &name, &topname_url.to_string()),
            );
            Ok(())
        }
        Err(error) => match error {
            InvalidInput(_) => Err(eyre!(error)
                .wrap_err(
                    "The register command can only register a topname, it cannot add subnames.",
                )
                .suggestion(
                    "Please use the nrs add command with the --register-top-name \
                        argument to register a topname and add a subname at the same time.",
                )
                .suggestion(
                    "Alternatively, register the topname first with the register command, \
                        then use the add command to add the subname.",
                )),
            NrsNameAlreadyExists(_) => Err(eyre!(error)
                .wrap_err(format!(
                    "Could not register topname {}. That name is already taken.",
                    name
                ))
                .suggestion("Try the command again with a different name.")),
            _ => Err(eyre!(error)),
        },
    }
}

async fn run_add_subcommand(
    name: String,
    link: Option<String>,
    register_top_name: bool,
    default: bool,
    safe: &mut Safe,
    output_fmt: OutputFmt,
) -> Result<()> {
    let link = get_from_arg_or_stdin(link, Some("...awaiting link URL from stdin"))?;
    let link_url = get_target_url(&link)?;
    let (url, topname_was_registered) = if register_top_name {
        add_public_name_for_url(&name, safe, &link_url).await?
    } else {
        (
            associate_url_with_public_name(&name, safe, &link_url).await?,
            false,
        )
    };

    let mut summary_header = String::new();
    if topname_was_registered {
        writeln!(summary_header, "New NRS Map created.")?;
        write!(
            summary_header,
            "The container for the map is located at {}",
            SafeUrl::from_url(&format!("safe://{}", url.top_name()))?.to_xorurl_string()
        )?;
    } else {
        write!(summary_header, "Existing NRS Map updated. ")?;
    }
    let version = url
        .content_version()
        .ok_or_else(|| eyre!("Content version not set for returned NRS SafeUrl"))?
        .to_string();
    write!(summary_header, "\nNow at version {version}. ")?;

    if default {
        let topname = get_topname_from_public_name(&name)?;
        associate_url_with_public_name(&topname, safe, &link_url).await?;
        write!(
            summary_header,
            "This link was also set as the default location for {topname}."
        )?;
    }
    print_summary(
        output_fmt,
        &summary_header,
        "".to_string(),
        &SafeUrl::from_url(&format!("safe://{}", url.top_name()))?.to_xorurl_string(),
        &url,
        ("+", &name, &link),
    );
    Ok(())
}

async fn run_remove_subcommand(name: String, safe: &mut Safe, output_fmt: OutputFmt) -> Result<()> {
    match safe.nrs_remove(&name).await {
        Ok(url) => {
            let version = url
                .content_version()
                .ok_or_else(|| eyre!("Content version not set for returned NRS SafeUrl"))?
                .to_string();
            print_summary(
                output_fmt,
                &format!("NRS Map updated (version {})", version),
                "".to_string(),
                &SafeUrl::from_url(&format!("safe://{}", url.top_name()))?.to_xorurl_string(),
                &url,
                ("-", &name, ""),
            );
            Ok(())
        }
        Err(error) => match error {
            // This is the type of error returned when you supply a topname that doesn't exist.
            // Although obviously, this error could occur due to a general connectivity issue,
            // which is why the error message advises that the topname is "likely" not registered.
            NetDataError(_) => {
                let topname = get_topname_from_public_name(&name)?;
                Err(eyre!(error)
                    .wrap_err(format!(
                        "Failed to remove {}. The topname {} is likely not registered in Safe NRS.",
                        name, topname
                    ))
                    .suggestion(format!(
                        "Try the command again or verify that {} is a registered topname.",
                        topname
                    )))
            }
            _ => Err(eyre!(error)),
        },
    }
}

async fn associate_url_with_public_name(
    public_name: &str,
    safe: &mut Safe,
    url: &SafeUrl,
) -> Result<SafeUrl> {
    match safe.nrs_associate(public_name, url).await {
        Ok(new_url) => Ok(new_url),
        Err(error) => match error {
            UnversionedContentError(_) => Err(eyre!(error)
                .wrap_err(
                    "The destination you're trying to link to is versionable content. \
                        When linking to versionable content, you must supply a version hash on the \
                        url. The requested topname was not registered.",
                )
                .suggestion(
                    "Please run the command again with the version hash appended to the link. \
                            The link should have the form safe://<url>?v=<versionhash>.",
                )),
            _ => Err(eyre!(error)),
        },
    }
}

async fn add_public_name_for_url(
    public_name: &str,
    safe: &mut Safe,
    url: &SafeUrl,
) -> Result<(SafeUrl, bool)> {
    match safe.nrs_add(public_name, url).await {
        Ok((new_url, topname_was_registered)) => Ok((new_url, topname_was_registered)),
        Err(error) => match error {
            UnversionedContentError(_) => Err(eyre!(error)
                .wrap_err(
                    "The destination you're trying to link to is versionable content. \
                        When linking to versionable content, you must supply a version hash on the \
                        url. The requested topname was not registered.",
                )
                .suggestion(
                    "Please run the command again with the version hash appended to the link. \
                            The link should have the form safe://<url>?v=<versionhash>.",
                )),
            _ => Err(eyre!(error)),
        },
    }
}

fn print_summary(
    output_fmt: OutputFmt,
    header: &str,
    summary: String,
    container_xorurl: &str,
    nrs_url: &SafeUrl,
    processed_entry: (&str, &str, &str),
) {
    if OutputFmt::Pretty == output_fmt {
        let mut table = Table::new();
        let (change, top_name, url) = processed_entry;
        table.add_row(&vec![change, top_name, url]);
        println!("{}", header);
        if !summary.is_empty() {
            println!("{}", summary.trim());
        }
        println!("{table}");
    } else {
        println!(
            "{}",
            serialise_output(&(container_xorurl, nrs_url, processed_entry), output_fmt)
        );
    }
}

fn get_topname_from_public_name(public_name: &str) -> Result<String> {
    let mut parts = public_name.split('.');
    let topname = parts
        .next_back()
        .ok_or_else(|| {
            eyre!(format!(
                "Could not parse topname from public name {}",
                public_name
            ))
        })?
        .to_string();
    Ok(topname)
}
