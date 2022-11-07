// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    helpers::{get_from_arg_or_stdin, serialise_output},
    OutputFmt,
};
use crate::operations::config::Config;
use bls::{PublicKey, SecretKey};
use clap::Subcommand;
use color_eyre::{eyre::eyre, eyre::Error, Help, Result};
use sn_api::{Error as ApiError, Safe};
use sn_dbc::{Dbc, Error as DbcError};
use std::path::{Path, PathBuf};
use tokio::fs;

#[derive(Subcommand, Debug)]
pub enum WalletSubCommands {
    #[clap(name = "create")]
    /// Create a new wallet
    Create {},
    #[clap(name = "balance")]
    /// Query a wallet's balance
    Balance {
        /// The URL of wallet to query
        target: Option<String>,
    },
    #[clap(name = "deposit")]
    /// Deposit a spendable DBC in a wallet. If the DBC is not bearer, we will try to deposit using
    /// the secret key configured for use with safe. If you wish to use a different key, use the
    /// --secret-key argument.
    Deposit {
        /// The URL of the wallet for the deposit
        wallet_url: String,
        /// The name to give this spendable DBC
        #[clap(long = "name")]
        name: Option<String>,
        /// A path to a file containing hex encoded DBC data, or you can supply the data directly.
        /// Depending on the shell or OS in use, due to the length of the data string, supplying
        /// directly may not work.
        #[clap(long = "dbc")]
        dbc: Option<String>,
        #[clap(long = "secret-key")]
        /// Use this argument to specify a secret key for an owned DBC. It should be a hex-encoded
        /// BLS key.
        secret_key_hex: Option<String>,
        /// When this flag is set, the DBC will be deposited into the wallet without
        /// trying to verify the DBC hasn't been already spent.
        #[clap(long = "force")]
        force: bool,
    },
    #[clap(name = "reissue")]
    /// Reissue a DBC from a wallet.
    Reissue {
        /// The amount to reissue
        amount: String,
        /// The URL of wallet to reissue from
        #[clap(long = "from")]
        from: String,
        /// To reissue the DBC to a particular owner, provide their public key. This should be a
        /// hex-encoded BLS key. Otherwise the DBC will be reissued as bearer, meaning anyone can
        /// spend it. This argument and the --owned argument are mutually exclusive.
        #[clap(long = "to")]
        to: Option<String>,
        /// Set this flag to reissue as an owned DBC, using the public key configured for use with
        /// safe. This argument and the --public-key argument are mutually exclusive.
        #[clap(long = "owned")]
        owned: bool,
        /// A file path to store the content of the reissued DBC.
        #[clap(long = "save")]
        save: Option<PathBuf>,
    },
}

pub async fn wallet_commander(
    cmd: WalletSubCommands,
    output_fmt: OutputFmt,
    safe: &mut Safe,
    config: &Config,
) -> Result<()> {
    match cmd {
        WalletSubCommands::Create {} => {
            let wallet_xorurl = safe.wallet_create().await?;

            if OutputFmt::Pretty == output_fmt {
                println!("Wallet created at: \"{}\"", wallet_xorurl);
            } else {
                println!("{}", serialise_output(&wallet_xorurl, output_fmt));
            }

            Ok(())
        }
        WalletSubCommands::Balance { target } => {
            let target = get_from_arg_or_stdin(
                target,
                Some("...awaiting wallet address/location from STDIN stream..."),
            )?;

            let balance = safe.wallet_balance(&target).await?;

            if OutputFmt::Pretty == output_fmt {
                println!(
                    "Wallet at \"{}\" has a total balance of {} safecoins",
                    target, balance
                );
            } else {
                println!("{}", balance);
            }

            Ok(())
        }
        WalletSubCommands::Deposit {
            wallet_url,
            name,
            dbc,
            secret_key_hex,
            force,
        } => {
            let dbc = if let Some(dbc) = dbc {
                let path = Path::new(&dbc);
                if path.exists() {
                    if path.is_dir() {
                        return Err(eyre!("The path supplied refers to a directory.")
                            .suggestion("A file path must be specified for the DBC data."));
                    }
                    let dbc_data = std::fs::read_to_string(path)?;
                    Dbc::from_hex(dbc_data.trim()).map_err(|e| {
                        eyre!(e.to_string()).suggestion(
                            "This file does not appear to have DBC data. \
                            Please select another file with valid hex-encoded DBC data.",
                        )
                    })?
                } else {
                    Dbc::from_hex(&dbc)?
                }
            } else {
                let dbc_hex = get_from_arg_or_stdin(dbc, None)?;
                Dbc::from_hex(dbc_hex.trim())?
            };

            let (sk, key_image) = if dbc.is_bearer() {
                (None, dbc.key_image_bearer()?)
            } else if let Some(sk_hex) = secret_key_hex {
                // This is an owned DBC and its secret key has been supplied
                let sk = SecretKey::from_hex(&sk_hex)?;
                let key_image = dbc
                    .key_image(&sk)
                    .map_err(|e| map_invalid_sk_error(e.into()))?;
                (Some(sk), key_image)
            } else {
                // This is an owned DBC but its secret key was not provided,
                // thus attempt to use the key configured for use with the CLI.
                let sk = read_key_from_configured_credentials(
                    config,
                    "This is an owned DBC. To deposit, it requires a secret key. \
                         A secret key was not supplied and there were no credentials \
                         configured for use with safe."
                        .to_string(),
                    "Please run the command again using the --secret-key \
                         argument to specify the key."
                        .to_string(),
                )?;
                let key_image = dbc
                    .key_image(&sk)
                    .map_err(|e| map_invalid_sk_error(e.into()))?;
                (Some(sk), key_image)
            };

            if force {
                println!(
                    "\nWARNING: --force flag set, hence skipping verification to check if \
                supplied DBC has been already spent.\n"
                );
            } else if safe.is_dbc_spent(key_image).await? {
                return Err(
                    eyre!("The supplied DBC has been already spent on the network.").suggestion(
                        "Please run the command again with the --force flag if you still \
                            wish to deposit it into the wallet.",
                    ),
                );
            }

            let (name, balance) = safe
                .wallet_deposit(&wallet_url, name.as_deref(), &dbc, sk)
                .await
                .map_err(map_invalid_sk_error)?;

            if OutputFmt::Pretty == output_fmt {
                println!(
                    "Spendable DBC deposited ({} safecoins) with name '{}' in wallet located at \"{}\"",
                    balance, name, wallet_url
                );
            } else {
                println!("{}", serialise_output(&(wallet_url, name), output_fmt));
            }

            Ok(())
        }
        WalletSubCommands::Reissue {
            amount,
            from,
            save,
            to,
            owned,
        } => {
            if owned && to.is_some() {
                return Err(eyre!(
                    "The --owned and --to arguments are mutually exclusive."
                )
                .suggestion(
                    "Please run the command again and use one or the other, but not both, of these \
                    arguments."));
            }
            let pk = if let Some(pk_hex) = to {
                Some(PublicKey::from_hex(&pk_hex)?)
            } else if owned {
                let sk = read_key_from_configured_credentials(
                    config,
                    "The --owned argument requires credentials to be configured for safe."
                        .to_string(),
                    "Run the 'keys create --for-cli' command to generate a credentials then run \
                    this command again."
                        .to_string(),
                )?;
                Some(sk.public_key())
            } else {
                None
            };
            let dbc = safe.wallet_reissue(&from, &amount, pk).await?;
            let dbc_hex = dbc.to_hex()?;

            // Write the DBC to a file if the user requested it, but fall
            // back to print it to stdout if that fails.
            let print_out_dbc = match save {
                None => true,
                Some(path) => match fs::write(&path, dbc_hex.clone()).await {
                    Ok(()) => {
                        println!("DBC content written at '{}'.", path.display());
                        false
                    }
                    Err(err) => {
                        eprintln!(
                            "Error: Unable to write DBC at '{}': {}.",
                            path.display(),
                            err
                        );
                        true
                    }
                },
            };

            if OutputFmt::Pretty == output_fmt {
                println!("Reissued DBC with {} safecoins.", amount);
                if print_out_dbc {
                    println!("-------- DBC DATA --------");
                    println!("{}", dbc_hex);
                    println!("--------------------------");
                }

                if let Some(pk) = pk {
                    println!("This DBC is owned by public key {}", pk.to_hex());
                } else {
                    println!("This is a bearer DBC that can be spent by anyone.");
                }
            } else if print_out_dbc {
                println!("{}", dbc_hex);
            }

            Ok(())
        }
    }
}

/// Helper to get the secret key from the credentials that are configured for use with safe.
///
/// Different error and suggestion messages need to be provided depending on the context in which
/// it is used.
///
/// Returns an error if the credentials file is missing.
fn read_key_from_configured_credentials(
    config: &Config,
    error: String,
    suggestion: String,
) -> Result<SecretKey> {
    let mut credentials_path = config.cli_config_path.clone();
    credentials_path.pop();
    credentials_path.push("credentials");
    if !credentials_path.exists() {
        return Err(eyre!(error).suggestion(suggestion));
    }
    let sk_hex = std::fs::read_to_string(credentials_path)?;
    Ok(SecretKey::from_hex(&sk_hex)?)
}

fn map_invalid_sk_error(api_error: ApiError) -> Error {
    match api_error {
        ApiError::DbcError(DbcError::SecretKeyDoesNotMatchPublicKey)
        | ApiError::DbcDepositInvalidSecretKey => {
            eyre!("The supplied secret key did not match the public key for this DBC.").suggestion(
                "Please run the command again with the correct key for the --secret-key argument.",
            )
        }
        _ => api_error.into(),
    }
}
