// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.


use eyre::Result;
use novedad::Config;
use tokio::{fs::remove_file, io};

// This example is to demonstrate how the node configuration is constructed
// The node will attempt to read a cached config file from disk
// The node will then overwrite the config using the provided command line args

// Note: This is essentially a test, but, when using test filtering, Clap tries to
// parse the filter as an argument passed resulting in a `UnmatchedArgument` error.

#[tokio::main]
async fn main() -> Result<()> {
    // Create some config and write it to disk
    let file_config = Config::new()?;

    // TODO: Uncomment the below lines once we enable reading config from disk
    // file_config.local_addr = Some("127.0.0.1".parse()?);
    // file_config.public_addr = Some(std::net::SocketAddr::from((
    //     std::net::Ipv4Addr::UNSPECIFIED,
    //     12345,
    // )));
    file_config.write_to_disk().await?;

    // This should load the config from disk and
    // use the command line arguments to overwrite the config
    // with any provided arguments
    let config = Config::new()?;

    let command_line_args = Config::from_args();

    if command_line_args.wallet_id.is_some() {
        assert_eq!(command_line_args.wallet_id, config.wallet_id)
    } else {
        assert_eq!(file_config.wallet_id, config.wallet_id)
    }

    if command_line_args.root_dir.is_some() {
        assert_eq!(command_line_args.root_dir, config.root_dir)
    } else {
        assert_eq!(file_config.root_dir, config.root_dir)
    }

    if command_line_args.verbose > 0 {
        assert_eq!(command_line_args.verbose, config.verbose)
    } else {
        assert_eq!(file_config.verbose, config.verbose)
    }

    if command_line_args.completions.is_some() {
        assert_eq!(command_line_args.completions, config.completions)
    } else {
        assert_eq!(file_config.completions, config.completions)
    }

    if command_line_args.log_dir.is_some() {
        assert_eq!(command_line_args.log_dir, config.log_dir)
    } else {
        assert_eq!(file_config.log_dir, config.log_dir)
    }

    assert_eq!(
        config.update,
        file_config.update || command_line_args.update
    );
    assert_eq!(
        config.no_confirm,
        file_config.no_confirm || command_line_args.no_confirm
    );
    assert_eq!(
        config.clear_data,
        file_config.clear_data || command_line_args.clear_data
    );

    if command_line_args.local_addr.is_some() {
        assert_eq!(command_line_args.local_addr, config.local_addr);
    } else {
        assert_eq!(file_config.local_addr, config.local_addr);
    }

    if command_line_args.first.is_some() {
        assert!(config.first.is_some());
    }

    clear_disk_config().await?;

    Ok(())
}

async fn clear_disk_config() -> io::Result<()> {
    let mut path = dirs_next::home_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Home directory not found"))?;

    path.push(".safe");
    path.push("node");
    path.push("node.config");

    remove_file(path).await
}
