// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use color_eyre::{eyre::eyre, eyre::WrapErr, Result};
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs::{create_dir_all, File},
    path::PathBuf,
};

const BASE_DOWNLOAD_URL: &str = "https://sn-node.s3.eu-west-2.amazonaws.com";

/// Downloads and installs either the latest `sn_node` binary or a specific version.
///
/// The self update crate is used to query the Github API to find the latest version number.
/// After we have the version (or if it was optionally supplied by the user), we download the
/// compressed tar archive from S3 and extract it. We just do this by convention, based on the
/// version number and the current target platform.
///
/// It happens to be the case that the self update crate also provides some generic functions for
/// downloading and extracting archives from any source, so we use that code here.
pub fn download_and_install_node(
    target_path: PathBuf,
    exec_file_name: &str,
    repo_name: &str,
    version: Option<String>,
) -> Result<String> {
    let target = get_target();
    let updater = self_update::backends::github::Update::configure()
        .repo_owner("maidsafe")
        .repo_name(repo_name)
        .target(&target)
        .bin_name(exec_file_name)
        .current_version(env!("CARGO_PKG_VERSION"))
        .build()
        .wrap_err(format!(
            "Error fetching list of releases for maidsafe/{repo_name} repository",
        ))?;
    let version = if let Some(version) = version {
        version
    } else {
        let release = updater
            .get_latest_release()
            .wrap_err("Failed to obtain the latest release from Github")?;
        get_version_from_release_version(&release.version)?
    };
    download_and_install_bin(target_path, &target, &version, exec_file_name)
}

// Private helpers

fn get_target() -> String {
    let target = self_update::get_target();
    if target.contains("linux") {
        // For now, all of our Linux builds are using musl, so we can make this
        // assumption. We would need to update this code if we changed that.
        target.replace("gnu", "musl")
    } else {
        target.to_string()
    }
}

fn download_and_install_bin(
    target_path: PathBuf,
    target: &str,
    version: &str,
    exec_file_name: &str,
) -> Result<String> {
    println!("Downloading sn_node version: {version}");
    let tmp_dir = std::env::temp_dir();
    let archive_file_name = format!("sn_node-{version}-{target}.tar.gz");
    let tmp_tarball_path = tmp_dir.join(&archive_file_name);
    let tmp_tarball = File::create(&tmp_tarball_path).wrap_err_with(|| {
        format!(
            "Error creating temp file ('{}') for downloading the release",
            tmp_tarball_path.display(),
        )
    })?;

    let download_url = format!("{BASE_DOWNLOAD_URL}/{archive_file_name}");
    println!("Downloading {download_url}...");
    self_update::Download::from_url(&download_url)
        .show_progress(true)
        .download_to(&tmp_tarball)
        .wrap_err_with(|| format!("Error downloading release from '{download_url}'"))?;

    if !target_path.exists() {
        println!("Creating '{}' folder", target_path.display());
        create_dir_all(target_path.clone())
            .wrap_err("Couldn't create target path to install binary")?;
    }

    println!(
        "Installing {} binary at {} ...",
        exec_file_name,
        target_path.display()
    );
    self_update::Extract::from_source(&tmp_tarball_path)
        .extract_file(target_path.as_path(), exec_file_name)
        .wrap_err_with(|| {
            format!(
                "Error extracting binary from downloaded asset '{}'",
                tmp_tarball_path.display(),
            )
        })?;

    set_exec_perms(target_path.join(exec_file_name))?;

    println!("Done!");
    Ok(target_path.display().to_string())
}

#[cfg(target_os = "windows")]
#[allow(clippy::unnecessary_wraps)]
#[inline]
fn set_exec_perms(_file_path: PathBuf) -> Result<()> {
    // no need to set execution permissions on Windows
    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[inline]
fn set_exec_perms(file_path: PathBuf) -> Result<()> {
    println!(
        "Setting execution permissions to installed binary '{}'...",
        file_path.display()
    );
    let file = File::open(&file_path).wrap_err_with(|| {
        format!(
            "Error when preparing to set execution permissions to installed binary '{}'",
            file_path.display(),
        )
    })?;

    let mut perms = file
        .metadata()
        .wrap_err_with(|| {
            format!(
                "Error when reading metadata from installed binary '{}'",
                file_path.display(),
            )
        })?
        .permissions();

    // set execution permissions bits for owner, group and others
    // Allow unusual bit grouping to clearly separate 3 bits parts
    #[allow(clippy::unusual_byte_groupings)]
    perms.set_mode(perms.mode() | 0b0_001_001_001);
    file.set_permissions(perms).wrap_err_with(|| {
        format!(
            "Failed to set execution permissions to installed binary '{}'",
            file_path.display(),
        )
    })?;

    Ok(())
}

/// Gets the version number from the full version number string.
///
/// The `release_version` input is in the form "0.17.1-0.15.3-0.2.1-0.78.2-0.73.3-0.76.1-0.69.0",
/// which is the `sn_interface`, `sn_fault_detection`, `sn_comms`, `sn_client`, `sn_node`,
/// `sn_api`, `sn_cli` respectively. This function will return the `safe_network` part.
fn get_version_from_release_version(release_version: &str) -> Result<String> {
    let mut parts = release_version.split('-');
    parts.next();
    parts.next();
    parts.next();
    parts.next();
    let version = parts
        .next()
        .ok_or_else(|| eyre!("Could not parse version number from {}", release_version))?
        .to_string();
    Ok(version)
}
