// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{metadata::get_metadata, FilesMapChange, ProcessedFiles};
use crate::{Error, Result, Safe, XorUrl};
use bytes::Bytes;
use log::info;
use sn_client::Error as ClientError;
use std::{
    fs,
    path::{Path, PathBuf},
};
use walkdir::{DirEntry, WalkDir};

const MAX_RECURSIVE_DEPTH: usize = 10_000;

// Upload a file to the Network
pub(crate) async fn upload_file_to_net(safe: &mut Safe, path: &Path) -> Result<XorUrl> {
    let data = fs::read(path).map_err(|err| {
        Error::InvalidInput(format!("Failed to read file from local location: {}", err))
    })?;
    let data = Bytes::from(data);

    let mut mime_type_for_xorurl = mime_guess::from_path(path).first_raw();
    let result = match safe.store_bytes(data.clone(), mime_type_for_xorurl).await {
        Ok(xorurl) => Ok(xorurl),
        Err(Error::InvalidMediaType(_)) => {
            // Let's then upload it and set media-type to be simply raw content
            mime_type_for_xorurl = None;
            safe.store_bytes(data.clone(), mime_type_for_xorurl).await
        }
        other_err => other_err,
    };

    // If the upload verification failed, the file could still have been uploaded successfully,
    // thus let's report the error but providing the xorurl for the user to be aware of.
    if let Err(Error::ClientError(ClientError::NotEnoughChunksRetrieved { .. })) = result {
        // Let's obtain the xorurl with using dry-run mode.
        // Use a dry runner only for this next operation
        let mut dry_runner = Safe::dry_runner(Some(safe.xorurl_base));
        let xorurl = dry_runner.store_bytes(data, mime_type_for_xorurl).await?;

        Err(Error::ContentUploadVerificationFailed(xorurl))
    } else {
        result
    }
}

// Simply change Windows style path separator into `/`
pub(crate) fn normalise_path_separator(from: &str) -> String {
    str::replace(from, "\\", "/")
}

// Walk the local filesystem starting from `location`, creating a list of files paths,
// and if not requested as a `dry_run` upload the files to the network filling up
// the list of files with their corresponding XOR-URLs
pub(crate) async fn file_system_dir_walk(
    safe: &mut Safe,
    location: &Path,
    recursive: bool,
    follow_links: bool,
) -> Result<ProcessedFiles> {
    info!("Reading files from {}", location.display());

    let (metadata, _) = get_metadata(location, follow_links)?;
    if metadata.is_dir() || !recursive {
        // TODO: option to enable following symlinks?
        // We now compare both FilesMaps to upload the missing files
        let max_depth = if recursive { MAX_RECURSIVE_DEPTH } else { 1 };
        let mut processed_files = ProcessedFiles::default();
        let children_to_process = WalkDir::new(location)
            .follow_links(follow_links)
            .into_iter()
            .filter_entry(|e| valid_depth(e, max_depth))
            .filter_map(|v| v.ok());

        for (idx, child) in children_to_process.enumerate() {
            let current_file_path = child.path();
            let current_path_str = current_file_path.to_str().unwrap_or("").to_string();
            info!("Processing {}...", current_path_str);
            let normalised_path = PathBuf::from(normalise_path_separator(&current_path_str));

            let result = get_metadata(current_file_path, follow_links);
            match result {
                Ok((metadata, _)) => {
                    if metadata.file_type().is_dir() {
                        if idx == 0 && normalised_path.display().to_string().ends_with('/') {
                            // If the first directory ends with '/' then it is
                            // the root, and we are only interested in the children,
                            // so we skip it.
                            continue;
                        }
                        if !recursive {
                            // We do not include sub-dirs unless recursing.
                            continue;
                        }
                        // Everything is in the iter. We dont need to recurse.
                        //
                        // so what do we do with dirs? We don't upload them as immutable data.
                        // They are only a type of metadata in the FileContainer.
                        // Empty dirs are not reflected in the paths of uploaded files.
                        // We include dirs with an empty xorurl.
                        // Callers can inspect the file's metadata.
                        processed_files.insert(
                            normalised_path.clone(),
                            FilesMapChange::Added(String::default()),
                        );
                    }

                    if metadata.file_type().is_symlink() {
                        processed_files.insert(
                            normalised_path.clone(),
                            FilesMapChange::Added(String::default()),
                        );
                    }

                    if metadata.file_type().is_file() {
                        match upload_file_to_net(safe, current_file_path).await {
                            Ok(xorurl) => {
                                processed_files
                                    .insert(normalised_path, FilesMapChange::Added(xorurl));
                            }
                            Err(err) => {
                                info!("Skipping file \"{}\". {}", normalised_path.display(), err);
                                processed_files.insert(
                                    normalised_path,
                                    FilesMapChange::Failed(format!("{}", err)),
                                );
                            }
                        }
                    }
                }
                Err(err) => {
                    info!(
                        "Skipping file \"{}\" since no metadata could be read from local location: {:?}",
                        normalised_path.display(), err);
                    processed_files
                        .insert(normalised_path, FilesMapChange::Failed(format!("{}", err)));
                }
            }
        }

        Ok(processed_files)
    } else {
        // Recursive only works on a dir path. Let's error as the user may be making a mistake
        // so it's better for the user to double check and either provide the correct path
        // or remove the 'recursive' flag from the args
        Err(Error::InvalidInput(format!(
            "'{}' is not a directory. The \"recursive\" arg is only supported for folders.",
            location.display()
        )))
    }
}

// Checks if the depth in the dir hierarchy is under a threshold
fn valid_depth(entry: &DirEntry, max_depth: usize) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|_| entry.depth() <= max_depth)
        .unwrap_or(false)
}

// Read the local filesystem at `location`, creating a list of one single file's path,
// and if not as a `dry_run` upload the file to the network and putting
// the obtained XOR-URL in the single file list returned
pub(crate) async fn file_system_single_file(
    safe: &mut Safe,
    location: &Path,
) -> Result<ProcessedFiles> {
    info!("Reading file {}", location.display());
    let (metadata, _) = get_metadata(location, true)?; // follows symlinks.

    // We now compare both FilesMaps to upload the missing files
    let mut processed_files = ProcessedFiles::default();
    let normalised_path = PathBuf::from(normalise_path_separator(&location.display().to_string()));
    if metadata.is_dir() {
        Err(Error::InvalidInput(format!(
            "'{}' is a directory, only individual files can be added. Use files sync operation for uploading folders",
            location.display()
        )))
    } else {
        match upload_file_to_net(safe, location).await {
            Ok(xorurl) => {
                processed_files.insert(normalised_path, FilesMapChange::Added(xorurl));
            }
            Err(err) => {
                info!("Skipping file \"{}\". {}", normalised_path.display(), err);
                processed_files.insert(normalised_path, FilesMapChange::Failed(format!("{}", err)));
            }
        };
        Ok(processed_files)
    }
}
