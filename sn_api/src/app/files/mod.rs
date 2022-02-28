// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod file_system;
mod files_map;
mod metadata;
mod realpath;

use crate::{
    app::consts::*, app::nrs::VersionHash, resolver::Range, ContentType, DataType, Error, Result,
    Safe, SafeUrl, Scope, XorUrl,
};
use bytes::{Buf, Bytes};
use file_system::{
    file_system_dir_walk, file_system_single_file, normalise_path_separator, upload_file_to_net,
};
use files_map::add_or_update_file_item;
use log::{debug, info, warn};
use relative_path::RelativePath;
use safe_network::client::Client;
use sn_interface::types::BytesAddress;
use std::{
    collections::{BTreeMap, HashSet},
    iter::FromIterator,
    path::{Path, PathBuf},
    str,
};

pub(crate) use files_map::{file_map_for_path, get_file_link_and_metadata};
pub(crate) use metadata::FileMeta;
pub(crate) use realpath::RealPath;

pub use files_map::{FileInfo, FilesMap, FilesMapChange, GetAttr};

// List of files uploaded with details if they were added, updated or removed from FilesContainer
pub type ProcessedFiles = BTreeMap<PathBuf, FilesMapChange>;

const ERROR_MSG_NO_FILES_CONTAINER_FOUND: &str = "No FilesContainer found at this address";
// Type tag to use for the FilesContainer stored on Register
pub(crate) const FILES_CONTAINER_TYPE_TAG: u64 = 1_100;

impl Safe {
    /// # Create an empty FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    ///     safe.connect(None, None, None).await.unwrap();
    ///     let xorurl = safe.files_container_create().await.unwrap();
    ///     assert!(xorurl.contains("safe://"))
    /// # });
    /// ```
    pub async fn files_container_create(&self) -> Result<XorUrl> {
        // Build a Register creation operation
        let xorurl = self
            .register_create(
                None,
                FILES_CONTAINER_TYPE_TAG,
                false,
                ContentType::FilesContainer,
            )
            .await?;

        Ok(xorurl)
    }

    /// # Create a FilesContainer containing files uploaded from a local folder.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    ///     safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, _processed_files, _files_map) = safe.files_container_create_from("./testdata", None, true, true).await.unwrap();
    ///     assert!(xorurl.contains("safe://"))
    /// # });
    /// ```
    pub async fn files_container_create_from<P: AsRef<Path>>(
        &self,
        location: P,
        dst: Option<&Path>,
        recursive: bool,
        follow_links: bool,
    ) -> Result<(XorUrl, ProcessedFiles, FilesMap)> {
        // Let's upload the files (if not dry_run) and generate the list of local files paths
        let mut processed_files =
            file_system_dir_walk(self, location.as_ref(), recursive, follow_links).await?;

        // The FilesContainer is stored on a Register
        // and the link to the serialised FilesMap as the entry's value
        let files_map = files_map_create(
            self,
            &mut processed_files,
            location.as_ref(),
            dst,
            follow_links,
        )
        .await?;

        // Create a Register
        let xorurl = self.files_container_create().await?;

        if self.dry_run_mode {
            Ok((xorurl.to_string(), processed_files, files_map))
        } else {
            // Store files map on network
            let files_map_xorurl = self.store_files_map(&files_map).await?;

            let mut reg_url = SafeUrl::from_xorurl(&xorurl)?;

            // Write pointer to files_map onto our register
            let reg_address = self.get_register_address(&reg_url)?;
            let entry = files_map_xorurl.as_bytes().to_vec();
            let client = self.get_safe_client()?;
            let (entry_hash, reg_op) = client
                .write_to_register(reg_address, entry, Default::default())
                .await?;

            client.publish_register_ops(reg_op).await?;

            // We return versioned xorurl
            reg_url.set_content_version(Some(VersionHash::from(&entry_hash)));

            Ok((reg_url.to_string(), processed_files, files_map))
        }
    }

    /// # Fetch an existing FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, _processed_files, _files_map) = safe.files_container_create_from("./testdata", None, true, true).await.unwrap();
    ///     let (version, files_map) = safe.files_container_get(&xorurl).await.unwrap().unwrap();
    ///     println!("FilesContainer fetched is at version: {}", version);
    ///     println!("FilesMap of fetched version is: {:?}", files_map);
    /// # });
    /// ```
    pub async fn files_container_get(&self, url: &str) -> Result<Option<(VersionHash, FilesMap)>> {
        debug!("Getting files container from: {:?}", url);
        let safe_url = self.parse_and_resolve_url(url).await?;

        self.fetch_files_container(&safe_url).await
    }

    /// Fetch a FilesContainer from a SafeUrl without performing any type of URL resolution
    pub(crate) async fn fetch_files_container(
        &self,
        safe_url: &SafeUrl,
    ) -> Result<Option<(VersionHash, FilesMap)>> {
        // fetch register entries and wrap errors
        debug!(
            "Fetching FilesContainer from {}, address type: {:?}",
            safe_url,
            safe_url.address()
        );

        let entries = self
            .register_fetch_entries(safe_url)
            .await
            .map_err(|e| match e {
                Error::ContentNotFound(_) => {
                    Error::ContentNotFound(ERROR_MSG_NO_FILES_CONTAINER_FOUND.to_string())
                }
                Error::HashNotFound(_) => Error::VersionNotFound(format!(
                    "Version '{}' is invalid for FilesContainer found at \"{}\"",
                    match safe_url.content_version() {
                        Some(v) => v.to_string(),
                        None => "None".to_owned(),
                    },
                    safe_url
                )),
                err => Error::NetDataError(format!("Failed to get current version: {}", err)),
            })?;

        // take the 1st entry (TODO Multiple entries)
        debug!(
            "Retrieved {} entries for register at {}",
            entries.len(),
            safe_url.to_string()
        );
        if entries.len() > 1 {
            return Err(Error::NotImplementedError("Multiple file container entries not managed, this happends when 2 clients write concurrently to a file container".to_string()));
        }
        let first_entry = entries.iter().next();
        let (version, files_map_xorurl) = if let Some((v, m)) = first_entry {
            (v.into(), str::from_utf8(m)?)
        } else {
            warn!("FilesContainer found at \"{:?}\" was empty", safe_url);
            return Ok(None);
        };

        // Using the FilesMap XOR-URL we can now fetch the FilesMap and deserialise it
        let files_map_url = SafeUrl::from_xorurl(files_map_xorurl)?;
        let serialised_files_map = self.fetch_public_data(&files_map_url, None).await?;
        let files_map = serde_json::from_slice(serialised_files_map.chunk()).map_err(|err| {
            Error::ContentError(format!(
                "Couldn't deserialise the FilesMap stored in the FilesContainer: {:?}",
                err
            ))
        })?;
        debug!("Files map retrieved.... {:?}", &version);

        Ok(Some((version, files_map)))
    }

    /// # Sync up local folder with the content on a FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, _processed_files, _files_map) = safe.files_container_create_from("./testdata", None, true, false).await.unwrap();
    ///     let (version, new_processed_files, new_files_map) = safe.files_container_sync("./testdata", &xorurl, true, true, false, false).await.unwrap();
    ///     println!("FilesContainer synced up is at version: {}", version);
    ///     println!("The local files that were synced up are: {:?}", new_processed_files);
    ///     println!("The FilesMap of the updated FilesContainer now is: {:?}", new_files_map);
    /// # });
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn files_container_sync<P: AsRef<Path>>(
        &self,
        location: P,
        url: &str,
        recursive: bool,
        follow_links: bool,
        delete: bool,
        update_nrs: bool,
    ) -> Result<(Option<(VersionHash, FilesMap)>, ProcessedFiles)> {
        if delete && !recursive {
            return Err(Error::InvalidInput(
                "'delete' is not allowed if 'recursive' is not set".to_string(),
            ));
        }

        let safe_url = SafeUrl::from_url(url)?;

        // If NRS name shall be updated then the URL has to be an NRS-URL
        if update_nrs && safe_url.content_type() != ContentType::NrsMapContainer {
            return Err(Error::InvalidInput(
                "'update-nrs' is not allowed since the URL provided is not an NRS URL".to_string(),
            ));
        }

        let mut safe_url = self.parse_and_resolve_url(url).await?;

        // If the FilesContainer URL was resolved from an NRS name we need to remove
        // the version from it so we can fetch latest version of it for sync-ing
        safe_url.set_content_version(None);

        let (current_version, current_files_map) =
            match self.fetch_files_container(&safe_url).await? {
                Some((version, files_map)) => (Some(version), files_map),
                None => (None, FilesMap::default()),
            };

        // Let's generate the list of local files paths, without uploading any new file yet.
        // Use a dry runner only for this next operation
        let dry_runner = Safe::dry_runner(Some(self.xorurl_base));
        let processed_files =
            file_system_dir_walk(&dry_runner, location.as_ref(), recursive, follow_links).await?;

        let dst_path = Path::new(safe_url.path());

        let (processed_files, new_files_map, success_count) = files_map_sync(
            self,
            current_files_map,
            location.as_ref(),
            processed_files,
            Some(dst_path),
            delete,
            false,
            true,
            follow_links,
        )
        .await?;

        self.update_files_container(
            success_count,
            current_version,
            new_files_map,
            processed_files,
            url,
            safe_url,
            update_nrs,
        )
        .await
    }

    /// # Add a file, either a local path or an already uploaded file, on an existing FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, _processed_files, _files_map) = safe.files_container_create_from("./testdata", None, true, true).await.unwrap();
    ///     let new_file_name = format!("{}/new_name_test.md", xorurl);
    ///     let (version, new_processed_files, new_files_map) = safe.files_container_add("./testdata/test.md", &new_file_name, false, false, true).await.unwrap();
    ///     println!("FilesContainer is now at version: {}", version);
    ///     println!("The local files that were synced up are: {:?}", new_processed_files);
    ///     println!("The FilesMap of the updated FilesContainer now is: {:?}", new_files_map);
    /// # });
    /// ```
    pub async fn files_container_add(
        &self,
        source_file: &str,
        url: &str,
        force: bool,
        update_nrs: bool,
        follow_links: bool,
    ) -> Result<(Option<(VersionHash, FilesMap)>, ProcessedFiles)> {
        debug!("Adding file to FilesContainer at {}", url);
        let (safe_url, current_version, current_files_map) =
            validate_files_add_params(self, source_file, url, update_nrs).await?;

        let dst_path = Path::new(safe_url.path());

        // Let's act according to if it's a local file path or a safe:// location
        let (processed_files, new_files_map, success_count) = if source_file.starts_with("safe://")
        {
            files_map_add_link(self, current_files_map, source_file, dst_path, force).await?
        } else {
            // We then assume source is a local path
            let source_path = Path::new(source_file);

            // Let's generate the list of local files paths, without uploading any new file yet.
            // Use dry runner only for this next operation
            let dry_runner = Safe::dry_runner(Some(self.xorurl_base));
            let processed_files = file_system_single_file(&dry_runner, source_path).await?;

            files_map_sync(
                self,
                current_files_map,
                source_path,
                processed_files,
                Some(dst_path),
                false,
                force,
                false,
                follow_links,
            )
            .await?
        };

        self.update_files_container(
            success_count,
            current_version,
            new_files_map,
            processed_files,
            url,
            safe_url,
            update_nrs,
        )
        .await
    }

    /// # Add a file, from raw bytes, on an existing FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, _processed_files, _files_map) = safe.files_container_create_from("./testdata", None, true, true).await.unwrap();
    ///     let new_file_name = format!("{}/new_name_test.md", xorurl);
    ///     let (version, new_processed_files, new_files_map) = safe.files_container_add_from_raw(b"0123456789", &new_file_name, false, false).await.unwrap();
    ///     println!("FilesContainer is now at version: {}", version);
    ///     println!("The local files that were synced up are: {:?}", new_processed_files);
    ///     println!("The FilesMap of the updated FilesContainer now is: {:?}", new_files_map);
    /// # });
    /// ```
    pub async fn files_container_add_from_raw(
        &self,
        data: Bytes,
        url: &str,
        force: bool,
        update_nrs: bool,
    ) -> Result<(Option<(VersionHash, FilesMap)>, ProcessedFiles)> {
        let (safe_url, current_version, current_files_map) =
            validate_files_add_params(self, "", url, update_nrs).await?;

        let new_file_xorurl = self.store_public_bytes(data, None).await?;

        let dst_path = Path::new(safe_url.path());
        let (processed_files, new_files_map, success_count) =
            files_map_add_link(self, current_files_map, &new_file_xorurl, dst_path, force).await?;

        self.update_files_container(
            success_count,
            current_version,
            new_files_map,
            processed_files,
            url,
            safe_url,
            update_nrs,
        )
        .await
    }

    /// # Remove a file from an existing FilesContainer.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let (xorurl, processed_files, files_map) = safe.files_container_create_from("./testdata/", None, true, true).await.unwrap();
    ///     let remote_file_path = format!("{}/test.md", xorurl);
    ///     let (version, new_processed_files, new_files_map) = safe.files_container_remove_path(&remote_file_path, false, false).await.unwrap();
    ///     println!("FilesContainer is now at version: {}", version);
    ///     println!("The files that were removed: {:?}", new_processed_files);
    ///     println!("The FilesMap of the updated FilesContainer now is: {:?}", new_files_map);
    /// # });
    /// ```
    pub async fn files_container_remove_path(
        &self,
        url: &str,
        recursive: bool,
        update_nrs: bool,
    ) -> Result<(VersionHash, ProcessedFiles, FilesMap)> {
        let safe_url = SafeUrl::from_url(url)?;
        let dst_path = safe_url.path();
        if dst_path.is_empty() {
            return Err(Error::InvalidInput(
                "The destination URL should include a target file path".to_string(),
            ));
        }

        // If NRS name shall be updated then the URL has to be an NRS-URL
        if update_nrs && safe_url.content_type() != ContentType::NrsMapContainer {
            return Err(Error::InvalidInput(
                "'update-nrs' is not allowed since the URL provided is not an NRS URL".to_string(),
            ));
        }

        let mut safe_url = self.parse_and_resolve_url(url).await?;

        // If the FilesContainer URL was resolved from an NRS name we need to remove
        // the version from it so we can fetch latest version of it
        safe_url.set_content_version(None);

        let (current_version, files_map) = match self.fetch_files_container(&safe_url).await? {
            Some(info) => info,
            None => {
                return Err(Error::EmptyContent(format!(
                    "FilesContainer found at \"{}\" was empty",
                    safe_url
                )))
            }
        };

        let (processed_files, new_files_map, success_count) =
            files_map_remove_path(Path::new(dst_path), files_map, recursive)?;

        let version = if success_count == 0 {
            current_version
        } else {
            self.append_version_to_files_container(
                HashSet::from_iter([current_version]),
                &new_files_map,
                url,
                safe_url,
                update_nrs,
            )
            .await?
        };

        Ok((version, processed_files, new_files_map))
    }

    // Private helper to append new FilesMap entry to container, and/or return
    // information regarding the update and new version if so
    #[allow(clippy::too_many_arguments)]
    async fn update_files_container(
        &self,
        files_map_changes_count: u64,
        current_version: Option<VersionHash>,
        new_files_map: FilesMap,
        processed_files: ProcessedFiles,
        url: &str,
        safe_url: SafeUrl,
        update_nrs: bool,
    ) -> Result<(Option<(VersionHash, FilesMap)>, ProcessedFiles)> {
        if files_map_changes_count == 0 {
            if let Some(version) = current_version {
                // We had a FilesMap but there were no changes to it, so let's
                // return the existing version and files map, along with
                // details about the processed files.
                // Note: the 'new_files_map' should be the same as to 'current_files_map'.
                Ok((Some((version, new_files_map)), processed_files))
            } else {
                // The container was empty, and is still empty, but let's return
                // the details about proessed files still
                Ok((None, processed_files))
            }
        } else {
            // There were changes to current FilesMap, so append new version to the container
            let parent_versions = if let Some(version) = current_version {
                HashSet::from_iter([version])
            } else {
                HashSet::new()
            };

            let new_version = self
                .append_version_to_files_container(
                    parent_versions,
                    &new_files_map,
                    url,
                    safe_url,
                    update_nrs,
                )
                .await?;

            Ok((Some((new_version, new_files_map)), processed_files))
        }
    }

    // Private helper function to append new version of the FilesMap to the Files Container
    // It flagged with `update_nrs`, it will also update the link in the corresponding NRS Map Container
    #[allow(clippy::too_many_arguments)]
    async fn append_version_to_files_container(
        &self,
        current_version: HashSet<VersionHash>,
        new_files_map: &FilesMap,
        url: &str,
        mut safe_url: SafeUrl,
        update_nrs: bool,
    ) -> Result<VersionHash> {
        // The FilesContainer is updated by adding an entry containing the link to
        // the file with the serialised new version of the FilesMap.
        let files_map_xorurl = if !self.dry_run_mode {
            self.store_files_map(new_files_map).await?
        } else {
            "".to_string()
        };

        // append entry to register
        let entry = files_map_xorurl.as_bytes().to_vec();
        let replace = current_version.iter().map(|e| e.entry_hash()).collect();
        let entry_hash = &self
            .register_write(&safe_url.to_string(), entry, replace)
            .await?;
        let new_version: VersionHash = entry_hash.into();

        if update_nrs {
            // We need to update the link in the NRS container as well,
            // to link it to the new new_version of the FilesContainer we just generated
            safe_url.set_content_version(Some(new_version));
            let nrs_url = SafeUrl::from_url(url)?;
            let top_name = nrs_url.top_name();
            let _ = self.nrs_associate(top_name, &safe_url).await?;
        }

        Ok(new_version)
    }

    /// # Store a public file
    ///
    /// Store files onto the network. The data will be saved as one or more chunks,
    /// depending on the size of the data. If it's less than 3072 bytes, it'll be stored in a single chunk,
    /// otherwise, it'll be stored in multiple chunks.
    ///
    /// ## Example
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let data = b"Something super good";
    ///     let xorurl = safe.store_public_data(data, Some("text/plain")).await.unwrap();
    ///     let received_data = safe.files_get_public(&xorurl, None).await.unwrap();
    ///     assert_eq!(received_data, data);
    /// # });
    /// ```
    pub async fn store_public_bytes(
        &self,
        bytes: Bytes,
        media_type: Option<&str>,
    ) -> Result<XorUrl> {
        let content_type = media_type.map_or_else(
            || Ok(ContentType::Raw),
            |media_type_str| {
                if SafeUrl::is_media_type_supported(media_type_str) {
                    Ok(ContentType::MediaType(media_type_str.to_string()))
                } else {
                    Err(Error::InvalidMediaType(format!(
                        "Media-type '{}' not supported. You can pass 'None' as the 'media_type' for this content to be treated as raw",
                        media_type_str
                    )))
                }
            },
        )?;

        let address = if self.dry_run_mode {
            debug!(
                "Calculating network address for {} bytes of data",
                bytes.len()
            );
            Client::calculate_address(bytes, Scope::Public)?
        } else {
            debug!("Storing {} bytes of data", bytes.len());
            let client = self.get_safe_client()?;
            let (address, _) = client.upload_and_verify(bytes, Scope::Public).await?;
            address
        };
        let xorurl = SafeUrl::encode_bytes(address, content_type, self.xorurl_base)?;

        Ok(xorurl)
    }

    /// # Get a file
    /// Get file from the network.
    ///
    /// ## Example
    /// ```no_run
    /// # use sn_api::Safe;
    /// # let mut safe = Safe::default();
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// #   safe.connect(None, None, None).await.unwrap();
    ///     let data = b"Something super good";
    ///     let xorurl = safe.files_store_public(data, None).await.unwrap();
    ///     let received_data = safe.files_get_public(&xorurl, None).await.unwrap();
    ///     assert_eq!(received_data, data);
    /// # });
    /// ```
    pub async fn files_get_public(&self, url: &str, range: Range) -> Result<Bytes> {
        // TODO: do we want ownership from other PKs yet?
        let safe_url = self.parse_and_resolve_url(url).await?;
        self.fetch_public_data(&safe_url, range).await
    }

    /// Fetch an file from a SafeUrl without performing any type of URL resolution
    pub(crate) async fn fetch_public_data(
        &self,
        safe_url: &SafeUrl,
        range: Range,
    ) -> Result<Bytes> {
        match safe_url.data_type() {
            DataType::File => {
                self.get_bytes(BytesAddress::Public(safe_url.xorname()), range)
                    .await
            }
            other => {
                return Err(Error::ContentError(format!("{}", other)));
            }
        }
    }

    pub async fn get_bytes(&self, address: BytesAddress, range: Range) -> Result<Bytes> {
        debug!("Attempting to fetch data from {:?}", address.name());
        let client = self.get_safe_client()?;
        let data = if let Some((start, end)) = range {
            let start = start.map(|start_index| start_index as usize).unwrap_or(0);
            let len = end
                .map(|end_index| end_index as usize - start)
                .unwrap_or(usize::MAX);

            client.read_from(address, start, len).await
        } else {
            client.read_bytes(address).await
        }
        .map_err(|e| Error::NetDataError(format!("Failed to GET file: {:?}", e)))?;

        debug!(
            "{} bytes of data successfully retrieved from: {:?}",
            data.len(),
            address.name()
        );

        Ok(data)
    }

    // Private helper to serialise a FilesMap and store it in a file
    async fn store_files_map(&self, files_map: &FilesMap) -> Result<String> {
        // The FilesMapContainer is a Register where each NRS Map version is
        // an entry containing the XOR-URL of the file that contains the serialised NrsMap.
        let serialised_files_map = serde_json::to_string(&files_map).map_err(|err| {
            Error::Serialisation(format!(
                "Couldn't serialise the FilesMap generated: {:?}",
                err
            ))
        })?;

        let files_map_xorurl = self
            .store_public_bytes(Bytes::from(serialised_files_map), None)
            .await?;

        Ok(files_map_xorurl)
    }
}

// Helper functions

// Make sure the input params are valid for a files_container_add operation
async fn validate_files_add_params(
    safe: &Safe,
    source_file: &str,
    url: &str,
    update_nrs: bool,
) -> Result<(SafeUrl, Option<VersionHash>, FilesMap)> {
    let safe_url = SafeUrl::from_url(url)?;

    // If NRS name shall be updated then the URL has to be an NRS-URL
    if update_nrs && safe_url.content_type() != ContentType::NrsMapContainer {
        return Err(Error::InvalidInput(
            "'update-nrs' is not allowed since the URL provided is not an NRS URL".to_string(),
        ));
    }

    let mut safe_url = safe.parse_and_resolve_url(url).await?;

    // If the FilesContainer URL was resolved from an NRS name we need to remove
    // the version from it so we can fetch latest version of it for sync-ing
    safe_url.set_content_version(None);

    // Let's act according to if it's a local file path or a safe:// location
    if source_file.starts_with("safe://") {
        let source_safe_url = SafeUrl::from_url(source_file)?;
        if source_safe_url.data_type() != DataType::File {
            return Err(Error::InvalidInput(format!(
                "The source URL should target a file ('{}'), but the URL provided targets a '{}'",
                DataType::File,
                source_safe_url.content_type()
            )));
        }

        if safe_url.path().is_empty() {
            return Err(Error::InvalidInput(
                "The destination URL should include a target file path since we are adding a link"
                    .to_string(),
            ));
        }
    }

    let (current_version, current_files_map) = match safe.fetch_files_container(&safe_url).await? {
        Some((version, files_map)) => (Some(version), files_map),
        None => (None, FilesMap::default()),
    };

    Ok((safe_url, current_version, current_files_map))
}

// From the location path and the destination path chosen by the user, calculate
// the destination path considering ending '/' in both the location and dst path
fn get_base_paths(location: &Path, dst_path: Option<&Path>) -> (String, String) {
    // Let's normalise the path to use '/' (instead of '\' as on Windows)
    let location_base_path = if location.to_str() == Some(".") {
        String::from("./")
    } else {
        normalise_path_separator(&location.display().to_string())
    };

    let new_dst_path = match dst_path {
        Some(path) => {
            let path_str = path.display().to_string();
            if path_str.is_empty() {
                "/".to_string()
            } else {
                path_str
            }
        }
        None => "/".to_string(),
    };

    // Let's first check if it ends with '/'
    let dst_base_path = if new_dst_path.ends_with('/') {
        if location_base_path.ends_with('/') {
            new_dst_path
        } else {
            // Location is a folder, then append it to dst path
            let parts_vec: Vec<&str> = location_base_path.split('/').collect();
            let dir_name = parts_vec[parts_vec.len() - 1];
            format!("{}{}", new_dst_path, dir_name)
        }
    } else {
        // Then just append an ending '/'
        format!("{}/", new_dst_path)
    };

    (location_base_path, dst_base_path)
}

// From the provided list of local files paths, find the local changes made in comparison with the
// target FilesContainer, uploading new files as necessary, and creating a new FilesMap with file's
// metadata and their corresponding links, as well as generating the report of processed files
#[allow(clippy::too_many_arguments)]
async fn files_map_sync(
    safe: &Safe,
    mut current_files_map: FilesMap,
    location: &Path,
    new_content: ProcessedFiles,
    dst_path: Option<&Path>,
    delete: bool,
    force: bool,
    compare_file_content: bool,
    follow_links: bool,
) -> Result<(ProcessedFiles, FilesMap, u64)> {
    let (location_base_path, dst_base_path) = get_base_paths(location, dst_path);
    let mut updated_files_map = FilesMap::new();
    let mut processed_files = ProcessedFiles::new();
    let mut success_count = 0;

    for (local_file_name, _) in new_content.iter().filter(|(_, change)| change.is_success()) {
        let file_path = Path::new(&local_file_name);

        let file_name = RelativePath::new(
            &local_file_name
                .display()
                .to_string()
                .replace(&location_base_path, &dst_base_path),
        )
        .normalize();
        // Above normalize removes initial slash, and uses '\' if it's on Windows
        // here, we trim any trailing '/', as it could be a filename.
        let mut normalised_file_name = format!("/{}", normalise_path_separator(file_name.as_str()))
            .trim_end_matches('/')
            .to_string();

        if normalised_file_name.is_empty() {
            normalised_file_name = "/".to_string();
        }

        // Let's update FileInfo if there is a change or it doesn't exist in current_files_map
        match current_files_map.get(&normalised_file_name) {
            None => {
                // We need to add a new FileInfo
                if add_or_update_file_item(
                    safe,
                    local_file_name,
                    &normalised_file_name,
                    file_path,
                    &FileMeta::from_path(local_file_name, follow_links)?,
                    None, // no xorurl link
                    false,
                    &mut updated_files_map,
                    &mut processed_files,
                )
                .await
                {
                    success_count += 1;

                    // We remove self and any parent directories
                    // from the current list so we know it has been processed
                    let mut trail = Vec::<&str>::new();
                    for part in normalised_file_name.split('/') {
                        trail.push(part);
                        let ancestor = if trail.len() > 1 {
                            trail.join("/")
                        } else {
                            "/".to_string()
                        };
                        if ancestor != normalised_file_name {
                            if let Some(fi) = current_files_map.get(&ancestor) {
                                updated_files_map.insert(ancestor.clone(), fi.clone());
                                current_files_map.remove(&ancestor);
                            }
                        }
                    }
                }
            }
            Some(file_item) => {
                let is_modified =
                    is_file_item_modified(safe, Path::new(local_file_name), file_item).await;
                if force || (compare_file_content && is_modified) {
                    // We need to update the current FileInfo
                    if add_or_update_file_item(
                        safe,
                        local_file_name,
                        &normalised_file_name,
                        file_path,
                        &FileMeta::from_path(local_file_name.as_path(), follow_links)?,
                        None, // no xorurl link
                        true,
                        &mut updated_files_map,
                        &mut processed_files,
                    )
                    .await
                    {
                        success_count += 1;
                    }
                } else {
                    // No need to update FileInfo just copy the existing one
                    updated_files_map.insert(normalised_file_name.to_string(), file_item.clone());

                    if !force && !compare_file_content {
                        let (err_type, comp_str) = if is_modified {
                            (
                                Error::FileNameConflict(normalised_file_name.clone()),
                                "different",
                            )
                        } else {
                            (
                                Error::FileAlreadyExists(normalised_file_name.clone()),
                                "same",
                            )
                        };

                        processed_files.insert(
                            local_file_name.to_path_buf(),
                            FilesMapChange::Failed(format!("{}", err_type)),
                        );
                        info!("Skipping file \"{}\" since a file named \"{}\" with {} content already exists on target. You can use the 'force' flag to replace the existing file with the new one", local_file_name.display(), normalised_file_name, comp_str);
                    }
                }

                // let's now remove it from the current list so we now it has been processed
                current_files_map.remove(&normalised_file_name);

                // We also remove any parent directories
                // from the current list, so they will not be deleted.
                let mut trail = Vec::<&str>::new();
                for part in normalised_file_name.split('/') {
                    trail.push(part);
                    let ancestor = if trail.len() > 1 {
                        trail.join("/")
                    } else {
                        "/".to_string()
                    };
                    if ancestor != normalised_file_name {
                        if let Some(fi) = current_files_map.get(&ancestor) {
                            updated_files_map.insert(ancestor.clone(), fi.clone());
                            current_files_map.remove(&ancestor);
                        }
                    }
                }
            }
        }
    }

    // Finally, unless 'delete' was set keep the files that are currently
    // in FilesContainer but not in source location
    current_files_map.iter().for_each(|(file_name, file_item)| {
        if !delete {
            updated_files_map.insert(file_name.to_string(), file_item.clone());
        } else {
            // note: files have link property, dirs and symlinks do not
            let xorurl = file_item
                .get(PREDICATE_LINK)
                .unwrap_or(&String::default())
                .to_string();

            processed_files.insert(PathBuf::from(file_name), FilesMapChange::Removed(xorurl));
            success_count += 1;
        }
    });

    Ok((processed_files, updated_files_map, success_count))
}

async fn is_file_item_modified(safe: &Safe, local_filename: &Path, file_item: &FileInfo) -> bool {
    if FileMeta::filetype_is_file(&file_item[PREDICATE_TYPE]) {
        // Use a dry runner only for this next operation
        let dry_runner = Safe::dry_runner(Some(safe.xorurl_base));
        let is_uploaded = match upload_file_to_net(&dry_runner, local_filename).await {
            Ok(local_xorurl) => file_item[PREDICATE_LINK] != local_xorurl,
            Err(_) => false,
        };

        is_uploaded
    } else {
        // for now, we just return false if a symlink or directory.
        // In the future, should check if symlink has been modified.
        // Also, could check if ctime or mtime is different, though that
        // could apply to files as well, and some use-cases would not want to
        // sync remotely if actual content has not changed.  So it should
        // probably be a user flag to enable.
        false
    }
}

async fn files_map_add_link(
    safe: &Safe,
    mut files_map: FilesMap,
    file_link: &str,
    file_name: &Path,
    force: bool,
) -> Result<(ProcessedFiles, FilesMap, u64)> {
    let mut processed_files = ProcessedFiles::new();
    let mut success_count = 0;
    let file_type = match SafeUrl::from_url(file_link) {
        Err(err) => {
            info!("Skipping file \"{}\". {}", file_link, err);
            processed_files.insert(
                PathBuf::from(file_link),
                FilesMapChange::Failed(format!("{}", err)),
            );
            return Ok((processed_files, files_map, success_count));
        }
        Ok(safe_url) => match safe_url.content_type() {
            ContentType::MediaType(media_type) => media_type,
            other => format!("{}", other),
        },
    };

    let file_path = Path::new("");
    let file_size = ""; // unknown
    let file_name_str = file_name.display().to_string();

    // Let's update FileInfo if the link is different or it doesn't exist in the files_map
    let dry_runner = Safe::dry_runner(Some(safe.xorurl_base));
    match files_map.get(&file_name_str) {
        Some(current_file_item) => {
            let mut file_meta = FileMeta::from_file_item(current_file_item);
            file_meta.file_type = file_type;
            file_meta.file_size = file_size.to_string();

            let is_modified = if file_meta.is_file() {
                current_file_item[PREDICATE_LINK] != file_link
            } else {
                // directory: nothing to check.
                // symlink: TODO: check if sym-link path has changed.
                false
            };

            if is_modified {
                if force {
                    if add_or_update_file_item(
                        &dry_runner,
                        file_name,
                        &file_name_str,
                        file_path,
                        &file_meta,
                        Some(file_link),
                        true,
                        &mut files_map,
                        &mut processed_files,
                    )
                    .await
                    {
                        success_count += 1;
                    }
                } else {
                    info!("Skipping file \"{}\" since a file with name \"{}\" already exists on target. You can use the 'force' flag to replace the existing file with the new one", file_link, file_name_str);
                    processed_files.insert(
                        file_name.to_path_buf(),
                        FilesMapChange::Failed(format!(
                            "<{}>",
                            Error::FileNameConflict(file_name_str)
                        )),
                    );
                }
            } else {
                info!("Skipping file \"{}\" since a file with name \"{}\" already exists on target with the same link", file_link, file_name_str);
                processed_files.insert(
                    PathBuf::from(file_link),
                    FilesMapChange::Failed(format!(
                        "<{}>",
                        Error::FileAlreadyExists(file_name_str)
                    )),
                );
            }
        }
        None => {
            if add_or_update_file_item(
                &dry_runner,
                file_name,
                &file_name_str,
                file_path,
                &FileMeta::from_type_and_size(&file_type, file_size),
                Some(file_link),
                false,
                &mut files_map,
                &mut processed_files,
            )
            .await
            {
                success_count += 1;
            }
        }
    };

    Ok((processed_files, files_map, success_count))
}

// Remove a path from the FilesMap provided
fn files_map_remove_path(
    dst_path: &Path,
    mut files_map: FilesMap,
    recursive: bool,
) -> Result<(ProcessedFiles, FilesMap, u64)> {
    let mut processed_files = ProcessedFiles::default();
    let (success_count, new_files_map) = if recursive {
        let mut success_count = 0;
        let mut new_files_map = FilesMap::default();
        let folder_path = if !dst_path.ends_with("/") {
            format!("{}/", dst_path.display())
        } else {
            dst_path.display().to_string()
        };

        files_map.iter().for_each(|(file_path, file_item)| {
            // if the current file_path is a subfolder we remove it
            if file_path.starts_with(&folder_path) {
                // note: files have link property, dirs and symlinks do not
                let xorurl = file_item
                    .get(PREDICATE_LINK)
                    .unwrap_or(&String::default())
                    .to_string();

                processed_files.insert(PathBuf::from(file_path), FilesMapChange::Removed(xorurl));
                success_count += 1;
            } else {
                new_files_map.insert(file_path.to_string(), file_item.clone());
            }
        });
        (success_count, new_files_map)
    } else {
        let file_item = files_map
            .remove(&dst_path.display().to_string())
            .ok_or_else(|| Error::ContentError(format!(
                "No content found matching the \"{}\" path on the target FilesContainer. If you are trying to remove a folder rather than a file, you need to pass the 'recursive' flag",
                dst_path.display()
            )))?;

        // note: files have link property, dirs and symlinks do not
        let xorurl = file_item
            .get(PREDICATE_LINK)
            .unwrap_or(&String::default())
            .to_string();

        processed_files.insert(dst_path.to_path_buf(), FilesMapChange::Removed(xorurl));

        (1, files_map)
    };

    Ok((processed_files, new_files_map, success_count))
}

// From the provided list of local files paths and corresponding files XOR-URLs,
// create a FilesMap with file's metadata and their corresponding links
async fn files_map_create(
    safe: &Safe,
    content: &mut ProcessedFiles,
    location: &Path,
    dst_path: Option<&Path>,
    follow_links: bool,
) -> Result<FilesMap> {
    let mut files_map = FilesMap::default();

    let (location_base_path, dst_base_path) = get_base_paths(location, dst_path);

    // We want to iterate over the BTreeMap and also modify it.
    // We DON'T want to clone/dup the whole thing, might be very big.
    // Rust doesn't allow that exactly, but we can get the keys
    // to iterate over instead.  Cloning the keys isn't ideal
    // either, but is much less data.  Is there a more efficient way?
    let names = content.keys().cloned().collect::<Vec<_>>();
    for file_name in names {
        let link = match &content[&file_name] {
            FilesMapChange::Failed(_) => continue,
            FilesMapChange::Added(link)
            | FilesMapChange::Updated(link)
            | FilesMapChange::Removed(link) => link.clone(),
        };

        let new_file_name = RelativePath::new(
            &file_name
                .display()
                .to_string()
                .replace(&location_base_path, &dst_base_path),
        )
        .normalize();

        // Above normalize removes initial slash, and uses '\' if it's on Windows
        // here, we trim any trailing '/', as it could be a filename.
        let final_name = format!("/{}", normalise_path_separator(new_file_name.as_str()))
            .trim_end_matches('/')
            .to_string();

        debug!("FileInfo item name: {:?}", &file_name);

        add_or_update_file_item(
            safe,
            &file_name,
            &final_name,
            &file_name,
            &FileMeta::from_path(&file_name, follow_links)?,
            if link.is_empty() { None } else { Some(&link) },
            false,
            &mut files_map,
            content,
        )
        .await;
    }

    Ok(files_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::test_helpers::{new_safe_instance, random_nrs_name},
        register::EntryHash,
        retry_loop, retry_loop_for_pattern,
    };
    use anyhow::{anyhow, bail, Result};
    use assert_matches::assert_matches;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    const TEST_DATA_FOLDER: &str = "./testdata/";
    const TEST_DATA_FOLDER_NO_SLASH: &str = "./testdata";

    // make some constants for these, in case entries in the testdata folder change.
    const TESTDATA_PUT_FILEITEM_COUNT: usize = 11;
    const TESTDATA_PUT_FILESMAP_COUNT: usize = 10; // TODO: review case of empty folder and empty file
    const TESTDATA_NO_SLASH_PUT_FILEITEM_COUNT: usize = 12;
    const TESTDATA_NO_SLASH_PUT_FILESMAP_COUNT: usize = 11; // TODO: review case of empty file
    const SUBFOLDER_PUT_FILEITEM_COUNT: usize = 2;
    const SUBFOLDER_NO_SLASH_PUT_FILEITEM_COUNT: usize = 3;

    // Helper function to create a files container with all files from TEST_DATA_FOLDER
    async fn new_files_container_from_testdata(
        safe: &Safe,
    ) -> Result<(String, ProcessedFiles, FilesMap)> {
        let (xorurl, processed_files, files_map) =
            retry_loop!(safe.files_container_create_from(TEST_DATA_FOLDER, None, true, true,));

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), TESTDATA_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), TESTDATA_PUT_FILESMAP_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));

        Ok((xorurl, processed_files, files_map))
    }

    #[tokio::test]
    async fn test_files_map_create() -> Result<()> {
        let safe = new_safe_instance().await?;
        let mut processed_files = ProcessedFiles::new();
        let first_xorurl = SafeUrl::from_url("safe://top_xorurl")?.to_xorurl_string();
        let second_xorurl = SafeUrl::from_url("safe://second_xorurl")?.to_xorurl_string();

        processed_files.insert(
            PathBuf::from("./testdata/test.md"),
            FilesMapChange::Added(first_xorurl.clone()),
        );
        processed_files.insert(
            PathBuf::from("./testdata/subfolder/subexists.md"),
            FilesMapChange::Added(second_xorurl.clone()),
        );
        let files_map = files_map_create(
            &safe,
            &mut processed_files,
            Path::new(TEST_DATA_FOLDER_NO_SLASH),
            Some(Path::new("")),
            true,
        )
        .await?;
        assert_eq!(files_map.len(), 2);
        let file_item1 = &files_map["/testdata/test.md"];
        assert_eq!(file_item1[PREDICATE_LINK], first_xorurl);
        assert_eq!(file_item1[PREDICATE_TYPE], "text/markdown");
        assert_eq!(file_item1[PREDICATE_SIZE], "12");

        let file_item2 = &files_map["/testdata/subfolder/subexists.md"];
        assert_eq!(file_item2[PREDICATE_LINK], second_xorurl);
        assert_eq!(file_item2[PREDICATE_TYPE], "text/markdown");
        assert_eq!(file_item2[PREDICATE_SIZE], "23");
        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_empty() -> Result<()> {
        let safe = new_safe_instance().await?;
        let xorurl = safe.files_container_create().await?;

        assert!(xorurl.starts_with("safe://"));

        let _ = retry_loop!(safe.fetch(&xorurl, None));

        // we check that the container is empty, i.e. no entry in the underlying Register.
        let file_map = retry_loop!(safe.files_container_get(&xorurl));
        assert!(file_map.is_none());

        // let's add a file
        let (content, new_processed_files) = safe
            .files_container_add("./testdata/test.md", &xorurl, false, false, false)
            .await?;
        let (_, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), 1);

        let filename = Path::new("./testdata/test.md");
        assert!(new_processed_files[filename].is_added());
        assert_eq!(
            new_processed_files[filename].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_store_public() -> Result<()> {
        let safe = new_safe_instance().await?;
        let random_content: String = thread_rng().sample_iter(&Alphanumeric).take(20).collect();

        let file_xorurl = safe
            .store_public_bytes(Bytes::from(random_content.to_owned()), None)
            .await?;

        let retrieved = retry_loop!(safe.files_get_public(&file_xorurl, None));
        assert_eq!(retrieved, random_content.as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_file() -> Result<()> {
        let safe = new_safe_instance().await?;
        let filename = Path::new("./testdata/test.md");
        let (xorurl, processed_files, files_map) = safe
            .files_container_create_from(&filename.display().to_string(), None, false, false)
            .await?;

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), 1);
        assert_eq!(files_map.len(), 1);
        assert!(processed_files[filename].is_added());
        assert_eq!(
            processed_files[filename].link(),
            Some(&files_map["/test.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_dry_run() -> Result<()> {
        let mut safe = new_safe_instance().await?;
        safe.dry_run_mode = true;
        let (xorurl, processed_files, files_map) = safe
            .files_container_create_from(TEST_DATA_FOLDER, None, true, false)
            .await?;

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), TESTDATA_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), TESTDATA_PUT_FILESMAP_COUNT);

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_matches!(processed_files[filename1].link(), Some(link) if !link.is_empty());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_matches!(processed_files[filename2].link(), Some(link) if !link.is_empty());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_matches!(processed_files[filename3].link(), Some(link) if !link.is_empty());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_matches!(processed_files[filename4].link(), Some(link) if !link.is_empty());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&files_map["/noextension"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_folder_without_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            TEST_DATA_FOLDER_NO_SLASH,
            None,
            true,
            true,
        ));

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), TESTDATA_NO_SLASH_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), TESTDATA_NO_SLASH_PUT_FILESMAP_COUNT);

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/testdata/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&files_map["/testdata/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&files_map["/testdata/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&files_map["/testdata/noextension"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_folder_with_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (_, processed_files, files_map) = new_files_container_from_testdata(&safe).await?;

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&files_map["/noextension"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_dst_path_without_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = safe
            .files_container_create_from(
                TEST_DATA_FOLDER_NO_SLASH,
                Some(Path::new("/myroot")),
                true,
                true,
            )
            .await?;

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), TESTDATA_NO_SLASH_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), TESTDATA_NO_SLASH_PUT_FILESMAP_COUNT);

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/myroot/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&files_map["/myroot/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&files_map["/myroot/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&files_map["/myroot/noextension"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_create_from_dst_path_with_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = safe
            .files_container_create_from(
                TEST_DATA_FOLDER_NO_SLASH,
                Some(Path::new("/myroot/")),
                true,
                true,
            )
            .await?;

        assert!(xorurl.starts_with("safe://"));
        assert_eq!(processed_files.len(), TESTDATA_NO_SLASH_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), TESTDATA_NO_SLASH_PUT_FILESMAP_COUNT);

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/myroot/testdata/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&files_map["/myroot/testdata/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&files_map["/myroot/testdata/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&files_map["/myroot/testdata/noextension"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, _) = new_files_container_from_testdata(&safe).await?;

        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let (content, new_processed_files) = safe
            .files_container_sync("./testdata/subfolder/", &xorurl, true, true, false, false)
            .await?;
        let (version, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version, version0);
        assert_eq!(new_processed_files.len(), 2);
        assert_eq!(
            new_files_map.len(),
            TESTDATA_PUT_FILESMAP_COUNT + SUBFOLDER_PUT_FILEITEM_COUNT
        );

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&new_files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&new_files_map["/noextension"][PREDICATE_LINK])
        );

        let filename5 = Path::new("./testdata/subfolder/subexists.md");
        assert!(new_processed_files[filename5].is_added());
        assert_eq!(
            new_processed_files[filename5].link(),
            Some(&new_files_map["/subexists.md"][PREDICATE_LINK])
        );

        let filename6 = Path::new("./testdata/subfolder/sub2.md");
        assert!(new_processed_files[filename6].is_added());
        assert_eq!(
            new_processed_files[filename6].link(),
            Some(&new_files_map["/sub2.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync_dry_run() -> Result<()> {
        let mut safe = new_safe_instance().await?;
        let (xorurl, processed_files, _) = new_files_container_from_testdata(&safe).await?;

        // set dry_run flag on
        safe.dry_run_mode = true;
        let (content, new_processed_files) = safe
            .files_container_sync("./testdata/subfolder/", &xorurl, true, true, false, false)
            .await?;
        let (_, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(new_processed_files.len(), 2);
        assert_eq!(
            new_files_map.len(),
            TESTDATA_PUT_FILESMAP_COUNT + SUBFOLDER_PUT_FILEITEM_COUNT
        );

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&new_files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&new_files_map["/noextension"][PREDICATE_LINK])
        );

        let filename5 = Path::new("./testdata/subfolder/subexists.md");
        assert!(new_processed_files[filename5].is_added());
        assert_matches!(new_processed_files[filename5].link(), Some(link) if !link.is_empty());
        assert_eq!(
            new_processed_files[filename5].link(),
            Some(&new_files_map["/subexists.md"][PREDICATE_LINK])
        );

        let filename6 = Path::new("./testdata/subfolder/sub2.md");
        assert!(new_processed_files[filename6].is_added());
        assert_matches!(new_processed_files[filename6].link(), Some(link) if !link.is_empty());
        assert_eq!(
            new_processed_files[filename6].link(),
            Some(&new_files_map["/sub2.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync_same_size() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = safe
            .files_container_create_from("./testdata/test.md", None, false, false)
            .await?;

        assert_eq!(processed_files.len(), 1);
        assert_eq!(files_map.len(), 1);

        let _ = retry_loop!(safe.fetch(&xorurl, None));

        let (content, new_processed_files) = safe
            .files_container_sync(
                "./testdata/.subhidden/test.md",
                &xorurl,
                false,
                false,
                false,
                false,
            )
            .await?;
        let (_, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), 1);

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&files_map["/test.md"][PREDICATE_LINK])
        );
        let filename2 = Path::new("./testdata/.subhidden/test.md");
        assert!(new_processed_files[filename2].is_updated());
        assert_eq!(
            new_processed_files[filename2].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        // check sizes are the same but links are different
        assert_eq!(
            files_map["/test.md"][PREDICATE_SIZE],
            new_files_map["/test.md"][PREDICATE_SIZE]
        );
        assert_ne!(
            files_map["/test.md"][PREDICATE_LINK],
            new_files_map["/test.md"][PREDICATE_LINK]
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_files_container_sync_with_versioned_target() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) = new_files_container_from_testdata(&safe).await?;

        match safe
            .files_container_sync(
                "./testdata/subfolder/",
                &xorurl,
                false,
                false,
                false,
                // FIXME: shall we just set this to false
                true, // this flag requests the update-nrs
            )
            .await
        {
            Ok(_) => Err(anyhow!("Sync was unexpectedly successful".to_string(),)),
            Err(Error::InvalidInput(msg)) => {
                assert_eq!(
                    msg,
                    format!("The target URL cannot contain a version: {}", xorurl)
                );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_sync_with_delete() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, files_map) = new_files_container_from_testdata(&safe).await?;

        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let (version1_content, new_processed_files) = safe
            .files_container_sync(
                "./testdata/subfolder/",
                &xorurl,
                true,
                false,
                true, // this sets the delete flag
                false,
            )
            .await?;
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version1, version0);
        assert_eq!(
            new_processed_files.len(),
            TESTDATA_PUT_FILESMAP_COUNT + SUBFOLDER_PUT_FILEITEM_COUNT
        );
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);

        // first check all previous files were removed
        let file_path1 = Path::new("/test.md");
        assert!(new_processed_files[file_path1].is_removed());
        assert_eq!(
            new_processed_files[file_path1].link(),
            Some(&files_map[&file_path1.display().to_string()][PREDICATE_LINK])
        );

        let file_path2 = Path::new("/another.md");
        assert!(new_processed_files[file_path2].is_removed());
        assert_eq!(
            new_processed_files[file_path2].link(),
            Some(&files_map[&file_path2.display().to_string()][PREDICATE_LINK])
        );

        let file_path3 = Path::new("/subfolder/subexists.md");
        assert!(new_processed_files[file_path3].is_removed());
        assert_eq!(
            new_processed_files[file_path3].link(),
            Some(&files_map[&file_path3.display().to_string()][PREDICATE_LINK])
        );

        let file_path4 = Path::new("/noextension");
        assert!(new_processed_files[file_path4].is_removed());
        assert_eq!(
            new_processed_files[file_path4].link(),
            Some(&files_map[&file_path4.display().to_string()][PREDICATE_LINK])
        );

        // and finally check the synced file was added
        let filename5 = Path::new("./testdata/subfolder/subexists.md");
        assert!(new_processed_files[filename5].is_added());
        assert_eq!(
            new_processed_files[filename5].link(),
            Some(&new_files_map["/subexists.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync_delete_without_recursive() -> Result<()> {
        let safe = new_safe_instance().await?;
        match safe
            .files_container_sync(
                "./testdata/subfolder/",
                "some-url",
                false, // this sets the recursive flag to off
                false, // do not follow links
                true,  // this sets the delete flag
                false,
            )
            .await
        {
            Ok(_) => Err(anyhow!("Sync was unexpectedly successful".to_string(),)),
            Err(Error::InvalidInput(msg)) => {
                assert_eq!(
                    msg,
                    "'delete' is not allowed if 'recursive' is not set".to_string()
                );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_sync_update_nrs_unversioned_link() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) = new_files_container_from_testdata(&safe).await?;

        let nrsurl = random_nrs_name();
        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(None);
        let unversioned_link = safe_url;
        match safe.nrs_add(&nrsurl, &unversioned_link).await {
            Ok(_) => Err(anyhow!(
                "NRS create was unexpectedly successful".to_string(),
            )),
            Err(Error::UnversionedContentError(msg)) => {
                assert_eq!(
                msg,
                "FilesContainer content is versionable. NRS requires the supplied link to specify \
                a version hash.",
            );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_sync_update_nrs_with_xorurl() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) = new_files_container_from_testdata(&safe).await?;

        match safe
            .files_container_sync(
                "./testdata/subfolder/",
                &xorurl,
                false,
                false,
                false,
                true, // this flag requests the update-nrs
            )
            .await
        {
            Ok(_) => Err(anyhow!("Sync was unexpectedly successful".to_string(),)),
            Err(Error::InvalidInput(msg)) => {
                assert_eq!(
                    msg,
                    "'update-nrs' is not allowed since the URL provided is not an NRS URL"
                        .to_string()
                );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    #[ignore] // TODO: tmp because hang
    async fn test_files_container_sync_update_nrs_versioned_link() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) = new_files_container_from_testdata(&safe).await?;

        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let nrsurl = random_nrs_name();
        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(Some(version0));
        let (nrs_xorurl, did_create) = retry_loop!(safe.nrs_add(&nrsurl, &safe_url));
        assert!(did_create);
        let _ = retry_loop!(safe.fetch(&nrs_xorurl.to_string(), None));

        let (version1_content, _) = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder/",
            &nrsurl,
            false,
            false,
            false,
            true, // this flag requests the update-nrs
        ));
        let (version1, _) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        // wait for it
        retry_loop_for_pattern!(safe
            .files_container_get(&safe_url.to_string()), Ok(Some((version, _))) if *version == version1)?;

        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(Some(version1));
        let new_link = retry_loop!(safe.parse_and_resolve_url(&nrsurl));
        // NRS points to the v0: check if different from v1 url
        assert_ne!(new_link.to_string(), safe_url.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync_target_path_without_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, _) = new_files_container_from_testdata(&safe).await?;

        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_path("path/when/sync");
        let (content, new_processed_files) = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder",
            &safe_url.to_string(),
            true,
            false,
            false,
            false,
        ));
        let (_, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(
            new_processed_files.len(),
            SUBFOLDER_NO_SLASH_PUT_FILEITEM_COUNT
        );
        assert_eq!(
            new_files_map.len(),
            TESTDATA_PUT_FILESMAP_COUNT + SUBFOLDER_NO_SLASH_PUT_FILEITEM_COUNT
        );

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&new_files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&new_files_map["/noextension"][PREDICATE_LINK])
        );

        // and finally check the synced file is there
        let filename5 = Path::new("./testdata/subfolder/subexists.md");
        assert!(new_processed_files[filename5].is_added());
        assert_eq!(
            new_processed_files[filename5].link(),
            Some(&new_files_map["/path/when/sync/subexists.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_sync_target_path_with_trailing_slash() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, _) = new_files_container_from_testdata(&safe).await?;

        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_path("/path/when/sync/");
        let (content, new_processed_files) = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder",
            &safe_url.to_string(),
            true,
            false,
            false,
            false,
        ));
        let (_, new_files_map) =
            content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(
            new_processed_files.len(),
            SUBFOLDER_NO_SLASH_PUT_FILEITEM_COUNT
        );
        assert_eq!(
            new_files_map.len(),
            TESTDATA_PUT_FILESMAP_COUNT + SUBFOLDER_NO_SLASH_PUT_FILEITEM_COUNT
        );

        let filename1 = Path::new("./testdata/test.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/test.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/another.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/another.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename3].is_added());
        assert_eq!(
            processed_files[filename3].link(),
            Some(&new_files_map["/subfolder/subexists.md"][PREDICATE_LINK])
        );

        let filename4 = Path::new("./testdata/noextension");
        assert!(processed_files[filename4].is_added());
        assert_eq!(
            processed_files[filename4].link(),
            Some(&new_files_map["/noextension"][PREDICATE_LINK])
        );

        // and finally check the synced file is there
        let filename5 = Path::new("./testdata/subfolder/subexists.md");
        assert!(new_processed_files[filename5].is_added());
        assert_eq!(
            new_processed_files[filename5].link(),
            Some(&new_files_map["/path/when/sync/subfolder/subexists.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_get() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, files_map) = new_files_container_from_testdata(&safe).await?;

        let (_, fetched_files_map) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(fetched_files_map.len(), TESTDATA_PUT_FILESMAP_COUNT);
        assert_eq!(files_map.len(), fetched_files_map.len());
        assert_eq!(files_map["/test.md"], fetched_files_map["/test.md"]);
        assert_eq!(files_map["/another.md"], fetched_files_map["/another.md"]);
        assert_eq!(
            files_map["/subfolder/subexists.md"],
            fetched_files_map["/subfolder/subexists.md"]
        );
        assert_eq!(files_map["/noextension"], fetched_files_map["/noextension"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_version() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) = new_files_container_from_testdata(&safe).await?;

        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let (version1_content, _) = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder/",
            &xorurl,
            true,
            false,
            true, // this sets the delete flag,
            false,
        ));
        let (version1, _) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version1, version0);

        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(None);
        let (version, _) = retry_loop_for_pattern!(safe
            .files_container_get(&safe_url.to_string()), Ok(Some((version, _))) if *version == version1)?.ok_or(anyhow!("files container was unexpectedly empty"))?;
        assert_eq!(version, version1);

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_get_with_version() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, files_map) = new_files_container_from_testdata(&safe).await?;

        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        // let's create a new version of the files container
        let (version1_content, _) = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder/",
            &xorurl,
            true,
            false,
            true, // this sets the delete flag
            false,
        ));
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        // let's fetch version 0
        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(Some(version0));
        let (version, v0_files_map) = retry_loop!(safe.files_container_get(&safe_url.to_string()))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(version, version0);
        assert_eq!(files_map, v0_files_map);
        // let's check that one of the files in v1 is still there
        let file_path1 = Path::new("/test.md");
        assert_eq!(
            files_map[&file_path1.display().to_string()][PREDICATE_LINK],
            v0_files_map[&file_path1.display().to_string()][PREDICATE_LINK]
        );

        // let's fetch version1
        safe_url.set_content_version(Some(version1));
        let (version, v1_files_map) = retry_loop_for_pattern!(safe
                .files_container_get(&safe_url.to_string()), Ok(Some((version, _))) if *version == version1)?.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(version, version1);
        assert_eq!(new_files_map, v1_files_map);
        // let's check that some of the files are no in v2 anymore
        let file_path2 = Path::new("/another.md");
        let file_path3 = Path::new("/subfolder/subexists.md");
        let file_path4 = Path::new("/noextension");
        assert!(v1_files_map
            .get(&file_path1.display().to_string())
            .is_none());
        assert!(v1_files_map
            .get(&file_path2.display().to_string())
            .is_none());
        assert!(v1_files_map
            .get(&file_path3.display().to_string())
            .is_none());
        assert!(v1_files_map
            .get(&file_path4.display().to_string())
            .is_none());

        // let's fetch invalid version
        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let version_hash = VersionHash::from(&random_hash);
        safe_url.set_content_version(Some(version_hash));
        match safe.files_container_get(&safe_url.to_string()).await {
            Ok(_) => Err(anyhow!(
                "Unexpectedly retrieved invalid version of container".to_string(),
            )),
            Err(Error::VersionNotFound(msg)) => {
                assert_eq!(
                    msg,
                    format!(
                        "Version '{}' is invalid for FilesContainer found at \"{}\"",
                        version_hash, safe_url
                    )
                );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_create_from_get_empty_folder() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, files_map) = new_files_container_from_testdata(&safe).await?;

        let (_, files_map_get) = retry_loop!(safe.files_container_get(&xorurl.to_string()))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(files_map, files_map_get);
        assert_eq!(files_map_get["/emptyfolder"], files_map["/emptyfolder"]);
        assert_eq!(
            files_map_get["/emptyfolder"]["type"],
            MIMETYPE_FILESYSTEM_DIR
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore = "fix unknown issue"]
    async fn test_files_container_sync_with_nrs_url() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, _) =
            retry_loop!(safe.files_container_create_from("./testdata/test.md", None, false, true,));
        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let nrsurl = random_nrs_name();
        let mut safe_url = SafeUrl::from_url(&xorurl)?;
        safe_url.set_content_version(Some(version0));
        let (nrs_xorurl, did_create) = retry_loop!(safe.nrs_add(&nrsurl, &safe_url));

        assert!(did_create);
        let _ = retry_loop!(safe.fetch(&nrs_xorurl.to_string(), None));

        let _ = retry_loop!(safe.files_container_sync(
            "./testdata/subfolder/",
            &xorurl,
            false,
            false,
            false,
            false,
        ));

        let (version2_content, _) = retry_loop!(safe.files_container_sync(
            TEST_DATA_FOLDER,
            &nrsurl,
            false,
            false,
            false,
            true, // this flag requests the update-nrs
        ));
        let (version2, _) =
            version2_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        // now it should look like:
        // safe://<nrs>
        // ├── .hidden.txt
        // ├── another.md
        // ├── noextension
        // ├── sub2.md
        // ├── subexists.md
        // └── test.md
        //
        // So, we have 6 items.
        let (_, fetched_files_map) = retry_loop_for_pattern!(safe.files_container_get(&xorurl), Ok(Some((version, _))) if *version == version2)?.ok_or(anyhow!("files container was unexpectedly empty"))?;
        assert_eq!(fetched_files_map.len(), 6);

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_add() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path("/new_filename_test.md");

        let (version1_content, new_processed_files) = retry_loop!(safe.files_container_add(
            "./testdata/test.md",
            &url_with_path.to_string(),
            false,
            false,
            false,
        ));
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version1, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);

        let filename1 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/subexists.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/subfolder/sub2.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/sub2.md"][PREDICATE_LINK])
        );

        let filename3 = Path::new("./testdata/test.md");
        assert!(new_processed_files[filename3].is_added());
        assert_eq!(
            new_processed_files[filename3].link(),
            Some(&new_files_map["/new_filename_test.md"][PREDICATE_LINK])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_add_dry_run() -> Result<()> {
        let mut safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path("/new_filename_test.md");

        safe.dry_run_mode = true;
        let (version1_content, new_processed_files) = retry_loop!(safe.files_container_add(
            "./testdata/test.md",
            &url_with_path.to_string(),
            false,
            false,
            false,
        ));
        let (_, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        // skip version hash check since not NotImplemented
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);

        // a dry run again should give the exact same results
        let (version2_content, new_processed_files2) = retry_loop!(safe.files_container_add(
            "./testdata/test.md",
            &url_with_path.to_string(),
            false,
            false,
            false,
        ));
        let (_, new_files_map2) =
            version2_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(new_processed_files.len(), new_processed_files2.len());
        assert_eq!(new_files_map.len(), new_files_map2.len());

        let filename = Path::new("./testdata/test.md");
        assert!(new_processed_files[filename].is_added());
        assert!(new_processed_files2[filename].is_added());
        assert_eq!(
            new_processed_files[filename].link(),
            Some(&new_files_map["/new_filename_test.md"][PREDICATE_LINK])
        );
        assert_eq!(
            new_processed_files2[filename].link(),
            Some(&new_files_map2["/new_filename_test.md"][PREDICATE_LINK])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_add_dir() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT); // root "/" + 2 files
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));

        match safe
            .files_container_add(TEST_DATA_FOLDER_NO_SLASH, &xorurl, false, false, false)
            .await
        {
            Ok(_) => Err(anyhow!(
                "Unexpectedly added a folder to files container".to_string(),
            )),
            Err(Error::InvalidInput(msg)) => {
                assert_eq!(
                    msg,
                    "'./testdata' is a directory, only individual files can be added. Use files sync operation for uploading folders".to_string(),
                );
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_add_existing_name() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);

        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path("/sub2.md");

        // let's try to add a file with same target name and same content, it should fail
        let filename1 = Path::new("./testdata/subfolder/sub2.md");
        let (version1_content, new_processed_files) = safe
            .files_container_add(
                &filename1.display().to_string(),
                &url_with_path.to_string(),
                false,
                false,
                false,
            )
            .await?;
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(version1, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_matches!(
            &new_processed_files[filename1],
            FilesMapChange::Failed(msg) if msg == &format!("{}", Error::FileAlreadyExists("/sub2.md".to_string()))
        );
        assert_eq!(files_map, new_files_map);

        // let's try to add a file with same target name but with different content, it should still fail
        let filename2 = Path::new("./testdata/test.md");
        let (version2_content, new_processed_files) = retry_loop!(safe.files_container_add(
            &filename2.display().to_string(),
            &url_with_path.to_string(),
            false,
            false,
            false,
        ));
        let (version2, new_files_map) =
            version2_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_eq!(version2, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_matches!(
            &new_processed_files[filename2],
            FilesMapChange::Failed(msg) if msg == &format!("{}", Error::FileNameConflict("/sub2.md".to_string()))
        );
        assert_eq!(files_map, new_files_map);

        // let's now force it
        let (version3_content, new_processed_files) = retry_loop!(safe.files_container_add(
            &filename2.display().to_string(),
            &url_with_path.to_string(),
            true, //force it
            false,
            false,
        ));
        let (version3, new_files_map) =
            version3_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version3, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert!(new_processed_files[filename2].is_updated());
        assert_eq!(
            new_processed_files[filename2].link(),
            Some(&new_files_map["/sub2.md"]["link"])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_fail_add_or_sync_invalid_path() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) =
            retry_loop!(safe.files_container_create_from("./testdata/test.md", None, false, true,));
        assert_eq!(processed_files.len(), 1);
        assert_eq!(files_map.len(), 1);
        let _ = retry_loop!(safe.fetch(&xorurl, None));

        match safe
            .files_container_sync("/non-existing-path", &xorurl, false, false, false, false)
            .await
        {
            Ok(_) => {
                bail!("Unexpectedly added a folder to files container".to_string(),)
            }
            Err(Error::FileSystemError(msg)) => {
                assert!(msg
                    .starts_with("Couldn't read metadata from source path ('/non-existing-path')"))
            }
            other => {
                bail!("Error returned is not the expected one: {:?}", other)
            }
        }

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path("/test.md");

        match safe
            .files_container_add(
                "/non-existing-path",
                &url_with_path.to_string(),
                false,
                false,
                false,
            )
            .await
        {
            Ok(_) => Err(anyhow!(
                "Unexpectedly added a folder to files container".to_string(),
            )),
            Err(Error::FileSystemError(msg)) => {
                assert!(msg
                    .starts_with("Couldn't read metadata from source path ('/non-existing-path')"));
                Ok(())
            }
            other => Err(anyhow!(
                "Error returned is not the expected one: {:?}",
                other
            )),
        }
    }

    #[tokio::test]
    async fn test_files_container_add_a_url() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let data = Bytes::from("0123456789");
        let file_xorurl = retry_loop!(safe.store_public_bytes(data.clone(), None));
        let new_filename = Path::new("/new_filename_test.md");

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path(&new_filename.display().to_string());

        let (version1_content, new_processed_files) = retry_loop!(safe.files_container_add(
            &file_xorurl,
            &url_with_path.to_string(),
            false,
            false,
            false,
        ));
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version1, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);

        let filename1 = Path::new("./testdata/subfolder/subexists.md");
        assert!(processed_files[filename1].is_added());
        assert_eq!(
            processed_files[filename1].link(),
            Some(&new_files_map["/subexists.md"][PREDICATE_LINK])
        );

        let filename2 = Path::new("./testdata/subfolder/sub2.md");
        assert!(processed_files[filename2].is_added());
        assert_eq!(
            processed_files[filename2].link(),
            Some(&new_files_map["/sub2.md"][PREDICATE_LINK])
        );

        assert!(new_processed_files[new_filename].is_added());
        assert_eq!(
            new_processed_files[new_filename].link(),
            Some(&new_files_map[&new_filename.display().to_string()][PREDICATE_LINK])
        );
        assert_eq!(
            new_files_map[&new_filename.display().to_string()][PREDICATE_LINK],
            file_xorurl
        );

        // let's add another file but with the same name
        let data = Bytes::from("9876543210");
        let other_file_xorurl = retry_loop!(safe.store_public_bytes(data.clone(), None));
        let (version2_content, new_processed_files) = retry_loop!(safe.files_container_add(
            &other_file_xorurl,
            &url_with_path.to_string(),
            true, // force to overwrite it with new link
            false,
            false,
        ));
        let (version2, new_files_map) =
            version2_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version2, version0);
        assert_ne!(version2, version1);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);
        assert!(new_processed_files[new_filename].is_updated());
        assert_eq!(
            new_processed_files[new_filename].link(),
            Some(&new_files_map[&new_filename.display().to_string()][PREDICATE_LINK])
        );
        assert_eq!(
            new_files_map[&new_filename.display().to_string()][PREDICATE_LINK],
            other_file_xorurl
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_add_from_raw() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, processed_files, files_map) = retry_loop!(safe.files_container_create_from(
            "./testdata/subfolder/",
            None,
            false,
            true,
        ));
        assert_eq!(processed_files.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        assert_eq!(files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT);
        let _ = retry_loop!(safe.fetch(&xorurl, None));
        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let data = Bytes::from("0123456789");
        let new_filename = Path::new("/new_filename_test.md");

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path(&new_filename.display().to_string());

        let (version1_content, new_processed_files) = retry_loop!(safe
            .files_container_add_from_raw(data.clone(), &url_with_path.to_string(), false, false,));
        let (version1, new_files_map) =
            version1_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version1, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);

        assert!(new_processed_files[new_filename].is_added());
        assert_eq!(
            new_processed_files[new_filename].link(),
            Some(&new_files_map[&new_filename.display().to_string()][PREDICATE_LINK])
        );

        // let's add another file but with the same name
        let data = Bytes::from("9876543210");
        let (version2_content, new_processed_files) = retry_loop!(safe
            .files_container_add_from_raw(
                data.clone(),
                &url_with_path.to_string(),
                true, // force to overwrite it with new link
                false,
            ));
        let (version2, new_files_map) =
            version2_content.ok_or(anyhow!("files container was unexpectedly empty"))?;

        assert_ne!(version2, version0);
        assert_ne!(version2, version1);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), SUBFOLDER_PUT_FILEITEM_COUNT + 1);
        assert!(new_processed_files[new_filename].is_updated());
        assert_eq!(
            new_processed_files[new_filename].link(),
            Some(&new_files_map[&new_filename.display().to_string()][PREDICATE_LINK])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_files_container_remove_path() -> Result<()> {
        let safe = new_safe_instance().await?;
        let (xorurl, _, files_map) = new_files_container_from_testdata(&safe).await?;

        let (version0, _) = retry_loop!(safe.files_container_get(&xorurl))
            .ok_or(anyhow!("files container was unexpectedly empty"))?;

        let mut url_with_path = SafeUrl::from_xorurl(&xorurl)?;
        url_with_path.set_path("/test.md");

        // let's remove a file first
        let (version1, new_processed_files, new_files_map) =
            retry_loop!(safe.files_container_remove_path(&url_with_path.to_string(), false, false));

        assert_ne!(version1, version0);
        assert_eq!(new_processed_files.len(), 1);
        assert_eq!(new_files_map.len(), TESTDATA_PUT_FILESMAP_COUNT - 1);

        let filepath = Path::new("/test.md");
        assert!(new_processed_files[filepath].is_removed());
        assert_eq!(
            new_processed_files[filepath].link(),
            Some(&files_map[&filepath.display().to_string()][PREDICATE_LINK])
        );

        // let's remove an entire folder now with recursive flag
        url_with_path.set_path("/subfolder");
        let (version2, new_processed_files, new_files_map) =
            retry_loop!(safe.files_container_remove_path(&url_with_path.to_string(), true, false));

        assert_ne!(version2, version0);
        assert_ne!(version2, version1);
        assert_eq!(new_processed_files.len(), 2);
        assert_eq!(
            new_files_map.len(),
            TESTDATA_PUT_FILESMAP_COUNT - SUBFOLDER_PUT_FILEITEM_COUNT - 1
        );

        let filename1 = Path::new("/subfolder/subexists.md");
        assert!(new_processed_files[filename1].is_removed());
        assert_eq!(
            new_processed_files[filename1].link(),
            Some(&files_map[&filename1.display().to_string()][PREDICATE_LINK])
        );

        let filename2 = Path::new("/subfolder/sub2.md");
        assert!(new_processed_files[filename2].is_removed());
        assert_eq!(
            new_processed_files[filename2].link(),
            Some(&files_map[&filename2.display().to_string()][PREDICATE_LINK])
        );

        Ok(())
    }
}
