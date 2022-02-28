// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.


use multibase::{self};




use sn_interface::network_knowledge::prefix_map::NetworkPrefixMap;
use sn_interface::types::{Error, Result};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn compare_and_write_prefix_map_to_disk(
    prefix_map: &NetworkPrefixMap,
) -> Result<()> {
    // Open or create `$User/.safe/prefix_maps` dir
    let prefix_map_dir = dirs_next::home_dir()
        .ok_or_else(|| Error::DirectoryHandling("Could not read '.safe' directory".to_string()))?
        .join(".safe")
        .join("prefix_maps");

    tokio::fs::create_dir_all(prefix_map_dir.clone())
        .await
        .map_err(|_| {
            Error::DirectoryHandling("Could not read '.safe/prefix_maps' directory".to_string())
        })?;

    let prefix_map_file = prefix_map_dir.join(format!("{:?}", prefix_map.genesis_key()));

    // Check if the prefixMap is already present and is latest to the provided Map.
    let disk_map = read_prefix_map_from_disk(&prefix_map_file).await.ok();

    if let Some(old_map) = disk_map {
        // Return early as the PrefixMap in disk is the equivalent/latest already
        if &old_map >= prefix_map {
            info!("Equivalent/Latest PrefixMap already in disk");
            return Ok(());
        }
    }

    trace!("Writing prefix_map to disk at {:?}", prefix_map_file);
    let serialized =
        rmp_serde::to_vec(prefix_map).map_err(|e| Error::Serialisation(e.to_string()))?;

    let mut file = File::create(prefix_map_file)
        .await
        .map_err(|e| Error::FileHandling(e.to_string()))?;

    let _ = file
        .write_all(&serialized)
        .await
        .map_err(|e| Error::FileHandling(e.to_string()))?;

    file.sync_all()
        .await
        .map_err(|e| Error::FileHandling(e.to_string()))?;

    Ok(())
}

pub(crate) async fn read_prefix_map_from_disk(path: &Path) -> Result<NetworkPrefixMap> {
    // Read NetworkPrefixMap from disk if present else create a new one
    match File::open(path).await {
        Ok(mut prefix_map_file) => {
            let mut prefix_map_contents = vec![];
            let _ = prefix_map_file
                .read_to_end(&mut prefix_map_contents)
                .await
                .map_err(|err| {
                    Error::FileHandling(format!(
                        "Error reading PrefixMap from {:?}: {:?}",
                        path, err
                    ))
                })?;

            let prefix_map: NetworkPrefixMap = rmp_serde::from_slice(&prefix_map_contents)
                .map_err(|err| {
                    Error::FileHandling(format!(
                        "Error deserializing PrefixMap from disk: {:?}",
                        err
                    ))
                })?;
            Ok(prefix_map)
        }
        Err(e) => Err(Error::FailedToParse(e.to_string())),
    }
}
