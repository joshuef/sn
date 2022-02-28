// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod join;
mod relocate;

pub(crate) use join::join_network;
pub(crate) use relocate::JoiningAsRelocated;

#[cfg(not(test))]
use crate::node::Error;
use crate::node::Result;
use sn_interface::network_knowledge::prefix_map::NetworkPrefixMap;

use bls::PublicKey as BlsPublicKey;
use std::{collections::HashSet, net::SocketAddr};
#[cfg(not(test))]
use tokio::{fs::File, io::AsyncReadExt};

type UsedRecipientSaps = HashSet<(SocketAddr, BlsPublicKey)>;

#[cfg(not(test))]
// Reads PrefixMap from '~/.safe/prefix_map' if present.
async fn read_prefix_map_from_disk(genesis_key: BlsPublicKey) -> Result<NetworkPrefixMap> {
    let read_prefix_map = match dirs_next::home_dir() {
        None => None,
        Some(mut prefix_map_dir) => {
            // Read NetworkPrefixMap from disk if present
            prefix_map_dir.push(".safe");
            prefix_map_dir.push("prefix_maps");

            if let Ok(mut prefix_map_file) = File::open(prefix_map_dir.clone()).await {
                let mut prefix_map_contents = vec![];
                match prefix_map_file.read_to_end(&mut prefix_map_contents).await {
                    Ok(_) => rmp_serde::from_slice(&prefix_map_contents)
                        .ok()
                        .map(|map: NetworkPrefixMap| (map, prefix_map_dir)),
                    Err(_) => None,
                }
            } else {
                None
            }
        }
    };

    match read_prefix_map {
        Some((prefix_map, dir)) => {
            info!(
                "Read PrefixMap from disk successfully from {}",
                dir.display()
            );
            if prefix_map.genesis_key() != genesis_key {
                Err(Error::InvalidGenesisKey(prefix_map.genesis_key()))
            } else {
                Ok(prefix_map)
            }
        }
        None => Ok(NetworkPrefixMap::new(genesis_key)),
    }
}

#[cfg(test)]
async fn read_prefix_map_from_disk(genesis_key: BlsPublicKey) -> Result<NetworkPrefixMap> {
    Ok(NetworkPrefixMap::new(genesis_key))
}
