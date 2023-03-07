// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod link;

pub(crate) use link::Link;
pub use link::LinkError;

use sn_interface::{messaging::MsgId, types::NodeId};

use qp2p::Endpoint;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc, time::Instant};
use tokio::sync::RwLock;

/// This is tailored to the use-case of connecting on send.
/// It keeps a Link instance per node, and it's designed to make sure
/// underlying I/O connection resources are not leaked, overused or left dangling.
#[derive(Clone, Debug)]
pub(super) struct NodeLinks {
    links: Arc<RwLock<BTreeMap<NodeId, Link>>>,
    endpoint: Endpoint,
}

impl NodeLinks {
    pub(super) fn new(endpoint: Endpoint) -> Self {
        Self {
            links: Arc::new(RwLock::new(BTreeMap::new())),
            endpoint,
        }
    }

    /// This method is tailored to the use-case of connecting on send.
    /// I.e. it will not connect here, but on calling send on the returned link.
    pub(super) async fn get_or_create_link(
        &self,
        node_id: &NodeId,
        connect_now: bool,
        msg_id: Option<MsgId>,
    ) -> Link {
        let start = Instant::now();
        if let Some(link) = self.get(node_id).await {
            if connect_now {
                if let Err(error) = link.create_connection_if_none_exist(msg_id).await {
                    error!(
                        "Error during create connection attempt for link to {node_id:?}: {error:?}"
                    );
                }
            }
            return link;
        }

        // if node is not in list, the entire list needs to be locked
        // i.e. first comms to any node, will impact all sending at that instant..
        // however, first comms should be a minor part of total time spent using link,
        // so that is ok
        let links = self.links.read().await;
        let mut insert_new_link = false;
        let a_link = match links.get(node_id).cloned() {
            // someone else inserted in the meanwhile, so use that
            Some(link) => return link,
            // still not in list, go ahead and create + insert
            None => {
                let link = Link::new(*node_id, self.endpoint.clone());
                if connect_now {
                    if let Err(error) = link.create_connection_if_none_exist(msg_id).await {
                        error!("Error during create connection attempt for link to {node_id:?}: {error:?}");
                    }
                }
                // let _ = links.insert(*node_id, link.clone());
                // link
                insert_new_link = true;
                link
            }
        };

        drop(links);

        if insert_new_link {
            // final double check
            let mut links = self.links.write().await;
            links.entry(*node_id).or_insert(a_link.clone());
            // .insert(key, value)

            debug!(
                "Time to get link (after write lock) was: {:?}",
                start.elapsed()
            );
            a_link
        } else {
            debug!("Time to get link was: {:?}", start.elapsed());
            a_link
        }
    }

    async fn get(&self, node_id: &NodeId) -> Option<Link> {
        let links = self.links.read().await;
        links.get(node_id).cloned()
    }

    /// Removes a link from NodeLinks.
    /// It does NOT disconnect it, as it could still be used to receveive messages on
    pub(super) async fn remove(&self, node_id: &NodeId) {
        debug!("removing link with {node_id:?}");
        let mut links = self.links.write().await;
        let _existing_link = links.remove(node_id);
    }
}
