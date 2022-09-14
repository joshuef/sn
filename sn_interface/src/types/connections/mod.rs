// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod link;

pub use link::{Link, SendToOneError};

use super::Peer;

use qp2p::Endpoint;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
};
use tokio::sync::RwLock;

/// This is tailored to the use-case of connecting on send.
/// It keeps a Link instance per node, and is designed to make sure
/// underlying I/O connection resources are not leaked, overused or left dangling.
#[derive(Clone, Debug)]
pub struct PeerLinks {
    links: Arc<RwLock<BTreeMap<Peer, Link>>>,
    endpoint: Endpoint,
}

impl PeerLinks {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            links: Arc::new(RwLock::new(BTreeMap::new())),
            endpoint,
        }
    }

    #[allow(unused)]
    pub async fn linked_peers(&self) -> BTreeSet<Peer> {
        let links = self.links.read().await;

        links.keys().into_iter().cloned().collect()
    }

    /// Any number of incoming qp2p:Connections can be added.
    /// We will eventually converge to the same one in our comms with the peer.
    pub async fn add_incoming(&self, peer: &Peer, conn: qp2p::Connection) {
        {
            let link = self.links.read().await;
            if let Some(c) = link.get(peer) {
                // peer exists, add to it
                c.add(conn).await;
                return;
            }
            // else still not in list, go ahead and insert
        }

        let mut links = self.links.write().await;
        match links.get(peer) {
            // someone else inserted in the meanwhile, add to it
            Some(c) => c.add(conn).await,
            // still not in list, go ahead and insert
            None => {
                let link = Link::new_with(*peer, self.endpoint.clone(), conn).await;
                let _ = links.insert(*peer, link);
            }
        }
    }

    #[allow(unused)]
    pub async fn is_connected(&self, peer: &Peer) -> bool {
        let link = self.links.read().await;
        if let Some(c) = link.get(peer) {
            // peer exists, check if connected
            return c.is_connected().await;
        }

        false
    }

    /// This method is tailored to the use-case of connecting on send.
    /// I.e. it will not connect here, but on calling send on the returned link.
    pub async fn get_or_create_link(&self, peer: &Peer, force_new_link: bool) -> Link {
        if force_new_link {
            let link = Link::new(*peer, self.endpoint.clone());
            let _ = self.links.write().await.insert(*peer, link.clone());
            return link;
        }

        if let Some(link) = self.get(peer).await {
            return link;
        }

        // if peer is not in list, the entire list needs to be locked
        // i.e. first comms to any node, will impact all sending at that instant..
        // however, first comms should be a minor part of total time spent using link,
        // so that is ok
        let mut links = self.links.write().await;
        match links.get(peer).cloned() {
            // someone else inserted in the meanwhile, so use that
            Some(link) => link,
            // still not in list, go ahead and create + insert
            None => {
                let link = Link::new(*peer, self.endpoint.clone());
                let _ = links.insert(*peer, link.clone());
                link
            }
        }
    }

    /// Disposes of the link and all underlying resources.
    #[allow(unused)]
    pub async fn disconnect(&self, id: Peer) {
        let mut links = self.links.write().await;
        let link = match links.remove(&id) {
            // someone else inserted in the meanwhile, so use that
            Some(link) => link,
            // none here, all good
            None => {
                trace!("Attempted to remove {id:?}, it was not found");
                return;
            }
        };

        link.disconnect().await;
    }

    async fn get(&self, id: &Peer) -> Option<Link> {
        let links = self.links.read().await;
        links.get(id).cloned()
    }
}
