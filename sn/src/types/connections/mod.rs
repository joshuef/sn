// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod link;

pub(crate) use link::{Link, SendToOneError};

use qp2p::Endpoint;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::RwLock;
use xor_name::XorName;

type PeerId = (XorName, SocketAddr);

/// This is tailored to the use-case of connecting on send.
/// It keeps a Link instance per node, and is designed to make sure
/// underlying I/O connection resources are not leaked, overused or left dangling.
#[derive(Clone, Debug)]
pub(crate) struct PeerLinks {
    links: Arc<RwLock<BTreeMap<PeerId, Link>>>,
    endpoint: Endpoint,
}

impl PeerLinks {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            links: Arc::new(RwLock::new(BTreeMap::new())),
            endpoint,
        }
    }
    pub(crate) async fn linked_peers(&self) -> BTreeSet<PeerId> {
        let links = self.links.read().await;

        links.keys().into_iter().cloned().collect()
    }

    /// Any number of incoming qp2p:Connections can be added.
    /// We will eventually converge to the same one in our comms with the peer.
    pub(crate) async fn add_incoming(&self, id: &PeerId, conn: qp2p::Connection) {
        {
            let link = self.links.read().await;
            if let Some(c) = link.get(id) {
                // peer id exists, add to it
                c.add(conn).await;
                return;
            }
            // else still not in list, go ahead and insert
        }

        let mut links = self.links.write().await;
        match links.get(id) {
            // someone else inserted in the meanwhile, add to it
            Some(c) => c.add(conn).await,
            // still not in list, go ahead and insert
            None => {
                let link = Link::new_with(*id, self.endpoint.clone(), conn).await;
                let _ = links.insert(*id, link);
            }
        }
    }

    pub(crate) async fn peer_is_connected(&self, id: &PeerId) -> bool {
        let link = self.links.read().await;
        if let Some(c) = link.get(id) {
            // peer id exists, check if connected
            return c.is_connected().await;
        }

        false
    }

    /// This method is tailored to the use-case of connecting on send.
    /// I.e. it will not connect here, but on calling send on the returned link.
    pub(crate) async fn get_or_create(&self, id: &PeerId) -> Link {
        if let Some(link) = self.get(id).await {
            return link;
        }

        // if id is not in list, the entire list needs to be locked
        // i.e. first comms to any node, will impact all sending at that instant..
        // however, first comms should be a minor part of total time spent using link,
        // so that is ok
        let mut links = self.links.write().await;
        match links.get(id).cloned() {
            // someone else inserted in the meanwhile, so use that
            Some(link) => link,
            // still not in list, go ahead and create + insert
            None => {
                let link = Link::new(*id, self.endpoint.clone());
                let _ = links.insert(*id, link.clone());
                link
            }
        }
    }

    /// Disposes of the link and all underlying resources.
    pub(crate) async fn disconnect(&self, id: PeerId) {
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

    async fn get(&self, id: &PeerId) -> Option<Link> {
        let links = self.links.read().await;
        links.get(id).cloned()
    }
}
