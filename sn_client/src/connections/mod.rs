// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod listeners;
mod messaging;

use crate::Result;
use sn_interface::{
    messaging::data::QueryResponse, network_knowledge::SectionTree, types::PeerLinks,
};

use qp2p::{Config as QuicP2pConfig, Endpoint};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct QueryResult {
    pub response: QueryResponse,
}

impl QueryResult {
    /// Returns true if the QueryResponse is DataNotFound
    pub(crate) fn data_was_found(&self) -> bool {
        let found = !self.response.is_data_not_found();

        debug!("was the data found??? {found:?}, {self:?}");

        found
    }
}

#[derive(Clone, Debug)]
pub(super) struct Session {
    // Session endpoint.
    endpoint: Endpoint,
    /// All elders we know about from AE messages
    pub(super) network: Arc<RwLock<SectionTree>>,
    /// Links to nodes
    peer_links: PeerLinks,
}

impl Session {
    /// Acquire a session by bootstrapping to a section, maintaining connections to several nodes.
    #[instrument(level = "debug")]
    pub(crate) fn new(
        qp2p_config: QuicP2pConfig,
        local_addr: SocketAddr,
        network_contacts: SectionTree,
    ) -> Result<Self> {
        let endpoint = Endpoint::new_client(local_addr, qp2p_config)?;
        let peer_links = PeerLinks::new(endpoint.clone());

        let session = Self {
            endpoint,
            network: Arc::new(RwLock::new(network_contacts)),
            peer_links,
        };

        Ok(session)
    }
}
