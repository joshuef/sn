// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod api;
mod back_pressure;
mod bootstrap;
mod capacity;
mod chunk_records;
mod chunk_store;
mod comm;
mod connectivity;
mod delivery_group;
mod liveness_tracking;
mod messaging;
mod msg_count;
mod msg_handling;
mod register_storage;
mod split_barrier;

pub(crate) use back_pressure::BackPressure;
pub(crate) use bootstrap::{join_network, JoiningAsRelocated};
pub(crate) use capacity::{CHUNK_COPY_COUNT, MIN_LEVEL_WHEN_FULL};
pub(crate) use chunk_store::ChunkStore;
pub(crate) use comm::{Comm, ConnectionEvent, SendStatus};
pub(crate) use register_storage::RegisterStorage;

use self::split_barrier::SplitBarrier;
use crate::dbs::UsedSpace;
use crate::messaging::system::SystemMsg;
use crate::messaging::{
    signature_aggregator::SignatureAggregator,
    system::{Proposal, Section},
};
use crate::prefix_map::NetworkPrefixMap;
use crate::routing::{
    dkg::{DkgVoter, ProposalAggregator},
    error::Result,
    log_markers::LogMarker,
    node::Node,
    relocation::RelocateState,
    routing_api::command::Command,
    section::{SectionKeyShare, SectionKeysProvider},
    Elders, Event, NodeElderChange, SectionAuthorityProviderUtils,
};
use capacity::Capacity;
use itertools::Itertools;
use liveness_tracking::Liveness;
use resource_proof::ResourceProof;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{mpsc, RwLock};
use xor_name::{Prefix, XorName};

pub(super) const RESOURCE_PROOF_DATA_SIZE: usize = 64;
pub(super) const RESOURCE_PROOF_DIFFICULTY: u8 = 2;
const KEY_CACHE_SIZE: u8 = 5;

// State + logic of a routing node.
pub(crate) struct Core {
    pub(crate) comm: Comm,
    node: Node,
    section: Section,
    network: NetworkPrefixMap,
    section_keys_provider: SectionKeysProvider,
    message_aggregator: SignatureAggregator,
    proposal_aggregator: ProposalAggregator,
    split_barrier: Arc<RwLock<SplitBarrier>>,
    // Voter for Dkg
    dkg_voter: DkgVoter,
    relocate_state: Option<RelocateState>,
    pub(super) event_tx: mpsc::Sender<Event>,
    joins_allowed: Arc<RwLock<bool>>,
    resource_proof: ResourceProof,
    used_space: UsedSpace,
    pub(super) register_storage: RegisterStorage,
    pub(super) chunk_storage: ChunkStore,
    root_storage_dir: PathBuf,
    capacity: Capacity,
    liveness: Liveness,
}

impl Core {
    // Creates `Core` for a regular node.
    pub(crate) async fn new(
        comm: Comm,
        mut node: Node,
        section: Section,
        section_key_share: Option<SectionKeyShare>,
        event_tx: mpsc::Sender<Event>,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
    ) -> Result<Self> {
        let section_keys_provider =
            SectionKeysProvider::new(KEY_CACHE_SIZE, section_key_share).await;

        // make sure the Node has the correct local addr as Comm
        node.addr = comm.our_connection_info();

        let register_storage = RegisterStorage::new(&root_storage_dir, used_space.clone())?;
        let chunk_storage = ChunkStore::new(&root_storage_dir, used_space.clone())?;

        let capacity = Capacity::new(BTreeMap::new());
        let adult_liveness = Liveness::new();
        let genesis_pk = *section.genesis_key();

        Ok(Self {
            comm,
            node,
            section,
            network: NetworkPrefixMap::new(genesis_pk),
            section_keys_provider,
            proposal_aggregator: ProposalAggregator::default(),
            split_barrier: Arc::new(RwLock::new(SplitBarrier::new())),
            message_aggregator: SignatureAggregator::default(),
            dkg_voter: DkgVoter::default(),
            relocate_state: None,
            event_tx,
            joins_allowed: Arc::new(RwLock::new(true)),
            resource_proof: ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY),
            register_storage,
            chunk_storage,
            capacity,
            liveness: adult_liveness,
            root_storage_dir,
            used_space,
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // Miscellaneous
    ////////////////////////////////////////////////////////////////////////////

    pub(crate) fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            is_elder: self.is_elder(),
            last_key: *self.section.chain().last_key(),
            prefix: *self.section.prefix(),
            elders: self.section().authority_provider().names(),
        }
    }

    pub(crate) fn generate_probe_message(&self) -> Result<Command> {
        // Generate a random address not belonging to our Prefix
        let mut dst = XorName::random();

        // We don't probe ourselves
        while self.section.prefix().matches(&dst) {
            dst = XorName::random();
        }

        let matching_section = self.network.section_by_name(&dst)?;

        let message = SystemMsg::AntiEntropyProbe(dst);
        let section_key = matching_section.section_key();
        let dst_name = matching_section.prefix().name();
        let recipients = matching_section.elders.into_iter().collect::<Vec<_>>();

        info!(
            "ProbeMessage target {:?} w/key {:?}",
            matching_section.prefix, section_key
        );

        self.send_direct_message_to_nodes(recipients, message, dst_name, section_key)
    }

    /// Generate commands and fire events based upon any node state changes.
    pub(crate) async fn update_for_new_node_state_and_fire_events(
        &mut self,
        old: StateSnapshot,
    ) -> Result<Vec<Command>> {
        let mut commands = vec![];
        let new = self.state_snapshot();

        self.section_keys_provider
            .finalise_dkg(self.section.chain().last_key())
            .await;

        if new.prefix != old.prefix {
            info!("Split");
        }

        if new.last_key != old.last_key {
            if new.is_elder {
                info!(
                    "Section updated: prefix: ({:b}), key: {:?}, elders: {}",
                    new.prefix,
                    new.last_key,
                    self.section.authority_provider().peers().format(", ")
                );

                if self.section_keys_provider.has_key_share().await {
                    commands.extend(self.promote_and_demote_elders().await?);

                    // Whenever there is an elders change, casting a round of joins_allowed
                    // proposals to sync.
                    commands.extend(
                        self.propose(Proposal::JoinsAllowed(*self.joins_allowed.read().await))
                            .await?,
                    );
                }

                self.print_network_stats();
            }

            if new.is_elder || old.is_elder {
                commands.extend(self.send_ae_update_to_our_section(&self.section)?);
            }

            let current: BTreeSet<_> = self.section.authority_provider().names();
            let added = current.difference(&old.elders).copied().collect();
            let removed = old.elders.difference(&current).copied().collect();
            let remaining = old.elders.intersection(&current).copied().collect();

            let elders = Elders {
                prefix: new.prefix,
                key: new.last_key,
                remaining,
                added,
                removed,
            };

            let self_status_change = if !old.is_elder && new.is_elder {
                trace!("{}", LogMarker::PromotedToElder);
                NodeElderChange::Promoted
            } else if old.is_elder && !new.is_elder {
                trace!("{}", LogMarker::DemotedFromElder);
                info!("Demoted");
                self.network = NetworkPrefixMap::new(*self.section.genesis_key());
                self.section_keys_provider = SectionKeysProvider::new(KEY_CACHE_SIZE, None).await;
                NodeElderChange::Demoted
            } else {
                NodeElderChange::None
            };

            let sibling_elders = if new.prefix != old.prefix {
                self.network.get(&new.prefix.sibling()).map(|sec_auth| {
                    let current: BTreeSet<_> = sec_auth.names();
                    let added = current.difference(&old.elders).copied().collect();
                    let removed = old.elders.difference(&current).copied().collect();
                    let remaining = old.elders.intersection(&current).copied().collect();
                    Elders {
                        prefix: new.prefix.sibling(),
                        key: sec_auth.section_key(),
                        remaining,
                        added,
                        removed,
                    }
                })
            } else {
                None
            };

            let event = if let Some(sibling_elders) = sibling_elders {
                Event::SectionSplit {
                    elders,
                    sibling_elders,
                    self_status_change,
                }
            } else {
                Event::EldersChanged {
                    elders,
                    self_status_change,
                }
            };

            self.send_event(event).await;
        }

        if !new.is_elder {
            commands.extend(self.return_relocate_promise());
        }

        Ok(commands)
    }

    pub(crate) fn section_key_by_name(&self, name: &XorName) -> bls::PublicKey {
        if self.section.prefix().matches(name) {
            *self.section.chain().last_key()
        } else if let Ok(sap) = self.network.section_by_name(name) {
            sap.section_key()
        } else if self.section.prefix().sibling().matches(name) {
            // For sibling with unknown key, use the previous key in our chain under the assumption
            // that it's the last key before the split and therefore the last key of theirs we know.
            // In case this assumption is not correct (because we already progressed more than one
            // key since the split) then this key would be unknown to them and they would send
            // us back their whole section chain. However, this situation should be rare.
            *self.section.chain().prev_key()
        } else {
            *self.section.chain().root_key()
        }
    }

    pub(crate) fn print_network_stats(&self) {
        self.network
            .network_stats(self.section.authority_provider())
            .print();
        self.comm.print_stats();
    }
}

pub(crate) struct StateSnapshot {
    is_elder: bool,
    last_key: bls::PublicKey,
    prefix: Prefix,
    elders: BTreeSet<XorName>,
}
