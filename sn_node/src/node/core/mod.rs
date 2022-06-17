// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod api;
mod bootstrap;
mod comm;
mod connectivity;
mod data;
mod delivery_group;
mod messaging;
mod proposal;
mod relocation;
mod split_barrier;

/// DataStorage apis.
pub use self::data::DataStorage;

pub(crate) use bootstrap::{join_network, JoiningAsRelocated};
pub(crate) use comm::{Comm, DeliveryStatus, MsgEvent};
pub(crate) use data::MIN_LEVEL_WHEN_FULL;
pub(crate) use proposal::Proposal;
#[cfg(test)]
pub(crate) use relocation::{check as relocation_check, ChurnId};

use self::{data::Capacity, split_barrier::SplitBarrier};

use super::{
    api::cmds::Cmd, dkg::DkgVoter, handover::Handover, membership::Membership, Elders, Event,
    NodeElderChange,
};

use crate::node::{
    error::{Error, Result},
    membership::elder_candidates,
    membership::try_split_dkg,
};
use crate::UsedSpace;

use sn_dysfunction::{DysfunctionDetection, DysfunctionSeverity, IssueType};
use sn_interface::{
    messaging::{
        data::OperationId,
        signature_aggregator::SignatureAggregator,
        system::{DkgSessionId, NodeState, SystemMsg},
        AuthorityProof, SectionAuth, SectionAuthorityProvider,
    },
    network_knowledge::utils::compare_and_write_prefix_map_to_disk,
    network_knowledge::{
        supermajority, NetworkKnowledge, NodeInfo, SectionKeyShare, SectionKeysProvider,
    },
    types::{keys::ed25519::Digest256, log_markers::LogMarker, Cache, Peer},
};

use backoff::ExponentialBackoff;
use dashmap::DashSet;
use itertools::Itertools;
use resource_proof::ResourceProof;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    rc::Rc,
    time::Duration,
};
use tokio::sync::mpsc;
use uluru::LRUCache;
use xor_name::{Prefix, XorName};

pub(super) const RESOURCE_PROOF_DATA_SIZE: usize = 128;
pub(super) const RESOURCE_PROOF_DIFFICULTY: u8 = 10;

const BACKOFF_CACHE_LIMIT: usize = 100;

// How long to hold on to correlated `Peer`s for data queries. Since data queries are forwarded
// from elders (with whom the client is connected) to adults (who hold the data), the elder handling
// the query cannot reply immediately. For now, they stash a reference to the client `Peer` in
// `Core::pending_data_queries`, which is a cache with duration-based expiry.
// TODO: The value chosen here is shorter than the default client timeout (see
// `use sn_client::SN_CLIENT_QUERY_TIMEOUT`), but the timeout is configurable. Ideally this would be
// based on liveness properties (e.g. the timeout should be dynamic based on the responsiveness of
// the section).
const DATA_QUERY_TIMEOUT: Duration = Duration::from_secs(15);

// This prevents pending query limit unbound growth
pub(crate) const DATA_QUERY_LIMIT: usize = 100;
// per query we can have this many peers, so the total peers waiting can be QUERY_LIMIT * MAX_WAITING_PEERS_PER_QUERY
pub(crate) const MAX_WAITING_PEERS_PER_QUERY: usize = 100;

#[derive(Debug, Clone)]
pub(crate) struct DkgSessionInfo {
    pub(crate) session_id: DkgSessionId,
    pub(crate) authority: AuthorityProof<SectionAuth>,
}

// Store up to 100 in use backoffs
pub(crate) type AeBackoffCache =
    Rc<RefCell<LRUCache<(Peer, ExponentialBackoff), BACKOFF_CACHE_LIMIT>>>;

// Core state + logic of a node.
pub(crate) struct Node {
    pub(super) event_tx: mpsc::Sender<Event>,
    pub(crate) info: Rc<RefCell<NodeInfo>>,

    pub(crate) comm: Comm,

    pub(super) data_storage: DataStorage, // Adult only before cache

    resource_proof: ResourceProof,
    // Network resources
    pub(crate) section_keys_provider: SectionKeysProvider,
    network_knowledge: NetworkKnowledge,
    // Signature aggregators
    message_aggregator: SignatureAggregator,
    proposal_aggregator: SignatureAggregator,
    // DKG/Split/Churn modules
    split_barrier: Rc<RefCell<SplitBarrier>>,
    dkg_sessions: Rc<RefCell<HashMap<Digest256, DkgSessionInfo>>>,
    dkg_voter: DkgVoter,
    relocate_state: Rc<RefCell<Option<Box<JoiningAsRelocated>>>>,
    // ======================== Elder only ========================
    pub(crate) membership: Rc<RefCell<Option<Membership>>>,
    // Section handover consensus state (Some for Elders, None for others)
    pub(crate) handover_voting: Rc<RefCell<Option<Handover>>>,
    joins_allowed: Rc<RefCell<bool>>,
    // Trackers
    capacity: Capacity,
    dysfunction_tracking: DysfunctionDetection,
    pending_data_queries: Rc<Cache<OperationId, Rc<DashSet<Peer>>>>,
    // Caches
    ae_backoff_cache: AeBackoffCache,
}

impl Node {
    // Creates `Core` for a regular node.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        comm: Comm,
        mut info: NodeInfo,
        network_knowledge: NetworkKnowledge,
        section_key_share: Option<SectionKeyShare>,
        event_tx: mpsc::Sender<Event>,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
    ) -> Result<Self> {
        let membership = if let Some(key) = section_key_share.clone() {
            let n_elders = network_knowledge
                .section_signed_authority_provider()
                .elder_count();

            // TODO: the bootstrap members should come from handover
            let bootstrap_members = BTreeSet::from_iter(
                network_knowledge
                    .section_signed_members()
                    .into_iter()
                    .map(|section_auth| section_auth.value.to_msg()),
            );

            Some(Membership::from(
                (key.index as u8, key.secret_key_share),
                key.public_key_set,
                n_elders,
                bootstrap_members,
            ))
        } else {
            None
        };

        let section_keys_provider = SectionKeysProvider::new(section_key_share.clone());

        // make sure the Node has the correct local addr as Comm
        info.addr = comm.our_connection_info();

        let data_storage = DataStorage::new(&root_storage_dir, used_space)?;

        info!("Creating DysfunctionDetection checks");
        let node_dysfunction_detector = DysfunctionDetection::new(
            network_knowledge
                .adults()
                .iter()
                .map(|peer| peer.name())
                .collect::<Vec<XorName>>(),
        );
        info!(
            "DysfunctionDetection check: {:?}",
            node_dysfunction_detector
        );

        // create handover
        let handover = if let Some(key) = section_key_share {
            let secret_key = (key.index as u8, key.secret_key_share);
            let elders = key.public_key_set;
            let n_elders = network_knowledge.elders().len();

            let handover_data = Handover::from(secret_key, elders, n_elders);
            Some(handover_data)
        } else {
            None
        };

        Ok(Self {
            comm,
            info: Rc::new(RefCell::new(info)),
            network_knowledge,
            section_keys_provider,
            dkg_sessions: Rc::new(RefCell::new(HashMap::default())),
            proposal_aggregator: SignatureAggregator::default(),
            split_barrier: Rc::new(RefCell::new(SplitBarrier::new())),
            message_aggregator: SignatureAggregator::default(),
            dkg_voter: DkgVoter::default(),
            relocate_state: Rc::new(RefCell::new(None)),
            event_tx,
            handover_voting: Rc::new(RefCell::new(handover)),
            joins_allowed: Rc::new(RefCell::new(true)),
            resource_proof: ResourceProof::new(RESOURCE_PROOF_DATA_SIZE, RESOURCE_PROOF_DIFFICULTY),
            data_storage,
            capacity: Capacity::default(),
            dysfunction_tracking: node_dysfunction_detector,
            pending_data_queries: Rc::new(Cache::with_expiry_duration(DATA_QUERY_TIMEOUT)),
            ae_backoff_cache: AeBackoffCache::default(),
            membership: Rc::new(RefCell::new(membership)),
        })
    }

    ////////////////////////////////////////////////////////////////////////////
    // Miscellaneous
    ////////////////////////////////////////////////////////////////////////////

    pub(crate) fn generate_probe_msg(&self) -> Result<Cmd> {
        // Generate a random address not belonging to our Prefix
        let mut dst = xor_name::rand::random();

        // We don't probe ourselves
        while self.network_knowledge.prefix().matches(&dst) {
            dst = xor_name::rand::random();
        }

        let matching_section = self.network_knowledge.section_by_name(&dst)?;

        let message = SystemMsg::AntiEntropyProbe;
        let section_key = matching_section.section_key();
        let dst_name = matching_section.prefix().name();
        let recipients = matching_section.elders_vec();

        info!(
            "ProbeMsg target {:?} w/key {:?}",
            matching_section.prefix(),
            section_key
        );

        self.send_direct_msg_to_nodes(recipients, message, dst_name, section_key)
    }

    pub(crate) async fn generate_section_probe_msg(&self) -> Result<Cmd> {
        let our_section = self.network_knowledge.authority_provider();

        let message = SystemMsg::AntiEntropyProbe;
        let section_key = our_section.section_key();
        let dst_name = our_section.prefix().name();
        let recipients = our_section.elders_vec();

        info!(
            "ProbeMsg target section {:?} w/key {:?}",
            our_section.prefix(),
            section_key
        );

        self.send_direct_msg_to_nodes(recipients, message, dst_name, section_key)
    }

    /// Removes any PeerLinks not from our section elders
    pub(crate) async fn cleanup_non_elder_peers(&self) -> Result<()> {
        let elders = self.network_knowledge.elders();
        self.comm
            .cleanup_peers(elders, self.dysfunction_tracking.clone())
            .await
    }

    /// returns names that are relatively dysfunctional
    pub(crate) async fn get_dysfunctional_node_names(&self) -> Result<BTreeSet<XorName>> {
        self.dysfunction_tracking
            .get_nodes_beyond_severity(DysfunctionSeverity::Dysfunctional)
            .await
            .map_err(Error::from)
    }

    /// Log a communication problem
    pub(crate) async fn log_comm_issue(&self, name: XorName) -> Result<()> {
        trace!("Logging comms issue in dysfunction");
        self.dysfunction_tracking
            .track_issue(name, IssueType::Communication)
            .await
            .map_err(Error::from)
    }

    /// Log a knowledge issue
    pub(crate) async fn log_knowledge_issue(&self, name: XorName) -> Result<()> {
        trace!("Logging Knowledge issue in dysfunction");
        self.dysfunction_tracking
            .track_issue(name, IssueType::Knowledge)
            .await
            .map_err(Error::from)
    }

    /// Log a dkg issue (ie, an initialised but unfinished dkg round for a given participant)
    pub(crate) async fn log_dkg_issue(&self, name: XorName) -> Result<()> {
        trace!("Logging Dkg Issue in dysfunction");
        self.dysfunction_tracking
            .track_issue(name, IssueType::Dkg)
            .await
            .map_err(Error::from)
    }

    /// Log a dkg session as responded to
    pub(crate) async fn log_dkg_session(&self, name: &XorName) {
        trace!("Logging Dkg session as responded to in dysfunction");
        self.dysfunction_tracking.dkg_ack_fulfilled(name).await;
    }

    pub(crate) async fn write_prefix_map(&self) {
        info!("Writing our latest PrefixMap to disk");
        // TODO: Make this serialization human readable

        let prefix_map = self.network_knowledge.prefix_map().clone();

        let _ = tokio::task::spawn_local(async move {
            // Compare and write Prefix to `~/.safe/prefix_maps` dir
            if let Err(e) = compare_and_write_prefix_map_to_disk(&prefix_map).await {
                error!("Error writing PrefixMap to `~/.safe` dir: {:?}", e);
            }
        });
    }

    pub(super) async fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            is_elder: self.is_elder(),
            section_key: self.network_knowledge.section_key(),
            prefix: self.network_knowledge.prefix(),
            elders: self.network_knowledge().authority_provider().names(),
        }
    }

    /// Generate a new section info(s) based on the current set of members,
    /// excluding any member matching a name in the provided `excluded_names` set.
    /// Returns a set of candidate DkgSessionId's.
    pub(super) async fn promote_and_demote_elders(
        &self,
        excluded_names: &BTreeSet<XorName>,
    ) -> Result<Vec<DkgSessionId>> {
        let sap = self.network_knowledge.authority_provider();
        let chain_len = self.network_knowledge.chain_len();

        // get current gen and members
        let current_gen;
        let members: BTreeMap<XorName, NodeState> =
            if let Some(m) = self.membership.borrow().as_ref() {
                current_gen = m.generation();
                m.current_section_members()
                    .iter()
                    .filter(|(name, _node_state)| !excluded_names.contains(*name))
                    .map(|(n, s)| (*n, s.clone()))
                    .collect()
            } else {
                error!(
                "attempted to promote and demote elders when we don't have a membership instance"
            );
                return Ok(vec![]);
            };

        // Try splitting
        trace!("{}", LogMarker::SplitAttempt);
        if let Some((zero_dkg_id, one_dkg_id)) =
            try_split_dkg(&members, &sap, chain_len, current_gen)
        {
            debug!(
                "Upon section split attempt, section size: zero {:?}, one {:?}",
                zero_dkg_id.bootstrap_members.len(),
                one_dkg_id.bootstrap_members.len()
            );
            info!("Splitting {:?} {:?}", zero_dkg_id, one_dkg_id);

            // lets track ongoing DKG sessions
            for candidate in zero_dkg_id.elders.keys() {
                self.log_dkg_issue(*candidate).await?;
            }
            for candidate in one_dkg_id.elders.keys() {
                self.log_dkg_issue(*candidate).await?;
            }

            return Ok(vec![zero_dkg_id, one_dkg_id]);
        }

        // Candidates for elders out of all the nodes in the section, even out of the
        // relocating nodes if there would not be enough instead.
        let sap = self.network_knowledge.authority_provider();
        let elder_candidates = elder_candidates(
            members
                .values()
                .cloned()
                .filter(|node| !excluded_names.contains(&node.name)),
            &sap,
        );
        let current_elders = BTreeSet::from_iter(sap.elders().copied());

        info!(
            "ELDER CANDIDATES (current gen:{}) {}: {:?}",
            current_gen,
            elder_candidates.len(),
            elder_candidates
        );

        let res = if elder_candidates
            .iter()
            .map(NodeState::peer)
            .eq(current_elders.iter())
        {
            vec![]
        } else if elder_candidates.len() < supermajority(current_elders.len()) {
            warn!("ignore attempt to reduce the number of elders too much");
            vec![]
        } else if elder_candidates.len() < current_elders.len() {
            // TODO: this special case doesn't seem valid to me, what if the section shrinks to below the elder size.
            // Could be due to the newly promoted elder doesn't have enough knowledge of
            // existing members.
            warn!("Ignore attempt to shrink the elders");
            trace!("current_names  {:?}", current_elders);
            trace!("expected_names {:?}", elder_candidates);
            trace!("excluded_names {:?}", excluded_names);
            trace!("section_peers {:?}", members);
            vec![]
        } else {
            let chain_len = self.network_knowledge.chain_len();
            let session_id = DkgSessionId {
                prefix: sap.prefix(),
                elders: BTreeMap::from_iter(
                    elder_candidates
                        .into_iter()
                        .map(|node| (node.name, node.addr)),
                ),
                section_chain_len: chain_len,
                bootstrap_members: BTreeSet::from_iter(members.into_values()),
                membership_gen: current_gen,
            };
            // track init of DKG
            for candidate in session_id.elders.keys() {
                self.log_dkg_issue(*candidate).await?;
            }

            vec![session_id]
        };

        Ok(res)
    }

    async fn initialize_membership(&self, sap: SectionAuthorityProvider) -> Result<()> {
        let key = self
            .section_keys_provider
            .key_share(&self.network_knowledge.section_key())?;

        let mut membership = self.membership.borrow_mut();

        *membership = Some(Membership::from(
            (key.index as u8, key.secret_key_share),
            key.public_key_set,
            sap.elders.len(),
            BTreeSet::from_iter(sap.members.into_values()),
        ));

        Ok(())
    }

    async fn initialize_handover(&self) -> Result<()> {
        let key = self
            .section_keys_provider
            .key_share(&self.network_knowledge.section_key())?;
        let n_elders = self.network_knowledge.authority_provider().elder_count();

        // reset split barrier for
        let mut split_barrier = self.split_barrier.borrow_mut();
        *split_barrier = SplitBarrier::new();

        let mut handover_voting = self.handover_voting.borrow_mut();
        *handover_voting = Some(Handover::from(
            (key.index as u8, key.secret_key_share),
            key.public_key_set,
            n_elders,
        ));

        Ok(())
    }

    async fn initialize_elder_state(&self) -> Result<()> {
        let sap = self
            .network_knowledge
            .section_signed_authority_provider()
            .value
            .to_msg();
        self.initialize_membership(sap).await?;
        self.initialize_handover().await?;
        Ok(())
    }

    /// Generate cmds and fire events based upon any node state changes.
    pub(super) async fn update_self_for_new_node_state(
        &self,
        old: StateSnapshot,
    ) -> Result<Vec<Cmd>> {
        let mut cmds = vec![];
        let new = self.state_snapshot().await;

        if new.section_key != old.section_key {
            if new.is_elder {
                let sap = self.network_knowledge.authority_provider();
                info!(
                    "Section updated: prefix: ({:b}), key: {:?}, elders: {}",
                    new.prefix,
                    new.section_key,
                    sap.elders().format(", ")
                );

                // It can happen that we recieve the SAP demonstrating that we've become elders
                // before our local DKG can update the section_keys_provider with our Elder key share.
                //
                // Eventually our local DKG instance will complete and add our key_share to the
                // `section_keys_provider` cache. Once that happens, this function will be called
                // again and we can complete our Elder state transition.
                let we_have_our_key_share_for_new_section_key = self
                    .section_keys_provider
                    .key_share(&new.section_key)
                    .is_ok();

                if we_have_our_key_share_for_new_section_key {
                    // The section-key has changed, we are now able to function as an elder.
                    self.initialize_elder_state().await?;

                    cmds.extend(
                        self.promote_and_demote_elders_except(&BTreeSet::new())
                            .await?,
                    );

                    // NB TODO make sure this in only called once (after handover)
                    // and that it cannot interfere with the handover voting process as it resets the handover state completely
                    // NB TODO we should keep a copy of old handover states (since they contain valuable information like who is faulty)
                    self.initialize_handover().await?;

                    // Whenever there is an elders change, casting a round of joins_allowed
                    // proposals to sync.
                    let joins_allowed = { *self.joins_allowed.borrow() };

                    cmds.extend(self.propose(Proposal::JoinsAllowed(joins_allowed))?);
                }

                self.print_network_stats().await;
                self.log_section_stats().await;
            } else {
                // if not elder
                let mut handover_voting = self.handover_voting.borrow_mut();
                *handover_voting = None;
            }

            if new.is_elder || old.is_elder {
                cmds.extend(self.send_ae_update_to_our_section().await);
            }

            let current: BTreeSet<_> = self.network_knowledge.authority_provider().names();
            let added = current.difference(&old.elders).copied().collect();
            let removed = old.elders.difference(&current).copied().collect();
            let remaining = old.elders.intersection(&current).copied().collect();

            let elders = Elders {
                prefix: new.prefix,
                key: new.section_key,
                remaining,
                added,
                removed,
            };

            let self_status_change = if !old.is_elder && new.is_elder {
                info!("{}: {:?}", LogMarker::PromotedToElder, new.prefix);
                NodeElderChange::Promoted
            } else if old.is_elder && !new.is_elder {
                info!("{}", LogMarker::DemotedFromElder);
                self.section_keys_provider.wipe();
                NodeElderChange::Demoted
            } else {
                NodeElderChange::None
            };

            // During the split, sibling's SAP could be unknown to us yet.
            // Hence, fire the SectionSplit event whenever detect a prefix change.
            // We also need to update other nodes w/ our known data.
            let event = if (new.prefix != old.prefix) && new.is_elder {
                info!("{}: {:?}", LogMarker::SplitSuccess, new.prefix);

                if old.is_elder {
                    info!("{}: {:?}", LogMarker::StillElderAfterSplit, new.prefix);
                }

                cmds.extend(self.send_updates_to_sibling_section(&old).await?);
                self.liveness_retain_only(
                    self.network_knowledge
                        .adults()
                        .iter()
                        .map(|peer| peer.name())
                        .collect(),
                )
                .await?;

                Event::SectionSplit {
                    elders,
                    self_status_change,
                }
            } else {
                cmds.extend(
                    self.send_metadata_updates_to_nodes(
                        self.network_knowledge.authority_provider().elders_vec(),
                        &self.network_knowledge.prefix(),
                        new.section_key,
                    )
                    .await?,
                );

                Event::EldersChanged {
                    elders,
                    self_status_change,
                }
            };

            cmds.extend(
                self.send_metadata_updates_to_nodes(
                    self.network_knowledge
                        .authority_provider()
                        .elders()
                        .filter(|peer| !old.elders.contains(&peer.name()))
                        .cloned()
                        .collect(),
                    &self.network_knowledge.prefix(),
                    new.section_key,
                )
                .await?,
            );

            self.send_event(event).await
        }

        Ok(cmds)
    }

    pub(super) fn section_key_by_name(&self, name: &XorName) -> bls::PublicKey {
        if self.network_knowledge.prefix().matches(name) {
            self.network_knowledge.section_key()
        } else if let Ok(sap) = self.network_knowledge.section_by_name(name) {
            sap.section_key()
        } else if self.network_knowledge.prefix().sibling().matches(name) {
            // For sibling with unknown key, use the previous key in our chain under the assumption
            // that it's the last key before the split and therefore the last key of theirs we know.
            // In case this assumption is not correct (because we already progressed more than one
            // key since the split) then this key would be unknown to them and they would send
            // us back their whole section chain. However, this situation should be rare.
            *self.network_knowledge.section_chain().prev_key()
        } else {
            *self.network_knowledge.genesis_key()
        }
    }

    pub(super) async fn print_network_stats(&self) {
        self.network_knowledge
            .prefix_map()
            .network_stats(&self.network_knowledge.authority_provider())
            .print();
    }

    pub(super) async fn log_section_stats(&self) {
        if let Some(m) = self.membership.borrow().as_ref() {
            let adults = self.network_knowledge.adults().len();

            let elders = self.network_knowledge.authority_provider().elder_count();

            let membership_adults = m.current_section_members().len() - elders;
            let prefix = self.network_knowledge.prefix();

            debug!("{prefix:?}: {elders} Elders, {adults}~{membership_adults} Adults.");
        } else {
            debug!("log_section_stats: No membership instance");
        };
    }
}

pub(crate) struct StateSnapshot {
    is_elder: bool,
    section_key: bls::PublicKey,
    prefix: Prefix,
    pub(crate) elders: BTreeSet<XorName>,
}
