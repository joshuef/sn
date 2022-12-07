// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Implementation of the "Node" node for the SAFE Network.

/// Node Configuration
pub mod cfg;

mod api;
mod bootstrap;
mod connectivity;
mod data;
mod dkg;
pub(crate) mod error;
mod flow_ctrl;
mod handover;
mod logging;
mod membership;
mod messages;
mod messaging;
mod node_starter;
mod node_test_api;
mod relocation;

/// Standard channel size, to allow for large swings in throughput
pub static STANDARD_CHANNEL_SIZE: usize = 100_000;

use self::{bootstrap::join_network, core::MyNode, flow_ctrl::cmds::Cmd, node_starter::CmdChannel};
pub use self::{
    cfg::config_handler::Config,
    error::{Error, Result},
    node_starter::{new_test_api, start_node},
    node_test_api::NodeTestApi,
};
pub use crate::storage::DataStorage;
#[cfg(test)]
pub(crate) use relocation::{check as relocation_check, ChurnId};

pub use sn_interface::network_knowledge::MIN_ADULT_AGE;
use sn_interface::{
    messaging::system::{NodeMsg, Proposal},
    types::Peer,
};

pub use qp2p::{Config as NetworkConfig, SendStream};
pub use xor_name::{Prefix, XorName, XOR_NAME_LEN}; // TODO remove pub on API update

mod core {
    use crate::{
        comm::Comm,
        node::{
            bootstrap::JoiningAsRelocated,
            data::Capacity,
            dkg::DkgVoter,
            flow_ctrl::{cmds::Cmd, dysfunction::DysCmds},
            handover::Handover,
            membership::{elder_candidates, try_split_dkg, Membership},
            messaging::Peers,
            DataStorage, Error, Proposal, Result, XorName,
        },
        UsedSpace,
    };
    use sn_consensus::Generation;
    use sn_dysfunction::IssueType;
    use sn_interface::{
        messaging::{
            signature_aggregator::SignatureAggregator,
            system::{DkgSessionId, SectionSigned},
            AuthorityProof, SectionSig,
        },
        network_knowledge::{
            supermajority, MyNodeInfo, NetworkKnowledge, NodeState, SectionAuthorityProvider,
            SectionKeyShare, SectionKeysProvider,
        },
        types::{keys::ed25519::Digest256, log_markers::LogMarker},
    };

    use ed25519_dalek::Keypair;
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        net::SocketAddr,
        path::PathBuf,
        sync::Arc,
    };
    use tokio::sync::mpsc;

    // File name where to cache this node's section tree (stored at this node's set root storage dir)
    const SECTION_TREE_FILE_NAME: &str = "section_tree";

    #[derive(Debug, Clone)]
    pub(crate) struct DkgSessionInfo {
        pub(crate) session_id: DkgSessionId,
        pub(crate) authority: AuthorityProof<SectionSig>,
    }

    pub(crate) struct MyNode {
        pub(crate) comm: Comm,
        pub(crate) addr: SocketAddr, // does this change? if so... when? only at node start atm?
        root_storage_dir: PathBuf,
        pub(crate) data_storage: DataStorage, // Adult only before cache
        pub(crate) keypair: Arc<Keypair>,
        // Network resources
        pub(crate) section_keys_provider: SectionKeysProvider,
        pub(crate) network_knowledge: NetworkKnowledge,
        // Proposal aggregators
        pub(crate) proposal_aggregator: SignatureAggregator,
        // DKG/Split/Churn modules
        pub(crate) dkg_start_aggregator: SignatureAggregator,
        pub(crate) dkg_sessions_info: HashMap<Digest256, DkgSessionInfo>,
        pub(crate) dkg_voter: DkgVoter,
        pub(crate) pending_split_sections:
            BTreeMap<Generation, BTreeSet<SectionSigned<SectionAuthorityProvider>>>,
        pub(crate) relocate_state: Option<Box<JoiningAsRelocated>>,
        // ======================== Elder only ========================
        pub(crate) membership: Option<Membership>,
        // Section handover consensus state (Some for Elders, None for others)
        pub(crate) handover_voting: Option<Handover>,
        pub(crate) joins_allowed: bool,
        pub(crate) joins_allowed_until_split: bool,
        // Trackers
        pub(crate) capacity: Capacity,
        pub(crate) dysfunction_cmds_sender: mpsc::Sender<DysCmds>,
    }

    #[derive(custom_debug::Debug, Clone)]
    pub struct NodeContext {
        pub(crate) root_storage_dir: PathBuf,
        pub(crate) is_elder: bool,
        pub(crate) data_storage: DataStorage,
        pub(crate) name: XorName,
        pub(crate) info: MyNodeInfo,
        pub(crate) keypair: Arc<Keypair>,
        pub(crate) network_knowledge: NetworkKnowledge,
        pub(crate) section_keys_provider: SectionKeysProvider,
        #[debug(skip)]
        pub(crate) comm: Comm,
        pub(crate) joins_allowed: bool,
        #[debug(skip)]
        pub(crate) dysfunction_cmds_sender: mpsc::Sender<DysCmds>,
    }

    impl NodeContext {
        /// Returns the SAP of the section matching the name.
        pub(crate) fn section_sap_matching_name(
            &self,
            name: &XorName,
        ) -> Result<SectionAuthorityProvider> {
            self.network_knowledge
                .section_auth_by_name(name)
                .map_err(Error::from)
        }

        /// Log an issue in dysfunction
        /// Spawns a process to send this incase the channel may be full, we don't hold up
        /// processing around this (as this can be called during dkg eg)
        pub(crate) fn log_node_issue(&self, name: XorName, issue: IssueType) {
            trace!("Logging issue {issue:?} in dysfunction for {name}");
            let dysf_sender = self.dysfunction_cmds_sender.clone();
            // TODO: do we need to kill the node if we fail tracking dysf?
            let _handle = tokio::spawn(async move {
                if let Err(error) = dysf_sender.send(DysCmds::TrackIssue(name, issue)).await {
                    // Log the issue, and error. We need to be wary of actually hitting this.
                    warn!("Could not send DysCmds through dysfunctional_cmds_tx: {error}");
                }
            });
        }
    }

    impl MyNode {
        /// Get the  current node state as a NodeContext
        /// Useful for longer running processes to avoid having to acquire
        /// read locks eg.
        pub(crate) fn context(&self) -> NodeContext {
            NodeContext {
                root_storage_dir: self.root_storage_dir.clone(),
                is_elder: self.is_elder(),
                name: self.name(),
                info: self.info(),
                keypair: self.keypair.clone(),
                network_knowledge: self.network_knowledge().clone(),
                section_keys_provider: self.section_keys_provider.clone(),
                comm: self.comm.clone(),
                joins_allowed: self.joins_allowed || self.joins_allowed_until_split,
                data_storage: self.data_storage.clone(),
                dysfunction_cmds_sender: self.dysfunction_cmds_sender.clone(),
            }
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) async fn new(
            comm: Comm,
            keypair: Arc<Keypair>,
            network_knowledge: NetworkKnowledge,
            section_key_share: Option<SectionKeyShare>,
            used_space: UsedSpace,
            root_storage_dir: PathBuf,
            dysfunction_cmds_sender: mpsc::Sender<DysCmds>,
        ) -> Result<Self> {
            let addr = comm.socket_addr();
            let membership = if let Some(key) = section_key_share.clone() {
                let n_elders = network_knowledge.signed_sap().elder_count();

                // TODO: the bootstrap members should come from handover
                let bootstrap_members = BTreeSet::from_iter(
                    network_knowledge
                        .section_signed_members()
                        .into_iter()
                        .map(|section_auth| section_auth.value),
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

            let data_storage = DataStorage::new(&root_storage_dir, used_space)?;

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

            let node = Self {
                comm,
                addr,
                keypair,
                network_knowledge,
                section_keys_provider,
                root_storage_dir,
                dkg_sessions_info: HashMap::default(),
                proposal_aggregator: SignatureAggregator::default(),
                pending_split_sections: Default::default(),
                dkg_start_aggregator: SignatureAggregator::default(),
                dkg_voter: DkgVoter::default(),
                relocate_state: None,
                handover_voting: handover,
                joins_allowed: true,
                joins_allowed_until_split: false,
                data_storage,
                capacity: Capacity::default(),
                dysfunction_cmds_sender,
                membership,
            };

            let context = &node.context();

            // Write the section tree to this node's root storage directory
            MyNode::write_section_tree(context);

            Ok(node)
        }

        pub(crate) fn info(&self) -> MyNodeInfo {
            let keypair = self.keypair.clone();
            let addr = self.addr;
            MyNodeInfo { keypair, addr }
        }

        pub(crate) fn name(&self) -> XorName {
            self.info().name()
        }

        ////////////////////////////////////////////////////////////////////////////
        // Miscellaneous
        ////////////////////////////////////////////////////////////////////////////

        /// Generates a random AE probe for _anywhere_ on the network.
        pub(crate) fn generate_probe_msg(context: &NodeContext) -> Result<Cmd> {
            // Generate a random address not belonging to our Prefix
            let mut dst = xor_name::rand::random();

            // We don't probe ourselves
            while context.network_knowledge.prefix().matches(&dst) {
                dst = xor_name::rand::random();
            }

            let matching_section = context.network_knowledge.section_auth_by_name(&dst)?;
            let recipients = matching_section.elders_set();

            let probe = context.network_knowledge.anti_entropy_probe();

            info!("ProbeMsg target {:?}: {probe:?}", matching_section.prefix());

            Ok(MyNode::send_system_msg(
                probe,
                Peers::Multiple(recipients),
                context.clone(),
            ))
        }

        /// Generates a SectionProbeMsg with our current knowledge,
        /// targetting our section elders.
        /// Even if we're up to date, we expect a response.
        pub(crate) fn generate_section_probe_msg(context: &NodeContext) -> Cmd {
            let our_section = context.network_knowledge.section_auth();
            let recipients = our_section.elders_set();

            info!(
                "ProbeMsg target our section {:?} recipients {:?}",
                our_section.prefix(),
                recipients,
            );

            let probe = context.network_knowledge.anti_entropy_probe();
            MyNode::send_system_msg(probe, Peers::Multiple(recipients), context.clone())
        }

        /// Generates section infos for the best elder candidate among the members at the given generation
        /// Returns a set of candidate `DkgSessionId`'s.
        pub(crate) fn best_elder_candidates_at_gen(
            &self,
            membership_gen: u64,
        ) -> Vec<DkgSessionId> {
            let sap = self.network_knowledge.section_auth();
            let chain_len = self.network_knowledge.section_chain_len();

            // get members for membership gen
            let members: BTreeMap<XorName, NodeState> = if let Some(m) = self.membership.as_ref() {
                m.section_members(membership_gen)
                    .unwrap_or_default()
                    .iter()
                    .map(|(n, s)| (*n, s.clone()))
                    .collect()
            } else {
                error!("Attempted to find best elder candidates when we don't have a membership instance");
                return vec![];
            };

            // Try splitting
            trace!("{}", LogMarker::SplitAttempt);
            if let Some((zero_dkg_id, one_dkg_id)) =
                try_split_dkg(&members, &sap, chain_len, membership_gen)
            {
                debug!(
                    "Upon section split attempt, section size: zero {:?}, one {:?}",
                    zero_dkg_id.bootstrap_members.len(),
                    one_dkg_id.bootstrap_members.len()
                );
                info!("Splitting {:?} {:?}", zero_dkg_id, one_dkg_id);

                // Lets track ongoing DKG sessions.
                // However we won't receive DKG messages from the other after-split section.
                // So, shall only track the side that we are in as well.
                if zero_dkg_id.elders.contains_key(&self.info().name()) {
                    for candidate in zero_dkg_id.elders.keys() {
                        self.log_node_issue(*candidate, IssueType::Dkg);
                    }
                } else if one_dkg_id.elders.contains_key(&self.info().name()) {
                    for candidate in one_dkg_id.elders.keys() {
                        self.log_node_issue(*candidate, IssueType::Dkg);
                    }
                }

                return vec![zero_dkg_id, one_dkg_id];
            }

            // Candidates for elders out of all the nodes in the section, even out of the
            // relocating nodes if there would not be enough instead.
            let sap = self.network_knowledge.section_auth();
            let elder_candidates = elder_candidates(members.values().cloned(), &sap);
            let current_elders = BTreeSet::from_iter(sap.elders().copied());

            info!(
                "ELDER CANDIDATES (current gen:{}) {}: {:?}",
                membership_gen,
                elder_candidates.len(),
                elder_candidates
            );

            if elder_candidates
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
                trace!("section_peers {:?}", members);
                vec![]
            } else {
                let chain_len = self.network_knowledge.section_chain_len();
                let session_id = DkgSessionId {
                    prefix: sap.prefix(),
                    elders: BTreeMap::from_iter(
                        elder_candidates
                            .into_iter()
                            .map(|node| (node.name(), node.addr())),
                    ),
                    section_chain_len: chain_len,
                    bootstrap_members: BTreeSet::from_iter(members.into_values()),
                    membership_gen,
                };
                // track init of DKG
                for candidate in session_id.elders.keys() {
                    self.log_node_issue(*candidate, IssueType::Dkg);
                }

                vec![session_id]
            }
        }

        /// Generates section infos for the current best elder candidate among the current members
        /// Returns a set of candidate `DkgSessionId`'s.
        pub(crate) fn best_elder_candidates(&self) -> Vec<DkgSessionId> {
            match self.membership.as_ref() {
                Some(m) => self.best_elder_candidates_at_gen(m.generation()),
                None => {
                    error!("Attempted to find best elder candidates when we don't have a membership instance");
                    vec![]
                }
            }
        }

        fn initialize_membership(&mut self, key: SectionKeyShare) -> bool {
            let sap = self.network_knowledge.signed_sap().value;

            // IDEMPOTENCY CHECK: Check if this membership instance has already been
            // initialized for the current SAP, this allows this function to be
            // safely called everytime we process an AE update.
            if let Some(m) = self.membership.as_ref() {
                if m.section_key_set().public_key() == sap.section_key() {
                    return false;
                }
            }

            self.membership = Some(Membership::from(
                (key.index as u8, key.secret_key_share),
                key.public_key_set,
                sap.elders().count(),
                BTreeSet::from_iter(sap.members().cloned()),
            ));

            true
        }

        fn initialize_handover(&mut self, key: SectionKeyShare) -> bool {
            let sap = self.network_knowledge.signed_sap().value;

            // IDEMPOTENCY CHECK: Check if this handover instance has already been
            // initialized for the current SAP, this allows this function to be
            // safely called everytime we process an AE update.
            if let Some(h) = self.handover_voting.as_ref() {
                if h.section_key_set().public_key() == sap.section_key() {
                    return false;
                }
            }

            self.handover_voting = Some(Handover::from(
                (key.index as u8, key.secret_key_share),
                key.public_key_set,
                sap.elders().count(),
            ));

            true
        }

        /// Initializes elder state given the elders key share.
        /// Returns true if something was initialized, false otherwise.
        fn initialize_elder_state(&mut self, key: SectionKeyShare) -> bool {
            let mut updated = self.initialize_membership(key.clone());
            updated |= self.initialize_handover(key);
            updated
        }

        /// Updates various state if elders changed.
        pub(crate) async fn update_on_elder_change(
            &mut self,
            old: &NodeContext,
        ) -> Result<Vec<Cmd>> {
            let new = self.context();
            let new_section_key = new.network_knowledge.section_key();
            let new_prefix = new.network_knowledge.prefix();
            let old_prefix = old.network_knowledge.prefix();
            let old_section_key = old.network_knowledge.section_key();

            let mut cmds = vec![];

            // clean up DKG sessions 5 generations older than current
            // `session_id.section_chain_len + 5 < current_chain_len`
            // we voluntarily keep the previous DKG rounds
            // so lagging elder candidates can still get responses to their gossip.
            // At generation+5, they are likely not going to be elders anymore so we can safely discard it
            let current_chain_len = self.network_knowledge.section_chain_len();
            let old_hashes = Vec::from_iter(
                self.dkg_sessions_info
                    .iter()
                    .filter(|(_, info)| info.session_id.section_chain_len + 5 < current_chain_len)
                    .map(|(hash, _)| *hash),
            );
            for hash in old_hashes {
                if let Some(info) = self.dkg_sessions_info.remove(&hash) {
                    debug!(
                        "Removed old DKG s{} of chain len {} when we are at {}",
                        info.session_id.sh(),
                        info.session_id.section_chain_len,
                        current_chain_len
                    );
                }
                self.dkg_voter.remove(&hash);
            }

            if new_section_key != old_section_key {
                // clean up pending split sections since they no longer apply to the new section
                self.pending_split_sections = Default::default();
            }

            if new.is_elder {
                let sap = self.network_knowledge.section_auth();
                info!(
                    "Section updated: prefix: ({:b}), key: {:?}, elders: {:?}",
                    new_prefix,
                    new_section_key,
                    sap.elders_vec(),
                );

                // It can happen that we recieve the SAP demonstrating that we've become elders
                // before our local DKG can update the section_keys_provider with our Elder key share.
                //
                // Eventually our local DKG instance will complete and add our key_share to the
                // `section_keys_provider` cache. Once that happens, this function will be called
                // again and we can complete our Elder state transition.
                if let Ok(key) = self.section_keys_provider.key_share(&sap.section_key()) {
                    // The section-key has changed, we are now able to function as an elder.
                    if self.initialize_elder_state(key) {
                        cmds.extend(self.trigger_dkg()?);

                        // Whenever there is an elders change, casting a round of joins_allowed
                        // proposals to sync this particular state.
                        cmds.extend(self.propose(Proposal::JoinsAllowed(
                            self.joins_allowed || self.joins_allowed_until_split,
                        ))?);
                    }
                } else {
                    warn!("We're an elder but are missing our section key share, delaying elder state initialization until we receive it: sap={sap:?}");
                }

                self.log_network_stats();
                self.log_section_stats();
            } else {
                // if not elder, clear elder-only state
                self.handover_voting = None;
                self.membership = None;
            }

            if new.is_elder || old.is_elder {
                cmds.extend(self.send_ae_update_to_our_section()?);
            }

            let old_elders = old
                .network_knowledge
                .elders()
                .iter()
                .map(|e| e.name())
                .collect();
            let current_elders: BTreeSet<_> = self.network_knowledge.section_auth().names();
            let added_elders: BTreeSet<_> =
                current_elders.difference(&old_elders).copied().collect();
            let removed_elders: BTreeSet<_> =
                old_elders.difference(&current_elders).copied().collect();
            let remaining_elders: BTreeSet<_> =
                old_elders.intersection(&current_elders).copied().collect();

            let new_elders = !added_elders.is_empty();
            let section_split = new_prefix != old_prefix;
            let elders_changed = !added_elders.is_empty() || !removed_elders.is_empty();

            if !old.is_elder && new.is_elder {
                debug!("Node has been promoted to Elder");
                info!("{}: {:?}", LogMarker::PromotedToElder, new_prefix);
            } else if old.is_elder && !new.is_elder {
                debug!("Node has been demoted from Elder");
                info!("{}", LogMarker::DemotedFromElder);
                self.section_keys_provider.wipe();
            }

            if section_split && new.is_elder {
                info!("{}: {:?}", LogMarker::SplitSuccess, new_prefix);

                if old.is_elder {
                    info!("{}: {:?}", LogMarker::StillElderAfterSplit, new_prefix);
                }

                cmds.extend(self.send_updates_to_sibling_section(old)?);
                self.liveness_retain_only(
                    self.network_knowledge
                        .adults()
                        .iter()
                        .map(|peer| peer.name())
                        .collect(),
                )
                .await;

                debug!(
                    "Section has been split, new_prefix: {:?}, section_key {:?}, remaining elders\
                    in our section {:?}, new elders {:?} removed elders {:?}",
                    new_prefix,
                    new.network_knowledge.section_key(),
                    remaining_elders,
                    new_elders,
                    removed_elders
                );
            };

            if !section_split && elders_changed {
                debug!(
                    "Elders has been changed. prefix: {:?}, section_key {:?},  remaining elders\
                    in our section {:?}, new elders {:?} removed elders {:?}",
                    new_prefix,
                    new.network_knowledge.section_key(),
                    remaining_elders,
                    new_elders,
                    removed_elders
                );
            }

            // when we split, the range of data of adults are responsible for is halved, i.e. their threshold is no longer reached.
            if section_split && old.is_elder {
                self.clear_full_adults();
                self.joins_allowed_until_split = false;
            }
            if section_split && self.data_storage.is_threshold_reached() {
                // would only be set if we were an adult..
                self.data_storage.clear_threshold_reached();
            }

            // update new elders if we were an adult and our storage threshold has been reached
            if new_elders && !old.is_elder && self.data_storage.is_threshold_reached() {
                cmds.push(
                    self.report_us_as_full(
                        self.network_knowledge
                            .section_auth()
                            .elders()
                            .filter(|peer| added_elders.contains(&peer.name()))
                            .cloned()
                            .collect(),
                    ),
                );
            };

            Ok(cmds)
        }

        /// Log an issue in dysfunction
        /// Spawns a process to send this incase the channel may be full, we don't hold up
        /// processing around this (as this can be called during dkg eg)
        pub(crate) fn log_node_issue(&self, name: XorName, issue: IssueType) {
            trace!("Logging issue {issue:?} in dysfunction for {name}");
            let dysf_sender = self.dysfunction_cmds_sender.clone();
            // TODO: do we need to kill the node if we fail tracking dysf?
            let _handle = tokio::spawn(async move {
                if let Err(error) = dysf_sender.send(DysCmds::TrackIssue(name, issue)).await {
                    // Log the issue, and error. We need to be wary of actually hitting this.
                    warn!("Could not send DysCmds through dysfunctional_cmds_tx: {error}");
                }
            });
        }

        /// Sends `DysCmds::UntrackIssue` cmd
        /// Spawns a process to send this incase the channel may be full, we don't hold up
        /// processing around this (as this can be called during dkg eg)
        pub(crate) fn untrack_node_issue(&self, name: XorName, issue: IssueType) {
            let dysf_sender = self.dysfunction_cmds_sender.clone();
            // TODO: do we need to kill the node if we fail tracking dysf?
            let _handle = tokio::spawn(async move {
                if let Err(error) = dysf_sender.send(DysCmds::UntrackIssue(name, issue)).await {
                    warn!("Could not send DysCmds through dysfunctional_cmds_tx: {error}");
                }
            });
        }

        #[allow(unused)]
        pub(crate) fn section_key_by_name(&self, name: &XorName) -> Result<bls::PublicKey> {
            if self.network_knowledge.prefix().matches(name) {
                Ok(self.network_knowledge.section_key())
            } else if let Ok(sap) = self.network_knowledge.section_auth_by_name(name) {
                Ok(sap.section_key())
            } else if self.network_knowledge.prefix().sibling().matches(name) {
                // For sibling with unknown key, use the previous key in our chain under the assumption
                // that it's the last key before the split and therefore the last key of theirs we know.
                // In case this assumption is not correct (because we already progressed more than one
                // key since the split) then this key would be unknown to them and they would send
                // us back their whole section chain. However, this situation should be rare.

                // section_chain contains a single leaf key
                let leaf_key = self.network_knowledge.section_chain().last_key()?;
                match self.section_chain().get_parent_key(&leaf_key) {
                    Ok(prev_pk) => Ok(prev_pk.unwrap_or(*self.section_chain().genesis_key())),
                    Err(_) => {
                        error!("SectionsDAG fields went out of sync");
                        Ok(leaf_key)
                    }
                }
            } else {
                Ok(*self.network_knowledge.genesis_key())
            }
        }

        pub(crate) fn log_network_stats(&self) {
            info!(
                "{}",
                self.network_knowledge
                    .section_tree()
                    .network_stats(&self.network_knowledge.section_auth())
            );
        }

        pub(crate) fn log_section_stats(&self) {
            if let Some(m) = self.membership.as_ref() {
                let adults = self.network_knowledge.adults().len();

                let elders = self.network_knowledge.section_auth().elder_count();

                let membership_adults = m.current_section_members().len() - elders;
                let prefix = self.network_knowledge.prefix();

                debug!("{prefix:?}: {elders} Elders, {adults}~{membership_adults} Adults.");
            } else {
                debug!("log_section_stats: No membership instance");
            };
        }

        pub(crate) fn write_section_tree(context: &NodeContext) {
            let section_tree = context.network_knowledge.section_tree().clone();
            let path = context
                .root_storage_dir
                .clone()
                .join(SECTION_TREE_FILE_NAME);

            let _ = tokio::spawn(async move {
                if let Err(err) = section_tree.write_to_disk(&path).await {
                    error!(
                        "Error writing SectionTree to `{}` dir: {:?}",
                        path.display(),
                        err
                    );
                }
            });
        }
    }
}
