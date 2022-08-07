// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Dysfunctional "Node" detection for the SAFE Network.
//! All dysfunction detection should route through this for proper weighting in
//! relation to other possible sources of dysfunction

// For quick_error
#![recursion_limit = "256"]
#![doc(
    html_logo_url = "https://github.com/maidsafe/QA/raw/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(deny(warnings)))
)]
// Forbid some very bad patterns. Forbid is stronger than `deny`, preventing us from suppressing the
// lint with `#[allow(...)]` et-all.
#![forbid(
    arithmetic_overflow,
    mutable_transmutes,
    no_mangle_const_items,
    unknown_crate_types,
    unsafe_code
)]
// Turn on some additional warnings to encourage good style.
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    clippy::unicode_not_nfc
)]

#[macro_use]
extern crate tracing;

mod detection;
mod error;

pub use detection::{DysfunctionSeverity, IssueType};

pub use crate::error::Error;
use sn_interface::messaging::data::OperationId;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};
use xor_name::XorName;

/// Some reproducible xorname derived from the operation. This is a permanent reference needed for logging all dysfunction.
type NodeIdentifier = XorName;

pub(crate) type TimedTracker = BTreeMap<NodeIdentifier, VecDeque<Instant>>;

#[derive(Clone, Debug)]
/// Dysfunctional node tracking. Allows various potential issues to be tracked and weighted,
/// with unresposive or suspect nodes being noted on request, against which action can then be taken.
pub struct DysfunctionDetection {
    /// The communication issues logged against a node, along with a timestamp.
    pub communication_issues: TimedTracker,
    /// The dkg issues logged against a node, along with a timestamp to expire after some time.
    pub dkg_issues: TimedTracker,
    /// The probe issues logged against a node and as yet unfulfilled, along with a timestamp to expire after some time.
    pub probe_issues: TimedTracker,
    /// The knowledge issues logged against a node, along with a timestamp.
    pub knowledge_issues: TimedTracker,
    /// The unfulfilled pending request operation issues logged against a node, along with an
    /// operation ID.
    pub unfulfilled_ops: BTreeMap<NodeIdentifier, Vec<(OperationId, Instant)>>,
    nodes: Vec<XorName>,
}

impl DysfunctionDetection {
    /// Set up a new Dysfunctional Node Tracker.
    pub fn new(nodes: Vec<NodeIdentifier>) -> Self {
        Self {
            communication_issues: BTreeMap::new(),
            dkg_issues: BTreeMap::new(),
            probe_issues: BTreeMap::new(),
            knowledge_issues: BTreeMap::new(),
            unfulfilled_ops: BTreeMap::new(),
            nodes,
        }
    }

    /// Adds an issue to the dysfunction tracker.
    ///
    /// The `op_id` only applies when adding an operational issue.
    pub fn track_issue(&mut self, node_id: NodeIdentifier, issue_type: IssueType) {
        debug!("Adding a new issue to the dysfunction tracker: {issue_type:?}");
        match issue_type {
            IssueType::Dkg => {
                let queue = self.dkg_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::AwaitingProbeResponse => {
                let queue = self.probe_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::Communication => {
                let queue = self.communication_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::Knowledge => {
                let queue = self.knowledge_issues.entry(node_id).or_default();
                queue.push_back(Instant::now());
            }
            IssueType::PendingRequestOperation(op_id) => {
                let queue = self.unfulfilled_ops.entry(node_id).or_default();
                trace!("New issue has associated operation ID: {op_id:#?}");
                queue.push((op_id, Instant::now()));
            }
        }
    }

    /// Removes a `pending_operation` from the node liveness records. Returns true if a record was removed
    pub fn request_operation_fulfilled(
        &mut self,
        node_id: &NodeIdentifier,
        operation_id: OperationId,
    ) -> bool {
        trace!(
            "Attempting to remove pending_operation {:?} op: {:?}",
            node_id,
            operation_id
        );
        let mut has_removed = false;

        if let Some(v) = self.unfulfilled_ops.get_mut(node_id) {
            // only remove the first instance from the vec
            v.retain(|(op_id, _)| {
                if has_removed || op_id != &operation_id {
                    true
                } else {
                    has_removed = true;
                    false
                }
            });
            if has_removed {
                trace!(
                    "Pending operation removed for node: {:?} op: {:?}",
                    node_id,
                    operation_id
                );
            } else {
                trace!(
                    "No Pending operation found for node: {:?} op: {:?}",
                    node_id,
                    operation_id
                );
            }
        }
        has_removed
    }

    /// Removes a DKG session from the node liveness records. Returns true if a record was removed
    pub fn dkg_ack_fulfilled(&mut self, node_id: &NodeIdentifier) {
        trace!("Attempting to remove logged dkg session for {:?}", node_id,);

        if let Some(v) = self.dkg_issues.get_mut(node_id) {
            // only remove the first instance from the vec
            let prev = v.pop_front();

            if prev.is_some() {
                trace!("Pending dkg session removed for node: {:?}", node_id,);
            } else {
                trace!("No Pending dkg session found for node: {:?}", node_id);
            }
        }
    }
    /// Removes a probe tracker from the node liveness records. Returns true if a record was removed
    pub fn ae_update_msg_received(&mut self, node_id: &NodeIdentifier) {
        trace!(
            "Attempting to remove pending AEProbe response for {:?}",
            node_id,
        );

        if let Some(v) = self.probe_issues.get_mut(node_id) {
            // only remove the first instance from the vec
            let prev = v.pop_front();

            if prev.is_some() {
                trace!("Pending probe msg removed for node: {:?}", node_id,);
            } else {
                trace!("No Pending probe session found for node: {:?}", node_id);
            }
        }
    }

    /// Gets the unfulfilled operation IDs for a given node.
    ///
    /// This is for convenience, to wrap reading the concurrent data structure that stores the
    /// values.
    ///
    /// If there are no unfulfilled operations tracked, an empty list will be returned.
    pub fn get_unfulfilled_ops(&self, node: XorName) -> Vec<OperationId> {
        if let Some(val) = self.unfulfilled_ops.get(&node) {
            return val.to_vec().iter().map(|(op_id, _)| *op_id).collect();
        }
        Vec::new()
    }

    /// List all current tracked nodes
    pub fn current_nodes(&self) -> Vec<XorName> {
        self.nodes.to_vec()
    }

    /// Add a new node to the tracker and recompute closest nodes.
    pub fn add_new_node(&mut self, node: XorName) {
        info!("Adding new node:{node} to DysfunctionDetection tracker");
        self.nodes.push(node);
    }

    /// Removes tracked nodes not present in `current_members`.
    ///
    /// Tracked issues related to nodes that were removed will also be removed.
    pub fn retain_members_only(&mut self, current_members: BTreeSet<XorName>) {
        let nodes = &mut self.nodes;
        let nodes_being_removed = nodes
            .iter()
            .filter(|x| !current_members.contains(x))
            .copied()
            .collect::<Vec<XorName>>();
        nodes.retain(|x| current_members.contains(x));
        for node in nodes_being_removed.iter() {
            let _ = self.communication_issues.remove(node);
            let _ = self.knowledge_issues.remove(node);
            let _ = self.unfulfilled_ops.remove(node);
        }
    }
}

/// Calculates the avg value in a data set
/// https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html
pub(crate) fn get_mean_of(data: &[f32]) -> Option<f32> {
    let sum: f32 = data.iter().sum();
    let count = data.len();
    if count > 0 {
        Some(sum / count as f32)
    } else {
        None
    }
}

/// Calculates the standard deviation of a data set
fn std_deviation(data: &[f32]) -> Option<f32> {
    match (get_mean_of(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data
                .iter()
                .map(|value| {
                    let diff = data_mean - *value;

                    diff * diff
                })
                .sum::<f32>()
                / count as f32;

            Some(variance.sqrt())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{DysfunctionDetection, IssueType};
    use sn_interface::messaging::data::OperationId;

    use eyre::Error;
    use std::{collections::BTreeSet, sync::Once};
    use xor_name::{rand::random as random_xorname, XorName};

    type Result<T, E = Error> = std::result::Result<T, E>;

    static INIT: Once = Once::new();

    /// Initialise logger for tests, this is run only once, even if called multiple times.
    pub(crate) fn init_test_logger() {
        INIT.call_once(|| {
            tracing_subscriber::fmt::fmt()
                // NOTE: uncomment this line for pretty printed log output.
                .with_thread_names(true)
                .with_ansi(false)
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_target(false)
                // .event_format(LogFormatter::default())
                .try_init()
                .unwrap_or_else(|_| println!("Error initializing logger"));
        });
    }

    #[tokio::test]
    async fn retain_members_should_remove_other_nodes() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());
        let nodes_to_retain = nodes[5..10].iter().cloned().collect::<BTreeSet<XorName>>();

        dysfunctional_detection.retain_members_only(nodes_to_retain.clone());

        let current_nodes = dysfunctional_detection.current_nodes();
        assert_eq!(current_nodes.len(), 5);
        for member in current_nodes {
            assert!(nodes_to_retain.contains(&member));
        }

        Ok(())
    }

    #[tokio::test]
    async fn retain_members_should_remove_issues_relating_to_nodes_not_retained() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        // Track some issues for nodes that are going to be removed.
        for node in nodes.iter().take(3) {
            dysfunctional_detection.track_issue(*node, IssueType::Communication);
            dysfunctional_detection.track_issue(*node, IssueType::Knowledge);
            dysfunctional_detection.track_issue(
                *node,
                IssueType::PendingRequestOperation(OperationId([1; 32])),
            );
        }

        // Track some issues for nodes that will be retained.
        dysfunctional_detection.track_issue(nodes[5], IssueType::Communication);
        dysfunctional_detection.track_issue(nodes[6], IssueType::Knowledge);
        dysfunctional_detection.track_issue(
            nodes[7],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );

        let nodes_to_retain = nodes[5..10].iter().cloned().collect::<BTreeSet<XorName>>();

        dysfunctional_detection.retain_members_only(nodes_to_retain);

        assert_eq!(dysfunctional_detection.communication_issues.len(), 1);
        assert_eq!(dysfunctional_detection.knowledge_issues.len(), 1);
        assert_eq!(dysfunctional_detection.unfulfilled_ops.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_comm_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        dysfunctional_detection.track_issue(nodes[0], IssueType::Communication);

        assert_eq!(dysfunctional_detection.communication_issues.len(), 1);
        assert_eq!(dysfunctional_detection.knowledge_issues.len(), 0);
        assert_eq!(dysfunctional_detection.unfulfilled_ops.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_knowledge_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        dysfunctional_detection.track_issue(nodes[0], IssueType::Knowledge);

        assert_eq!(dysfunctional_detection.knowledge_issues.len(), 1);
        assert_eq!(dysfunctional_detection.communication_issues.len(), 0);
        assert_eq!(dysfunctional_detection.unfulfilled_ops.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn track_issue_should_add_a_pending_op_issue() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );

        assert_eq!(dysfunctional_detection.unfulfilled_ops.len(), 1);
        assert_eq!(dysfunctional_detection.knowledge_issues.len(), 0);
        assert_eq!(dysfunctional_detection.communication_issues.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn add_new_node_should_track_new_node() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes);

        let new_adult = random_xorname();
        dysfunctional_detection.add_new_node(new_adult);

        let current_nodes = dysfunctional_detection.current_nodes();

        assert_eq!(current_nodes.len(), 11);
        Ok(())
    }

    #[tokio::test]
    async fn get_unfulfilled_ops_should_return_op_ids() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([2; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([3; 32])),
        );

        let op_ids = dysfunctional_detection.get_unfulfilled_ops(nodes[0]);

        assert_eq!(3, op_ids.len());
        Ok(())
    }

    #[tokio::test]
    async fn get_unfulfilled_ops_should_return_empty_list_for_node_with_no_ops() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([2; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([3; 32])),
        );

        let op_ids = dysfunctional_detection.get_unfulfilled_ops(nodes[1]);

        assert_eq!(0, op_ids.len());
        Ok(())
    }

    #[tokio::test]
    async fn request_operation_fulfilled_should_remove_pending_op() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());
        let op_id = OperationId([2; 32]);

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([2; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([3; 32])),
        );

        let has_removed = dysfunctional_detection.request_operation_fulfilled(&nodes[0], op_id);

        assert!(has_removed);
        let op_ids = dysfunctional_detection.get_unfulfilled_ops(nodes[0]);
        assert_eq!(2, op_ids.len());
        assert_eq!(OperationId([1; 32]), op_ids[0]);
        assert_eq!(OperationId([3; 32]), op_ids[1]);
        Ok(())
    }

    #[tokio::test]
    async fn request_operation_fulfilled_should_return_false_for_node_with_no_ops() -> Result<()> {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());
        let op_id = OperationId([2; 32]);

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([2; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([3; 32])),
        );

        let has_removed = dysfunctional_detection.request_operation_fulfilled(&nodes[1], op_id);

        assert!(!has_removed);
        Ok(())
    }

    #[tokio::test]
    async fn request_operation_fulfilled_should_return_false_when_op_id_not_tracked() -> Result<()>
    {
        let nodes = (0..10).map(|_| random_xorname()).collect::<Vec<XorName>>();
        let mut dysfunctional_detection = DysfunctionDetection::new(nodes.clone());
        let op_id = OperationId([4; 32]);

        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([1; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([2; 32])),
        );
        dysfunctional_detection.track_issue(
            nodes[0],
            IssueType::PendingRequestOperation(OperationId([3; 32])),
        );

        let has_removed = dysfunctional_detection.request_operation_fulfilled(&nodes[1], op_id);

        assert!(!has_removed);
        let op_ids = dysfunctional_detection.get_unfulfilled_ops(nodes[0]);
        assert_eq!(3, op_ids.len());
        Ok(())
    }
}
