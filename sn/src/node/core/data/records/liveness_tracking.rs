// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::XorName;

use dashmap::DashMap;
use itertools::Itertools;
use sn_interface::messaging::data::{chunk_operation_id, OperationId};
use sn_interface::network_knowledge::DEFAULT_ELDER_COUNT;
use sn_interface::types::ChunkAddress;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Count of neighbours to every node that we keep track of.
// NOTE: To be set relevant to section size when tweaked
const NEIGHBOUR_COUNT: usize = DEFAULT_ELDER_COUNT;

/// Minimum number of pending operations that are allowed to stagnate before liveness checks kick in
// ~400 ops was the maximum number stagnating ops seen with client_api tests(multi-threaded).
// Therefore 500 is better higher cap with some buffer. Setting it to 400 or lower might start replication on CI unnecessarily.
const MIN_PENDING_OPS: usize = 500;

// If the pending ops count of a node is 5 times higher than it's neighbours, it will be kicked.
const EXCESSIVE_OPS_TOLERANCE: f64 = 5.0; // increasing this number increases tolerance

// If the pending ops count of a node is EXCESSIVE_OPS_TOLERANCE / 2 times higher than it's neighbours, preemptive replication starts.
const PREEMPTIVE_REPLICATION_TOLERANCE: f64 = EXCESSIVE_OPS_TOLERANCE / 2.0; // increasing this number increases tolerance

/// Some reproducible xorname derived from the operation. Which can be re-derived from the appropriate response when received (to remove from tracking)
type NodeIdentifier = XorName;

#[derive(Clone, Debug)]
pub(crate) struct Liveness {
    /// One of (potentially many) different ways of assessing unresponsiveness of nodes.
    unfulfilled_requests: Arc<DashMap<NodeIdentifier, Arc<RwLock<Vec<OperationId>>>>>,
    closest_nodes_to: Arc<DashMap<XorName, Vec<XorName>>>,
}

impl Liveness {
    pub(crate) fn new(our_adults: Vec<NodeIdentifier>) -> Self {
        let closest_nodes_to = DashMap::new();

        for adult in our_adults.iter() {
            let closest_nodes = our_adults
                .iter()
                .filter(|&key| key != adult)
                .sorted_by(|lhs, rhs| adult.cmp_distance(lhs, rhs))
                .take(NEIGHBOUR_COUNT)
                .cloned()
                .collect::<Vec<_>>();
            let _old_entry = closest_nodes_to.insert(*adult, closest_nodes);
        }
        Self {
            unfulfilled_requests: Arc::new(DashMap::new()),
            closest_nodes_to: Arc::new(closest_nodes_to),
        }
    }

    pub(crate) fn current_nodes(&self) -> Vec<XorName> {
        self.closest_nodes_to
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<XorName>>()
    }

    /// Add a new adult to the tracker and recompute closest nodes.
    pub(crate) fn add_new_adult(&self, adult: XorName) {
        info!("Adding new adult:{adult} to Liveness tracker");

        let our_adults: Vec<_> = self
            .closest_nodes_to
            .iter()
            .map(|entry| *entry.key())
            .collect();

        let closest_nodes = our_adults
            .iter()
            .filter(|&key| key != &adult)
            .sorted_by(|lhs, rhs| adult.cmp_distance(lhs, rhs))
            .take(NEIGHBOUR_COUNT)
            .cloned()
            .collect::<Vec<_>>();

        info!("Closest nodes to {adult}:{closest_nodes:?}");

        if let Some(_old_entry) = self.closest_nodes_to.insert(adult, closest_nodes) {
            warn!("Throwing old liveness tracker for Adult {adult}:{_old_entry:?}");
        }

        self.recompute_closest_nodes();
    }

    // Inserts a pending_operation, and is deemed as such until we get the appropriate response from the node
    // Returns false if the operation already existed.
    pub(crate) async fn add_a_pending_request_operation(
        &self,
        node_id: NodeIdentifier,
        operation_id: OperationId,
    ) {
        let entry = self.unfulfilled_requests.entry(node_id).or_default();

        trace!(
            "Adding pending operation against node: {:?}: for op: {:?}",
            node_id,
            &operation_id
        );

        let v = entry.value();
        v.write().await.push(operation_id);
    }

    pub(crate) fn retain_members_only(&self, current_members: BTreeSet<XorName>) {
        let all_keys: Vec<_> = self.current_nodes();

        for key in &all_keys {
            if !current_members.contains(key) {
                let _prev = self.unfulfilled_requests.remove(key);
                let _prev = self.closest_nodes_to.remove(key);
            }
        }

        self.recompute_closest_nodes();
    }

    /// Inserts a random OperationId to decrease the credibility of the given Adult node.
    #[allow(unused)]
    pub(crate) async fn penalise_member(&self, member: XorName) {
        if let Ok(random_op_id) = chunk_operation_id(&ChunkAddress(XorName::random())) {
            self.add_a_pending_request_operation(member, random_op_id)
                .await
        } else {
            error!("Error generating a random OperationID for penalising member")
        }
    }

    /// Removes a pending_operation from the node liveness records
    pub(crate) async fn request_operation_fulfilled(
        &self,
        node_id: &NodeIdentifier,
        operation_id: OperationId,
    ) -> bool {
        trace!(
            "Attempting to remove pending_operation {:?} op: {:?}",
            node_id,
            operation_id
        );
        let mut has_removed = false;

        if let Some(entry) = self.unfulfilled_requests.get(node_id) {
            let v = entry.value();

            // only remove the first instance from the vec
            v.write().await.retain(|x| {
                if has_removed || x != &operation_id {
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

    pub(crate) fn recompute_closest_nodes(&self) {
        let all_known_nodes: Vec<_> = self.current_nodes();

        self.closest_nodes_to.alter_all(|name, _| {
            all_known_nodes
                .iter()
                .filter(|&key| key != name)
                .sorted_by(|lhs, rhs| name.cmp_distance(lhs, rhs))
                .take(NEIGHBOUR_COUNT)
                .copied()
                .collect::<Vec<_>>()
        });
    }

    // This is not an exact definition, thus has tolerance for variance due to concurrency
    pub(crate) async fn find_unresponsive_and_deviant_nodes(
        &self,
    ) -> (Vec<(XorName, usize)>, BTreeSet<XorName>) {
        info!("Checking for unresponsive and deviant nodes");
        let mut unresponsive_nodes = Vec::new();
        let mut deviants = Vec::new();

        for entry in self.closest_nodes_to.iter() {
            let (node, neighbours) = entry.pair();
            info!("Checking node/neighbours: {:?}/{:?}", node, neighbours);

            let node = *node;
            let mut max_pending_by_neighbours = 0;
            for neighbour in neighbours.iter() {
                if let Some(entry) = self.unfulfilled_requests.get(neighbour) {
                    let val = entry.value().read().await.len();

                    if val > max_pending_by_neighbours {
                        max_pending_by_neighbours = val
                    }
                }
            }

            let pending_operations_count = if let Some(entry) = self.unfulfilled_requests.get(&node)
            {
                entry.value().read().await.len()
            } else {
                0
            };

            let crossed_min_threshold = pending_operations_count > MIN_PENDING_OPS;

            let excessive_stagnating_ops = pending_operations_count as f64
                > max_pending_by_neighbours as f64 * EXCESSIVE_OPS_TOLERANCE;

            let need_for_preemptive_replication = pending_operations_count as f64
                > max_pending_by_neighbours as f64 * PREEMPTIVE_REPLICATION_TOLERANCE;

            // Replicate preemptively at 2.5x of neighbours max pending ops and kick at 5x.
            // Checks begin only if op count crosses MIN_PENDING_OPS
            if crossed_min_threshold && excessive_stagnating_ops {
                tracing::info!(
                    "Pending ops for {}: {} Neighbour max: {}",
                    node,
                    pending_operations_count,
                    max_pending_by_neighbours
                );
                unresponsive_nodes.push((node, pending_operations_count));
            }

            if crossed_min_threshold && need_for_preemptive_replication {
                info!(
                    "Probable deviant {node} crossed PREEMPTIVE_REPLICATION_TOLERANCE: \
                {pending_operations_count}: Neighbour max: {max_pending_by_neighbours}",
                );
                deviants.push(node);
            }
        }
        (
            unresponsive_nodes,
            deviants.into_iter().collect::<BTreeSet<XorName>>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Liveness, EXCESSIVE_OPS_TOLERANCE};

    use crate::node::Error;
    use sn_interface::messaging::data::chunk_operation_id;
    use sn_interface::types::ChunkAddress;

    use crate::node::core::data::records::liveness_tracking::MIN_PENDING_OPS;
    use itertools::Itertools;
    use std::collections::BTreeSet;
    use xor_name::XorName;

    #[tokio::test]
    async fn liveness_basics() -> Result<(), Error> {
        let adults = (0..10).map(|_| XorName::random()).collect::<Vec<XorName>>();
        let liveness_tracker = Liveness::new(adults.clone());

        // Write data MIN_PENDING_OPS times to the 10 adults
        for adult in &adults {
            for _ in 0..MIN_PENDING_OPS {
                let random_addr = ChunkAddress(XorName::random());
                let op_id = chunk_operation_id(&random_addr)?;
                liveness_tracker
                    .add_a_pending_request_operation(*adult, op_id)
                    .await;
            }
        }

        // Assert there are not any unresponsive nodes
        // This is because all of them are within the tolerance ratio of each other
        assert_eq!(
            liveness_tracker
                .find_unresponsive_and_deviant_nodes()
                .await
                .0
                .len(),
            0
        );

        // Add a new adults
        let new_adult = XorName::random();
        liveness_tracker.add_new_adult(new_adult);

        // Assert total adult count
        assert_eq!(liveness_tracker.closest_nodes_to.len(), 11);

        // Write data (EXCESSIVE_OPS_TOLERANCE/2) + 1 x MIN_PENDING_OPS times to the new adult to check for preemptive replication
        for _ in 0..MIN_PENDING_OPS * ((EXCESSIVE_OPS_TOLERANCE as usize / 2) + 1) {
            let random_addr = ChunkAddress(XorName::random());
            let op_id = chunk_operation_id(&random_addr)?;
            liveness_tracker
                .add_a_pending_request_operation(new_adult, op_id)
                .await;
        }

        // Assert that the new adult is detected as deviant.
        assert!(liveness_tracker
            .find_unresponsive_and_deviant_nodes()
            .await
            .1
            .iter()
            .contains(&new_adult));

        // Write data another EXCESSIVE_OPS_TOLERANCE x 50 times to the new adult to check for unresponsiveness.
        for _ in 0..MIN_PENDING_OPS * EXCESSIVE_OPS_TOLERANCE as usize {
            let random_addr = ChunkAddress(XorName::random());
            let op_id = chunk_operation_id(&random_addr)?;
            liveness_tracker
                .add_a_pending_request_operation(new_adult, op_id)
                .await;
        }

        // Assert that the new adult is detected unresponsive.
        assert!(liveness_tracker
            .find_unresponsive_and_deviant_nodes()
            .await
            .0
            .iter()
            .map(|node| node.0)
            .contains(&new_adult));

        Ok(())
    }

    #[tokio::test]
    async fn liveness_retain_members() -> Result<(), Error> {
        let adults = (0..10).map(|_| XorName::random()).collect::<Vec<XorName>>();
        let liveness_tracker = Liveness::new(adults.clone());

        let live_adults = adults[5..10].iter().cloned().collect::<BTreeSet<XorName>>();

        liveness_tracker.retain_members_only(live_adults.clone());

        let all_known_nodes: Vec<_> = liveness_tracker.current_nodes();

        for member in all_known_nodes {
            assert!(live_adults.contains(&member));
        }

        Ok(())
    }

    #[tokio::test]
    async fn liveness_compute_closest() -> Result<(), Error> {
        // Adults with prefix 0
        let mut adults0 = (0..10)
            .map(|_| XorName::random().with_bit(0, false))
            .collect::<Vec<XorName>>();

        // Adults with prefix 1
        let mut adults1 = (0..10)
            .map(|_| XorName::random().with_bit(0, true))
            .collect::<Vec<XorName>>();

        // Whole set of Adults
        let mut all_adults = vec![];
        all_adults.extend(adults0.clone());
        all_adults.extend(adults1.clone());

        let liveness_tracker = Liveness::new(all_adults);

        for entry in liveness_tracker.closest_nodes_to.iter() {
            let (node, neighbours) = entry.pair();

            // Assert all the neigbours fall under the correct set
            if node.bit(0) {
                for neigbour in neighbours {
                    assert!(adults1.contains(neigbour))
                }
            } else {
                for neigbour in neighbours {
                    assert!(adults0.contains(neigbour))
                }
            }
        }

        // Add 5 new adults for each 0 and 1 prefix
        let new_adults0 = (0..5)
            .map(|_| XorName::random().with_bit(0, false))
            .collect::<Vec<XorName>>();

        let new_adults1 = (0..5)
            .map(|_| XorName::random().with_bit(0, true))
            .collect::<Vec<XorName>>();

        let mut new_adults = vec![];
        new_adults.extend(new_adults0.clone());
        new_adults.extend(new_adults1.clone());

        // Add them to the larger set
        adults0.extend(new_adults0);
        adults1.extend(new_adults1);

        // Add all of them to the liveness tracker
        for new_adult in new_adults {
            liveness_tracker.add_new_adult(new_adult);
        }

        // Check if the recomputed nodes all fall within their respective sets
        for entry in liveness_tracker.closest_nodes_to.iter() {
            let (node, neighbours) = entry.pair();

            // Assert all the neighbours fall under the correct set
            if node.bit(0) {
                for neigbour in neighbours {
                    assert!(adults1.contains(neigbour))
                }
            } else {
                for neigbour in neighbours {
                    assert!(adults0.contains(neigbour))
                }
            }
        }

        Ok(())
    }
}
