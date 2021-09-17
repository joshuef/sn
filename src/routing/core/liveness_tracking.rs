// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::routing::XorName;

use crate::messaging::data::OperationId;
use dashmap::DashMap;
use itertools::Itertools;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::RwLock;

const NEIGHBOUR_COUNT: usize = 2;
const MIN_PENDING_OPS: usize = 10;
const PENDING_OP_TOLERANCE_RATIO: f64 = 0.1;

/// Some reproducible xorname derived from the operation. Which can be re-derived from the appropriate response when received (to remove from tracking)
type NodeIdentifier = XorName;

#[derive(Clone, Debug)]
pub(crate) struct Liveness {
    /// One of (potentially many) different ways of assessing unresponsiveness of nodes.
    unfulfilled_requests: Arc<DashMap<NodeIdentifier, Arc<RwLock<Vec<OperationId>>>>>,
    closest_nodes_to: Arc<DashMap<XorName, Vec<XorName>>>,
}

impl Liveness {
    pub(crate) fn new() -> Self {
        Self {
            unfulfilled_requests: Arc::new(DashMap::new()),
            closest_nodes_to: Arc::new(DashMap::new()),
        }
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
        let all_keys: Vec<_> = self
            .closest_nodes_to
            .iter()
            .map(|entry| *entry.key())
            .collect();

        for key in &all_keys {
            if !current_members.contains(key) {
                let _ = self.unfulfilled_requests.remove(key);
                let _ = self.closest_nodes_to.remove(key);
            }
        }

        self.recompute_closest_nodes();
    }

    /// Removes a pending_operation from the node liveness records
    pub(crate) async fn request_operation_fulfilled(
        &self,
        node_id: &NodeIdentifier,
        operation_id: OperationId,
    ) {
        trace!(
            "Attempting to remove pending_operation {:?} op: {:?}",
            node_id,
            operation_id
        );

        if let Some(entry) = self.unfulfilled_requests.get(node_id) {
            let v = entry.value();
            let mut has_removed = false;

            // only remove the first instance from the vec
            v.write().await.retain(|x| {
                if has_removed || x != &operation_id {
                    true
                } else {
                    has_removed = true;
                    false
                }
            });
            trace!(
                "Pending operation removed for node: {:?} op: {:?}",
                node_id,
                operation_id
            );
        }
    }

    pub(crate) fn recompute_closest_nodes(&self) {
        let all_known_nodes: Vec<_> = self
            .closest_nodes_to
            .iter()
            .map(|entry| *entry.key())
            .collect();

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

    // this is not an exact definition, thus has tolerance for variance due to concurrency
    pub(crate) async fn find_unresponsive_nodes(&self) -> Vec<(XorName, usize)> {
        let mut unresponsive_nodes = Vec::new();
        for entry in self.closest_nodes_to.iter() {
            let (node, neighbours) = entry.pair();

            let node = *node;
            let mut max_pending_by_neighbours = 0;
            // if let Some(max_pending_by_neighbours) =
            for neighbour in neighbours.iter() {
                if let Some(entry) = self.unfulfilled_requests.get(neighbour) {
                    // let (k,v) = entry.pair();
                    let val = entry.value().read().await.len();

                    if val > max_pending_by_neighbours {
                        max_pending_by_neighbours = val
                    }
                }
            }

            let pending_operations_count = if let Some(entry) = self.unfulfilled_requests.get(&node)
            {
                // let (k,v) = entry.pair();
                entry.value().read().await.len()
            } else {
                0
            };

            if pending_operations_count > MIN_PENDING_OPS
                && max_pending_by_neighbours > MIN_PENDING_OPS
                && pending_operations_count as f64 * PENDING_OP_TOLERANCE_RATIO
                    > max_pending_by_neighbours as f64
            {
                tracing::info!(
                    "Pending ops for {}: {} Neighbour max: {}",
                    node,
                    pending_operations_count,
                    max_pending_by_neighbours
                );
                unresponsive_nodes.push((node, pending_operations_count));
            }
        }
        unresponsive_nodes
    }
}
