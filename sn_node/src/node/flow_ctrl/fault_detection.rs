// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::flow_ctrl::FlowCtrl;
use crate::node::STANDARD_CHANNEL_SIZE;
use sn_fault_detection::{FaultDetection, IssueType};
use std::collections::BTreeSet;
use tokio::sync::mpsc::{self, Receiver, Sender};
use xor_name::XorName;

pub(crate) struct FaultChannels {
    pub(crate) cmds_sender: Sender<FaultsCmd>,
    pub(crate) faulty_nodes_receiver: Receiver<Vec<XorName>>,
}

/// Set of cmds to interact with the `FaultDetection` module
pub(crate) enum FaultsCmd {
    AddNode(XorName),
    RetainNodes(BTreeSet<XorName>),
    TrackIssue(XorName, IssueType),
    UntrackIssue(XorName, IssueType),
    GetFaultyNodes,
}

impl FlowCtrl {
    /// Spawns a tokio task that listens for the `FaultsCmd` and processes them
    pub(crate) fn start_fault_detection(
        mut tracker: FaultDetection,
        mut fault_cmds_from_node: Receiver<FaultsCmd>,
    ) -> Receiver<Vec<XorName>> {
        let (fault_nodes_sender, faulty_nodes_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let _ = tokio::task::spawn(async move {
            while let Some(cmd) = fault_cmds_from_node.recv().await {
                match cmd {
                    FaultsCmd::AddNode(node) => tracker.add_new_node(node),
                    FaultsCmd::RetainNodes(nodes) => tracker.retain_members_only(nodes),
                    FaultsCmd::TrackIssue(node, issue) => tracker.track_issue(node, issue),
                    FaultsCmd::UntrackIssue(node, issue) => {
                        debug!("Attempting to remove {issue:?} from {node:?}");
                        match issue {
                            IssueType::AeProbeMsg => tracker.ae_update_msg_received(&node),
                            IssueType::Dkg => tracker.dkg_ack_fulfilled(&node),
                            _ => {}
                        };
                    }
                    FaultsCmd::GetFaultyNodes => {
                        if let Err(error) =
                            fault_nodes_sender.send(tracker.get_faulty_nodes()).await
                        {
                            warn!(
                                "Could not send faulty nodes through the mpsc channel: {error:?}"
                            );
                        }
                    }
                }
            }
        });

        faulty_nodes_receiver
    }

    /// returns names that are relatively faulty
    pub(crate) async fn get_faulty_node_names(&mut self) -> Vec<XorName> {
        // send a FaultCmd asking for the faulty nodes
        if let Err(error) = self
            .fault_channels
            .cmds_sender
            .send(FaultsCmd::GetFaultyNodes)
            .await
        {
            warn!("Could not send FaultsCmd through fault_cmds_tx: {error}");
            vec![]
        } else {
            // read the rx channel to get the faulty nodes
            if let Some(faulty_nodes) = self.fault_channels.faulty_nodes_receiver.recv().await {
                faulty_nodes
            } else {
                warn!("faulty_nodes_rx channel closed?");
                vec![]
            }
        }
    }
}
