// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::flow_ctrl::FlowCtrl;
use crate::node::STANDARD_CHANNEL_SIZE;
use sn_dysfunction::{DysfunctionDetection, IssueType};
use std::collections::BTreeSet;
use tokio::sync::mpsc::{self, Receiver, Sender};
use xor_name::XorName;

pub(crate) struct DysfunctionChannels {
    pub(crate) cmds_sender: Sender<DysCmds>,
    pub(crate) dys_nodes_receiver: Receiver<BTreeSet<XorName>>,
}

/// Set of Cmds to interact with the `DysfunctionDetection` module
pub(crate) enum DysCmds {
    AddNode(XorName),
    RetainNodes(BTreeSet<XorName>),
    TrackIssue(XorName, IssueType),
    UntrackIssue(XorName, IssueType),
    GetDysfunctionalNodes,
}

impl FlowCtrl {
    /// Spawns a tokio task that listens for the `DysCmds` and processes them
    pub(crate) fn start_dysfunction_detection(
        mut dysfunction: DysfunctionDetection,
        mut dys_cmds_from_node: Receiver<DysCmds>,
    ) -> Receiver<BTreeSet<XorName>> {
        let (dys_nodes_sender, dys_nodes_receiver) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let _ = tokio::task::spawn(async move {
            while let Some(cmd) = dys_cmds_from_node.recv().await {
                match cmd {
                    DysCmds::AddNode(node) => dysfunction.add_new_node(node),
                    DysCmds::RetainNodes(nodes) => dysfunction.retain_members_only(nodes),
                    DysCmds::TrackIssue(node, issue) => dysfunction.track_issue(node, issue),
                    DysCmds::UntrackIssue(node, issue) => {
                        debug!("Attempting to remove {issue:?} from {node:?}");
                        match issue {
                            IssueType::AeProbeMsg => dysfunction.ae_update_msg_received(&node),
                            IssueType::Dkg => dysfunction.dkg_ack_fulfilled(&node),
                            IssueType::RequestOperation(op_id) => {
                                let _ = dysfunction.request_operation_fulfilled(&node, op_id);
                            }
                            _ => {}
                        };
                    }
                    DysCmds::GetDysfunctionalNodes => {
                        if let Err(error) = dys_nodes_sender
                            .send(dysfunction.get_dysfunctional_nodes())
                            .await
                        {
                            error!("Could not send dysfunctional nodes through the mpsc channel: {error:?}");
                        }
                    }
                }
            }
        });

        dys_nodes_receiver
    }

    /// returns names that are relatively dysfunctional
    pub(crate) async fn get_dysfunctional_node_names(&mut self) -> BTreeSet<XorName> {
        // send a DysCmd asking for the dysfunctional nodes
        if let Err(error) = self
            .dysfunction_channels
            .cmds_sender
            .send(DysCmds::GetDysfunctionalNodes)
            .await
        {
            warn!("Could not send DysCmds through dysfunctional_cmds_tx: {error}");
            BTreeSet::new()
        } else {
            // read the rx channel to get the dysfunctional nodes
            if let Some(dysfunctional_nodes) =
                self.dysfunction_channels.dys_nodes_receiver.recv().await
            {
                dysfunctional_nodes
            } else {
                error!("dysfunctional_nodes_rx channel closed?");
                BTreeSet::new()
            }
        }
    }
}
