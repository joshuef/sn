// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{
    flow_ctrl::{cmds::Cmd, ReadOnlyProcessingEvent, RejoinReason},
    Error, MyNode, STANDARD_CHANNEL_SIZE,
};

use sn_interface::types::{DataAddress, NodeId};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Takes care of spawning a new task for the processing of a cmd,
/// collecting resulting cmds from it, and sending it back to the calling context,
/// all the while logging the correlation between incoming and resulting cmds.
pub(crate) struct CmdCtrl {
    id_counter: Arc<AtomicUsize>,
    data_replication_sender: Sender<(Vec<DataAddress>, NodeId)>,
}

impl CmdCtrl {
    pub(crate) fn new() -> (Self, Receiver<(Vec<DataAddress>, NodeId)>) {
        #[cfg(feature = "statemap")]
        sn_interface::statemap::log_metadata();
        let (data_replication_sender, data_replication_receiver) = channel(STANDARD_CHANNEL_SIZE);

        (
            Self {
                id_counter: Arc::new(AtomicUsize::new(0)),
                data_replication_sender,
            },
            data_replication_receiver,
        )
    }

    /// Processes the passed in cmd on a new task
    pub(crate) async fn process_cmd_job(
        &self,
        node: &mut MyNode,
        cmd: Cmd,
        mut id: Vec<usize>,
        cmd_process_api: Sender<ReadOnlyProcessingEvent>,
        // cmd_process_api: Sender<(Cmd, Vec<usize>)>,
        rejoin_network_sender: Sender<RejoinReason>,
    ) {
        let node_identifier = node.info().name();

        if id.is_empty() {
            id.push(self.id_counter.fetch_add(1, Ordering::SeqCst));
        }

        trace!("Processing for {cmd:?}, id: {id:?}");

        // TODO: move this somewhere neater
        if node.data_replication_sender.is_none() {
            node.data_replication_sender = Some(self.data_replication_sender.clone());
        }

        #[cfg(feature = "statemap")]
        sn_interface::statemap::log_state(node_identifier.to_string(), cmd.statemap_state());

        match MyNode::process_cmd(cmd, node).await {
            Ok(cmds) => {
                let _handle = tokio::task::spawn(async move {
                    for (child_nr, cmd) in cmds.into_iter().enumerate() {
                        // zero based, first child of first cmd => [0, 0], second child => [0, 1], first child of second child => [0, 1, 0]
                        let child_id = [id.clone(), [child_nr].to_vec()].concat();

                        // match cmd_process_api.send((cmd, child_id)).await {
                        match cmd_process_api
                            .send(ReadOnlyProcessingEvent::Cmd(cmd))
                            .await
                        {
                            Ok(_) => (), // no issues
                            Err(error) => {
                                let child_id = [id.clone(), [child_nr].to_vec()].concat();
                                error!(
                                    "Could not enqueue child cmd with id: {child_id:?}: {error:?}",
                                );
                            }
                        }
                    }
                });
            }
            Err(error) => {
                warn!("Error when processing cmd: {:?}", error);
                if let Error::RejoinRequired(reason) = error {
                    if rejoin_network_sender.send(reason).await.is_err() {
                        error!("Could not send rejoin reason through channel.");
                    }
                }
            }
        }

        #[cfg(feature = "statemap")]
        sn_interface::statemap::log_state(
            node_identifier.to_string(),
            sn_interface::statemap::State::Idle,
        );
    }
}
