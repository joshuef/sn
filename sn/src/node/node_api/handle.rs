// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    messaging::{send, send_error, send_to_nodes},
    role::{AdultRole, ElderRole, Role},
    Node,
};
use crate::node::{
    event_mapping::MsgContext,
    node_ops::{NodeDuties, NodeDuty},
    Result,
};
use crate::routing::ELDER_SIZE;
use tokio::task::JoinHandle;
use tracing::{debug, info};

#[derive(Debug)]
pub(super) enum NodeTask {
    None,
    Result(Box<(NodeDuties, Option<MsgContext>)>),
    Thread(JoinHandle<Result<NodeTask>>),
}

impl From<NodeDuties> for NodeTask {
    fn from(duties: NodeDuties) -> Self {
        Self::Result(Box::new((duties, None)))
    }
}

impl Node {
    ///
    pub(super) async fn handle(&self, duty: NodeDuty) -> Result<NodeTask> {
        if !matches!(duty, NodeDuty::NoOp) {
            debug!("Handling NodeDuty: {:?}", duty);
        }

        match duty {
            NodeDuty::Genesis => {
                self.level_up().await?;
                let elder = self.as_elder().await?;
                *elder.received_initial_sync.write().await = true;
                Ok(NodeTask::None)
            }
            NodeDuty::EldersChanged { newbie, .. } => {
                if newbie {
                    info!("Promoted to Elder on Churn");
                    self.level_up().await?;
                    if self.network_api.our_prefix().await.is_empty()
                        && self.network_api.section_chain().await.len() <= ELDER_SIZE
                    {
                        let elder = self.as_elder().await?;
                        *elder.received_initial_sync.write().await = true;
                    }
                }

                Ok(NodeTask::None)
            }
            NodeDuty::AdultsChanged {
                added,
                removed,
                remaining,
            } => {
                let our_name = self.our_name().await;
                let adult_role = self.as_adult().await?;
                let handle = tokio::spawn(async move {
                    Ok(NodeTask::from(
                        adult_role
                            .reorganize_chunks(our_name, added, removed, remaining)
                            .await?,
                    ))
                });
                Ok(NodeTask::Thread(handle))
            }
            NodeDuty::SectionSplit {
                our_key, newbie, ..
            } => {
                if newbie {
                    info!("Beginning split as Newbie");
                    self.begin_split_as_newbie(our_key).await?;
                }

                Ok(NodeTask::None)
            }
            //
            // ---------- Levelling --------------
            NodeDuty::LevelDown => {
                *self.role.write().await = Role::Adult(AdultRole {
                    network_api: self.network_api.clone(),
                });
                Ok(NodeTask::None)
            }
            //
            // ------- Misc ------------
            NodeDuty::Send(msg) => {
                let network_api = self.network_api.clone();
                let handle = tokio::spawn(async move {
                    send(msg, &network_api).await?;
                    Ok(NodeTask::None)
                });
                Ok(NodeTask::Thread(handle))
            }
            NodeDuty::SendError(msg) => {
                let network_api = self.network_api.clone();
                let handle = tokio::spawn(async move {
                    send_error(msg, &network_api).await?;
                    Ok(NodeTask::None)
                });
                Ok(NodeTask::Thread(handle))
            }
            NodeDuty::SendToNodes {
                msg_id,
                msg,
                targets,
                aggregation,
            } => {
                let network_api = self.network_api.clone();
                let handle = tokio::spawn(async move {
                    send_to_nodes(msg_id, msg, targets, aggregation, &network_api).await?;
                    Ok(NodeTask::None)
                });
                Ok(NodeTask::Thread(handle))
            }
            NodeDuty::SetNodeJoinsAllowed(joins_allowed) => {
                let mut network_api = self.network_api.clone();
                let handle = tokio::spawn(async move {
                    network_api
                        .set_joins_allowed(cfg!(feature = "always-joinable") || joins_allowed)
                        .await?;
                    Ok(NodeTask::None)
                });
                Ok(NodeTask::Thread(handle))
            }
            NodeDuty::NoOp => Ok(NodeTask::None),
        }
    }

    async fn as_adult(&self) -> Result<AdultRole> {
        let role = self.role.read().await;
        Ok(role.as_adult()?.clone())
    }

    async fn as_elder(&self) -> Result<ElderRole> {
        let role = self.role.read().await;
        Ok(role.as_elder()?.clone())
    }
}
