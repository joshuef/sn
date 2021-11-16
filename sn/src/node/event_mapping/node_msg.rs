// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Mapping, MsgContext};
use crate::messaging::{
    system::{NodeCmd, SystemMsg},
    DstLocation, MessageId, SrcLocation,
};
use crate::node::{
    error::convert_to_error_message,
    node_ops::{MsgType, NodeDuty, OutgoingMsg},
    Error,
};
use crate::routing::MessageReceived;
use tracing::debug;

pub(super) fn map_node_msg(
    msg_id: MessageId,
    src: SrcLocation,
    dst: DstLocation,
    msg: MessageReceived,
) -> Mapping {
    debug!(
        "Handling Node message received event with id {:?}: {:?}",
        msg_id, msg
    );

    match dst {
        DstLocation::Section { .. } | DstLocation::Node { .. } => Mapping {
            op: match_node_msg(msg.clone()),
            ctx: Some(MsgContext::Node { msg, src }),
        },
        _ => {
            let error = convert_to_error_message(Error::InvalidMessage(
                msg_id,
                format!("Invalid dst: {:?}", msg),
            ));

            if let SrcLocation::EndUser(_) = src {
                tracing::error!("Logic error! EndUser cannot send NodeMsgs. ({:?})", msg);
                return Mapping {
                    op: NodeDuty::NoOp,
                    ctx: None,
                };
            }

            Mapping {
                op: NodeDuty::Send(OutgoingMsg {
                    msg: MsgType::Node(SystemMsg::NodeMsgError {
                        error,
                        correlation_id: msg_id,
                    }),
                    dst: src.to_dst(),
                    aggregation: false,
                }),
                ctx: Some(MsgContext::Node { msg, src }),
            }
        }
    }
}

fn match_node_msg(msg: MessageReceived) -> NodeDuty {
    match msg {
        //
        // ------ system cmd ------
        MessageReceived::NodeCmd(NodeCmd::RecordStorageLevel { node_id, level, .. }) => {
            NodeDuty::SetStorageLevel { node_id, level }
        }
        _ => {
            error!("Unexpected message received at the node (should probably be handled in routing: {:?}" , msg);
            NodeDuty::NoOp
        }
    }
}
