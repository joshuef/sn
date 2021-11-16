// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod node_msg;

use crate::messaging::SrcLocation;
use crate::node::{network::Network, node_ops::NodeDuty};
use crate::routing::{Event as RoutingEvent, MessageReceived, NodeElderChange, MIN_AGE};
use crate::types::PublicKey;
use node_msg::map_node_msg;
use std::{thread::sleep, time::Duration};
use tracing::{debug, error, info, trace};

#[derive(Debug)]
pub(super) struct Mapping {
    pub(super) op: NodeDuty,
    pub(super) ctx: Option<MsgContext>,
}

#[derive(Debug, Clone)]
pub(super) enum MsgContext {
    Node {
        msg: MessageReceived,
        src: SrcLocation,
    },
}

// Process any routing event
pub(super) async fn map_routing_event(event: RoutingEvent, network_api: &Network) -> Mapping {
    info!("Handling RoutingEvent: {:?}", event);
    match event {
        RoutingEvent::MessageReceived {
            msg_id,
            src,
            dst,
            msg,
        } => map_node_msg(msg_id, src, dst, *msg),
        RoutingEvent::SectionSplit {
            elders,
            self_status_change,
        } => {
            let newbie = match self_status_change {
                NodeElderChange::None => false,
                NodeElderChange::Promoted => true,
                NodeElderChange::Demoted => {
                    error!("This should be unreachable, as there would be no demotions of Elders during a split.");
                    return Mapping {
                        op: NodeDuty::NoOp,
                        ctx: None,
                    };
                }
            };
            Mapping {
                op: NodeDuty::SectionSplit {
                    our_prefix: elders.prefix,
                    our_key: PublicKey::from(elders.key),
                    newbie,
                },
                ctx: None,
            }
        }
        RoutingEvent::EldersChanged {
            elders,
            self_status_change,
        } => {
            log_network_stats(network_api).await;
            let first_section = network_api.our_prefix().await.is_empty();
            let first_elder = network_api.our_elder_names().await.len() == 1;
            if first_section && first_elder {
                return Mapping {
                    op: NodeDuty::Genesis,
                    ctx: None,
                };
            }

            match self_status_change {
                NodeElderChange::None => {
                    if !network_api.is_elder().await {
                        return Mapping {
                            op: NodeDuty::NoOp,
                            ctx: None,
                        };
                    }
                    // sync to others if we are elder
                    // -- ugly temporary until fixed in routing --
                    let mut sanity_counter = 0_i32;
                    while sanity_counter < 240 {
                        match network_api.our_public_key_set().await {
                            Ok(pk_set) => {
                                if elders.key == pk_set.public_key() {
                                    break;
                                } else {
                                    trace!("******Elders changed, we are still Elder but we seem to be lagging the DKG...");
                                }
                            }
                            Err(e) => {
                                trace!(
                                    "******Elders changed, should NOT be an error here...! ({:?})",
                                    e
                                );
                                sanity_counter += 1;
                            }
                        }
                        sleep(Duration::from_millis(500))
                    }
                    // -- ugly temporary until fixed in routing --

                    trace!("******Elders changed, we are still Elder");
                    Mapping {
                        op: NodeDuty::EldersChanged {
                            our_prefix: elders.prefix,
                            new_elders: elders.added,
                            newbie: false,
                        },
                        ctx: None,
                    }
                }
                NodeElderChange::Promoted => {
                    // -- ugly temporary until fixed in routing --
                    let mut sanity_counter = 0_i32;
                    while network_api.our_public_key_set().await.is_err() {
                        if sanity_counter > 240 {
                            trace!("******Elders changed, we were promoted, but no key share found, so skip this..");
                            return Mapping {
                                op: NodeDuty::NoOp,
                                ctx: None,
                            };
                        }
                        sanity_counter += 1;
                        trace!("******Elders changed, we are promoted, but still no key share..");
                        sleep(Duration::from_millis(500))
                    }
                    // -- ugly temporary until fixed in routing --

                    trace!("******Elders changed, we are promoted");

                    Mapping {
                        op: NodeDuty::EldersChanged {
                            our_prefix: elders.prefix,
                            new_elders: elders.added,
                            newbie: true,
                        },
                        ctx: None,
                    }
                }
                NodeElderChange::Demoted => Mapping {
                    op: NodeDuty::LevelDown,
                    ctx: None,
                },
            }
        }
        RoutingEvent::MemberJoined { previous_name, .. } => {
            log_network_stats(network_api).await;
            let op = if previous_name.is_some() {
                trace!("A relocated node has joined the section.");
                // Switch joins_allowed off a new adult joining.
                NodeDuty::SetNodeJoinsAllowed(false)
            } else if network_api.our_prefix().await.is_empty() {
                NodeDuty::NoOp
            } else {
                NodeDuty::SetNodeJoinsAllowed(false)
            };
            Mapping { op, ctx: None }
        }
        RoutingEvent::Relocated { .. } => {
            // Check our current status
            let age = network_api.age().await;
            if age > MIN_AGE {
                info!("Relocated, our Age: {:?}", age);
            }
            Mapping {
                op: NodeDuty::NoOp,
                ctx: None,
            }
        }
        RoutingEvent::AdultsChanged {
            remaining,
            added,
            removed,
        } => Mapping {
            op: NodeDuty::AdultsChanged {
                remaining,
                added,
                removed,
            },
            ctx: None,
        },
        // Ignore all other events
        _ => Mapping {
            op: NodeDuty::NoOp,
            ctx: None,
        },
    }
}

pub(super) async fn log_network_stats(network_api: &Network) {
    let adults = network_api.our_adults().await.len();
    let elders = network_api.our_elder_names().await.len();
    let prefix = network_api.our_prefix().await;
    debug!("{:?}: {:?} Elders, {:?} Adults.", prefix, elders, adults);
}
