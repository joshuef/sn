// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Session;
use crate::client::connections::messaging::NUM_OF_ELDERS_SUBSET_FOR_QUERIES;
use crate::client::{connections::messaging::send_message, Error};
use crate::messaging::data::DataCmd;
use crate::messaging::{
    data::{CmdError, ServiceMsg},
    system::{KeyedSig, SectionAuth, SystemMsg},
    DstLocation, MessageId, MessageType, MsgKind, SectionAuthorityProvider, WireMsg,
};
use crate::routing::ELDER_SIZE;
use crate::types::PublicKey;
use bytes::Bytes;
use itertools::Itertools;
use qp2p::IncomingMessages;
use secured_linked_list::SecuredLinkedList;
use std::net::SocketAddr;
use xor_name::XorName;

impl Session {
    // Listen for incoming messages on a connection
    pub(crate) async fn spawn_message_listener_thread(
        mut session: Session,
        mut incoming_messages: IncomingMessages,
    ) {
        debug!("Listening for incoming messages");
        let _ = tokio::spawn(async move {
            loop {
                session = match Self::get_incoming_message(&mut incoming_messages).await {
                    Ok((src, msg)) => match Self::handle_msg(msg, src, session.clone()).await {
                        Ok(session) => session,
                        Err(err) => {
                            error!("Error while processing incoming message: {:?}. Listening for next message...", err);
                            session
                        }
                    },
                    Err(Error::Generic(_)) => {
                        // TODO: FIX error type
                        info!("IncomingMessages listener has closed.");
                        break;
                    }
                    Err(err) => {
                        error!("Error while getting incoming message: {:?}. Listening for next message...", err);
                        session
                    }
                }
            }
        });
    }

    pub(crate) async fn get_incoming_message(
        incoming_messages: &mut IncomingMessages,
    ) -> Result<(SocketAddr, MessageType), Error> {
        if let Some((src, message)) = incoming_messages.next().await {
            let msg_type = WireMsg::deserialize(message)?;
            trace!("Incoming message from {:?}", &src);
            Ok((src, msg_type))
        } else {
            Err(Error::Generic("Nothing..".to_string())) // TODO: FIX error type
        }
    }

    pub(crate) async fn handle_msg(
        msg: MessageType,
        src: SocketAddr,
        session: Session,
    ) -> Result<Session, Error> {
        match msg {
            MessageType::Service { msg_id, msg, .. } => {
                Self::handle_client_msg(session, msg_id, msg, src).await
            }
            MessageType::System {
                msg:
                    SystemMsg::AntiEntropyRedirect {
                        section_auth,
                        section_signed,
                        bounced_msg,
                    },
                ..
            } => {
                let result = Self::handle_ae_redirect_msg(
                    session,
                    section_auth,
                    section_signed,
                    bounced_msg,
                    src,
                )
                .await;
                if result.is_err() {
                    warn!("Failed to handle AE-Redirect");
                }
                result
            }
            MessageType::System {
                msg:
                    SystemMsg::AntiEntropyRetry {
                        section_auth,
                        section_signed,
                        bounced_msg,
                        proof_chain,
                    },
                ..
            } => {
                let result = Self::handle_ae_retry_msg(
                    session,
                    section_auth,
                    section_signed,
                    bounced_msg,
                    proof_chain,
                )
                .await;
                if result.is_err() {
                    warn!("Failed to handle AE-Retry msg");
                }
                result
            }
            msg_type => {
                warn!("Unexpected message type received: {:?}", msg_type);
                Ok(session)
            }
        }
    }

    // Handle messages intended for client consumption (re: queries + commands)
    async fn handle_client_msg(
        session: Session,
        msg_id: MessageId,
        msg: ServiceMsg,
        src: SocketAddr,
    ) -> Result<Session, Error> {
        debug!("ServiceMsg with id {:?} received from {:?}", msg_id, src);
        let queries = session.pending_queries.clone();
        let error_sender = session.incoming_err_sender.clone();

        let _ = tokio::spawn(async move {
            match msg {
                ServiceMsg::QueryResponse { response, .. } => {
                    // Note that this doesn't remove the sender from here since multiple
                    // responses corresponding to the same message ID might arrive.
                    // Once we are satisfied with the response this is channel is discarded in
                    // ConnectionManager::send_query

                    if let Ok(op_id) = response.operation_id() {
                        if let Some(sender) = &queries.read().await.get(&op_id) {
                            trace!("Sending response for query w/{} via channel.", op_id);
                            let _ = sender.send(response).await;
                        } else {
                            // TODO: The trace is only needed when we have an identified case of not finding a channel, but expecting one.
                            // When expecting one, we can log "No channel found for operation", (and then probably at warn or error level).
                            // But when we have received enough responses, we aren't really expecting a channel there, so there is no reason to log anything.
                            // Right now, if we have already received enough responses for a query,
                            // we drop the channels and drop any further responses for that query.
                            // but we should not drop it immediately, but clean it up after a while
                            // and then not log that "no channel was found" when we already had enough responses.
                            //trace!("No channel found for operation {}", op_id);
                        }
                    } else {
                        warn!("Ignoring query response without operation id");
                    }
                }
                ServiceMsg::CmdError {
                    error,
                    correlation_id,
                    ..
                } => {
                    debug!(
                        "CmdError was received for Message w/ID: {:?}, sending on error channel",
                        correlation_id
                    );
                    warn!("CmdError received is: {:?}", error);
                    let _ = error_sender.send(error.clone()).await;

                    match error {
                        CmdError::Data(_error) => {
                            // do nothing just yet
                        }
                    }
                }
                msg => {
                    warn!("Ignoring unexpected message type received: {:?}", msg);
                }
            };
        });

        Ok(session)
    }

    // Handle Anti-Entropy Redirect messages
    async fn handle_ae_redirect_msg(
        session: Session,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        bounced_msg: Bytes,
        sender: SocketAddr,
    ) -> Result<Session, Error> {
        // Check if SAP signature is valid
        if !bincode::serialize(&section_auth)
            .map(|bytes| section_signed.verify(&bytes))
            .unwrap_or(false)
        {
            warn!(
                "Signature returned with SAP in AE-Redirect response is invalid: {:?}",
                section_auth
            );
            return Ok(session);
        }

    // Remove expired items from ae_cache before checking.
        // It might be late to not retry now.
        session.ae_cache.remove_expired().await;

        // Deserialize the bounced message for resending
        let (msg_id, service_msg, mut dst_location, auth): (_, ServiceMsg, _, _) =
            match WireMsg::deserialize(bounced_msg)? {
                MessageType::Service {
                    msg_id,
                    msg,
                    auth,
                    dst_location,
                } => (msg_id, msg, dst_location, auth),
                other => {
                    warn!(
                        "Unexpected non-serviceMsg returned in AE response: {:?}",
                        other
                    );
                    return Ok(session);
                }
            };

        let (targets, dst_address_of_bounced_msg) = match &service_msg {
            ServiceMsg::Cmd(cmd) => {
                match &cmd {
                    DataCmd::StoreChunk(_) => (3, cmd.dst_name()), // stored at Adults, so only 1 correctly functioning Elder need to relay
                    DataCmd::Register(_) => (7, cmd.dst_name()), // only stored at Elders, all need a copy
                }
            }
            ServiceMsg::Query(query) => (NUM_OF_ELDERS_SUBSET_FOR_QUERIES, query.dst_name()),
            _ => {
                warn!(
                    "Bounced message ({:?}) received in AE response: {:?} is of invalid type",
                    msg_id, service_msg
                );
                return Ok(session);
            }
        };

        if let Some(old_elders) = session.ae_cache.get(&dst_address_of_bounced_msg).await {
            let received_elders = section_auth
                .elders
                .values()
                .cloned()
                .collect::<Vec<SocketAddr>>();
            if old_elders == received_elders {
                warn!("We have already resent this message on a AE-Retry. Dropping this instance");
                return Ok(session);
            }
        }

        debug!(
            "Received AE-Redirect for {:?}, from {}, with SAP: {:?}",
            msg_id, sender, section_auth
        );

        debug!(
            "Bounced message ({:?}) received in AE-Redirect response: {:?}",
            msg_id, service_msg
        );


        //  // Update our network knowledge making sure proof chain
        // // validates the new SAP based on currently known remote section SAP.
        // match session.network.update(
        //     SectionAuth {
        //         value: section_auth.clone(),
        //         sig: section_signed,
        //     },
        //     &proof_chain,
        // ) {
        //     Ok(updated) => {
        //         if updated {
        //             debug!(
        //                 "Anti-Entropy: updated remote section SAP updated for {:?}",
        //                 section_auth.prefix
        //             );
        //         } else {
        //             debug!(
        //                 "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
        //                 section_auth.prefix, section_auth
        //             );
        //         }
        //     }
        //     Err(err) => {
        //         warn!(
        //             "Anti-Entropy: failed to update remote section SAP, bounced msg dropped: {:?}",
        //             err
        //         );
        //         return Ok(session);
        //     }
        // }


        let payload = WireMsg::serialize_msg_payload(&service_msg)?;

        // // TODO: we cannot trust these Elders belong to the network we are intended
        // // to connect to (based on the genesis key we know). We could send the genesis key
        // // as the destination section key and that should cause an AE-Retry response,
        // // which we could use to verify the SAP we receive an trust.
        // let elders = section_auth
        //     .elders
        //     .values()
        //     .cloned()
        //     .take(targets)
        //     .collect::<Vec<SocketAddr>>();
        // let section_pk = section_auth.public_key_set.public_key();


        // Let's rebuild the message with the updated destination details
        let elders = section_auth
            .elders
            .into_iter()
            .sorted_by(|(lhs_name, _), (rhs_name, _)| {
                dst_address_of_bounced_msg.cmp_distance(lhs_name, rhs_name)
            })
            .map(|(_, addr)| addr)
            .take(targets)
            .collect::<Vec<SocketAddr>>();

            dst_location.set_section_pk(section_auth.public_key_set.public_key());

            let wire_msg = WireMsg::new_msg(
                msg_id,
                payload,
                MsgKind::ServiceMsg(auth.into_inner()),
                dst_location,
            )?;
    
            send_message(elders.clone(), wire_msg, session.endpoint.clone(), msg_id).await?;
            if let Some(old_elders) = session
                .ae_cache
                .set(dst_address_of_bounced_msg, elders.clone(), None)
                .await
            {
                warn!("We have already sent this message to Elders {:?} Updating cache with latest elders {:?}", old_elders, &elders);
            }
        Ok(session)
    }

    // Handle Anti-Entropy Retry messages
    async fn handle_ae_retry_msg(
        session: Session,
        section_auth: SectionAuthorityProvider,
        section_signed: KeyedSig,
        bounced_msg: Bytes,
        proof_chain: SecuredLinkedList,
    ) -> Result<Session, Error> {
        // Remove expired items from ae_cache before checking.
        // It might be late to not retry now.
        session.ae_cache.remove_expired().await;

        // Deserialize the bounced message for resending
        let (msg_id, service_msg, mut dst_location, auth): (_, ServiceMsg, _, _) =
            match WireMsg::deserialize(bounced_msg)? {
                MessageType::Service {
                    msg_id,
                    msg,
                    auth,
                    dst_location,
                } => (msg_id, msg, dst_location, auth),
                other => {
                    warn!(
                        "Unexpected non-serviceMsg returned in AE response: {:?}",
                        other
                    );
                    return Ok(session);
                }
            };

        let (targets, dst_address_of_bounced_msg) = match &service_msg {
            ServiceMsg::Cmd(cmd) => {
                match &cmd {
                    DataCmd::StoreChunk(_) => (3, cmd.dst_name()), // stored at Adults, so only 1 correctly functioning Elder need to relay
                    DataCmd::Register(_) => (7, cmd.dst_name()), // only stored at Elders, all need a copy
                }
            }
            ServiceMsg::Query(query) => (NUM_OF_ELDERS_SUBSET_FOR_QUERIES, query.dst_name()),
            _ => {
                warn!(
                    "Bounced message ({:?}) received in AE response: {:?} is of invalid type",
                    msg_id, service_msg
                );
                return Ok(session);
            }
        };

        if let Some(old_elders) = session.ae_cache.get(&dst_address_of_bounced_msg).await {
            let received_elders = section_auth
                .elders
                .values()
                .cloned()
                .collect::<Vec<SocketAddr>>();
            if old_elders == received_elders {
                debug!("We have already resent this message on a AE-Retry. Dropping this instance");
                return Ok(session);
            }
        }

        debug!(
            "Received AE-Retry for msg_id: {:?} with new SAP: {:?}",
            msg_id, section_auth
        );
        // Update our network knowledge making sure proof chain
        // validates the new SAP based on currently known remote section SAP.
        match session.network.update(
            SectionAuth {
                value: section_auth.clone(),
                sig: section_signed,
            },
            &proof_chain,
        ) {
            Ok(updated) => {
                if updated {
                    debug!(
                        "Anti-Entropy: updated remote section SAP updated for {:?}",
                        section_auth.prefix
                    );
                } else {
                    debug!(
                        "Anti-Entropy: discarded SAP for {:?} since it's the same as the one in our records: {:?}",
                        section_auth.prefix, section_auth
                    );
                }
            }
            Err(err) => {
                warn!(
                    "Anti-Entropy: failed to update remote section SAP, bounced msg dropped: {:?}",
                    err
                );
                return Ok(session);
            }
        }

        debug!(
            "Bounced message ({:?}) received in AE response: {:?}",
            msg_id, service_msg
        );
        
        let payload = WireMsg::serialize_msg_payload(&service_msg)?;

        // Let's rebuild the message with the updated destination details
        let elders = section_auth
            .elders
            .into_iter()
            .sorted_by(|(lhs_name, _), (rhs_name, _)| {
                dst_address_of_bounced_msg.cmp_distance(lhs_name, rhs_name)
            })
            .map(|(_, addr)| addr)
            .take(targets)
            .collect::<Vec<SocketAddr>>();

        dst_location.set_section_pk(section_auth.public_key_set.public_key());

        let wire_msg = WireMsg::new_msg(
            msg_id,
            payload,
            MsgKind::ServiceMsg(auth.into_inner()),
            dst_location,
        )?;

        send_message(elders.clone(), wire_msg, session.endpoint.clone(), msg_id).await?;
        if let Some(old_elders) = session
            .ae_cache
            .set(dst_address_of_bounced_msg, elders.clone(), None)
            .await
        {
            warn!("We have already sent this message to Elders {:?} Updating cache with latest elders {:?}", old_elders, &elders);
        }

        Ok(session)
    }
}
