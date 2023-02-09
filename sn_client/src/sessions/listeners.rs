// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{MsgResponse, Session};

use crate::{Error, Result};

use qp2p::{RecvStream, UsrMsgBytes};
use sn_interface::{
    messaging::{
        data::ClientDataResponse, AuthorityProof, ClientAuth, Dst, MsgId, MsgKind, MsgType, WireMsg,
    },
    network_knowledge::SectionTreeUpdate,
    types::{log_markers::LogMarker, Peer},
};

use bytes::Bytes;
use itertools::Itertools;

// Maximum number of times we'll re-send a msg upon receiving an AE response for it
const MAX_AE_RETRIES_TO_ATTEMPT: u8 = 5;

// If the msg was resent due to AE response, we internally pass the information
// about where the msg was resent to, and the bi-stream to read the response on.
struct MsgResent {
    new_peer: Peer,
    new_recv_stream: RecvStream,
}

impl Session {
    #[instrument(skip_all, level = "debug")]
    async fn read_resp_from_recvstream(
        recv_stream: RecvStream,
        peer: Peer,
        correlation_id: MsgId,
    ) -> Result<(MsgId, ClientDataResponse), Error> {
        let recv_stream_id = recv_stream.id();
        let bytes = recv_stream.read().await?;
        match WireMsg::deserialize(bytes)? {
            MsgType::ClientDataResponse { msg_id, msg } => Ok((msg_id, msg)),
            msg => {
                warn!(
                    "Unexpected msg type received on {} from {peer:?} in response \
                    to {correlation_id:?}: {msg:?}",
                    recv_stream_id
                );
                Err(Error::UnexpectedMsgType {
                    correlation_id,
                    peer,
                    msg,
                })
            }
        }
        // } else {
        //     Err(Error::ResponseStreamClosed {
        //         msg_id: correlation_id,
        //         peer,
        //     })
        // }
    }

    // Wait for a msg response incoming on the provided RecvStream
    #[instrument(skip_all, level = "debug")]
    pub(crate) async fn recv_stream_listener(
        &self,
        correlation_id: MsgId,
        mut peer: Peer,
        peer_index: usize,
        mut recv_stream: RecvStream,
    ) -> MsgResponse {
        // Unless we receive AntiEntropy responses, which require re-sending the
        // message, the first msg received is the response we expect and return
        let mut attempt = 0;
        let stream_id = recv_stream.id();
        let result = loop {
            let addr = peer.addr();
            if attempt > MAX_AE_RETRIES_TO_ATTEMPT {
                break MsgResponse::Failure(
                    addr,
                    Error::AntiEntropyMaxRetries {
                        msg_id: correlation_id,
                        retries: attempt - 1,
                    },
                );
            }

            debug!("Waiting for response msg on {stream_id} from {peer:?} @ index: {peer_index} for {correlation_id:?}, attempt #{attempt}");
            let (msg_id, resp_msg) =
                match Self::read_resp_from_recvstream(recv_stream, peer, correlation_id).await
                {
                    Ok(resp_info) => resp_info,
                    Err(err) => break MsgResponse::Failure(addr, err),
                };

            match resp_msg {
                ClientDataResponse::NetworkIssue(err) => {
                    break MsgResponse::Failure(
                        peer.addr(),
                        Error::CmdError {
                            source: err,
                            msg_id: correlation_id,
                        },
                    )
                }
                ClientDataResponse::QueryResponse {
                    response,
                    correlation_id,
                } => {
                    trace!(
                        "QueryResponse with id {msg_id:?} regarding correlation_id \
                        {correlation_id:?} from {peer:?} with response: {response:?}"
                    );
                    break MsgResponse::QueryResponse(addr, Box::new(response));
                }
                ClientDataResponse::CmdResponse {
                    response,
                    correlation_id,
                } => {
                    trace!(
                        "CmdResponse with id {msg_id:?} regarding correlation_id \
                        {correlation_id:?} from {peer:?} with response {response:?}"
                    );
                    break MsgResponse::CmdResponse(addr, Box::new(response));
                }
                ClientDataResponse::AntiEntropy {
                    section_tree_update,
                    bounced_msg,
                } => {
                    debug!(
                        "AntiEntropy msg with id {msg_id:?} received for {correlation_id:?} \
                        from {peer:?}@{peer_index}"
                    );

                    let ae_resp_outcome = self
                        .handle_ae_msg(
                            section_tree_update,
                            bounced_msg,
                            peer,
                            peer_index,
                            correlation_id,
                        )
                        .await;

                    match ae_resp_outcome {
                        Err(err) => break MsgResponse::Failure(addr, err),
                        Ok(MsgResent {
                            new_peer,
                            new_recv_stream,
                        }) => {
                            recv_stream = new_recv_stream;
                            trace!(
                                "{} of correlation {correlation_id:?} to {} on {stream_id}",
                                LogMarker::ReceiveCompleted,
                                addr,
                            );
                            peer = new_peer;
                            attempt += 1;
                            continue;
                        }
                    }
                }
            }
        };

        trace!(
            "{} of correlation {correlation_id:?} to {}, on {}, with {result:?}",
            LogMarker::ReceiveCompleted,
            peer.addr(),
            stream_id
        );
        result
    }

    // Handle Anti-Entropy Redirect or Retry msgs
    #[instrument(skip_all, level = "debug")]
    async fn handle_ae_msg(
        &self,
        section_tree_update: SectionTreeUpdate,
        bounced_msg: UsrMsgBytes,
        src_peer: Peer,
        src_peer_index: usize,
        correlation_id: MsgId,
    ) -> Result<MsgResent> {
        let target_sap = section_tree_update.signed_sap.value.clone();
        debug!(
            "Received Anti-Entropy msg from {src_peer}@{src_peer_index}, with SAP: {target_sap:?}"
        );

        // Try to update our network knowledge first
        self.update_network_knowledge(section_tree_update, src_peer)
            .await;

        let (msg_id, elders, query_index, payload, dst, auth) = self
            .new_target_elders(src_peer, bounced_msg, correlation_id)
            .await?;

        // The actual order of Elders doesn't really matter. All that matters is we pass each AE response
        // we get through the same hoops, to then be able to ping a new Elder on a 1-1 basis for the src_peer
        // we initially targetted.
        let ordered_elders: Vec<_> = elders
            .into_iter()
            .sorted_by(|lhs, rhs| dst.name.cmp_distance(&lhs.name(), &rhs.name()))
            .collect();

        // We send this to only one elder for each AE message we get in. We _should_ have one per elder we sent to,
        // deterministically sent to closest elder based upon the initial sender index
        let target_elder = ordered_elders.get(src_peer_index);

        // there should always be one
        if let Some(elder) = target_elder {
            let wire_msg = WireMsg::new_msg(
                msg_id,
                payload,
                MsgKind::Client {
                    auth: auth.into_inner(),
                    is_spend: false,
                    query_index,
                },
                dst,
            );
            let bytes = wire_msg.serialize()?;

            debug!("{msg_id:?} AE bounced msg going out again. Resending original message (sent to index {src_peer_index:?} peer: {src_peer:?}) to new section elder {elder:?}");

            let link = self
                .peer_links
                .get_or_create_link(elder, false, Some(correlation_id))
                .await;
            let new_recv_stream = link
                .send_bi(bytes, msg_id)
                .await
                .map_err(|error| Error::FailedToInitateBiDiStream { msg_id, error })?;

            Ok(MsgResent {
                new_peer: *elder,
                new_recv_stream,
            })
        } else {
            Err(Error::AntiEntropyNoSapElders)
        }
    }

    /// Update our network knowledge making sure proof chain validates the
    /// new SAP based on currently known remote section SAP or genesis key.
    async fn update_network_knowledge(
        &self,
        section_tree_update: SectionTreeUpdate,
        src_peer: Peer,
    ) {
        debug!("Attempting to update our network knowledge...");
        let sap = section_tree_update.signed_sap.value.clone();
        let prefix = sap.prefix();
        let mut network = self.network.write().await;
        debug!("Attempting to update our network knowledge... WRITE LOCK GOT");
        // Update our network SectionTree based upon passed in knowledge
        match network.update_the_section_tree(section_tree_update) {
            Ok(true) => {
                debug!("Anti-Entropy: updated remote section SAP for {prefix:?} to {sap:?}");
            }
            Ok(false) => {
                debug!(
                    "Anti-Entropy: discarded SAP for {prefix:?} since it's the same as \
                    the one in our records: {sap:?}",
                );
            }
            Err(err) => {
                warn!(
                    "Anti-Entropy: failed to update remote section SAP and DAG \
                    sent by: {src_peer:?}, section key: {:?}, w/ err: {err:?}",
                    sap.section_key()
                );
            }
        }
    }

    /// Finds new target elders based on current network knowledge
    /// (to be used after applying a new SectionTreeUpdate)
    #[instrument(skip_all, level = "debug")]
    #[allow(clippy::type_complexity)]
    async fn new_target_elders(
        &self,
        src_peer: Peer,
        bounced_msg: UsrMsgBytes,
        correlation_id: MsgId,
    ) -> Result<
        (
            MsgId,
            Vec<Peer>,
            Option<usize>,
            Bytes,
            Dst,
            AuthorityProof<ClientAuth>,
        ),
        Error,
    > {
        let wire_msg = WireMsg::from(bounced_msg)?;
        let msg_kind = wire_msg.kind();
        let msg_type = wire_msg.into_msg()?;
        let query_index = *msg_kind.query_index();
        let (msg_id, client_msg, bounced_msg_dst, auth) = match msg_type {
            MsgType::Client {
                msg_id,
                msg,
                dst,
                auth,
            } => (msg_id, msg, dst, auth),
            msg => {
                warn!("Unexpected bounced msg received in AE response: {msg:?}");
                return Err(Error::UnexpectedMsgType {
                    correlation_id,
                    peer: src_peer,
                    msg,
                });
            }
        };

        trace!(
            "Bounced msg {msg_id:?} received in an AE response: {client_msg:?} from {src_peer:?}"
        );

        let knowlege = self.network.read().await;

        // Get the best sap we know of now.
        // We don't just rely on the returned SAP, as we should be updating the knowledge if it's valid, before we get here.
        let best_sap = knowlege
            .closest(&bounced_msg_dst.name, None)
            .ok_or(Error::NoCloseSapFound(bounced_msg_dst.name))?;

        trace!("{msg_id:?} from  {src_peer:?}. New SAP of for bounced msg: {best_sap:?}");

        let target_elders = best_sap.elders_vec();
        if target_elders.is_empty() {
            Err(Error::AntiEntropyNoSapElders)
        } else {
            // Let's rebuild the msg with the updated destination details
            let dst = Dst {
                name: bounced_msg_dst.name,
                section_key: best_sap.section_key(),
            };
            debug!(
                "Final target elders for resending {msg_id:?}: {client_msg:?} msg \
                are {target_elders:?}"
            );
            Ok((
                msg_id,
                target_elders,
                query_index,
                wire_msg.payload,
                dst,
                auth,
            ))
        }
    }
}
