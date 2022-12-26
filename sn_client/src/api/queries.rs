// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

// use std::collections::BTreeSet;

use super::Client;
use crate::{errors::Error, sessions::QueryResult};

use sn_interface::{
    data_copy_count,
    messaging::{
        data::{ClientMsg, DataQuery, DataQueryVariant},
        ClientAuth, WireMsg,
    },
    types::{Peer, PublicKey, Signature},
};

use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use tokio::time::sleep;
use tracing::{debug, info_span};

impl Client {
    /// Send a Query to the network and await a response.
    /// Queries are automatically retried using exponential backoff if the timeout is hit.
    #[cfg(not(feature = "check-replicas"))]
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query(&self, query: DataQueryVariant) -> Result<QueryResult, Error> {
        self.send_query_with_retry(query, true).await
    }

    /// Send a Query to the network and await a response.
    /// Queries are not retried if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query_without_retry(
        &self,
        query: DataQueryVariant,
    ) -> Result<QueryResult, Error> {
        self.send_query_with_retry(query, false).await
    }

    // Send a Query to the network and await a response.
    // Queries are automatically retried if the timeout is hit
    // This function is a private helper.
    #[instrument(skip(self), level = "debug")]
    async fn send_query_with_retry(
        &self,
        query: DataQueryVariant,
        retry: bool,
    ) -> Result<QueryResult, Error> {
        let client_pk = self.public_key();
        let mut query = DataQuery {
            node_index: 0,
            variant: query,
        };

        // Add jitter so not all clients retry at the same rate. This divider will knock on to the overall retry window
        // and should help prevent elders from being conseceutively overwhelmed
        trace!("Setting up query retry");

        let span = info_span!("Attempting a query");
        let _ = span.enter();
        let dst = query.variant.dst_name();

        let max_interval = self.max_backoff_interval;

        let mut backoff = ExponentialBackoff {
            initial_interval: max_interval / 2,
            max_interval,
            max_elapsed_time: Some(self.query_timeout),
            randomization_factor: 1.5,
            ..Default::default()
        };

        // this seems needed for custom settings to take effect
        backoff.reset();

        loop {
            let msg = ClientMsg::Query(query.clone());
            let serialised_query = WireMsg::serialize_msg_payload(&msg)?;
            let signature = self.keypair.sign(&serialised_query);
            debug!("Attempting {query:?} (node_index #{})", query.node_index);

            // grab up to date destination section from our local network knowledge
            let (section_pk, elders) = self.session.get_query_elders(dst).await?;

            let res = self
                .send_signed_query_to_section(
                    query.clone(),
                    client_pk,
                    serialised_query.clone(),
                    signature.clone(),
                    Some((section_pk, elders.clone())),
                )
                .await;

            // There should not be more than a certain number of nodes holding
            // copies of the data. Retry the closest node again.
            if !retry || query.node_index >= data_copy_count() - 1 {
                // we don't want to retry beyond `data_copy_count()` nodes
                return res;
            }

            if let Some(delay) = backoff.next_backoff() {
                // if we've an acceptable result, return instead of wait/retry loop
                if let Ok(result) = res {
                    if result.data_was_found() {
                        debug!("{query:?} sent and received okay");
                        return Ok(result);
                    } else {
                        warn!(
                            "Data not found... querying again until we hit query_timeout ({:?})",
                            self.query_timeout
                        );
                    }
                }

                // In the next attempt, try the next node, further away.
                query.node_index += 1;
                debug!("Sleeping before trying query again: {delay:?} sleep for {query:?}");
                sleep(delay).await;
            } else {
                warn!("Finished trying and last response to {query:?} is {res:?}");
                // we're done trying
                return res;
            }
        }
    }

    /// Send a Query to the network and await a response.
    /// This is part of a public API, for the user to
    /// provide the serialised and already signed query.
    pub async fn send_signed_query(
        &self,
        query: DataQuery,
        client_pk: PublicKey,
        serialised_query: Bytes,
        signature: Signature,
    ) -> Result<QueryResult, Error> {
        debug!("Sending Query: {:?}", query);
        self.send_signed_query_to_section(query, client_pk, serialised_query, signature, None)
            .await
    }

    // Private helper to send a signed query, with the option to define the destination section.
    // If no destination section is provided, it will be derived from the query content.
    #[allow(clippy::too_many_arguments)]
    async fn send_signed_query_to_section(
        &self,
        query: DataQuery,
        client_pk: PublicKey,
        serialised_query: Bytes,
        signature: Signature,
        dst_section_info: Option<(bls::PublicKey, Vec<Peer>)>,
    ) -> Result<QueryResult, Error> {
        let auth = ClientAuth {
            public_key: client_pk,
            signature,
        };

        self.session
            .send_query(query, auth, serialised_query, dst_section_info)
            .await
    }

    /// Send a Query to the network and await a response.
    /// Queries are sent once per each replica, i.e. it sends the query targeting
    /// all replicas (using `node_index`) to make sure the piece of content
    /// is stored in each and all of the expected data replica nodes in the section.
    #[cfg(feature = "check-replicas")]
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query(&self, query: DataQueryVariant) -> Result<QueryResult, Error> {
        use crate::errors::DataReplicasCheckError;
        let span = info_span!("Attempting a query");
        let _ = span.enter();

        let client_pk = self.public_key();
        let dst = query.dst_name();

        // grab up to date destination section from our local network knowledge
        let (section_pk, elders) = self.session.get_query_elders(dst).await?;

        // Send queries to all replicas concurrently
        let num_of_replicas = data_copy_count();
        let mut tasks = vec![];
        for node_index in 0..num_of_replicas {
            let data_query = DataQuery {
                node_index,
                variant: query.clone(),
            };
            let msg = ClientMsg::Query(data_query.clone());
            let serialised_query = WireMsg::serialize_msg_payload(&msg)?;
            let signature = self.keypair.sign(&serialised_query);
            debug!("Attempting {data_query:?}");

            let client = self.clone();
            let elders_clone = elders.clone();
            tasks.push(async move {
                let result = client
                    .send_signed_query_to_section(
                        data_query,
                        client_pk,
                        serialised_query,
                        signature,
                        Some((section_pk, elders_clone)),
                    )
                    .await;

                (result, node_index)
            });
        }

        // Let's await for all queries to be sent
        let results = futures::future::join_all(tasks).await;

        let mut errors = vec![];
        let mut responses = vec![];
        results.into_iter().for_each(|result| match result {
            (Err(error), node_index) => errors.push((error, node_index)),
            (Ok(resp), node_index) => responses.push((resp, node_index)),
        });

        if !errors.is_empty() {
            let error_msg = errors.iter().fold(
                format!(
                    "Errors occurred when sending the query to {}/{num_of_replicas} \
                    of the replicas. Errors received: ",
                    errors.len()
                ),
                |acc, (e, i)| format!("{acc}, [ Node-#{i}: {e:?} ]"),
            );
            error!(error_msg);

            return Err(DataReplicasCheckError::ReceivedErrors {
                replicas: num_of_replicas,
                query,
                errors,
            }
            .into());
        }

        if let Some((resp, node_index)) = responses.pop() {
            if responses.iter().all(|(r, _)| r.response == resp.response) {
                return Ok(resp);
            }

            // put the last response back in the list so it's included in the report
            responses.push((resp, node_index));
            let error_msg = responses.iter().fold(
                format!(
                    "Not all responses received are the same when sending query to all \
                    replicas: {query:?}. Responses received: "
                ),
                |acc, (r, i)| format!("{acc}, [ Adult-#{i}: {r:?} ]"),
            );
            error!(error_msg);

            return Err(DataReplicasCheckError::DifferentResponses {
                replicas: num_of_replicas,
                query,
                responses,
            }
            .into());
        }

        Err(DataReplicasCheckError::NoResponse(query).into())
    }
}
