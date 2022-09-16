// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::collections::BTreeSet;

use super::Client;
use crate::{connections::QueryResult, errors::Error};

use sn_interface::{
    data_copy_count,
    messaging::{
        data::{DataQuery, DataQueryVariant, ServiceMsg},
        ServiceAuth, WireMsg,
    },
    types::{Peer, PublicKey, Signature},
};

use bytes::Bytes;
use rand::Rng;
use tracing::{debug, info_span};

impl Client {
    /// Send a Query to the network and await a response.
    /// Queries are automatically retried using exponential backoff if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query(&self, query: DataQueryVariant) -> Result<QueryResult, Error> {
        self.send_query_with_retry_count(query, self.max_retries)
            .await
    }

    /// Send a Query to the network and await a response.
    /// Queries are not retried if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query_without_retry(
        &self,
        query: DataQueryVariant,
    ) -> Result<QueryResult, Error> {
        self.send_query_with_retry_count(query, 1).await
    }

    // Send a Query to the network and await a response.
    // Queries are automatically retried if the timeout is hit
    // This function is a private helper.
    #[instrument(skip(self), level = "debug")]
    async fn send_query_with_retry_count(
        &self,
        query: DataQueryVariant,
        retry_count: usize,
    ) -> Result<QueryResult, Error> {
        let client_pk = self.public_key();
        let mut query = DataQuery {
            adult_index: 0,
            variant: query,
        };

        let mut rng = rand::rngs::OsRng;
        // Add jitter so not all clients retry at the same rate. This divider will knock on to the overall retry window
        // and should help prevent elders from being conseceutively overwhelmed
        let jitter = rng.gen_range(1.0..1.5);
        let attempt_timeout = self.query_timeout.div_f32(retry_count as f32 + jitter);
        trace!("Setting up query retry, interval is: {:?}", attempt_timeout);

        let span = info_span!("Attempting a query");
        let _ = span.enter();
        let mut attempts = 1;
        let dst = query.variant.dst_name();
        // should we force a fresh connection to the nodes?
        let mut force_new_link = false;
        let mut data_not_found_count = BTreeSet::default();
        loop {
            let msg = ServiceMsg::Query(query.clone());
            let serialised_query = WireMsg::serialize_msg_payload(&msg)?;
            let signature = self.keypair.sign(&serialised_query);
            debug!(
                "Attempting {:?} (attempt #{}) with a query timeout of {:?}, will force new: {force_new_link}",
                query, attempts, attempt_timeout
            );

            // grab up to date destination section from our local network knowledge
            let (section_pk, elders) = self.session.get_query_elders(dst).await?;

            let res = tokio::time::timeout(
                attempt_timeout,
                self.send_signed_query_to_section(
                    query.clone(),
                    client_pk,
                    serialised_query.clone(),
                    signature.clone(),
                    Some((section_pk, elders.clone())),
                    force_new_link,
                ),
            )
            .await;

            match res {
                Ok(Ok(query_result)) => {
                    if query_result.is_data_not_found() {
                        let _existed = data_not_found_count.insert(query.adult_index);
                        // if we've received DataNotFound from each adult_index,
                        // we can return that error
                        if data_not_found_count.len() == data_copy_count() {
                            return Ok(query_result);
                        }
                    } else {
                        return Ok(query_result);
                    }

                    warn!(
                        "{query:?} DataNotFound returned from adult_index: {:?}",
                        query.adult_index
                    )
                }
                Ok(Err(err)) if attempts > self.max_retries => {
                    debug!(
                        "Retries ({}) all failed returning no response for {:?}",
                        self.max_retries, query
                    );
                    break Err(Error::NoResponseAfterRetrying {
                        query,
                        attempts,
                        last_error: Box::new(err),
                    });
                }
                Err(_) if attempts > self.max_retries => {
                    // this should be due to our tokio time out, rather than an error
                    // returned by `send_signed_query_to_section`
                    debug!(
                        "Retries ({}) all failed returning no response for {:?}",
                        self.max_retries, query
                    );
                    break Err(Error::NoResponseAfterRetrying {
                        query,
                        attempts,
                        last_error: Box::new(Error::NoResponse(elders)),
                    });
                }
                _ => {
                    if attempts > self.max_retries / 3 {
                        // any further attempts should use fresh links in case of issues
                        force_new_link = true;
                    }
                }
            }

            attempts += 1;

            // In the next attempt, try the next adult, further away.
            query.adult_index += 1;
            // There should not be more than a certain amount of adults holding copies of the data. Retry the closest adult again.
            if query.adult_index >= data_copy_count() {
                query.adult_index = 0;
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
        self.send_signed_query_to_section(
            query,
            client_pk,
            serialised_query,
            signature,
            None,
            false,
        )
        .await
    }

    // Private helper to send a signed query, with the option to define the destination section.
    // If no destination section is provided, it will be derived from the query content.
    async fn send_signed_query_to_section(
        &self,
        query: DataQuery,
        client_pk: PublicKey,
        serialised_query: Bytes,
        signature: Signature,
        dst_section_info: Option<(bls::PublicKey, Vec<Peer>)>,
        force_new_link: bool,
    ) -> Result<QueryResult, Error> {
        let auth = ServiceAuth {
            public_key: client_pk,
            signature,
        };

        self.session
            .send_query(
                query,
                auth,
                serialised_query,
                #[cfg(feature = "traceroute")]
                self.public_key(),
                dst_section_info,
                force_new_link,
            )
            .await
    }
}
