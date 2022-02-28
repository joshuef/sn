// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;
use crate::client::{connections::QueryResult, errors::Error};
use bytes::Bytes;
use rand::Rng;
use sn_interface::messaging::{
    data::{DataQuery, ServiceMsg},
    ServiceAuth, WireMsg,
};
use sn_interface::types::{PublicKey, Signature};
use tracing::{debug, info_span};

// We divide the total query timeout by this number.
// This also represents the max retries possible, while still staying within the max_timeout.
const MAX_RETRY_COUNT: f32 = 25.0;

impl Client {
    /// Send a Query to the network and await a response.
    /// Queries are automatically retried using exponential backoff if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query(&self, query: DataQuery) -> Result<QueryResult, Error> {
        self.send_query_with_retry_count(query, MAX_RETRY_COUNT)
            .await
    }

    /// Send a Query to the network and await a response.
    /// Queries are not retried if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_query_without_retry(&self, query: DataQuery) -> Result<QueryResult, Error> {
        self.send_query_with_retry_count(query, 1.0).await
    }

    // Send a Query to the network and await a response.
    // Queries are automatically retried if the timeout is hit
    // This function is a private helper.
    #[instrument(skip(self), level = "debug")]
    async fn send_query_with_retry_count(
        &self,
        query: DataQuery,
        retry_count: f32,
    ) -> Result<QueryResult, Error> {
        let client_pk = self.public_key();
        let msg = ServiceMsg::Query(query.clone());
        let serialised_query = WireMsg::serialize_msg_payload(&msg)?;
        let signature = self.keypair.sign(&serialised_query);

        let mut rng = rand::rngs::OsRng;

        // Add jitter so not all clients retry at the same rate. This divider will knock on to the overall retry window
        // and should help prevent elders from being conseceutively overwhelmed
        let jitter = rng.gen_range(1.0, 1.5);
        let attempt_timeout = self.query_timeout.div_f32(retry_count + jitter);
        trace!("Setting up query retry, interval is: {:?}", attempt_timeout);

        let span = info_span!("Attempting a query");
        let _ = span.enter();
        let mut attempt = 1.0;
        loop {
            debug!(
                "Attempting {:?} (attempt #{}) with a query timeout of {:?}",
                query, attempt, attempt_timeout
            );

            let res = tokio::time::timeout(
                attempt_timeout,
                self.send_signed_query(
                    query.clone(),
                    client_pk,
                    serialised_query.clone(),
                    signature.clone(),
                ),
            )
            .await;

            if let Ok(Ok(query_result)) = res {
                break Ok(query_result);
            } else if attempt > MAX_RETRY_COUNT {
                debug!(
                    "Retries ({}) all failed returning no response for {:?}",
                    MAX_RETRY_COUNT, query
                );
                break Err(Error::NoResponse);
            }

            attempt += 1.0;
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
        let auth = ServiceAuth {
            public_key: client_pk,
            signature,
        };

        self.session.send_query(query, auth, serialised_query).await
    }
}
