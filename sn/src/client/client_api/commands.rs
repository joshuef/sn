// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{Client, MAX_RETRY_COUNT};
use crate::at_least_one_correct_elder;
use crate::client::Error;
use crate::messaging::{
    data::{DataCmd, ServiceMsg},
    ServiceAuth, WireMsg,
};
use crate::types::{PublicKey, Signature};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use tokio::time::Duration;
use xor_name::XorName;

impl Client {
    /// Send a Cmd to the network and await a response.
    /// Commands are not retried if the timeout is hit.
    #[instrument(skip(self), level = "debug")]
    pub async fn send_cmd_without_retry(&self, cmd: DataCmd) -> Result<(), Error> {
        self.send_cmd_with_retry_count(cmd, 1.0).await
    }

    // Send a Cmd to the network and await a response.
    // Commands are automatically retried if an error is returned
    // This function is a private helper.
    #[instrument(skip(self), level = "debug")]
    async fn send_cmd_with_retry_count(&self, cmd: DataCmd, retry_count: f32) -> Result<(), Error> {
        let client_pk = self.public_key();
        let dst_name = cmd.dst_name(); // let msg = ServiceMsg::Cmd(cmd.clone());

        let debug_cmd = format!("{:?}", cmd);
        let targets = at_least_one_correct_elder(); // stored at Adults, so only 1 correctly functioning Elder need to relay

        let serialised_cmd = {
            let msg = ServiceMsg::Cmd(cmd);
            WireMsg::serialize_msg_payload(&msg)?
        };
        let signature = self.keypair.sign(&serialised_cmd);

        let op_limit = self.query_timeout;

        let mut backoff = ExponentialBackoff {
            initial_interval: Duration::from_secs(5),
            max_interval: Duration::from_secs(60),
            max_elapsed_time: Some(op_limit),
            randomization_factor: 1.8,
            ..Default::default()
        };

        let span = info_span!("Attempting a cmd");
        let _ = span.enter();

        let mut attempt = 1.0;
        loop {
            debug!("Attempting {:?} (attempt #{})", debug_cmd, attempt);

            let res = self
                .send_signed_command(
                    dst_name,
                    client_pk,
                    serialised_cmd.clone(),
                    signature.clone(),
                    targets,
                )
                .await;

            if let Ok(cmd_result) = res {
                break Ok(cmd_result);
            }

            attempt += 1.0;

            if let Some(delay) = backoff.next_backoff() {
                tokio::time::sleep(delay).await;
            } else {
                // we're done trying

                break res;
            }
        }
    }

    /// Send a signed DataCmd to the network.
    /// This is to be part of a public API, for the user to
    /// provide the serialised and already signed command.
    pub async fn send_signed_command(
        &self,
        dst_address: XorName,
        client_pk: PublicKey,
        serialised_cmd: Bytes,
        signature: Signature,
        targets: usize,
    ) -> Result<(), Error> {
        let auth = ServiceAuth {
            public_key: client_pk,
            signature,
        };

        self.session
            .send_cmd(dst_address, auth, serialised_cmd, targets)
            .await
    }

    /// Send a DataCmd to the network without awaiting for a response.
    /// Cmds are automatically retried using exponential backoff if an error is returned.
    /// This function is a helper private to this module.
    #[instrument(skip_all, level = "debug", name = "client-api send cmd")]
    pub(crate) async fn send_cmd(&self, cmd: DataCmd) -> Result<(), Error> {
        self.send_cmd_with_retry_count(cmd, MAX_RETRY_COUNT).await
    }
}
