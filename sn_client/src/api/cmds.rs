// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;
use crate::{Error, Result};

use sn_interface::{
    messaging::{
        data::{ClientMsg, DataCmd},
        ClientAuth, WireMsg,
    },
    types::{PublicKey, Signature},
};

use bytes::Bytes;
use xor_name::XorName;

impl Client {
    /// Sign data using the client keypair
    pub fn sign(&self, data: &[u8]) -> Signature {
        self.keypair.sign(data)
    }

    /// Send a signed `DataCmd` to the network.
    /// This is part of the public API, for the user to
    /// provide the serialised and already signed cmd.
    pub async fn send_signed_cmd(
        &self,
        dst_address: XorName,
        client_pk: PublicKey,
        serialised_cmd: Bytes,
        signature: Signature,
        is_spend_cmd: bool,
    ) -> Result<()> {
        let auth = ClientAuth {
            public_key: client_pk,
            signature,
        };

        // tokio::time::timeout(self.cmd_timeout, async {
            self.session
                .send_cmd(dst_address, auth, serialised_cmd, is_spend_cmd)
                .await?
        // })
        // .await
        // .map_err(|_| Error::CmdAckValidationTimeout {
        //     elapsed: self.cmd_timeout,
        //     dst_address,
        // })?
    }

    /// Public API to send a `DataCmd` to the network.
    /// The provided `DataCmd` is serialised and signed with the
    /// keypair this Client instance has been setup with.
    #[instrument(skip_all, level = "debug", name = "client-api send cmd")]
    pub async fn send_cmd(&self, cmd: DataCmd) -> Result<()> {
        let client_pk = self.public_key();
        let dst_name = cmd.dst_name();

        let debug_cmd = format!("{:?}", cmd);
        debug!("Attempting {debug_cmd}");

        let is_spend_cmd = matches!(cmd, DataCmd::Spentbook(_));
        let serialised_cmd = {
            let msg = ClientMsg::Cmd(cmd);
            WireMsg::serialize_msg_payload(&msg)?
        };
        let signature = self.sign(&serialised_cmd);

        let res = self
            .send_signed_cmd(dst_name, client_pk, serialised_cmd, signature, is_spend_cmd)
            .await;

        if res.is_ok() {
            debug!("{debug_cmd} sent okay: {res:?}");
        } else {
            trace!("Failed response on {debug_cmd}, response: {:?}", res);
        }

        res
    }
}
