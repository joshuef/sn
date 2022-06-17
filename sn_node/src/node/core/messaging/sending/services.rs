// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::node::{api::cmds::Cmd, core::Node, Result};

use sn_interface::{
    messaging::{
        data::{CmdError, ServiceMsg},
        AuthKind, DstLocation, EndUser, MsgId, ServiceAuth, WireMsg,
    },
    types::{Peer, PublicKey, Signature},
};

use bytes::Bytes;
use ed25519_dalek::Signer;

impl Node {
    /// Forms a CmdError msg to send back to the client
    pub(crate) async fn send_cmd_error_response(
        &self,
        error: CmdError,
        target: Peer,
        msg_id: MsgId,
    ) -> Result<Vec<Cmd>> {
        let the_error_msg = ServiceMsg::CmdError {
            error,
            correlation_id: msg_id,
        };
        self.send_cmd_response(target, the_error_msg).await
    }

    /// Forms a CmdAck msg to send back to the client
    pub(crate) async fn send_cmd_ack(&self, target: Peer, msg_id: MsgId) -> Result<Vec<Cmd>> {
        let the_ack_msg = ServiceMsg::CmdAck {
            correlation_id: msg_id,
        };
        self.send_cmd_response(target, the_ack_msg).await
    }

    /// Forms a cmd to send a cmd response error/ack to the client
    async fn send_cmd_response(&self, target: Peer, msg: ServiceMsg) -> Result<Vec<Cmd>> {
        let dst = DstLocation::EndUser(EndUser(target.name()));

        let (msg_kind, payload) = self.ed_sign_client_msg(&msg).await?;
        let wire_msg = WireMsg::new_msg(MsgId::new(), payload, msg_kind, dst)?;

        let cmd = Cmd::SendMsg {
            recipients: vec![target],
            wire_msg,
        };

        Ok(vec![cmd])
    }

    /// Currently using node's Ed key. May need to use bls key share for concensus purpose.
    pub(crate) async fn ed_sign_client_msg(
        &self,
        client_msg: &ServiceMsg,
    ) -> Result<(AuthKind, Bytes)> {
        let keypair = self.info.borrow().keypair.clone();
        let payload = WireMsg::serialize_msg_payload(client_msg)?;
        let signature = keypair.sign(&payload);

        let msg = AuthKind::Service(ServiceAuth {
            public_key: PublicKey::Ed25519(keypair.public),
            signature: Signature::Ed25519(signature),
        });

        Ok((msg, payload))
    }
}
