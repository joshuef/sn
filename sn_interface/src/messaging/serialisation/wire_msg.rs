// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::wire_msg_header::WireMsgHeader;
use crate::messaging::{
    data::{ServiceError, ServiceMsg},
    system::SystemMsg,
    AuthorityProof, DstLocation, Error, MsgId, MsgKind, MsgType, NodeMsgAuthority, Result,
    ServiceAuth,
};
use bls::PublicKey as BlsPublicKey;
use bytes::Bytes;
use custom_debug::Debug;
use serde::Serialize;
use std::io::Write;
use xor_name::XorName;

/// In order to send a message over the wire, it needs to be serialized
/// along with a header (WireMsgHeader) which contains the information needed
/// by the recipient to properly deserialize it.
/// The WireMsg struct provides the utilities to serialize and deserialize messages.
#[derive(Clone, Debug)]
pub struct WireMsg {
    /// Message header
    pub header: WireMsgHeader,
    /// Serialised message
    #[debug(skip)]
    pub payload: Bytes,

    /// Extra debug info if the relevant feature is enabled.
    // This is behind a feature because it's potentially expensive to carry around the message as
    // well as its serialization.
    #[cfg(feature = "unstable-wiremsg-debuginfo")]
    pub payload_debug: Option<std::sync::Arc<dyn std::fmt::Debug + Send + Sync>>,
}

impl PartialEq for WireMsg {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.payload == other.payload
    }
}

impl WireMsg {
    /// Serializes the message provided. This function shall be used to
    /// obtain the serialized payload which is needed to create the `MsgKind`.
    /// Once the caller obtains both the serialized payload and `MsgKind`,
    /// it can invoke the `new_msg` function to instantiate a `WireMsg`.
    pub fn serialize_msg_payload<T: Serialize>(msg: &T) -> Result<Bytes> {
        let payload_vec = rmp_serde::to_vec_named(&msg).map_err(|err| {
            Error::Serialisation(format!(
                "could not serialize message payload with Msgpack: {}",
                err
            ))
        })?;

        Ok(Bytes::from(payload_vec))
    }

    /// Creates a new `WireMsg` with the provided serialized payload and `MsgKind`.
    pub fn new_msg(
        msg_id: MsgId,
        payload: Bytes,
        msg_kind: MsgKind,
        dst_location: DstLocation,
    ) -> Result<Self> {
        Ok(Self {
            header: WireMsgHeader::new(msg_id, msg_kind, dst_location),
            payload,
            #[cfg(feature = "unstable-wiremsg-debuginfo")]
            payload_debug: None,
        })
    }

    /// Attempts to create an instance of WireMsg by deserialising the bytes provided.
    /// To succeed, the bytes should contain at least a valid WireMsgHeader.
    pub fn from(bytes: Bytes) -> Result<Self> {
        // Deserialize the header bytes first
        let (header, payload) = WireMsgHeader::from(bytes)?;

        // We can now create a deserialized WireMsg using the read bytes
        Ok(Self {
            header,
            payload,
            #[cfg(feature = "unstable-wiremsg-debuginfo")]
            payload_debug: None,
        })
    }

    /// Return the serialized WireMsg, which contains the WireMsgHeader bytes,
    /// followed by the payload bytes, i.e. the serialized Message.
    pub fn serialize(&self) -> Result<Bytes> {
        // First we create a buffer with the capacity
        // needed to serialize the wire msg
        // FIXME: don't multiplying the max size by a factor of 10 and calculate the correct size.
        let max_length = 10 * (WireMsgHeader::max_size() as usize + self.payload.len());
        let mut buffer = vec![0u8; max_length];

        let (mut buf_at_payload, bytes_written) = self.header.write(&mut buffer)?;

        // ...and finally we write the bytes of the serialized payload to the original buffer
        buf_at_payload.write_all(&self.payload).map_err(|err| {
            Error::Serialisation(format!(
                "message payload (size {}) couldn't be serialized: {}",
                self.payload.len(),
                err
            ))
        })?;

        // We can now return the buffer containing the written bytes
        buffer.truncate(bytes_written as usize + self.payload.len());
        Ok(Bytes::from(buffer))
    }

    /// Deserialize the payload from this WireMsg returning a MsgType instance.
    pub fn into_msg(&self) -> Result<MsgType> {
        match self.header.msg_envelope.msg_kind.clone() {
            MsgKind::ServiceMsg(auth) => {
                let msg: ServiceMsg = rmp_serde::from_slice(&self.payload).map_err(|err| {
                    Error::FailedToParse(format!("Data message payload as Msgpack: {}", err))
                })?;

                let auth = if let ServiceMsg::ServiceError(ServiceError {
                    source_message: Some(payload),
                    ..
                }) = &msg
                {
                    AuthorityProof::verify(auth, payload)?
                } else {
                    AuthorityProof::verify(auth, &self.payload)?
                };

                Ok(MsgType::Service {
                    msg_id: self.header.msg_envelope.msg_id,
                    auth,
                    dst_location: self.header.msg_envelope.dst_location,
                    msg,
                })
            }
            MsgKind::NodeAuthMsg(node_signed) => {
                let msg: SystemMsg = rmp_serde::from_slice(&self.payload).map_err(|err| {
                    Error::FailedToParse(format!("Node signed message payload as Msgpack: {}", err))
                })?;

                Ok(MsgType::System {
                    msg_id: self.header.msg_envelope.msg_id,
                    msg_authority: NodeMsgAuthority::Node(AuthorityProof::verify(
                        node_signed,
                        &self.payload,
                    )?),
                    dst_location: self.header.msg_envelope.dst_location,
                    msg,
                })
            }
            MsgKind::NodeBlsShareAuthMsg(bls_share_signed) => {
                let msg: SystemMsg = rmp_serde::from_slice(&self.payload).map_err(|err| {
                    Error::FailedToParse(format!(
                        "Node message payload (BLS share signed) as Msgpack: {}",
                        err
                    ))
                })?;

                Ok(MsgType::System {
                    msg_id: self.header.msg_envelope.msg_id,
                    msg_authority: NodeMsgAuthority::BlsShare(AuthorityProof::verify(
                        bls_share_signed,
                        &self.payload,
                    )?),
                    dst_location: self.header.msg_envelope.dst_location,
                    msg,
                })
            }
        }
    }

    /// Return the message id of this message
    pub fn msg_id(&self) -> MsgId {
        self.header.msg_envelope.msg_id
    }

    /// Update the message ID
    pub fn set_msg_id(&mut self, msg_id: MsgId) {
        self.header.msg_envelope.msg_id = msg_id;
    }

    /// Return the kind of this message
    pub fn msg_kind(&self) -> &MsgKind {
        &self.header.msg_envelope.msg_kind
    }

    /// Return the destination section PublicKey for this message
    pub fn dst_section_pk(&self) -> Option<BlsPublicKey> {
        self.header.msg_envelope.dst_location.section_pk()
    }

    /// Update the destination section PublicKey for this message
    pub fn set_dst_section_pk(&mut self, pk: BlsPublicKey) {
        self.header.msg_envelope.dst_location.set_section_pk(pk)
    }

    /// Update the destination XorName for this message
    pub fn set_dst_xorname(&mut self, name: XorName) {
        self.header.msg_envelope.dst_location.set_name(name)
    }

    /// Return the destination for this message
    pub fn dst_location(&self) -> &DstLocation {
        &self.header.msg_envelope.dst_location
    }

    /// Return the source section PublicKey for this
    /// message if it's a NodeMsg
    pub fn src_section_pk(&self) -> Option<BlsPublicKey> {
        match &self.header.msg_envelope.msg_kind {
            MsgKind::NodeAuthMsg(node_signed) => Some(node_signed.section_pk),
            MsgKind::NodeBlsShareAuthMsg(bls_share_signed) => Some(bls_share_signed.section_pk),
            _ => None,
        }
    }

    /// Convenience function which creates a temporary WireMsg from the provided
    /// bytes, returning the deserialized message.
    pub fn deserialize(bytes: Bytes) -> Result<MsgType> {
        Self::from(bytes)?.into_msg()
    }

    /// Convenience function which validates the signature on a ServiceMsg.
    pub fn verify_sig(auth: ServiceAuth, msg: ServiceMsg) -> Result<AuthorityProof<ServiceAuth>> {
        Self::serialize_msg_payload(&msg).and_then(|payload| AuthorityProof::verify(auth, &payload))
    }

    #[cfg(feature = "unstable-wiremsg-debuginfo")]
    pub fn set_payload_debug(
        // take ownership for ergonomics in `cfg(...)` blocks
        mut self,
        payload_debug: impl std::fmt::Debug + Send + Sync + 'static,
    ) -> Self {
        self.payload_debug = Some(std::sync::Arc::new(payload_debug));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        messaging::{
            data::{DataQuery, ServiceMsg, StorageLevel},
            system::{NodeCmd, SystemMsg},
            AuthorityProof, MsgId, NodeAuth, ServiceAuth,
        },
        types::{ChunkAddress, Keypair},
    };
    use bls::SecretKey;
    use eyre::Result;
    use rand::rngs::OsRng;
    use xor_name::XorName;

    #[test]
    fn serialisation_node_msg() -> Result<()> {
        let src_section_pk = SecretKey::random().public_key();
        let mut rng = OsRng;
        let src_node_keypair = ed25519_dalek::Keypair::generate(&mut rng);

        let dst_name = XorName::random();
        let dst_section_pk = SecretKey::random().public_key();
        let dst_location = DstLocation::Node {
            name: dst_name,
            section_pk: dst_section_pk,
        };

        let msg_id = MsgId::new();
        let pk = crate::types::PublicKey::Bls(dst_section_pk);

        let node_msg = SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel {
            node_id: pk,
            section: pk.into(),
            level: StorageLevel::zero(),
        });

        let payload = WireMsg::serialize_msg_payload(&node_msg)?;
        let node_auth = NodeAuth::authorize(src_section_pk, &src_node_keypair, &payload);

        let msg_kind = MsgKind::NodeAuthMsg(node_auth.clone().into_inner());

        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;
        let serialized = wire_msg.serialize()?;

        // test deserialisation of header
        let deserialized = WireMsg::from(serialized)?;
        assert_eq!(deserialized, wire_msg);
        assert_eq!(deserialized.msg_id(), wire_msg.msg_id());
        assert_eq!(deserialized.dst_location(), &dst_location);
        assert_eq!(deserialized.dst_section_pk(), Some(dst_section_pk));
        assert_eq!(deserialized.src_section_pk(), Some(src_section_pk));

        // test deserialisation of payload
        assert_eq!(
            deserialized.into_msg()?,
            MsgType::System {
                msg_id: wire_msg.msg_id(),
                msg_authority: NodeMsgAuthority::Node(node_auth),
                dst_location,
                msg: node_msg,
            }
        );

        Ok(())
    }

    #[test]
    fn serialisation_client_msg() -> Result<()> {
        let mut rng = OsRng;
        let src_client_keypair = Keypair::new_ed25519(&mut rng);

        let dst_name = XorName::random();
        let dst_section_pk = SecretKey::random().public_key();
        let dst_location = DstLocation::Node {
            name: dst_name,
            section_pk: dst_section_pk,
        };

        let msg_id = MsgId::new();

        let client_msg = ServiceMsg::Query(DataQuery::GetChunk(ChunkAddress(XorName::random())));

        let payload = WireMsg::serialize_msg_payload(&client_msg)?;
        let auth = ServiceAuth {
            public_key: src_client_keypair.public_key(),
            signature: src_client_keypair.sign(&payload),
        };
        let auth_proof = AuthorityProof::verify(auth.clone(), &payload).unwrap();

        let msg_kind = MsgKind::ServiceMsg(auth);

        let wire_msg = WireMsg::new_msg(msg_id, payload, msg_kind, dst_location)?;
        let serialized = wire_msg.serialize()?;

        // test deserialisation of header
        let deserialized = WireMsg::from(serialized)?;
        assert_eq!(deserialized, wire_msg);
        assert_eq!(deserialized.msg_id(), wire_msg.msg_id());
        assert_eq!(deserialized.dst_location(), &dst_location);
        assert_eq!(deserialized.dst_section_pk(), Some(dst_section_pk));
        assert_eq!(deserialized.src_section_pk(), None);

        // test deserialisation of payload
        assert_eq!(
            deserialized.into_msg()?,
            MsgType::Service {
                msg_id: wire_msg.msg_id(),
                auth: auth_proof,
                dst_location,
                msg: client_msg,
            }
        );

        Ok(())
    }
}
