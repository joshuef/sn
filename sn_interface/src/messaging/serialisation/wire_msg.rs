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
    AuthKind, AuthorityProof, Dst, Error, MsgId, MsgType, NodeMsgAuthority, Result, ServiceAuth,
};
use bytes::{Bytes, BytesMut};
use custom_debug::Debug;
use serde::Serialize;

#[cfg(feature = "traceroute")]
use crate::types::PublicKey;
#[cfg(feature = "traceroute")]
use serde::Deserialize;

#[cfg(feature = "traceroute")]
use itertools::Itertools;
#[cfg(feature = "traceroute")]
use std::fmt::{Debug as StdDebug, Display, Formatter};

/// In order to send a message over the wire, it needs to be serialized
/// along with a header (`WireMsgHeader`) which contains the information needed
/// by the recipient to properly deserialize it.
/// The `WireMsg` struct provides the utilities to serialize and deserialize messages.
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
    #[cfg(feature = "test-utils")]
    pub payload_debug: Option<std::sync::Arc<dyn std::fmt::Debug + Send + Sync>>,
}

#[cfg(feature = "traceroute")]
/// PublicKey of the entity that created/handled its associated WireMsg
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Entity {
    Elder(PublicKey),
    Adult(PublicKey),
    Client(PublicKey),
}

#[cfg(feature = "traceroute")]
impl Display for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Elder(key) => write!(f, "Elder({})", key),
            Self::Adult(key) => write!(f, "Adult({})", key),
            Self::Client(key) => write!(f, "Client({})", key),
        }
    }
}

#[cfg(feature = "traceroute")]
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Traceroute(pub Vec<Entity>);

#[cfg(feature = "traceroute")]
impl Display for Traceroute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Traceroute: ")?;
        let res = self.0.iter().join(" => ");
        write!(f, "{}", res)
    }
}

#[cfg(feature = "traceroute")]
impl StdDebug for Traceroute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Traceroute: ")?;
        let res = self.0.iter().join(" => ");
        write!(f, "{}", res)
    }
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
    pub fn new_msg(msg_id: MsgId, payload: Bytes, auth: AuthKind, dst: Dst) -> Self {
        Self {
            header: WireMsgHeader::new(
                msg_id,
                auth,
                dst,
                #[cfg(feature = "traceroute")]
                Traceroute(vec![]),
            ),
            payload,
            #[cfg(feature = "test-utils")]
            payload_debug: None,
        }
    }

    /// Attempts to create an instance of `WireMsg` by deserialising the bytes provided.
    /// To succeed, the bytes should contain at least a valid `WireMsgHeader`.
    pub fn from(bytes: Bytes) -> Result<Self> {
        // Deserialize the header bytes first
        let (header, payload) = WireMsgHeader::from(bytes)?;

        // We can now create a deserialized WireMsg using the read bytes
        Ok(Self {
            header,
            payload,
            #[cfg(feature = "test-utils")]
            payload_debug: None,
        })
    }

    /// Attempts to update the Dst bytes only in the provided wire_msg_bytes.
    /// This avoids reserializing message bytes for sending to a new Dst
    /// if the rest of the content is the same
    pub fn update_dst_in_wire_msg_bytes(bytes: Bytes, new_dst: Dst) -> Result<Bytes> {
        // Deserialize the header bytes first
        let updated_bytes = WireMsgHeader::change_dst_in_bytes(bytes, new_dst)?;

        // We now have a new msg going to a new dst without all the re-serializing
        Ok(updated_bytes)
    }

    /// Return the serialized `WireMsg`, which contains the `WireMsgHeader` bytes,
    /// followed by the payload bytes, i.e. the serialized Message.
    pub fn serialize(&self) -> Result<Bytes> {
        // First we create a buffer with the capacity
        // needed to serialize the wire msg
        let max_length = WireMsgHeader::max_size() as usize + self.payload.len();
        let buffer = BytesMut::with_capacity(max_length);

        let (mut buffer, _bytes_written) = self.header.write(buffer)?;

        // ...and finally we write the bytes of the serialized payload to the original buffer
        buffer.extend_from_slice(&self.payload);

        // We can now return the buffer containing the written bytes
        Ok(buffer.freeze())
    }

    /// Deserialize the payload from this `WireMsg` returning a `MsgType` instance.
    pub fn into_msg(&self) -> Result<MsgType> {
        match self.header.msg_envelope.auth.clone() {
            #[cfg(any(feature = "chunks", feature = "registers"))]
            AuthKind::Service(auth) => {
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
                    dst: self.header.msg_envelope.dst,
                    msg,
                })
            }
            AuthKind::Node(node_signed) => {
                let msg: SystemMsg = rmp_serde::from_slice(&self.payload).map_err(|err| {
                    Error::FailedToParse(format!("Node signed message payload as Msgpack: {}", err))
                })?;

                Ok(MsgType::System {
                    msg_id: self.header.msg_envelope.msg_id,
                    msg_authority: NodeMsgAuthority::Node(AuthorityProof::verify(
                        node_signed,
                        &self.payload,
                    )?),
                    dst: self.header.msg_envelope.dst,
                    msg,
                })
            }
            AuthKind::NodeBlsShare(bls_share_signed) => {
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
                    dst: self.header.msg_envelope.dst,
                    msg,
                })
            }
        }
    }

    /// Return the message id of this message
    pub fn msg_id(&self) -> MsgId {
        self.header.msg_envelope.msg_id
    }

    /// Return the auth of this message
    pub fn auth(&self) -> &AuthKind {
        &self.header.msg_envelope.auth
    }

    /// Return the destination section `PublicKey` for this message
    pub fn dst_section_key(&self) -> bls::PublicKey {
        self.header.msg_envelope.dst.section_key
    }

    /// Return the source section `PublicKey` for this
    /// message if it's a `NodeMsg`
    pub fn src_section_pk(&self) -> Option<bls::PublicKey> {
        match &self.header.msg_envelope.auth {
            AuthKind::Node(node_signed) => Some(node_signed.section_pk),
            AuthKind::NodeBlsShare(bls_share_signed) => Some(bls_share_signed.section_pk),
            _ => None,
        }
    }

    /// Return the dst of this msg
    pub fn dst(&self) -> &Dst {
        &self.header.msg_envelope.dst
    }

    /// Convenience function which creates a temporary `WireMsg` from the provided
    /// bytes, returning the deserialized message.
    pub fn deserialize(bytes: Bytes) -> Result<MsgType> {
        Self::from(bytes)?.into_msg()
    }

    /// Convenience function which validates the signature on a `ServiceMsg`.
    pub fn verify_sig(auth: ServiceAuth, msg: ServiceMsg) -> Result<AuthorityProof<ServiceAuth>> {
        Self::serialize_msg_payload(&msg).and_then(|payload| AuthorityProof::verify(auth, &payload))
    }

    #[cfg(feature = "test-utils")]
    pub fn set_payload_debug(
        // take ownership for ergonomics in `cfg(...)` blocks
        mut self,
        payload_debug: impl std::fmt::Debug + Send + Sync + 'static,
    ) -> Self {
        self.payload_debug = Some(std::sync::Arc::new(payload_debug));
        self
    }
}

#[cfg(feature = "traceroute")]
impl WireMsg {
    pub fn append_trace(&mut self, traceroute: &mut Traceroute) {
        self.header
            .msg_envelope
            .traceroute
            .0
            .append(&mut traceroute.0)
    }

    pub fn traceroute(&self) -> Traceroute {
        self.header.msg_envelope.traceroute.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        messaging::{
            data::{DataQuery, DataQueryVariant, ServiceMsg, StorageLevel},
            system::{NodeCmd, SystemMsg},
            AuthorityProof, MsgId, NodeAuth, ServiceAuth,
        },
        types::{ChunkAddress, Keypair},
    };
    use bls::SecretKey;
    use eyre::Result;
    use rand_07::rngs::OsRng;

    #[test]
    fn serialisation_node_msg() -> Result<()> {
        let src_section_pk = SecretKey::random().public_key();
        let mut rng = OsRng;
        let src_node_keypair = ed25519_dalek::Keypair::generate(&mut rng);

        let dst = Dst {
            name: xor_name::rand::random(),
            section_key: SecretKey::random().public_key(),
        };

        let msg_id = MsgId::new();
        let pk = crate::types::PublicKey::Bls(dst.section_key);

        let msg = SystemMsg::NodeCmd(NodeCmd::RecordStorageLevel {
            node_id: pk,
            section: pk.into(),
            level: StorageLevel::zero(),
        });

        let payload = WireMsg::serialize_msg_payload(&msg)?;
        let node_auth = NodeAuth::authorize(src_section_pk, &src_node_keypair, &payload);

        let auth = AuthKind::Node(node_auth.clone().into_inner());

        let wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);
        let serialized = wire_msg.serialize()?;

        // test deserialisation of header
        let deserialized = WireMsg::from(serialized)?;
        assert_eq!(deserialized, wire_msg);
        assert_eq!(deserialized.msg_id(), wire_msg.msg_id());
        assert_eq!(deserialized.dst(), &dst);
        assert_eq!(deserialized.dst_section_key(), dst.section_key);
        assert_eq!(deserialized.src_section_pk(), Some(src_section_pk));

        // test deserialisation of payload
        assert_eq!(
            deserialized.into_msg()?,
            MsgType::System {
                msg_id: wire_msg.msg_id(),
                msg_authority: NodeMsgAuthority::Node(node_auth),
                dst,
                msg,
            }
        );

        Ok(())
    }

    #[test]
    fn serialisation_client_msg() -> Result<()> {
        let src_client_keypair = Keypair::new_ed25519();

        let dst = Dst {
            name: xor_name::rand::random(),
            section_key: SecretKey::random().public_key(),
        };

        let msg_id = MsgId::new();

        let client_msg = ServiceMsg::Query(DataQuery {
            adult_index: 0,
            variant: DataQueryVariant::GetChunk(ChunkAddress(xor_name::rand::random())),
        });

        let payload = WireMsg::serialize_msg_payload(&client_msg)?;
        let auth = ServiceAuth {
            public_key: src_client_keypair.public_key(),
            signature: src_client_keypair.sign(&payload),
        };
        let auth_proof = AuthorityProof::verify(auth.clone(), &payload).unwrap();

        let auth = AuthKind::Service(auth);

        let wire_msg = WireMsg::new_msg(msg_id, payload, auth, dst);
        let serialized = wire_msg.serialize()?;

        // test deserialisation of header
        let deserialized = WireMsg::from(serialized)?;
        assert_eq!(deserialized, wire_msg);
        assert_eq!(deserialized.msg_id(), wire_msg.msg_id());
        assert_eq!(deserialized.dst(), &dst);
        assert_eq!(deserialized.dst_section_key(), dst.section_key);
        assert_eq!(deserialized.src_section_pk(), None);

        // test deserialisation of payload
        assert_eq!(
            deserialized.into_msg()?,
            MsgType::Service {
                msg_id: wire_msg.msg_id(),
                auth: auth_proof,
                dst,
                msg: client_msg,
            }
        );

        Ok(())
    }
}
