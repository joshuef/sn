// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod wire_msg;
mod wire_msg_header;

pub use self::wire_msg::WireMsg;
#[cfg(feature = "traceroute")]
pub use self::wire_msg::{Entity, Traceroute};
use super::{AuthorityProof, BlsShareAuth, NodeAuth, SectionAuth};

use crate::types::PublicKey;

use xor_name::XorName;

/// Authority of a `NodeMsg`.
/// Src of message and authority to send it. Authority is validated by the signature.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum NodeMsgAuthority {
    /// Authority of a single peer.
    Node(AuthorityProof<NodeAuth>),
    /// Authority of a single peer that uses it's BLS Keyshare to sign the message.
    BlsShare(AuthorityProof<BlsShareAuth>),
    /// Authority of a whole section.
    Section(AuthorityProof<SectionAuth>),
}

impl NodeMsgAuthority {
    /// Returns the `XorName` of the authority used for the auth signing
    pub fn get_auth_xorname(&self) -> XorName {
        match self.clone() {
            NodeMsgAuthority::BlsShare(auth_proof) => {
                let auth = auth_proof.into_inner();
                auth.src_name
            }
            NodeMsgAuthority::Node(auth_proof) => {
                let auth = auth_proof.into_inner();
                let pk = auth.node_ed_pk;

                XorName::from(PublicKey::from(pk))
            }
            NodeMsgAuthority::Section(auth_proof) => {
                let auth = auth_proof.into_inner();
                auth.src_name
            }
        }
    }
}
