// Copyright 2023 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    node::{
        flow_ctrl::{cmds::Cmd, fault_detection::FaultsCmd},
        MyNode, Result,
    },
    UsedSpace,
};

use sn_comms::Comm;
use sn_interface::{
    dbcs::gen_genesis_dbc,
    messaging::system::SectionSigned,
    network_knowledge::{NetworkKnowledge, SectionsDAG, GENESIS_DBC_SK},
    types::{log_markers::LogMarker, Peer},
    SectionAuthorityProvider,
};

use ed25519_dalek::Keypair;
use sn_dbc::Dbc;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

impl MyNode {
    pub(crate) fn first_node(
        comm: Comm,
        keypair: Keypair,
        used_space: UsedSpace,
        root_storage_dir: PathBuf,
        genesis_sk_set: bls::SecretKeySet,
        fault_cmds_sender: mpsc::Sender<FaultsCmd>,
    ) -> Result<(Self, Dbc)> {
        let peer = Peer::from(comm.socket_addr(), keypair.public);

        let genesis_dbc =
            gen_genesis_dbc(&genesis_sk_set, &bls::SecretKey::from_hex(GENESIS_DBC_SK)?)?;

        let (network_knowledge, section_key_share) =
            NetworkKnowledge::first_node(peer, genesis_sk_set)?;

        let node = Self::new(
            comm,
            Arc::new(keypair),
            network_knowledge,
            Some(section_key_share),
            used_space,
            root_storage_dir,
            fault_cmds_sender,
        )?;

        Ok((node, genesis_dbc))
    }

    pub(crate) fn switch_section(
        &mut self,
        dst_sap: SectionSigned<SectionAuthorityProvider>,
        new_keypair: Keypair,
    ) -> Result<()> {
        // try to relocate to the section that matches our current name
        self.network_knowledge.switch_section(dst_sap)?;
        self.keypair = Arc::new(new_keypair);
        Ok(())
    }

    pub(crate) fn network_knowledge(&self) -> &NetworkKnowledge {
        &self.network_knowledge
    }

    pub(crate) fn section_chain(&self) -> SectionsDAG {
        self.network_knowledge.section_chain()
    }

    /// Is this node an elder?
    pub(crate) fn is_elder(&self) -> bool {
        self.network_knowledge.is_elder(&self.info().name())
    }

    pub(crate) fn is_not_elder(&self) -> bool {
        !self.is_elder()
    }

    // ----------------------------------------------------------------------------------------
    //   ---------------------------------- Mut ------------------------------------------
    // ----------------------------------------------------------------------------------------

    // Generate a new section info based on the current set of members, but
    // excluding the ones in the provided list. And if the outcome list of candidates
    // differs from the current elders, trigger a DKG.
    pub(crate) fn trigger_dkg(&mut self) -> Result<Vec<Cmd>> {
        info!("{}", LogMarker::TriggeringPromotionAndDemotion);
        let mut cmds = vec![];
        for session_id in self.best_elder_candidates() {
            cmds.extend(self.send_dkg_start(session_id)?);
        }

        Ok(cmds)
    }
}
