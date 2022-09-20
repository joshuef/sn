// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::Client;

use crate::Error;

use sn_dbc::{KeyImage, RingCtTransaction, SpentProof, SpentProofShare};
use sn_interface::{
    messaging::data::{DataCmd, DataQueryVariant, QueryResponse, SpentbookCmd, SpentbookQuery},
    types::SpentbookAddress,
};

use std::collections::BTreeSet;
use xor_name::XorName;

impl Client {
    //----------------------
    // Write Operations
    //---------------------

    /// Spend a DBC's key image.
    #[instrument(skip(self), level = "debug")]
    pub async fn spend_dbc(
        &self,
        key_image: KeyImage,
        tx: RingCtTransaction,
        spent_proofs: BTreeSet<SpentProof>,
        spent_transactions: BTreeSet<RingCtTransaction>,
    ) -> Result<(), Error> {
        let cmd = SpentbookCmd::Spend {
            key_image,
            tx,
            spent_proofs,
            spent_transactions,
        };
        self.send_cmd(DataCmd::Spentbook(cmd)).await
    }

    //----------------------
    // Read Spentbook
    //---------------------

    /// Return the set of spent proof shares if the provided DBC's key image is spent
    #[instrument(skip(self), level = "debug")]
    pub async fn spent_proof_shares(
        &self,
        key_image: KeyImage,
    ) -> Result<Vec<SpentProofShare>, Error> {
        let address = SpentbookAddress::new(XorName::from_content(&key_image.to_bytes()));
        let query = DataQueryVariant::Spentbook(SpentbookQuery::SpentProofShares(address));
        let query_result = self.send_query(query.clone()).await?;
        match query_result.response {
            QueryResponse::SpentProofShares(res) => {
                res.map_err(|err| Error::ErrorMsg { source: err })
            }
            other => Err(Error::UnexpectedQueryResponse {
                query,
                response: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::test_utils::{
        create_test_client_with, init_logger, read_genesis_dbc_from_first_node,
    };
    use eyre::{bail, Result};
    use sn_dbc::{rng, Hash, OwnerOnce, TransactionBuilder};
    use tokio::time::Duration;

    const MAX_ATTEMPTS: u8 = 3;
    const SLEEP_DURATION: Duration = Duration::from_secs(3);

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spentbook_spend_dbc() -> Result<()> {
        init_logger();

        let genesis_dbc = read_genesis_dbc_from_first_node()?;
        let dbc_owner = genesis_dbc.owner_base().clone();
        let client = create_test_client_with(None, Some(dbc_owner.clone()), None).await?;

        let genesis_key_image = genesis_dbc.key_image_bearer()?;

        let output_owner = OwnerOnce::from_owner_base(dbc_owner, &mut rng::thread_rng());
        let dbc_builder = TransactionBuilder::default()
            .set_decoys_per_input(0)
            .set_require_all_decoys(false)
            .add_input_dbc_bearer(&genesis_dbc)?;

        let inputs_amount_sum = dbc_builder.inputs_amount_sum();
        let dbc_builder = dbc_builder
            .add_output_by_amount(inputs_amount_sum, output_owner)
            .build(&mut rng::thread_rng())?;

        assert_eq!(dbc_builder.inputs().len(), 1);
        let (key_image, tx) = &dbc_builder.inputs()[0];
        assert_eq!(&genesis_key_image, key_image);

        // Spend the key_image.
        client
            .spend_dbc(
                *key_image,
                tx.clone(),
                genesis_dbc.spent_proofs,
                genesis_dbc.spent_transactions,
            )
            .await?;

        // The query could be too close to the spend which make adult only accumulated
        // part of shares. To avoid assertion faiure, more attempts are needed.
        let mut attempts = 0;
        loop {
            attempts += 1;

            // Get spent proof shares for the key_image.
            let spent_proof_shares = client.spent_proof_shares(*key_image).await?;

            // Note this test could have been run more than once thus the genesis DBC
            // could have been spent a few times already, so we filter
            // the SpentProofShares that belong to the TX we just spent in this run.
            // TODO: once we have our Spentbook which prevents double spents
            // we shouldnt't need this filtering.
            let num_of_spent_proof_shares = spent_proof_shares
                .iter()
                .filter(|proof| proof.content.transaction_hash == Hash::from(tx.hash()))
                .count();

            if (5..=7).contains(&num_of_spent_proof_shares) {
                break Ok(());
            } else if attempts == MAX_ATTEMPTS {
                bail!(
                    "Failed to obtained enough spent proof shares after {} attempts, {} retrieved in last attempt",
                    MAX_ATTEMPTS, num_of_spent_proof_shares
                );
            }

            tokio::time::sleep(SLEEP_DURATION).await;
        }
    }
}
