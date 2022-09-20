// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{Safe, SafeUrl};

use sn_client::utils::test_utils::read_genesis_dbc_from_first_node;
use sn_dbc::{rng, Dbc, Owner, OwnerOnce, Token};
use sn_interface::types::Keypair;

use anyhow::{anyhow, bail, Context, Result};
use async_once::AsyncOnce;
use bls::SecretKey;
use lazy_static::lazy_static;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{collections::HashMap, env::var, ops::Index, sync::Once};
use tokio::sync::Mutex;
use tracing_subscriber::{fmt, EnvFilter};

// Environment variable which can be set with the auth credentials
// to be used for all sn_api tests
const TEST_AUTH_CREDENTIALS: &str = "TEST_AUTH_CREDENTIALS";

// Number of DBCs to reissue from genesis DBC so there is enough
// for each individual test to use a different one
const NUM_OF_DBCS_TO_REISSUE: usize = 40;

// Range of values to pick the random balances each of
// the NUM_OF_DBCS_TO_REISSUE reissued DBCs will own
const REISSUED_DBC_MIN_BALANCE: u64 = 5_000_000_000;
const REISSUED_DBC_MAX_BALANCE: u64 = 100_000_000_000;

// Load the genesis DBC.
lazy_static! {
    pub static ref GENESIS_DBC: Dbc = match read_genesis_dbc_from_first_node() {
        Ok(dbc) => dbc,
        Err(err) => panic!("Failed to read genesis DBC for tests: {:?}", err),
    };
}

// Initialise logger for tests, this is run only once, even if called multiple times.
fn init_logger() {
    static INIT_LOGGER: Once = Once::new();

    INIT_LOGGER.call_once(|| {
        fmt()
            // NOTE: comment out this line for more compact (but less readable) log output.
            // .pretty()
            .with_ansi(false)
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .init()
    });
}

// Return the next unused DBC along with the balance it owns
pub async fn get_next_bearer_dbc() -> Result<(Dbc, Token)> {
    lazy_static! {
        static ref NEXT_DBC_INDEX: Mutex<usize> = Mutex::new(0);
        static ref REISSUED_DBCS: AsyncOnce<Vec<(Dbc, Token)>> = AsyncOnce::new(async {
            match reissue_bearer_dbcs().await {
                Ok(dbcs) => dbcs,
                Err(err) => panic!("Failed to reissue DBCs from genesis DBC: {:?}", err),
            }
        });
    }

    let mut index = NEXT_DBC_INDEX.lock().await;
    let next_dbc = REISSUED_DBCS
        .get()
        .await
        .get(*index)
        .ok_or_else(|| anyhow!("No more unused DBCs available, already used: {}", index))?
        .clone();

    *index += 1;

    Ok(next_dbc)
}

// Build a set of bearer DBCs with random amounts, by reissuing them from testnet genesis DBC.
async fn reissue_bearer_dbcs() -> Result<Vec<(Dbc, Token)>> {
    let total_balance = match GENESIS_DBC.amount_secrets_bearer() {
        Ok(amount_secrets) => amount_secrets.amount().as_nano(),
        Err(err) => bail!("Failed to obtain genesis DBC balance: {:?}", err),
    };

    let mut rng = rand::thread_rng();
    let amounts: Vec<u64> = (0..NUM_OF_DBCS_TO_REISSUE)
        .map(|_| rng.gen_range(REISSUED_DBC_MIN_BALANCE..REISSUED_DBC_MAX_BALANCE))
        .collect();

    let total_output_amount: u64 = amounts.iter().sum();
    let change_amount = total_balance - total_output_amount;

    let output_amounts: Vec<(Token, OwnerOnce)> = amounts
        .into_iter()
        .map(|amount| {
            let owner = Owner::from_random_secret_key(&mut rng::thread_rng());
            let output_owner = OwnerOnce::from_owner_base(owner, &mut rng::thread_rng());
            (Token::from_nano(amount), output_owner)
        })
        .collect();

    let safe = new_safe_instance().await?;
    let (output_dbcs, _) = safe
        .reissue_dbcs(
            vec![GENESIS_DBC.clone()],
            output_amounts,
            Token::from_nano(change_amount),
        )
        .await?;

    Ok(output_dbcs
        .into_iter()
        .map(|(dbc, _, amount_secrets)| {
            let amount = amount_secrets.amount();
            (dbc, amount)
        })
        .collect())
}

// Instantiate a Safe instance, and also obtain an unspent/unused DBC
pub async fn new_safe_instance_with_dbc() -> Result<(Safe, Dbc, Token)> {
    let (dbc, balance) = get_next_bearer_dbc().await?;
    let safe = new_safe_instance().await?;

    Ok((safe, dbc, balance))
}

pub struct TestDataFilesContainer {
    pub url: SafeUrl,
    pub files_map: HashMap<String, SafeUrl>,
}

impl TestDataFilesContainer {
    pub async fn get_container<'a>(
        files: impl IntoIterator<Item = &'a str>,
    ) -> Result<TestDataFilesContainer> {
        let mut map: HashMap<String, SafeUrl> = HashMap::new();
        let safe = new_safe_instance().await?;
        let (container_xorurl, _, files_map) = safe
            .files_container_create_from("./testdata", None, false, false)
            .await?;
        let container_url = SafeUrl::from_url(&container_xorurl)?;
        for file in files {
            let file_info = files_map
                .get(file)
                .ok_or_else(|| anyhow!(format!("could not retrieve {file} from files map")))?;
            let file_link = file_info
                .get("link")
                .ok_or_else(|| anyhow!("could not retrieve file link"))?;
            let file_url = SafeUrl::from_url(file_link)?;
            map.insert(file.to_string(), file_url);
        }
        Ok(TestDataFilesContainer {
            url: container_url,
            files_map: map,
        })
    }
}

impl Index<&str> for TestDataFilesContainer {
    type Output = SafeUrl;

    fn index(&self, file_path: &str) -> &Self::Output {
        match self.files_map.get(file_path) {
            Some(url) => url,
            None => panic!("cannot find file in files map"),
        }
    }
}

// Instantiate a Safe instance
pub async fn new_safe_instance() -> Result<Safe> {
    init_logger();
    let credentials = match var(TEST_AUTH_CREDENTIALS) {
        Ok(val) => serde_json::from_str(&val).with_context(|| {
            format!(
                "Failed to parse credentials read from {} env var",
                TEST_AUTH_CREDENTIALS
            )
        })?,
        Err(_) => Keypair::new_ed25519(),
    };
    let safe = Safe::connected(Some(credentials), None, None, None).await?;
    Ok(safe)
}

pub async fn new_safe_instance_with_dbc_owner(secret_key: &str) -> Result<(Safe, Owner)> {
    init_logger();
    let credentials = match var(TEST_AUTH_CREDENTIALS) {
        Ok(val) => serde_json::from_str(&val).with_context(|| {
            format!(
                "Failed to parse credentials read from {} env var",
                TEST_AUTH_CREDENTIALS
            )
        })?,
        Err(_) => Keypair::new_ed25519(),
    };

    let sk: SecretKey = bincode::deserialize(secret_key.as_bytes())
        .with_context(|| "Failed to deserialize secret key for DBC owner")?;
    let dbc_owner = Owner::from(sk);
    let safe = Safe::connected(Some(credentials), None, None, Some(dbc_owner.clone())).await?;

    Ok((safe, dbc_owner))
}

// Instantiate a Safe instance with read-only access
pub async fn new_read_only_safe_instance() -> Result<Safe> {
    init_logger();
    let safe = Safe::connected(None, None, None, None).await?;
    Ok(safe)
}

// Create a random NRS name
pub fn random_nrs_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect()
}

#[macro_export]
macro_rules! retry_loop {
    ($n:literal, $async_func:expr) => {{
        let mut retries: u64 = $n;
        loop {
            match $async_func.await {
                Ok(val) => break val,
                Err(_) if retries > 0 => {
                    retries -= 1;
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                Err(e) => anyhow::bail!("Failed after {} retries: {:?}", $n, e),
            }
        }
    }};
    // Defaults to 10 retries if n is not provided
    ($async_func:expr) => {{
        retry_loop!(10, $async_func)
    }};
}

#[macro_export]
macro_rules! retry_loop_for_pattern {
    ($n:literal, $async_func:expr, $pattern:pat $(if $cond:expr)?) => {{
        let mut retries: u64 = $n;
        loop {
            let result = $async_func.await;
            match &result {
                $pattern $(if $cond)? => break result,
                Ok(_) | Err(_) if retries > 0 => {
                    retries -= 1;
                    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                },
                Err(e) => anyhow::bail!("Failed after {} retries: {:?}", $n, e),
                Ok(_) => anyhow::bail!("Failed to match pattern after {} retries", $n),
            }
        }
    }};
    // Defaults to 10 retries if n is not provided
    ($async_func:expr, $pattern:pat $(if $cond:expr)?) => {{
        retry_loop_for_pattern!(10, $async_func, $pattern $(if $cond)?)
    }};
}
