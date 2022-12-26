use bytes::Bytes;
use clap::Parser;
use eyre::{eyre, Result};
use rand::{thread_rng, Rng};
use self_encryption::MIN_ENCRYPTABLE_BYTES;
use sn_client::{Client, Error};
use sn_interface::{
    data_copy_count,
    messaging::{
        data::{ClientMsg, DataQuery, DataQueryVariant, QueryResponse},
        WireMsg,
    },
    types::{Chunk, ChunkAddress},
};
use std::{
    fs::File,
    io::{ErrorKind, Write},
    time::Duration,
};
use tokio::time::timeout;
use xor_name::XorName;

mod log;

#[derive(Parser, Debug)]
#[clap(long_about = None)]
struct Args {
    /// Path of file to use
    #[clap()]
    file: String,

    /// Upload the file to the network before querying
    #[clap(short, long)]
    upload: bool,

    /// Query up to nth node
    #[clap(long, default_value_t = data_copy_count() - 1)]
    up_to_node: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    // Initialize

    let (args, bytes) = init()?;
    let client = Client::builder().build().await?;

    // Print out chunk information of file

    let (data_map_xor, chunks) = Client::chunk_bytes(bytes.clone())?;
    println!("DataMap XOR: {}", data_map_xor);
    for chunk in &chunks {
        println!(
            "Chunk (length: {}) XOR: {}",
            chunk.value().len(),
            chunk.address().0
        );
    }

    // Upload file to network

    if args.upload {
        let xor = client.upload(bytes).await?;
        println!("Uploaded file with DataMap XOR: {}", xor);

        assert_eq!(data_map_xor, xor);
    }

    // Query nodes for chunks

    let mut error_occurred = false;
    for chunk in chunks {
        println!(
            "Querying node 0-{} for Chunk({})",
            args.up_to_node,
            chunk.address().0
        );

        for i in 0..=args.up_to_node {
            let query_fut = query_chunk(&client, i, chunk.address().0);
            let res = match timeout(Duration::from_secs(10), query_fut).await {
                Ok(res) => res,
                Err(_) => {
                    error_occurred = true;
                    eprintln!(
                        "Error for Chunk({}) at node `{i}`: timed out!",
                        chunk.address().0
                    );
                    continue;
                }
            };

            if let Err(e) = res {
                error_occurred = true;
                eprintln!(
                    "Error for Chunk({}) at node `{i}`: {e:?}",
                    chunk.address().0
                );
            }
        }
    }

    if error_occurred {
        return Err(eyre!(
            "A chunk was not succesfully returned from a holder node"
        ));
    }

    Ok(())
}

async fn query_chunk(client: &Client, node_index: usize, address: XorName) -> Result<Chunk> {
    let variant = DataQueryVariant::GetChunk(ChunkAddress(address));
    let query = DataQuery {
        node_index,
        variant: variant.clone(),
    };
    let query_response = send_query(client, query).await?;
    match query_response {
        QueryResponse::GetChunk(result) => result.map_err(|e| e.into()),
        response => Err(Error::UnexpectedQueryResponse {
            query: variant,
            response,
        })
        .map_err(|e| e.into()),
    }
}

async fn send_query(client: &Client, query: DataQuery) -> Result<QueryResponse> {
    let client_pk = client.public_key();
    let msg = ClientMsg::Query(query.clone());
    let serialised_query = WireMsg::serialize_msg_payload(&msg)?;
    let signature = client.keypair().sign(&serialised_query);
    Ok(client
        .send_signed_query(
            query,
            client_pk,
            serialised_query.clone(),
            signature.clone(),
        )
        .await?
        .response)
}

/// Parse arguments and setup file/bytes to use
fn init() -> Result<(Args, Bytes)> {
    let args = Args::parse();

    // Read bytes from file
    let bytes = match std::fs::read(&args.file) {
        Ok(contents) => Bytes::from(contents),
        // If file is not found, generate file and put it at file path
        Err(err) if err.kind() == ErrorKind::NotFound => {
            let bytes = self_encryptable_file();

            let mut f = File::create(&args.file)?;
            f.write_all(&bytes)?;

            println!("Wrote random data file to \"{}\"", &args.file);

            bytes
        }
        Err(err) => return Err(err.into()),
    };

    Ok((args, bytes))
}

/// Generate random bytes with minimal self-encryption size
fn self_encryptable_file() -> Bytes {
    let mut file = [0u8; MIN_ENCRYPTABLE_BYTES];
    thread_rng().fill(&mut file[..]);

    Bytes::copy_from_slice(&file)
}
