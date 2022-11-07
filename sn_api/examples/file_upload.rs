// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use bytes::Buf;
use color_eyre::Result;
use sn_api::{resolver::SafeData, Safe, SafeUrl};
use std::{env::temp_dir, fs::File, io::Write, path::PathBuf};

const FILE_TO_UPLOAD: &str = "file_to_upload.rs";

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // We first create a temporary file with some content,
    // which is the file we'll then upload to the network.
    let file_path = create_tmp_file()?;

    // The Safe instance is what will give us access to the network API.
    let mut safe = Safe::connected(None, None, None, None).await?;

    // We can now upload the file to the network, using the following information
    let dst = None; // root path at destination container
    let recursive = false; // do not do a recursive look up of files on local path
    let follow_links = false; // do not attempt to follow local links

    println!("Uploading '{}' to Safe ...", file_path.display());
    let (xorurl, _, _) = safe
        .files_container_create_from(&file_path, dst, recursive, follow_links)
        .await?;

    // The 'files_container_create_from' API returns (among other information) the
    // XOR-URL of the FilesContainer where the file was uplaoded to
    println!(
        "\nFile '{}' uploaded to Safe at {}",
        file_path.display(),
        xorurl
    );

    // We give the network a moment to make sure nodes get in sync
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Using the FilesContainer XOR-URL we can construct the Safe-URL of
    // the file by post fixing it with its path name,
    // i.e. safe://<FilesContainer XOR-URL>/<file name>
    let mut url = SafeUrl::from_url(&xorurl)?;
    url.set_path(FILE_TO_UPLOAD);
    println!("\nRetrieving file from {} ...\n", url);

    // Now we can simly fetch the file using `fetch` API,
    // it will return not only thee content of the file
    // but its metadata too so we can distinguish what has
    // been fetched from the provided Safe-URL.
    let fetched = safe.fetch(&url.to_string(), None).await;
    if let Ok(SafeData::PublicFile { data, .. }) = fetched {
        println!(
            "Content retrieved:\n{}",
            String::from_utf8(data.chunk().to_vec())?
        );
    } else {
        println!("Failed to retrieve file, obtained: {:?}", fetched);
    }

    Ok(())
}

// Creates a temporary file
fn create_tmp_file() -> Result<PathBuf> {
    let tmp_dir = temp_dir();
    let file_path = tmp_dir.join(FILE_TO_UPLOAD);
    let mut file = File::create(&file_path)?;
    writeln!(file, "Hello Safe!")?;

    Ok(file_path)
}
