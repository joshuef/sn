// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    helpers::{div_or, pluralize, processed_files_err_report, prompt_user},
    OutputFmt,
};
use bytes::Buf;
use color_eyre::{eyre::bail, eyre::eyre, eyre::WrapErr, Result};
use console::Term;
use sn_api::{
    files::{FilesMap, GetAttr},
    resolver::Range,
    resolver::SafeData,
    DataType, Result as ApiResult, Safe, SafeUrl, XorUrl,
};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufWriter, Write},
    path::Path,
};
use tracing::{debug, info, trace, warn};

/// # Retrieval/write status for current file and overall transfer.
#[derive(Debug, Clone)]
pub struct FilesGetStatus<'a, 'b> {
    pub path_remote: &'a Path,
    pub path_local: &'b Path,
    pub total_files: u64,
    pub current_file: u64,
    pub total_transfer_bytes: u64,
    pub transfer_bytes_written: u64,
    pub file_size: u64,
    pub file_bytes_written: u64,
    pub file_type: String,
}

/// # Action to perform when downloading if a file already exists.
#[derive(Debug)]
pub enum FileExistsAction {
    Overwrite,
    Preserve,
    Ask,
}

/// Default action is Ask
impl Default for FileExistsAction {
    fn default() -> Self {
        FileExistsAction::Ask
    }
}

// implement FromStr for parsing "--exists" arg.
impl std::str::FromStr for FileExistsAction {
    type Err = String;
    fn from_str(str: &str) -> Result<Self, String> {
        match str {
            "overwrite" => Ok(Self::Overwrite),
            "preserve" => Ok(Self::Preserve),
            "ask" => Ok(Self::Ask),
            other => Err(format!(
                "'{}' not supported. Supported values are ask, preserve, and overwrite",
                other
            )),
        }
    }
}

// What type of Progress Indicator to display.
#[derive(Debug)]
pub enum ProgressIndicator {
    Text,
    None,
}

// implement FromStr for parsing "--exists" arg.
impl std::str::FromStr for ProgressIndicator {
    type Err = String;
    fn from_str(str: &str) -> Result<Self, String> {
        match str {
            "text" => Ok(Self::Text),
            "none" => Ok(Self::None),
            other => Err(format!(
                "'{}' not supported. Supported values are bars, text, and none",
                other
            )),
        }
    }
}

impl Default for ProgressIndicator {
    fn default() -> Self {
        ProgressIndicator::Text
    }
}

// processes the `safe files get` command.  called by files.rs
//
// dst is a local path.  defaults to "."
//   Path will be created if not existing, else error.
//
// TODO: _preserve file attributes is not yet implemented, we need them
//   stored in metadata first.
//
// TBD: how should we handle OutputFmt?  Presently, we are displaying
// progress bars, and also [possibly] prompting user about overwrites.
// We have a list of processed files that we could present as json
// or in table form.  But if stdout format is json and we prompt user,
// then any process parsing output as json will break.  So possibly
// --exists=ask and --json should conflict and not be allowed.
//
// This command is really similar to cp or scp, and people are fine
// using those without a report.  So it doesn't seem especially urgent.
pub async fn process_get_command(
    safe: &mut Safe,
    source: XorUrl,
    dst: Option<String>,
    exists: FileExistsAction,
    progress: ProgressIndicator,
    _preserve: bool,
    _output_fmt: OutputFmt,
) -> Result<()> {
    let str_path = dst.unwrap_or_else(|| ".".to_string());
    let path = Path::new(&str_path);

    let mut overwrites: u64 = 0;
    let mut preserves: u64 = 0;

    let (_version, processed_files) =
        files_container_get_files(safe, &source, &str_path, |status| {
            let mut overwrite = true;
            let mut mystatus = status.clone();

            if status.file_bytes_written == 0 {
                // It is an error/warning if the dst path attempts to use
                // an existing file as a directory. But other files should
                // still be written.  eg:
                // $ mkdir -p /tmp/a/b/c && touch /tmp/a/file.txt
                // $ mkdir /tmp/target && touch /tmp/target/b   (b is a file)
                // $ cp -r /tmp/a/* /tmp/target
                //    cp: cannot overwrite non-directory '/tmp/target/b' with directory '/tmp/a/b'
                // $ ls -l /tmp/target/
                //      total 0
                //      -rw-rw-r-- 1 user user 0 Mar 31 14:38 b         (b still a file)
                //      -rw-rw-r-- 1 user user 0 Mar 31 14:38 file.txt  (other file written)
                //
                // TBD: Should FileExistsAction apply to this case?
                //      unix cp does not provide any flag/option/prompt to permit this
                //      and it always emits a warning.  So I am satisfied with this
                //      working the same way, at least for now.
                let dirpath = if status.file_type == "inode/directory" {
                    Some(status.path_local)
                } else {
                    status.path_local.parent()
                };
                if let Some(parent) = dirpath {
                    if let Some(filepath) = path_contains_file(parent) {
                        let msg = format!(
                            "cannot overwrite non-directory '{}' with directory in '{}'",
                            filepath.display(),
                            status.path_local.display()
                        );

                        warn!("Skipping file \"{}\". {}", status.path_local.display(), msg);
                        if atty::is(atty::Stream::Stderr) {
                            eprintln!("Warning: {}", msg);
                        }
                        overwrite = false;
                    }
                }
                if status.path_local.exists() && overwrite {
                    overwrite = match exists {
                        FileExistsAction::Overwrite => true,
                        FileExistsAction::Preserve => false,
                        FileExistsAction::Ask => {
                            let prompt = format!("overwrite '{}'? ", status.path_local.display());
                            prompt_yes_no(&prompt, "Y")
                        }
                    };
                    if overwrite {
                        overwrites += 1;
                    } else {
                        preserves += 1;
                        mystatus.total_transfer_bytes -= mystatus.file_size;
                    }
                }
            }
            if overwrite {
                match progress {
                    ProgressIndicator::Text => {
                        print_status(status);
                    }
                    ProgressIndicator::None => {}
                }
            }
            overwrite
        })
        .await?;

    if processed_files.is_empty() && preserves == 0 {
        bail!("Path '{}' not found", path.display());
    }

    print_results(&processed_files, path, overwrites, preserves);

    Ok(())
}

// detects if a path contains a file at any level.
//   eg    /tmp/foo/somefile/bar/other
//   if somefile exists and is a file, it will be returned.
fn path_contains_file(path: &Path) -> Option<&Path> {
    let mut p: &Path = path;

    loop {
        if p.is_file() {
            return Some(p);
        }
        match p.parent() {
            Some(parent) => {
                p = parent;
            }
            None => break,
        }
    }
    None
}

// prints results/summary of GET transfer
fn print_results(
    processed_files: &BTreeMap<String, (String, String)>,
    path: &Path,
    overwrites: u64,
    preserves: u64,
) {
    if overwrites > 0 || preserves > 0 {
        println!(
            "Done. Retrieved {} {} to {}.\n  pre-existing: {}   (overwritten: {}  preserved: {})",
            processed_files.len(),
            pluralize("file", "files", processed_files.len() as u64),
            path.display(),
            overwrites + preserves,
            overwrites,
            preserves
        );
    } else {
        println!(
            "Done. Retrieved {} files to {}",
            processed_files.len(),
            path.display()
        );
    }
}

fn print_status(status: &FilesGetStatus) {
    // TBD: This is displaying pretty much all progress info, and it might be
    // information overload.
    println!(
        "{} - files: {} of {} ({:.0}%). transfer: {} of {} ({:.0}%), file: {} of {} ({:.0}%)",
        status.path_remote.display(),
        status.current_file,
        status.total_files,
        div_or(status.current_file as f64, status.total_files as f64, 1.0) * 100.0,
        status.transfer_bytes_written,
        status.total_transfer_bytes,
        div_or(
            status.transfer_bytes_written as f64,
            status.total_transfer_bytes as f64,
            1.0
        ) * 100.0,
        status.file_bytes_written,
        status.file_size,
        div_or(
            status.file_bytes_written as f64,
            status.file_size as f64,
            1.0
        ) * 100.0
    );
}

// Prompts user for [Y/n] input.
// TODO: make i18n friendly.
fn prompt_yes_no(prompt_msg: &str, default: &str) -> bool {
    let yes_no = "[Y/n]";
    let msg = format!("{}{}: ", prompt_msg, yes_no);
    loop {
        let choice = match prompt_user(&msg, "") {
            Ok(input) => input.to_uppercase(),
            Err(_) => default.to_string(),
        };
        match choice.as_str() {
            "Y" => {
                return true;
            }
            "N" => {
                return false;
            }
            _ => {}
        };
        // prevent scrolling after user hits Enter.
        // This is a partially successful attempt to keep progress bar
        // painting from getting screwed up.
        Term::stdout().clear_last_lines(1).unwrap_or(())
    }
}

/// # Downloads all files within a `FilesContainer` and writes them to disk, preserving paths.
///
/// TODO: In the future, this will have options for preserving symlinks and
/// file attributes.
async fn files_container_get_files(
    safe: &mut Safe,
    url: &str,
    dirpath: &str,
    callback: impl FnMut(&FilesGetStatus) -> bool,
) -> Result<(String, BTreeMap<String, (String, String)>)> {
    // Rather than returning a VersionHash, a String is returned, because there doesn't seem to be
    // a representation of an empty VersionHash just now. Not sure that it makes sense here to
    // generate a new one, since the version that's returned by this function is not used by the
    // caller. Previously we were returning 0 or a number, so it seems reasonable to return either
    // "0" or the VersionHash as a string (it implements the Display trait).
    debug!("Getting files in container {:?}", url);
    let (version, files_map) = match safe.fetch(url, None).await? {
        SafeData::FilesContainer {
            version, files_map, ..
        } => (version.map_or("".to_string(), |v| v.to_string()), files_map),
        SafeData::PublicFile { metadata, .. } => {
            if let Some(file_item) = metadata {
                let mut files_map = FilesMap::new();
                files_map.insert("".to_string(), file_item);
                ("0".to_string(), files_map)
            } else {
                // TODO: support it even if no stats are shown of the file being downloaded
                bail!(
                    "You can target files only by providing a FilesContainer with the file's path"
                );
            }
        }
        _other_type => bail!("Make sure the URL targets a FilesContainer"),
    };

    // Todo: This test will need to be modified once we support empty directories.
    let is_single_file = files_map.len() == 1;

    let safeurl = SafeUrl::from_url(url)?;
    let urlpath = safeurl.path_decoded()?;

    let root = find_root_path(dirpath, &urlpath, is_single_file)?;

    // This is a constraint to verify that parent of dirpath exists.
    // Without this check, files_map_get_files() will happily create
    // any missing dirs, which "might" be ok.  However, unix 'cp'
    // enforces that parent dir exists, so we will do the same to avoid
    // surprising users.
    ensure_parent_dir_exists(&root)?;

    let processed_files = files_map_get_files(safe, &files_map, &root, callback).await?;
    Ok((version, processed_files))
}

// Determines the root (translated) path to download files to.
// The root path is determined as per the follow matrix:
/*

source     |source type| dst                      | dst exists | dst type | translated
---------------------------------------------------------------------------------------
testdata   | dir       | /tmp/testdata             | Y           | dir       | /tmp/testdata/testdata
testdata   | dir       | /tmp/testdata             | Y           | file      | error:  cannot overwrite non-directory '/tmp/testdata' with directory './testdata/'
testdata   | dir       | /tmp/testdata             | N           | --        | /tmp/testdata

testdata   | dir       | /tmp/newname              | Y           | dir       | /tmp/newname/testdata
testdata   | dir       | /tmp/newname              | Y           | file      | error:  cannot overwrite non-directory '/tmp/testdata' with directory './testdata/'
testdata   | dir       | /tmp/newname              | N           | --        | /tmp/newname

-- source is a file --

testdata   | file      | /tmp/testdata             | Y           | dir       | /tmp/testdata/testdata
testdata   | file      | /tmp/testdata             | Y           | file      | /tmp/testdata
testdata   | file      | /tmp/testdata             | N           | --        | /tmp/testdata

testdata   | file      | /tmp/newname              | Y           | dir       | /tmp/newname/testdata
testdata   | file      | /tmp/newname              | Y           | file      | /tmp/newname
testdata   | file      | /tmp/newname              | N           | --        | /tmp/newname
*/
fn find_root_path(destpath: &str, sourcepath: &str, source_is_single_file: bool) -> Result<String> {
    // Note: The if+else clauses could be combined to be more
    // compact, but I am leaving it in expanded form to be more easily
    // understood in context of the path matrix in the fn comment.

    let mut root = Path::new(destpath).to_path_buf();
    if source_is_single_file && root.exists() {
        if root.is_dir() {
            let p = Path::new(sourcepath);
            if let Some(fname) = p.file_name() {
                root.push(fname);
            }
        }
    } else if root.exists() {
        if root.is_dir() {
            let p = Path::new(sourcepath);
            if let Some(fname) = p.file_name() {
                root.push(fname);
            }
        } else {
            let msg = format!(
                "cannot overwrite non-directory '{}' with a directory",
                destpath
            );
            bail!(msg);
        }
    }
    Ok(root.display().to_string())
}

// Verifies that parent directory of a given path exists.
fn ensure_parent_dir_exists(path: &str) -> Result<()> {
    let p = Path::new(path);

    // a relative path such as '.' or 'somedir' or 'somefile'
    // has an implicit parent.
    if p.is_relative() && p.components().count() == 1 {
        return Ok(());
    }

    if let Some(pa) = p.parent() {
        if pa.is_dir() {
            Ok(())
        } else {
            bail!("No such directory: \"{}\"", pa.display());
        }
    } else {
        // This should never happen.
        Err(eyre!("Parent directory not found for: \"{}\"", p.display()))
    }
}

/// # Downloads files within a `FilesMap` and writes them to disk, preserving paths.
///
/// TODO: In the future, this will have options for preserving file attributes.
async fn files_map_get_files(
    safe: &mut Safe,
    files_map: &FilesMap,
    dirpath: &str,
    mut callback: impl FnMut(&FilesGetStatus) -> bool,
) -> Result<BTreeMap<String, (String, String)>> {
    trace!("Fetching files from FilesMap");

    let dpath = Path::new(dirpath);

    let mut processed_files = BTreeMap::new();
    let mut transfer_bytes_written = 0;

    // We need to calc total_transfer_bytes in advance for status callback
    let mut total_transfer_bytes = files_map
        .iter()
        .map(|(_path, details)| &details["size"]) // todo: use FileItem::getattr()
        .fold(0, |tot, size| tot + size.parse().unwrap_or(0));

    // Loop through files map and download each file.
    // caller may cancel individual files, but not entire transfer.
    for (idx, (path, details)) in files_map.iter().enumerate() {
        let abspath = if !path.is_empty() {
            dpath.join(path.trim_matches('/'))
        } else {
            dpath.to_path_buf()
        };
        trace!("target path: {}", abspath.display());

        // determine the file size from metadata.  string must be parsed.
        let size_str = details.getattr("size")?;
        let size: u64 = size_str
            .parse()
            .context(format!("Invalid file size: {} for {}", size_str, path))?;

        // Setup status to notify our caller of progress in callback.
        let mut status = FilesGetStatus {
            path_remote: Path::new(path),
            path_local: abspath.as_path(),
            total_files: files_map.len() as u64,
            current_file: idx as u64 + 1,
            total_transfer_bytes,
            transfer_bytes_written,
            file_size: size,
            file_bytes_written: 0,
            file_type: details.getattr("type")?.to_string(),
        };

        // status callback before file download begins.
        let b_write = callback(&status);
        if !b_write {
            // If caller decides not to download this file, then we need to
            // deduct the file size from total bytes in transfer.
            total_transfer_bytes -= size;
            continue;
        }

        // If a directory, we just create and continue.
        if details.getattr("type")? == "inode/directory" {
            create_dir_all(&abspath)?;
            continue;
        }

        // ensure parent dir exists.
        let dir_path = match Path::new(&abspath).parent() {
            Some(p) => p,
            None => {
                let msg = "Could not get parent directory";
                processed_files.insert(path.to_string(), ("E".to_string(), format!("<{}>", msg)));
                warn!("Skipping file \"{}\". {}", path, msg);
                continue;
            }
        };
        create_dir_all(dir_path)?;

        if details.getattr("type")? == "inode/symlink" {
            create_symlink(
                Path::new(&denormalize_slashes(details.getattr("symlink_target")?)),
                &abspath,
                details.getattr("symlink_target_type")?,
            )?;
            continue;
        }

        // Note: must never get here if a directory/symlink.
        let xorurl = &details.getattr("link")?;

        // Download file
        match download_file_from_net(safe, xorurl, abspath.as_path(), size).await {
            Ok(file_bytes_written) => {
                processed_files.insert(path.to_string(), ("+".to_string(), xorurl.to_string()));
                transfer_bytes_written += file_bytes_written;
                status.transfer_bytes_written = transfer_bytes_written;
                status.file_bytes_written = file_bytes_written;

                // status callback for this file which has been downloaded.
                callback(&status);
            }
            Err(err) => {
                processed_files.insert(path.to_string(), processed_files_err_report(&err));
                info!("Skipping file \"{}\". {}", path, err);
            }
        };
    }

    Ok(processed_files)
}

#[cfg(unix)]
fn create_symlink_worker(
    target: &Path,
    link: &Path,
    _target_type: &str,
) -> Result<(), (String, String)> {
    std::os::unix::fs::symlink(target, link).map_err(|e| {
        (
            format!(
                "Could not create symlink: {} --> {}",
                link.display(),
                target.display()
            ),
            format!("{:?}", e),
        )
    })
}

#[cfg(windows)]
fn create_symlink_worker(
    target: &Path,
    link: &Path,
    target_type: &str,
) -> Result<(), (String, String)> {
    let result = if target_type == "dir" {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    };
    result.map_err(|e|
        (format!("Could not create symlink: {} --> {}\nPerhaps try 'Run as Administrator' or enable Windows Developer mode.",
            link.display(),
            target.display()),
         format!("{:?}", e)
        )
    )
}

fn create_symlink(target: &Path, link: &Path, target_type: &str) -> ApiResult<()> {
    info!(
        "creating symlink: {} --> {}",
        link.display(),
        target.display()
    );

    let result = create_symlink_worker(target, link, target_type);
    match result {
        Ok(_) => {}
        Err((msg, os_err)) => {
            warn!("{}", msg);
            warn!("{}", os_err);
            println!("{}", msg);
        }
    }

    Ok(())
}

fn denormalize_slashes(p: &str) -> String {
    p.replace('/', &std::path::MAIN_SEPARATOR.to_string())
}

// Downloads a file from the network to a given file path
// xorurl must point to a file
// size (in bytes) must be provided
async fn download_file_from_net(
    safe: &mut Safe,
    xorurl: &str,
    path: &Path,
    size: u64,
) -> Result<u64> {
    debug!("downloading file {} to {}", xorurl, path.display());

    // TODO: download the file by concurrently (spawning tasks/threads) pulling chunks.
    // The chunk_size can be based on https://stackoverflow.com/questions/8803515/optimal-buffer-size-for-write2
    // which can be 4096 to match common disk block size, but that seems a bit small for the
    // network, so I multiplied by 16. Perhaps should make it a param so caller can decide.
    let mut rcvd: u64 = 0;
    let mut bytes_written: u64 = 0;

    let fh = file_create(path)?;
    let mut stream = BufWriter::new(fh);

    // gets public or private, based on xorurl type
    let filedata = files_get(safe, xorurl, None).await?;
    bytes_written += stream_write(&mut stream, &filedata, path)? as u64;
    rcvd += filedata.len() as u64;
    trace!("received {} bytes of {}", rcvd, size,);

    // Close may generate an error, so we do a flush/sync first to detect such.
    // see https://github.com/rust-lang/rust/pull/63410#issuecomment-519965351
    let fh = bufwriter_into_inner(stream, path)?;
    file_sync_all(&fh, path)?;

    Ok(bytes_written as u64)
}

// syncs file to filesystem.
fn file_sync_all(f: &fs::File, path: &Path) -> Result<()> {
    f.sync_all()
        .with_context(|| format!("Error syncing file: \"{}\"", path.display(),))
}

// causes BufWriter to flush() file.
fn bufwriter_into_inner<W: Write>(w: BufWriter<W>, path: &Path) -> Result<W> {
    match w.into_inner() {
        Ok(inner) => Ok(inner),
        Err(err) => Err(eyre!("Error flushing file \"{}\": {}", path.display(), err)),
    }
}

// Writes data to a file/stream.
fn stream_write(writer: &mut dyn Write, data: &[u8], path: &Path) -> Result<usize> {
    writer
        .write(data)
        .with_context(|| format!("Error writing to file: \"{}\"", path.display(),))
}

// Creates a file, ready for writing.
fn file_create(path: &Path) -> Result<fs::File> {
    fs::File::create(path).with_context(|| format!("Couldn't create file: \"{}\"", path.display(),))
}

// create all directories in path if possible.
fn create_dir_all(dir_path: &Path) -> Result<()> {
    if dir_path.is_file() {
        bail!(
            "cannot overwrite non-directory '{}' with a directory",
            dir_path.display()
        );
    }
    fs::create_dir_all(dir_path)
        .with_context(|| format!("Couldn't create path: \"{}\"", dir_path.display(),))
}

/// # Get Public or Private file
/// Get immutable files from the network.
pub async fn files_get(safe: &mut Safe, url: &str, range: Range) -> Result<Vec<u8>> {
    match SafeUrl::from_url(url)?.data_type() {
        DataType::File => {
            let bytes = safe.files_get(url, range).await?;
            Ok(bytes.chunk().to_vec())
        }
        _ => Err(eyre!("URL target is not immutable data")),
    }
}
