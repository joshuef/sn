// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    files_get::{process_get_command, FileExistsAction, ProgressIndicator},
    helpers::{
        gen_processed_files_table, get_from_arg_or_stdin, get_from_stdin, get_target_url, if_tty,
        notice_dry_run, parse_stdin_arg, pluralize, serialise_output,
    },
    OutputFmt,
};
use ansi_term::Colour;
use bytes::Bytes;
use clap::Subcommand;
use color_eyre::{eyre::bail, eyre::eyre, Result};
use comfy_table::Table;
use serde::Serialize;
use sn_api::{
    files::{FilesMap, ProcessedFiles},
    nrs::VersionHash,
    resolver::SafeData,
    Safe, SafeUrl, XorUrl,
};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Component, Path, PathBuf},
};
use tracing::debug;

type FileDetails = BTreeMap<String, String>;

const UNKNOWN_FILE_NAME: &str = "<unknown>";

// Differentiates between nodes in a file system.
#[derive(Debug, Serialize, PartialEq)]
enum FileTreeNodeType {
    File,
    Directory,
    Symlink,
}

// A recursive type to represent a directory tree.
// used by `safe files tree`
#[derive(Debug, Serialize)]
struct FileTreeNode {
    name: String,

    // This field could be useful in json output, because presently json
    // consumer cannot differentiate between an empty sub-directory and
    // a file. Though also at present, SAFE does not appear to store and
    // retrieve empty subdirectories.
    #[serde(skip)]
    fs_type: FileTreeNodeType,

    details: FileDetails,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    sub: Vec<FileTreeNode>,
}

impl FileTreeNode {
    // create a new FileTreeNode (either a Directory, File or Symlink)
    fn new(name: &str, fs_type: FileTreeNodeType, details: FileDetails) -> FileTreeNode {
        Self {
            name: name.to_string(),
            fs_type,
            details,
            sub: Vec::<FileTreeNode>::new(),
        }
    }

    // find's a (mutable) child node matching `name`
    fn find_child(&mut self, name: &str) -> Option<&mut FileTreeNode> {
        self.sub.iter_mut().find(|c| c.name == name)
    }

    // adds a child node
    // warning: does not enforce unique `name` between child nodes.
    fn add_child<T>(&mut self, leaf: T) -> &mut Self
    where
        T: Into<FileTreeNode>,
    {
        self.sub.push(leaf.into());
        self
    }
}

#[derive(Subcommand, Debug)]
pub enum FilesSubCommands {
    #[clap(name = "put")]
    /// Put a file or folder's files onto the SAFE Network
    Put {
        /// The source file/folder local path
        location: String,
        /// The destination path (in the FilesContainer) for the uploaded files and folders (default is '/')
        dst: Option<PathBuf>,
        /// Recursively upload folders and files found in the source location
        #[clap(short = 'r', long = "recursive")]
        recursive: bool,
        /// Follow symlinks
        #[clap(short = 'l', long = "follow-links")]
        follow_links: bool,
    },
    /// Get a file or folder from the SAFE Network
    Get {
        /// The target FilesContainer to retrieve from, optionally including the path to the directory or file within
        source: String,
        /// The local destination path for the retrieved files and folders (default is '.')
        dst: Option<String>,
        /// How to handle pre-existing files.
        #[clap(short = 'e', long = "exists", possible_values = &["ask", "preserve", "overwrite"], default_value="ask")]
        exists: FileExistsAction,
        /// How to display progress.
        #[clap(short = 'i', long = "progress", possible_values = &["text", "none"], default_value="text")]
        progress: ProgressIndicator,
        /// Preserves modification times, access times, and modes from the original file
        #[clap(short = 'p', long = "preserve")]
        preserve: bool,
    },
    #[clap(name = "sync")]
    /// Sync files to the SAFE Network
    Sync {
        /// The source location
        location: String,
        /// The target FilesContainer to sync up source files with, optionally including the destination path (default is '/')
        target: Option<String>,
        /// Recursively sync folders and files found in the source location
        #[clap(short = 'r', long = "recursive")]
        recursive: bool,
        /// Follow symlinks
        #[clap(short = 'l', long = "follow-links")]
        follow_links: bool,
        /// Delete files found at the target FilesContainer that are not in the source location. This is only allowed when --recursive is passed as well
        #[clap(short = 'd', long = "delete")]
        delete: bool,
        /// Automatically update the NRS name to link to the new version of the FilesContainer. This is only allowed if an NRS URL was provided, and if the NRS name is currently linked to a specific version of the FilesContainer
        #[clap(short = 'u', long = "update-nrs")]
        update_nrs: bool,
    },
    #[clap(name = "add")]
    /// Add a file to an existing FilesContainer on the network
    Add {
        /// The source file location.  Specify '-' to read from stdin
        #[clap(
            parse(from_str = parse_stdin_arg),
            requires_if("", "target"),
            requires_if("-", "target")
        )]
        location: String,
        /// The target FilesContainer to add the source file to, optionally including the destination path (default is '/') and new file name
        #[clap(parse(from_str = parse_stdin_arg))]
        target: Option<String>,
        /// Automatically update the NRS name to link to the new version of the FilesContainer. This is only allowed if an NRS URL was provided, and if the NRS name is currently linked to a specific version of the FilesContainer
        #[clap(short = 'u', long = "update-nrs")]
        update_nrs: bool,
        /// Overwrite the file on the FilesContainer if there already exists a file with the same name
        #[clap(short = 'f', long = "force")]
        force: bool,
        /// Follow symlinks
        #[clap(short = 'l', long = "follow-links")]
        follow_links: bool,
    },
    #[clap(name = "rm")]
    /// Remove a file from an existing FilesContainer on the network
    Rm {
        /// The full URL of the file to remove from its FilesContainer
        target: String,
        /// Automatically update the NRS name to link to the new version of the FilesContainer. This is only allowed if an NRS URL was provided, and if the NRS name is currently linked to a specific version of the FilesContainer
        #[clap(short = 'u', long = "update-nrs")]
        update_nrs: bool,
        /// Recursively remove files found in the target path
        #[clap(short = 'r', long = "recursive")]
        recursive: bool,
    },
    #[clap(name = "ls")]
    /// List files found in an existing FilesContainer on the network
    Ls {
        /// The target FilesContainer to list files from, optionally including a path (default is '/')
        target: Option<String>,
    },
    #[clap(name = "tree")]
    /// Recursively list files found in an existing FilesContainer on the network
    Tree {
        /// The target FilesContainer to list files from, optionally including a path (default is '/')
        target: Option<String>,
        /// Include file details
        #[clap(short = 'd', long = "details")]
        details: bool,
    },
}

pub async fn files_commander(
    cmd: FilesSubCommands,
    output_fmt: OutputFmt,
    safe: &mut Safe,
) -> Result<()> {
    match cmd {
        FilesSubCommands::Put {
            location,
            dst,
            recursive,
            follow_links,
        } => {
            // create FilesContainer from a given path to local files/folders
            if safe.dry_run_mode && OutputFmt::Pretty == output_fmt {
                notice_dry_run();
            }
            let (files_container_xorurl, processed_files, _) = safe
                .files_container_create_from(&location, dst.as_deref(), recursive, follow_links)
                .await?;

            // Now let's just print out a list of the files uploaded/processed
            if OutputFmt::Pretty == output_fmt {
                if safe.dry_run_mode {
                    println!("FilesContainer not created since running in dry-run mode");
                } else {
                    println!("FilesContainer created at: \"{}\"", files_container_xorurl);
                }

                let (table, _) = gen_processed_files_table(&processed_files, true);
                println!("{table}");
            } else {
                print_serialized_output(files_container_xorurl, None, &processed_files, output_fmt);
            }

            Ok(())
        }
        FilesSubCommands::Sync {
            location,
            target,
            recursive,
            follow_links,
            delete,
            update_nrs,
        } => {
            let target = get_from_arg_or_stdin(target, None)?;
            let mut target_url = get_target_url(&target)?;
            if safe.dry_run_mode && OutputFmt::Pretty == output_fmt {
                notice_dry_run();
            }
            // Update the FilesContainer on the Network
            let (content, processed_files) = safe
                .files_container_sync(
                    &location,
                    &target_url.to_string(),
                    recursive,
                    follow_links,
                    delete,
                    update_nrs,
                )
                .await?;
            let version = content.map(|(version, _)| version);

            // Now let's just print out a list of the files synced/processed
            let (table, success_count) = gen_processed_files_table(&processed_files, true);
            if OutputFmt::Pretty == output_fmt {
                let version_str = version.map_or("empty".to_string(), |v| format!("version {}", v));
                if success_count > 0 {
                    target_url.set_content_version(version);
                    target_url.set_path("");
                    println!(
                        "FilesContainer synced up ({}): \"{}\"",
                        version_str, target_url
                    );
                    println!("{table}");
                } else if !processed_files.is_empty() {
                    println!(
                        "No changes were made to FilesContainer ({}) at \"{}\"",
                        version_str, target_url
                    );
                    println!("{table}");
                } else {
                    println!(
                        "No changes were required, source location is already in sync with \
                        FilesContainer ({}) at: \"{}\"",
                        version_str, target
                    );
                }
            } else {
                print_serialized_output(target.to_string(), version, &processed_files, output_fmt);
            }
            Ok(())
        }
        FilesSubCommands::Add {
            location,
            target,
            update_nrs,
            follow_links,
            force,
        } => {
            // Validate that location and target are not both "", ie stdin.
            let target_url = target.unwrap_or_default();
            if target_url.is_empty() && location.is_empty() {
                bail!("Cannot read both <location> and <target> from stdin");
            }

            let target_url =
                get_from_arg_or_stdin(Some(target_url), Some("...awaiting target URl from STDIN"))?;

            if safe.dry_run_mode && OutputFmt::Pretty == output_fmt {
                notice_dry_run();
            }

            let (content, processed_files) =
                // If location is empty then we read arg from STDIN, which can still be a safe:// URL
                if location.is_empty() {
                    let file_content = get_from_stdin(Some("...awaiting file's content to add from STDIN"))?;
                    // Update the FilesContainer on the Network
                    safe.files_container_add_from_raw(Bytes::from(file_content), &target_url, force, update_nrs).await?
                } else {
                    // Update the FilesContainer on the Network
                    safe.files_container_add(&location, &target_url, force, update_nrs, follow_links).await?
                };

            // Now let's just print out a list of the files synced/processed
            output_processed_files_list(
                output_fmt,
                &processed_files,
                content.map(|(version, _)| version),
                target_url,
            );
            Ok(())
        }
        FilesSubCommands::Rm {
            target,
            update_nrs,
            recursive,
        } => {
            let target_url =
                get_from_arg_or_stdin(Some(target), Some("...awaiting target URl from STDIN"))?;

            if safe.dry_run_mode && OutputFmt::Pretty == output_fmt {
                notice_dry_run();
            }

            // Update the FilesContainer on the Network
            let (version, processed_files, _) = safe
                .files_container_remove_path(&target_url, recursive, update_nrs)
                .await?;

            // Now let's just print out a list of the files removed
            output_processed_files_list(output_fmt, &processed_files, Some(version), target_url);
            Ok(())
        }
        FilesSubCommands::Ls { target } => {
            let target_url =
                get_from_arg_or_stdin(target, Some("...awaiting target URl from STDIN"))?;

            debug!("Getting files in container {:?}", target_url);
            let mut resolution_chain = safe.inspect(&target_url).await?;
            let resolved_content = resolution_chain
                .pop()
                .ok_or_else(|| eyre!("Unexpectedly failed to obtain the resolved content"))?;

            let (version, files_map, total) = match resolved_content {
                SafeData::FilesContainer {
                    version, files_map, ..
                } => {
                    let (total, filtered_filesmap) = filter_files_map(&files_map, &target_url)?;
                    (version, filtered_filesmap, total)
                }
                SafeData::PublicFile { metadata, .. } => {
                    if let Some(file_item) = metadata {
                        let mut files_map = FilesMap::new();
                        let name = match file_item.get("name") {
                            Some(name) => name,
                            None => UNKNOWN_FILE_NAME,
                        };
                        files_map.insert(name.to_string(), file_item);

                        let container_version = match resolution_chain.pop() {
                            Some(SafeData::FilesContainer { version, .. }) => version,
                            _ => bail!("Unexpectedly failed to obtain the container's version"),
                        };

                        (container_version, files_map, 1)
                    } else {
                        bail!(
                            "You can target files only by providing a FilesContainer with the file's path"
                        );
                    }
                }
                _other_type => bail!("Make sure the URL targets a FilesContainer"),
            };

            if OutputFmt::Pretty == output_fmt {
                print_files_map(&files_map, total, version, &target_url);
            } else {
                println!("{}", serialise_output(&(target_url, files_map), output_fmt));
            }

            Ok(())
        }
        FilesSubCommands::Tree { target, details } => {
            process_tree_command(safe, target, details, output_fmt).await
        }
        FilesSubCommands::Get {
            source,
            dst,
            exists,
            progress,
            preserve,
        } => process_get_command(safe, source, dst, exists, progress, preserve, output_fmt).await,
    }
}

// processes the `safe files tree` command.
async fn process_tree_command(
    safe: &mut Safe,
    target: Option<XorUrl>,
    details: bool,
    output_fmt: OutputFmt,
) -> Result<()> {
    let target_url = get_from_arg_or_stdin(target, Some("...awaiting target URl from STDIN"))?;

    debug!("Getting files in container {:?}", target_url);
    let files_map = match safe.fetch(&target_url, None).await? {
        SafeData::FilesContainer { files_map, .. } => files_map,
        _other_type => bail!("Make sure the URL targets a FilesContainer"),
    };

    // Create a top/root node representing `target_url`.
    let mut top = FileTreeNode::new(
        &target_url,
        FileTreeNodeType::Directory,
        FileDetails::default(),
    );
    // Transform flat list in `files_map` to a hierarchy in `top`
    let mut files: u64 = 0;
    let mut dirs: u64 = 0;
    for (name, file_details) in &files_map {
        let path_parts: Vec<String> = name
            .to_string()
            .trim_matches('/')
            .split('/')
            .map(|s| s.to_string())
            .collect();
        let (d, f) = build_tree(&mut top, &path_parts, file_details, 0);
        files += f;
        dirs += d;
    }
    // Display.  with or without details.
    if OutputFmt::Pretty == output_fmt {
        if details {
            print_file_system_node_details(&top, dirs, files);
        } else {
            print_file_system_node(&top, dirs, files);
        }
    } else {
        println!("{}", serialise_output(&top, output_fmt));
    }

    Ok(())
}

fn print_serialized_output(
    xorurl: XorUrl,
    change_version: Option<VersionHash>,
    processed_files: &ProcessedFiles,
    output_fmt: OutputFmt,
) {
    let url = match SafeUrl::from_url(&xorurl) {
        Ok(mut safeurl) => {
            if change_version.is_some() {
                safeurl.set_content_version(change_version);
            }
            safeurl.to_string()
        }
        Err(_) => xorurl,
    };
    println!("{}", serialise_output(&(url, processed_files), output_fmt));
}

fn output_processed_files_list(
    output_fmt: OutputFmt,
    processed_files: &ProcessedFiles,
    version: Option<VersionHash>,
    target_url: String,
) {
    if OutputFmt::Pretty == output_fmt {
        let (table, success_count) = gen_processed_files_table(processed_files, true);
        if success_count > 0 {
            let url = match SafeUrl::from_url(&target_url) {
                Ok(mut safeurl) => {
                    safeurl.set_content_version(version);
                    safeurl.set_path("");
                    safeurl.to_string()
                }
                Err(_) => target_url,
            };

            println!(
                "FilesContainer updated (version {}): \"{}\"",
                version.map_or_else(|| 0.to_string(), |v| v.to_string()),
                url
            );
            println!("{table}");
        } else if !processed_files.is_empty() {
            println!(
                "No changes were made to FilesContainer (version {}) at \"{}\"",
                version.map_or_else(|| 0.to_string(), |v| v.to_string()),
                target_url
            );
            println!("{table}");
        } else {
            println!(
                "No changes were made to the FilesContainer (version {}) at: \"{}\"",
                version.map_or_else(|| 0.to_string(), |v| v.to_string()),
                target_url
            );
        }
    } else {
        print_serialized_output(target_url, version, processed_files, output_fmt);
    }
}

// Builds a file-system tree (hierarchy) from a single file path, split into its parts.
// May be called multiple times to expand the tree.
fn build_tree(
    node: &mut FileTreeNode,
    path_parts: &[String],
    details: &FileDetails,
    depth: usize,
) -> (u64, u64) {
    let mut dirs: u64 = 0;
    let mut files: u64 = 0;
    if depth < path_parts.len() {
        let item = &path_parts[depth];

        if item.is_empty() {
            return (dirs, files);
        }

        let node = match node.find_child(item) {
            Some(n) => n,
            None => {
                let (fs_type, d, di, fi) = match details["type"].as_str() {
                    "inode/directory" => (FileTreeNodeType::Directory, details, 1, 0),
                    "inode/symlink" => {
                        let target_type = details["symlink_target_type"].as_str();
                        let (dir, fil) = if target_type == "dir" { (1, 0) } else { (0, 1) };
                        (FileTreeNodeType::Symlink, details, dir, fil)
                    }
                    _ => (FileTreeNodeType::File, details, 0, 1),
                };

                dirs += di;
                files += fi;
                let n = FileTreeNode::new(item, fs_type, d.clone());
                node.add_child(n);
                // Very gross, but it works.
                // if this can be done in a better way,
                // please show me. We just need to return the node
                // that was added via add_child().  I tried modifying
                // add_child() to return it instead of &self, but couldn't
                // get it to work.  Also, using `n` does not work.

                match node.find_child(item) {
                    Some(n2) => n2,
                    None => panic!("But that's impossible!"),
                }
            }
        };
        let (di, fi) = build_tree(node, path_parts, details, depth + 1);
        dirs += di;
        files += fi;
    }
    (dirs, files)
}

// A function to print a FileTreeNode in format similar to unix `tree` command.
// prints a summary row below the main tree body.
fn print_file_system_node(dir: &FileTreeNode, dirs: u64, files: u64) {
    let mut siblings = HashMap::new();
    print_file_system_node_body(dir, 0, &mut siblings);

    // print summary row
    println!(
        "\n{} {}, {} {}",
        dirs,
        pluralize("directory", "directories", dirs),
        files,
        pluralize("file", "files", files),
    );
}

// generates tree body for print_file_system_node()
// operates recursively on `dir`
fn print_file_system_node_body(dir: &FileTreeNode, depth: u32, siblings: &mut HashMap<u32, bool>) {
    println!("{}", format_file_system_node_line(dir, depth, siblings));

    // And now, for some recursion...
    for (idx, child) in dir.sub.iter().enumerate() {
        let is_last = idx == dir.sub.len() - 1;
        siblings.insert(depth, !is_last);
        print_file_system_node_body(child, depth + 1, siblings);
    }
}

// A function to print a FileTreeNode in format similar to unix `tree` command.
// File details are displayed in a table to the left of the tree.
// prints a summary row below the main body.
fn print_file_system_node_details(dir: &FileTreeNode, dirs: u64, files: u64) {
    let mut siblings = HashMap::new();
    let mut table = Table::new();

    table.add_row(&vec!["SIZE", "CREATED", "MODIFIED", "NAME"]);

    print_file_system_node_details_body(dir, 0, &mut siblings, &mut table);

    println!("{table}");

    // print summary row
    println!(
        "\n{} {}, {} {}",
        dirs,
        pluralize("directory", "directories", dirs),
        files,
        pluralize("file", "files", files),
    );
}

// generates table body for print_file_system_node_details()
// operates recursively on `dir`
fn print_file_system_node_details_body(
    dir: &FileTreeNode,
    depth: u32,
    siblings: &mut HashMap<u32, bool>,
    table: &mut Table,
) {
    let name = format_file_system_node_line(dir, depth, siblings);

    let d = &dir.details;
    table.add_row(&vec![
        d.get("size").unwrap_or(&String::default()),
        d.get("created").unwrap_or(&String::default()),
        d.get("modified").unwrap_or(&String::default()),
        &name,
    ]);

    // And now, for some recursion...
    for (idx, child) in dir.sub.iter().enumerate() {
        let is_last = idx == dir.sub.len() - 1;
        siblings.insert(depth, !is_last);
        print_file_system_node_details_body(child, depth + 1, siblings, table);
    }
}

// Generates a single line when printing a FileTreeNode
// in unix `tree` format.
fn format_file_system_node_line(
    dir: &FileTreeNode,
    depth: u32,
    siblings: &mut HashMap<u32, bool>,
) -> String {
    if depth == 0 {
        siblings.insert(depth, false);
        if_tty(&dir.name, Colour::Blue.bold())
    } else {
        let is_last = !siblings[&(depth - 1)];
        let conn = if is_last { "└──" } else { "├──" };

        let mut buf: String = "".to_owned();
        for x in 0..depth - 1 {
            if siblings[&(x)] {
                buf.push_str("│   ");
            } else {
                buf.push_str("    ");
            }
        }
        let name = if dir.fs_type == FileTreeNodeType::Directory {
            if_tty(&dir.name, Colour::Blue.bold())
        } else if dir.fs_type == FileTreeNodeType::Symlink {
            format_symlink(&dir.name, &dir.details)
        } else {
            dir.name.clone()
        };
        format!("{}{} {}", buf, conn, name)
    }
}

fn format_symlink(name: &str, fd: &FileDetails) -> String {
    // display link name as cyan normally, or red if a broken link.
    let name_txt = match fd.get("symlink_target_type") {
        Some(t) if t == "unknown" => if_tty(name, Colour::Red.bold()),
        _ => if_tty(name, Colour::Cyan.bold()),
    };
    match fd.get("symlink_target") {
        Some(target) => {
            let target_txt = match fd.get("symlink_target_type") {
                Some(t) if t == "dir" => if_tty(target, Colour::Blue.bold()),
                _ => target.to_string(),
            };
            format!("{} -> {}", name_txt, target_txt)
        }
        None => name_txt, // this shouldn't happen.  means that FileDetail was created incorrectly.
    }
}

// A function to print a FilesMap in human-friendly table format.
fn print_files_map(
    files_map: &FilesMap,
    total_files: u64,
    version: Option<VersionHash>,
    target_url: &str,
) {
    println!(
        "Files of FilesContainer ({}) at \"{}\":",
        version.map_or("empty".to_string(), |v| format!("version {}", v)),
        target_url
    );
    let mut table = Table::new();

    let mut total_bytes = 0;
    let mut cwd_files = 0;
    let mut cwd_size = 0;

    // Columns in output:
    // 1. file/directory size,
    // 2. created timestamp,
    // 3. modified timestamp,
    // 4. file/directory name
    table.add_row(&vec!["SIZE", "CREATED", "MODIFIED", "NAME"]);
    for (name, file_item) in files_map.iter() {
        total_bytes += file_item["size"].parse().unwrap_or(0);
        if name.ends_with('/') {
            table.add_row(&vec![
                &file_item["size"],
                &file_item["created"],
                &file_item["modified"],
                name,
            ]);
        } else {
            if name.trim_matches('/').find('/').is_none() {
                cwd_size += file_item["size"].parse().unwrap_or(0);
                cwd_files += 1;
            }
            let name_field = if file_item["type"] == "inode/symlink" {
                format_symlink(name, file_item)
            } else {
                name.to_string()
            };

            table.add_row(&vec![
                &file_item["size"],
                &file_item["created"],
                &file_item["modified"],
                &name_field,
            ]);
        }
    }
    println!(
        "Files: {}   Size: {}   Total Files: {}   Total Size: {}",
        cwd_files, cwd_size, total_files, total_bytes
    );
    println!("{table}");
}

fn filter_files_map(files_map: &FilesMap, target_url: &str) -> Result<(u64, FilesMap)> {
    let mut filtered_filesmap = FilesMap::default();
    let mut safeurl = SafeUrl::from_url(target_url)?;
    let mut total = 0;

    for (filepath, fileitem) in files_map.iter() {
        // TODO:
        // for now we just filter out directory entries,
        // and use existing code-path.  We could however
        // refactor to display the mtime and ctime fields
        // from these directory FileItem(s).
        if fileitem["type"] == "inode/directory" {
            continue;
        }

        // Iterate through p_filepath with normalized path components
        // to populate a list of subdirs.
        let filepath_components = Path::new(filepath).components();
        let mut subdirs = Vec::<&str>::new();
        for p in filepath_components {
            // We only want 'normal' path components.
            // Other types, eg "../" will never match in a filemap.
            if let Component::Normal(comp) = p {
                match comp.to_str() {
                    Some(c) => subdirs.push(c),
                    None => bail!("Encountered invalid unicode sequence in path".to_string()),
                };
            } else {
                continue;
            }
        }

        if !subdirs.is_empty() {
            total += 1;

            // let's get base path of current file item
            let mut is_folder = false;
            let base_path = if subdirs.len() > 1 {
                is_folder = true;
                format!("{}/", subdirs[0])
            } else {
                subdirs[0].to_string()
            };

            // insert or merge current file item into the filtered list
            match filtered_filesmap.get_mut(&base_path) {
                None => {
                    let mut fileitem = fileitem.clone();
                    if is_folder {
                        // then set link to xorurl with path current subfolder
                        safeurl.set_path(subdirs[0]);
                        let link = safeurl.to_string();
                        fileitem.insert("link".to_string(), link);
                        fileitem.insert("type".to_string(), "".to_string());
                    }

                    filtered_filesmap.insert(base_path.to_string(), fileitem);
                }
                Some(item) => {
                    // current file item belongs to same base path as other files,
                    // we need to merge them together into the filtered list

                    // Add up files sizes
                    let current_dir_size = (*item["size"]).parse::<u32>().unwrap_or(0);
                    let additional_dir_size = fileitem["size"].parse::<u32>().unwrap_or(0);
                    (*item).insert(
                        "size".to_string(),
                        format!("{}", current_dir_size + additional_dir_size),
                    );

                    // If current file item's modified date is more recent
                    // set it as the folder's modififed date
                    if fileitem["modified"] > item["modified"] {
                        (*item).insert("modified".to_string(), fileitem["modified"].clone());
                    }

                    // If current file item's creation date is older than others
                    // set it as the folder's created date
                    if fileitem["created"] > item["created"] {
                        (*item).insert("created".to_string(), fileitem["created"].clone());
                    }
                }
            }
        }
    }

    Ok((total, filtered_filesmap))
}
