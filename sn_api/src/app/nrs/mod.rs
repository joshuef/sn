// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod nrs_map;

pub use crate::app::multimap::Multimap;
pub use crate::safeurl::{ContentType, DataType, VersionHash};
pub use nrs_map::NrsMap;

use crate::{app::Safe, register::EntryHash, Error, Result, SafeUrl};

use log::{debug, info};
use std::collections::{BTreeMap, BTreeSet};
use std::str;

/// Type tag to use for the NrsMapContainer stored on Register
pub const NRS_MAP_TYPE_TAG: u64 = 1_500;

impl Safe {
    /// # Creates a `nrs_map_container` for a chosen top name
    /// ```
    /// safe://<subName>.<topName>/path/to/whatever?var=value
    ///        |-----------------|
    ///            Public Name
    /// ```
    /// Registers the given NRS top name on the network.
    /// Returns the NRS SafeUrl: `safe://{top_name}
    /// Note that this NRS SafeUrl is not linked to anything yet. You just registered the topname here.
    /// You can now associate public_names (with that topname) to links using `nrs_associate` or `nrs_add`
    pub async fn nrs_create(&self, top_name: &str) -> Result<SafeUrl> {
        info!("Creating an NRS map for: {}", top_name);

        let mut nrs_url = validate_nrs_top_name(top_name)?;
        nrs_url.set_content_type(ContentType::NrsMapContainer)?;
        let nrs_xorname = SafeUrl::from_nrsurl(&nrs_url.to_string())?.xorname();
        debug!("XorName for \"{:?}\" is \"{:?}\"", &nrs_url, &nrs_xorname);
        if self.nrs_get_subnames_map(top_name, None).await.is_ok() {
            return Err(Error::NrsNameAlreadyExists(top_name.to_owned()));
        }

        let _ = self
            .multimap_create(Some(nrs_xorname), NRS_MAP_TYPE_TAG)
            .await?;

        Ok(nrs_url)
    }

    /// # Associates a public name to a link
    /// The top name of the input public name needs to be registered first with `nrs_create`
    ///
    /// ```no_run
    /// safe://<subName>.<topName>/path/to/whatever?var=value
    ///        |-----------------|
    ///            Public Name
    /// ```
    /// Associates the given `public_name` to the link.
    /// Errors out if the topname is not registered.
    /// Returns the versioned NRS `SafeUrl` (containing a `VersionHash`) now pointing to the provided link:
    /// `safe://{public_name}?v={version_hash}`
    pub async fn nrs_associate(&self, public_name: &str, link: &SafeUrl) -> Result<SafeUrl> {
        info!(
            "Associating public name \"{}\" to \"{}\" in NRS map container",
            public_name, link
        );

        let mut url = validate_nrs_public_name(public_name)?;
        validate_nrs_url(link)?;

        let current_versions = self
            .fetch_multimap_values_by_key(&url, public_name.as_bytes())
            .await?
            .into_iter()
            .map(|(hash, _)| hash)
            .collect();

        let entry = (
            public_name.as_bytes().to_vec(),
            link.to_string().as_bytes().to_vec(),
        );
        let entry_hash = self
            .multimap_insert(&url.to_string(), entry, current_versions)
            .await?;
        set_nrs_url_props(&mut url, entry_hash)?;

        Ok(url)
    }

    /// # Associates any public name to a link
    ///
    /// Associates the given `public_name` to the link registering the topname on the way if needed.
    /// Returns the versioned NRS `SafeUrl` (containing a `VersionHash`) now pointing to the provided link:
    /// `safe://{public_name}?v={version_hash}`
    /// Also returns a bool to indicate whether it registered the topname in the process or not.
    pub async fn nrs_add(&self, public_name: &str, link: &SafeUrl) -> Result<(SafeUrl, bool)> {
        info!(
            "Adding public name \"{}\" to \"{}\" in an NRS map container",
            public_name, link
        );

        let url = validate_nrs_public_name(public_name)?;
        let top_name = url.top_name();
        let creation_result = self.nrs_create(top_name).await;
        let did_register_topname = match creation_result {
            Ok(_) => Ok(true),
            Err(Error::NrsNameAlreadyExists(_)) => Ok(false),
            Err(e) => Err(e),
        }?;

        let new_url = self.nrs_associate(public_name, link).await?;
        Ok((new_url, did_register_topname))
    }

    /// # Removes a public name
    /// The top name of the input public name needs to be registered first with `nrs_create`
    /// ```no_run
    /// safe://<subName>.<topName>/path/to/whatever?var=value
    ///        |-----------------|
    ///            Public Name
    /// ```
    /// Removes the given `public_name` from the `NrsMap` registered for the public name's top name
    /// on the network.
    /// Returns a versioned NRS `SafeUrl` (containing a `VersionHash`) pointing to the latest version
    /// (including the deletion) for the provided public name.
    /// `safe://{public_name}?v={version_hash}`
    pub async fn nrs_remove(&self, public_name: &str) -> Result<SafeUrl> {
        info!(
            "Removing public name \"{}\" from NRS map container",
            public_name
        );

        let mut url = validate_nrs_public_name(public_name)?;
        let current_versions = self
            .fetch_multimap_values_by_key(&url, public_name.as_bytes())
            .await?
            .into_iter()
            .map(|(hash, _)| hash)
            .collect();

        let entry_hash = self
            .multimap_remove(&url.to_string(), current_versions)
            .await?;
        set_nrs_url_props(&mut url, entry_hash)?;
        Ok(url)
    }

    /// # Gets a public name's associated link
    /// If no version is specified, returns the latest.
    /// The top name of the input public name needs to be registered first with `nrs_create`
    /// ```no_run
    /// safe://<subName>.<topName>/path/to/whatever?var=value
    ///        |-----------------|
    ///            Public Name
    /// ```
    /// Finds the `SafeUrl` associated with the given public name on the network.
    /// If multiple entries are found for the same public name, there's a conflict.
    /// If there are conflicts for subnames other than the one requested, get proceeds as usual,
    /// but the `NrsMap` returned will ignore those conflicts.
    /// Otherwise, it returns an error.
    /// Returns the associated `SafeUrl` for the given public name for that version along with an `NrsMap`
    pub async fn nrs_get(
        &self,
        public_name: &str,
        version: Option<VersionHash>,
    ) -> Result<(Option<SafeUrl>, NrsMap)> {
        info!(
            "Getting link for public name: {} for version: {:?}",
            public_name, version
        );

        // get nrs_map, ignoring conflicting entries if they are not the ones we're getting
        let nrs_map = match self.nrs_get_subnames_map(public_name, version).await {
            Ok(result) => Ok(result),
            Err(Error::ConflictingNrsEntries(str, conflicting_entries, map)) => {
                if conflicting_entries.iter().any(|(p, _)| p == public_name) {
                    Err(Error::ConflictingNrsEntries(str, conflicting_entries, map))
                } else {
                    Ok(map)
                }
            }
            Err(e) => Err(e),
        }?;

        let url = nrs_map.get(public_name)?;
        Ok((url, nrs_map))
    }

    /// Get the mapping of all subNames and their associated `SafeUrl` for the Nrs Map Container at the given public name
    pub async fn nrs_get_subnames_map(
        &self,
        public_name: &str,
        version: Option<VersionHash>,
    ) -> Result<NrsMap> {
        let url = SafeUrl::from_url(&format!("safe://{}", public_name))?;
        let mut multimap = match self.fetch_multimap(&url).await {
            Ok(s) => Ok(s),
            Err(Error::EmptyContent(_)) => Ok(BTreeSet::new()),
            Err(Error::ContentNotFound(e)) => Err(Error::ContentNotFound(format!(
                "No Nrs Map entry found at {}: {}",
                url, e
            ))),
            Err(e) => Err(Error::NetDataError(format!(
                "Failed to get Nrs Map entries: {}",
                e
            ))),
        }?;

        if let Some(version) = version {
            if !multimap
                .iter()
                .any(|(h, _)| VersionHash::from(h) == version)
            {
                let key_val = self
                    .fetch_multimap_value_by_hash(&url, version.entry_hash())
                    .await?;
                multimap.insert((version.entry_hash(), key_val));
            }
        }

        // The set may have duplicate entries; the map doesn't.
        let subnames_set = convert_multimap_to_nrs_set(&multimap, public_name, version)?;
        let nrs_map = get_nrs_map_from_set(&subnames_set)?;

        if nrs_map.map.len() != subnames_set.len() {
            let diff_set: BTreeSet<(String, SafeUrl)> = nrs_map.map.clone().into_iter().collect();
            let conflicting_entries: Vec<(String, SafeUrl)> =
                subnames_set.difference(&diff_set).cloned().collect();
            return Err(Error::ConflictingNrsEntries(
                "Found multiple entries for the same name. This happens when 2 clients write \
                concurrently to the same NRS mapping. It can be fixed by associating a new link to \
                the conflicting names."
                    .to_string(),
                conflicting_entries,
                nrs_map,
            ));
        }
        Ok(nrs_map)
    }
}

/// Converts the Multimap to a set, which may contain duplicate entries.
///
/// If the user has requested a specific version of a subname, only that version of it will be in
/// the set. The 'versioned set' is queried for all entries matching the given subname, then any
/// that *don't* match the specified version are removed.
fn convert_multimap_to_nrs_set(
    multimap: &Multimap,
    public_name: &str,
    subname_version: Option<VersionHash>,
) -> Result<BTreeSet<(String, SafeUrl)>> {
    if let Some(version) = subname_version {
        let mut versioned_set: BTreeSet<(VersionHash, String, SafeUrl)> = multimap
            .clone()
            .into_iter()
            .map(|x| {
                let version = VersionHash::from(&x.0);
                let kv = x.1;
                let public_name = str::from_utf8(&kv.0)?;
                let url = SafeUrl::from_url(str::from_utf8(&kv.1)?)?;
                Ok((version, public_name.to_owned(), url))
            })
            .collect::<Result<BTreeSet<(VersionHash, String, SafeUrl)>>>()?;
        let duplicate_entries = versioned_set
            .clone()
            .into_iter()
            .filter(|x| x.1 == public_name)
            .filter(|x| x.0 != version)
            .collect::<BTreeSet<(VersionHash, String, SafeUrl)>>();
        for entry in &duplicate_entries {
            versioned_set.remove(entry);
        }
        let set: BTreeSet<(String, SafeUrl)> = versioned_set
            .iter()
            .map(|x| (x.1.clone(), x.2.clone()))
            .collect::<BTreeSet<(String, SafeUrl)>>();
        return Ok(set);
    }

    let set: BTreeSet<(String, SafeUrl)> = multimap
        .clone()
        .into_iter()
        .map(|x| {
            let kv = x.1;
            let public_name = str::from_utf8(&kv.0)?;
            let url = SafeUrl::from_url(str::from_utf8(&kv.1)?)?;
            Ok((public_name.to_owned(), url))
        })
        .collect::<Result<BTreeSet<(String, SafeUrl)>>>()?;
    Ok(set)
}

fn get_nrs_map_from_set(set: &BTreeSet<(String, SafeUrl)>) -> Result<NrsMap> {
    // Duplicate entries are automatically removed from the set -> map conversion.
    let public_names_map: BTreeMap<String, SafeUrl> = set
        .clone()
        .into_iter()
        .map(|x| (x.0, x.1))
        .collect::<BTreeMap<String, SafeUrl>>();
    let nrs_map = NrsMap {
        map: public_names_map,
    };
    Ok(nrs_map)
}

fn set_nrs_url_props(url: &mut SafeUrl, entry_hash: EntryHash) -> Result<()> {
    url.set_content_version(Some(VersionHash::from(&entry_hash)));
    url.set_content_type(ContentType::NrsMapContainer)?;
    Ok(())
}

fn validate_nrs_top_name(top_name: &str) -> Result<SafeUrl> {
    let url = SafeUrl::from_url(&format!("safe://{}", top_name))?;
    if url.top_name() != top_name {
        return Err(Error::InvalidInput(format!(
            "The NRS top name \"{}\" is invalid because it contains url parts. Please \
                remove any path, version or subnames.",
            top_name
        )));
    }
    Ok(url)
}

fn validate_nrs_public_name(public_name: &str) -> Result<SafeUrl> {
    let url = SafeUrl::from_url(&format!("safe://{}", public_name))?;
    if url.public_name() != public_name {
        return Err(Error::InvalidInput(format!(
            "The NRS public name \"{}\" is invalid because it contains url parts. Please \
                remove any path or version.",
            public_name
        )));
    }
    Ok(url)
}

/// Helper to check if an NRS `SafeUrl`:
/// - is valid
/// - has a version (if its data is versionable)
fn validate_nrs_url(link: &SafeUrl) -> Result<()> {
    if link.content_version().is_none() {
        let content_type = link.content_type();
        let data_type = link.data_type();
        if content_type == ContentType::FilesContainer
            || content_type == ContentType::NrsMapContainer
        {
            return Err(Error::UnversionedContentError(format!(
                "{} content is versionable. NRS requires the supplied link to specify a version hash.",
                content_type
            )));
        } else if data_type == DataType::Register {
            return Err(Error::UnversionedContentError(format!(
                "{} content is versionable. NRS requires the supplied link to specify a version hash.",
                data_type
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::test_helpers::{new_safe_instance, random_nrs_name, TestDataFilesContainer},
        retry_loop_for_pattern, Error, SafeUrl,
    };
    use anyhow::{anyhow, bail, Result};
    use std::matches;

    const TEST_DATA_FILE: &str = "./testdata/test.md";

    #[tokio::test]
    async fn test_nrs_create() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let url = safe.nrs_create(&site_name).await?;

        assert_eq!(url.content_type(), ContentType::NrsMapContainer);
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_create_with_invalid_topname() -> Result<()> {
        let safe = new_safe_instance().await?;

        let invalid_top_name = "atffdgasd/d";
        let result = safe.nrs_create(invalid_top_name).await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            format!(
                "InvalidInput: The NRS top name \"{}\" is invalid because it contains url parts. \
                Please remove any path, version or subnames.",
                invalid_top_name
            ),
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_create_with_duplicate_topname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        safe.nrs_create(&site_name).await?;
        let _ = safe.nrs_get_subnames_map(&site_name, None).await?;
        let result = safe.nrs_create(&site_name).await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            format!("NrsNameAlreadyExists: {}", site_name),
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_topname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([]).await?;

        safe.nrs_create(&site_name).await?;
        let url = safe.nrs_associate(&site_name, &files_container.url).await?;

        assert_eq!(url.public_name(), site_name);
        assert!(url.content_version().is_some());
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 1);
        assert_eq!(
            *nrs_map.map.get(&site_name).ok_or_else(|| anyhow!(format!(
                "'{}' subname should have been present in retrieved NRS map",
                site_name
            )))?,
            files_container.url
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_subname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container(["/testdata/test.md"]).await?;
        let public_name = &format!("test.{site_name}");

        safe.nrs_create(&site_name).await?;
        let url = safe
            .nrs_associate(public_name, &files_container["/testdata/test.md"])
            .await?;

        assert_eq!(url.public_name(), public_name);
        assert!(url.content_version().is_some());
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 1);
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("test.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'test.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/test.md"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_multiple_subnames() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container =
            TestDataFilesContainer::get_container(["/testdata/test.md", "/testdata/another.md"])
                .await?;
        safe.nrs_create(&site_name).await?;
        safe.nrs_associate(
            &format!("test.{site_name}"),
            &files_container["/testdata/test.md"],
        )
        .await?;
        safe.nrs_associate(
            &format!("another.{site_name}"),
            &files_container["/testdata/another.md"],
        )
        .await?;

        // The last couple of tests verified the returned URLs are correct; for this test we don't
        // need that.
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 2);
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("test.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'test.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/test.md"]
        );
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("another.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'another.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/another.md"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_non_versioned_files_container_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([]).await?;
        let mut url = files_container.url.clone();
        url.set_content_version(None);
        let public_name = &format!("test.{site_name}");

        safe.nrs_create(&site_name).await?;
        let result = safe.nrs_associate(public_name, &url).await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "UnversionedContentError: FilesContainer content is versionable. NRS requires the \
            supplied link to specify a version hash."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_non_versioned_nrs_container_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let public_name = &format!("test.{site_name}");
        let mut nrs_container_url = safe.nrs_create(&site_name).await?;
        nrs_container_url.set_content_version(None);

        let result = safe.nrs_associate(public_name, &nrs_container_url).await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "UnversionedContentError: NrsMapContainer content is versionable. NRS requires the \
            supplied link to specify a version hash."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_register_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let register_link = safe
            .register_create(None, NRS_MAP_TYPE_TAG, ContentType::Raw)
            .await?;
        let mut register_url = SafeUrl::from_xorurl(&register_link)?;
        register_url.set_content_version(None);

        let result = safe
            .nrs_associate(&format!("test.{site_name}"), &register_url)
            .await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "UnversionedContentError: Register content is versionable. NRS requires the \
            supplied link to specify a version hash."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_associate_with_invalid_url() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container(["/testdata/test.md"]).await?;
        let public_name = &format!("test./{site_name}");

        safe.nrs_create(&site_name).await?;
        let result = safe
            .nrs_associate(public_name, &files_container["/testdata/test.md"])
            .await;
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            format!(
                "InvalidInput: The NRS public name \"{}\" is invalid because it contains url \
                parts. Please remove any path or version.",
                public_name
            )
        );
        Ok(())
    }

    /// Since nrs_add is a wrapper around nrs_create and nrs_associate, we won't re-test all
    /// the scenarios already covered by those and instead just provide this one test.
    #[tokio::test]
    async fn test_nrs_add_with_subname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container(["/testdata/test.md"]).await?;
        let public_name = &format!("test.{site_name}");

        let (_, topname_registered) = match safe
            .nrs_add(public_name, &files_container["/testdata/test.md"])
            .await
        {
            Ok(res) => res,
            Err(error) => bail!("Error during nrs add {error:?}"),
        };

        assert!(topname_registered);

        // we're retrying until an adult returns with the data we've just put.
        let nrs_map = retry_loop_for_pattern!(safe.nrs_get_subnames_map(&site_name, None), Ok(nrs_map) if nrs_map.map.len() > 0)?;

        assert_eq!(nrs_map.map.len(), 1, "nrs map has len 1");
        assert_eq!(
            *nrs_map.map.get(public_name).ok_or_else(|| anyhow!(format!(
                "'{public_name}' subname should have been present in retrieved NRS map"
            )))?,
            files_container["/testdata/test.md"],
            "added subname is correct"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_remove_with_subname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container =
            TestDataFilesContainer::get_container(["/testdata/test.md", "/testdata/another.md"])
                .await?;

        safe.nrs_create(&site_name).await?;
        safe.nrs_associate(
            &format!("test.{site_name}"),
            &files_container["/testdata/test.md"],
        )
        .await?;
        safe.nrs_associate(
            &format!("another.{site_name}"),
            &files_container["/testdata/another.md"],
        )
        .await?;
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 2);

        let url = safe.nrs_remove(&format!("another.{site_name}")).await?;

        assert_eq!(url.public_name(), &format!("another.{site_name}"));
        assert!(url.content_version().is_some());
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 1);
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("test.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'test.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/test.md"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_remove_with_topname() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([]).await?;

        safe.nrs_create(&site_name).await?;
        safe.nrs_associate(&site_name, &files_container.url).await?;
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 1);

        let url = safe.nrs_remove(&site_name).await?;

        assert_eq!(url.public_name(), site_name);
        assert!(url.content_version().is_some());
        let nrs_map = safe.nrs_get_subnames_map(&site_name, None).await?;
        assert_eq!(nrs_map.map.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_conflicting_names() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        // let's create an empty files container so we have a valid to link
        let (link, _, _) = safe
            .files_container_create_from(TEST_DATA_FILE, None, false, false)
            .await?;
        let (version0, _) = safe
            .files_container_get(&link)
            .await?
            .ok_or_else(|| anyhow!("files container was unexpectedly empty"))?;

        // associate a first name
        let mut valid_link = SafeUrl::from_url(&link)?;
        valid_link.set_content_version(Some(version0));

        let (nrs_url, did_create) = safe.nrs_add(&site_name, &valid_link).await?;
        assert!(did_create);

        let _ = safe.fetch(&nrs_url.to_string(), None).await?;

        // associate a second name
        let second_valid_link = SafeUrl::from_url(&link)?;
        valid_link.set_content_version(Some(version0));
        let site_name2 = format!("sub.{}", &site_name);

        let (nrs_url2, did_create) = safe.nrs_add(&site_name2, &second_valid_link).await?;
        assert!(!did_create);

        let _ = safe.fetch(&nrs_url2.to_string(), None).await?;

        // manually add a conflicting name
        let another_valid_url = nrs_url;
        let url = validate_nrs_top_name(&site_name)?;
        let entry = (
            site_name.as_bytes().to_vec(),
            another_valid_url.to_string().as_bytes().to_vec(),
        );
        let _ = safe
            .multimap_insert(&url.to_string(), entry, BTreeSet::new())
            .await?;

        // get of other name should be ok
        let (res_url, _) = safe.nrs_get(&site_name2, None).await?;
        assert_eq!(
            res_url.ok_or_else(|| anyhow!("url should not be None"))?,
            second_valid_link
        );

        // get of conflicting name should error out
        let conflict_error = safe.nrs_get(&site_name, None).await;
        assert!(matches!(
            conflict_error,
            Err(Error::ConflictingNrsEntries { .. })
        ));

        // check for the error content
        if let Err(Error::ConflictingNrsEntries(_, dups, _)) = conflict_error {
            let got_entries: Result<()> = dups.into_iter().try_for_each(|(public_name, url)| {
                assert_eq!(public_name, site_name);
                assert!(url == valid_link || url == another_valid_url);
                Ok(())
            });
            assert!(got_entries.is_ok());
        }

        // resolve the error
        let _ = safe.nrs_associate(&site_name, &valid_link).await?;

        // get should work now
        let (res_url, _) = safe.nrs_get(&site_name, None).await?;
        assert_eq!(
            res_url.ok_or_else(|| anyhow!("url should not be None"))?,
            valid_link
        );
        Ok(())
    }

    /// The scenario here is:
    /// * Register a topname
    /// * Associate a 'test' subname 3 times with different links
    /// * Associate an 'another' subname with a link
    ///
    /// We then retrieve the 'test' subname with the first version. We'd therefore expect the
    /// returned url to be the first link 'test' was associated with and for the entry in the NRS
    /// map to be the same link. There should also only be two entries in the map.
    #[tokio::test]
    async fn test_nrs_get_with_duplicate_subname_versions() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([
            "/testdata/test.md",
            "/testdata/another.md",
            "/testdata/noextension",
        ])
        .await?;
        let public_name = &format!("test.{site_name}");

        safe.nrs_create(&site_name).await?;
        let nrs_url = safe
            .nrs_associate(public_name, &files_container["/testdata/test.md"])
            .await?;
        let version = nrs_url
            .content_version()
            .ok_or_else(|| anyhow!("nrs_url should have a version"))?;
        safe.nrs_associate(public_name, &files_container["/testdata/another.md"])
            .await?;
        safe.nrs_associate(public_name, &files_container["/testdata/noextension"])
            .await?;
        safe.nrs_associate(
            &format!("another.{site_name}"),
            &files_container["/testdata/another.md"],
        )
        .await?;

        let (url, nrs_map) = safe.nrs_get(public_name, Some(version)).await?;
        assert_eq!(
            url.ok_or_else(|| anyhow!("url should not be None"))?,
            files_container["/testdata/test.md"]
        );
        assert_eq!(nrs_map.map.len(), 2);
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("test.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'test.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/test.md"]
        );
        assert_eq!(
            *nrs_map
                .map
                .get(&format!("another.{site_name}"))
                .ok_or_else(|| anyhow!(format!(
                    "'another.{site_name}' subname should have been present in retrieved NRS map"
                )))?,
            files_container["/testdata/another.md"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_get_with_topname_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([]).await?;

        safe.nrs_create(&site_name).await?;
        safe.nrs_associate(&site_name, &files_container.url).await?;

        let (url, _) = safe.nrs_get(&site_name, None).await?;

        assert_eq!(
            url.ok_or_else(|| anyhow!("url should not be None"))?,
            files_container.url
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_get_with_nrs_map_container_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container([]).await?;

        let nrs_url = safe.nrs_create(&site_name).await?;
        let nrs_map_container_url = SafeUrl::from_url(&nrs_url.to_xorurl_string())?;
        safe.nrs_associate(&site_name, &files_container.url).await?;

        let (url, _) = safe
            .nrs_get(nrs_map_container_url.public_name(), None)
            .await?;
        assert!(url.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_nrs_get_when_topname_has_no_link() -> Result<()> {
        let site_name = random_nrs_name();
        let safe = new_safe_instance().await?;

        let files_container = TestDataFilesContainer::get_container(["/testdata/test.md"]).await?;

        safe.nrs_create(&site_name).await?;
        safe.nrs_associate(
            &format!("test.{}", site_name),
            &files_container["/testdata/test.md"],
        )
        .await?;

        let (url, _) = safe.nrs_get(&site_name, None).await?;
        assert!(url.is_none());
        Ok(())
    }
}
