// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Implementation of the urls for the SAFE Network.

mod errors;
mod url_parts;
mod version_hash;
mod xorurl_media_types;

pub use errors::{Error, Result};
use multibase::{decode as base_decode, encode as base_encode, Base};
use sn_interface::types::{BytesAddress, DataAddress, RegisterAddress, SafeKeyAddress, Scope};
use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::{info, trace, warn};
use url::Url;
use url_parts::UrlParts;
pub use version_hash::VersionHash;
use xor_name::{XorName, XOR_NAME_LEN};
use xorurl_media_types::{MEDIA_TYPE_CODES, MEDIA_TYPE_STR};

/// Type tag to use for the NrsMapContainer stored on Register
pub const NRS_MAP_TYPE_TAG: u64 = 1_500;

/// Default base encoding used for XOR URLs
pub const DEFAULT_XORURL_BASE: XorUrlBase = XorUrlBase::Base32z;

const URL_PROTOCOL: &str = "safe://";
const URL_SCHEME: &str = "safe";
const XOR_URL_VERSION_1: u64 = 0x1; // TODO: consider using 16 bits
const XOR_URL_STR_MAX_LENGTH: usize = 44;
const XOR_NAME_BYTES_OFFSET: usize = 5; // offset where to find the XoR name bytes
const URL_VERSION_QUERY_NAME: &str = "v";

/// The XOR-URL type
pub type XorUrl = String;

/// Supported base encoding for XOR URLs
#[derive(Copy, Clone, Debug)]
pub enum XorUrlBase {
    #[allow(missing_docs)]
    Base32z,
    #[allow(missing_docs)]
    Base32,
    #[allow(missing_docs)]
    Base64,
}

impl std::str::FromStr for XorUrlBase {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self> {
        match str {
            "base32z" => Ok(Self::Base32z),
            "base32" => Ok(Self::Base32),
            "base64" => Ok(Self::Base64),
            other => Err(Error::InvalidInput(format!(
                "Invalid XOR URL base encoding: {}. Supported values are base32z, base32, and base64",
                other
            ))),
        }
    }
}

impl fmt::Display for XorUrlBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl XorUrlBase {
    #[allow(missing_docs)]
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Base32z),
            1 => Ok(Self::Base32),
            2 => Ok(Self::Base64),
            _other => Err(Error::InvalidInput("Invalid XOR URL base encoding code. Supported values are 0=base32z, 1=base32, and 2=base64".to_string())),
        }
    }

    #[allow(missing_docs)]
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::Base32z),
            1 => Ok(Self::Base32),
            2 => Ok(Self::Base64),
            _other => Err(Error::InvalidInput("Invalid XOR URL base encoding code. Supported values are 0=base32z, 1=base32, and 2=base64".to_string())),
        }
    }
}

/// We encode the content type that a XOR-URL is targetting, this allows the consumer/user to
/// treat the content in particular ways when the content requires it.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub enum ContentType {
    #[allow(missing_docs)]
    Raw,
    #[allow(missing_docs)]
    Wallet,
    #[allow(missing_docs)]
    FilesContainer,
    #[allow(missing_docs)]
    NrsMapContainer,
    #[allow(missing_docs)]
    Multimap,
    #[allow(missing_docs)]
    MediaType(String),
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ContentType {
    #[allow(missing_docs)]
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Wallet),
            2 => Ok(Self::FilesContainer),
            3 => Ok(Self::NrsMapContainer),
            4 => Ok(Self::Multimap),
            _other => Err(Error::InvalidInput("Invalid Media-type code".to_string())),
        }
    }

    #[allow(missing_docs)]
    pub fn value(&self) -> Result<u16> {
        match &*self {
            Self::Raw => Ok(0),
            Self::Wallet => Ok(1),
            Self::FilesContainer => Ok(2),
            Self::NrsMapContainer => Ok(3),
            Self::Multimap => Ok(4),
            Self::MediaType(media_type) => match MEDIA_TYPE_CODES.get(media_type) {
                Some(media_type_code) => Ok(*media_type_code),
                None => Err(Error::UnsupportedMediaType(format!("Media-type '{}' not supported. You can use 'ContentType::Raw' as the 'content_type' for this type of content", media_type))),
            },
        }
    }
}

/// We also encode the native SAFE data type where the content is being stored on the SAFE Network,
/// this allows us to fetch the targetted data using the corresponding API, regardless of the
/// data that is being held which is identified by the ContentType instead.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub enum DataType {
    #[allow(missing_docs)]
    SafeKey = 0x00,
    #[allow(missing_docs)]
    File = 0x01,
    #[allow(missing_docs)]
    Register = 0x02,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// An enumeration of possible SafeUrl types.
///
/// This is the type of safe url itself,
/// not the content it points to.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum UrlType {
    #[allow(missing_docs)]
    XorUrl,
    #[allow(missing_docs)]
    NrsUrl,
}

impl UrlType {
    #[allow(missing_docs)]
    pub fn value(&self) -> Result<u16> {
        match &*self {
            Self::XorUrl => Ok(0),
            Self::NrsUrl => Ok(1),
        }
    }
}

/// Represents a SafeUrl
///
/// A SafeUrl can be in one of two formats:  nrs or xor.
///   aka:  nrsurl or xorurl
///
/// Here is a breakdown of how name terminology is used.
///
/// case 1: safe://a.b.shinything:
///
///   public_name()    --> a.b.shinything
///   top_name()  --> shinything
///   sub_names()    --> a.b
///
/// case 2: safe://shinything:
///
///   public_name()   --> shinything
///   top_name() --> shinything
///   sub_names()   --> None
///
/// case 3: safe://a.b.hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///
///   public_name()   --> a.b.hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///   top_name() --> hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///   sub_names()   --> a.b
///
/// case 4: safe://hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///   public_name()   --> hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///   top_name() --> hnyynyzhjjjatqkfkjux8maaojtj8r59aphcnue6a11qgecpcebidkywmybnc
///   sub_names()   --> None
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub struct SafeUrl {
    encoding_version: u64,      // currently only v1 supported
    public_name: String,        // "a.b.name" in "a.b.name"
    top_name: String,           // "name" in "a.b.name"
    sub_names: String,          // "a.b" in "a.b.name"
    sub_names_vec: Vec<String>, // vec!["a", "b"] in "a.b.name"
    type_tag: u64,
    address: DataAddress,                 // See DataAddress
    content_type: ContentType,            // See ContentType
    content_type_u16: u16,                // validated u16 id of content_type
    path: String,                         // path, no separator, percent-encoded
    query_string: String,                 // query-string, no separator, url-encoded
    fragment: String,                     // fragment, no separator
    content_version: Option<VersionHash>, // convenience for ?v=<version
    url_type: UrlType,                    // nrsurl or xorurl
}

/// This implementation performs semi-rigorous validation,
/// when parsing a URL using ::from_url(), ::from_xorurl(),
/// or ::from_nrsurl().
///
/// However setters and new() do not enforce all the rules
/// and using them with invalid input can result in serializing
/// invalid URLs.  GIGO.
///
/// As such, it is recommended to check validity by
/// calling SafeUrl::validate() after instantiating
/// or modifying.
///
// TBD: In the future, we may want to perform all validity
// checks in the setters, however, this requires modifying
// setters to return a Result, which potentially impacts a
// bunch of code elsewhere.
impl SafeUrl {
    #[allow(clippy::too_many_arguments)]
    /// Instantiates a new SafeUrl
    ///
    /// Performs some basic validation checks, however it is
    /// possible to create invalid urls using this method.
    ///
    /// Arguments
    /// * `xor_name` - XorName hash
    /// * `nrs_name` - complete nrs name, or None for xorurl
    /// * `type_tag` - type tag
    /// * `data_type` - DataType
    /// * `content_type` - ContentType
    /// * `path` - must already be percent-encoded if Some. leading '/' optional.
    /// * `xorurl_sub_names` - sub_names. ignored if nrs_name is present.
    /// * `query_string` - must already be percent-encoded, without ? separator
    /// * `fragment` - url fragment, without # separator
    /// * `content_version` - overrides value of "?v" in query-string if not None.
    pub fn new(
        address: DataAddress,
        nrs_name: Option<&str>,
        type_tag: u64,
        content_type: ContentType,
        path: Option<&str>,
        sub_names: Option<Vec<String>>,
        query_string: Option<&str>,
        fragment: Option<&str>,
        content_version: Option<VersionHash>,
    ) -> Result<Self> {
        let content_type_u16 = content_type.value()?;

        let public_name: String;
        let top_name: String;
        let sub_names_str: String;
        let sub_names_vec: Vec<String>;
        let url_type: UrlType;
        if let Some(nh) = nrs_name {
            // we have an NRS SafeUrl
            if nh.is_empty() {
                let msg = "nrs_name cannot be empty string.".to_string();
                return Err(Error::InvalidInput(msg));
            }
            // Validate that nrs_name hash matches xor_name
            let tmpurl = format!("{}{}", URL_PROTOCOL, nh);
            let parts = UrlParts::parse(&tmpurl, false)?;
            let hashed_name = Self::xor_name_from_nrs_string(&parts.top_name);
            if &hashed_name != address.name() {
                let msg = format!(
                    "input mis-match. nrs_name `{}` does not hash to address.name() `{}`",
                    parts.top_name,
                    address.name()
                );
                return Err(Error::InvalidInput(msg));
            }
            public_name = parts.public_name;
            top_name = parts.top_name;
            sub_names_str = parts.sub_names;
            sub_names_vec = parts.sub_names_vec; // use sub_names from nrs_name, ignoring sub_names arg, in case they do not match.
            url_type = UrlType::NrsUrl;
        } else {
            // we have an xorurl
            public_name = String::default(); // set later
            top_name = String::default(); // set later
            sub_names_vec = sub_names.unwrap_or_default();
            sub_names_str = sub_names_vec.join(".");
            url_type = UrlType::XorUrl;

            for s in &sub_names_vec {
                if s.is_empty() {
                    let msg = "empty subname".to_string();
                    return Err(Error::InvalidInput(msg));
                }
            }
        }

        // finally, instantiate.
        let mut url = Self {
            encoding_version: XOR_URL_VERSION_1,
            address,
            public_name,
            top_name,
            sub_names: sub_names_str,
            sub_names_vec,
            type_tag,
            content_type,
            content_type_u16,
            path: String::default(),         // set below.
            query_string: String::default(), // set below.
            fragment: fragment.unwrap_or("").to_string(),
            content_version: None, // set below.
            url_type,
        };

        // now we can call ::name_to_base(), to generate the top_name.
        if url.url_type == UrlType::XorUrl {
            url.top_name = url.name_to_base(DEFAULT_XORURL_BASE, false);
            let sep = if url.sub_names.is_empty() { "" } else { "." };
            url.public_name = format!("{}{}{}", url.sub_names(), sep, url.top_name);
        }

        // we call this to add leading slash if needed
        // but we do NOT want percent-encoding as caller
        // must already provide it that way.
        url.set_path_internal(path.unwrap_or(""), false);

        // we set query_string and content_version using setters to
        // ensure they are in sync.
        url.set_query_string(query_string.unwrap_or(""))?;

        // If present, content_version will override ?v in query string.
        if let Some(version) = content_version {
            url.set_content_version(Some(version));
        }

        Ok(url)
    }

    /// A non-member utility function to check if a media-type is currently supported by XOR-URL encoding
    pub fn is_media_type_supported(media_type: &str) -> bool {
        MEDIA_TYPE_CODES.get(media_type).is_some()
    }

    /// Parses a safe url into SafeUrl
    ///
    /// # Arguments
    ///
    /// * `url` - either nrsurl or xorurl
    pub fn from_url(url: &str) -> Result<Self> {
        match Self::from_xorurl(url) {
            Ok(enc) => Ok(enc),
            Err(err) => {
                info!(
                    "Falling back to NRS. XorUrl decoding failed with: {:?}",
                    err
                );
                Self::from_nrsurl(url)
            }
        }
    }

    /// Parses an NRS SafeUrl into SafeUrl
    ///
    /// # Arguments
    ///
    /// * `nrsurl` - an nrsurl.
    pub fn from_nrsurl(nrsurl: &str) -> Result<Self> {
        let parts = UrlParts::parse(nrsurl, false)?;
        let hashed_name = Self::xor_name_from_nrs_string(&parts.top_name);
        let address = DataAddress::Register(RegisterAddress::new(
            hashed_name,
            Scope::Public,
            NRS_MAP_TYPE_TAG,
        ));

        Self::new(
            address,
            Some(&parts.public_name),
            NRS_MAP_TYPE_TAG,
            ContentType::NrsMapContainer,
            Some(&parts.path),
            Some(parts.sub_names_vec),
            Some(&parts.query_string),
            Some(&parts.fragment),
            None,
        )
    }

    /// Parses a XorUrl into SafeUrl
    ///
    /// # Arguments
    ///
    /// * `xorurl` - an xorurl.
    pub fn from_xorurl(xorurl: &str) -> Result<Self> {
        let parts = UrlParts::parse(xorurl, true)?;

        let (_base, xorurl_bytes): (Base, Vec<u8>) = base_decode(&parts.top_name)
            .map_err(|err| Error::InvalidXorUrl(format!("Failed to decode XOR-URL: {:?}", err)))?;

        let type_tag_offset = XOR_NAME_BYTES_OFFSET + XOR_NAME_LEN; // offset where to find the type tag bytes

        // check if too short
        if xorurl_bytes.len() < type_tag_offset {
            return Err(Error::InvalidXorUrl(format!(
                "Invalid XOR-URL, encoded string too short: {} bytes",
                xorurl_bytes.len()
            )));
        }

        // check if too long
        if xorurl_bytes.len() > XOR_URL_STR_MAX_LENGTH {
            return Err(Error::InvalidXorUrl(format!(
                "Invalid XOR-URL, encoded string too long: {} bytes",
                xorurl_bytes.len()
            )));
        }

        // let's make sure we support the XOR_URL version
        let u8_version: u8 = xorurl_bytes[0];
        let encoding_version: u64 = u64::from(u8_version);
        if encoding_version != XOR_URL_VERSION_1 {
            return Err(Error::InvalidXorUrl(format!(
                "Invalid or unsupported XOR-URL encoding version: {}",
                encoding_version
            )));
        }

        let mut content_type_bytes = [0; 2];
        content_type_bytes[0..].copy_from_slice(&xorurl_bytes[1..3]);
        let content_type = match u16::from_be_bytes(content_type_bytes) {
            0 => ContentType::Raw,
            1 => ContentType::Wallet,
            2 => ContentType::FilesContainer,
            3 => ContentType::NrsMapContainer,
            4 => ContentType::Multimap,
            other => match MEDIA_TYPE_STR.get(&other) {
                Some(media_type_str) => ContentType::MediaType((*media_type_str).to_string()),
                None => {
                    return Err(Error::InvalidXorUrl(format!(
                        "Invalid content type encoded in the XOR-URL string: {}",
                        other
                    )))
                }
            },
        };

        trace!(
            "Attempting to match content type of URL: {}, {:?}",
            &xorurl,
            content_type
        );

        let scope = match xorurl_bytes[3] {
            0 => Scope::Public,
            1 => Scope::Private,
            other => {
                return Err(Error::InvalidXorUrl(format!(
                    "Invalid scope encoded in the XOR-URL string: {}",
                    other
                )))
            }
        };

        let mut xor_name = XorName::default();
        xor_name
            .0
            .copy_from_slice(&xorurl_bytes[XOR_NAME_BYTES_OFFSET..type_tag_offset]);

        let type_tag_bytes_len = xorurl_bytes.len() - type_tag_offset;

        let mut type_tag_bytes = [0; 8];
        type_tag_bytes[8 - type_tag_bytes_len..].copy_from_slice(&xorurl_bytes[type_tag_offset..]);
        let type_tag: u64 = u64::from_be_bytes(type_tag_bytes);

        let address = match xorurl_bytes[4] {
            0 => DataAddress::SafeKey(SafeKeyAddress::new(xor_name, scope)),
            1 => DataAddress::Bytes(BytesAddress::new(xor_name, scope)),
            2 => DataAddress::Register(RegisterAddress::new(xor_name, scope, type_tag)),
            other => {
                return Err(Error::InvalidXorUrl(format!(
                    "Invalid data type encoded in the XOR-URL string: {}",
                    other
                )))
            }
        };

        Self::new(
            address,
            None, // no nrs_name for an xorurl
            type_tag,
            content_type,
            Some(&parts.path),
            Some(parts.sub_names_vec),
            Some(&parts.query_string),
            Some(&parts.fragment),
            None,
        )
    }

    /// The url scheme.  Only 'safe' scheme is presently supported.
    pub fn scheme(&self) -> &str {
        URL_SCHEME
    }

    /// returns encoding version of xorurl
    pub fn encoding_version(&self) -> u64 {
        self.encoding_version
    }

    /// returns SAFE data type
    pub fn data_type(&self) -> DataType {
        match self.address {
            DataAddress::Bytes(_) => DataType::File,
            DataAddress::Register(_) => DataType::Register,
            DataAddress::SafeKey(_) => DataType::SafeKey,
        }
    }

    /// returns SAFE content type
    pub fn content_type(&self) -> ContentType {
        self.content_type.clone()
    }

    /// sets the SAFE content type
    pub fn set_content_type(&mut self, content_type: ContentType) -> Result<()> {
        self.content_type_u16 = content_type.value()?;
        self.content_type = content_type;
        Ok(())
    }

    /// returns XorName
    pub fn xorname(&self) -> XorName {
        *self.address().name()
    }

    /// returns address
    pub fn address(&self) -> DataAddress {
        self.address
    }

    /// returns public_name portion of xorurl using the
    /// default xorurl encoding.
    ///
    /// public_name means sub_names + top_name
    ///
    /// useful for retrieving xorurl name associated
    /// with an nrsurl.
    ///
    /// For a different encoding, see name_to_base()
    pub fn xorurl_public_name(&self) -> String {
        self.name_to_base(DEFAULT_XORURL_BASE, true)
    }

    /// The public_name in url.  Either nrs_name or xor_name.
    ///
    /// eg a.b.name --> a.b.name
    pub fn public_name(&self) -> &str {
        &self.public_name
    }

    /// returns top name of name field.
    ///
    /// eg: a.b.name --> name
    pub fn top_name(&self) -> &str {
        &self.top_name
    }

    /// returns sub_names
    ///
    /// eg: a.b.name --> a.b
    pub fn sub_names(&self) -> &str {
        &self.sub_names
    }

    /// returns sub_names in an array slice
    ///
    /// eg: a.b.name --> &["a", "b"]
    pub fn sub_names_vec(&self) -> &[String] {
        &self.sub_names_vec
    }

    /// sets sub_names portion of URL
    pub fn set_sub_names(&mut self, sub_names: &str) -> Result<()> {
        let sep = if sub_names.is_empty() { "" } else { "." };
        let tmpurl = format!("{}{}{}{}", URL_PROTOCOL, sub_names, sep, self.top_name());
        let parts = UrlParts::parse(&tmpurl, true)?;
        self.sub_names = parts.sub_names;
        self.sub_names_vec = parts.sub_names_vec;
        self.public_name = parts.public_name;
        Ok(())
    }

    /// returns XorUrl type tag
    pub fn type_tag(&self) -> u64 {
        self.type_tag
    }

    /// returns XorUrl scope
    pub fn scope(&self) -> Scope {
        self.address().scope()
    }

    /// returns path portion of URL, percent encoded (unmodified).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// returns path portion of URL, percent decoded
    pub fn path_decoded(&self) -> Result<String> {
        Self::url_percent_decode(&self.path)
    }

    /// sets path portion of URL
    ///
    /// input string must not be percent-encoded.
    /// The encoding is done internally.
    ///
    /// leading slash is automatically added if necessary.
    pub fn set_path(&mut self, path: &str) {
        self.set_path_internal(path, true);
    }

    /// gets content version
    ///
    /// This is a shortcut method for getting the "?v=" query param.
    pub fn content_version(&self) -> Option<VersionHash> {
        self.content_version
    }

    /// sets content version
    ///
    /// This is a shortcut method for setting the "?v=" query param.
    ///
    /// # Arguments
    ///
    /// * `version` - u64 representing value of ?v=<val>
    pub fn set_content_version(&mut self, version: Option<VersionHash>) {
        // Convert Option<u64> to Option<&str>
        let version_string: String;
        let v_option = match version {
            Some(v) => {
                version_string = v.to_string();
                Some(version_string.as_str())
            }
            None => None,
        };

        // note: We are being passed a u64
        // which logically should never fail to be set.  Details of
        // this implementation presently require parsing the query
        // string, but that could change in the future without API changing.
        // eg: by storing parsed key/val pairs.
        // Parsing of the query string is checked/validated by
        // set_query_string().  Thus, it should never be invalid, else
        // we have a serious bug in SafeUrl impl.
        self.set_query_key(URL_VERSION_QUERY_NAME, v_option)
            .unwrap_or_else(|e| {
                warn!("{}", e);
            });
    }

    /// sets or unsets a key/val pair in query string.
    ///
    /// if val is Some, then key=val will be set in query string.
    ///    If there is more than one instance of key in query string,
    ///    there will be only one after this call.
    /// If val is None, then the key will be removed from query string.
    ///
    /// To set key without any value, pass Some<""> as the val.
    ///
    /// `val` should not be percent-encoded.  That is done internally.
    ///
    /// # Arguments
    ///
    /// * `key` - name of url query string var
    /// * `val` - an option representing the value, or none.
    pub fn set_query_key(&mut self, key: &str, val: Option<&str>) -> Result<()> {
        let mut pairs = url::form_urlencoded::Serializer::new(String::new());
        let url = Self::query_string_to_url(&self.query_string)?;
        let mut set_key = false;
        for (k, v) in url.query_pairs() {
            if k == key {
                // note: this will consolidate multiple ?k= into just one.
                if let Some(v) = val {
                    if !set_key {
                        let _res = pairs.append_pair(key, v);
                        set_key = true;
                    }
                }
            } else {
                let _res = pairs.append_pair(&k, &v);
            }
        }
        if !set_key {
            if let Some(v) = val {
                let _res = pairs.append_pair(key, v);
            }
        }

        self.query_string = pairs.finish();
        trace!("Set query_string: {}", self.query_string);

        if key == URL_VERSION_QUERY_NAME {
            self.set_content_version_internal(val)?;
        }

        Ok(())
    }

    /// sets query string.
    ///
    /// If the query string contains ?v=<version> then it
    /// will take effect as if set_content_version() had been
    /// called.
    ///
    /// # Arguments
    ///
    /// * `query` - percent-encoded key/val pairs.
    pub fn set_query_string(&mut self, query: &str) -> Result<()> {
        // ?v is a special case, so if it is contained in query string
        // we parse it and update our stored content_version.
        // tbd: another option could be to throw an error if input
        // contains ?v.
        let v_option = Self::query_key_last_internal(query, URL_VERSION_QUERY_NAME);
        self.set_content_version_internal(v_option.as_deref())?;

        self.query_string = query.to_string();
        Ok(())
    }

    /// Retrieves query string
    ///
    /// This contains the percent-encoded key/value pairs
    /// as seen in a url.
    pub fn query_string(&self) -> &str {
        &self.query_string
    }

    /// Retrieves query string, with ? separator if non-empty.
    pub fn query_string_with_separator(&self) -> String {
        let qs = self.query_string();
        if qs.is_empty() {
            qs.to_string()
        } else {
            format!("?{}", qs)
        }
    }

    /// Retrieves all query pairs, percent-decoded.
    pub fn query_pairs(&self) -> Vec<(String, String)> {
        Self::query_pairs_internal(&self.query_string)
    }

    /// Queries a key from the query string.
    ///
    /// Can return 0, 1, or many values because a given key
    /// may exist 0, 1, or many times in a URL query-string.
    pub fn query_key(&self, key: &str) -> Vec<String> {
        Self::query_key_internal(&self.query_string, key)
    }

    /// returns the last matching key from a query string.
    ///
    /// eg in safe://name?color=red&age=5&color=green&color=blue
    ///    blue would be returned when key is "color".
    pub fn query_key_last(&self, key: &str) -> Option<String> {
        Self::query_key_last_internal(&self.query_string, key)
    }

    /// returns the first matching key from a query string.
    ///
    /// eg in safe://name?color=red&age=5&color=green&color=blue
    ///    red would be returned when key is "color".
    pub fn query_key_first(&self, key: &str) -> Option<String> {
        Self::query_key_first_internal(&self.query_string, key)
    }

    /// sets url fragment
    pub fn set_fragment(&mut self, fragment: String) {
        self.fragment = fragment;
    }

    /// Retrieves url fragment, without # separator
    pub fn fragment(&self) -> &str {
        &self.fragment
    }

    /// Retrieves url fragment, with # separator if non-empty.
    pub fn fragment_with_separator(&self) -> String {
        if self.fragment.is_empty() {
            "".to_string()
        } else {
            format!("#{}", self.fragment)
        }
    }

    /// returns true if an NrsUrl, false if an XorUrl
    pub fn is_nrsurl(&self) -> bool {
        self.url_type == UrlType::NrsUrl
    }

    /// returns true if an XorUrl, false if an NrsUrl
    pub fn is_xorurl(&self) -> bool {
        self.url_type == UrlType::XorUrl
    }

    /// returns type of this url.
    ///
    /// for type of the linked content, see
    ///   ::content_type()
    pub fn url_type(&self) -> &UrlType {
        &self.url_type
    }

    // XOR-URL encoding format (var length from 37 to 45 bytes):
    // 1 byte for encoding version
    // 2 bytes for content type (enough to start including some MIME types also)
    // 1 byte for scope
    // 1 byte for data type
    // 32 bytes for XoR Name
    // and up to 8 bytes for type_tag
    // query param "v=" is treated as the content version

    /// serializes the URL to an XorUrl string.
    ///
    /// This function may be called on an NrsUrl and
    /// the corresponding XorUrl will be returned.
    pub fn to_xorurl_string(&self) -> String {
        self.to_base(DEFAULT_XORURL_BASE)
    }

    /// serializes the URL to an NrsUrl string.
    ///
    /// This function returns None when is_nrsurl() is false.
    pub fn to_nrsurl_string(&self) -> Option<String> {
        if !self.is_nrsurl() {
            return None;
        }

        let query_string = self.query_string_with_separator();
        let fragment = self.fragment_with_separator();

        let url = format!(
            "{}{}{}{}{}",
            URL_PROTOCOL, self.public_name, self.path, query_string, fragment
        );

        Some(url)
    }

    /// serializes entire xorurl using a particular base encoding.
    pub fn to_base(&self, base: XorUrlBase) -> String {
        let name = self.name_to_base(base, true);

        let query_string = self.query_string_with_separator();
        let fragment = self.fragment_with_separator();

        // serialize full xorurl
        format!(
            "{}{}{}{}{}",
            URL_PROTOCOL, name, self.path, query_string, fragment
        )
    }

    /// serializes name portion of xorurl using a particular base encoding.
    pub fn name_to_base(&self, base: XorUrlBase, include_subnames: bool) -> String {
        // let's set the first byte with the XOR-URL format version
        let mut cid_vec: Vec<u8> = vec![XOR_URL_VERSION_1 as u8];

        cid_vec.extend_from_slice(&self.content_type_u16.to_be_bytes());

        // push the scope byte
        cid_vec.push(self.address().scope() as u8);

        // push the data type byte
        cid_vec.push(self.data_type() as u8);

        // add the xor_name 32 bytes
        cid_vec.extend_from_slice(&self.address().name().0);

        // let's get non-zero bytes only from th type_tag
        let start_byte: usize = (self.type_tag.leading_zeros() / 8) as usize;
        // add the non-zero bytes of type_tag
        cid_vec.extend_from_slice(&self.type_tag.to_be_bytes()[start_byte..]);

        let base_encoding = match base {
            XorUrlBase::Base32z => Base::Base32Z,
            XorUrlBase::Base32 => Base::Base32Lower,
            XorUrlBase::Base64 => Base::Base64,
        };
        let top_name = base_encode(base_encoding, cid_vec);

        if include_subnames {
            let sub_names = self.sub_names();
            let sep = if sub_names.is_empty() { "" } else { "." };
            format!("{}{}{}", sub_names, sep, top_name)
        } else {
            top_name
        }
    }

    /// Utility function to perform url percent decoding.
    pub fn url_percent_decode(s: &str) -> Result<String> {
        match urlencoding::decode(s) {
            Ok(c) => Ok(c),
            Err(e) => Err(Error::InvalidInput(format!("{:#?}", e))),
        }
    }

    /// Utility function to perform url percent encoding.
    pub fn url_percent_encode(s: &str) -> String {
        urlencoding::encode(s)
    }

    /// Validates that a SafeUrl instance can be parsed correctly.
    ///
    /// SafeUrl::from_url() performs rigorous validation,
    /// however setters and new() do not enforce all the rules
    ///
    /// This routine enables a caller to easily validate
    /// that the present instance passes all validation checks
    pub fn validate(&self) -> Result<()> {
        let s = self.to_string();
        Self::from_url(&s).map(|_| ())
    }

    /// A non-member encoder function for convenience in some cases
    #[allow(clippy::too_many_arguments)]
    pub fn encode(
        address: DataAddress,
        nrs_name: Option<&str>,
        type_tag: u64,
        content_type: ContentType,
        path: Option<&str>,
        sub_names: Option<Vec<String>>,
        query_string: Option<&str>,
        fragment: Option<&str>,
        content_version: Option<VersionHash>,
        base: XorUrlBase,
    ) -> Result<String> {
        let url = SafeUrl::new(
            address,
            nrs_name,
            type_tag,
            content_type,
            path,
            sub_names,
            query_string,
            fragment,
            content_version,
        )?;

        Ok(url.to_base(base))
    }

    /// A non-member SafeKey encoder function for convenience
    pub fn encode_safekey(xor_name: XorName, base: XorUrlBase) -> Result<String> {
        SafeUrl::encode(
            DataAddress::SafeKey(SafeKeyAddress::new(xor_name, Scope::Public)),
            None,
            0,
            ContentType::Raw,
            None,
            None,
            None,
            None,
            None,
            base,
        )
    }

    /// A non-member bytes encoder function for convenience
    pub fn encode_bytes(
        address: BytesAddress,
        content_type: ContentType,
        base: XorUrlBase,
    ) -> Result<String> {
        SafeUrl::encode(
            DataAddress::Bytes(address),
            None,
            0,
            content_type,
            None,
            None,
            None,
            None,
            None,
            base,
        )
    }

    /// A non-member Register data URL encoder function for convenience
    pub fn encode_register(
        xor_name: XorName,
        type_tag: u64,
        scope: Scope,
        content_type: ContentType,
        base: XorUrlBase,
    ) -> Result<String> {
        SafeUrl::encode(
            DataAddress::Register(RegisterAddress::new(xor_name, scope, type_tag)),
            None,
            type_tag,
            content_type,
            None,
            None,
            None,
            None,
            None,
            base,
        )
    }

    // utility to generate a dummy url from a query string.
    fn query_string_to_url(query_str: &str) -> Result<Url> {
        let dummy = format!("file://dummy?{}", query_str);
        match Url::parse(&dummy) {
            Ok(u) => Ok(u),
            Err(_e) => {
                let msg = format!("Invalid query string: {}", query_str);
                Err(Error::InvalidInput(msg))
            }
        }
    }

    // utility to retrieve all unescaped key/val pairs from query string.
    //
    // note: It's not that efficient parsing query-string for each
    // get_key request, but the alternative would be storing pairs
    // in the struct when query string is set, which takes extra
    // space.  and since query-string is mostly opaque to us anyway,
    // I figure this is adequate for now, but might want to revisit
    // later if perf ever becomes important, eg for Client Apps.
    // API needn't change.
    fn query_pairs_internal(query_str: &str) -> Vec<(String, String)> {
        let url = match Self::query_string_to_url(query_str) {
            Ok(u) => u,
            Err(_) => {
                return Vec::<(String, String)>::new();
            }
        };

        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        pairs
    }

    // sets content_version property.
    //
    // This should never be called directly.
    // Use ::set_content_version() or ::set_query_key() instead.
    fn set_content_version_internal(&mut self, version_option: Option<&str>) -> Result<()> {
        if let Some(version_str) = version_option {
            let version = version_str.parse::<VersionHash>().map_err(|_e| {
                let msg = format!(
                    "{} param could not be parsed as VersionHash. invalid: '{}'",
                    URL_VERSION_QUERY_NAME, version_str
                );
                Error::InvalidInput(msg)
            })?;
            self.content_version = Some(version);
        } else {
            self.content_version = None;
        }
        trace!("Set version: {:#?}", self.content_version);
        Ok(())
    }

    // sets path portion of URL
    //
    // input path may be percent-encoded or not, but
    // percent_encode param must be set appropriately
    // to avoid not-encoded or double-encoded isues.
    //
    // leading slash is automatically added if necessary.
    fn set_path_internal(&mut self, path: &str, percent_encode: bool) {
        // fast path for empty string.
        if path.is_empty() {
            if !self.path.is_empty() {
                self.path = path.to_string();
            }
            return;
        }

        // impl note: this func tries to behave like Url::set_path()
        // with respect to percent-encoding each path component.
        //
        // tbd: It might be more correct to simply instantiate a
        // dummy URL and call set_path(), return path();
        // counter-argument is that Url::set_path() does not
        // prefix leading slash and allows urls to be created
        // that merge name and path together.
        let parts: Vec<&str> = path.split('/').collect();
        let mut new_parts = Vec::<String>::new();
        for (count, p) in parts.iter().enumerate() {
            if !p.is_empty() || count > 0 {
                if percent_encode {
                    new_parts.push(Self::url_percent_encode(p));
                } else {
                    new_parts.push(p.to_string());
                }
            }
        }
        let new_path = new_parts.join("/");

        let separator = if new_path.is_empty() { "" } else { "/" };
        self.path = format!("{}{}", separator, new_path);
    }

    // utility to query a key from a query string, percent-decoded.
    // Can return 0, 1, or many values because a given key
    // can exist 0, 1, or many times in a URL query-string.
    fn query_key_internal(query_str: &str, key: &str) -> Vec<String> {
        let pairs = Self::query_pairs_internal(query_str);
        let mut values = Vec::<String>::new();

        for (k, val) in pairs {
            if k == key {
                values.push(val);
            }
        }
        values
    }

    // utility to query a key from a query string, percent-decoded.
    // returns the last matching key.
    // eg in safe://name?color=red&age=5&color=green&color=blue
    //    blue would be returned when key is "color".
    fn query_key_last_internal(query_str: &str, key: &str) -> Option<String> {
        let matches = Self::query_key_internal(query_str, key);
        matches.last().map(|v| v.to_string())
    }

    // utility to query a key from a query string.
    // returns the last matching key.
    // eg in safe://name?color=red&age=5&color=green&color=blue
    //    blue would be returned when key is "color".
    fn query_key_first_internal(query_str: &str, key: &str) -> Option<String> {
        let matches = Self::query_key_internal(query_str, key);
        matches.first().map(|v| v.to_string())
    }

    fn xor_name_from_nrs_string(name: &str) -> XorName {
        XorName::from_content(name.as_bytes())
    }
}

impl fmt::Display for SafeUrl {
    /// serializes the URL to a string.
    ///
    /// an NrsUrl will be serialized in NrsUrl form.
    /// an XorUrl will be serialized in XorUrl form.
    ///
    /// See also:
    ///  * ::to_xorurl_string()
    ///  * ::to_nrs_url_string()
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let buf = if self.is_nrsurl() {
            match self.to_nrsurl_string() {
                Some(s) => s,
                None => {
                    warn!("to_nrsurl_string() return None when is_nrsurl() == true. '{}'.  This should never happen. Please investigate.", self.public_name);
                    return Err(fmt::Error);
                }
            }
        } else {
            self.to_xorurl_string()
        };
        write!(fmt, "{}", buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::{eyre::bail, eyre::eyre, Result};
    use rand::Rng;
    use sn_interface::types::{register::EntryHash, BytesAddress};

    macro_rules! verify_expected_result {
            ($result:expr, $pattern:pat $(if $cond:expr)?) => {
                match $result {
                    $pattern $(if $cond)? => Ok(()),
                    other => Err(eyre!("Expecting {}, got {:?}", stringify!($pattern), other)),
                }
            }
        }

    #[test]
    fn test_url_new_validation() -> Result<()> {
        // Tests some errors when calling Self::new()

        let xor_name = XorName(*b"12345678901234567890123456789012");
        let address = DataAddress::register(xor_name, Scope::Public, NRS_MAP_TYPE_TAG);

        // test: "Media-type '{}' not supported. You can use 'ContentType::Raw' as the 'content_type' for this type of content",
        let result = SafeUrl::new(
            address,
            None,
            NRS_MAP_TYPE_TAG,
            ContentType::MediaType("garbage/trash".to_string()),
            None,
            None,
            None,
            None,
            None,
        );
        verify_expected_result!(result, Err(Error::UnsupportedMediaType(err)) if err.contains("You can use 'ContentType::Raw'"))?;

        // test: "nrs_name cannot be empty string."
        let result = SafeUrl::new(
            address,
            Some(""), // passing empty string as nrs name
            NRS_MAP_TYPE_TAG,
            ContentType::NrsMapContainer,
            None,
            None,
            None,
            None,
            None,
        );
        verify_expected_result!(result, Err(Error::InvalidInput(err)) if err.contains("nrs_name cannot be empty string."))?;

        // test: "input mis-match. nrs_name `{}` does not hash to xor_name `{}`"
        let result = SafeUrl::new(
            address,
            Some("a.b.c"), // passing nrs name not matching xor_name.
            NRS_MAP_TYPE_TAG,
            ContentType::NrsMapContainer,
            None,
            None,
            None,
            None,
            None,
        );
        verify_expected_result!(result, Err(Error::InvalidInput(err)) if err.contains("does not hash to address.name()"))?;

        // test: "Host contains empty subname" (in nrs name)
        let result = SafeUrl::new(
            address,
            Some("a..b.c"), // passing empty sub-name in nrs name
            NRS_MAP_TYPE_TAG,
            ContentType::NrsMapContainer,
            None,
            None,
            None,
            None,
            None,
        );
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("name contains empty subname"))?;

        // test: "empty subname" (in xorurl sub_names)
        let result = SafeUrl::new(
            address,
            None, // not NRS
            NRS_MAP_TYPE_TAG,
            ContentType::NrsMapContainer,
            None,
            Some(vec!["a".to_string(), "".to_string(), "b".to_string()]),
            None,
            None,
            None,
        );
        verify_expected_result!(result, Err(Error::InvalidInput(err)) if err.contains("empty subname"))?;

        Ok(())
    }

    #[test]
    fn test_url_base32_encoding() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let address = DataAddress::bytes(xor_name, Scope::Public);

        let xorurl = SafeUrl::encode(
            address,
            None,
            0xa632_3c4d_4a32,
            ContentType::Raw,
            None,
            None,
            None,
            None,
            None,
            XorUrlBase::Base32,
        )?;

        let base32_xorurl =
            "safe://baeaaaaabgezdgnbvgy3tqojqgezdgnbvgy3tqojqgezdgnbvgy3tqojqgezkmmr4jvfde";
        assert_eq!(xorurl, base32_xorurl);
        Ok(())
    }

    #[test]
    fn test_url_base32z_encoding() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let xorurl = SafeUrl::encode_bytes(
            BytesAddress::Public(xor_name),
            ContentType::Raw,
            XorUrlBase::Base32z,
        )?;
        let base32z_xorurl = "safe://hyryyyyybgr3dgpbiga5uoqjogr3dgpbiga5uoqjogr3dgpbiga5uoqjogr3y";
        assert_eq!(xorurl, base32z_xorurl);
        Ok(())
    }

    #[test]
    fn test_url_base64_encoding() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let xorurl = SafeUrl::encode_register(
            xor_name,
            4_584_545,
            Scope::Public,
            ContentType::FilesContainer,
            XorUrlBase::Base64,
        )?;
        let base64_xorurl = "safe://mAQACAAIxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMkX0YQ";
        assert_eq!(xorurl, base64_xorurl);
        let url = SafeUrl::from_url(base64_xorurl)?;
        assert_eq!(base64_xorurl, url.to_base(XorUrlBase::Base64));
        assert_eq!("", url.path());
        assert_eq!(XOR_URL_VERSION_1, url.encoding_version());
        assert_eq!(xor_name, url.xorname());
        assert_eq!(4_584_545, url.type_tag());
        assert_eq!(Scope::Public, url.scope());
        assert_eq!(DataType::Register, url.data_type());
        assert_eq!(ContentType::FilesContainer, url.content_type());
        Ok(())
    }

    #[test]
    fn test_url_default_base_encoding() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let base32z_xorurl = "safe://hyryyyyybgr3dgpbiga5uoqjogr3dgpbiga5uoqjogr3dgpbiga5uoqjogr3y";
        let xorurl = SafeUrl::encode_bytes(
            BytesAddress::Public(xor_name),
            ContentType::Raw,
            DEFAULT_XORURL_BASE,
        )?;
        assert_eq!(xorurl, base32z_xorurl);
        Ok(())
    }

    #[test]
    fn test_url_decoding() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let type_tag: u64 = 0x0eef;
        let subdirs = "/dir1/dir2";
        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let content_version = VersionHash::from(&random_hash);
        let query_string = "k1=v1&k2=v2";
        let query_string_v = format!("{}&v={}", query_string, content_version);
        let fragment = "myfragment";
        let address = DataAddress::bytes(xor_name, Scope::Public);
        let xorurl = SafeUrl::encode(
            address,
            None,
            type_tag,
            ContentType::Raw,
            Some(subdirs),
            Some(vec!["subname".to_string()]),
            Some(query_string),
            Some(fragment),
            Some(content_version),
            XorUrlBase::Base32z,
        )?;
        let url = SafeUrl::from_url(&xorurl)?;

        assert_eq!(subdirs, url.path());
        assert_eq!(XOR_URL_VERSION_1, url.encoding_version());
        assert_eq!(xor_name, url.xorname());
        assert_eq!(type_tag, url.type_tag());
        assert_eq!(Scope::Public, url.scope());
        assert_eq!(DataType::File, url.data_type());
        assert_eq!(ContentType::Raw, url.content_type());
        assert_eq!(Some(content_version), url.content_version());
        assert_eq!(query_string_v, url.query_string());
        assert_eq!(fragment, url.fragment());
        Ok(())
    }

    #[test]
    fn test_url_decoding_with_path() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let type_tag: u64 = 0x0eef;
        let xorurl = SafeUrl::encode_register(
            xor_name,
            type_tag,
            Scope::Public,
            ContentType::Wallet,
            XorUrlBase::Base32z,
        )?;

        let xorurl_with_path = format!("{}/subfolder/file", xorurl);
        let url_with_path = SafeUrl::from_url(&xorurl_with_path)?;
        assert_eq!(xorurl_with_path, url_with_path.to_base(XorUrlBase::Base32z));
        assert_eq!("/subfolder/file", url_with_path.path());
        assert_eq!(XOR_URL_VERSION_1, url_with_path.encoding_version());
        assert_eq!(xor_name, url_with_path.xorname());
        assert_eq!(type_tag, url_with_path.type_tag());
        assert_eq!(Scope::Public, url_with_path.scope());
        assert_eq!(DataType::Register, url_with_path.data_type());
        assert_eq!(ContentType::Wallet, url_with_path.content_type());
        Ok(())
    }

    #[test]
    fn test_url_decoding_with_subname() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let type_tag: u64 = 0x0eef;
        let address = DataAddress::bytes(xor_name, Scope::Public);

        let xorurl_with_subname = SafeUrl::encode(
            address,
            None,
            type_tag,
            ContentType::NrsMapContainer,
            None,
            Some(vec!["sub".to_string()]),
            None,
            None,
            None,
            XorUrlBase::Base32z,
        )?;

        assert!(xorurl_with_subname.contains("safe://sub."));
        let url_with_subname = SafeUrl::from_url(&xorurl_with_subname)?;
        assert_eq!(
            xorurl_with_subname,
            url_with_subname.to_base(XorUrlBase::Base32z)
        );
        assert_eq!("", url_with_subname.path());
        assert_eq!(1, url_with_subname.encoding_version());
        assert_eq!(xor_name, url_with_subname.xorname());
        assert_eq!(type_tag, url_with_subname.type_tag());
        assert_eq!(&["sub"], url_with_subname.sub_names_vec());
        assert_eq!(
            ContentType::NrsMapContainer,
            url_with_subname.content_type()
        );
        Ok(())
    }

    #[test]
    fn encode_bytes_should_set_media_type() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let xorurl = SafeUrl::encode_bytes(
            BytesAddress::Public(xor_name),
            ContentType::MediaType("text/html".to_string()),
            XorUrlBase::Base32z,
        )?;

        let url = SafeUrl::from_url(&xorurl)?;
        assert_eq!(
            ContentType::MediaType("text/html".to_string()),
            url.content_type()
        );
        Ok(())
    }

    #[test]
    fn encode_bytes_should_set_data_type() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let xorurl = SafeUrl::encode_bytes(
            BytesAddress::Public(xor_name),
            ContentType::MediaType("text/html".to_string()),
            XorUrlBase::Base32z,
        )?;

        let url = SafeUrl::from_url(&xorurl)?;
        assert_eq!(url.data_type(), DataType::File);
        Ok(())
    }

    #[test]
    fn test_url_too_long() -> Result<()> {
        let xorurl =
            "safe://heyyynunctugo4ucp3a8radnctugo4ucp3a8radnctugo4ucp3a8radnctmfp5zq75zq75zq7";

        match SafeUrl::from_xorurl(xorurl) {
            Ok(_) => Err(eyre!(
                "Unexpectedly parsed an invalid (too long) xorurl".to_string(),
            )),
            Err(Error::InvalidXorUrl(msg)) => {
                assert!(msg.starts_with("Invalid XOR-URL, encoded string too long"));
                Ok(())
            }
            other => Err(eyre!("Error returned is not the expected one: {:?}", other)),
        }
    }

    #[test]
    fn test_url_too_short() -> Result<()> {
        let xor_name = XorName(*b"12345678901234567890123456789012");
        let xorurl = SafeUrl::encode_bytes(
            BytesAddress::Public(xor_name),
            ContentType::MediaType("text/html".to_string()),
            XorUrlBase::Base32z,
        )?;

        // TODO: we need to add checksum to be able to detect even 1 single char change
        let len = xorurl.len() - 2;
        match SafeUrl::from_xorurl(&xorurl[..len]) {
            Ok(_) => Err(eyre!(
                "Unexpectedly parsed an invalid (too short) xorurl".to_string(),
            )),
            Err(Error::InvalidXorUrl(msg)) => {
                assert!(msg.starts_with("Invalid XOR-URL, encoded string too short"));
                Ok(())
            }
            other => Err(eyre!("Error returned is not the expected one: {:?}", other)),
        }
    }

    #[test]
    fn test_url_query_key_first() -> Result<()> {
        let x = SafeUrl::from_url("safe://myname?name=John+Doe&name=Jane%20Doe")?;
        let name = x.query_key_first("name");
        assert_eq!(name, Some("John Doe".to_string()));

        Ok(())
    }

    #[test]
    fn test_url_query_key_last() -> Result<()> {
        let x = SafeUrl::from_url("safe://myname?name=John+Doe&name=Jane%20Doe")?;
        let name = x.query_key_last("name");
        assert_eq!(name, Some("Jane Doe".to_string()));

        Ok(())
    }

    #[test]
    fn test_url_query_key() -> Result<()> {
        let x = SafeUrl::from_url("safe://myname?name=John+Doe&name=Jane%20Doe")?;
        let name = x.query_key("name");
        assert_eq!(name, vec!["John Doe".to_string(), "Jane Doe".to_string()]);

        Ok(())
    }

    #[test]
    fn test_url_set_query_key() -> Result<()> {
        let mut x = SafeUrl::from_url("safe://myname?name=John+Doe&name=Jane%20Doe")?;

        // set_query_key should replace the multiple name= with a single instance.
        let peggy_sue = "Peggy Sue".to_string();
        x.set_query_key("name", Some(&peggy_sue))?;
        assert_eq!(x.query_key_first("name"), Some(peggy_sue.clone()));
        assert_eq!(x.query_key_last("name"), Some(peggy_sue));
        assert_eq!(x.to_string(), "safe://myname?name=Peggy+Sue");

        // None should remove the name param.
        x.set_query_key("name", None)?;
        assert_eq!(x.query_key_last("name"), None);
        assert_eq!(x.to_string(), "safe://myname");

        // Test setting an empty key.
        x.set_query_key("name", Some(""))?;
        x.set_query_key("age", Some("25"))?;
        assert_eq!(x.query_key_last("name"), Some("".to_string()));
        assert_eq!(x.query_key_last("age"), Some("25".to_string()));
        assert_eq!(x.to_string(), "safe://myname?name=&age=25");

        // Test setting content version via ?v=61342
        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let version_hash = VersionHash::from(&random_hash);
        x.set_query_key(URL_VERSION_QUERY_NAME, Some(&version_hash.to_string()))?;
        assert_eq!(
            x.query_key_last(URL_VERSION_QUERY_NAME),
            Some(version_hash.to_string())
        );
        assert_eq!(x.content_version(), Some(version_hash));

        // Test unsetting content version via ?v=None
        x.set_query_key(URL_VERSION_QUERY_NAME, None)?;
        assert_eq!(x.query_key_last(URL_VERSION_QUERY_NAME), None);
        assert_eq!(x.content_version(), None);

        // Test parse error for version via ?v=non-hash
        let result = x.set_query_key(URL_VERSION_QUERY_NAME, Some("non-hash"));
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_url_set_sub_names() -> Result<()> {
        let mut x = SafeUrl::from_url("safe://sub1.sub2.myname")?;
        assert_eq!(x.sub_names(), "sub1.sub2");
        assert_eq!(x.sub_names_vec(), ["sub1", "sub2"]);

        x.set_sub_names("s1.s2.s3")?;
        assert_eq!(x.sub_names(), "s1.s2.s3");
        assert_eq!(x.sub_names_vec(), ["s1", "s2", "s3"]);

        assert_eq!(x.to_string(), "safe://s1.s2.s3.myname");
        Ok(())
    }

    #[test]
    fn test_url_set_content_version() -> Result<()> {
        let mut x = SafeUrl::from_url("safe://myname?name=John+Doe&name=Jane%20Doe")?;

        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let version_hash = VersionHash::from(&random_hash);
        x.set_content_version(Some(version_hash));
        assert_eq!(
            x.query_key_first(URL_VERSION_QUERY_NAME),
            Some(version_hash.to_string())
        );
        assert_eq!(x.content_version(), Some(version_hash));
        assert_eq!(
            x.to_string(),
            format!(
                "safe://myname?name=John+Doe&name=Jane+Doe&v={}",
                version_hash
            )
        );

        x.set_content_version(None);
        assert_eq!(x.query_key_first(URL_VERSION_QUERY_NAME), None);
        assert_eq!(x.content_version(), None);
        assert_eq!(x.to_string(), "safe://myname?name=John+Doe&name=Jane+Doe");

        Ok(())
    }

    #[test]
    fn test_url_path() -> Result<()> {
        // Make sure we can read percent-encoded paths, and set them as well.
        // Here we verify that Url has the same path encoding behavior
        // as our implementation...for better or worse.
        let mut x = SafeUrl::from_url("safe://domain/path/to/my%20file.txt")?;
        let mut u = Url::parse("safe://domain/path/to/my%20file.txt").map_err(|e| {
            Error::InvalidInput(format!(
                "Unexpectedly failed to parse with third-party Url::parse: {}",
                e
            ))
        })?;

        assert_eq!(x.path(), "/path/to/my%20file.txt");
        assert_eq!(x.path(), u.path());

        x.set_path("/path/to/my new file.txt");
        u.set_path("/path/to/my new file.txt");
        assert_eq!(x.path(), "/path/to/my%20new%20file.txt");
        assert_eq!(x.path(), u.path());
        assert_eq!(x.path_decoded()?, "/path/to/my new file.txt");

        x.set_path("/trailing/slash/");
        u.set_path("/trailing/slash/");
        assert_eq!(x.path(), "/trailing/slash/");
        assert_eq!(x.path(), u.path());

        x.set_path("no-leading-slash");
        u.set_path("no-leading-slash");
        assert_eq!(x.path(), "/no-leading-slash");
        assert_eq!(x.path(), u.path());
        assert_eq!(x.to_string(), "safe://domain/no-leading-slash");
        assert_eq!(x.to_string(), u.to_string());

        x.set_path("");
        u.set_path("");
        assert_eq!(x.path(), ""); // no slash if path is empty.
        assert_eq!(x.path(), u.path());
        assert_eq!(x.to_string(), "safe://domain");
        assert_eq!(x.to_string(), u.to_string());

        // TODO: Url preserves the missing slash, and allows path to
        // merge with domain...seems kind of broken.  bug?
        x.set_path("/");
        u.set_path("/");
        assert_eq!(x.path(), "");
        assert_eq!(u.path(), "/"); // slash removed if path otherwise empty.
        assert_eq!(x.to_string(), "safe://domain"); // note that slash in path omitted.
        assert_eq!(u.to_string(), "safe://domain/");

        Ok(())
    }

    #[test]
    fn test_url_to_string() -> Result<()> {
        // These two are equivalent.  ie, the xorurl is the result of nrs.to_xorurl_string()
        let nrsurl = "safe://my.sub.domain/path/my%20dir/my%20file.txt?this=that&this=other&color=blue&v=hyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy&name=John+Doe#somefragment";
        let xorurl = "safe://my.sub.hyryygyynpm7tjdim96rdtz9kkyc758zqth5b3ynzya69njrjshgu7w84k3tomzy/path/my%20dir/my%20file.txt?this=that&this=other&color=blue&v=hyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy&name=John+Doe#somefragment";

        let nrs = SafeUrl::from_url(nrsurl)?;
        let xor = SafeUrl::from_url(xorurl)?;

        assert_eq!(nrs.to_string(), nrsurl);
        assert_eq!(xor.to_string(), xorurl);

        assert_eq!(nrs.to_nrsurl_string(), Some(nrsurl.to_string()));
        assert_eq!(nrs.to_xorurl_string(), xorurl);

        assert_eq!(xor.to_nrsurl_string(), None);
        assert_eq!(xor.to_xorurl_string(), xorurl);

        Ok(())
    }

    #[test]
    fn test_url_parts() -> Result<()> {
        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let nrsurl_version_hash = VersionHash::from(&random_hash);
        let random_hash = EntryHash(rand::thread_rng().gen::<[u8; 32]>());
        let xorurl_version_hash = VersionHash::from(&random_hash);

        // These two are equivalent.  ie, the xorurl is the result of nrs.to_xorurl_string()
        let nrsurl = format!("safe://my.sub.domain/path/my%20dir/my%20file.txt?this=that&this=other&color=blue&v={}&name=John+Doe#somefragment", nrsurl_version_hash);
        let xorurl = format!("safe://my.sub.hnyydyiixsfrqix9aoqg97jebuzc6748uc8rykhdd5hjrtg5o4xso9jmggbqh/path/my%20dir/my%20file.txt?this=that&this=other&color=blue&v={}&name=John+Doe#somefragment", xorurl_version_hash);

        let nrs = SafeUrl::from_url(&nrsurl)?;
        let xor = SafeUrl::from_url(&xorurl)?;

        assert_eq!(nrs.scheme(), URL_SCHEME);
        assert_eq!(xor.scheme(), URL_SCHEME);

        assert_eq!(nrs.public_name(), "my.sub.domain");
        assert_eq!(
            xor.public_name(),
            "my.sub.hnyydyiixsfrqix9aoqg97jebuzc6748uc8rykhdd5hjrtg5o4xso9jmggbqh"
        );

        assert_eq!(nrs.top_name(), "domain");
        assert_eq!(
            xor.top_name(),
            "hnyydyiixsfrqix9aoqg97jebuzc6748uc8rykhdd5hjrtg5o4xso9jmggbqh"
        );

        assert_eq!(nrs.sub_names(), "my.sub");
        assert_eq!(xor.sub_names(), "my.sub");

        assert_eq!(nrs.sub_names_vec(), ["my", "sub"]);
        assert_eq!(xor.sub_names_vec(), ["my", "sub"]);

        assert_eq!(nrs.path(), "/path/my%20dir/my%20file.txt");
        assert_eq!(xor.path(), "/path/my%20dir/my%20file.txt");

        assert_eq!(nrs.path_decoded()?, "/path/my dir/my file.txt");
        assert_eq!(xor.path_decoded()?, "/path/my dir/my file.txt");

        assert_eq!(
            nrs.query_string(),
            format!(
                "this=that&this=other&color=blue&v={}&name=John+Doe",
                nrsurl_version_hash
            )
        );
        assert_eq!(
            xor.query_string(),
            format!(
                "this=that&this=other&color=blue&v={}&name=John+Doe",
                xorurl_version_hash
            )
        );

        assert_eq!(nrs.fragment(), "somefragment");
        assert_eq!(xor.fragment(), "somefragment");

        assert_eq!(nrs.content_version(), Some(nrsurl_version_hash));
        assert_eq!(xor.content_version(), Some(xorurl_version_hash));

        Ok(())
    }

    #[test]
    fn test_url_from_url_validation() -> Result<()> {
        // Tests basic URL syntax errors that are common to
        // both ::from_xorurl() and ::from_nrsurl()

        let result = SafeUrl::from_url("withoutscheme");
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("relative URL without a base"))?;

        let result = SafeUrl::from_url("http://badscheme");
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("invalid scheme"))?;

        let result = SafeUrl::from_url("safe:///emptyname");
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("missing name"))?;

        let result = SafeUrl::from_url("safe://space in name");
        verify_expected_result!(result, Err(Error::InvalidInput(err)) if err.contains("The URL cannot contain whitespace"))?;

        let result = SafeUrl::from_url("safe://my.sub..name");
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("name contains empty subname"))?;

        let result = SafeUrl::from_url("safe://name//");
        verify_expected_result!(result, Err(Error::InvalidXorUrl(err)) if err.contains("path contains empty component"))?;

        // note: ?? is actually ok in a standard url.  I suppose no harm in allowing for safe
        // see:  https://stackoverflow.com/questions/2924160/is-it-valid-to-have-more-than-one-question-mark-in-a-url
        let _url = SafeUrl::from_url("safe://name??foo=bar")?;

        // note: ## and #frag1#frag2 are accepted by rust URL parser.
        // tbd: if we want to disallow.
        // see: https://stackoverflow.com/questions/10850781/multiple-hash-signs-in-url
        let _url = SafeUrl::from_url("safe://name?foo=bar##fragment")?;

        // note: single%percent/in/path is accepted by rust URL parser.
        // tbd: if we want to disallow.
        let _url = SafeUrl::from_nrsurl("safe://name/single%percent/in/path")?;

        Ok(())
    }

    #[test]
    fn test_url_from_xorurl_validation() -> Result<()> {
        // Tests some URL errors that are specific to xorurl

        let msg = "Expected error";
        let wrong_err = "Wrong error type".to_string();

        // test: "Failed to decode XOR-URL"
        let result = SafeUrl::from_xorurl("safe://invalidxor").expect_err(msg);
        match result {
            Error::InvalidXorUrl(e) => assert!(e.contains("Failed to decode XOR-URL")),
            _ => bail!(wrong_err),
        }

        // test: xorurl with a space in it
        let result = SafeUrl::from_xorurl(
            "safe://hnyydy iixsfrqix9aoqg97jebuzc6748uc8rykhdd5hjrtg5o4xso9jmggbqh",
        )
        .expect_err(msg);
        println!("{:#?}", result);
        match result {
            Error::InvalidInput(e) => assert!(e.contains("The URL cannot contain whitespace")),
            _ => bail!(wrong_err),
        }

        // note: too long/short have separate tests already.
        // "Invalid XOR-URL, encoded string too short"
        // "Invalid XOR-URL, encoded string too long"

        // todo: we should have tests for these.  help anyone?
        // "Invalid or unsupported XOR-URL encoding version: {}",
        // "Invalid content type encoded in the XOR-URL string: {}",
        // "Invalid data type encoded in the XOR-URL string: {}",

        Ok(())
    }

    #[test]
    fn test_url_validate() -> Result<()> {
        let nrsurl = "safe://my.sub.domain/path/my%20dir/my%20file.txt?this=that&this=other&color=blue&v=hyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy&name=John+Doe#somefragment";
        let trailing_slash = "safe://my.domain/";
        let double_q = "safe://my.domain/??foo=bar";

        let nrs = SafeUrl::from_url(nrsurl)?;
        let xor = SafeUrl::from_url(&nrs.to_xorurl_string())?;

        assert!(nrs.validate().is_ok());
        assert!(xor.validate().is_ok());

        assert!(SafeUrl::from_url(trailing_slash)?.validate().is_ok());
        assert!(SafeUrl::from_url(double_q)?.validate().is_ok());

        Ok(())
    }
}
