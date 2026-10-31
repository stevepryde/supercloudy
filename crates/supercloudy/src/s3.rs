pub(crate) mod faults;
mod http;
mod protocol;

#[cfg(test)]
mod properties;

use crate::fault_config::Faults;
use axum::{body::to_bytes, extract::Request, http::Method, response::Response};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use http::Fields;
use md5::{Digest, Md5};
use sha2::Sha256;
use std::{collections::BTreeMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::RwLock;

// Bound in-memory objects independently of DynamoDB's JSON request policy.
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
// Allow an additional MiB for aws-chunked framing and checksum trailers.
const MAX_WIRE_BODY_BYTES: usize = MAX_OBJECT_BYTES + 1024 * 1024;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(crate) struct Error {
    code: &'static str,
    message: String,
    range_total: Option<usize>,
}
impl Error {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            range_total: None,
        }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new("InvalidArgument", message)
    }
    fn unsupported(message: impl Into<String>) -> Self {
        Self::new("NotImplemented", message)
    }
    fn status(&self) -> u16 {
        match self.code {
            "NoSuchBucket" | "NoSuchKey" => 404,
            "AccessDenied" => 403,
            "InvalidRange" => 416,
            "NotImplemented" => 501,
            "SlowDown" => 503,
            "InternalError" => 500,
            _ => 400,
        }
    }
}

#[derive(Clone)]
struct Object {
    bytes: Arc<[u8]>,
    content_type: String,
    encoding: Option<String>,
    metadata: Fields,
    tags: Fields,
    checksum: Option<(String, String)>,
    etag: String,
    modified: OffsetDateTime,
    kms: Option<Option<String>>,
}

type Store = BTreeMap<String, BTreeMap<String, Object>>;

#[derive(Clone)]
pub(crate) struct Service {
    store: Arc<RwLock<Store>>,
    faults: Faults,
}

pub(crate) fn validate_bucket(name: &str) -> std::result::Result<(), String> {
    let bytes = name.as_bytes();
    if !(3..=63).contains(&bytes.len())
        || !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'.')
        || name.contains("..")
        || name.parse::<std::net::Ipv4Addr>().is_ok()
        || ["xn--", "sthree-", "amzn-s3-demo-"]
            .iter()
            .any(|p| name.starts_with(p))
        || ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|s| name.ends_with(s))
    {
        return Err("Invalid general-purpose bucket name".into());
    }
    Ok(())
}

impl Service {
    pub(crate) fn new(buckets: Vec<String>, faults: Faults) -> std::result::Result<Self, String> {
        let mut store = Store::new();
        for bucket in buckets {
            validate_bucket(&bucket)?;
            store.entry(bucket).or_default();
        }
        Ok(Self {
            store: Arc::new(RwLock::new(store)),
            faults,
        })
    }
    pub(crate) async fn dispatch(&self, request: Request) -> Response {
        let head = request.method() == Method::HEAD;
        let resource = request.uri().path().to_owned();
        match self.execute(request).await {
            Ok((status, headers, body)) => protocol::response(status, headers, body, head),
            Err(error) => protocol::error(error, &resource, head),
        }
    }

    async fn execute(&self, request: Request) -> Result<(u16, Fields, Vec<u8>)> {
        let now = OffsetDateTime::now_utc();
        let query = http::query(request.uri())?;
        http::presign(&query, now)?;
        let (bucket, key) = http::address(request.uri(), request.headers())?;
        if bucket.is_empty() {
            return Err(Error::unsupported("S3 operation requires a bucket"));
        }
        validate_bucket(&bucket)
            .map_err(|_| Error::new("InvalidBucketName", "Invalid bucket name"))?;
        let mut fields = http::headers(request.headers(), &query)?;
        validate_options(&fields, &query)?;
        let method = request.method().clone();
        let operation = match (&method, key.as_ref()) {
            (&Method::PUT, None) => "CreateBucket",
            (&Method::PUT, Some(_)) if fields.contains_key("x-amz-copy-source") => "CopyObject",
            (&Method::PUT, Some(_)) => "PutObject",
            (&Method::GET, Some(_)) => "GetObject",
            (&Method::HEAD, Some(_)) => "HeadObject",
            (&Method::DELETE, Some(_)) => "DeleteObject",
            (&Method::GET, None) if query.get("list-type").is_some_and(|v| v == "2") => {
                "ListObjectsV2"
            }
            _ => return Err(Error::unsupported("Unsupported S3 operation")),
        };
        if query.contains_key("list-type") && operation != "ListObjectsV2" {
            return Err(Error::invalid("Invalid listing selector"));
        }
        if operation != "ListObjectsV2"
            && ["prefix", "max-keys", "continuation-token", "encoding-type"]
                .iter()
                .any(|name| query.contains_key(*name))
        {
            return Err(Error::unsupported("Listing options require ListObjectsV2"));
        }
        if operation != "CopyObject"
            && [
                "x-amz-copy-source",
                "x-amz-metadata-directive",
                "x-amz-tagging-directive",
                "x-amz-checksum-algorithm",
            ]
            .iter()
            .any(|name| fields.contains_key(*name))
        {
            return Err(Error::unsupported("Copy options require CopyObject"));
        }
        if operation != "GetObject" && query.contains_key("response-content-disposition") {
            return Err(Error::unsupported(
                "Response disposition requires GetObject",
            ));
        }
        if !matches!(operation, "GetObject" | "HeadObject") && fields.contains_key("range") {
            return Err(Error::unsupported("Range requires GetObject"));
        }
        if operation == "HeadObject" && fields.contains_key("range") {
            return Err(Error::unsupported("Range on HEAD is unsupported"));
        }
        let disposition = query.get("response-content-disposition").cloned();
        if let Some(value) = &disposition {
            axum::http::HeaderValue::from_str(value)
                .map_err(|_| Error::invalid("Invalid response disposition"))?;
        }
        let checksum_read = match fields.get("x-amz-checksum-mode").map(String::as_str) {
            None => false,
            Some("ENABLED") => true,
            _ => return Err(Error::invalid("Invalid checksum mode")),
        };
        let wire = to_bytes(request.into_body(), MAX_WIRE_BODY_BYTES)
            .await
            .map_err(|error| {
                use std::error::Error as _;
                if error
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                {
                    Error::new("EntityTooLarge", "Request body exceeds supported size")
                } else {
                    Error::new("InvalidRequest", "Unable to read request body")
                }
            })?;
        let bytes = http::body(&wire, &mut fields)?;
        if bytes.len() > MAX_OBJECT_BYTES {
            return Err(Error::new("EntityTooLarge", "Object exceeds 64 MiB limit"));
        }
        match operation {
            "CreateBucket" => {
                create_configuration(&bytes)?;
                let mut store = self.store.write().await;
                self.fault(operation, &bucket, None)?;
                store.entry(bucket.clone()).or_default();
                drop(store);
                Ok((
                    200,
                    Fields::from([("location".into(), format!("/{bucket}"))]),
                    Vec::new(),
                ))
            }
            "PutObject" => {
                let object = object(bytes, &fields, now)?;
                let mut store = self.store.write().await;
                let objects = store.get_mut(&bucket).ok_or_else(no_bucket)?;
                self.fault(operation, &bucket, key.as_deref())?;
                objects.insert(key.unwrap(), object.clone());
                drop(store);
                let mut headers = Fields::new();
                protocol::write_headers(&object, true, &mut headers);
                Ok((200, headers, Vec::new()))
            }
            "CopyObject" => {
                let source = http::copy_source(&fields["x-amz-copy-source"])?;
                let metadata = directive(&fields, "x-amz-metadata-directive")?;
                let tagging = directive(&fields, "x-amz-tagging-directive")?;
                let attributes = object(Vec::new(), &fields, now)?;
                let algorithm = fields
                    .get("x-amz-checksum-algorithm")
                    .map(|s| algorithm(s))
                    .transpose()?;
                let mut store = self.store.write().await;
                if !store.contains_key(&bucket) {
                    return Err(no_bucket());
                }
                let mut copied = store
                    .get(&source.0)
                    .ok_or_else(no_bucket)?
                    .get(&source.1)
                    .cloned()
                    .ok_or_else(no_key)?;
                if metadata == "REPLACE" {
                    copied.content_type = attributes.content_type;
                    copied.metadata = attributes.metadata;
                    copied.encoding = attributes.encoding;
                }
                if tagging == "REPLACE" {
                    copied.tags = attributes.tags;
                }
                copied.kms = attributes.kms;
                copied.modified = now;
                if let Some(algorithm) = algorithm {
                    copied.checksum = Some((algorithm.into(), digest(algorithm, &copied.bytes)));
                }
                self.fault(operation, &bucket, key.as_deref())?;
                store
                    .get_mut(&bucket)
                    .unwrap()
                    .insert(key.unwrap(), copied.clone());
                drop(store);
                let checksum = copied
                    .checksum
                    .as_ref()
                    .map(|(a, v)| format!("<Checksum{a}>{}</Checksum{a}>", protocol::xml(v)))
                    .unwrap_or_default();
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ETag>{}</ETag><LastModified>{}</LastModified>{checksum}</CopyObjectResult>",
                    protocol::xml(&copied.etag),
                    protocol::timestamp(&copied)
                );
                let mut headers = xml_headers();
                protocol::write_headers(&copied, true, &mut headers);
                Ok((200, headers, xml.into_bytes()))
            }
            "GetObject" | "HeadObject" => {
                let store = self.store.read().await;
                let object = store
                    .get(&bucket)
                    .ok_or_else(no_bucket)?
                    .get(key.as_ref().unwrap())
                    .cloned()
                    .ok_or_else(no_key)?;
                let range = fields
                    .get("range")
                    .map(|s| byte_range(s, object.bytes.len()))
                    .transpose()?;
                self.fault(operation, &bucket, key.as_deref())?;
                drop(store);
                let mut headers =
                    protocol::object_headers(&object, checksum_read && range.is_none());
                if let Some(disposition) = disposition {
                    headers.insert("content-disposition".into(), disposition);
                }
                if let Some((start, end)) = range {
                    headers.insert("content-length".into(), (end - start + 1).to_string());
                    headers.insert(
                        "content-range".into(),
                        format!("bytes {start}-{end}/{}", object.bytes.len()),
                    );
                    Ok((206, headers, object.bytes[start..=end].to_vec()))
                } else {
                    Ok((
                        200,
                        headers,
                        if operation == "HeadObject" {
                            Vec::new()
                        } else {
                            object.bytes.to_vec()
                        },
                    ))
                }
            }
            "DeleteObject" => {
                let mut store = self.store.write().await;
                let objects = store.get_mut(&bucket).ok_or_else(no_bucket)?;
                self.fault(operation, &bucket, key.as_deref())?;
                objects.remove(key.as_ref().unwrap());
                drop(store);
                Ok((204, Fields::new(), Vec::new()))
            }
            "ListObjectsV2" => self.list(&bucket, &query).await,
            _ => unreachable!(),
        }
    }
    fn fault(&self, operation: &str, bucket: &str, key: Option<&str>) -> Result<()> {
        if let Some(code) = self.faults.s3_effect(operation, bucket, key) {
            Err(Error::new(code, "Injected service failure"))
        } else {
            Ok(())
        }
    }

    async fn list(&self, bucket: &str, query: &Fields) -> Result<(u16, Fields, Vec<u8>)> {
        let max = query
            .get("max-keys")
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|_| Error::invalid("Invalid max-keys"))
            })
            .transpose()?
            .unwrap_or(1000)
            .min(1000);
        let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
        let encoded = match query.get("encoding-type").map(String::as_str) {
            None => false,
            Some("url") => true,
            _ => return Err(Error::invalid("Unsupported encoding type")),
        };
        let after = query
            .get("continuation-token")
            .map(|s| cursor(s, bucket, prefix))
            .transpose()?;
        let store = self.store.read().await;
        let objects = store.get(bucket).ok_or_else(no_bucket)?;
        let mut matches = objects.iter().filter(|(key, _)| {
            key.starts_with(prefix) && after.as_ref().is_none_or(|after| *key > after)
        });
        let page: Vec<_> = matches
            .by_ref()
            .take(max)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let truncated = max != 0 && matches.next().is_some();
        self.fault("ListObjectsV2", bucket, None)?;
        drop(store);
        let text = |s: &str| {
            if encoded {
                percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            } else {
                protocol::xml(s)
            }
        };
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max}</MaxKeys><KeyCount>{}</KeyCount><IsTruncated>{truncated}</IsTruncated>",
            protocol::xml(bucket),
            text(prefix),
            page.len()
        );
        if encoded {
            xml.push_str("<EncodingType>url</EncodingType>");
        }
        if let Some(token) = query.get("continuation-token") {
            xml.push_str(&format!(
                "<ContinuationToken>{}</ContinuationToken>",
                protocol::xml(token)
            ));
        }
        if truncated {
            let token = URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&(1, bucket, prefix, &page.last().unwrap().0)).unwrap());
            xml.push_str(&format!(
                "<NextContinuationToken>{token}</NextContinuationToken>"
            ));
        }
        for (key, object) in page {
            xml.push_str(&format!("<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>", text(&key), protocol::timestamp(&object), protocol::xml(&object.etag), object.bytes.len()));
        }
        xml.push_str("</ListBucketResult>");
        Ok((200, xml_headers(), xml.into_bytes()))
    }
}

pub(crate) fn routing_error(request: &Request) -> Response {
    protocol::error(
        Error::new("InvalidRequest", "Invalid routing parameters"),
        request.uri().path(),
        request.method() == Method::HEAD,
    )
}

fn no_bucket() -> Error {
    Error::new("NoSuchBucket", "The specified bucket does not exist")
}
fn no_key() -> Error {
    Error::new("NoSuchKey", "The specified key does not exist")
}
fn xml_headers() -> Fields {
    Fields::from([("content-type".into(), "application/xml".into())])
}
fn directive<'a>(fields: &'a Fields, name: &str) -> Result<&'a str> {
    match fields.get(name).map(String::as_str).unwrap_or("COPY") {
        value @ ("COPY" | "REPLACE") => Ok(value),
        _ => Err(Error::invalid("Invalid copy directive")),
    }
}

fn object(bytes: Vec<u8>, fields: &Fields, now: OffsetDateTime) -> Result<Object> {
    let checksum = checksums(&bytes, fields)?;
    let tags = fields
        .get("x-amz-tagging")
        .map(|s| http::tags(s))
        .transpose()?
        .unwrap_or_default();
    let kms = match fields
        .get("x-amz-server-side-encryption")
        .map(String::as_str)
    {
        Some("aws:kms") => Some(
            fields
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .cloned(),
        ),
        Some(_) => return Err(Error::unsupported("Unsupported encryption mode")),
        None if fields.contains_key("x-amz-server-side-encryption-aws-kms-key-id") => {
            return Err(Error::invalid("KMS key requires aws:kms mode"));
        }
        None => None,
    };
    let etag = format!("\"{:x}\"", Md5::digest(&bytes));
    Ok(Object {
        bytes: bytes.into(),
        content_type: fields
            .get("content-type")
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".into()),
        encoding: fields.get("content-encoding").cloned(),
        metadata: fields
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("x-amz-meta-")
                    .map(|key| (key.into(), v.clone()))
            })
            .collect(),
        tags,
        checksum,
        etag,
        modified: now,
        kms,
    })
}

fn algorithm(value: &str) -> Result<&'static str> {
    match value {
        "SHA256" => Ok("SHA256"),
        "CRC32" => Ok("CRC32"),
        _ => Err(Error::unsupported("Unsupported checksum algorithm")),
    }
}
fn digest(algorithm: &str, bytes: &[u8]) -> String {
    match algorithm {
        "SHA256" => STANDARD.encode(Sha256::digest(bytes)),
        "CRC32" => STANDARD.encode(crc32fast::hash(bytes).to_be_bytes()),
        _ => unreachable!(),
    }
}
fn checksums(bytes: &[u8], fields: &Fields) -> Result<Option<(String, String)>> {
    if let Some(value) = fields.get("content-md5") {
        validate_digest(value, &Md5::digest(bytes))?;
    }
    let mut checksum = None;
    for (algorithm, name) in [
        ("SHA256", "x-amz-checksum-sha256"),
        ("CRC32", "x-amz-checksum-crc32"),
    ] {
        if let Some(value) = fields.get(name) {
            let expected = STANDARD.decode(digest(algorithm, bytes)).unwrap();
            validate_digest(value, &expected)?;
            if checksum.is_some() {
                return Err(Error::new("InvalidRequest", "Multiple checksum algorithms"));
            }
            checksum = Some((algorithm.to_owned(), value.clone()));
        }
    }
    if let Some(selected) = fields.get("x-amz-sdk-checksum-algorithm") {
        let selected = algorithm(selected)?;
        if checksum.as_ref().is_none_or(|(a, _)| a != selected) {
            return Err(Error::new(
                "InvalidRequest",
                "Checksum algorithm requires matching digest",
            ));
        }
    }
    Ok(checksum)
}
fn validate_digest(value: &str, expected: &[u8]) -> Result<()> {
    let value = STANDARD
        .decode(value)
        .map_err(|_| Error::new("InvalidDigest", "Invalid base64 digest"))?;
    if value.len() != expected.len() {
        return Err(Error::new("InvalidDigest", "Invalid digest length"));
    }
    if value != expected {
        return Err(Error::new("BadDigest", "Checksum does not match payload"));
    }
    Ok(())
}

fn cursor(token: &str, bucket: &str, prefix: &str) -> Result<String> {
    let invalid = || Error::invalid("Invalid continuation token");
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
    let (version, stored_bucket, stored_prefix, key): (u8, String, String, String) =
        serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if version != 1
        || stored_bucket != bucket
        || stored_prefix != prefix
        || !key.starts_with(prefix)
    {
        return Err(invalid());
    }
    Ok(key)
}

fn byte_range(raw: &str, total: usize) -> Result<(usize, usize)> {
    let value = raw
        .strip_prefix("bytes=")
        .ok_or_else(|| Error::invalid("Invalid byte range"))?;
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| Error::invalid("Invalid byte range"))?;
    if (start.is_empty() && end.is_empty())
        || !start.bytes().chain(end.bytes()).all(|b| b.is_ascii_digit())
    {
        return Err(Error::invalid("Invalid byte range"));
    }
    let number = |s: &str| {
        s.parse::<u64>()
            .map_err(|_| Error::invalid("Invalid byte range"))
    };
    let unsatisfied = || {
        let mut e = Error::new("InvalidRange", "The requested range is not satisfiable");
        e.range_total = Some(total);
        e
    };
    if start.is_empty() {
        let count = number(end)?;
        if count == 0 || total == 0 {
            return Err(unsatisfied());
        }
        return Ok((
            total.saturating_sub(usize::try_from(count).unwrap_or(usize::MAX)),
            total - 1,
        ));
    }
    let start = number(start)?;
    let end = if end.is_empty() {
        total.saturating_sub(1) as u64
    } else {
        number(end)?
    };
    if start > end || start >= total as u64 || total == 0 {
        return Err(unsatisfied());
    }
    Ok((start as usize, end.min(total as u64 - 1) as usize))
}

fn create_configuration(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0usize;
    let mut root = false;
    loop {
        match reader
            .read_event()
            .map_err(|_| Error::new("MalformedXML", "Invalid bucket configuration"))?
        {
            Event::Start(e) => {
                let name = e.local_name();
                if depth == 0 {
                    if root || name.as_ref() != b"CreateBucketConfiguration" {
                        return Err(Error::new("MalformedXML", "Invalid bucket configuration"));
                    }
                    root = true;
                } else if depth != 1 || name.as_ref() != b"LocationConstraint" {
                    return Err(Error::unsupported("Unsupported bucket configuration"));
                }
                depth += 1;
            }
            Event::Empty(e) => {
                if depth == 0 && e.local_name().as_ref() == b"CreateBucketConfiguration" && !root {
                    root = true;
                } else if depth != 1 || e.local_name().as_ref() != b"LocationConstraint" {
                    return Err(Error::new("MalformedXML", "Invalid bucket configuration"));
                }
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| Error::new("MalformedXML", "Invalid bucket configuration"))?;
            }
            Event::Eof => break,
            Event::DocType(_) => return Err(Error::new("MalformedXML", "DTD is unsupported")),
            Event::Text(e) => {
                let text = e
                    .xml_content()
                    .map_err(|_| Error::new("MalformedXML", "Invalid XML text"))?;
                if depth < 2 && !text.chars().all(char::is_whitespace) {
                    return Err(Error::new(
                        "MalformedXML",
                        "Unexpected bucket configuration text",
                    ));
                }
            }
            Event::CData(e) => {
                if depth != 2 {
                    return Err(Error::new("MalformedXML", "Unexpected CDATA"));
                }
                e.xml_content()
                    .map_err(|_| Error::new("MalformedXML", "Invalid XML text"))?;
            }
            Event::GeneralRef(e) => {
                if depth != 2 {
                    return Err(Error::new("MalformedXML", "Unexpected XML reference"));
                }
                let name = e
                    .decode()
                    .map_err(|_| Error::new("MalformedXML", "Invalid XML reference"))?;
                quick_xml::escape::unescape(&format!("&{name};"))
                    .map_err(|_| Error::new("MalformedXML", "Invalid XML reference"))?;
            }
            _ => {}
        }
    }
    if !root || depth != 0 {
        return Err(Error::new("MalformedXML", "Invalid bucket configuration"));
    }
    Ok(())
}

fn validate_options(fields: &Fields, query: &Fields) -> Result<()> {
    for name in query.keys() {
        if matches!(
            name.as_str(),
            "list-type"
                | "prefix"
                | "max-keys"
                | "continuation-token"
                | "encoding-type"
                | "response-content-disposition"
                | "x-id"
                | "x-amz-algorithm"
                | "x-amz-credential"
                | "x-amz-date"
                | "x-amz-expires"
                | "x-amz-signedheaders"
                | "x-amz-signature"
                | "x-amz-security-token"
        ) || name.starts_with("x-amz-meta-")
            || matches!(
                name.as_str(),
                "x-amz-checksum-sha256"
                    | "x-amz-checksum-crc32"
                    | "x-amz-sdk-checksum-algorithm"
                    | "x-amz-checksum-algorithm"
                    | "x-amz-checksum-mode"
                    | "x-amz-content-sha256"
                    | "x-amz-tagging"
                    | "x-amz-tagging-directive"
                    | "x-amz-metadata-directive"
                    | "x-amz-server-side-encryption"
                    | "x-amz-server-side-encryption-aws-kms-key-id"
                    | "x-amz-copy-source"
                    | "x-amz-decoded-content-length"
                    | "x-amz-trailer"
                    | "content-md5"
                    | "content-type"
                    | "content-length"
            )
        {
            continue;
        }
        return Err(Error::unsupported("Unsupported S3 query option"));
    }
    for name in fields.keys() {
        if name.starts_with("if-")
            || name.starts_with("x-amz-copy-source-if-")
            || name.starts_with("x-amz-server-side-encryption-customer-")
            || name.starts_with("x-amz-copy-source-server-side-encryption-customer-")
            || matches!(
                name.as_str(),
                "x-amz-acl"
                    | "x-amz-storage-class"
                    | "x-amz-website-redirect-location"
                    | "x-amz-object-lock-mode"
                    | "x-amz-object-lock-retain-until-date"
                    | "x-amz-object-lock-legal-hold"
                    | "x-amz-bypass-governance-retention"
                    | "x-amz-server-side-encryption-bucket-key-enabled"
                    | "x-amz-copy-source-range"
            )
            || name.starts_with("x-amz-grant-")
            || (name.starts_with("x-amz-checksum-")
                && !matches!(
                    name.as_str(),
                    "x-amz-checksum-sha256"
                        | "x-amz-checksum-crc32"
                        | "x-amz-checksum-mode"
                        | "x-amz-checksum-algorithm"
                ))
        {
            return Err(Error::unsupported("Unsupported S3 request option"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    #[tokio::test]
    async fn stored_tag_values_survive_copy_and_replace_independently_of_metadata() {
        let service = Service::new(vec!["synthetic-bucket".into()], Faults::default()).unwrap();
        let put = Request::builder()
            .method("PUT")
            .uri("/synthetic-bucket/source")
            .header(
                "x-amz-tagging",
                "label=hello+world&path=a%252Fb&unicode=%E2%98%83",
            )
            .header("x-amz-meta-kind", "original")
            .body(Body::from("payload"))
            .unwrap();
        assert_eq!(service.dispatch(put).await.status(), 200);
        let copy = Request::builder()
            .method("PUT")
            .uri("/synthetic-bucket/copied")
            .header("x-amz-copy-source", "synthetic-bucket/source")
            .header("x-amz-metadata-directive", "REPLACE")
            .header("x-amz-meta-kind", "new")
            .body(Body::empty())
            .unwrap();
        assert_eq!(service.dispatch(copy).await.status(), 200);
        {
            let store = service.store.read().await;
            let copied = &store["synthetic-bucket"]["copied"];
            assert_eq!(
                copied.tags.get("label").map(String::as_str),
                Some("hello world")
            );
            assert_eq!(copied.tags.get("path").map(String::as_str), Some("a%2Fb"));
            assert_eq!(copied.tags.get("unicode").map(String::as_str), Some("☃"));
            assert_eq!(copied.metadata.get("kind").map(String::as_str), Some("new"));
        }
        let replace = Request::builder()
            .method("PUT")
            .uri("/synthetic-bucket/replaced")
            .header("x-amz-copy-source", "synthetic-bucket/copied")
            .header("x-amz-tagging-directive", "REPLACE")
            .header("x-amz-tagging", "label=replaced%2Bvalue")
            .body(Body::empty())
            .unwrap();
        assert_eq!(service.dispatch(replace).await.status(), 200);
        let invalid = Request::builder()
            .method("PUT")
            .uri("/synthetic-bucket/replaced")
            .header("x-amz-tagging", "duplicate=one&duplicate=two")
            .body(Body::from("bad replacement"))
            .unwrap();
        assert_eq!(service.dispatch(invalid).await.status(), 400);
        let store = service.store.read().await;
        let replaced = &store["synthetic-bucket"]["replaced"];
        assert_eq!(replaced.tags.len(), 1);
        assert_eq!(
            replaced.tags.get("label").map(String::as_str),
            Some("replaced+value")
        );
        assert_eq!(replaced.bytes.as_ref(), b"payload");
        assert_eq!(
            store["synthetic-bucket"]["source"]
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("original")
        );
    }
}
