use super::{Error, Result};
use axum::http::{HeaderMap, Uri, uri::Authority};
use std::collections::BTreeMap;
use time::{OffsetDateTime, PrimitiveDateTime, macros::format_description};

pub(super) type Fields = BTreeMap<String, String>;

pub(crate) fn decode(raw: &str) -> Result<String> {
    let bytes = raw.as_bytes();
    for (i, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (i + 2 >= bytes.len()
                || !bytes[i + 1].is_ascii_hexdigit()
                || !bytes[i + 2].is_ascii_hexdigit())
        {
            return Err(Error::invalid("Invalid percent encoding"));
        }
    }
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|_| Error::invalid("Invalid UTF-8"))
}

pub(super) fn query(uri: &Uri) -> Result<Fields> {
    let mut fields = Fields::new();
    for pair in uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|s| !s.is_empty())
    {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode(key)?.to_ascii_lowercase();
        let value = decode(value)?;
        if fields.insert(key, value).is_some() {
            return Err(Error::new("InvalidRequest", "Duplicate query parameter"));
        }
    }
    Ok(fields)
}

pub(super) fn headers(headers: &HeaderMap, query: &Fields) -> Result<Fields> {
    let mut fields = Fields::new();
    for (name, value) in headers {
        let name = name.as_str().to_owned();
        let value = value
            .to_str()
            .map_err(|_| Error::invalid("Invalid header"))?
            .to_owned();
        if fields.insert(name, value).is_some() {
            return Err(Error::new("InvalidRequest", "Duplicate header"));
        }
    }
    for (name, value) in query {
        if hoisted(name) {
            if fields.get(name).is_some_and(|v| v != value) {
                return Err(Error::new(
                    "InvalidRequest",
                    "Conflicting header and query parameter",
                ));
            }
            axum::http::HeaderValue::from_str(value)
                .map_err(|_| Error::invalid("Invalid header value"))?;
            fields.insert(name.clone(), value.clone());
        }
    }
    Ok(fields)
}

fn hoisted(name: &str) -> bool {
    name.starts_with("x-amz-meta-")
        || matches!(
            name,
            "x-amz-sdk-checksum-algorithm"
                | "x-amz-checksum-algorithm"
                | "x-amz-checksum-sha256"
                | "x-amz-checksum-crc32"
                | "x-amz-checksum-mode"
                | "x-amz-tagging"
                | "x-amz-tagging-directive"
                | "x-amz-metadata-directive"
                | "x-amz-server-side-encryption"
                | "x-amz-server-side-encryption-aws-kms-key-id"
                | "x-amz-copy-source"
                | "x-amz-content-sha256"
                | "x-amz-decoded-content-length"
                | "x-amz-trailer"
                | "content-md5"
                | "content-type"
                | "content-length"
        )
}

pub(super) fn address(uri: &Uri, headers: &HeaderMap) -> Result<(String, Option<String>)> {
    let host = headers
        .get("host")
        .map(|v| v.to_str().map_err(|_| Error::invalid("Invalid Host")))
        .transpose()?;
    let host = host
        .map(|s| {
            s.parse::<Authority>()
                .map_err(|_| Error::invalid("Invalid Host"))
        })
        .transpose()?;
    if let (Some(a), Some(b)) = (uri.authority(), host.as_ref())
        && !a.as_str().eq_ignore_ascii_case(b.as_str())
    {
        return Err(Error::new("InvalidRequest", "Conflicting authorities"));
    }
    let authority = uri.authority().or(host.as_ref());
    let hostname = authority
        .map(|a| a.host().to_ascii_lowercase())
        .unwrap_or_default();
    let path = uri.path().strip_prefix('/').unwrap_or(uri.path());
    if let Some(bucket) = hostname.strip_suffix(".localhost") {
        return Ok((
            bucket.to_owned(),
            if path.is_empty() {
                None
            } else {
                Some(decode(path)?)
            },
        ));
    }
    let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
    Ok((
        decode(bucket)?,
        if key.is_empty() {
            None
        } else {
            Some(decode(key)?)
        },
    ))
}

pub(super) fn presign(query: &Fields, now: OffsetDateTime) -> Result<()> {
    const NAMES: [&str; 7] = [
        "x-amz-algorithm",
        "x-amz-credential",
        "x-amz-date",
        "x-amz-expires",
        "x-amz-signedheaders",
        "x-amz-signature",
        "x-amz-security-token",
    ];
    if !NAMES.iter().any(|name| query.contains_key(*name)) {
        return Ok(());
    }
    if NAMES[..6]
        .iter()
        .any(|name| query.get(*name).is_none_or(String::is_empty))
    {
        return Err(Error::invalid("Incomplete presigned request"));
    }
    if query["x-amz-algorithm"] != "AWS4-HMAC-SHA256" {
        return Err(Error::invalid("Invalid signing algorithm"));
    }
    let date = PrimitiveDateTime::parse(
        &query["x-amz-date"],
        format_description!("[year][month][day]T[hour][minute][second]Z"),
    )
    .map_err(|_| Error::invalid("Invalid signing date"))?
    .assume_utc();
    let expiry = query["x-amz-expires"]
        .parse::<i64>()
        .ok()
        .filter(|v| (1..=604800).contains(v))
        .ok_or_else(|| Error::invalid("Invalid expiry"))?;
    if now.unix_timestamp() >= date.unix_timestamp() + expiry {
        return Err(Error::new("AccessDenied", "Request has expired"));
    }
    Ok(())
}

pub(super) fn tags(raw: &str) -> Result<Fields> {
    let mut tags = Fields::new();
    if raw.is_empty() {
        return Ok(tags);
    }
    for pair in raw.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode(&key.replace('+', " "))?;
        let value = decode(&value.replace('+', " "))?;
        if key.is_empty() || tags.insert(key, value).is_some() || tags.len() > 10 {
            return Err(Error::invalid("Invalid tags"));
        }
    }
    Ok(tags)
}

pub(super) fn copy_source(raw: &str) -> Result<(String, String)> {
    if raw.contains('?') {
        return Err(Error::unsupported(
            "Copy source version selectors are unsupported",
        ));
    }
    let decoded = decode(raw)?;
    let (bucket, key) = decoded
        .strip_prefix('/')
        .unwrap_or(&decoded)
        .split_once('/')
        .ok_or_else(|| Error::invalid("Invalid copy source"))?;
    super::validate_bucket(bucket).map_err(|_| Error::invalid("Invalid copy source bucket"))?;
    if key.is_empty() {
        return Err(Error::invalid("Empty copy source key"));
    }
    Ok((bucket.to_owned(), key.to_owned()))
}

pub(super) fn body(wire: &[u8], fields: &mut Fields) -> Result<Vec<u8>> {
    if fields
        .get("content-length")
        .is_some_and(|length| length.parse::<usize>().ok() != Some(wire.len()))
    {
        return Err(Error::new("InvalidRequest", "Content length mismatch"));
    }
    let chunked = fields.get("content-encoding").is_some_and(|s| {
        s.split(',')
            .any(|v| v.trim().eq_ignore_ascii_case("aws-chunked"))
    });
    let decoded = if chunked {
        let (decoded, trailers) = chunks(wire)?;
        let announced: Vec<_> = fields
            .get("x-amz-trailer")
            .map(|s| {
                s.split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();
        for name in &announced {
            if !matches!(
                name.as_str(),
                "x-amz-checksum-sha256" | "x-amz-checksum-crc32"
            ) {
                return Err(Error::unsupported("Unsupported trailer"));
            }
            let value = trailers
                .get(name)
                .ok_or_else(|| Error::new("InvalidRequest", "Missing announced trailer"))?;
            if fields.get(name).is_some_and(|old| old != value) {
                return Err(Error::new("InvalidRequest", "Conflicting checksum trailer"));
            }
            fields.insert(name.clone(), value.clone());
        }
        if trailers
            .keys()
            .any(|k| k != "x-amz-trailer-signature" && !announced.contains(k))
        {
            return Err(Error::new("InvalidRequest", "Unannounced trailer"));
        }
        let encoding = fields
            .get("content-encoding")
            .unwrap()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
            .collect::<Vec<_>>()
            .join(", ");
        if encoding.is_empty() {
            fields.remove("content-encoding");
        } else {
            fields.insert("content-encoding".into(), encoding);
        }
        decoded
    } else {
        if fields.contains_key("x-amz-trailer") {
            return Err(Error::new(
                "InvalidRequest",
                "Trailers require aws-chunked encoding",
            ));
        }
        wire.to_vec()
    };
    if let Some(length) = fields.get("x-amz-decoded-content-length")
        && length.parse::<usize>().ok() != Some(decoded.len())
    {
        return Err(Error::new(
            "InvalidRequest",
            "Decoded content length mismatch",
        ));
    }
    Ok(decoded)
}

fn line<'a>(wire: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
    let end = wire
        .get(*at..)
        .and_then(|s| s.windows(2).position(|w| w == b"\r\n"))
        .ok_or_else(|| Error::invalid("Truncated chunk framing"))?
        + *at;
    let value = &wire[*at..end];
    *at = end + 2;
    Ok(value)
}

fn chunks(wire: &[u8]) -> Result<(Vec<u8>, Fields)> {
    let mut at = 0;
    let mut decoded = Vec::new();
    let mut trailers = Fields::new();
    loop {
        let size = std::str::from_utf8(line(wire, &mut at)?)
            .map_err(|_| Error::invalid("Invalid chunk size"))?;
        let size = size.split(';').next().unwrap_or_default();
        if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::invalid("Invalid chunk size"));
        }
        let size =
            usize::from_str_radix(size, 16).map_err(|_| Error::invalid("Invalid chunk size"))?;
        if size == 0 {
            break;
        }
        let end = at
            .checked_add(size)
            .filter(|e| *e <= wire.len())
            .ok_or_else(|| Error::invalid("Truncated chunk"))?;
        decoded.extend_from_slice(&wire[at..end]);
        at = end;
        if wire.get(at..at + 2) != Some(b"\r\n") {
            return Err(Error::invalid("Missing chunk terminator"));
        }
        at += 2;
    }
    // AWS streaming payloads place trailers directly after the terminal size line.
    loop {
        let value = line(wire, &mut at)?;
        if value.is_empty() {
            break;
        }
        let value = std::str::from_utf8(value).map_err(|_| Error::invalid("Invalid trailer"))?;
        let (name, value) = value
            .split_once(':')
            .ok_or_else(|| Error::invalid("Invalid trailer"))?;
        if trailers
            .insert(name.to_ascii_lowercase(), value.trim().to_owned())
            .is_some()
        {
            return Err(Error::invalid("Duplicate trailer"));
        }
    }
    if at != wire.len() {
        return Err(Error::invalid("Unexpected bytes after terminal chunk"));
    }
    Ok((decoded, trailers))
}
