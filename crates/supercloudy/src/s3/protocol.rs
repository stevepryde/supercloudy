use super::{Error, Object};
use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};
use time::format_description::well_known::Rfc3339;

static REQUEST: AtomicU64 = AtomicU64::new(1);
pub(super) fn xml(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}
pub(super) fn timestamp(object: &Object) -> String {
    object.modified.format(&Rfc3339).expect("UTC timestamp")
}

pub(super) fn response(
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    head: bool,
) -> Response {
    let mut response = Response::new(if head {
        Body::empty()
    } else {
        Body::from(body)
    });
    *response.status_mut() = StatusCode::from_u16(status).expect("S3 status");
    for (name, value) in headers {
        // Request-derived header values have already passed HeaderValue validation.
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            response.headers_mut().insert(name, value);
        }
    }
    let id = REQUEST.fetch_add(1, Ordering::Relaxed).to_string();
    response
        .headers_mut()
        .insert("x-amz-request-id", HeaderValue::from_str(&id).unwrap());
    response
        .headers_mut()
        .insert("x-amz-id-2", HeaderValue::from_static("supercloudy"));
    response
}

pub(crate) fn error(error: Error, resource: &str, head: bool) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>supercloudy</RequestId></Error>",
        xml(error.code),
        xml(&error.message),
        xml(resource)
    );
    let mut headers = BTreeMap::from([("content-type".into(), "application/xml".into())]);
    if let Some(total) = error.range_total {
        headers.insert("content-range".into(), format!("bytes */{total}"));
    }
    response(error.status(), headers, body.into_bytes(), head)
}

pub(super) fn object_headers(object: &Object, checksum: bool) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::from([
        ("content-type".into(), object.content_type.clone()),
        ("content-length".into(), object.bytes.len().to_string()),
        ("etag".into(), object.etag.clone()),
        (
            "last-modified".into(),
            httpdate::fmt_http_date(object.modified.into()),
        ),
        ("accept-ranges".into(), "bytes".into()),
    ]);
    if let Some(encoding) = &object.encoding {
        headers.insert("content-encoding".into(), encoding.clone());
    }
    for (name, value) in &object.metadata {
        headers.insert(format!("x-amz-meta-{name}"), value.clone());
    }
    if !object.tags.is_empty() {
        headers.insert("x-amz-tagging-count".into(), object.tags.len().to_string());
    }
    write_headers(object, checksum, &mut headers);
    headers
}

pub(super) fn write_headers(
    object: &Object,
    checksum: bool,
    headers: &mut BTreeMap<String, String>,
) {
    headers.insert("etag".into(), object.etag.clone());
    if checksum && let Some((algorithm, value)) = &object.checksum {
        headers.insert(
            format!("x-amz-checksum-{}", algorithm.to_ascii_lowercase()),
            value.clone(),
        );
    }
    if let Some(kms) = &object.kms {
        headers.insert("x-amz-server-side-encryption".into(), "aws:kms".into());
        if let Some(key) = kms {
            headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id".into(),
                key.clone(),
            );
        }
    }
}
