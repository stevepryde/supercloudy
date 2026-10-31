use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    presigning::{PresignedRequest, PresigningConfig},
    primitives::ByteStream,
    types::{
        ChecksumAlgorithm, ChecksumMode, MetadataDirective, ServerSideEncryption, TaggingDirective,
    },
};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use std::{
    net::Ipv4Addr,
    time::{Duration, SystemTime},
};
use tokio::{net::TcpListener, task::JoinHandle};

const BUCKET: &str = "synthetic-bucket";
// Published SHA-256("abc") and CRC32("123456789") vectors, encoded as S3 requires.
const SHA256_ABC: &str = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";
const CRC32_DIGITS: &str = "y/Q5Jg==";

// Keep test failures from printing signed HTTP request details.
#[track_caller]
fn success<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("local S3 request failed"))
}

#[derive(Clone, Debug)]
struct LocalDns;
impl ResolveDns for LocalDns {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        if name == "localhost" || name.ends_with(".localhost") {
            DnsFuture::ready(Ok(vec![Ipv4Addr::LOCALHOST.into()]))
        } else {
            DnsFuture::ready(Err(ResolveDnsError::new(std::io::Error::other(
                "non-local DNS disabled",
            ))))
        }
    }
}

fn client(endpoint: &str, path_style: bool) -> Client {
    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
        .endpoint_url(endpoint)
        .http_client(
            aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
                ))
                .build_with_resolver(LocalDns),
        );
    if path_style {
        config = config.force_path_style(true);
    }
    Client::from_conf(config.build())
}

struct Emulator {
    client: Client,
    endpoint: String,
    port: u16,
    http: reqwest::Client,
    task: JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Emulator {
    async fn new() -> Self {
        Self::with_config(supercloudy::server::Config {
            buckets: vec![BUCKET.into()],
            faults: Default::default(),
        })
        .await
    }
    async fn with_config(config: supercloudy::server::Config) -> Self {
        let router = success(supercloudy::server::router_with_config(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint = format!("http://127.0.0.1:{port}");
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            client: client(&endpoint, true),
            endpoint,
            port,
            http: reqwest::Client::builder().no_proxy().build().unwrap(),
            task,
        }
    }
    fn url(&self, key: &str) -> String {
        format!("{}/{BUCKET}/{key}", self.endpoint)
    }
    async fn put(&self, key: &str, bytes: &'static [u8]) {
        success(
            self.client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(ByteStream::from_static(bytes))
                .send()
                .await,
        );
    }
    async fn bytes(&self, key: &str) -> Vec<u8> {
        let response = success(
            self.client
                .get_object()
                .bucket(BUCKET)
                .key(key)
                .send()
                .await,
        );
        success(response.body.collect().await).into_bytes().to_vec()
    }
    async fn send_presigned(
        &self,
        request: PresignedRequest,
        body: Option<&'static [u8]>,
    ) -> reqwest::Response {
        let mut send = self
            .http
            .request(success(request.method().parse()), request.uri());
        for (name, value) in request.headers() {
            send = send.header(name, value);
        }
        if let Some(bytes) = body {
            send = send.body(bytes);
        }
        success(send.send().await)
    }
}

#[tokio::test]
async fn s3_size_policy_preserves_binary_sdk_uploads_and_rejects_oversized_overwrites() {
    let e = Emulator::new().await;
    // Above DynamoDB's JSON body cap, with invalid UTF-8 and zero bytes.
    let mut binary = vec![0xff; 33 * 1024 * 1024];
    binary[..4].copy_from_slice(&[0, 0x80, 0xfe, 0xff]);
    success(
        e.client
            .put_object()
            .bucket(BUCKET)
            .key("large")
            .metadata("fixture", "binary")
            .body(ByteStream::from(binary.clone()))
            .send()
            .await,
    );
    let get = success(
        e.client
            .get_object()
            .bucket(BUCKET)
            .key("large")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert!(get.checksum_crc32().is_some());
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        binary
    );
    drop(binary);

    // The documented object boundary is independent of the wire framing budget.
    let maximum = vec![0x80; 64 * 1024 * 1024];
    let mut framed = b"4000000\r\n".to_vec();
    framed.extend_from_slice(&maximum);
    framed.extend_from_slice(b"\r\n0\r\n\r\n");
    let response = success(
        e.http
            .put(e.url("boundary"))
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "67108864")
            .body(framed)
            .send()
            .await,
    );
    assert_eq!(response.status(), 200);
    assert_eq!(e.bytes("boundary").await, maximum);
    drop(maximum);
    for excessive_framing in [false, true] {
        let mut request = e.http.put(e.url("large"));
        let payload = if excessive_framing {
            // A tiny decoded object with wire framing over budget isolates the wire cap.
            let mut wire = b"1;padding=".to_vec();
            wire.resize(65 * 1024 * 1024, b'a');
            wire.extend_from_slice(b"\r\nx\r\n0\r\n\r\n");
            request = request
                .header("content-encoding", "aws-chunked")
                .header("x-amz-decoded-content-length", "1");
            wire
        } else {
            vec![0; 64 * 1024 * 1024 + 1]
        };
        let response = success(request.body(payload).send().await);
        assert_eq!(response.status(), 400);
        assert!(success(response.text().await).contains("<Code>EntityTooLarge</Code>"));
        let head = success(
            e.client
                .head_object()
                .bucket(BUCKET)
                .key("large")
                .send()
                .await,
        );
        assert_eq!(head.content_length(), Some(33 * 1024 * 1024));
        assert_eq!(
            head.metadata()
                .and_then(|m| m.get("fixture"))
                .map(String::as_str),
            Some("binary")
        );
        let stored = e.bytes("large").await;
        assert_eq!(&stored[..4], &[0, 0x80, 0xfe, 0xff]);
        assert!(stored[4..].iter().all(|byte| *byte == 0xff));
    }
}

#[tokio::test]
async fn sdk_object_lifecycle_default_crc32_and_modeled_errors() {
    let e = Emulator::new().await;
    let c = &e.client;
    let created = success(c.create_bucket().bucket("synthetic-created").send().await);
    assert!(created.location().is_some());
    let put = success(
        c.put_object()
            .bucket(BUCKET)
            .key("digits")
            .body(ByteStream::from_static(b"123456789"))
            .send()
            .await,
    );
    assert_eq!(put.checksum_crc32(), Some(CRC32_DIGITS));
    assert_eq!(put.e_tag(), Some("\"25f9e794323b453885f5181f1b624d0b\""));
    let get = success(
        c.get_object()
            .bucket(BUCKET)
            .key("digits")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(get.checksum_crc32(), Some(CRC32_DIGITS));
    assert_eq!(get.content_type(), Some("application/octet-stream"));
    assert_eq!(get.content_length(), Some(9));
    assert!(get.last_modified().is_some());
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        b"123456789"
    );
    success(c.create_bucket().bucket(BUCKET).send().await);
    assert_eq!(e.bytes("digits").await, b"123456789");
    e.put("digits", b"replacement").await;
    assert_eq!(e.bytes("digits").await, b"replacement");
    success(c.delete_object().bucket(BUCKET).key("digits").send().await);
    success(c.delete_object().bucket(BUCKET).key("digits").send().await);
    let missing = c
        .get_object()
        .bucket(BUCKET)
        .key("digits")
        .send()
        .await
        .err()
        .unwrap();
    assert!(missing.as_service_error().unwrap().is_no_such_key());
    let bucket = c
        .get_object()
        .bucket("synthetic-absent")
        .key("key")
        .send()
        .await
        .err()
        .unwrap();
    assert_eq!(
        bucket.as_service_error().unwrap().code(),
        Some("NoSuchBucket")
    );
    let head = c
        .head_object()
        .bucket(BUCKET)
        .key("missing")
        .send()
        .await
        .err()
        .unwrap();
    assert_eq!(head.raw_response().unwrap().status().as_u16(), 404);
    let other = Emulator::new().await;
    assert!(
        other
            .client
            .get_object()
            .bucket(BUCKET)
            .key("digits")
            .send()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sdk_sha256_metadata_tags_sse_copy_and_replacement() {
    let e = Emulator::new().await;
    let c = &e.client;
    let put = success(
        c.put_object()
            .bucket(BUCKET)
            .key("source")
            .body(ByteStream::from_static(b"abc"))
            .content_type("text/plain")
            .metadata("fixture", "synthetic")
            .tagging("category=synthetic+fixture&encoded=a%2Bb")
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_ABC)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id("test")
            .send()
            .await,
    );
    assert_eq!(put.checksum_sha256(), Some(SHA256_ABC));
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(put.ssekms_key_id(), Some("test"));
    let head = success(
        c.head_object()
            .bucket(BUCKET)
            .key("source")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(head.checksum_sha256(), Some(SHA256_ABC));
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(
        head.metadata().unwrap().get("fixture").map(String::as_str),
        Some("synthetic")
    );
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(head.ssekms_key_id(), Some("test"));
    let raw = success(e.http.head(e.url("source")).send().await);
    assert_eq!(raw.headers()["x-amz-tagging-count"], "2");
    assert_eq!(raw.headers()["content-length"], "3");
    assert!(success(raw.bytes().await).is_empty());
    let copy = success(
        c.copy_object()
            .bucket(BUCKET)
            .key("copy")
            .copy_source(format!("{BUCKET}/source"))
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id("test")
            .send()
            .await,
    );
    let result = copy.copy_object_result().unwrap();
    assert_eq!(result.checksum_sha256(), Some(SHA256_ABC));
    assert!(result.last_modified().is_some());
    assert_eq!(copy.ssekms_key_id(), Some("test"));
    let copied = success(
        c.get_object()
            .bucket(BUCKET)
            .key("copy")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(copied.content_type(), Some("text/plain"));
    assert_eq!(copied.tag_count(), Some(2));
    assert_eq!(
        copied
            .metadata()
            .unwrap()
            .get("fixture")
            .map(String::as_str),
        Some("synthetic")
    );
    assert_eq!(
        success(copied.body.collect().await).into_bytes().as_ref(),
        b"abc"
    );
    success(
        c.copy_object()
            .bucket(BUCKET)
            .key("copy")
            .copy_source(format!("/{BUCKET}/source"))
            .metadata_directive(MetadataDirective::Replace)
            .metadata("new", "yes")
            .tagging_directive(TaggingDirective::Replace)
            .tagging("single=tag")
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await,
    );
    let changed = success(
        c.get_object()
            .bucket(BUCKET)
            .key("copy")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(changed.content_type(), Some("application/octet-stream"));
    assert_eq!(changed.tag_count(), Some(1));
    assert_eq!(changed.metadata().unwrap().len(), 1);
    assert_eq!(
        changed.metadata().unwrap().get("new").map(String::as_str),
        Some("yes")
    );
    assert_eq!(changed.checksum_crc32(), Some("NSRBwg=="));
    assert!(changed.server_side_encryption().is_none());
    assert_eq!(
        success(changed.body.collect().await).into_bytes().as_ref(),
        b"abc"
    );
    success(
        c.copy_object()
            .bucket(BUCKET)
            .key("copy")
            .copy_source(format!("{BUCKET}/source"))
            .tagging_directive(TaggingDirective::Replace)
            .send()
            .await,
    );
    let cleared = success(c.get_object().bucket(BUCKET).key("copy").send().await);
    assert!(cleared.tag_count().is_none_or(|count| count == 0));
    assert_eq!(
        success(cleared.body.collect().await).into_bytes().as_ref(),
        b"abc"
    );
    assert_eq!(e.bytes("source").await, b"abc");
    e.put("source", b"fresh").await;
    let replaced = success(c.head_object().bucket(BUCKET).key("source").send().await);
    assert!(
        replaced
            .metadata()
            .is_none_or(|metadata| metadata.is_empty())
    );
    assert!(replaced.server_side_encryption().is_none());
}

#[tokio::test]
async fn sdk_ranges_and_empty_objects() {
    let e = Emulator::new().await;
    e.put("range", b"0123456789abcdef").await;
    for (range, expected, content_range) in [
        ("bytes=0-4", &b"01234"[..], "bytes 0-4/16"),
        ("bytes=0-11", &b"0123456789ab"[..], "bytes 0-11/16"),
        ("bytes=12-", &b"cdef"[..], "bytes 12-15/16"),
        ("bytes=-3", &b"def"[..], "bytes 13-15/16"),
        ("bytes=14-100", &b"ef"[..], "bytes 14-15/16"),
    ] {
        let get = success(
            e.client
                .get_object()
                .bucket(BUCKET)
                .key("range")
                .range(range)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await,
        );
        assert_eq!(get.content_range(), Some(content_range));
        assert_eq!(get.accept_ranges(), Some("bytes"));
        assert_eq!(get.content_length(), Some(expected.len() as i64));
        assert!(get.checksum_crc32().is_none());
        assert_eq!(
            success(get.body.collect().await).into_bytes().as_ref(),
            expected
        );
    }
    e.put("empty", b"").await;
    for key in ["range", "empty"] {
        let error = e
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .range("bytes=99-")
            .send()
            .await
            .err()
            .unwrap();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 416);
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("InvalidRange")
        );
    }
    for range in ["bytes=1-2,4-5", "bytes=wrong"] {
        let response = success(
            e.http
                .get(e.url("range"))
                .header("range", range)
                .send()
                .await,
        );
        assert_eq!(response.status(), 400);
    }
    let response = success(
        e.http
            .get(e.url("empty"))
            .header("range", "bytes=0-0")
            .send()
            .await,
    );
    assert_eq!(response.status(), 416);
    assert_eq!(response.headers()["content-range"], "bytes */0");
    let response = success(
        e.http
            .head(e.url("range"))
            .header("range", "bytes=0-1")
            .send()
            .await,
    );
    assert_eq!(response.status(), 501);
    assert!(success(response.bytes().await).is_empty());
}

#[tokio::test]
async fn sdk_copy_source_decodes_once_and_preserves_arbitrary_key_text() {
    let e = Emulator::new().await;
    let c = &e.client;
    let key = "dir/space +雪/%2F?value";
    success(
        c.put_object()
            .bucket(BUCKET)
            .key(key)
            .content_type("text/plain")
            .metadata("fixture", "copy")
            .body(ByteStream::from_static(b"copy fixture"))
            .send()
            .await,
    );
    for (index, source) in [
        "synthetic-bucket/dir/space%20%2B%E9%9B%AA/%252F%3Fvalue",
        "/synthetic-bucket/dir%2Fspace%20%2B%E9%9B%AA%2F%252F%3Fvalue",
        "synthetic-bucket%2Fdir%2Fspace%20%2B%E9%9B%AA%2F%252F%3Fvalue",
    ]
    .iter()
    .enumerate()
    {
        let destination = format!("copy-{index}");
        success(
            c.copy_object()
                .bucket(BUCKET)
                .key(&destination)
                .copy_source(*source)
                .send()
                .await,
        );
        let get = success(c.get_object().bucket(BUCKET).key(&destination).send().await);
        assert_eq!(get.content_type(), Some("text/plain"));
        assert_eq!(
            get.metadata().unwrap().get("fixture").map(String::as_str),
            Some("copy")
        );
        assert_eq!(
            success(get.body.collect().await).into_bytes().as_ref(),
            b"copy fixture"
        );
    }
    assert_eq!(e.bytes(key).await, b"copy fixture");
    success(
        c.create_bucket()
            .bucket("synthetic-destination")
            .send()
            .await,
    );
    success(
        c.copy_object()
            .bucket("synthetic-destination")
            .key("cross-bucket")
            .copy_source("synthetic-bucket/dir/space%20%2B%E9%9B%AA/%252F%3Fvalue")
            .send()
            .await,
    );
    let get = success(
        c.get_object()
            .bucket("synthetic-destination")
            .key("cross-bucket")
            .send()
            .await,
    );
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        b"copy fixture"
    );
}

#[tokio::test]
async fn sdk_list_paginates_more_than_a_thousand_and_binds_cursors() {
    let e = Emulator::new().await;
    for index in 0..1005 {
        success(
            e.client
                .put_object()
                .bucket(BUCKET)
                .key(format!("matching/{index:04}"))
                .body(ByteStream::from_static(b"x"))
                .send()
                .await,
        );
    }
    e.put("other/0000", b"other").await;
    let mut token = None;
    let mut keys = Vec::new();
    loop {
        let page = success(
            e.client
                .list_objects_v2()
                .bucket(BUCKET)
                .prefix("matching/")
                .set_continuation_token(token)
                .send()
                .await,
        );
        assert!(page.contents().len() <= 1000);
        assert_eq!(page.key_count(), Some(page.contents().len() as i32));
        for object in page.contents() {
            assert_eq!(object.size(), Some(1));
            assert!(object.e_tag().is_some());
            assert!(object.last_modified().is_some());
            assert_eq!(object.storage_class().unwrap().as_str(), "STANDARD");
            keys.push(object.key().unwrap().to_owned());
        }
        token = page.next_continuation_token().map(str::to_owned);
        if !page.is_truncated().unwrap_or(false) {
            break;
        }
        assert!(token.is_some());
    }
    assert_eq!(
        keys,
        (0..1005)
            .map(|i| format!("matching/{i:04}"))
            .collect::<Vec<_>>()
    );
    let first = success(
        e.client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix("matching/")
            .max_keys(2)
            .send()
            .await,
    );
    assert_eq!(first.contents().len(), 2);
    let token = first.next_continuation_token().unwrap();
    success(
        e.client
            .delete_object()
            .bucket(BUCKET)
            .key("matching/0001")
            .send()
            .await,
    );
    let next = success(
        e.client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix("matching/")
            .max_keys(2)
            .continuation_token(token)
            .send()
            .await,
    );
    assert_eq!(next.continuation_token(), Some(token));
    assert_eq!(next.contents()[0].key(), Some("matching/0002"));
    assert_eq!(next.contents()[1].key(), Some("matching/0003"));
    for (bucket, prefix, cursor) in [(BUCKET, "other/", token), (BUCKET, "matching/", "garbage")] {
        let error = e
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .continuation_token(cursor)
            .send()
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("InvalidArgument")
        );
    }
    success(
        e.client
            .create_bucket()
            .bucket("synthetic-other")
            .send()
            .await,
    );
    assert!(
        e.client
            .list_objects_v2()
            .bucket("synthetic-other")
            .prefix("matching/")
            .continuation_token(token)
            .send()
            .await
            .is_err()
    );
    for (prefix, max_keys) in [("matching/", 0), ("absent/", 1000)] {
        let page = success(
            e.client
                .list_objects_v2()
                .bucket(BUCKET)
                .prefix(prefix)
                .max_keys(max_keys)
                .send()
                .await,
        );
        assert!(page.contents().is_empty());
        assert_eq!(page.is_truncated(), Some(false));
    }
    let capped = success(
        e.client
            .list_objects_v2()
            .bucket(BUCKET)
            .max_keys(5000)
            .send()
            .await,
    );
    assert_eq!(capped.max_keys(), Some(1000));
    assert_eq!(capped.contents().len(), 1000);
    e.put("url/space +雪", b"x").await;
    let encoded = success(
        e.client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix("url/")
            .encoding_type(aws_sdk_s3::types::EncodingType::Url)
            .send()
            .await,
    );
    assert_eq!(encoded.encoding_type().unwrap().as_str(), "url");
    assert_eq!(
        encoded.contents()[0].key(),
        Some("url%2Fspace%20%2B%E9%9B%AA")
    );
}

#[tokio::test]
async fn sdk_presigned_put_get_attributes_disposition_and_expiry() {
    let e = Emulator::new().await;
    let config = || PresigningConfig::expires_in(Duration::from_secs(900)).unwrap();
    let request = success(
        e.client
            .put_object()
            .bucket(BUCKET)
            .key("presigned")
            .content_type("text/plain")
            .metadata("fixture", "presigned")
            .tagging("type=synthetic&purpose=test")
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_sha256(SHA256_ABC)
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id("test")
            .presigned(config())
            .await,
    );
    assert_eq!(e.send_presigned(request, Some(b"abc")).await.status(), 200);
    let head = success(
        e.client
            .head_object()
            .bucket(BUCKET)
            .key("presigned")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(head.checksum_sha256(), Some(SHA256_ABC));
    assert_eq!(head.ssekms_key_id(), Some("test"));
    assert_eq!(
        head.metadata().unwrap().get("fixture").map(String::as_str),
        Some("presigned")
    );
    let tag_head = success(e.http.head(e.url("presigned")).send().await);
    assert_eq!(tag_head.headers()["x-amz-tagging-count"], "2");
    let disposition = "attachment; filename=\"synthetic report + 1.txt\"";
    let request = success(
        e.client
            .get_object()
            .bucket(BUCKET)
            .key("presigned")
            .response_content_disposition(disposition)
            .presigned(config())
            .await,
    );
    let response = e.send_presigned(request, None).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-disposition"], disposition);
    assert_eq!(success(response.bytes().await).as_ref(), b"abc");
    let unchanged = success(e.http.get(e.url("presigned")).send().await);
    assert!(!unchanged.headers().contains_key("content-disposition"));
    let expired = PresigningConfig::builder()
        .start_time(SystemTime::now() - Duration::from_secs(1800))
        .expires_in(Duration::from_secs(900))
        .build()
        .unwrap();
    let request = success(
        e.client
            .put_object()
            .bucket(BUCKET)
            .key("presigned")
            .presigned(expired)
            .await,
    );
    assert_eq!(
        e.send_presigned(request, Some(b"overwrite")).await.status(),
        403
    );
    assert_eq!(e.bytes("presigned").await, b"abc");
}

#[tokio::test]
async fn sdk_default_endpoint_resolution_and_virtual_host_addressing() {
    let e = Emulator::new().await;
    let virtual_client = client(&format!("http://localhost:{}", e.port), false);
    let request = success(
        virtual_client
            .get_object()
            .bucket(BUCKET)
            .key("nested/key")
            .presigned(PresigningConfig::expires_in(Duration::from_secs(900)).unwrap())
            .await,
    );
    assert!(
        request
            .uri()
            .starts_with(&format!("http://{BUCKET}.localhost:{}/nested/key?", e.port))
    );
    success(
        virtual_client
            .put_object()
            .bucket(BUCKET)
            .key("nested/key")
            .body(ByteStream::from_static(b"virtual"))
            .send()
            .await,
    );
    assert_eq!(e.bytes("nested/key").await, b"virtual");
    let get = success(
        virtual_client
            .get_object()
            .bucket(BUCKET)
            .key("nested/key")
            .send()
            .await,
    );
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        b"virtual"
    );
    let ip_default = client(&e.endpoint, false);
    success(
        ip_default
            .put_object()
            .bucket(BUCKET)
            .key("ip-default")
            .body(ByteStream::from_static(b"ip"))
            .send()
            .await,
    );
    assert_eq!(e.bytes("ip-default").await, b"ip");
    // Dotted names and host ownership are tested directly, because SDK TLS rules may use path style.
    success(
        e.client
            .create_bucket()
            .bucket("synthetic.dotted")
            .send()
            .await,
    );
    let response = success(
        e.http
            .put(format!("{}/another/key", e.endpoint))
            .header("host", format!("SYNTHETIC.DOTTED.localhost:{}", e.port))
            .body("host-owned")
            .send()
            .await,
    );
    assert_eq!(response.status(), 200);
    let get = success(
        e.client
            .get_object()
            .bucket("synthetic.dotted")
            .key("another/key")
            .send()
            .await,
    );
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        b"host-owned"
    );
}

#[tokio::test]
async fn malformed_requests_fail_before_overwriting_object_and_attributes() {
    let e = Emulator::new().await;
    success(
        e.client
            .put_object()
            .bucket(BUCKET)
            .key("protected")
            .content_type("text/plain")
            .metadata("fixture", "original")
            .body(ByteStream::from_static(b"original"))
            .send()
            .await,
    );
    for (headers, expected) in [
        (vec![("x-amz-checksum-sha256", SHA256_ABC)], 400),
        (vec![("x-amz-checksum-crc32", "not-base64")], 400),
        (vec![("content-md5", "kAFQmDzST7DWlj99KOF/cg==")], 400),
        (vec![("x-amz-tagging", "duplicate=one&duplicate=two")], 400),
        (vec![("x-amz-tagging", "=empty")], 400),
        (vec![("x-amz-server-side-encryption", "AES256")], 501),
        (
            vec![
                ("x-amz-copy-source", "synthetic-bucket/protected"),
                ("x-amz-metadata-directive", "WRONG"),
            ],
            400,
        ),
        (
            vec![
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "10"),
            ],
            400,
        ),
    ] {
        // Valid framing isolates decoded-length mismatch from parser failures.
        let body = if headers
            .iter()
            .any(|(name, _)| *name == "x-amz-decoded-content-length")
        {
            "3\r\nabc\r\n0\r\n\r\n"
        } else {
            "overwrite"
        };
        let mut request = e.http.put(e.url("protected")).body(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        assert_eq!(success(request.send().await).status().as_u16(), expected);
        assert_eq!(e.bytes("protected").await, b"original");
    }
    for query in ["uploads", "uploadId=test", "tagging", "versionId=test"] {
        let response = success(
            e.http
                .put(format!("{}?{query}", e.url("protected")))
                .body("overwrite")
                .send()
                .await,
        );
        assert_eq!(response.status(), 501);
        assert_eq!(e.bytes("protected").await, b"original");
    }
    let head = success(
        e.client
            .head_object()
            .bucket(BUCKET)
            .key("protected")
            .send()
            .await,
    );
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(
        head.metadata().unwrap().get("fixture").map(String::as_str),
        Some("original")
    );
    let missing = success(e.http.head(e.url("missing")).send().await);
    assert_eq!(missing.status(), 404);
    assert!(success(missing.bytes().await).is_empty());
    let escaped = success(
        e.http
            .get(format!("{}/{BUCKET}/missing%3C&key", e.endpoint))
            .send()
            .await,
    );
    assert_eq!(escaped.status(), 404);
    assert!(escaped.headers().contains_key("x-amz-request-id"));
    assert!(escaped.headers().contains_key("x-amz-id-2"));
    let xml = success(escaped.text().await);
    assert!(xml.contains("&amp;key"));
    e.put("xml<&key", b"xml fixture").await;
    let listed = success(
        e.client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix("xml")
            .send()
            .await,
    );
    assert_eq!(listed.contents()[0].key(), Some("xml<&key"));
    for query in [
        "max-keys=-1",
        "max-keys=bad",
        "delimiter=%2F",
        "start-after=key",
    ] {
        let response = success(
            e.http
                .get(format!("{}/{BUCKET}?list-type=2&{query}", e.endpoint))
                .send()
                .await,
        );
        assert_eq!(
            response.status(),
            if query.starts_with("max-keys") {
                400
            } else {
                501
            }
        );
    }
}

#[tokio::test]
async fn aws_chunked_decoding_checksum_trailers_and_rejected_framing() {
    let e = Emulator::new().await;
    let response = success(e.http.put(e.url("chunked"))
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        .header("x-amz-decoded-content-length", "9")
        .header("x-amz-sdk-checksum-algorithm", "CRC32")
        .header("x-amz-trailer", "x-amz-checksum-crc32")
        .body("4;chunk-signature=test\r\n1234\r\n5\r\n56789\r\n0\r\nx-amz-checksum-crc32:y/Q5Jg==\r\n\r\n")
        .send().await);
    assert_eq!(response.status(), 200);
    assert_eq!(e.bytes("chunked").await, b"123456789");
    for (body, decoded_length, trailer) in [
        ("4\r\nabc", "3", true),
        // No announced trailer: only the decoded length is wrong.
        ("3\r\nabc\r\n0\r\n\r\n", "4", false),
        ("3\r\nabc\r\n0\r\n\r\n", "3", true),
        ("g\r\nabc\r\n0\r\n\r\n", "3", true),
    ] {
        let mut request = e
            .http
            .put(e.url("chunked"))
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", decoded_length);
        if trailer {
            request = request.header("x-amz-trailer", "x-amz-checksum-crc32");
        }
        let response = success(request.body(body).send().await);
        assert_eq!(response.status(), 400);
        assert_eq!(e.bytes("chunked").await, b"123456789");
    }
}

#[tokio::test]
async fn sdk_file_stream_upload_uses_normal_checksum_transport() {
    let e = Emulator::new().await;
    let path = std::env::temp_dir().join(format!(
        "supercloudy-s3-synthetic-{}-{}",
        std::process::id(),
        e.port
    ));
    std::fs::write(&path, b"123456789").unwrap();
    let body = success(ByteStream::from_path(&path).await);
    let uploaded = e
        .client
        .put_object()
        .bucket(BUCKET)
        .key("sdk-stream")
        .body(body)
        .send()
        .await;
    std::fs::remove_file(path).unwrap();
    assert_eq!(success(uploaded).checksum_crc32(), Some(CRC32_DIGITS));
    let get = success(
        e.client
            .get_object()
            .bucket(BUCKET)
            .key("sdk-stream")
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await,
    );
    assert_eq!(get.checksum_crc32(), Some(CRC32_DIGITS));
    assert_eq!(
        success(get.body.collect().await).into_bytes().as_ref(),
        b"123456789"
    );
}

#[tokio::test]
async fn shared_faults_validate_first_preserve_mutations_and_allow_sdk_retry() {
    let faults = serde_json::from_value(serde_json::json!({"rules": [
        {"service":"s3","operation":"PutObject","bucket":BUCKET,"key":"protected",
         "first_request":2,"repeat":1,"effect":{"type":"throttle"}},
        {"service":"s3","operation":"CopyObject","bucket":BUCKET,"key":"destination",
         "first_request":2,"repeat":1,"effect":{"type":"internal_server_error"}},
        {"service":"s3","operation":"GetObject","bucket":BUCKET,"key":"retry",
         "first_request":1,"repeat":1,"effect":{"type":"throttle"}},
        {"operation":"CreateTable","table":"synthetic-items","first_request":1,"repeat":1,
         "effect":{"type":"throttle"}}
    ]}))
    .unwrap();
    let e = Emulator::with_config(supercloudy::server::Config {
        buckets: vec![BUCKET.into()],
        faults,
    })
    .await;
    e.put("protected", b"original").await;
    let invalid = success(
        e.http
            .put(e.url("protected"))
            .header("x-amz-checksum-sha256", SHA256_ABC)
            .body("overwrite")
            .send()
            .await,
    );
    assert_eq!(invalid.status(), 400);
    let throttled = success(
        e.http
            .put(e.url("protected"))
            .body("overwrite")
            .send()
            .await,
    );
    assert_eq!(throttled.status(), 503);
    assert_eq!(e.bytes("protected").await, b"original");
    let copied = success(
        e.http
            .put(e.url("destination"))
            .header("x-amz-copy-source", "synthetic-bucket/protected")
            .send()
            .await,
    );
    assert_eq!(copied.status(), 200);
    e.put("protected", b"new source").await;
    let failed = success(
        e.http
            .put(e.url("destination"))
            .header("x-amz-copy-source", "synthetic-bucket/protected")
            .send()
            .await,
    );
    assert_eq!(failed.status(), 500);
    assert_eq!(e.bytes("destination").await, b"original");
    e.put("retry", b"sdk retries").await;
    assert_eq!(e.bytes("retry").await, b"sdk retries");
    // S3 calls cannot consume the legacy DynamoDB rule on the shared scheduler.
    let create = serde_json::json!({"TableName":"synthetic-items", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]});
    let response = success(
        e.http
            .post(&e.endpoint)
            .header("x-amz-target", "DynamoDB_20120810.CreateTable")
            .header("content-type", "application/x-amz-json-1.0")
            .body(create.to_string())
            .send()
            .await,
    );
    assert_eq!(response.status(), 400);
    assert!(success(response.text().await).contains("ProvisionedThroughputExceededException"));
    let response = success(
        e.http
            .post(&e.endpoint)
            .header("x-amz-target", "DynamoDB_20120810.CreateTable")
            .header("content-type", "application/x-amz-json-1.0")
            .body(create.to_string())
            .send()
            .await,
    );
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn routing_precedence_conflicts_and_partial_presigns_do_not_execute() {
    let e = Emulator::new().await;
    e.put("protected", b"original").await;
    let target = success(e.http.put(e.url("protected"))
        .header("x-amz-target", "Unknown.Target")
        .header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=test")
        .body("overwrite").send().await);
    assert_eq!(target.status(), 400);
    assert!(success(target.text().await).contains("UnknownOperationException"));
    for query in [
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "X-Amz-Credential=test%2F20261031%2Fus-east-1%2Fs3%2Faws4_request",
        "X-Amz-Date=20261031T000000Z&X-Amz-Expires=900",
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Algorithm=AWS4-HMAC-SHA256",
    ] {
        let response = success(
            e.http
                .put(format!("{}?{query}", e.url("protected")))
                .body("overwrite")
                .send()
                .await,
        );
        assert_eq!(response.status(), 400);
    }
    let conflict = success(e.http.put(format!("{}?X-Amz-Credential=test%2F20261031%2Fus-east-1%2Fdynamodb%2Faws4_request", e.url("protected")))
        .header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=test")
        .body("overwrite").send().await);
    assert_eq!(conflict.status(), 400);
    let unsupported = success(e.http.put(e.url("protected"))
        .header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=test")
        .body("overwrite").send().await);
    assert_eq!(unsupported.status(), 400);
    assert!(success(unsupported.text().await).contains("UnknownOperationException"));
    assert_eq!(e.bytes("protected").await, b"original");
    let root = success(e.http.post(&e.endpoint).body("not json").send().await);
    assert_eq!(root.status(), 400);
    assert!(success(root.text().await).contains("UnknownOperationException"));
}

#[tokio::test]
async fn unsupported_copy_encryption_and_malformed_bucket_xml_preserve_state() {
    let e = Emulator::new().await;
    e.put("source", b"source bytes").await;
    e.put("destination", b"original destination").await;
    for header in [
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
    ] {
        let response = success(
            e.http
                .put(e.url("destination"))
                .header("x-amz-copy-source", "synthetic-bucket/source")
                .header(header, "test")
                .send()
                .await,
        );
        assert_eq!(response.status(), 501);
        assert!(success(response.text().await).contains("<Code>NotImplemented</Code>"));
        assert_eq!(e.bytes("destination").await, b"original destination");
    }
    for (index, body) in [
        "<CreateBucketConfiguration><LocationConstraint>&undeclared;</LocationConstraint></CreateBucketConfiguration>",
        "<CreateBucketConfiguration>unexpected text</CreateBucketConfiguration>",
        "<![CDATA[unexpected]]><CreateBucketConfiguration/>",
    ].iter().enumerate() {
        let bucket_url = format!("{}/synthetic-invalid-{index}", e.endpoint);
        let response = success(e.http.put(&bucket_url).body(*body).send().await);
        assert_eq!(response.status(), 400);
        assert!(success(response.text().await).contains("<Code>MalformedXML</Code>"));
        let absent = success(e.http.get(format!("{bucket_url}?list-type=2")).send().await);
        assert_eq!(absent.status(), 404);
        assert!(success(absent.text().await).contains("<Code>NoSuchBucket</Code>"));
    }
    for (index, location) in ["us-east-1", "<![CDATA[us-east-1]]>", "us-east-&#49;"]
        .iter()
        .enumerate()
    {
        let bucket_url = format!("{}/synthetic-valid-{index}", e.endpoint);
        let body = format!(
            "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{location}</LocationConstraint></CreateBucketConfiguration>"
        );
        assert_eq!(
            success(e.http.put(&bucket_url).body(body).send().await).status(),
            200
        );
        assert_eq!(
            success(e.http.get(format!("{bucket_url}?list-type=2")).send().await).status(),
            200
        );
    }
}

#[tokio::test]
async fn size_rejection_preserves_the_next_s3_fault() {
    let faults = serde_json::from_value(serde_json::json!({"rules":[{
        "service":"s3","operation":"PutObject","bucket":BUCKET,"key":"protected",
        "first_request":2,"repeat":1,"effect":{"type":"throttle"}
    }]}))
    .unwrap();
    let e = Emulator::with_config(supercloudy::server::Config {
        buckets: vec![BUCKET.into()],
        faults,
    })
    .await;
    success(
        e.http
            .put(e.url("protected"))
            .header("content-type", "text/plain")
            .header("x-amz-meta-fixture", "original")
            .body("original")
            .send()
            .await,
    );
    let response = success(
        e.http
            .put(e.url("protected"))
            .body(vec![0; 64 * 1024 * 1024 + 1])
            .send()
            .await,
    );
    assert_eq!(response.status(), 400);
    assert!(success(response.text().await).contains("<Code>EntityTooLarge</Code>"));
    let response = success(e.http.put(e.url("protected")).body("rejected").send().await);
    assert_eq!(response.status(), 503);
    assert!(success(response.text().await).contains("<Code>SlowDown</Code>"));
    assert_eq!(e.bytes("protected").await, b"original");
    let head = success(
        e.client
            .head_object()
            .bucket(BUCKET)
            .key("protected")
            .send()
            .await,
    );
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(
        head.metadata().unwrap().get("fixture").map(String::as_str),
        Some("original")
    );
    assert_eq!(
        success(e.http.put(e.url("protected")).body("accepted").send().await).status(),
        200
    );
    assert_eq!(e.bytes("protected").await, b"accepted");
}
