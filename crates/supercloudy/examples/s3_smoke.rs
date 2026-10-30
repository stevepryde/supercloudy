//! Run against a separately started local binary:
//! cargo run --example s3_smoke -- http://127.0.0.1:4566 synthetic-images
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
    presigning::PresigningConfig,
    primitives::ByteStream,
};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use std::{net::Ipv4Addr, time::Duration};

#[track_caller]
fn success<T, E>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|_| panic!("local S3 smoke request failed"))
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

#[tokio::main]
async fn main() {
    let endpoint = std::env::args()
        .nth(1)
        .expect("supply an HTTP loopback endpoint");
    let bucket_name = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "synthetic-smoke".into());
    let mut url = success(reqwest::Url::parse(&endpoint));
    assert_eq!(url.scheme(), "http", "use HTTP loopback only");
    assert!(
        matches!(url.host_str(), Some("127.0.0.1" | "localhost")),
        "use IPv4 loopback or localhost"
    );
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    for path_style in [true, false] {
        if !path_style {
            success(url.set_host(Some("localhost")));
        }
        let mut config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
            .endpoint_url(url.as_str())
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
        let client = Client::from_conf(config.build());
        let bucket = bucket_name.as_str();
        success(client.create_bucket().bucket(bucket).send().await);
        success(
            client
                .put_object()
                .bucket(bucket)
                .key("source")
                .body(ByteStream::from_static(b"synthetic image bytes"))
                .send()
                .await,
        );
        let head = success(
            client
                .head_object()
                .bucket(bucket)
                .key("source")
                .send()
                .await,
        );
        assert_eq!(head.content_length(), Some(21));
        let range = success(
            client
                .get_object()
                .bucket(bucket)
                .key("source")
                .range("bytes=0-4")
                .send()
                .await,
        );
        assert_eq!(
            success(range.body.collect().await).into_bytes().as_ref(),
            b"synth"
        );
        success(
            client
                .copy_object()
                .bucket(bucket)
                .key("copy")
                .copy_source(format!("{bucket}/source"))
                .send()
                .await,
        );
        let get = success(client.get_object().bucket(bucket).key("copy").send().await);
        assert_eq!(
            success(get.body.collect().await).into_bytes().as_ref(),
            b"synthetic image bytes"
        );
        let listed = success(client.list_objects_v2().bucket(bucket).send().await);
        assert!(
            listed
                .contents()
                .iter()
                .any(|object| object.key() == Some("copy"))
        );
        let request = success(
            client
                .put_object()
                .bucket(bucket)
                .key("presigned")
                .presigned(PresigningConfig::expires_in(Duration::from_secs(900)).unwrap())
                .await,
        );
        // Reqwest needs the same explicit DNS mapping as the SDK for virtual-host requests.
        let port = url.port().expect("supply an explicit port");
        let presign_http = reqwest::Client::builder()
            .no_proxy()
            .resolve(
                &format!("{bucket}.localhost"),
                (Ipv4Addr::LOCALHOST, port).into(),
            )
            .build()
            .unwrap();
        let mut send = presign_http.put(request.uri()).body("presigned fixture");
        for (name, value) in request.headers() {
            send = send.header(name, value);
        }
        assert_eq!(success(send.send().await).status(), 200);
        let request = success(
            client
                .get_object()
                .bucket(bucket)
                .key("presigned")
                .presigned(PresigningConfig::expires_in(Duration::from_secs(900)).unwrap())
                .await,
        );
        let mut send = presign_http.get(request.uri());
        for (name, value) in request.headers() {
            send = send.header(name, value);
        }
        let response = success(send.send().await);
        assert_eq!(response.status(), 200);
        assert_eq!(
            success(response.bytes().await).as_ref(),
            b"presigned fixture"
        );
        for key in ["source", "copy", "presigned"] {
            success(client.delete_object().bucket(bucket).key(key).send().await);
        }
        assert!(
            success(client.list_objects_v2().bucket(bucket).send().await)
                .contents()
                .is_empty()
        );
        println!(
            "{} S3 workflow passed",
            if path_style {
                "Path-style"
            } else {
                "Virtual-host"
            }
        );
    }
    // The same listener must retain target-based DynamoDB dispatch.
    let response = success(
        http.post(&endpoint)
            .header("x-amz-target", "DynamoDB_20120810.Unknown")
            .body("synthetic")
            .send()
            .await,
    );
    assert_eq!(response.status(), 400);
    assert!(success(response.text().await).contains("UnknownOperationException"));
}
