use super::*;
use axum::body::Body;
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed, TestRunner},
};
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn config() -> Config {
    Config {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(96),
        rng_seed: std::env::var("PROPTEST_RNG_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(RngSeed::Fixed(0x2085_3301)),
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/s3-xml.txt",
        ))),
        ..Config::default()
    }
}

fn escaped_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// XML parsing is synchronous: a missing EOF transition can spin without ever
// polling a Tokio timer. Bound the whole property in a process we can reap.
fn with_deadline(name: &str, run: impl FnOnce()) {
    let test = format!("s3::properties::{name}");
    if std::env::var("SUPERCLOUDY_XML_PROPERTY_WORKER").as_deref() == Ok(test.as_str()) {
        run();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture"])
        .env("SUPERCLOUDY_XML_PROPERTY_WORKER", &test)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    // Allow scheduling delays under parallel mutation builds; the previous
    // ten-second budget falsely killed unchanged XML workers under that load.
    // Still bound synchronous parser hangs and scale deeper PROPTEST_CASES runs.
    let deadline = Duration::from_secs(60) * config().cases.max(96).div_ceil(96);
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "XML property worker failed: {test}");
            return;
        }
        if started.elapsed() >= deadline {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("XML property worker timed out: {test}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn chunked_upload_stores_only_the_decoded_payload() {
    let service = Service::new(vec!["synthetic-bucket".into()], Faults::default()).unwrap();
    let upload = Request::builder()
        .method("PUT")
        .uri("/synthetic-bucket/chunked")
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "5")
        .body(Body::from("2\r\nhe\r\n3\r\nllo\r\n0\r\n\r\n"))
        .unwrap();
    assert_eq!(service.dispatch(upload).await.status(), 200);
    let download = Request::builder()
        .uri("/synthetic-bucket/chunked")
        .body(Body::empty())
        .unwrap();
    let response = service.dispatch(download).await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .as_ref(),
        b"hello"
    );
}

#[test]
fn arbitrary_bucket_xml_bytes_do_not_panic() {
    with_deadline("arbitrary_bucket_xml_bytes_do_not_panic", || {
        TestRunner::new(config())
            .run(
                &(prop::collection::vec(any::<u8>(), 0..8192), ".{0,4096}"),
                |(bytes, text)| {
                    let _ = create_configuration(&bytes);
                    let _ = create_configuration(text.as_bytes());
                    Ok(())
                },
            )
            .unwrap();
    });
}

#[test]
fn generated_bucket_xml_creates_the_requested_bucket() {
    with_deadline("generated_bucket_xml_creates_the_requested_bucket", || {
        TestRunner::new(config())
            .run(
                &("[a-zA-Z0-9 &<>]{0,80}", "[a-z0-9]{1,20}", any::<bool>(), any::<bool>(), any::<bool>()),
                |(location, suffix, namespaced, cdata, empty_location)| {
                    let (root, child, namespace) = if namespaced {
                        ("s3:CreateBucketConfiguration", "s3:LocationConstraint", " xmlns:s3=\"http://s3.amazonaws.com/doc/2006-03-01/\"")
                    } else {
                        ("CreateBucketConfiguration", "LocationConstraint", "")
                    };
                    let content = if cdata { format!("<![CDATA[{location}]]>") } else { escaped_text(&location) };
                    let child_xml = if empty_location { format!("<{child}/>") } else { format!("<{child}>{content}</{child}>") };
                    let body = format!("<?xml version=\"1.0\"?>\n<{root}{namespace}><!--synthetic-->{child_xml}</{root}>");
                    let bucket = format!("synthetic-{suffix}");
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    runtime.block_on(async {
                        let service = Service::new(vec![], Faults::default()).unwrap();
                        let request = Request::builder().method("PUT").uri(format!("/{bucket}")).body(Body::from(body)).unwrap();
                        prop_assert_eq!(service.dispatch(request).await.status(), 200);
                        prop_assert!(service.store.read().await.contains_key(&bucket));
                        Ok(())
                    })
                },
            )
            .unwrap();
    });
}

#[test]
fn malformed_bucket_xml_does_not_create_a_bucket() {
    with_deadline("malformed_bucket_xml_does_not_create_a_bucket", || {
        TestRunner::new(config())
            .run(
                &("[A-Za-z]{1,24}", 2usize..160, 0u8..4),
                |(unsupported, depth, variant)| {
                    let body = match variant {
                        0 => format!("<CreateBucketConfiguration><Unsupported{unsupported}/></CreateBucketConfiguration>"),
                        1 => format!("<CreateBucketConfiguration><LocationConstraint>{}&unknown;</LocationConstraint></CreateBucketConfiguration>", " ".repeat(depth)),
                        2 => format!("<CreateBucketConfiguration>{}{}</CreateBucketConfiguration>", "<LocationConstraint>".repeat(depth), "</LocationConstraint>".repeat(depth)),
                        _ => format!("<CreateBucketConfiguration><LocationConstraint>{}</WrongEnd></CreateBucketConfiguration>", "x".repeat(depth)),
                    };
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    runtime.block_on(async {
                        let service = Service::new(vec![], Faults::default()).unwrap();
                        let request = Request::builder().method("PUT").uri("/synthetic-invalid").body(Body::from(body)).unwrap();
                        let response = service.dispatch(request).await;
                        prop_assert!(!response.status().is_success());
                        prop_assert!(!service.store.read().await.contains_key("synthetic-invalid"));
                        Ok(())
                    })
                },
            )
            .unwrap();
    });
}
