use super::*;
use axum::body::Body;
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed},
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

proptest! {
    #![proptest_config(config())]

    #[test]
    fn arbitrary_bucket_xml_bytes_do_not_panic(bytes in prop::collection::vec(any::<u8>(), 0..8192), text in ".{0,4096}") {
        let _ = create_configuration(&bytes);
        let _ = create_configuration(text.as_bytes());
    }

    #[test]
    fn generated_bucket_xml_creates_the_requested_bucket(
        location in "[a-zA-Z0-9 &<>]{0,80}",
        suffix in "[a-z0-9]{1,20}",
        namespaced in any::<bool>(),
        cdata in any::<bool>(),
        empty_location in any::<bool>(),
    ) {
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
        })?;
    }

    #[test]
    fn malformed_bucket_xml_does_not_create_a_bucket(
        unsupported in "[A-Za-z]{1,24}",
        depth in 2usize..160,
        variant in 0u8..4,
    ) {
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
        })?;
    }
}
