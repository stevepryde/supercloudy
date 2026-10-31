use reqwest::{Client, StatusCode};
use serde_json::json;
use supercloudy::server::{Config, router_with_config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Emulator {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn emulator(faults: serde_json::Value) -> Emulator {
    let router = router_with_config(Config {
        buckets: vec!["synthetic-bucket".into(), "dotted.bucket".into()],
        faults: serde_json::from_value(faults).unwrap(),
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Emulator { endpoint, task }
}
fn auth(service: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test"
    )
}

#[tokio::test]
async fn unknown_targets_respond_before_body_arrives_despite_s3_hints() {
    let e = emulator(json!({"rules":[]})).await;
    for target in [
        "DynamoDB_20120810.NotAnOperation",
        "UnimplementedService.GetParameter",
    ] {
        let mut stream = TcpStream::connect(e.endpoint.strip_prefix("http://").unwrap())
            .await
            .unwrap();
        // Keep the connection open and deliberately withhold every body byte.
        let headers = format!(
            "POST / HTTP/1.1\r\nHost: synthetic-bucket.localhost\r\nX-Amz-Target: {target}\r\nAuthorization: {}\r\nContent-Length: 100\r\n\r\n",
            auth("s3")
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !response
                .windows(b"com.amazon.coral.service#UnknownOperationException".len())
                .any(|w| w == b"com.amazon.coral.service#UnknownOperationException")
            {
                let mut buffer = [0; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0, "connection closed without protocol error");
                response.extend_from_slice(&buffer[..count]);
            }
        })
        .await
        .expect("routing waited for an unread request body");
        assert!(response.starts_with(b"HTTP/1.1 400 "));
    }
}

#[tokio::test]
async fn target_and_signing_scope_ownership_precede_rest_paths() {
    let e = emulator(json!({"rules":[]})).await;
    let c = Client::new();
    for (method, path, target, authorization) in [
        (
            "PUT",
            "/synthetic-bucket/key",
            Some("DynamoDB_20120810.PutItem"),
            Some(auth("s3")),
        ),
        ("POST", "/", Some("UnimplementedService.GetParameter"), None),
        ("PUT", "/synthetic-bucket/key", None, Some(auth("dynamodb"))),
        ("POST", "/", None, None),
    ] {
        let mut request = c.request(method.parse().unwrap(), format!("{}{path}", e.endpoint));
        if let Some(target) = target {
            request = request.header("x-amz-target", target);
        }
        if let Some(auth) = authorization {
            request = request.header("authorization", auth);
        }
        let response = request.body("{").send().await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.text().await.unwrap();
        assert!(body.contains("com.amazon.coral.service#UnknownOperationException"));
    }
    let lambda = c
        .put(format!("{}/synthetic-bucket/key", e.endpoint))
        .header("authorization", auth("lambda"))
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(lambda.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        lambda.headers()["x-amzn-errortype"],
        "ResourceNotFoundException"
    );
    let response = c
        .get(format!("{}/synthetic-bucket/key", e.endpoint))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<Code>NoSuchKey</Code>")
    );
    let response = c
        .post(format!("{}/", e.endpoint))
        .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
        .header("host", "dotted.bucket.localhost")
        .header("authorization", auth("s3"))
        .body(r#"{"TableName":"synthetic-table"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("ResourceNotFoundException")
    );
    for credential in [
        "test%2F20261031%2Fus-east-1%2Flambda%2Faws4_request",
        "test%2F20261031%2Fus-west-2%2Fs3%2Faws4_request",
    ] {
        let response = c
            .put(format!(
                "{}/synthetic-bucket/key?X-Amz-Credential={credential}",
                e.endpoint
            ))
            .header("authorization", auth("s3"))
            .body("invalid")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains("<Code>InvalidRequest</Code>")
        );
    }
    let response = c
        .get(format!("{}/key", e.endpoint))
        .header("host", "DOTTED.BUCKET.LOCALHOST:8000")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<Code>NoSuchKey</Code>")
    );
}

#[tokio::test]
async fn s3_faults_count_only_valid_requests_and_preserve_state_and_instance_isolation() {
    let e = emulator(json!({"rules":[
        {"service":"s3","operation":"PutObject","bucket":"synthetic-bucket","key":"key","first_request":1,"repeat":1,"effect":{"type":"throttle"}},
        {"service":"s3","operation":"PutObject","bucket":"synthetic-bucket","key":"key","first_request":2,"repeat":1,"effect":{"type":"internal_server_error"}},
        {"operation":"DescribeTable","first_request":1,"repeat":1,"effect":{"type":"throttle"}}
    ]})).await;
    let c = Client::new();
    let url = format!("{}/synthetic-bucket/key", e.endpoint);
    let invalid = c
        .put(&url)
        .header("x-amz-checksum-sha256", "malformed")
        .body("bad")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    for (status, code) in [
        (StatusCode::SERVICE_UNAVAILABLE, "SlowDown"),
        (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
    ] {
        let response = c.put(&url).body("rejected").send().await.unwrap();
        assert_eq!(response.status(), status);
        assert!(
            response
                .text()
                .await
                .unwrap()
                .contains(&format!("<Code>{code}</Code>"))
        );
        assert_eq!(
            c.get(&url).send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        c.put(&url).body("accepted").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        c.get(&url).send().await.unwrap().text().await.unwrap(),
        "accepted"
    );
    let response = c
        .post(format!("{}/", e.endpoint))
        .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
        .body(r#"{"TableName":"synthetic-table"}"#)
        .send()
        .await
        .unwrap();
    // A missing table is invalid before fault scheduling, preserving existing DynamoDB semantics.
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("ResourceNotFoundException")
    );
    let isolated = emulator(json!({"rules":[]})).await;
    assert_eq!(
        c.get(format!("{}/synthetic-bucket/key", isolated.endpoint))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn service_specific_fault_selectors_are_rejected_at_construction() {
    for rule in [
        json!({"service":"s3","operation":"PutObject","table":"table","effect":{"type":"throttle"}}),
        json!({"operation":"PutItem","bucket":"synthetic-bucket","effect":{"type":"throttle"}}),
        json!({"service":"s3","operation":"PutObject","effect":{"type":"batch_deferral","positions":{"table":[0]}}}),
        json!({"service":"s3","operation":"PutObject","effect":{"type":"transaction_canceled","reasons":["TransactionConflict"]}}),
        json!({"service":"s3","operation":"CreateTable","effect":{"type":"throttle"}}),
        json!({"service":"s3","operation":"PutObject","bucket":"","effect":{"type":"throttle"}}),
        json!({"service":"s3","operation":"PutObject","key":"","effect":{"type":"throttle"}}),
        json!({"operation":"GetObject","effect":{"type":"throttle"}}),
        json!({"operation":"GetItem","table":"","effect":{"type":"throttle"}}),
        json!({"service":"lambda","operation":"PutObject","effect":{"type":"throttle"}}),
    ] {
        let mut rule = rule;
        rule["first_request"] = json!(1);
        rule["repeat"] = json!(1);
        let faults = serde_json::from_value(json!({"rules":[rule]})).unwrap();
        assert!(supercloudy::server::router_with_faults(faults).is_err());
    }
    assert!(
        router_with_config(Config {
            buckets: vec!["Bad_Bucket".into()],
            faults: Default::default()
        })
        .is_err()
    );
}

#[tokio::test]
async fn service_fault_adapters_preserve_selectors_commits_and_identical_instance_counters() {
    let config = json!({"rules":[
        {"service":"s3","operation":"CreateBucket","bucket":"synthetic-new","first_request":1,"repeat":1,"effect":{"type":"internal_server_error"}},
        {"service":"s3","operation":"DeleteObject","bucket":"synthetic-bucket","key":"selected","first_request":1,"repeat":1,"effect":{"type":"throttle"}},
        {"service":"s3","operation":"PutObject","bucket":"synthetic-bucket","key":"selected","first_request":2,"repeat":1,"effect":{"type":"internal_server_error"}}
    ]});
    let first = emulator(config.clone()).await;
    let second = emulator(config).await;
    let c = Client::new();
    // Exhaust the first instance's rules before repeating the same scenario on the second.
    for e in [&first, &second] {
        let bucket = format!("{}/synthetic-new", e.endpoint);
        assert_eq!(c.put(&bucket).send().await.unwrap().status(), 500);
        assert_eq!(
            c.get(format!("{bucket}?list-type=2"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        assert_eq!(c.put(&bucket).send().await.unwrap().status(), 200);
        for path in ["synthetic-bucket/other-key", "dotted.bucket/selected"] {
            assert_eq!(
                c.put(format!("{}/{path}", e.endpoint))
                    .body("unmatched")
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }
        let object = format!("{}/synthetic-bucket/selected", e.endpoint);
        assert_eq!(
            c.put(&object)
                .body("original")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            c.put(&object)
                .body("rejected")
                .send()
                .await
                .unwrap()
                .status(),
            500
        );
        assert_eq!(
            c.get(&object).send().await.unwrap().text().await.unwrap(),
            "original"
        );
        assert_eq!(c.delete(&object).send().await.unwrap().status(), 503);
        assert_eq!(
            c.get(&object).send().await.unwrap().text().await.unwrap(),
            "original"
        );
        assert_eq!(c.delete(&object).send().await.unwrap().status(), 204);
        assert_eq!(c.get(&object).send().await.unwrap().status(), 404);
        assert_eq!(
            c.put(&object)
                .body("accepted")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
}
