use reqwest::{Client, StatusCode};
use serde_json::json;
use supercloudy::server::{Config, router_with_config};
use tokio::net::TcpListener;

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
        ("POST", "/", Some("AmazonSSM.GetParameter"), None),
        ("PUT", "/synthetic-bucket/key", None, Some(auth("lambda"))),
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
