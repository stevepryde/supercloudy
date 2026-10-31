use aws_sdk_lambda::{
    Client,
    config::{Credentials, Region, retry::RetryConfig},
    error::ProvideErrorMetadata,
    primitives::Blob,
    types::InvocationType,
};
use axum::{Router, body::Bytes, routing::post};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use supercloudy::server::{Config, Runtime, ServiceConfig, runtime_with_services};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};

struct Listener {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Listener {
    async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, task }
    }
}
struct Emulator {
    client: Client,
    runtime: Arc<Runtime>,
    listener: Listener,
}
impl Emulator {
    async fn new(config: Value) -> Self {
        let runtime = Arc::new(
            runtime_with_services(
                Config::default(),
                ServiceConfig {
                    lambda: Some(serde_json::from_value(config).unwrap()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let listener = Listener::start(runtime.router()).await;
        let client = Client::from_conf(
            aws_sdk_lambda::Config::builder()
                .behavior_version_latest()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
                .endpoint_url(&listener.url)
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .build(),
        );
        Self {
            client,
            runtime,
            listener,
        }
    }
}

#[tokio::test]
async fn synchronous_raw_bytes_canned_handler_errors_and_dry_run() {
    let (seen, mut received) = mpsc::channel(4);
    let target = Listener::start(
        Router::new()
            .route(
                "/echo",
                post(move |body: Bytes| {
                    let seen = seen.clone();
                    async move {
                        seen.send(body.clone()).await.unwrap();
                        body
                    }
                }),
            )
            .route(
                "/failure",
                post(|| async {
                    (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        "handler failed",
                    )
                }),
            )
            .route(
                "/reported",
                post(|| async { ([("x-amz-function-error", "Handled")], "reported failure") }),
            ),
    )
    .await;
    let e = Emulator::new(json!({"functions": {
        "synthetic-echo": {"url":format!("{}/echo",target.url)},
        "synthetic-failure": {"url":format!("{}/failure",target.url)},
        "synthetic-reported": {"url":format!("{}/reported",target.url)},
        "synthetic-canned": {"response":"{\"value\":42}"},
        "synthetic-error": {"response":"{\"errorMessage\":\"sample\"}","function_error":"Unhandled"}
    }}))
    .await;
    let payload = b"{ \"value\": [1, 2] }\n\0\xff";
    let output = e
        .client
        .invoke()
        .function_name("synthetic-echo")
        .payload(Blob::new(payload))
        .send()
        .await
        .unwrap();
    assert_eq!(output.status_code(), 200);
    assert_eq!(output.payload.unwrap().as_ref(), payload);
    assert_eq!(received.recv().await.unwrap(), &payload[..]);
    let dry = e
        .client
        .invoke()
        .function_name("synthetic-echo")
        .invocation_type(InvocationType::DryRun)
        .send()
        .await
        .unwrap();
    assert_eq!(dry.status_code(), 204);
    assert!(received.try_recv().is_err());
    for (name, header, body) in [
        ("synthetic-failure", "Unhandled", "handler failed"),
        ("synthetic-reported", "Handled", "reported failure"),
        (
            "synthetic-error",
            "Unhandled",
            "{\"errorMessage\":\"sample\"}",
        ),
    ] {
        let output = e.client.invoke().function_name(name).send().await.unwrap();
        assert_eq!(output.status_code(), 200);
        assert_eq!(output.function_error(), Some(header));
        assert_eq!(output.payload.unwrap().as_ref(), body.as_bytes());
    }
    let canned = e
        .client
        .invoke()
        .function_name("synthetic-canned")
        .send()
        .await
        .unwrap();
    assert_eq!(canned.payload.unwrap().as_ref(), br#"{"value":42}"#);
    e.runtime.shutdown().await;
}

#[tokio::test]
async fn unknown_function_and_transport_failure_are_invocation_errors() {
    let gone = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", gone.local_addr().unwrap());
    drop(gone);
    let e = Emulator::new(json!({"functions":{"synthetic-offline":{"url":url}}})).await;
    let unknown = e
        .client
        .invoke()
        .function_name("missing-function")
        .send()
        .await
        .unwrap_err();
    assert!(
        unknown
            .as_service_error()
            .unwrap()
            .is_resource_not_found_exception()
    );
    let offline = e
        .client
        .invoke()
        .function_name("synthetic-offline")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        offline.as_service_error().unwrap().code(),
        Some("ServiceException")
    );
    assert_eq!(offline.raw_response().unwrap().status().as_u16(), 502);
    e.runtime.shutdown().await;
}

#[tokio::test]
async fn event_accepts_before_completion_and_runtime_drains_after_request_is_gone() {
    let gate = Arc::new(Notify::new());
    let release = gate.clone();
    let (seen, mut received) = mpsc::channel(1);
    let (done, mut completed) = mpsc::channel(1);
    let target = Listener::start(Router::new().route(
        "/work",
        post(move |payload: Bytes| {
            let (seen, done, gate) = (seen.clone(), done.clone(), gate.clone());
            async move {
                seen.send(payload).await.unwrap();
                gate.notified().await;
                done.send(()).await.unwrap();
                "done"
            }
        }),
    ))
    .await;
    let e = Emulator::new(json!({"max_concurrency":1,"functions":{"synthetic-worker":{"url":format!("{}/work",target.url)}}})).await;
    let output = timeout(
        Duration::from_secs(2),
        e.client
            .invoke()
            .function_name("synthetic-worker")
            .invocation_type(InvocationType::Event)
            .payload(Blob::new("accepted payload"))
            .send(),
    )
    .await
    .expect("Event must return before the blocked handler completes")
    .unwrap();
    assert_eq!(output.status_code(), 202);
    assert!(output.function_error().is_none());
    assert!(output.payload.is_none_or(|p| p.as_ref().is_empty()));
    assert_eq!(
        timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap(),
        "accepted payload"
    );
    assert!(completed.try_recv().is_err());
    let full = e
        .client
        .invoke()
        .function_name("synthetic-worker")
        .invocation_type(InvocationType::Event)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        full.as_service_error().unwrap().code(),
        Some("TooManyRequestsException")
    );
    // Another service remains live while the target is blocked (no service lock crosses forwarding).
    let ddb = reqwest::Client::new()
        .post(&e.listener.url)
        .timeout(Duration::from_secs(2))
        .header("x-amz-target", "DynamoDB_20120810.ListTables")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(ddb.status(), 400); // Unsupported operation responds without waiting for the target.
    drop(e.client);
    let runtime = e.runtime.clone();
    let mut draining = tokio::spawn(async move {
        runtime.shutdown().await;
    });
    assert!(
        timeout(Duration::from_millis(50), &mut draining)
            .await
            .is_err(),
        "shutdown must retain accepted work"
    );
    release.notify_one();
    timeout(Duration::from_secs(2), draining)
        .await
        .unwrap()
        .unwrap();
    completed
        .recv()
        .await
        .expect("accepted handler completes during drain");
}

#[tokio::test]
async fn routing_keeps_explicit_s3_ownership_and_validates_before_dispatch() {
    let e = Emulator::new(
        json!({"functions":{"synthetic-name:alias":{"response":"qualified"}},"faults":[
            {"first_request":1,"repeat":1,"effect":"throttle"}
        ]}),
    )
    .await;
    let http = reqwest::Client::new();
    let url = format!(
        "{}/2015-03-31/functions/synthetic-name%3Aalias/invocations",
        e.listener.url
    );
    let invalid = http
        .post(&url)
        .header("x-amz-invocation-type", "Unsupported")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 400);
    let fault = e
        .client
        .invoke()
        .function_name("synthetic-name:alias")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        fault.as_service_error().unwrap().code(),
        Some("TooManyRequestsException")
    );
    let okay = e
        .client
        .invoke()
        .function_name("synthetic-name:alias")
        .send()
        .await
        .unwrap();
    assert_eq!(okay.payload.unwrap().as_ref(), b"qualified");
    let s3 = http.post(&url).header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=test").send().await.unwrap();
    assert!(
        s3.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("xml")
    );
    let bucket = http
        .post(&url)
        .header("host", "synthetic-bucket.localhost")
        .send()
        .await
        .unwrap();
    assert!(
        bucket.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("xml")
    );
    let too_big = e
        .client
        .invoke()
        .function_name("synthetic-name:alias")
        .invocation_type(InvocationType::Event)
        .payload(Blob::new(vec![b'x'; 1024 * 1024 + 1]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(too_big.raw_response().unwrap().status().as_u16(), 413);
    e.runtime.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn cli_sigterm_keeps_storage_live_while_draining_accepted_event() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let (seen, mut observed) = mpsc::channel(1);
    let (done, mut completed) = mpsc::channel(1);
    let storage_endpoint = Arc::new(std::sync::OnceLock::<String>::new());
    let worker_endpoint = storage_endpoint.clone();
    let target = Listener::start(Router::new().route(
        "/work",
        post(move || {
            let (gate, seen, done, endpoint) = (
                gate.clone(), seen.clone(), done.clone(), worker_endpoint.clone(),
            );
            async move {
                seen.send(()).await.unwrap();
                gate.notified().await;
                let client = reqwest::Client::new();
                let written = client.post(endpoint.get().unwrap())
                    .timeout(Duration::from_secs(2))
                    .header("x-amz-target", "DynamoDB_20120810.PutItem")
                    .body(json!({"TableName":"synthetic-completions","Item":{"id":{"S":"job"},"state":{"S":"done"}}}).to_string())
                    .send().await.expect("accepted worker must retain storage access during drain");
                assert_eq!(written.status(), 200);
                let stored = client.post(endpoint.get().unwrap())
                    .timeout(Duration::from_secs(2))
                    .header("x-amz-target", "DynamoDB_20120810.GetItem")
                    .body(json!({"TableName":"synthetic-completions","Key":{"id":{"S":"job"}}}).to_string())
                    .send().await.unwrap().bytes().await.unwrap();
                done.send(serde_json::from_slice::<Value>(&stored).unwrap()).await.unwrap();
                "finished"
            }
        }),
    ))
    .await;
    struct File(std::path::PathBuf);
    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = File(std::env::temp_dir().join(format!(
        "supercloudy-lambda-{}-{}.json",
        std::process::id(),
        target.url.rsplit(':').next().unwrap()
    )));
    std::fs::write(
        &path.0,
        serde_json::to_vec(
            &json!({"functions":{"synthetic-worker":{"url":format!("{}/work",target.url)}}}),
        )
        .unwrap(),
    )
    .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_supercloudy"))
        .args(["up", "--port", "0", "--lambda-functions"])
        .arg(&path.0)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let endpoint = line.strip_prefix("Listening on ").unwrap();
    storage_endpoint.set(endpoint.to_string()).unwrap();
    let table = reqwest::Client::new().post(endpoint)
        .header("x-amz-target", "DynamoDB_20120810.CreateTable")
        .body(json!({"TableName":"synthetic-completions","BillingMode":"PAY_PER_REQUEST","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}).to_string())
        .send().await.unwrap();
    assert_eq!(table.status(), 200);
    let accepted = reqwest::Client::new()
        .post(format!(
            "{endpoint}/2015-03-31/functions/synthetic-worker/invocations"
        ))
        .header("x-amz-invocation-type", "Event")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    timeout(Duration::from_secs(2), observed.recv())
        .await
        .unwrap()
        .unwrap();
    let signal = tokio::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .await
        .unwrap();
    assert!(signal.success());
    assert!(
        timeout(Duration::from_millis(100), child.wait())
            .await
            .is_err(),
        "process must retain accepted Event during shutdown"
    );
    release.notify_one();
    let stored = timeout(Duration::from_secs(2), completed.recv())
        .await
        .unwrap()
        .expect("accepted worker completes its final database write during shutdown");
    assert_eq!(stored["Item"]["state"]["S"], "done");
    assert!(
        timeout(Duration::from_secs(2), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn shutdown_timeout_cancels_owned_forwarding() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", target.local_addr().unwrap());
    let (seen, accepted) = tokio::sync::oneshot::channel();
    let target_task = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        seen.send(()).unwrap();
        // Do not return a response. Cancellation must close the client connection.
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        let _ = stream.shutdown().await;
    });
    let e = Emulator::new(
        json!({"shutdown_timeout_ms":50,"functions":{"synthetic-blocked":{"url":url}}}),
    )
    .await;
    let out = e
        .client
        .invoke()
        .function_name("synthetic-blocked")
        .invocation_type(InvocationType::Event)
        .send()
        .await
        .unwrap();
    assert_eq!(out.status_code(), 202);
    timeout(Duration::from_secs(2), accepted)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), e.runtime.shutdown())
        .await
        .expect("shutdown must be bounded");
    timeout(Duration::from_secs(2), target_task)
        .await
        .expect("cancelled forwarding must close its socket")
        .unwrap();
    let rejected = e
        .client
        .invoke()
        .function_name("synthetic-blocked")
        .invocation_type(InvocationType::Event)
        .send()
        .await
        .unwrap_err();
    assert!(rejected.as_service_error().is_some());
}

#[tokio::test]
async fn ordered_faults_precede_dispatch_and_are_instance_local() {
    let config = json!({"functions":{"synthetic-function":{"response":"success"}},"faults":[
        {"function_name":"synthetic-function","first_request":1,"repeat":1,"effect":"throttle"},
        {"function_name":"synthetic-function","first_request":2,"repeat":1,"effect":"service_error"},
        {"function_name":"synthetic-function","first_request":3,"repeat":1,"effect":"function_error"}
    ]});
    let e = Emulator::new(config.clone()).await;
    let unknown = e
        .client
        .invoke()
        .function_name("unknown")
        .send()
        .await
        .unwrap_err();
    assert!(
        unknown
            .as_service_error()
            .unwrap()
            .is_resource_not_found_exception()
    );
    for code in ["TooManyRequestsException", "ServiceException"] {
        let result = e
            .client
            .invoke()
            .function_name("synthetic-function")
            .send()
            .await
            .unwrap_err();
        assert_eq!(result.as_service_error().unwrap().code(), Some(code));
    }
    let failed = e
        .client
        .invoke()
        .function_name("synthetic-function")
        .send()
        .await
        .unwrap();
    assert_eq!(failed.function_error(), Some("Unhandled"));
    let okay = e
        .client
        .invoke()
        .function_name("synthetic-function")
        .send()
        .await
        .unwrap();
    assert_eq!(okay.payload.unwrap().as_ref(), b"success");
    let other = Emulator::new(config).await;
    let isolated = other
        .client
        .invoke()
        .function_name("synthetic-function")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        isolated.as_service_error().unwrap().code(),
        Some("TooManyRequestsException")
    );
    e.runtime.shutdown().await;
    other.runtime.shutdown().await;
}
