use aws_sdk_ssm::{
    Client as SsmClient,
    config::{Credentials, Region, retry::RetryConfig},
    error::ProvideErrorMetadata,
    types::ParameterType,
};
use reqwest::{Client, Response};
use serde_json::{Value, json};
use supercloudy::server::{Config, ServiceConfig, router_with_services};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

struct Emulator {
    endpoint: String,
    http: Client,
    ssm: SsmClient,
    task: JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn parameters() -> Value {
    json!({"parameters":[
        {"name":"/example/plain","type":"String","value":"synthetic"},
        {"name":"/example/list","type":"StringList","value":"one,two"},
        {"name":"/example/secure","type":"SecureString","value":"test","encrypted_value":"synthetic-ciphertext"}
    ]})
}
fn fixtures() -> Value {
    json!({"fixtures":[
        {"format_name":"example_extraction","input_contains":"sample","output":{"items":["synthetic"]}},
        {"format_name":"example_extraction","output":{"fallback":true}},
        {"format_name":"other_extraction","output":[1,"two",null]}
    ]})
}
fn request(name: &str) -> Value {
    json!({"model":"synthetic-model","store":false,"max_output_tokens":100,"reasoning":{"effort":"low"},
        "input":[{"role":"user","content":[{"type":"input_text","text":"a sample input"}]}],
        "text":{"format":{"type":"json_schema","name":name,"strict":true,"schema":{"type":"object"}}}})
}
impl Emulator {
    async fn new(openai: Option<Value>, rules: Value) -> Self {
        let router = router_with_services(
            Config {
                buckets: vec!["synthetic-bucket".into()],
                faults: serde_json::from_value(json!({"rules":rules})).unwrap(),
            },
            ServiceConfig {
                firebase: None,
                ssm: serde_json::from_value(parameters()).unwrap(),
                openai: openai.map(|v| serde_json::from_value(v).unwrap()),
            },
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let ssm = SsmClient::from_conf(
            aws_sdk_ssm::Config::builder()
                .behavior_version_latest()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
                .endpoint_url(&endpoint)
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .build(),
        );
        Self {
            endpoint,
            http: Client::builder().no_proxy().build().unwrap(),
            ssm,
            task,
        }
    }
    async fn post(&self, body: &Value) -> Response {
        self.http
            .post(format!("{}/v1/responses", self.endpoint))
            .bearer_auth("test")
            .body(body.to_string())
            .send()
            .await
            .unwrap()
    }
    async fn ssm_raw(&self, body: &str) -> Response {
        self.http
            .post(format!("{}/", self.endpoint))
            .header("x-amz-target", "AmazonSSM.GetParameter")
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
    }
    async fn unfinished(&self, method: &str, path: &str, headers: &str, expected: &str) {
        let mut stream = TcpStream::connect(self.endpoint.strip_prefix("http://").unwrap())
            .await
            .unwrap();
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: synthetic-bucket.localhost\r\n{headers}Content-Length: 100\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !String::from_utf8_lossy(&response).contains(expected) {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0, "closed before expected response");
                response.extend_from_slice(&buffer[..count]);
            }
        })
        .await
        .expect("service waited for rejected request body");
    }
}
async fn json_body(response: Response, status: u16, id_header: &str) -> Value {
    assert_eq!(response.status().as_u16(), status);
    assert!(!response.headers()[id_header].to_str().unwrap().is_empty());
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}
async fn output(response: Response) -> Value {
    let body = json_body(response, 200, "x-request-id").await;
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["model"], "synthetic-model");
    assert!(body["created_at"].is_u64());
    assert!(body["id"].as_str().unwrap().starts_with("resp_"));
    assert!(body["error"].is_null());
    assert!(body["incomplete_details"].is_null());
    let messages = body["output"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["role"], "assistant");
    assert_eq!(messages[0]["type"], "message");
    let content = messages[0]["content"].as_array().unwrap();
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["type"], "output_text");
    serde_json::from_str(content[0]["text"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn ssm_sdk_types_decryption_and_modeled_missing_parameter() {
    let e = Emulator::new(None, json!([])).await;
    for (name, kind, value) in [
        ("/example/plain", ParameterType::String, "synthetic"),
        ("/example/list", ParameterType::StringList, "one,two"),
    ] {
        for decrypt in [None, Some(false), Some(true)] {
            let p = e
                .ssm
                .get_parameter()
                .name(format!(" {name} "))
                .set_with_decryption(decrypt)
                .send()
                .await
                .unwrap()
                .parameter
                .unwrap();
            assert_eq!(p.name(), Some(name));
            assert_eq!(p.r#type(), Some(&kind));
            assert_eq!(p.value(), Some(value));
            assert_eq!(p.version(), 1);
            assert_eq!(p.data_type(), Some("text"));
        }
    }
    for (decrypt, expected) in [
        (None, "synthetic-ciphertext"),
        (Some(false), "synthetic-ciphertext"),
        (Some(true), "test"),
    ] {
        let p = e
            .ssm
            .get_parameter()
            .name("/example/secure")
            .set_with_decryption(decrypt)
            .send()
            .await
            .unwrap()
            .parameter
            .unwrap();
        assert_eq!(p.r#type(), Some(&ParameterType::SecureString));
        assert_eq!(p.value(), Some(expected));
    }
    let error = e
        .ssm
        .get_parameter()
        .name("/example/missing")
        .send()
        .await
        .unwrap_err();
    assert!(error.as_service_error().unwrap().is_parameter_not_found());
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ParameterNotFound")
    );
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    assert!(
        error
            .raw_response()
            .unwrap()
            .headers()
            .get("x-amzn-requestid")
            .is_some()
    );
}

#[tokio::test]
async fn ssm_validation_protocol_and_body_limit() {
    let e = Emulator::new(None, json!([])).await;
    for body in [
        json!({}),
        json!({"Name":3}),
        json!({"Name":""}),
        json!({"Name":"/example/interior space"}),
        json!({"Name":"/example/plain:1"}),
        json!({"Name":"arn:aws:ssm:us-east-1:000000000000:parameter/example"}),
        json!({"Name":"/example/plain","WithDecryption":"true"}),
        json!({"Name":"/example/plain","WithDecryption":null}),
    ] {
        let r = e.ssm_raw(&body.to_string()).await;
        assert_eq!(r.headers()["content-type"], "application/x-amz-json-1.1");
        assert_eq!(
            json_body(r, 400, "x-amzn-requestid").await["__type"],
            "ValidationException"
        );
    }
    // Valid GetParameter JSON isolates collection overflow from JSON parse errors.
    let oversized = format!(
        "{}{}",
        json!({"Name":"/example/plain"}),
        " ".repeat(64 * 1024)
    );
    for body in ["{".to_owned(), oversized] {
        assert_eq!(
            json_body(e.ssm_raw(&body).await, 400, "x-amzn-requestid").await["__type"],
            "SerializationException"
        );
    }
    e.unfinished(
        "POST",
        "/",
        "X-Amz-Target: AmazonSSM.Unknown\r\n",
        "\"__type\":\"UnknownOperationException\"",
    )
    .await;
    e.unfinished(
        "PUT",
        "/v1/responses",
        "X-Amz-Target: AmazonSSM.GetParameter\r\n",
        "application/x-amz-json-1.1",
    )
    .await;
}

#[tokio::test]
async fn fixture_matches_format_and_text_only_in_file_order() {
    let e = Emulator::new(Some(fixtures()), json!([])).await;
    assert_eq!(
        output(e.post(&request("example_extraction")).await).await,
        json!({"items":["synthetic"]})
    );
    assert_eq!(
        output(e.post(&request("other_extraction")).await).await,
        json!([1, "two", null])
    );
    let mut body = request("example_extraction");
    body["input"] = json!("sample in a string");
    assert_eq!(
        output(e.post(&body).await).await,
        json!({"items":["synthetic"]})
    );
    for input in [
        json!([{"role":"user","content":[{"type":"input_text","text":"sam"},{"type":"input_text","text":"ple"}]}]),
        json!([{"role":"user","content":[{"type":"input_text","text":"SAMPLE"},{"type":"input_image","image_url":"data:image/png;base64,sample"},{"type":"input_file","filename":"synthetic.pdf","file_data":"data:application/pdf;base64,sample"}]}]),
    ] {
        body["input"] = input;
        body["text"]["format"]["schema"] = json!({"description":"sample"});
        assert_eq!(output(e.post(&body).await).await, json!({"fallback":true}));
    }
    let no_match = json_body(e.post(&request("missing")).await, 400, "x-request-id").await;
    assert_eq!(no_match["error"]["code"], "fixture_not_found");
}

#[tokio::test]
async fn bearer_is_required_before_reading_body() {
    let e = Emulator::new(Some(fixtures()), json!([])).await;
    for auth in [
        None,
        Some("Bearer "),
        Some("Basic test"),
        Some("Bearer test test"),
    ] {
        let mut r = e.http.post(format!("{}/v1/responses", e.endpoint));
        if let Some(auth) = auth {
            r = r.header("authorization", auth);
        }
        let body = json_body(r.body("{").send().await.unwrap(), 401, "x-request-id").await;
        assert_eq!(body["error"]["code"], "invalid_api_key");
        assert_eq!(body["error"]["type"], "authentication_error");
    }
    let r = e
        .http
        .post(format!("{}/v1/responses", e.endpoint))
        .header("authorization", "Bearer test")
        .header("authorization", "Bearer test")
        .body(request("example_extraction").to_string())
        .send()
        .await
        .unwrap();
    json_body(r, 401, "x-request-id").await;
    // Base host is needed here: bucket-host addressing deliberately belongs to S3.
    let mut stream = TcpStream::connect(e.endpoint.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    stream
        .write_all(b"POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = [0; 4096];
    let n = tokio::time::timeout(std::time::Duration::from_secs(3), stream.read(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes[..n]).starts_with("HTTP/1.1 401 "));
}

#[tokio::test]
async fn invalid_requests_do_not_consume_faults_and_all_matching_rules_advance() {
    let mut config = fixtures();
    config["faults"] = json!([
        {"first_request":1,"repeat":1,"effect":"throttle"},
        {"format_name":"example_extraction","first_request":2,"repeat":1,"effect":"internal_server_error"},
        {"format_name":"example_extraction","first_request":3,"repeat":1,"effect":"incomplete"}
    ]);
    let e = Emulator::new(Some(config.clone()), json!([])).await;
    for field in ["model", "input", "text"] {
        let mut invalid = request("example_extraction");
        invalid.as_object_mut().unwrap().remove(field);
        json_body(e.post(&invalid).await, 400, "x-request-id").await;
    }
    for (field, value) in [
        ("stream", json!(true)),
        ("background", json!(true)),
        ("tools", json!([{"type":"web_search"}])),
    ] {
        let mut invalid = request("example_extraction");
        invalid[field] = value;
        json_body(e.post(&invalid).await, 400, "x-request-id").await;
    }
    json_body(e.post(&request("missing")).await, 400, "x-request-id").await;
    let invalid = e
        .http
        .post(format!("{}/v1/responses", e.endpoint))
        .bearer_auth("test")
        .body("{")
        .send()
        .await
        .unwrap();
    json_body(invalid, 400, "x-request-id").await;
    let unauth = e
        .http
        .post(format!("{}/v1/responses", e.endpoint))
        .body(request("example_extraction").to_string())
        .send()
        .await
        .unwrap();
    json_body(unauth, 401, "x-request-id").await;
    let oversize = e
        .http
        .post(format!("{}/v1/responses", e.endpoint))
        .bearer_auth("test")
        .body(" ".repeat(32 * 1024 * 1024 + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        json_body(oversize, 413, "x-request-id").await["error"]["code"],
        "request_too_large"
    );
    // Other services do not advance OpenAI's counter.
    e.ssm
        .get_parameter()
        .name("/example/plain")
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.http
            .put(format!("{}/synthetic-bucket/key", e.endpoint))
            .body("synthetic")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    for emulator in [&e, &Emulator::new(Some(config), json!([])).await] {
        for (status, code, kind) in [
            (429, "rate_limit_exceeded", "rate_limit_error"),
            (500, "internal_server_error", "server_error"),
        ] {
            let body = json_body(
                emulator.post(&request("example_extraction")).await,
                status,
                "x-request-id",
            )
            .await;
            assert_eq!(body["error"]["code"], code);
            assert_eq!(body["error"]["type"], kind);
        }
        let r = json_body(
            emulator.post(&request("example_extraction")).await,
            200,
            "x-request-id",
        )
        .await;
        assert_eq!(r["status"], "incomplete");
        assert_eq!(r["output"], json!([]));
        assert_eq!(r["incomplete_details"]["reason"], "max_output_tokens");
        assert!(r["error"].is_null());
        assert_eq!(
            output(emulator.post(&request("example_extraction")).await).await,
            json!({"items":["synthetic"]})
        );
    }
}

fn signing(service: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test"
    )
}
#[tokio::test]
async fn service_ownership_raw_paths_enablement_and_shared_listener() {
    let e = Emulator::new(Some(fixtures()), json!([])).await;
    // Target ownership precedes S3 host and conflicting scopes, and the stub path.
    let r = e
        .http
        .post(format!("{}/", e.endpoint))
        .header("host", "synthetic-bucket.localhost")
        .header("authorization", signing("s3"))
        .header("x-amz-target", "AmazonSSM.GetParameter")
        .body(r#"{"Name":"/example/plain"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        json_body(r, 200, "x-amzn-requestid").await["Parameter"]["Value"],
        "synthetic"
    );
    for (target, path, expected) in [
        (
            "AmazonSSM.Unknown",
            "/v1/responses",
            "UnknownOperationException",
        ),
        (
            "DynamoDB_20120810.Unknown",
            "/v1/responses",
            "com.amazon.coral.service#UnknownOperationException",
        ),
        (
            "Unknown.Get",
            "/v1/responses",
            "com.amazon.coral.service#UnknownOperationException",
        ),
    ] {
        let r = e
            .http
            .post(format!("{}{path}", e.endpoint))
            .header("x-amz-target", target)
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(
            json_body(r, 400, "x-amzn-requestid").await["__type"],
            expected
        );
    }
    for service in ["ssm", "dynamodb", "lambda"] {
        let r = e
            .http
            .post(format!("{}/v1/responses", e.endpoint))
            .header("authorization", signing(service))
            .body("{")
            .send()
            .await
            .unwrap();
        let body = json_body(r, 400, "x-amzn-requestid").await;
        assert!(
            body["__type"]
                .as_str()
                .unwrap()
                .ends_with("UnknownOperationException")
        );
        if service == "ssm" {
            assert_eq!(body["__type"], "UnknownOperationException");
        }
    }
    let r = e
        .http
        .post(format!("{}/v1/responses", e.endpoint))
        .header("authorization", signing("s3"))
        .body("{")
        .send()
        .await
        .unwrap();
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>InvalidBucketName</Code>")
    );
    for path in ["/v1/responses/", "/v1/%72esponses", "/v1/responses/extra"] {
        let r = e
            .http
            .post(format!("{}{path}", e.endpoint))
            .body("{")
            .send()
            .await
            .unwrap();
        assert!(
            r.headers()["content-type"]
                .to_str()
                .unwrap()
                .contains("xml")
        );
    }
    for method in [reqwest::Method::GET, reqwest::Method::PUT] {
        let r = e
            .http
            .request(method, format!("{}/v1/responses", e.endpoint))
            .send()
            .await
            .unwrap();
        assert!(
            r.text()
                .await
                .unwrap()
                .contains("<Code>InvalidBucketName</Code>")
        );
    }
    let r = e
        .http
        .post(format!("{}/v1/responses?synthetic=query", e.endpoint))
        .bearer_auth("test")
        .body(request("example_extraction").to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(output(r).await, json!({"items":["synthetic"]}));
    // A valid bucket host retains the entire service-looking S3 object key.
    let url = format!("{}/v1/responses", e.endpoint);
    assert_eq!(
        e.http
            .put(&url)
            .header("host", "synthetic-bucket.localhost")
            .body("stored")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        e.http
            .get(&url)
            .header("host", "synthetic-bucket.localhost")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "stored"
    );
    let r = e.http.post(format!("{}/", e.endpoint)).header("x-amz-target", "DynamoDB_20120810.CreateTable")
        .body(json!({"TableName":"synthetic-table","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}).to_string()).send().await.unwrap();
    assert_eq!(
        json_body(r, 200, "x-amzn-requestid").await["TableDescription"]["TableName"],
        "synthetic-table"
    );
    let disabled = Emulator::new(None, json!([])).await;
    let r = disabled.post(&request("example_extraction")).await;
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("xml")
    );
    let empty = Emulator::new(Some(json!({"fixtures":[]})), json!([])).await;
    assert_eq!(
        json_body(
            empty.post(&request("example_extraction")).await,
            400,
            "x-request-id"
        )
        .await["error"]["code"],
        "fixture_not_found"
    );
}

#[test]
fn bad_configuration_is_rejected_before_serving() {
    for config in [
        json!({"parameters":[{"name":"/example/key","type":"String","value":"test","encrypted_value":"synthetic"}]}),
        json!({"parameters":[{"name":"/example/key","type":"SecureString","value":"test"}]}),
        json!({"parameters":[{"name":" invalid ","type":"String","value":"test"}]}),
        json!({"parameters":[{"name":"/same","type":"String","value":"test"},{"name":"/same","type":"String","value":"test"}]}),
    ] {
        let ssm = serde_json::from_value(config).unwrap();
        assert!(
            router_with_services(
                Config::default(),
                ServiceConfig {
                    ssm,
                    openai: None,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    for config in [
        json!({"parameters":[],"unknown":true}),
        json!({"parameters":[{"name":"/key","type":"String","value":"test","encrypted_value":null}]}),
        json!({"parameters":[{"name":"/key","type":"Unsupported","value":"test"}]}),
    ] {
        assert!(serde_json::from_value::<supercloudy::server::SsmConfig>(config).is_err());
    }
    for (first, repeat) in [(0, 1), (1, 0), (u64::MAX, 2)] {
        let openai = serde_json::from_value(json!({"fixtures":[],"faults":[{"first_request":first,"repeat":repeat,"effect":"throttle"}]})).unwrap();
        assert!(
            router_with_services(
                Config::default(),
                ServiceConfig {
                    openai: Some(openai),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    for config in [
        json!({"fixtures":[{"format_name":"","output":{}}]}),
        json!({"fixtures":[{"format_name":"example","input_contains":"","output":{}}]}),
    ] {
        let openai = serde_json::from_value(config).unwrap();
        assert!(
            router_with_services(
                Config::default(),
                ServiceConfig {
                    openai: Some(openai),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn chunked_wire_limit_without_content_length_preserves_fault() {
    let config = json!({"fixtures":[{"format_name":"example_extraction","output":{}}],"faults":[{"first_request":1,"repeat":2,"effect":"throttle"}]});
    let e = Emulator::new(Some(config), json!([])).await;
    let mut stream = TcpStream::connect(e.endpoint.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    stream.write_all(b"POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
    // Deliver actual body frames rather than trusting a declared Content-Length.
    let chunk = vec![b' '; 1024 * 1024];
    for _ in 0..32 {
        stream.write_all(b"100000\r\n").await.unwrap();
        stream.write_all(&chunk).await.unwrap();
        stream.write_all(b"\r\n").await.unwrap();
    }
    stream.write_all(b"1\r\nx\r\n0\r\n\r\n").await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 413 "));
    assert!(response.contains("x-request-id:"));
    assert!(response.contains("request_too_large"));
    let mut ids = Vec::new();
    for _ in 0..2 {
        let response = e.post(&request("example_extraction")).await;
        ids.push(
            response.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
        json_body(response, 429, "x-request-id").await;
    }
    assert_ne!(ids[0], ids[1]);
    assert_eq!(
        output(e.post(&request("example_extraction")).await).await,
        json!({})
    );
}

#[tokio::test]
async fn malformed_or_conflicting_aws_selectors_cannot_reach_stub() {
    let e = Emulator::new(Some(fixtures()), json!([])).await;
    for headers in [
        vec![
            ("x-amz-target", "AmazonSSM.GetParameter"),
            ("x-amz-target", "AmazonSSM.GetParameter"),
        ],
        vec![("x-amz-target", "AmazonSSM")],
        vec![("authorization", "AWS4-HMAC-SHA256 Credential=malformed")],
        vec![("authorization", "AWS4-HMAC-SHA256")],
    ] {
        let mut r = e.http.post(format!("{}/v1/responses", e.endpoint));
        for (key, value) in headers {
            r = r.header(key, value);
        }
        let body = json_body(r.body("{").send().await.unwrap(), 400, "x-amzn-requestid").await;
        assert_eq!(
            body["__type"],
            "com.amazon.coral.service#UnknownOperationException"
        );
    }
    let r = e
        .http
        .post(format!(
            "{}/v1/responses?X-Amz-Credential=test%2F20261031%2Fus-east-1%2Fssm%2Faws4_request",
            e.endpoint
        ))
        .header("authorization", signing("s3"))
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("<Code>InvalidRequest</Code>")
    );
}
