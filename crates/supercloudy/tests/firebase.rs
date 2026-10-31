use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::{Client, Response};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use supercloudy::server::{Config, FirebaseConfig, ServiceConfig, router_with_services};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const CERTS: &str = "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com";
const PROJECT: &str = "synthetic-project";
struct Emulator {
    endpoint: String,
    http: Client,
    task: JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Emulator {
    async fn new(firebase: Option<FirebaseConfig>) -> Self {
        let router = router_with_services(Config { buckets: vec!["synthetic-bucket".into(), "robot".into(), "mint".into()], ..Default::default() }, ServiceConfig {
            firebase,
            ssm: serde_json::from_value(json!({"parameters":[{"name":"/example/plain","type":"String","value":"synthetic"}]})).unwrap(),
            openai: Some(serde_json::from_value(json!({"fixtures":[{"format_name":"synthetic","output":{"ok":true}}]})).unwrap()),
        }).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            http: Client::builder().no_proxy().build().unwrap(),
            task,
        }
    }
    async fn mint(&self, body: Value) -> Response {
        self.http
            .post(format!("{}/mint", self.endpoint))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap()
    }
    async fn certs(&self) -> BTreeMap<String, String> {
        let r = self
            .http
            .get(format!("{}{CERTS}?synthetic=query", self.endpoint))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["cache-control"], "public, max-age=123");
        assert!(
            r.headers()["content-type"]
                .to_str()
                .unwrap()
                .contains("application/json")
        );
        serde_json::from_slice(&r.bytes().await.unwrap()).unwrap()
    }
    async fn unfinished(&self, method: &str, path: &str, headers: &str, expected: &str) {
        let mut stream = TcpStream::connect(self.endpoint.strip_prefix("http://").unwrap())
            .await
            .unwrap();
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n{headers}Content-Length: 100\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !String::from_utf8_lossy(&response).contains(expected) {
                let mut bytes = [0; 4096];
                let n = stream.read(&mut bytes).await.unwrap();
                assert_ne!(n, 0, "closed before expected response");
                response.extend_from_slice(&bytes[..n]);
            }
        })
        .await
        .expect("routing waited for an unselected body");
    }
}
fn config() -> FirebaseConfig {
    FirebaseConfig {
        project_id: Some(PROJECT.into()),
        cert_max_age: 123,
    }
}
fn input() -> Value {
    json!({"sub":"synthetic-user", "email":"user@example.test"})
}
async fn minted(r: Response) -> String {
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["cache-control"], "no-store");
    let value: Value = serde_json::from_slice(&r.bytes().await.unwrap()).unwrap();
    value["id_token"].as_str().unwrap().to_owned()
}
fn verify(token: &str, certs: &BTreeMap<String, String>, project: &str) -> Result<Value, String> {
    let header = decode_header(token).map_err(|_| "header".to_owned())?;
    if header.alg != Algorithm::RS256 {
        return Err("algorithm".into());
    }
    let cert = header.kid.and_then(|kid| certs.get(&kid)).ok_or("kid")?;
    let key = DecodingKey::from_rsa_pem(cert.as_bytes()).map_err(|_| "certificate".to_owned())?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.leeway = 60;
    validation.set_audience(&[project]);
    validation.set_issuer(&[format!("https://securetoken.google.com/{project}")]);
    validation.set_required_spec_claims(&["exp", "iat", "aud", "iss", "sub"]);
    let claims = decode::<Value>(token, &key, &validation)
        .map_err(|e| format!("{:?}", e.kind()))?
        .claims;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if claims["iat"].as_u64().is_none_or(|iat| iat > now + 60) {
        return Err("iat".into());
    }
    if claims["sub"]
        .as_str()
        .is_none_or(|sub| sub.is_empty() || sub.chars().count() > 128)
    {
        return Err("sub".into());
    }
    if claims["email_verified"] != true {
        return Err("email_verified".into());
    }
    Ok(claims)
}

#[tokio::test]
async fn served_certificate_verifies_google_claims_and_rejects_variants() {
    let e = Emulator::new(Some(config())).await;
    let certs = e.certs().await;
    assert_eq!(certs.len(), 1);
    assert!(
        certs
            .values()
            .all(|v| v.starts_with("-----BEGIN CERTIFICATE-----"))
    );
    let mut request = input();
    request["extra_claims"] = json!({"role":"reader", "synthetic":{"enabled":true}});
    let token = minted(e.mint(request).await).await;
    let claims =
        verify(&token, &certs, PROJECT).expect("minted token must verify using served x509");
    assert_eq!(claims["sub"], "synthetic-user");
    assert_eq!(claims["email"], "user@example.test");
    assert_eq!(claims["role"], "reader");
    assert_eq!(claims["synthetic"]["enabled"], true);
    assert_eq!(
        claims["firebase"]["identities"]["email"],
        json!(["user@example.test"])
    );
    assert_eq!(claims["firebase"]["sign_in_provider"], "password");
    assert_eq!(claims["auth_time"], claims["iat"]);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["iat"]
            .as_u64()
            .is_some_and(|iat| iat <= now && iat + 60 >= now)
    );
    assert!(
        claims["exp"]
            .as_u64()
            .is_some_and(|exp| exp >= now + 3540 && exp <= now + 3600)
    );
    for (variant, reason) in [
        ("expired", "ExpiredSignature"),
        ("bad_kid", "kid"),
        ("wrong_aud", "InvalidAudience"),
    ] {
        let mut request = input();
        request["variant"] = json!(variant);
        let token = minted(e.mint(request).await).await;
        assert_eq!(verify(&token, &certs, PROJECT).unwrap_err(), reason);
    }
    assert!(
        certs == e.certs().await,
        "instance certificates changed between requests"
    );
    let other = Emulator::new(Some(config())).await;
    let other_certs = other.certs().await;
    let mut request = input();
    request["kid"] = json!(certs.keys().next().unwrap());
    let foreign = minted(other.mint(request).await).await;
    assert_eq!(
        verify(&foreign, &certs, PROJECT).unwrap_err(),
        "InvalidSignature"
    );
    assert!(verify(&token, &other_certs, PROJECT).is_err());
}

#[tokio::test]
async fn explicit_project_times_kid_and_unverified_email() {
    let e = Emulator::new(Some(FirebaseConfig {
        project_id: None,
        ..config()
    }))
    .await;
    let certs = e.certs().await;
    assert_eq!(e.mint(input()).await.status(), 400);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut request = input();
    request["project_id"] = json!("other-project");
    request["iat"] = json!(now - 30);
    request["ttl"] = json!(900);
    request["kid"] = json!(certs.keys().next().unwrap());
    let token = minted(e.mint(request.clone()).await).await;
    let claims = verify(&token, &certs, "other-project").unwrap();
    assert_eq!(claims["iat"], now - 30);
    assert_eq!(claims["exp"], now + 870);
    request.as_object_mut().unwrap().remove("ttl");
    request["exp"] = json!(now + 200);
    let claims = verify(
        &minted(e.mint(request.clone()).await).await,
        &certs,
        "other-project",
    )
    .unwrap();
    assert_eq!(claims["exp"], now + 200);
    request["iat"] = json!(now + 3600);
    assert_eq!(
        verify(
            &minted(e.mint(request.clone()).await).await,
            &certs,
            "other-project"
        )
        .unwrap_err(),
        "iat"
    );
    request["iat"] = json!(now);
    request["email_verified"] = json!(false);
    assert_eq!(
        verify(
            &minted(e.mint(request).await).await,
            &certs,
            "other-project"
        )
        .unwrap_err(),
        "email_verified"
    );
}

#[tokio::test]
async fn mint_validation_is_bounded_and_does_not_echo_values() {
    let e = Emulator::new(Some(config())).await;
    for patch in [
        json!({"sub":""}),
        json!({"sub":"x".repeat(129)}),
        json!({"sub":12}),
        json!({"email":" "}),
        json!({"email_verified":"true"}),
        json!({"variant":"unknown"}),
        json!({"kid":""}),
        json!({"kid":"x".repeat(129)}),
        json!({"ttl":0}),
        json!({"ttl":-1}),
        json!({"iat":u64::MAX,"ttl":1}),
        json!({"ttl":1,"exp":2}),
        json!({"variant":"expired","exp":1}),
        json!({"variant":"bad_kid","kid":"explicit"}),
        json!({"extra_claims":{"iss":"synthetic-sensitive-value"}}),
        json!({"project_id":"bad/project"}),
        json!({"unknown":"synthetic-sensitive-value"}),
    ] {
        let mut request = input();
        request
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        let r = e.mint(request).await;
        assert_eq!(r.status(), 400);
        assert_eq!(r.headers()["cache-control"], "no-store");
        assert!(
            !r.text()
                .await
                .unwrap()
                .contains("synthetic-sensitive-value")
        );
    }
    for field in ["sub", "email"] {
        let mut request = input();
        request.as_object_mut().unwrap().remove(field);
        assert_eq!(e.mint(request).await.status(), 400);
    }
    for body in ["{".to_owned(), "[]".to_owned(), "null".to_owned()] {
        assert_eq!(
            e.http
                .post(format!("{}/mint", e.endpoint))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    // Valid JSON plus wire whitespace isolates collection overflow from decode failure.
    let body = format!("{}{}", input(), " ".repeat(64 * 1024));
    assert_eq!(
        e.http
            .post(format!("{}/mint", e.endpoint))
            .body(body)
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let certs = e.certs().await;
    verify(&minted(e.mint(input()).await).await, &certs, PROJECT).unwrap();
}

fn signing(service: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20261031/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test"
    )
}
#[tokio::test]
async fn ownership_precedes_body_reads_and_preserves_all_services() {
    let e = Emulator::new(Some(config())).await;
    e.unfinished("GET", CERTS, "", "public, max-age=123").await;
    e.unfinished(
        "POST",
        "/mint",
        "X-Amz-Target: AmazonSSM.GetParameter\r\n",
        "application/x-amz-json-1.1",
    )
    .await;
    e.unfinished(
        "POST",
        "/mint",
        "X-Amz-Target: DynamoDB_20120810.CreateTable\r\n",
        "UnknownOperationException",
    )
    .await;
    e.unfinished(
        "POST",
        "/mint",
        "Authorization: AWS4-HMAC-SHA256 Credential=malformed\r\n",
        "UnknownOperationException",
    )
    .await;
    for (method, path) in [
        ("GET", "/mint"),
        ("POST", CERTS),
        (
            "GET",
            "/robot/v1/metadata/x509/securetoken%40system.gserviceaccount.com",
        ),
        (
            "GET",
            "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com/",
        ),
        ("POST", "/mint/"),
        ("POST", "/%6dint"),
    ] {
        let r = e
            .http
            .request(method.parse().unwrap(), format!("{}{path}", e.endpoint))
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
    for path in [CERTS, "/mint"] {
        assert_eq!(
            e.http
                .put(format!("{}{path}", e.endpoint))
                .header("host", "synthetic-bucket.localhost")
                .body("stored-object")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            e.http
                .get(format!("{}{path}", e.endpoint))
                .header("host", "synthetic-bucket.localhost")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "stored-object"
        );
        let r = e
            .http
            .get(format!("{}{path}", e.endpoint))
            .header("authorization", signing("s3"))
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
    let r = e.http.post(format!("{}/",e.endpoint)).header("x-amz-target","DynamoDB_20120810.CreateTable").body(json!({"TableName":"synthetic-table","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}).to_string()).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = e
        .http
        .post(format!("{}/", e.endpoint))
        .header("x-amz-target", "AmazonSSM.GetParameter")
        .body(r#"{"Name":"/example/plain"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = e.http.post(format!("{}/v1/responses",e.endpoint)).bearer_auth("test").body(json!({"model":"synthetic","input":"synthetic", "text":{"format":{"type":"json_schema","name":"synthetic","schema":{}}}}).to_string()).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let disabled = Emulator::new(None).await;
    for path in [CERTS, "/mint"] {
        let r = disabled
            .http
            .request(
                if path == CERTS {
                    reqwest::Method::GET
                } else {
                    reqwest::Method::POST
                },
                format!("{}{path}", disabled.endpoint),
            )
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
}

#[test]
fn invalid_default_project_fails_construction() {
    assert!(
        router_with_services(
            Config::default(),
            ServiceConfig {
                firebase: Some(FirebaseConfig {
                    project_id: Some("bad/project".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }
        )
        .is_err()
    );
}
