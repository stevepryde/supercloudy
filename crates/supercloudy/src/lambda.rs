//! Local Invoke adapter. The runtime owns accepted Event tasks until drain/cancel.
pub mod faults;

use axum::{
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Semaphore, task::JoinSet};

const RESPONSE_LIMIT: usize = 6 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub functions: BTreeMap<String, Target>,
    #[serde(default = "concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "drain_timeout")]
    pub shutdown_timeout_ms: u64,
    #[serde(default)]
    pub faults: Vec<faults::Rule>,
}
fn concurrency() -> usize {
    16
}
fn drain_timeout() -> u64 {
    5000
}

/// Canned responses are literal UTF-8 bytes; HTTP targets receive raw Invoke bytes.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Target {
    Http {
        url: String,
    },
    Canned {
        response: String,
        function_error: Option<String>,
    },
}

struct Tasks {
    accepting: bool,
    events: JoinSet<()>,
}
struct Inner {
    functions: BTreeMap<String, Target>,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
    tasks: Mutex<Tasks>,
    shutdown_timeout: Duration,
    faults: faults::Faults,
}
#[derive(Clone)]
pub(crate) struct Service(Arc<Inner>);

pub(crate) fn reserved(path: &str) -> bool {
    path == "/2015-03-31/functions" || path.starts_with("/2015-03-31/functions/")
}
pub(crate) fn unconfigured() -> Response {
    error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        "Function does not exist",
    )
}

impl Service {
    pub(crate) fn new(config: Config) -> Result<Self, String> {
        if !(1..=1024).contains(&config.max_concurrency) || config.shutdown_timeout_ms == 0 {
            return Err(
                "Lambda requires concurrency 1..1024 and a positive shutdown timeout".into(),
            );
        }
        for (name, target) in &config.functions {
            if name.is_empty() || name.contains('/') {
                return Err("Lambda function names must be nonempty path segments".into());
            }
            match target {
                Target::Http { url } => {
                    let url = reqwest::Url::parse(url).map_err(|_| "Invalid Lambda target URL")?;
                    let local = url.host_str().is_some_and(|host| {
                        host == "localhost"
                            || host
                                .trim_matches(['[', ']'])
                                .parse::<IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    });
                    if url.scheme() != "http"
                        || !local
                        || !url.username().is_empty()
                        || url.password().is_some()
                        || url.fragment().is_some()
                    {
                        return Err(
                            "Lambda targets require credential-free loopback HTTP URLs".into()
                        );
                    }
                }
                Target::Canned {
                    response,
                    function_error,
                } => {
                    if response.len() > RESPONSE_LIMIT
                        || function_error
                            .as_ref()
                            .is_some_and(|v| v.is_empty() || HeaderValue::from_str(v).is_err())
                    {
                        return Err(
                            "Invalid Lambda canned response or function-error header".into()
                        );
                    }
                }
            }
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| "Unable to construct Lambda HTTP client")?;
        Ok(Self(Arc::new(Inner {
            functions: config.functions,
            client,
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            tasks: Mutex::new(Tasks {
                accepting: true,
                events: JoinSet::new(),
            }),
            shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
            faults: faults::Faults::new(config.faults)?,
        })))
    }

    pub(crate) async fn dispatch(&self, request: Request) -> Response {
        let path = request.uri().path();
        let name = path
            .strip_prefix("/2015-03-31/functions/")
            .and_then(|p| p.strip_suffix("/invocations"))
            .filter(|n| !n.is_empty() && !n.contains('/'));
        if request.method() != Method::POST || name.is_none() {
            return error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                "Unknown Lambda operation",
            );
        }
        let name = match percent_encoding::percent_decode_str(name.unwrap()).decode_utf8() {
            Ok(name) => name.into_owned(),
            Err(_) => return invalid("Invalid function name"),
        };
        let Some(target) = self.0.functions.get(&name).cloned() else {
            return error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                "Function does not exist",
            );
        };
        // Aliases/versions have no execution semantics in this local subset.
        if request.uri().query().is_some_and(|q| !q.is_empty()) {
            return invalid("Invoke query options are unsupported; map the exact function name");
        }
        if request
            .headers()
            .get_all("x-amz-invocation-type")
            .iter()
            .count()
            > 1
        {
            return invalid("Duplicate invocation type");
        }
        let invocation = match request.headers().get("x-amz-invocation-type") {
            None => "RequestResponse",
            Some(value) => match value.to_str() {
                Ok("RequestResponse") => "RequestResponse",
                Ok("Event") => "Event",
                Ok("DryRun") => "DryRun",
                _ => return invalid("Unsupported invocation type"),
            },
        };
        let limit = if invocation == "Event" {
            1024 * 1024
        } else {
            RESPONSE_LIMIT
        };
        let payload = match to_bytes(request.into_body(), limit).await {
            Ok(payload) => payload,
            Err(_) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "RequestTooLargeException",
                    "Unable to collect bounded Invoke payload",
                );
            }
        };
        if invocation == "DryRun" {
            return response(StatusCode::NO_CONTENT, Bytes::new(), None);
        }
        let permit = match self.0.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "TooManyRequestsException",
                    "Local invocation capacity unavailable",
                );
            }
        };
        let fault = self.0.faults.select(&name);
        if let Some(response) = fault.and_then(faults::invocation_failure) {
            return response;
        }
        {
            let mut tasks = self.0.tasks.lock().unwrap_or_else(|p| p.into_inner());
            if !tasks.accepting {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceException",
                    "Lambda runtime is shutting down",
                );
            }
            while tasks.events.try_join_next().is_some() {}
            if invocation == "Event" {
                let client = self.0.client.clone();
                // Acceptance is insertion into the runtime-owned JoinSet with a reserved permit.
                // No await occurs under this lock, including the outbound HTTP operation.
                tasks.events.spawn(async move {
                    let _permit = permit;
                    let _ = Self::invoke(client, target, payload, fault).await;
                });
                return response(StatusCode::ACCEPTED, Bytes::new(), None);
            }
        }
        let _permit = permit;
        Self::invoke(self.0.client.clone(), target, payload, fault).await
    }

    async fn invoke(
        client: reqwest::Client,
        target: Target,
        payload: Bytes,
        fault: Option<faults::Effect>,
    ) -> Response {
        if matches!(fault, Some(faults::Effect::FunctionError)) {
            return response(
                StatusCode::OK,
                Bytes::from_static(br#"{"errorMessage":"Injected handler failure"}"#),
                Some(HeaderValue::from_static("Unhandled")),
            );
        }
        match target {
            Target::Canned {
                response: body,
                function_error,
            } => response(
                StatusCode::OK,
                Bytes::from(body),
                function_error.map(|v| HeaderValue::from_str(&v).expect("validated header")),
            ),
            Target::Http { url } => {
                let result = client
                    .post(url)
                    .header("content-type", "application/octet-stream")
                    .body(payload)
                    .send()
                    .await;
                let Ok(mut result) = result else {
                    return transport_error();
                };
                let function_error = result
                    .headers()
                    .get("x-amz-function-error")
                    .cloned()
                    .or_else(|| {
                        (!result.status().is_success())
                            .then(|| HeaderValue::from_static("Unhandled"))
                    });
                let mut body = Vec::new();
                loop {
                    match result.chunk().await {
                        Ok(Some(chunk))
                            if body.len().saturating_add(chunk.len()) <= RESPONSE_LIMIT =>
                        {
                            body.extend_from_slice(&chunk)
                        }
                        Ok(None) => break,
                        _ => return transport_error(),
                    }
                }
                response(StatusCode::OK, Bytes::from(body), function_error)
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut events = {
            let mut tasks = self.0.tasks.lock().unwrap_or_else(|p| p.into_inner());
            tasks.accepting = false;
            self.0.permits.close();
            std::mem::take(&mut tasks.events)
        };
        let drain = async { while events.join_next().await.is_some() {} };
        if tokio::time::timeout(self.0.shutdown_timeout, drain)
            .await
            .is_err()
        {
            events.shutdown().await;
        }
    }
}

fn transport_error() -> Response {
    error(
        StatusCode::BAD_GATEWAY,
        "ServiceException",
        "Unable to invoke local handler",
    )
}
fn invalid(message: &str) -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        message,
    )
}
fn response(status: StatusCode, body: Bytes, function_error: Option<HeaderValue>) -> Response {
    let mut response = (status, Body::from(body)).into_response();
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response.headers_mut().insert(
        "x-amzn-requestid",
        HeaderValue::from_str(&crate::protocol::request_id()).unwrap(),
    );
    if let Some(value) = function_error {
        response.headers_mut().insert("x-amz-function-error", value);
    }
    response
}
fn error(status: StatusCode, code: &'static str, message: &str) -> Response {
    let mut result = response(
        status,
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({"Type":"User", "message":message})).unwrap(),
        ),
        None,
    );
    result
        .headers_mut()
        .insert("x-amzn-errortype", HeaderValue::from_static(code));
    result
}
