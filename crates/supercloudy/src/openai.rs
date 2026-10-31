//! Deterministic local Responses fixtures. No HTTP client or outbound operations.
pub(crate) mod faults;
use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub fixtures: Vec<Fixture>,
    #[serde(default)]
    pub faults: Vec<faults::Rule>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub format_name: String,
    pub input_contains: Option<String>,
    pub output: Value,
}
struct PreparedFixture {
    format_name: String,
    input_contains: Option<String>,
    output: String,
}
#[derive(Clone)]
pub(crate) struct Service {
    fixtures: Arc<Vec<PreparedFixture>>,
    faults: faults::Faults,
}
impl Service {
    pub(crate) fn new(config: Config) -> Result<Self, String> {
        let faults = faults::Faults::new(config.faults)?;
        let mut fixtures = Vec::new();
        for (index, fixture) in config.fixtures.into_iter().enumerate() {
            if fixture.format_name.trim().is_empty()
                || fixture
                    .input_contains
                    .as_ref()
                    .is_some_and(|v| v.is_empty())
            {
                return Err(format!(
                    "invalid OpenAI fixture {}: empty selector",
                    index + 1
                ));
            }
            fixtures.push(PreparedFixture {
                format_name: fixture.format_name,
                input_contains: fixture.input_contains,
                output: serde_json::to_string(&fixture.output).expect("JSON value serializes"),
            });
        }
        Ok(Self {
            fixtures: Arc::new(fixtures),
            faults,
        })
    }
    pub(crate) async fn dispatch(&self, request: Request) -> Response {
        let headers = request.headers();
        let mut auth = headers.get_all("authorization").iter();
        let bearer = auth.next().and_then(|v| v.to_str().ok()).is_some_and(|v| {
            v.split_once(' ').is_some_and(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("Bearer")
                    && !token.is_empty()
                    && !token.chars().any(char::is_whitespace)
            })
        });
        if !bearer || auth.next().is_some() {
            return error(
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                "invalid_api_key",
                "Bearer token required",
            );
        }
        let body = match to_bytes(request.into_body(), 32 * 1024 * 1024).await {
            Ok(body) => body,
            Err(_) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "invalid_request_error",
                    "request_too_large",
                    "Unable to collect request within 32 MiB limit",
                );
            }
        };
        let raw: Value = match serde_json::from_slice(&body) {
            Ok(raw) => raw,
            Err(_) => return invalid("Invalid JSON body"),
        };
        let (name, model, texts) = match validate_request(&raw) {
            Ok(fields) => fields,
            Err(message) => return invalid(message),
        };
        let fixture = self.fixtures.iter().find(|fixture| {
            fixture.format_name == name
                && fixture
                    .input_contains
                    .as_ref()
                    .is_none_or(|substring| texts.iter().any(|text| text.contains(substring)))
        });
        let Some(fixture) = fixture else {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "fixture_not_found",
                "No matching fixture",
            );
        };
        if let Some(response) = self.faults.response(name, model) {
            return response;
        }
        envelope(model, Some(&fixture.output))
    }
}
fn validate_request(raw: &Value) -> Result<(&str, &str, Vec<&str>), &'static str> {
    let model = raw["model"]
        .as_str()
        .filter(|m| !m.trim().is_empty())
        .ok_or("model must be nonempty")?;
    let format = &raw["text"]["format"];
    let name = format["name"]
        .as_str()
        .filter(|n| !n.trim().is_empty())
        .ok_or("text.format.name must be nonempty")?;
    if format["type"] != "json_schema" || !format["schema"].is_object() {
        return Err("text.format must contain json_schema and an object schema");
    }
    for option in ["stream", "background"] {
        if raw.get(option).is_some_and(|v| v != false) {
            return Err("stream/background must be false or absent");
        }
    }
    if raw
        .get("tools")
        .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
    {
        return Err("tools are unsupported");
    }
    let mut texts = Vec::new();
    if let Some(text) = raw["input"].as_str() {
        texts.push(text);
    } else {
        let items = raw["input"]
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or("input must be text or a nonempty message array")?;
        for item in items {
            if !matches!(
                item["role"].as_str(),
                Some("user" | "system" | "developer" | "assistant")
            ) {
                return Err("input message role is unsupported");
            }
            if let Some(text) = item["content"].as_str() {
                texts.push(text);
                continue;
            }
            let content = item["content"]
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or("input message content is required")?;
            for block in content {
                match block["type"].as_str() {
                    Some("input_text") => {
                        texts.push(block["text"].as_str().ok_or("input_text requires text")?)
                    }
                    Some("input_image")
                        if block["image_url"].as_str().is_some_and(|s| !s.is_empty()) => {}
                    Some("input_file")
                        if ["file_data", "file_id", "file_url"]
                            .iter()
                            .any(|key| block[key].as_str().is_some_and(|s| !s.is_empty())) => {}
                    _ => return Err("unsupported input content"),
                }
            }
        }
    }
    Ok((name, model, texts))
}
fn invalid(message: &str) -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_request_error",
        "invalid_request",
        message,
    )
}
fn error(status: StatusCode, kind: &str, code: &str, message: &str) -> Response {
    response(
        json!({"error": {"message": message, "type": kind, "param": null, "code": code}}),
        status,
        &crate::protocol::request_id(),
    )
}
fn response(body: Value, status: StatusCode, id: &str) -> Response {
    (status, [("x-request-id", id)], Json(body)).into_response()
}
fn envelope(model: &str, output: Option<&str>) -> Response {
    let id = crate::protocol::request_id();
    let status = if output.is_some() {
        "completed"
    } else {
        "incomplete"
    };
    let output = output.map_or_else(|| json!([]), |text| json!([{
        "id": format!("msg_{id}"), "type": "message", "role": "assistant", "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}]
    }]));
    response(
        json!({
            "id": format!("resp_{id}"), "object": "response",
            "created_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            "status": status, "model": model, "error": null,
            "incomplete_details": if status == "incomplete" { json!({"reason": "max_output_tokens"}) } else { Value::Null },
            "output": output
        }),
        StatusCode::OK,
        &id,
    )
}
