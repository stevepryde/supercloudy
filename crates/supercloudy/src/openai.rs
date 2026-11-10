//! Deterministic local Responses fixtures. No HTTP client or outbound operations.
pub(crate) mod faults;
mod streaming;
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
    #[serde(default)]
    pub fixtures: Vec<Fixture>,
    #[serde(default)]
    pub conversation_fixtures: Vec<ConversationFixture>,
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
/// Plain-text or function-call fixtures for conversational Responses clients.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationFixture {
    pub input_contains: Option<String>,
    pub function_output_contains: Option<String>,
    pub output_text: Option<String>,
    pub function_call: Option<FunctionCall>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionCall {
    pub name: String,
    /// JSON object encoded as a string, as required by the Responses wire format.
    pub arguments: String,
}
struct PreparedFixture {
    format_name: String,
    input_contains: Option<String>,
    output: String,
}
#[derive(Clone)]
pub(crate) struct Service {
    fixtures: Arc<Vec<PreparedFixture>>,
    conversation_fixtures: Arc<Vec<ConversationFixture>>,
    faults: faults::Faults,
}
impl Service {
    pub(crate) fn new(config: Config) -> Result<Self, String> {
        let faults = faults::Faults::new(config.faults)?;
        for (index, fixture) in config.conversation_fixtures.iter().enumerate() {
            if fixture
                .input_contains
                .as_ref()
                .is_some_and(|v| v.is_empty())
                || fixture
                    .function_output_contains
                    .as_ref()
                    .is_some_and(|v| v.is_empty())
                || (fixture.input_contains.is_some() && fixture.function_output_contains.is_some())
                || (fixture.output_text.is_some() == fixture.function_call.is_some())
                || fixture.function_call.as_ref().is_some_and(|call| {
                    call.name.trim().is_empty()
                        || !serde_json::from_str::<Value>(&call.arguments)
                            .is_ok_and(|arguments| arguments.is_object())
                })
            {
                return Err(format!(
                    "invalid conversation fixture {}: expected one output, at most one nonempty selector, and valid function arguments",
                    index + 1
                ));
            }
        }
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
            conversation_fixtures: Arc::new(config.conversation_fixtures),
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
        let (name, model, texts, stream) = match validate_request(&raw) {
            Ok(fields) => fields,
            Err(message) => return invalid(message),
        };
        if name.is_none() {
            return self.conversation_response(&raw, model, stream);
        }
        let name = name.expect("structured request has a format name");
        let fixture = self.fixtures.iter().find(|fixture| {
            fixture.format_name == name
                && fixture
                    .input_contains
                    .as_ref()
                    .is_none_or(|substring| texts.iter().any(|text| text.contains(substring)))
        });
        let Some(fixture) = fixture else {
            return no_fixture();
        };
        if let Some(response) = self.faults.response(Some(name), model, stream) {
            return response;
        }
        envelope(model, Some(&fixture.output), stream)
    }
    fn conversation_response(&self, raw: &Value, model: &str, stream: bool) -> Response {
        let Some((texts, function_output)) = latest_input(raw) else {
            return no_fixture();
        };
        let fixture = self.conversation_fixtures.iter().find(|fixture| {
            match (&fixture.function_output_contains, function_output) {
                (Some(needle), true) => texts.iter().any(|text| text.contains(needle)),
                (None, false) => fixture
                    .input_contains
                    .as_ref()
                    .is_none_or(|needle| texts.iter().any(|text| text.contains(needle))),
                _ => false,
            }
        });
        let Some(fixture) = fixture else {
            return no_fixture();
        };
        if let Some(response) = self.faults.response(None, model, stream) {
            return response;
        }
        let id = crate::protocol::request_id();
        let item = if let Some(call) = &fixture.function_call {
            json!({"id": format!("fc_{id}"), "type": "function_call", "status": "completed",
                "call_id": format!("call_{id}"), "name": call.name, "arguments": call.arguments})
        } else {
            message(
                &id,
                fixture.output_text.as_deref().expect("validated output"),
            )
        };
        render(model, Some(item), stream, &id)
    }
}
type RequestFields<'a> = (Option<&'a str>, &'a str, Vec<&'a str>, bool);
fn validate_request(raw: &Value) -> Result<RequestFields<'_>, &'static str> {
    let model = raw["model"]
        .as_str()
        .filter(|m| !m.trim().is_empty())
        .ok_or("model must be nonempty")?;
    let format = &raw["text"]["format"];
    let name = if format.is_null() || format == &json!({"type":"text"}) {
        None
    } else {
        if format["type"] != "json_schema" || !format["schema"].is_object() {
            return Err("text.format must be text or json_schema with an object schema");
        }
        Some(
            format["name"]
                .as_str()
                .filter(|n| !n.trim().is_empty())
                .ok_or("text.format.name must be nonempty")?,
        )
    };
    let stream = match raw.get("stream") {
        None | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        _ => return Err("stream must be a boolean"),
    };
    if raw.get("background").is_some_and(|v| v != false) {
        return Err("background must be false or absent");
    }
    if raw
        .get("tools")
        .is_some_and(|v| v.as_array().is_none_or(|a| name.is_some() && !a.is_empty()))
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
            if name.is_none() {
                match item["type"].as_str() {
                    Some("function_call")
                        if item["call_id"].is_string()
                            && item["name"].is_string()
                            && item["arguments"].is_string() =>
                    {
                        continue;
                    }
                    Some("function_call_output")
                        if item["call_id"].is_string()
                            && (item["output"].is_string()
                                || item["output"].as_array().is_some_and(|blocks| {
                                    blocks.iter().all(|block| {
                                        block["type"] == "input_text" && block["text"].is_string()
                                    })
                                })) =>
                    {
                        continue;
                    }
                    Some("reasoning") => continue,
                    Some("additional_tools") if item["tools"].is_array() => continue,
                    None | Some("message") => {}
                    _ => return Err("unsupported or invalid input item"),
                }
            }
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
                    Some("input_text") | Some("output_text") => {
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
    Ok((name, model, texts, stream))
}
/// History must not reselect an earlier turn's fixture or replay a tool call.
fn latest_input(raw: &Value) -> Option<(Vec<&str>, bool)> {
    if let Some(text) = raw["input"].as_str() {
        return Some((vec![text], false));
    }
    for item in raw["input"]
        .as_array()
        .expect("validated input")
        .iter()
        .rev()
    {
        if item["type"] == "function_call_output" {
            let texts = if let Some(text) = item["output"].as_str() {
                vec![text]
            } else {
                item["output"]
                    .as_array()
                    .expect("validated output")
                    .iter()
                    .filter_map(|block| block["text"].as_str())
                    .collect()
            };
            return Some((texts, true));
        }
        if item["role"] == "user" && matches!(item["type"].as_str(), None | Some("message")) {
            let texts = if let Some(text) = item["content"].as_str() {
                vec![text]
            } else {
                item["content"]
                    .as_array()
                    .expect("validated content")
                    .iter()
                    .filter(|block| block["type"] == "input_text")
                    .filter_map(|block| block["text"].as_str())
                    .collect()
            };
            return Some((texts, false));
        }
    }
    None
}
fn no_fixture() -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_request_error",
        "fixture_not_found",
        "No matching fixture",
    )
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
fn message(id: &str, text: &str) -> Value {
    json!({"id": format!("msg_{id}"), "type": "message", "role": "assistant", "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}]})
}
fn envelope(model: &str, output: Option<&str>, stream: bool) -> Response {
    let id = crate::protocol::request_id();
    render(model, output.map(|text| message(&id, text)), stream, &id)
}
fn render(model: &str, item: Option<Value>, stream: bool, id: &str) -> Response {
    let status = if item.is_some() {
        "completed"
    } else {
        "incomplete"
    };
    let body = json!({
        "id": format!("resp_{id}"), "object": "response",
        "created_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        "status": status, "model": model, "error": null,
        "incomplete_details": if status == "incomplete" { json!({"reason": "max_output_tokens"}) } else { Value::Null },
        "output": item.into_iter().collect::<Vec<_>>(),
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0,
            "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}}
    });
    if stream {
        streaming::response(body, id)
    } else {
        response(body, StatusCode::OK, id)
    }
}
