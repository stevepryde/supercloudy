//! Immutable synthetic Parameter Store subset; no credentials, encryption or cloud calls.
use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub parameters: Vec<Parameter>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: ParameterType,
    pub value: String,
    #[serde(default, deserialize_with = "optional_string")]
    pub encrypted_value: Option<String>,
}
fn optional_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}
#[derive(Clone, Copy, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
pub enum ParameterType {
    String,
    StringList,
    SecureString,
}
#[derive(Clone)]
pub(crate) struct Service(Arc<BTreeMap<String, Parameter>>);

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 2048
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/_.-".contains(&b))
}
impl Service {
    pub(crate) fn new(config: Config) -> Result<Self, String> {
        let mut parameters = BTreeMap::new();
        for (index, parameter) in config.parameters.into_iter().enumerate() {
            if !valid_name(&parameter.name)
                || (parameter.kind == ParameterType::SecureString)
                    != parameter.encrypted_value.is_some()
            {
                return Err(format!(
                    "invalid SSM parameter {}: name/type/encrypted_value combination",
                    index + 1
                ));
            }
            if parameters
                .insert(parameter.name.clone(), parameter)
                .is_some()
            {
                return Err(format!("duplicate SSM parameter at position {}", index + 1));
            }
        }
        Ok(Self(Arc::new(parameters)))
    }
    pub(crate) async fn dispatch(&self, operation: Option<&str>, request: Request) -> Response {
        if operation != Some("GetParameter") {
            return unknown_operation();
        }
        let body = match to_bytes(request.into_body(), 64 * 1024).await {
            Ok(body) => body,
            Err(_) => return error("SerializationException", "Unable to read request body"),
        };
        let raw: serde_json::Value = match serde_json::from_slice(&body) {
            Ok(raw) => raw,
            Err(_) => return error("SerializationException", "Invalid JSON body"),
        };
        let Some(name) = raw.get("Name").and_then(|v| v.as_str()).map(str::trim) else {
            return error("ValidationException", "Name must be a string");
        };
        if !valid_name(name) || raw.get("WithDecryption").is_some_and(|v| !v.is_boolean()) {
            return error(
                "ValidationException",
                "Unsupported name or WithDecryption value",
            );
        }
        let Some(parameter) = self.0.get(name) else {
            return error("ParameterNotFound", "Parameter not found");
        };
        let decrypt = raw
            .get("WithDecryption")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let value = if parameter.kind == ParameterType::SecureString && !decrypt {
            parameter
                .encrypted_value
                .as_deref()
                .expect("validated SecureString")
        } else {
            &parameter.value
        };
        response(
            json!({"Parameter": {"Name": name, "Type": parameter.kind, "Value": value, "Version": 1, "DataType": "text"}}),
            StatusCode::OK,
        )
    }
}
fn response(body: serde_json::Value, status: StatusCode) -> Response {
    (
        status,
        [
            ("content-type", "application/x-amz-json-1.1"),
            ("x-amzn-requestid", &crate::protocol::request_id()),
        ],
        Json(body),
    )
        .into_response()
}
fn error(code: &str, message: &str) -> Response {
    response(
        json!({"__type": code, "message": message}),
        StatusCode::BAD_REQUEST,
    )
}
pub(crate) fn unknown_operation() -> Response {
    error("UnknownOperationException", "Unknown operation")
}
