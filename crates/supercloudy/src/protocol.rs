use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
pub(crate) fn request_id() -> String {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed).to_string()
}
pub(crate) fn response(body: Value, status: StatusCode) -> Response {
    let id = request_id();
    (
        status,
        [
            (header::CONTENT_TYPE, "application/x-amz-json-1.0"),
            (header::HeaderName::from_static("x-amzn-requestid"), &id),
        ],
        Json(body),
    )
        .into_response()
}
pub(crate) fn error(code: &str, message: &str) -> Response {
    let namespace = if code == "UnknownOperationException" {
        "com.amazon.coral.service"
    } else {
        "com.amazonaws.dynamodb.v20120810"
    };
    response(
        json!({"__type":format!("{namespace}#{code}"),"message":message}),
        StatusCode::BAD_REQUEST,
    )
}
pub(crate) fn modeled_error(error: crate::dynamodb::error::Error) -> Response {
    let mut body = error.details;
    body.insert(
        "__type".into(),
        json!(format!("com.amazonaws.dynamodb.v20120810#{}", error.code)),
    );
    body.insert("message".into(), json!(error.message));
    response(
        Value::Object(body),
        StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    )
}
pub(crate) fn unknown_operation() -> Response {
    error("UnknownOperationException", "Unknown operation")
}
