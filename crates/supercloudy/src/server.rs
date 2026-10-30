use crate::{
    dynamodb::{self, Service},
    protocol,
};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::Response,
};
/// Construct an isolated in-memory emulator. Clones of this router share state.
pub fn router() -> Router {
    Router::new()
        .fallback(dispatch)
        .with_state(Service::default())
}

pub use crate::dynamodb::faults::{
    CancellationCode, FaultConfig, FaultConfigError, FaultEffect, FaultRule,
};

/// Construct an isolated emulator with explicit, validated deterministic faults.
pub fn router_with_faults(config: FaultConfig) -> Result<Router, FaultConfigError> {
    Ok(Router::new()
        .fallback(dispatch)
        .with_state(Service::with_faults(config)?))
}
async fn dispatch(State(service): State<Service>, request: Request) -> Response {
    if request.method() != Method::POST || request.uri().path() != "/" {
        return protocol::unknown_operation();
    }
    let target = request
        .headers()
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok());
    let Some(op) = dynamodb::operation(target) else {
        return protocol::unknown_operation();
    };
    let op = op.to_owned();
    let body = match to_bytes(request.into_body(), 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) if op == "BatchWriteItem" => {
            return protocol::error(
                "ValidationException",
                "Batch write request exceeds supported body size",
            );
        }
        Err(_) => return protocol::error("SerializationException", "Unable to read request body"),
    };
    match service.dispatch(&op, &body).await {
        Ok(body) => protocol::response(body, StatusCode::OK),
        Err(e) => protocol::modeled_error(e),
    }
}
