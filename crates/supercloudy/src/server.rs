use crate::{
    dynamodb,
    faults::Faults,
    protocol,
    routing::{self, Route},
    s3,
};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::Response,
};
use std::fmt;

pub use crate::dynamodb::faults::{
    CancellationCode, FaultConfig, FaultConfigError, FaultEffect, FaultRule,
};

/// Configuration is validated before a listener starts. Buckets are in memory.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub faults: FaultConfig,
    pub buckets: Vec<String>,
}
#[derive(Debug)]
pub struct ConfigError(String);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConfigError {}

#[derive(Clone)]
struct Services {
    dynamodb: dynamodb::Service,
    s3: s3::Service,
}

/// Construct an isolated in-memory emulator. Clones of this router share state.
pub fn router() -> Router {
    router_with_config(Config::default()).expect("default configuration is valid")
}

/// Construct an isolated emulator with explicit, validated deterministic faults.
pub fn router_with_faults(config: FaultConfig) -> Result<Router, FaultConfigError> {
    let faults = Faults::new(config)?;
    Ok(build(faults, Vec::new()).expect("empty bucket configuration is valid"))
}

pub fn router_with_config(config: Config) -> Result<Router, ConfigError> {
    let faults = Faults::new(config.faults).map_err(|e| ConfigError(e.to_string()))?;
    build(faults, config.buckets)
}
fn build(faults: Faults, buckets: Vec<String>) -> Result<Router, ConfigError> {
    let services = Services {
        dynamodb: dynamodb::Service::with_scheduler(faults.clone()),
        s3: s3::Service::new(buckets, faults).map_err(ConfigError)?,
    };
    Ok(Router::new().fallback(dispatch).with_state(services))
}
async fn dispatch(State(services): State<Services>, request: Request) -> Response {
    let op = match routing::classify(&request) {
        Route::S3 => return services.s3.dispatch(request).await,
        Route::InvalidS3 => return s3::routing_error(&request),
        Route::Unknown => return protocol::unknown_operation(),
        Route::DynamoDb(op) => op,
    };
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
    match services.dynamodb.dispatch(&op, &body).await {
        Ok(body) => protocol::response(body, StatusCode::OK),
        Err(e) => protocol::modeled_error(e),
    }
}
