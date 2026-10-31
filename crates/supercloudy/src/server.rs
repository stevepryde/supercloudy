use crate::{
    dynamodb,
    fault_config::Faults,
    firebase, openai, protocol,
    routing::{self, Route},
    s3, ssm,
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
/// Optional services on the existing listener. `Some` enables each HTTP stub.
/// Responses may have no fixtures. Parameters default to an empty immutable store.
#[derive(Clone, Debug, Default)]
pub struct ServiceConfig {
    pub ssm: SsmConfig,
    pub openai: Option<OpenAiConfig>,
    pub firebase: Option<FirebaseConfig>,
}
pub use crate::firebase::Config as FirebaseConfig;
pub use crate::openai::faults::{Effect as OpenAiFaultEffect, Rule as OpenAiFaultRule};
pub use crate::openai::{Config as OpenAiConfig, Fixture};
pub use crate::ssm::{Config as SsmConfig, Parameter, ParameterType};

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
    ssm: ssm::Service,
    openai: Option<openai::Service>,
    firebase: Option<firebase::Service>,
}

/// Construct an isolated in-memory emulator. Clones of this router share state.
pub fn router() -> Router {
    router_with_config(Config::default()).expect("default configuration is valid")
}

/// Construct an isolated emulator with explicit, validated deterministic faults.
pub fn router_with_faults(config: FaultConfig) -> Result<Router, FaultConfigError> {
    let faults = Faults::new(config)?;
    Ok(build(faults, Vec::new(), ServiceConfig::default())
        .expect("empty bucket configuration is valid"))
}

pub fn router_with_config(config: Config) -> Result<Router, ConfigError> {
    router_with_services(config, ServiceConfig::default())
}

pub fn router_with_services(
    config: Config,
    services: ServiceConfig,
) -> Result<Router, ConfigError> {
    let faults = Faults::new(config.faults).map_err(|e| ConfigError(e.to_string()))?;
    build(faults, config.buckets, services)
}
fn build(
    faults: Faults,
    buckets: Vec<String>,
    config: ServiceConfig,
) -> Result<Router, ConfigError> {
    let firebase = config
        .firebase
        .map(firebase::Service::new)
        .transpose()
        .map_err(ConfigError)?;
    let services = Services {
        firebase,
        dynamodb: dynamodb::Service::with_scheduler(faults.clone()),
        s3: s3::Service::new(buckets, faults).map_err(ConfigError)?,
        ssm: ssm::Service::new(config.ssm).map_err(ConfigError)?,
        openai: config
            .openai
            .map(openai::Service::new)
            .transpose()
            .map_err(ConfigError)?,
    };
    Ok(Router::new().fallback(dispatch).with_state(services))
}
async fn dispatch(State(services): State<Services>, request: Request) -> Response {
    let op = match routing::classify(
        &request,
        services.openai.is_some(),
        services.firebase.is_some(),
    ) {
        Route::Ssm(op) => return services.ssm.dispatch(op.as_deref(), request).await,
        Route::OpenAi => {
            return services
                .openai
                .as_ref()
                .expect("configured route")
                .dispatch(request)
                .await;
        }
        Route::Firebase => {
            return services
                .firebase
                .as_ref()
                .expect("configured route")
                .dispatch(request)
                .await;
        }
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
