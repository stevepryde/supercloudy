//! In-memory issuer for locally verifiable, synthetic Firebase ID tokens.
mod faults;

use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rsa::{RsaPrivateKey, pkcs1::EncodeRsaPrivateKey, pkcs8::EncodePrivateKey, rand_core::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::SystemTime};

pub(crate) const CERT_PATH: &str = "/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com";

/// Enables the issuer. Keys and certificates are generated anew for each instance.
#[derive(Clone, Debug)]
pub struct Config {
    pub project_id: Option<String>,
    pub cert_max_age: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            project_id: None,
            cert_max_age: 3600,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Service(Arc<Issuer>);
struct Issuer {
    config: Config,
    key: EncodingKey,
    kid: String,
    certificate: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mint {
    project_id: Option<String>,
    sub: String,
    email: String,
    #[serde(default = "verified")]
    email_verified: bool,
    #[serde(default)]
    extra_claims: Map<String, Value>,
    ttl: Option<u64>,
    exp: Option<u64>,
    iat: Option<u64>,
    variant: Option<faults::Variant>,
    kid: Option<String>,
}
fn verified() -> bool {
    true
}

#[derive(Serialize)]
struct Claims {
    iss: String,
    aud: String,
    auth_time: u64,
    iat: u64,
    exp: u64,
    sub: String,
    email: String,
    email_verified: bool,
    firebase: Value,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn project_valid(project: &str) -> bool {
    !project.is_empty()
        && project.len() <= 128
        && project
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || b"-_".contains(&v))
}
fn invalid(message: &'static str) -> Response {
    error(StatusCode::BAD_REQUEST, "invalid_request", message)
}
fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}

impl Service {
    pub(crate) fn new(config: Config) -> Result<Self, String> {
        if config
            .project_id
            .as_deref()
            .is_some_and(|v| !project_valid(v))
        {
            return Err("Invalid Firebase default project_id".into());
        }
        // rcgen's ring backend imports RSA keys; RSA generation uses the OS RNG.
        // DER remains in memory; no key/certificate files or debug output exist.
        let material = (|| -> Result<_, Box<dyn std::error::Error>> {
            let private = RsaPrivateKey::new(&mut OsRng, 2048)?;
            let der = private.to_pkcs8_der()?;
            let pair = KeyPair::try_from(der.as_bytes())?;
            let mut params = CertificateParams::default();
            params.distinguished_name = DistinguishedName::new();
            params
                .distinguished_name
                .push(DnType::CommonName, "Synthetic Firebase issuer");
            let cert = params.self_signed(&pair)?;
            let kid = format!("{:x}", Sha256::digest(cert.der()));
            // The JWT Rust backend consumes PKCS#1, while rcgen imports PKCS#8.
            let signing_der = private.to_pkcs1_der()?;
            let key = EncodingKey::from_rsa_der(signing_der.as_bytes());
            Ok((key, kid, cert.pem()))
        })()
        .map_err(|_| "Unable to generate Firebase signing material".to_owned())?;
        Ok(Self(Arc::new(Issuer {
            config,
            key: material.0,
            kid: material.1,
            certificate: material.2,
        })))
    }

    pub(crate) async fn dispatch(&self, request: Request) -> Response {
        if request.uri().path() == CERT_PATH {
            return (
                [(
                    header::CACHE_CONTROL,
                    format!("public, max-age={}", self.0.config.cert_max_age),
                )],
                Json(json!({self.0.kid.clone(): self.0.certificate})),
            )
                .into_response();
        }
        let body = match to_bytes(request.into_body(), 64 * 1024).await {
            Ok(body) => body,
            Err(_) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "Mint body exceeds 64 KiB or cannot be read",
                );
            }
        };
        let input: Mint = match serde_json::from_slice(&body) {
            Ok(input) => input,
            Err(_) => return invalid("Invalid mint JSON or field types"),
        };
        self.mint(input)
    }

    fn mint(&self, input: Mint) -> Response {
        let project = input
            .project_id
            .as_ref()
            .or(self.0.config.project_id.as_ref());
        let Some(project) = project.filter(|v| project_valid(v)) else {
            return invalid("A valid project_id is required in the request or configuration");
        };
        if input.sub.is_empty() || input.sub.chars().count() > 128 {
            return invalid("sub must contain 1 to 128 characters");
        }
        if input.email.trim().is_empty() {
            return invalid("email must be nonempty");
        }
        if input.ttl == Some(0) || (input.ttl.is_some() && input.exp.is_some()) {
            return invalid("ttl must be positive and cannot be combined with exp");
        }
        if input.extra_claims.keys().any(|v| {
            matches!(
                v.as_str(),
                "iss"
                    | "aud"
                    | "auth_time"
                    | "iat"
                    | "exp"
                    | "sub"
                    | "email"
                    | "email_verified"
                    | "firebase"
            )
        }) {
            return invalid("extra_claims cannot replace issuer or identity claims");
        }
        if let Err(message) = faults::validate(
            input.variant,
            input.kid.as_deref(),
            input.ttl.is_some() || input.iat.is_some() || input.exp.is_some(),
        ) {
            return invalid(message);
        }
        let Ok(now) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) else {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "clock_error",
                "Clock precedes Unix epoch",
            );
        };
        let now = now.as_secs();
        let iat = input.iat.unwrap_or(now);
        let exp = match input
            .exp
            .or_else(|| iat.checked_add(input.ttl.unwrap_or(3600)))
        {
            Some(exp) => exp,
            None => return invalid("Timestamp overflow"),
        };
        let mut claims = Claims {
            iss: format!("https://securetoken.google.com/{project}"),
            aud: project.clone(),
            auth_time: iat,
            iat,
            exp,
            sub: input.sub,
            firebase: json!({"identities":{"email":[input.email]}, "sign_in_provider":"password"}),
            email: input.email,
            email_verified: input.email_verified,
            extra: input.extra_claims,
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(input.kid.unwrap_or_else(|| self.0.kid.clone()));
        if let Some(variant) = input.variant {
            variant.apply(&mut claims, &mut header, now);
        }
        match encode(&header, &claims, &self.0.key) {
            Ok(token) => (
                [(header::CACHE_CONTROL, "no-store")],
                Json(json!({
                    "id_token": token, "project_id": project, "kid": header.kid,
                    "iat": claims.iat, "exp": claims.exp,
                })),
            )
                .into_response(),
            Err(_) => error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "signing_error",
                "Unable to sign token",
            ),
        }
    }
}
