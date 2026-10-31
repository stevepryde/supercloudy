//! Target ownership precedes explicit signing scopes and REST addressing.
use axum::{extract::Request, http::Method};

pub(crate) enum Route {
    DynamoDb(String),
    S3,
    Ssm(Option<String>),
    OpenAi,
    Firebase,
    Lambda,
    Unknown,
    InvalidS3,
}

fn scope(value: &str) -> Option<&str> {
    let parts: Vec<_> = value.split('/').collect();
    (parts.len() == 5
        && !parts[0].is_empty()
        && parts[1].len() == 8
        && parts[1].bytes().all(|v| v.is_ascii_digit())
        && !parts[2].is_empty()
        && !parts[3].is_empty()
        && parts[4] == "aws4_request")
        .then(|| parts[3])
}

pub(crate) fn classify(
    request: &Request,
    openai_enabled: bool,
    firebase_enabled: bool,
    lambda_enabled: bool,
) -> Route {
    let headers = request.headers();
    if headers.contains_key("x-amz-target") {
        if headers.get_all("x-amz-target").iter().count() != 1 {
            return Route::Unknown;
        }
        let Some(target) = headers.get("x-amz-target").and_then(|v| v.to_str().ok()) else {
            return Route::Unknown;
        };
        let valid_endpoint = request.method() == Method::POST && request.uri().path() == "/";
        if let Some(operation) = target.strip_prefix("AmazonSSM.") {
            return Route::Ssm(valid_endpoint.then(|| operation.to_owned()));
        }
        return if valid_endpoint {
            crate::dynamodb::operation(Some(target))
                .map_or(Route::Unknown, |op| Route::DynamoDb(op.into()))
        } else {
            Route::Unknown
        };
    }
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let authority = request
        .uri()
        .authority()
        .map(|a| a.as_str())
        .unwrap_or(host);
    let bucket_hint = authority
        .parse::<axum::http::uri::Authority>()
        .ok()
        .is_some_and(|a| a.host().to_ascii_lowercase().ends_with(".localhost"));
    let mut services = Vec::new();
    let mut scopes = Vec::new();
    let mut malformed = false;
    let mut s3_hint = bucket_hint;
    let authorizations: Vec<_> = headers.get_all("authorization").iter().collect();
    let aws_authorization = authorizations
        .iter()
        .any(|value| value.as_bytes().starts_with(b"AWS4-HMAC-SHA256"));
    if aws_authorization && authorizations.len() > 1 {
        malformed = true;
    }
    for authorization in authorizations {
        let Ok(value) = authorization.to_str() else {
            malformed |= aws_authorization;
            continue;
        };
        if value.starts_with("AWS4-HMAC-SHA256") && !value.starts_with("AWS4-HMAC-SHA256 ") {
            malformed = true;
        }
        if let Some(fields) = value.strip_prefix("AWS4-HMAC-SHA256 ") {
            let credentials: Vec<_> = fields
                .split(',')
                .filter_map(|field| field.trim().strip_prefix("Credential="))
                .collect();
            if credentials.len() != 1 {
                malformed = true;
            }
            for credential in credentials {
                s3_hint |= credential.split('/').nth(3) == Some("s3");
                match scope(credential) {
                    Some(service) => {
                        services.push(service.to_owned());
                        scopes.push(credential.split_once('/').unwrap().1.to_owned());
                    }
                    None => malformed = true,
                }
            }
        }
    }
    let mut query_credential_count = 0;
    for pair in request.uri().query().unwrap_or("").split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |v: &str| {
            percent_encoding::percent_decode_str(v)
                .decode_utf8()
                .map(|v| v.into_owned())
        };
        if decode(key)
            .ok()
            .is_some_and(|key| key.eq_ignore_ascii_case("X-Amz-Credential"))
        {
            query_credential_count += 1;
            match decode(value) {
                Ok(credential) => {
                    s3_hint |= credential.split('/').nth(3) == Some("s3");
                    match scope(&credential) {
                        Some(service) => {
                            services.push(service.to_owned());
                            scopes.push(credential.split_once('/').unwrap().1.to_owned());
                        }
                        None => malformed = true,
                    }
                }
                Err(_) => malformed = true,
            }
        }
    }
    malformed |= query_credential_count > 1 || scopes.windows(2).any(|v| v[0] != v[1]);
    if malformed {
        return if s3_hint {
            Route::InvalidS3
        } else {
            Route::Unknown
        };
    }
    if let Some(service) = services.first() {
        return match service.as_str() {
            "s3" => Route::S3,
            "ssm" => Route::Ssm(None),
            "lambda" => Route::Lambda,
            // DynamoDB without its required target retains its own unknown-operation codec.
            _ => Route::Unknown,
        };
    }
    if bucket_hint {
        return Route::S3;
    }
    if lambda_enabled && crate::lambda::reserved(request.uri().path()) {
        return Route::Lambda;
    }
    if openai_enabled && request.method() == Method::POST && request.uri().path() == "/v1/responses"
    {
        return Route::OpenAi;
    }
    if firebase_enabled
        && ((request.method() == Method::GET && request.uri().path() == crate::firebase::CERT_PATH)
            || (request.method() == Method::POST && request.uri().path() == "/mint"))
    {
        return Route::Firebase;
    }
    if request.uri().path() != "/" {
        Route::S3
    } else {
        Route::Unknown
    }
}
