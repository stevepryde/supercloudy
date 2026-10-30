//! Target ownership precedes explicit signing scopes and REST addressing.
use axum::{extract::Request, http::Method};

pub(crate) enum Route {
    DynamoDb(String),
    S3,
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

pub(crate) fn classify(request: &Request) -> Route {
    let headers = request.headers();
    if headers.contains_key("x-amz-target") {
        return if headers.get_all("x-amz-target").iter().count() == 1
            && request.method() == Method::POST
            && request.uri().path() == "/"
        {
            crate::dynamodb::operation(headers.get("x-amz-target").and_then(|v| v.to_str().ok()))
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
    if authorizations.len() > 1 {
        malformed = true;
    }
    for authorization in authorizations {
        let Ok(value) = authorization.to_str() else {
            malformed = true;
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
        return if service == "s3" {
            Route::S3
        } else {
            Route::Unknown
        };
    }
    if bucket_hint || request.uri().path() != "/" {
        Route::S3
    } else {
        Route::Unknown
    }
}
