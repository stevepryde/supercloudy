use super::error;
use crate::faults::{Schedule, validate_ordinals};
use axum::{http::StatusCode, response::Response};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub function_name: Option<String>,
    pub first_request: u64,
    pub repeat: u64,
    pub effect: Effect,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Throttle,
    ServiceError,
    FunctionError,
}

#[derive(Clone)]
pub(super) struct Faults(Schedule<Rule>);
impl Faults {
    pub(super) fn new(rules: Vec<Rule>) -> Result<Self, String> {
        for rule in &rules {
            validate_ordinals(rule.first_request, rule.repeat)?;
            if rule.function_name.as_ref().is_some_and(|n| n.is_empty()) {
                return Err("Lambda fault function_name must not be empty".into());
            }
        }
        Ok(Self(Schedule::new(
            rules.into_iter().map(|r| (r.first_request, r.repeat, r)),
        )))
    }

    pub(super) fn select(&self, name: &str) -> Option<Effect> {
        self.0
            .select(|r| r.function_name.as_deref().is_none_or(|n| n == name))
            .map(|r| r.effect)
    }
}

pub(super) fn invocation_failure(effect: Effect) -> Option<Response> {
    match effect {
        Effect::Throttle => Some(error(
            StatusCode::TOO_MANY_REQUESTS,
            "TooManyRequestsException",
            "Injected concurrency limit",
        )),
        Effect::ServiceError => Some(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServiceException",
            "Injected service failure",
        )),
        Effect::FunctionError => None,
    }
}
