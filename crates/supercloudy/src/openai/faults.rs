use super::{envelope, error};
use crate::faults::{Schedule, validate_ordinals};
use axum::{http::StatusCode, response::Response};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub format_name: Option<String>,
    pub first_request: u64,
    pub repeat: u64,
    pub effect: Effect,
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Throttle,
    InternalServerError,
    Incomplete,
}
#[derive(Clone)]
pub(crate) struct Faults(Schedule<Rule>);
impl Faults {
    pub(crate) fn new(rules: Vec<Rule>) -> Result<Self, String> {
        for (index, rule) in rules.iter().enumerate() {
            let invalid = |message| format!("OpenAI fault rule {}: {message}", index + 1);
            validate_ordinals(rule.first_request, rule.repeat).map_err(invalid)?;
            if rule
                .format_name
                .as_ref()
                .is_some_and(|name| name.trim().is_empty())
            {
                return Err(invalid("format_name must not be empty"));
            }
        }
        Ok(Self(Schedule::new(
            rules
                .into_iter()
                .map(|rule| (rule.first_request, rule.repeat, rule)),
        )))
    }
    pub(crate) fn response(&self, name: &str, model: &str) -> Option<Response> {
        self.0
            .select(|rule| rule.format_name.as_deref().is_none_or(|n| n == name))
            .map(|rule| match rule.effect {
                Effect::Throttle => error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_error",
                    "rate_limit_exceeded",
                    "Injected rate limit",
                ),
                Effect::InternalServerError => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "server_error",
                    "internal_server_error",
                    "Injected server error",
                ),
                Effect::Incomplete => envelope(model, None),
            })
    }
}
