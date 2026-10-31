use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Explicit, per-instance deterministic fault rules. No rules are enabled by default.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultConfig {
    pub rules: Vec<FaultRule>,
}

fn default_service() -> String {
    "dynamodb".into()
}

/// Ordinals start at one and count validated requests matching service selectors.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultRule {
    #[serde(default = "default_service")]
    pub service: String,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub operation: String,
    pub table: Option<String>,
    pub first_request: u64,
    pub repeat: u64,
    pub effect: FaultEffect,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FaultEffect {
    Throttle,
    InternalServerError,
    /// Reasons are in action order; omitted trailing actions receive `None`.
    TransactionCanceled {
        reasons: Vec<CancellationCode>,
    },
    /// Zero-based positions in each table's original key or write-request list.
    BatchDeferral {
        positions: BTreeMap<String, BTreeSet<usize>>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum CancellationCode {
    None,
    ConditionalCheckFailed,
    TransactionConflict,
}
#[derive(Debug)]
pub struct FaultConfigError(pub(crate) String);
impl fmt::Display for FaultConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for FaultConfigError {}

impl FaultConfig {
    pub fn validate(&self) -> std::result::Result<(), FaultConfigError> {
        for (index, rule) in self.rules.iter().enumerate() {
            let invalid =
                |message: &str| FaultConfigError(format!("fault rule {}: {message}", index + 1));
            match rule.service.as_str() {
                "dynamodb" => crate::dynamodb::faults::validate_rule(rule),
                "s3" => crate::s3::faults::validate_rule(rule),
                _ => Err("unsupported service"),
            }
            .map_err(invalid)?;
            crate::faults::validate_ordinals(rule.first_request, rule.repeat).map_err(invalid)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct Faults(crate::faults::Schedule<FaultRule>);
impl Faults {
    pub fn new(config: FaultConfig) -> Result<Self, FaultConfigError> {
        config.validate()?;
        Ok(Self(crate::faults::Schedule::new(
            config
                .rules
                .into_iter()
                .map(|rule| (rule.first_request, rule.repeat, rule)),
        )))
    }
    pub(crate) fn select(
        &self,
        service: &str,
        operation: &str,
        eligible: impl Fn(&FaultRule) -> bool,
    ) -> Option<FaultEffect> {
        self.0
            .select(|rule| rule.service == service && rule.operation == operation && eligible(rule))
            .map(|rule| rule.effect)
    }
}
