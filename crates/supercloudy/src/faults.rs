use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
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

/// Ordinals start at one and count valid requests matching the operation and table.
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
impl CancellationCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::ConditionalCheckFailed => "ConditionalCheckFailed",
            Self::TransactionConflict => "TransactionConflict",
        }
    }
}

#[derive(Debug)]
pub struct FaultConfigError(String);
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
                "dynamodb" => {
                    if rule.bucket.is_some() || rule.key.is_some() {
                        return Err(invalid("DynamoDB rules cannot select bucket or key"));
                    }
                    if !matches!(
                        rule.operation.as_str(),
                        "CreateTable"
                            | "DescribeTable"
                            | "DeleteTable"
                            | "GetItem"
                            | "PutItem"
                            | "UpdateItem"
                            | "DeleteItem"
                            | "Query"
                            | "Scan"
                            | "TransactWriteItems"
                            | "TransactGetItems"
                            | "BatchGetItem"
                            | "BatchWriteItem"
                    ) {
                        return Err(invalid("unsupported operation"));
                    }
                }
                "s3" => {
                    if rule.table.is_some()
                        || !matches!(
                            rule.effect,
                            FaultEffect::Throttle | FaultEffect::InternalServerError
                        )
                    {
                        return Err(invalid(
                            "S3 rules support bucket/key selectors and throttle/internal errors only",
                        ));
                    }
                    if !matches!(
                        rule.operation.as_str(),
                        "CreateBucket"
                            | "PutObject"
                            | "GetObject"
                            | "HeadObject"
                            | "CopyObject"
                            | "DeleteObject"
                            | "ListObjectsV2"
                    ) {
                        return Err(invalid("unsupported S3 operation"));
                    }
                    if rule.bucket.as_ref().is_some_and(|v| v.is_empty())
                        || rule.key.as_ref().is_some_and(|v| v.is_empty())
                    {
                        return Err(invalid("bucket and key selectors must not be empty"));
                    }
                }
                _ => return Err(invalid("unsupported service")),
            }
            if rule.table.as_ref().is_some_and(|table| table.is_empty()) {
                return Err(invalid("table must not be empty"));
            }
            if rule.first_request == 0 || rule.repeat == 0 {
                return Err(invalid("first_request and repeat must be positive"));
            }
            if rule.first_request.checked_add(rule.repeat - 1).is_none() {
                return Err(invalid("request ordinal range overflows"));
            }
            match &rule.effect {
                FaultEffect::TransactionCanceled { reasons } => {
                    if rule.operation != "TransactWriteItems" {
                        return Err(invalid(
                            "transaction cancellation requires TransactWriteItems",
                        ));
                    }
                    if reasons.is_empty()
                        || reasons.len() > 100
                        || reasons.iter().all(|code| *code == CancellationCode::None)
                    {
                        return Err(invalid("reasons must contain 1..100 entries and a failure"));
                    }
                }
                FaultEffect::BatchDeferral { positions } => {
                    let max = match rule.operation.as_str() {
                        "BatchGetItem" => 100,
                        "BatchWriteItem" => 25,
                        _ => return Err(invalid("batch deferral requires a batch operation")),
                    };
                    if positions.is_empty()
                        || positions.iter().any(|(table, positions)| {
                            table.is_empty()
                                || positions.is_empty()
                                || positions.iter().any(|position| *position >= max)
                        })
                    {
                        return Err(invalid(
                            "batch positions must name tables and valid positions",
                        ));
                    }
                }
                FaultEffect::Throttle | FaultEffect::InternalServerError => {}
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct Faults(Arc<Mutex<Vec<RuleState>>>);
struct RuleState {
    rule: FaultRule,
    requests: u64,
}
impl Faults {
    pub fn new(config: FaultConfig) -> std::result::Result<Self, FaultConfigError> {
        config.validate()?;
        Ok(Self(Arc::new(Mutex::new(
            config
                .rules
                .into_iter()
                .map(|rule| RuleState { rule, requests: 0 })
                .collect(),
        ))))
    }

    pub(crate) fn select(
        &self,
        service: &str,
        operation: &str,
        eligible: impl Fn(&FaultRule) -> bool,
    ) -> Option<FaultEffect> {
        let mut rules = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut selected = None;
        for state in rules.iter_mut() {
            if state.rule.service != service
                || state.rule.operation != operation
                || !eligible(&state.rule)
            {
                continue;
            }
            state.requests = state.requests.saturating_add(1);
            if selected.is_none()
                && state.requests >= state.rule.first_request
                && state.requests - state.rule.first_request < state.rule.repeat
            {
                selected = Some(state.rule.effect.clone());
            }
        }
        selected
    }
    pub(crate) fn s3_effect(
        &self,
        operation: &str,
        bucket: &str,
        key: Option<&str>,
    ) -> Option<&'static str> {
        self.select("s3", operation, |rule| {
            rule.bucket.as_deref().is_none_or(|b| b == bucket)
                && rule.key.as_deref().is_none_or(|k| Some(k) == key)
        })
        .map(|effect| match effect {
            FaultEffect::Throttle => "SlowDown",
            _ => "InternalError",
        })
    }
}
