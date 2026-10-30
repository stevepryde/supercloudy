use super::error::Error;
use serde::Deserialize;
use serde_json::{Value, json};
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

/// Ordinals start at one and count valid requests matching the operation and table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultRule {
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
    fn as_str(self) -> &'static str {
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
pub(crate) enum Effect {
    Error(Error),
    BatchDeferral(BTreeMap<String, BTreeSet<usize>>),
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

    /// Call once after complete request validation and before publishing mutations.
    pub fn effect(&self, operation: &str, request: &Value) -> Option<Effect> {
        let mut rules = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut selected = None;
        let action_count = request["TransactItems"].as_array().map_or(0, Vec::len);
        for state in rules.iter_mut() {
            if state.rule.operation != operation
                || matches!(&state.rule.effect, FaultEffect::TransactionCanceled { reasons }
                    if reasons.iter().take(action_count).all(|code| *code == CancellationCode::None))
                || state
                    .rule
                    .table
                    .as_ref()
                    .is_some_and(|table| !has_table(request, table))
            {
                continue;
            }
            state.requests = state.requests.saturating_add(1);
            if selected.is_none()
                && state.requests >= state.rule.first_request
                && state.requests - state.rule.first_request < state.rule.repeat
            {
                selected = Some(&state.rule.effect);
            }
        }
        selected.map(|effect| match effect {
            FaultEffect::Throttle => Effect::Error(Error::new(
                "ProvisionedThroughputExceededException",
                "Injected throughput failure",
            )),
            FaultEffect::InternalServerError => Effect::Error(
                Error::new("InternalServerError", "Injected internal service failure")
                    .with_status(500),
            ),
            FaultEffect::TransactionCanceled { reasons } => {
                let count = request["TransactItems"].as_array().map_or(0, Vec::len);
                let reasons = (0..count)
                    .map(|index| {
                        let code = reasons
                            .get(index)
                            .copied()
                            .unwrap_or(CancellationCode::None);
                        json!({"Code": code.as_str(), "Message": null})
                    })
                    .collect::<Vec<_>>();
                Effect::Error(Error::transaction_canceled(reasons))
            }
            FaultEffect::BatchDeferral { positions } => Effect::BatchDeferral(positions.clone()),
        })
    }
}

fn has_table(request: &Value, table: &str) -> bool {
    request["TableName"].as_str() == Some(table)
        || request["RequestItems"]
            .as_object()
            .is_some_and(|items| items.contains_key(table))
        || request["TransactItems"].as_array().is_some_and(|actions| {
            actions.iter().any(|action| {
                action.as_object().is_some_and(|action| {
                    action
                        .values()
                        .any(|body| body["TableName"].as_str() == Some(table))
                })
            })
        })
}
