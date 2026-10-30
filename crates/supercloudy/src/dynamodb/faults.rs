use super::error::Error;
pub(crate) use crate::faults::Faults;
pub use crate::faults::{CancellationCode, FaultConfig, FaultConfigError, FaultEffect, FaultRule};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

impl CancellationCode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::ConditionalCheckFailed => "ConditionalCheckFailed",
            Self::TransactionConflict => "TransactionConflict",
        }
    }
}

pub(crate) enum Effect {
    Error(Error),
    BatchDeferral(BTreeMap<String, BTreeSet<usize>>),
}
impl Faults {
    /// Call once after complete request validation and before publishing mutations.
    pub fn effect(&self, operation: &str, request: &Value) -> Option<Effect> {
        let action_count = request["TransactItems"].as_array().map_or(0, Vec::len);
        let selected = self.select("dynamodb", operation, |rule| {
            !matches!(&rule.effect, FaultEffect::TransactionCanceled { reasons }
                if reasons.iter().take(action_count).all(|code| *code == CancellationCode::None))
                && rule
                    .table
                    .as_ref()
                    .is_none_or(|table| has_table(request, table))
        });
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
            FaultEffect::BatchDeferral { positions } => Effect::BatchDeferral(positions),
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

pub(crate) fn validate_rule(rule: &FaultRule) -> Result<(), &'static str> {
    if rule.bucket.is_some() || rule.key.is_some() {
        return Err("DynamoDB rules cannot select bucket or key");
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
        return Err("unsupported operation");
    }
    if rule.table.as_ref().is_some_and(|table| table.is_empty()) {
        return Err("table must not be empty");
    }
    match &rule.effect {
        FaultEffect::TransactionCanceled { reasons } => {
            if rule.operation != "TransactWriteItems" {
                return Err("transaction cancellation requires TransactWriteItems");
            }
            if reasons.is_empty()
                || reasons.len() > 100
                || reasons.iter().all(|code| *code == CancellationCode::None)
            {
                return Err("reasons must contain 1..100 entries and a failure");
            }
        }
        FaultEffect::BatchDeferral { positions } => {
            let max = match rule.operation.as_str() {
                "BatchGetItem" => 100,
                "BatchWriteItem" => 25,
                _ => return Err("batch deferral requires a batch operation"),
            };
            if positions.is_empty()
                || positions.iter().any(|(table, positions)| {
                    table.is_empty()
                        || positions.is_empty()
                        || positions.iter().any(|position| *position >= max)
                })
            {
                return Err("batch positions must name tables and valid positions");
            }
        }
        FaultEffect::Throttle | FaultEffect::InternalServerError => {}
    }
    Ok(())
}
