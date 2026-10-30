use super::{
    api::Request,
    error::{Error, Result},
    operations::{self, PreparedWrite},
    storage::{Database, KeyTuple},
    value::{item_size_bytes, parse_item, wire_item},
};
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

pub(crate) struct TokenRecord {
    pub created: Instant,
    pub request: Json,
    pub response: Json,
}

pub(crate) struct WriteAction {
    pub request: Request,
    pub operation: &'static str,
    pub check_only: bool,
    pub return_old: bool,
}
pub(crate) struct WriteRequest {
    pub actions: Vec<WriteAction>,
    pub token: Option<String>,
    pub normalized: Json,
}
pub(crate) struct WritePlan {
    pub changes: Vec<(String, PreparedWrite, bool)>,
    pub replay: Option<Json>,
}

pub(crate) fn object<'a>(
    v: &'a Json,
    allowed: &[&str],
) -> Result<&'a serde_json::Map<String, Json>> {
    let o = v
        .as_object()
        .ok_or_else(|| Error::validation("Expected object"))?;
    if let Some(k) = o.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(Error::validation(format!(
            "Unsupported request parameter: {k}"
        )));
    }
    Ok(o)
}
pub(crate) fn array<'a>(v: &'a Json, field: &str, max: usize) -> Result<&'a Vec<Json>> {
    let a = v
        .get(field)
        .and_then(Json::as_array)
        .ok_or_else(|| Error::validation(format!("Missing or invalid {field}")))?;
    if a.is_empty() || a.len() > max {
        return Err(Error::validation(format!(
            "{field} must contain 1 to {max} entries"
        )));
    }
    Ok(a)
}
pub(crate) fn single(op: &str, value: &Json) -> Result<Request> {
    operations::decode(op, &serde_json::to_vec(value).expect("JSON serialization"))
}
fn normalize(v: &Json) -> Json {
    if let Ok(item) = parse_item(&json!({"value":v})) {
        return item["value"].wire();
    }
    match v {
        Json::Object(m) => Json::Object(m.iter().map(|(k, v)| (k.clone(), normalize(v))).collect()),
        Json::Array(a) => Json::Array(a.iter().map(normalize).collect()),
        _ => v.clone(),
    }
}
pub(crate) fn decode_write(raw: &Json) -> Result<WriteRequest> {
    object(raw, &["TransactItems", "ClientRequestToken"])?;
    let token = match raw.get("ClientRequestToken") {
        Some(Json::String(s)) if (1..=36).contains(&s.len()) => Some(s.clone()),
        None => None,
        _ => return Err(Error::validation("Invalid ClientRequestToken")),
    };
    let mut actions = Vec::new();
    for action in array(raw, "TransactItems", 100)? {
        let a = object(action, &["Put", "Delete", "Update", "ConditionCheck"])?;
        if a.len() != 1 {
            return Err(Error::validation(
                "Each transaction action must contain one operation",
            ));
        }
        let (kind, value) = a.iter().next().unwrap();
        let fields = match kind.as_str() {
            "Put" => vec![
                "TableName",
                "Item",
                "ConditionExpression",
                "ExpressionAttributeNames",
                "ExpressionAttributeValues",
                "ReturnValuesOnConditionCheckFailure",
            ],
            "Update" => vec![
                "TableName",
                "Key",
                "UpdateExpression",
                "ConditionExpression",
                "ExpressionAttributeNames",
                "ExpressionAttributeValues",
                "ReturnValuesOnConditionCheckFailure",
            ],
            _ => vec![
                "TableName",
                "Key",
                "ConditionExpression",
                "ExpressionAttributeNames",
                "ExpressionAttributeValues",
                "ReturnValuesOnConditionCheckFailure",
            ],
        };
        object(value, &fields)?;
        let return_old = match value
            .get("ReturnValuesOnConditionCheckFailure")
            .and_then(Json::as_str)
        {
            None if !value
                .as_object()
                .unwrap()
                .contains_key("ReturnValuesOnConditionCheckFailure") =>
            {
                false
            }
            Some("NONE") => false,
            Some("ALL_OLD") => true,
            _ => {
                return Err(Error::validation(
                    "Invalid ReturnValuesOnConditionCheckFailure",
                ));
            }
        };
        let mut input = value.clone();
        input
            .as_object_mut()
            .unwrap()
            .remove("ReturnValuesOnConditionCheckFailure");
        let op = match kind.as_str() {
            "Put" => "PutItem",
            "Update" => "UpdateItem",
            _ => "DeleteItem",
        };
        let request = single(op, &input)?;
        if kind == "ConditionCheck" && request.condition_expression.is_none() {
            return Err(Error::validation(
                "ConditionCheck requires ConditionExpression",
            ));
        }
        actions.push(WriteAction {
            request,
            operation: op,
            check_only: kind == "ConditionCheck",
            return_old,
        });
    }
    Ok(WriteRequest {
        actions,
        token,
        normalized: normalize(raw),
    })
}
fn target(db: &Database, action: &WriteAction) -> Result<KeyTuple> {
    let r = &action.request;
    let table = db.tables.get(&r.table_name).ok_or_else(|| {
        Error::new(
            "ResourceNotFoundException",
            "Requested table does not exist",
        )
    })?;
    let input = if action.operation == "PutItem" {
        &r.item
    } else {
        &r.key
    };
    let input = input
        .as_ref()
        .ok_or_else(|| Error::validation("Missing item or key"))?;
    table.key(&parse_item(input)?, action.operation != "PutItem")
}
pub(crate) fn prepare_write(db: &Database, r: &WriteRequest) -> Result<WritePlan> {
    if let Some(token) = &r.token
        && let Some(record) = db.tokens.get(token)
        && record.created.elapsed() < Duration::from_secs(600)
    {
        if record.request != r.normalized {
            return Err(Error::new(
                "IdempotentParameterMismatchException",
                "ClientRequestToken was used with different parameters",
            ));
        }
        return Ok(WritePlan {
            changes: vec![],
            replay: Some(record.response.clone()),
        });
    }
    let mut targets = BTreeSet::new();
    let mut keys = Vec::new();
    for action in &r.actions {
        let key = target(db, action)?;
        if !targets.insert((action.request.table_name.clone(), key.clone())) {
            return Err(Error::validation(
                "Transaction contains duplicate item targets",
            ));
        }
        keys.push(key);
    }
    let mut changes = Vec::new();
    let mut reasons = Vec::new();
    let mut failed = false;
    let mut size = 0;
    for (action, key) in r.actions.iter().zip(keys) {
        let table = &db.tables[&action.request.table_name];
        match operations::prepare_write(table, action.operation, &action.request) {
            Ok(prepared) => {
                size += prepared
                    .candidate
                    .as_ref()
                    .or_else(|| table.items.get(&key))
                    .map(item_size_bytes)
                    .unwrap_or(0);
                changes.push((
                    action.request.table_name.clone(),
                    prepared,
                    action.check_only,
                ));
                reasons.push(json!({"Code":"None","Message":Json::Null}));
            }
            Err(e) => {
                failed = true;
                let code = if e.code == "ConditionalCheckFailedException" {
                    "ConditionalCheckFailed"
                } else {
                    "ValidationError"
                };
                let mut reason = json!({"Code":code,"Message":e.message});
                if action.return_old
                    && let Some(old) = table.items.get(&key)
                {
                    reason["Item"] = wire_item(old);
                }
                reasons.push(reason);
            }
        }
    }
    if size > 4 * 1024 * 1024 {
        return Err(Error::validation("Transaction exceeds 4 MiB"));
    }
    if failed {
        return Err(Error::transaction_canceled(reasons));
    }
    Ok(WritePlan {
        changes,
        replay: None,
    })
}
pub(crate) fn commit(db: &mut Database, r: &WriteRequest, plan: WritePlan) -> Json {
    if let Some(response) = plan.replay {
        return response;
    }
    for (name, prepared, check_only) in plan.changes {
        if check_only {
            continue;
        }
        let table = db.tables.get_mut(&name).unwrap();
        if let Some(item) = prepared.candidate {
            table.items.insert(prepared.key, item);
        } else {
            table.items.remove(&prepared.key);
        }
    }
    let response = json!({});
    db.tokens
        .retain(|_, v| v.created.elapsed() < Duration::from_secs(600));
    if let Some(token) = &r.token {
        db.tokens.insert(
            token.clone(),
            TokenRecord {
                created: Instant::now(),
                request: r.normalized.clone(),
                response: response.clone(),
            },
        );
    }
    response
}
pub(crate) fn decode_get(raw: &Json) -> Result<Vec<Request>> {
    object(raw, &["TransactItems"])?;
    array(raw, "TransactItems", 100)?
        .iter()
        .map(|action| {
            let o = object(action, &["Get"])?;
            let get = o
                .get("Get")
                .ok_or_else(|| Error::validation("Expected Get"))?;
            object(
                get,
                &[
                    "TableName",
                    "Key",
                    "ProjectionExpression",
                    "ExpressionAttributeNames",
                ],
            )?;
            single("GetItem", get)
        })
        .collect()
}
pub(crate) fn get(db: &Database, requests: &[Request]) -> Result<Json> {
    let mut responses = Vec::new();
    let mut size = 0;
    for r in requests {
        let table = db.tables.get(&r.table_name).ok_or_else(|| {
            Error::new(
                "ResourceNotFoundException",
                "Requested table does not exist",
            )
        })?;
        let key = table.key(
            &parse_item(
                r.key
                    .as_ref()
                    .ok_or_else(|| Error::validation("Missing Key"))?,
            )?,
            true,
        )?;
        size += table.items.get(&key).map(item_size_bytes).unwrap_or(0);
        if size > 4 * 1024 * 1024 {
            return Err(Error::validation("Transaction read exceeds 4 MiB"));
        }
        responses.push(operations::read(db, "GetItem", r)?);
    }
    Ok(json!({"Responses":responses}))
}
