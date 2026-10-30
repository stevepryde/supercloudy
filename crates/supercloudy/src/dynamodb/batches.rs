use super::{
    api::Request,
    error::{Error, Result},
    operations::{self, PreparedWrite},
    storage::Database,
    transactions::{array, object, single},
    value::{item_size_bytes, parse_item},
};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type Deferred = BTreeMap<String, BTreeSet<usize>>;
pub(crate) struct WriteRequest {
    pub tables: BTreeMap<String, Vec<(Request, &'static str, Json)>>,
}
pub(crate) struct WritePlan {
    changes: BTreeMap<String, Vec<PreparedWrite>>,
}
pub(crate) struct GetRequest {
    tables: BTreeMap<String, GetTable>,
}
struct GetTable {
    requests: Vec<Request>,
    original: Json,
}
pub(crate) fn decode_write(raw: &Json, wire_size: usize) -> Result<WriteRequest> {
    object(raw, &["RequestItems"])?;
    if wire_size > 16 * 1024 * 1024 {
        return Err(Error::validation("Batch write request exceeds 16 MiB"));
    }
    let tables = raw
        .get("RequestItems")
        .and_then(Json::as_object)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| Error::validation("Missing or empty RequestItems"))?;
    let mut requests = BTreeMap::new();
    let mut count = 0;
    for (name, actions) in tables {
        let actions = actions
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or_else(|| Error::validation("Empty or invalid write requests"))?;
        count += actions.len();
        let mut writes = Vec::new();
        for action in actions {
            let a = object(action, &["PutRequest", "DeleteRequest"])?;
            if a.len() != 1 {
                return Err(Error::validation("Expected one batch write operation"));
            }
            let (kind, input) = a.iter().next().unwrap();
            let op = if kind == "PutRequest" {
                "PutItem"
            } else {
                "DeleteItem"
            };
            object(input, &[if op == "PutItem" { "Item" } else { "Key" }])?;
            let mut input = input.clone();
            input["TableName"] = json!(name);
            writes.push((single(op, &input)?, op, action.clone()));
        }
        requests.insert(name.clone(), writes);
    }
    if count > 25 {
        return Err(Error::validation(
            "BatchWriteItem permits at most 25 actions",
        ));
    }
    Ok(WriteRequest { tables: requests })
}
pub(crate) fn prepare_write(db: &Database, request: &WriteRequest) -> Result<WritePlan> {
    let mut changes = BTreeMap::new();
    for (name, requests) in &request.tables {
        let table = db.tables.get(name).ok_or_else(|| {
            Error::new(
                "ResourceNotFoundException",
                "Requested table does not exist",
            )
        })?;
        let mut keys = BTreeSet::new();
        let mut writes = Vec::new();
        for (request, op, _) in requests {
            let prepared = operations::prepare_write(table, op, request)?;
            if !keys.insert(prepared.key.clone()) {
                return Err(Error::validation("Duplicate batch target"));
            }
            writes.push(prepared);
        }
        changes.insert(name.clone(), writes);
    }
    Ok(WritePlan { changes })
}
pub(crate) fn commit(
    db: &mut Database,
    request: &WriteRequest,
    plan: WritePlan,
    deferred: &Deferred,
) -> Json {
    let mut unprocessed = serde_json::Map::new();
    for (name, writes) in plan.changes {
        let table = db.tables.get_mut(&name).unwrap();
        let mut pending = Vec::new();
        for (position, prepared) in writes.into_iter().enumerate() {
            if deferred.get(&name).is_some_and(|p| p.contains(&position)) {
                pending.push(request.tables[&name][position].2.clone());
                continue;
            }
            if let Some(item) = prepared.candidate {
                table.items.insert(prepared.key, item);
            } else {
                table.items.remove(&prepared.key);
            }
        }
        if !pending.is_empty() {
            unprocessed.insert(name, Json::Array(pending));
        }
    }
    json!({"UnprocessedItems":unprocessed})
}
pub(crate) fn decode_get(raw: &Json) -> Result<GetRequest> {
    object(raw, &["RequestItems"])?;
    let tables = raw
        .get("RequestItems")
        .and_then(Json::as_object)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| Error::validation("Missing or empty RequestItems"))?;
    let mut decoded = BTreeMap::new();
    let mut count = 0;
    for (name, input) in tables {
        object(
            input,
            &[
                "Keys",
                "ConsistentRead",
                "ProjectionExpression",
                "ExpressionAttributeNames",
            ],
        )?;
        let keys = array(input, "Keys", 100)?;
        count += keys.len();
        let mut requests = Vec::new();
        for key in keys {
            let mut get = input.clone();
            get.as_object_mut().unwrap().remove("Keys");
            get["Key"] = key.clone();
            get["TableName"] = json!(name);
            requests.push(single("GetItem", &get)?);
        }
        decoded.insert(
            name.clone(),
            GetTable {
                requests,
                original: input.clone(),
            },
        );
    }
    if count > 100 {
        return Err(Error::validation("BatchGetItem permits at most 100 keys"));
    }
    Ok(GetRequest { tables: decoded })
}
// Validation precedes fault selection and processing, including requests that will
// later be deferred. Responses are prepared while the same read guard is held.
pub(crate) fn get(db: &Database, request: &GetRequest, deferred: &Deferred) -> Result<Json> {
    let mut prepared = BTreeMap::new();
    for (name, input) in &request.tables {
        let table = db.tables.get(name).ok_or_else(|| {
            Error::new(
                "ResourceNotFoundException",
                "Requested table does not exist",
            )
        })?;
        let mut keys = BTreeSet::new();
        let mut responses = Vec::new();
        for r in &input.requests {
            let key = table.key(&parse_item(r.key.as_ref().unwrap())?, true)?;
            if !keys.insert(key.clone()) {
                return Err(Error::validation("Duplicate batch key"));
            }
            let response = operations::read(db, "GetItem", r)?;
            responses.push((key, response));
        }
        prepared.insert(name.clone(), responses);
    }
    let mut responses = serde_json::Map::new();
    let mut unprocessed = serde_json::Map::new();
    let mut total_size = 0;
    for (name, values) in prepared {
        let table = &db.tables[&name];
        let input = &request.tables[&name];
        let mut partition_sizes = BTreeMap::new();
        let mut items = Vec::new();
        let mut pending = Vec::new();
        for (position, (key, response)) in values.into_iter().enumerate() {
            let read_size = table.items.get(&key).map(item_size_bytes).unwrap_or(0);
            let response_size = response
                .get("Item")
                .map(parse_item)
                .transpose()?
                .as_ref()
                .map(item_size_bytes)
                .unwrap_or(0);
            let partition_size = partition_sizes.entry(key.0).or_insert(0);
            if deferred.get(&name).is_some_and(|p| p.contains(&position))
                || total_size + response_size > 16 * 1024 * 1024
                || *partition_size + read_size > 1024 * 1024
            {
                pending.push(input.requests[position].key.clone().unwrap());
                continue;
            }
            *partition_size += read_size;
            total_size += response_size;
            if let Some(item) = response.get("Item") {
                items.push(item.clone());
            }
        }
        responses.insert(name.clone(), Json::Array(items));
        if !pending.is_empty() {
            let mut options = input.original.clone();
            options["Keys"] = Json::Array(pending);
            unprocessed.insert(name, options);
        }
    }
    Ok(json!({"Responses":responses,"UnprocessedKeys":unprocessed}))
}
