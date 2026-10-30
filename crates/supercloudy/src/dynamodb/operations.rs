use super::{
    api::Request,
    error::{Error, Result},
    expressions::{Action, Condition, Operand, Parser, Segment, apply_updates},
    storage::{Database, Index, KeyTuple, Table},
    value::{Item, Scalar, Value, item_size_bytes, parse_item, wire_item},
};
use serde_json::{Value as Json, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn required<'a, T>(v: &'a Option<T>, name: &str) -> Result<&'a T> {
    v.as_ref()
        .ok_or_else(|| Error::validation(format!("Missing {name}")))
}
fn missing() -> Error {
    Error::new(
        "ResourceNotFoundException",
        "Requested table does not exist",
    )
}
fn table_name(s: &str) -> Result<()> {
    if !(3..=255).contains(&s.len())
        || !s
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err(Error::validation("Invalid table name"));
    }
    Ok(())
}

pub(crate) fn decode(op: &str, body: &[u8]) -> Result<Request> {
    let raw: Json = serde_json::from_slice(body)
        .map_err(|_| Error::new("SerializationException", "Invalid JSON body"))?;
    let object = raw
        .as_object()
        .ok_or_else(|| Error::new("SerializationException", "Expected request object"))?;
    let mut allowed = vec!["TableName"];
    match op {
        "CreateTable" => allowed.extend([
            "KeySchema",
            "AttributeDefinitions",
            "BillingMode",
            "ProvisionedThroughput",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
        ]),
        "DescribeTable" | "DeleteTable" => {}
        "GetItem" => allowed.extend([
            "Key",
            "ConsistentRead",
            "ProjectionExpression",
            "ExpressionAttributeNames",
        ]),
        "PutItem" => allowed.extend([
            "Item",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
            "ReturnValuesOnConditionCheckFailure",
        ]),
        "DeleteItem" => allowed.extend([
            "Key",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
            "ReturnValuesOnConditionCheckFailure",
        ]),
        "UpdateItem" => allowed.extend([
            "Key",
            "UpdateExpression",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
            "ReturnValuesOnConditionCheckFailure",
        ]),
        "Query" | "Scan" => allowed.extend([
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "Limit",
            "ExclusiveStartKey",
            "ConsistentRead",
            "Select",
            "IndexName",
            "ProjectionExpression",
            "FilterExpression",
        ]),
        _ => unreachable!(),
    }
    if op == "Query" {
        allowed.extend(["KeyConditionExpression", "ScanIndexForward"]);
    }
    if op == "Scan" {
        allowed.extend(["Segment", "TotalSegments"]);
    }
    if let Some(k) = object.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(Error::validation(format!(
            "Unsupported request parameter: {k}"
        )));
    }
    let r: Request = serde_json::from_value(raw)
        .map_err(|_| Error::new("SerializationException", "Invalid request fields"))?;
    table_name(&r.table_name)?;
    // Consistency is immediate for both modes in this in-memory server.
    let _ = r.consistent_read;
    Ok(r)
}
pub(crate) fn parser(r: &Request) -> Result<Parser> {
    Parser::new(
        r.expression_attribute_names.clone(),
        r.expression_attribute_values
            .as_ref()
            .map(parse_item)
            .transpose()?
            .unwrap_or_default(),
    )
}
fn condition(r: &Request, p: &mut Parser) -> Result<Option<Condition>> {
    r.condition_expression
        .as_ref()
        .map(|s| p.condition(s))
        .transpose()
}
pub(crate) fn check(c: &Option<Condition>, old: &Item) -> Result<()> {
    if c.as_ref()
        .map(|c| c.eval(old))
        .transpose()?
        .is_some_and(|b| !b)
    {
        return Err(Error::new(
            "ConditionalCheckFailedException",
            "The conditional request failed",
        ));
    }
    Ok(())
}
fn return_mode(r: &Request, update: bool) -> Result<&str> {
    let mode = r.return_values.as_deref().unwrap_or("NONE");
    if !(["NONE", "ALL_OLD"].contains(&mode)
        || update && ["ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"].contains(&mode))
    {
        return Err(Error::validation("Unsupported ReturnValues"));
    }
    Ok(mode)
}

pub(crate) fn write(db: &mut Database, op: &str, r: &Request) -> Result<Json> {
    match op {
        "CreateTable" => return create_table(db, r),
        "DeleteTable" => {
            let table = db.tables.remove(&r.table_name).ok_or_else(missing)?;
            return Ok(json!({"TableDescription":table.description()}));
        }
        _ => {}
    }
    let table = db.tables.get_mut(&r.table_name).ok_or_else(missing)?;
    let prepared = prepare_write(table, op, r)?;
    if let Some(item) = prepared.candidate {
        table.items.insert(prepared.key, item);
    } else {
        table.items.remove(&prepared.key);
    }
    Ok(prepared.response)
}
pub(crate) struct PreparedWrite {
    pub key: KeyTuple,
    pub candidate: Option<Item>,
    pub response: Json,
}
pub(crate) fn prepare_write(table: &Table, op: &str, r: &Request) -> Result<PreparedWrite> {
    if r.return_values_on_condition_check_failure
        .as_deref()
        .is_some_and(|s| !["NONE", "ALL_OLD"].contains(&s))
    {
        return Err(Error::validation(
            "Invalid ReturnValuesOnConditionCheckFailure",
        ));
    }
    let update = op == "UpdateItem";
    let mode = return_mode(r, update)?;
    let mut p = parser(r)?;
    let c = condition(r, &mut p)?;
    let actions = if update {
        p.updates(required(&r.update_expression, "UpdateExpression")?)?
    } else {
        vec![]
    };
    p.finish()?;
    if actions.iter().any(|a|matches!(&a.path[0],Segment::Name(n) if table.schema.iter().any(|k| &k.attribute_name==n))) {return Err(Error::validation("Cannot update primary key attributes"));}
    let input = parse_item(if op == "PutItem" {
        required(&r.item, "Item")?
    } else {
        required(&r.key, "Key")?
    })?;
    let key = table.key(&input, op != "PutItem")?;
    let old = table.items.get(&key).cloned();
    let empty = Item::new();
    let previous = old.as_ref().unwrap_or(&empty);
    if let Err(error) = check(&c, previous) {
        return Err(
            if r.return_values_on_condition_check_failure.as_deref() == Some("ALL_OLD")
                && old.is_some()
            {
                error.with_detail("Item", wire_item(previous))
            } else {
                error
            },
        );
    }
    let candidate = match op {
        "PutItem" => Some(input),
        "DeleteItem" => None,
        "UpdateItem" => {
            let base = old.as_ref().unwrap_or(&input);
            let item = apply_updates(base, &actions)?;
            if old.is_none() && actions.iter().all(|a| a.kind == "REMOVE") {
                None
            } else {
                Some(item)
            }
        }
        _ => unreachable!(),
    };
    if let Some(item) = &candidate {
        table.validate_candidate(item)?;
    }
    let attributes = match mode {
        "ALL_OLD" => old.clone(),
        "ALL_NEW" => candidate.clone(),
        "UPDATED_OLD" => old.as_ref().map(|i| updated_attributes(i, &actions)),
        "UPDATED_NEW" => candidate.as_ref().map(|i| updated_attributes(i, &actions)),
        _ => None,
    };
    let mut response = json!({});
    if let Some(item) = attributes.filter(|a| !a.is_empty()) {
        response["Attributes"] = wire_item(&item);
    }
    Ok(PreparedWrite {
        key,
        candidate,
        response,
    })
}

fn updated_attributes(item: &Item, actions: &[Action]) -> Item {
    let paths = actions
        .iter()
        .map(|a| a.path.as_slice())
        .collect::<Vec<_>>();
    updated_map(item, &paths)
}
fn updated_map(item: &Item, paths: &[&[Segment]]) -> Item {
    item.iter()
        .filter_map(|(name, value)| {
            let tails = paths
                .iter()
                .filter_map(|path| match path.split_first() {
                    Some((Segment::Name(n), tail)) if n == name => Some(tail),
                    _ => None,
                })
                .collect::<Vec<_>>();
            updated_value(value, &tails).map(|value| (name.clone(), value))
        })
        .collect()
}
fn updated_value(value: &Value, paths: &[&[Segment]]) -> Option<Value> {
    if paths.is_empty() {
        return None;
    }
    if paths.iter().any(|path| path.is_empty()) {
        return Some(value.clone());
    }
    match value {
        Value::M(item) => {
            let selected = updated_map(item, paths);
            (!selected.is_empty()).then_some(Value::M(selected))
        }
        Value::L(values) => {
            // Select by source index before compacting, so multiple paths into
            // one element merge and expression order cannot reorder the list.
            let selected = values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| {
                    let tails = paths
                        .iter()
                        .filter_map(|path| match path.split_first() {
                            Some((Segment::Index(i), tail)) if *i == index => Some(tail),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    updated_value(value, &tails)
                })
                .collect::<Vec<_>>();
            (!selected.is_empty()).then_some(Value::L(selected))
        }
        _ => None,
    }
}
fn validate_schema(mut schema: Vec<super::api::KeySchema>) -> Result<Vec<super::api::KeySchema>> {
    if !(1..=2).contains(&schema.len())
        || schema.iter().filter(|k| k.key_type == "HASH").count() != 1
        || schema.iter().filter(|k| k.key_type == "RANGE").count() != schema.len() - 1
        || schema
            .iter()
            .map(|k| &k.attribute_name)
            .collect::<BTreeSet<_>>()
            .len()
            != schema.len()
    {
        return Err(Error::validation("Invalid key schema"));
    }
    schema.sort_by_key(|k| if k.key_type == "HASH" { 0 } else { 1 });
    Ok(schema)
}
fn create_table(db: &mut Database, r: &Request) -> Result<Json> {
    let table = prepare_create_table(db, r)?;
    let response = json!({"TableDescription":table.description()});
    db.tables.insert(r.table_name.clone(), table);
    Ok(response)
}
pub(crate) fn prepare_create_table(db: &Database, r: &Request) -> Result<Table> {
    let schema = validate_schema(required(&r.key_schema, "KeySchema")?.clone())?;
    let definitions = required(&r.attribute_definitions, "AttributeDefinitions")?.clone();
    let mut indexes = vec![];
    let mut names = BTreeSet::new();
    let mut included_count = 0;
    for (local, input) in [
        (false, &r.global_secondary_indexes),
        (true, &r.local_secondary_indexes),
    ] {
        if let Some(input) = input {
            if input.is_empty() || input.len() > if local { 5 } else { 20 } {
                return Err(Error::validation("Invalid number of indexes"));
            }
            for definition in input {
                table_name(&definition.index_name)?;
                if !names.insert(definition.index_name.clone()) {
                    return Err(Error::validation("Duplicate index name"));
                }
                let keys = validate_schema(definition.key_schema.clone())?;
                if local
                    && (schema.len() != 2
                        || keys.len() != 2
                        || keys[0].attribute_name != schema[0].attribute_name
                        || keys[1].attribute_name == schema[1].attribute_name
                        || definition.provisioned_throughput.is_some())
                {
                    return Err(Error::validation("Invalid local secondary index"));
                }
                let projection = &definition.projection;
                match projection.projection_type.as_str() {
                    "ALL" | "KEYS_ONLY" if projection.non_key_attributes.is_none() => {}
                    "INCLUDE" => {
                        let attrs = projection.non_key_attributes.as_ref().ok_or_else(|| {
                            Error::validation("INCLUDE requires NonKeyAttributes")
                        })?;
                        if !(1..=20).contains(&attrs.len())
                            || attrs.iter().collect::<BTreeSet<_>>().len() != attrs.len()
                            || attrs.iter().any(|s| {
                                s.is_empty()
                                    || s.len() > 255
                                    || schema.iter().chain(&keys).any(|k| k.attribute_name == *s)
                            })
                        {
                            return Err(Error::validation("Invalid NonKeyAttributes"));
                        }
                        included_count += attrs.len();
                    }
                    _ => return Err(Error::validation("Invalid index projection")),
                }
                indexes.push(Index {
                    name: definition.index_name.clone(),
                    local,
                    schema: keys,
                    projection: projection.clone(),
                    throughput: definition.provisioned_throughput.clone(),
                });
            }
        }
    }
    if included_count > 100 {
        return Err(Error::validation("Too many projected attributes"));
    }
    let key_names: BTreeSet<_> = schema
        .iter()
        .chain(indexes.iter().flat_map(|i| &i.schema))
        .map(|k| &k.attribute_name)
        .collect();
    if key_names.iter().any(|s| s.is_empty() || s.len() > 255)
        || definitions.len() != key_names.len()
        || definitions
            .iter()
            .map(|d| &d.attribute_name)
            .collect::<BTreeSet<_>>()
            != key_names
        || definitions
            .iter()
            .any(|d| !["S", "N", "B"].contains(&d.attribute_type.as_str()))
    {
        return Err(Error::validation(
            "Attribute definitions must match all key schemas",
        ));
    }
    let billing = r.billing_mode.as_deref().unwrap_or("PROVISIONED");
    if !["PROVISIONED", "PAY_PER_REQUEST"].contains(&billing)
        || (billing == "PAY_PER_REQUEST" && r.provisioned_throughput.is_some())
    {
        return Err(Error::validation("Invalid billing configuration"));
    }
    if billing == "PROVISIONED"
        && r.provisioned_throughput
            .as_ref()
            .is_none_or(|t| t.read_capacity_units <= 0 || t.write_capacity_units <= 0)
    {
        return Err(Error::validation(
            "Positive provisioned throughput is required",
        ));
    }
    for index in &indexes {
        if !index.local
            && ((billing == "PAY_PER_REQUEST" && index.throughput.is_some())
                || (billing == "PROVISIONED"
                    && index
                        .throughput
                        .as_ref()
                        .is_none_or(|t| t.read_capacity_units <= 0 || t.write_capacity_units <= 0)))
        {
            return Err(Error::validation("Invalid index throughput"));
        }
    }
    if db.tables.contains_key(&r.table_name) {
        return Err(Error::new("ResourceInUseException", "Table already exists"));
    }
    let table = Table {
        name: r.table_name.clone(),
        schema,
        definitions,
        indexes,
        billing: billing.into(),
        throughput: r.provisioned_throughput.clone(),
        created: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        items: BTreeMap::new(),
    };
    Ok(table)
}

pub(crate) fn read(db: &Database, op: &str, r: &Request) -> Result<Json> {
    let table = db.tables.get(&r.table_name).ok_or_else(missing)?;
    match op {
        "DescribeTable" => Ok(json!({"Table":table.description()})),
        "GetItem" => {
            let key = table.key(&parse_item(required(&r.key, "Key")?)?, true)?;
            let mut p = parser(r)?;
            let projection = parse_projection(r, &mut p)?;
            p.finish()?;
            Ok(match table.items.get(&key) {
                Some(i) => json!({"Item":wire_item(&project(i, projection.as_deref()))}),
                None => json!({}),
            })
        }
        "Query" | "Scan" => page(table, op, r),
        _ => unreachable!(),
    }
}
pub(crate) fn parse_projection(
    r: &Request,
    p: &mut Parser,
) -> Result<Option<Vec<super::expressions::Path>>> {
    r.projection_expression
        .as_ref()
        .map(|s| p.projection(s))
        .transpose()
}
pub(crate) fn project(item: &Item, paths: Option<&[super::expressions::Path]>) -> Item {
    match paths {
        Some(paths) => updated_map(item, &paths.iter().map(Vec::as_slice).collect::<Vec<_>>()),
        None => item.clone(),
    }
}
fn operand_paths<'a>(operand: &'a Operand, paths: &mut Vec<&'a super::expressions::Path>) {
    match operand {
        Operand::Path(path) => paths.push(path),
        Operand::Function(_, args) => args.iter().for_each(|a| operand_paths(a, paths)),
        Operand::Arithmetic(a, _, b) => {
            operand_paths(a, paths);
            operand_paths(b, paths);
        }
        Operand::Literal(_) => {}
    }
}
fn condition_paths<'a>(condition: &'a Condition, paths: &mut Vec<&'a super::expressions::Path>) {
    match condition {
        Condition::Compare(a, _, b) => {
            operand_paths(a, paths);
            operand_paths(b, paths);
        }
        Condition::Between(a, b, c) => {
            operand_paths(a, paths);
            operand_paths(b, paths);
            operand_paths(c, paths);
        }
        Condition::In(a, args) => {
            operand_paths(a, paths);
            args.iter().for_each(|a| operand_paths(a, paths));
        }
        Condition::Function(_, args) => args.iter().for_each(|a| operand_paths(a, paths)),
        Condition::And(a, b) | Condition::Or(a, b) => {
            condition_paths(a, paths);
            condition_paths(b, paths);
        }
        Condition::Not(a) => condition_paths(a, paths),
    }
}
fn projected_name(table: &Table, index: &Index, name: &str) -> bool {
    index.projection.projection_type == "ALL"
        || table
            .schema
            .iter()
            .chain(&index.schema)
            .any(|k| k.attribute_name == name)
        || index
            .projection
            .non_key_attributes
            .as_ref()
            .is_some_and(|attrs| attrs.iter().any(|a| a == name))
}
// FNV-1a over the scalar type followed by its canonical value. This assignment
// keeps a logical partition together and is stable across server restarts.
fn segment_for(key: &Scalar, total: i64) -> i64 {
    let (tag, value) = match key {
        Scalar::S(s) => (b'S', s.as_bytes().to_vec()),
        Scalar::N(n) => (b'N', n.normalized().to_plain_string().into_bytes()),
        Scalar::B(b) => (b'B', b.clone()),
    };
    let hash = std::iter::once(tag)
        .chain(value)
        .fold(14695981039346656037u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(1099511628211)
        });
    (hash % total as u64) as i64
}
fn page(table: &Table, op: &str, r: &Request) -> Result<Json> {
    let query = op == "Query";
    if query && (r.segment.is_some() || r.total_segments.is_some())
        || !query && (r.key_condition_expression.is_some() || r.scan_index_forward.is_some())
    {
        return Err(Error::validation("Invalid parameters for read operation"));
    }
    let segment = match (r.segment, r.total_segments) {
        (None, None) => None,
        (Some(s), Some(t)) if (1..=1_000_000).contains(&t) && (0..t).contains(&s) => Some((s, t)),
        _ => return Err(Error::validation("Invalid Segment/TotalSegments")),
    };
    let index = r
        .index_name
        .as_ref()
        .map(|name| {
            table
                .indexes
                .iter()
                .find(|i| &i.name == name)
                .ok_or_else(|| Error::validation("Index does not exist"))
        })
        .transpose()?;
    if index.is_some_and(|i| !i.local) && r.consistent_read == Some(true) {
        return Err(Error::validation(
            "Global indexes do not support consistent reads",
        ));
    }
    let schema = index.map_or(table.schema.as_slice(), |i| i.schema.as_slice());
    let mut p = parser(r)?;
    let key_condition = if query {
        let c = p.condition(required(
            &r.key_condition_expression,
            "KeyConditionExpression",
        )?)?;
        validate_key_condition(table, schema, &c)?;
        Some(c)
    } else {
        None
    };
    let filter = r
        .filter_expression
        .as_ref()
        .map(|s| p.condition(s))
        .transpose()?;
    let projection = parse_projection(r, &mut p)?;
    p.finish()?;
    let mut filter_paths = vec![];
    if let Some(filter) = &filter {
        condition_paths(filter, &mut filter_paths);
    }
    if query && filter_paths.iter().any(|path| matches!(&path[0], Segment::Name(n) if schema.iter().any(|k| k.attribute_name == *n))) {
        return Err(Error::validation("Query filter cannot reference selected key attributes"));
    }
    let select = r.select.as_deref().unwrap_or(if projection.is_some() {
        "SPECIFIC_ATTRIBUTES"
    } else if index.is_some() {
        "ALL_PROJECTED_ATTRIBUTES"
    } else {
        "ALL_ATTRIBUTES"
    });
    if ![
        "COUNT",
        "ALL_ATTRIBUTES",
        "ALL_PROJECTED_ATTRIBUTES",
        "SPECIFIC_ATTRIBUTES",
    ]
    .contains(&select)
        || (projection.is_some() && select != "SPECIFIC_ATTRIBUTES")
        || (select == "SPECIFIC_ATTRIBUTES" && projection.is_none())
        || (index.is_none() && select == "ALL_PROJECTED_ATTRIBUTES")
    {
        return Err(Error::validation("Invalid Select/projection combination"));
    }
    let requested_paths: Vec<_> = filter_paths
        .iter()
        .copied()
        .chain(projection.iter().flatten())
        .collect();
    let unprojected = index.is_some_and(|i| {
        requested_paths
            .iter()
            .any(|path| matches!(&path[0], Segment::Name(n) if !projected_name(table, i, n)))
    });
    if index.is_some_and(|i| !i.local)
        && (unprojected
            || select == "ALL_ATTRIBUTES"
                && index.is_some_and(|i| i.projection.projection_type != "ALL"))
    {
        return Err(Error::validation(
            "Requested attributes are not projected into the global index",
        ));
    }
    let fetch_base = index.is_some_and(|i| i.local) && (unprojected || select == "ALL_ATTRIBUTES");
    let limit = r.limit.unwrap_or(i64::MAX);
    if limit <= 0 {
        return Err(Error::validation("Limit must be positive"));
    }
    let cursor_item = r.exclusive_start_key.as_ref().map(parse_item).transpose()?;
    let start = cursor_item
        .as_ref()
        .map(|item| {
            let expected: BTreeSet<_> = table
                .schema
                .iter()
                .chain(schema)
                .map(|k| &k.attribute_name)
                .collect();
            if item.keys().collect::<BTreeSet<_>>() != expected {
                return Err(Error::validation(
                    "ExclusiveStartKey does not match access path",
                ));
            }
            let access = table.schema_key(item, schema, false)?;
            let base = table.key(item, false)?;
            if segment.is_some_and(|(s, t)| segment_for(&access.0, t) != s) {
                return Err(Error::validation(
                    "ExclusiveStartKey belongs to another segment",
                ));
            }
            if key_condition
                .as_ref()
                .map(|condition| condition.eval(item))
                .transpose()?
                .is_some_and(|valid| !valid)
            {
                return Err(Error::validation(
                    "ExclusiveStartKey must match the key condition",
                ));
            }
            Ok((access, base))
        })
        .transpose()?;
    let forward = !query || r.scan_index_forward.unwrap_or(true);
    let mut rows = vec![];
    for (base, item) in &table.items {
        let access = if let Some(index) = index {
            match table.index_key(item, index)? {
                Some(key) => key,
                None => continue,
            }
        } else {
            base.clone()
        };
        if segment.is_some_and(|(s, t)| segment_for(&access.0, t) != s) {
            continue;
        }
        let order = (access, base.clone());
        if start
            .as_ref()
            .is_some_and(|s| if forward { &order <= s } else { &order >= s })
        {
            continue;
        }
        if key_condition
            .as_ref()
            .map(|c| c.eval(item))
            .transpose()?
            .is_some_and(|v| !v)
        {
            continue;
        }
        rows.push((order, item));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    if !forward {
        rows.reverse();
    }
    let mut scanned = 0i64;
    let mut index_bytes = 0usize;
    let mut fetched_bytes = 0usize;
    let mut count = 0;
    let mut items = vec![];
    let mut last = None;
    let mut stopped = false;
    for (_, base) in rows {
        let entry = index.map(|i| table.projected(base, i));
        let next_index_bytes = index_bytes + item_size_bytes(entry.as_ref().unwrap_or(base));
        let next_fetched_bytes = fetched_bytes
            + if fetch_base {
                item_size_bytes(base).div_ceil(4096) * 4096
            } else {
                0
            };
        // LSI base fetches count each complete base item in 4 KiB blocks,
        // plus the aggregate index data rounded to one 4 KiB block boundary.
        let page_bytes = if fetch_base {
            next_index_bytes.div_ceil(4096) * 4096 + next_fetched_bytes
        } else {
            next_index_bytes
        };
        if scanned == limit || (scanned > 0 && page_bytes > 1024 * 1024) {
            stopped = true;
            break;
        }
        scanned += 1;
        index_bytes = next_index_bytes;
        fetched_bytes = next_fetched_bytes;
        last = Some(base);
        let available = if fetch_base {
            base
        } else {
            entry.as_ref().unwrap_or(base)
        };
        if filter
            .as_ref()
            .map(|f| f.eval(available))
            .transpose()?
            .unwrap_or(true)
        {
            count += 1;
            if select != "COUNT" {
                let output = if select == "ALL_PROJECTED_ATTRIBUTES" {
                    entry.as_ref().unwrap_or(available)
                } else {
                    available
                };
                items.push(wire_item(&project(output, projection.as_deref())));
            }
        }
    }
    let mut response = json!({"Count":count,"ScannedCount":scanned});
    if select != "COUNT" {
        response["Items"] = json!(items);
    }
    if (stopped || scanned == limit)
        && let Some(item) = last
    {
        let mut key = table.key_item(item);
        for k in schema {
            key.insert(k.attribute_name.clone(), item[&k.attribute_name].clone());
        }
        response["LastEvaluatedKey"] = wire_item(&key);
    }
    Ok(response)
}
fn validate_key_condition(
    table: &Table,
    schema: &[super::api::KeySchema],
    c: &Condition,
) -> Result<()> {
    let clauses = match c {
        Condition::And(a, b) => vec![a.as_ref(), b.as_ref()],
        _ => vec![c],
    };
    if clauses.len() > schema.len() {
        return Err(Error::validation("Too many key conditions"));
    }
    let mut seen = BTreeSet::new();
    for c in clauses {
        let (operand, values, eq, prefix) = match c {
            Condition::Compare(a, op, b) if ["=", "<", "<=", ">", ">="].contains(&op.as_str()) => {
                if matches!(a, Operand::Literal(_)) {
                    (b, vec![a], op == "=", false)
                } else {
                    (a, vec![b], op == "=", false)
                }
            }
            Condition::Between(a, b, c) => (a, vec![b, c], false, false),
            Condition::Function(f, args) if f == "begins_with" => {
                (&args[0], vec![&args[1]], false, true)
            }
            _ => return Err(Error::validation("Invalid key condition")),
        };
        let Operand::Path(path) = operand else {
            return Err(Error::validation("Key condition must reference key"));
        };
        let [Segment::Name(name)] = path.as_slice() else {
            return Err(Error::validation("Invalid key path"));
        };
        let index = schema
            .iter()
            .position(|k| k.attribute_name == *name)
            .ok_or_else(|| Error::validation("Non-key attribute in key condition"))?;
        if !seen.insert(index) || (index == 0 && !eq) {
            return Err(Error::validation("Partition key requires equality"));
        }
        let tag = &table
            .definitions
            .iter()
            .find(|d| d.attribute_name == *name)
            .unwrap()
            .attribute_type;
        for v in &values {
            let Operand::Literal(v) = v else {
                return Err(Error::validation("Key operand must be a value"));
            };
            Scalar::from_value(v, tag, if index == 0 { 2048 } else { 1024 })?;
            if prefix && !matches!(v, Value::S(_) | Value::B(_)) {
                return Err(Error::validation("Invalid begins_with key type"));
            }
        }
    }
    if !seen.contains(&0) {
        return Err(Error::validation("Partition key equality is required"));
    }
    Ok(())
}
