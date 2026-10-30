use super::{
    api::Request,
    error::{Error, Result},
    expressions::{Action, Condition, Operand, Parser, Segment, apply_updates},
    storage::{Database, Table},
    value::{Item, Scalar, Value, parse_item, wire_item},
};
use serde_json::{Value as Json, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

fn required<'a, T>(v: &'a Option<T>, name: &str) -> Result<&'a T> {
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
        ]),
        "DescribeTable" | "DeleteTable" => {}
        "GetItem" => allowed.extend(["Key", "ConsistentRead"]),
        "PutItem" => allowed.extend([
            "Item",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
        ]),
        "DeleteItem" => allowed.extend([
            "Key",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
        ]),
        "UpdateItem" => allowed.extend([
            "Key",
            "UpdateExpression",
            "ConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "ReturnValues",
        ]),
        "Query" => allowed.extend([
            "KeyConditionExpression",
            "ExpressionAttributeNames",
            "ExpressionAttributeValues",
            "Limit",
            "ExclusiveStartKey",
            "ScanIndexForward",
            "ConsistentRead",
            "Select",
        ]),
        _ => unreachable!(),
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
fn parser(r: &Request) -> Result<Parser> {
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
fn check(c: &Option<Condition>, old: &Item) -> Result<()> {
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
    check(&c, previous)?;
    let candidate = match op {
        "PutItem" => Some(input),
        "DeleteItem" => None,
        "UpdateItem" => {
            let base = old.as_ref().unwrap_or(&input);
            Some(apply_updates(base, &actions)?)
        }
        _ => unreachable!(),
    };
    if let Some(item) = &candidate {
        table.key(item, false)?;
    }
    let attributes = match mode {
        "ALL_OLD" => old.clone(),
        "ALL_NEW" => candidate.clone(),
        "UPDATED_OLD" => old.as_ref().map(|i| updated_attributes(i, &actions)),
        "UPDATED_NEW" => candidate.as_ref().map(|i| updated_attributes(i, &actions)),
        _ => None,
    };
    if let Some(item) = candidate {
        table.items.insert(key, item);
    } else {
        table.items.remove(&key);
    }
    let mut response = json!({});
    if let Some(item) = attributes.filter(|a| !a.is_empty()) {
        response["Attributes"] = wire_item(&item);
    }
    Ok(response)
}
fn updated_attributes(item: &Item, actions: &[Action]) -> Item {
    // Return the affected top-level attributes, retaining their document values.
    actions
        .iter()
        .filter(|a| super::expressions::get_path(item, &a.path).is_some())
        .filter_map(|a| match &a.path[0] {
            Segment::Name(n) => item.get(n).map(|v| (n.clone(), v.clone())),
            _ => None,
        })
        .collect()
}
fn create_table(db: &mut Database, r: &Request) -> Result<Json> {
    let mut schema = required(&r.key_schema, "KeySchema")?.clone();
    let definitions = required(&r.attribute_definitions, "AttributeDefinitions")?.clone();
    if !(1..=2).contains(&schema.len())
        || schema.iter().filter(|k| k.key_type == "HASH").count() != 1
        || schema.iter().filter(|k| k.key_type == "RANGE").count() != schema.len() - 1
    {
        return Err(Error::validation("Invalid key schema"));
    }
    schema.sort_by_key(|k| if k.key_type == "HASH" { 0 } else { 1 });
    let key_names = schema
        .iter()
        .map(|k| &k.attribute_name)
        .collect::<BTreeSet<_>>();
    if key_names.len() != schema.len()
        || key_names.iter().any(|s| s.is_empty() || s.len() > 255)
        || definitions.len() != schema.len()
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
            "Attribute definitions must match key schema",
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
    if db.tables.contains_key(&r.table_name) {
        return Err(Error::new("ResourceInUseException", "Table already exists"));
    }
    let table = Table {
        name: r.table_name.clone(),
        schema,
        definitions,
        billing: billing.into(),
        throughput: r.provisioned_throughput.clone(),
        created: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        items: BTreeMap::new(),
    };
    let response = json!({"TableDescription":table.description()});
    db.tables.insert(r.table_name.clone(), table);
    Ok(response)
}

pub(crate) fn read(db: &Database, op: &str, r: &Request) -> Result<Json> {
    let table = db.tables.get(&r.table_name).ok_or_else(missing)?;
    match op {
        "DescribeTable" => Ok(json!({"Table":table.description()})),
        "GetItem" => {
            let key = table.key(&parse_item(required(&r.key, "Key")?)?, true)?;
            Ok(match table.items.get(&key) {
                Some(i) => json!({"Item":wire_item(i)}),
                None => json!({}),
            })
        }
        "Query" => query(table, r),
        _ => unreachable!(),
    }
}
fn query(table: &Table, r: &Request) -> Result<Json> {
    let mut p = parser(r)?;
    let condition = p.condition(required(
        &r.key_condition_expression,
        "KeyConditionExpression",
    )?)?;
    p.finish()?;
    validate_key_condition(table, &condition)?;
    let limit = r.limit.unwrap_or(i64::MAX);
    if limit <= 0 {
        return Err(Error::validation("Limit must be positive"));
    }
    let count_only = match r.select.as_deref().unwrap_or("ALL_ATTRIBUTES") {
        "COUNT" => true,
        "ALL_ATTRIBUTES" => false,
        _ => return Err(Error::validation("Unsupported Select")),
    };
    let start_item = r.exclusive_start_key.as_ref().map(parse_item).transpose()?;
    let start = start_item
        .as_ref()
        .map(|item| table.key(item, true))
        .transpose()?;
    if let Some(item) = start_item
        && !condition.eval(&item)?
    {
        return Err(Error::validation(
            "ExclusiveStartKey must match the key condition",
        ));
    }
    let forward = r.scan_index_forward.unwrap_or(true);
    let mut matches = vec![];
    for (key, item) in &table.items {
        if start
            .as_ref()
            .is_some_and(|s| if forward { key <= s } else { key >= s })
        {
            continue;
        }
        if condition.eval(item)? {
            matches.push(item);
        }
    }
    if !forward {
        matches.reverse();
    }
    let take = usize::try_from(limit)
        .unwrap_or(usize::MAX)
        .min(matches.len());
    let page = &matches[..take];
    let mut response = json!({"Count":page.len(),"ScannedCount":page.len()});
    if !count_only {
        response["Items"] = json!(page.iter().map(|i| wire_item(i)).collect::<Vec<_>>());
    }
    if take > 0 && take as i64 == limit {
        response["LastEvaluatedKey"] = wire_item(&table.key_item(page[take - 1]));
    }
    Ok(response)
}
fn validate_key_condition(table: &Table, c: &Condition) -> Result<()> {
    let clauses = match c {
        Condition::And(a, b) => vec![a.as_ref(), b.as_ref()],
        _ => vec![c],
    };
    if clauses.len() > table.schema.len() {
        return Err(Error::validation("Too many key conditions"));
    }
    let mut seen = BTreeSet::new();
    for c in clauses {
        let (operand, values, eq, prefix) = match c {
            Condition::Compare(a, op, b) if ["=", "<", "<=", ">", ">="].contains(&op.as_str()) => {
                (a, vec![b], op == "=", false)
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
        let index = table
            .schema
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
        if values.len() == 2 {
            let (Operand::Literal(a), Operand::Literal(b)) = (values[0], values[1]) else {
                unreachable!()
            };
            if a.compare(b) == Some(std::cmp::Ordering::Greater) {
                return Err(Error::validation("Invalid BETWEEN bounds"));
            }
        }
    }
    if !seen.contains(&0) {
        return Err(Error::validation("Partition key equality is required"));
    }
    Ok(())
}
