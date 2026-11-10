//! Pure BSON query, expression, and candidate-update evaluation.
use std::{cmp::Ordering, collections::BTreeMap, str::FromStr};

use bigdecimal::{BigDecimal, FromPrimitive};
use bson::{Bson, Document, doc};

pub(super) type Result<T> = std::result::Result<T, super::Error>;
type Vars = BTreeMap<String, Bson>;
fn error(message: impl Into<String>) -> super::Error {
    super::Error::new(2, message)
}
fn object(value: &Bson) -> Result<&Document> {
    value
        .as_document()
        .ok_or_else(|| error("expected an object"))
}
fn array(value: &Bson) -> Result<&Vec<Bson>> {
    value.as_array().ok_or_else(|| error("expected an array"))
}
fn string(value: &Bson) -> Result<&str> {
    value.as_str().ok_or_else(|| error("expected a string"))
}
fn integer(value: &Bson) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(i64::from(*n)),
        Bson::Int64(n) => Ok(*n),
        _ => Err(error("expected an integer")),
    }
}
fn numeric(value: &Bson) -> Option<BigDecimal> {
    match value {
        Bson::Int32(n) => Some(BigDecimal::from(*n)),
        Bson::Int64(n) => Some(BigDecimal::from(*n)),
        Bson::Double(n) => BigDecimal::from_f64(*n),
        Bson::Decimal128(n) => BigDecimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}
fn rank(value: &Bson) -> u8 {
    match value {
        Bson::MinKey => 0,
        Bson::Undefined => 1,
        Bson::Null => 2,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 3,
        Bson::String(_) | Bson::Symbol(_) => 4,
        Bson::Document(_) => 5,
        Bson::Array(_) => 6,
        Bson::Binary(_) => 7,
        Bson::ObjectId(_) => 8,
        Bson::Boolean(_) => 9,
        Bson::DateTime(_) => 10,
        Bson::Timestamp(_) => 11,
        Bson::RegularExpression(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::JavaScriptCodeWithScope(_) => 14,
        Bson::DbPointer(_) => 15,
        Bson::MaxKey => 255,
    }
}
pub(super) fn compare(a: &Bson, b: &Bson) -> Ordering {
    if rank(a) == 3 && rank(b) == 3 {
        fn special(v: &Bson) -> i8 {
            match v {
                Bson::Double(n) if n.is_nan() => -2,
                Bson::Double(n) if *n == f64::NEG_INFINITY => -1,
                Bson::Double(n) if *n == f64::INFINITY => 1,
                Bson::Decimal128(n) => match n.to_string().as_str() {
                    "NaN" | "-NaN" => -2,
                    "-Infinity" => -1,
                    "Infinity" => 1,
                    _ => 0,
                },
                _ => 0,
            }
        }
        let c = special(a).cmp(&special(b));
        if c != Ordering::Equal {
            return c;
        }
        if special(a) != 0 {
            return Ordering::Equal;
        }
    }
    if let (Some(a), Some(b)) = (numeric(a), numeric(b)) {
        return a.cmp(&b);
    }
    let r = rank(a).cmp(&rank(b));
    if r != Ordering::Equal {
        return r;
    }
    match (a, b) {
        (Bson::Double(a), Bson::Double(b)) => a.total_cmp(b),
        (Bson::String(a), Bson::String(b)) | (Bson::Symbol(a), Bson::Symbol(b)) => a.cmp(b),
        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a.bytes().cmp(&b.bytes()),
        (Bson::DateTime(a), Bson::DateTime(b)) => a.timestamp_millis().cmp(&b.timestamp_millis()),
        (Bson::Timestamp(a), Bson::Timestamp(b)) => {
            (a.time, a.increment).cmp(&(b.time, b.increment))
        }
        (Bson::Array(a), Bson::Array(b)) => compare_iter(a.iter(), b.iter()),
        (Bson::Document(a), Bson::Document(b)) => {
            let mut ai = a.iter();
            let mut bi = b.iter();
            loop {
                match (ai.next(), bi.next()) {
                    (None, None) => return Ordering::Equal,
                    (None, _) => return Ordering::Less,
                    (_, None) => return Ordering::Greater,
                    (Some((ak, av)), Some((bk, bv))) => {
                        let r = rank(av)
                            .cmp(&rank(bv))
                            .then_with(|| ak.cmp(bk))
                            .then_with(|| compare(av, bv));
                        if r != Ordering::Equal {
                            return r;
                        }
                    }
                }
            }
        }
        (Bson::Binary(a), Bson::Binary(b)) => a
            .bytes
            .len()
            .cmp(&b.bytes.len())
            .then_with(|| u8::from(a.subtype).cmp(&u8::from(b.subtype)))
            .then_with(|| a.bytes.cmp(&b.bytes)),
        (Bson::RegularExpression(a), Bson::RegularExpression(b)) => a
            .pattern
            .cmp(&b.pattern)
            .then_with(|| a.options.cmp(&b.options)),
        _ if a == b => Ordering::Equal,
        _ => a.to_string().cmp(&b.to_string()),
    }
}
fn compare_iter<'a>(
    mut a: impl Iterator<Item = &'a Bson>,
    mut b: impl Iterator<Item = &'a Bson>,
) -> Ordering {
    loop {
        match (a.next(), b.next()) {
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some(a), Some(b)) => {
                let r = compare(a, b);
                if r != Ordering::Equal {
                    return r;
                }
            }
        }
    }
}
pub(super) fn equal(a: &Bson, b: &Bson) -> bool {
    compare(a, b) == Ordering::Equal
}
pub(super) fn get<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let mut value = doc.get(parts.next()?)?;
    for part in parts {
        value = match value {
            Bson::Document(d) => d.get(part)?,
            Bson::Array(a) => a.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}
pub(super) fn values<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    fn descend<'a>(v: &'a Bson, parts: &[&str], out: &mut Vec<&'a Bson>) {
        if parts.is_empty() {
            out.push(v);
            return;
        }
        match v {
            Bson::Document(d) => {
                if let Some(v) = d.get(parts[0]) {
                    descend(v, &parts[1..], out)
                }
            }
            Bson::Array(a) => {
                if let Ok(i) = parts[0].parse::<usize>() {
                    if let Some(v) = a.get(i) {
                        descend(v, &parts[1..], out)
                    }
                } else {
                    for v in a {
                        descend(v, parts, out)
                    }
                }
            }
            _ => {}
        }
    }
    let parts: Vec<_> = path.split('.').collect();
    let mut out = vec![];
    if let Some(v) = doc.get(parts[0]) {
        descend(v, &parts[1..], &mut out)
    }
    out
}
fn truth(value: &Bson) -> bool {
    !matches!(value, Bson::Null | Bson::Undefined | Bson::Boolean(false))
        && numeric(value).is_none_or(|n| n != 0)
}
fn regex_match(value: &Bson, pattern: &str, options: &str) -> Result<bool> {
    if options.chars().any(|c| !"imsxu".contains(c)) {
        return Err(error("unsupported regular expression option"));
    }
    let re = regex::RegexBuilder::new(pattern)
        .case_insensitive(options.contains('i'))
        .multi_line(options.contains('m'))
        .dot_matches_new_line(options.contains('s'))
        .ignore_whitespace(options.contains('x'))
        .build()
        .map_err(|_| error("invalid regular expression"))?;
    Ok(value.as_str().is_some_and(|s| re.is_match(s)))
}
fn value_eq(actual: &Bson, expected: &Bson) -> Result<bool> {
    if equal(actual, expected) {
        return Ok(true);
    }
    if let Bson::RegularExpression(re) = expected
        && regex_match(actual, &re.pattern, &re.options)?
    {
        return Ok(true);
    }
    if let Bson::Array(a) = actual {
        for v in a {
            if equal(v, expected) {
                return Ok(true);
            }
            if let Bson::RegularExpression(re) = expected
                && regex_match(v, &re.pattern, &re.options)?
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn matches_values(actual: &[&Bson], condition: &Bson, vars: &Vars) -> Result<bool> {
    let Some(operators) = condition
        .as_document()
        .filter(|d| d.keys().any(|k| k.starts_with('$')))
    else {
        if actual.is_empty() {
            return Ok(matches!(condition, Bson::Null));
        }
        for v in actual {
            if value_eq(v, condition)? {
                return Ok(true);
            }
        }
        return Ok(false);
    };
    for (op, expected) in operators {
        let passed = match op.as_str() {
            "$eq" => literal_condition(actual, expected)?,
            "$ne" => !literal_condition(actual, expected)?,
            "$gt" | "$gte" | "$lt" | "$lte" => {
                let mut yes = false;
                for v in actual {
                    let candidates = match v {
                        Bson::Array(a) if !matches!(expected, Bson::Array(_)) => {
                            a.iter().collect::<Vec<_>>()
                        }
                        _ => vec![*v],
                    };
                    for v in candidates {
                        if rank(v) != rank(expected) {
                            continue;
                        }
                        let c = compare(v, expected);
                        yes |= match op.as_str() {
                            "$gt" => c.is_gt(),
                            "$gte" => c.is_ge(),
                            "$lt" => c.is_lt(),
                            _ => c.is_le(),
                        };
                    }
                }
                yes
            }
            "$in" | "$nin" => {
                let mut yes = false;
                for v in array(expected)? {
                    if literal_condition(actual, v)? {
                        yes = true;
                        break;
                    }
                }
                if op == "$nin" { !yes } else { yes }
            }
            "$exists" => {
                let b = expected
                    .as_bool()
                    .ok_or_else(|| error("$exists requires a boolean"))?;
                b != actual.is_empty()
            }
            "$not" => !matches_values(actual, expected, vars)?,
            "$size" => {
                let n = integer(expected)?;
                actual.iter().any(|v| {
                    v.as_array()
                        .is_some_and(|a| i64::try_from(a.len()).ok() == Some(n))
                })
            }
            "$all" => {
                let required = array(expected)?;
                let mut yes = false;
                for v in actual {
                    if let Some(a) = v.as_array() {
                        let mut all = !required.is_empty();
                        for want in required {
                            let mut found = false;
                            for item in a {
                                found |= value_eq(item, want)?;
                            }
                            all &= found;
                        }
                        yes |= all;
                    }
                }
                yes
            }
            "$elemMatch" => {
                let f = object(expected)?;
                let mut yes = false;
                for v in actual {
                    if let Some(a) = v.as_array() {
                        for element in a {
                            yes |= if f.keys().all(|k| k.starts_with('$')) {
                                matches_values(&[element], expected, vars)?
                            } else if let Some(d) = element.as_document() {
                                matches_vars(d, f, vars)?
                            } else {
                                false
                            };
                        }
                    }
                }
                yes
            }
            "$regex" => {
                let (pattern, options) = match expected {
                    Bson::String(s) => (s.as_str(), operators.get_str("$options").unwrap_or("")),
                    Bson::RegularExpression(r) => (r.pattern.as_str(), r.options.as_str()),
                    _ => return Err(error("$regex requires a string or regex")),
                };
                let mut yes = false;
                for v in actual {
                    if let Bson::Array(a) = v {
                        for v in a {
                            yes |= regex_match(v, pattern, options)?;
                        }
                    } else {
                        yes |= regex_match(v, pattern, options)?;
                    }
                }
                yes
            }
            "$options" => {
                if !operators.contains_key("$regex") {
                    return Err(error("$options requires $regex"));
                }
                true
            }
            "$type" => {
                let expected = query_type(expected)?;
                let matches_type = |v: &Bson| {
                    type_name(v) == expected
                        || (expected == "number"
                            && matches!(
                                v,
                                Bson::Int32(_)
                                    | Bson::Int64(_)
                                    | Bson::Double(_)
                                    | Bson::Decimal128(_)
                            ))
                };
                actual.iter().any(|v| {
                    matches_type(v) || v.as_array().is_some_and(|a| a.iter().any(matches_type))
                })
            }
            _ => return Err(error(format!("unsupported query operator: {op}"))),
        };
        if !passed {
            return Ok(false);
        }
    }
    Ok(true)
}
fn query_type(value: &Bson) -> Result<&str> {
    let name = match value {
        Bson::String(s) => s.as_str(),
        Bson::Int32(code) => match code {
            1 => "double",
            2 => "string",
            3 => "object",
            4 => "array",
            5 => "binData",
            6 => "undefined",
            7 => "objectId",
            8 => "bool",
            9 => "date",
            10 => "null",
            11 => "regex",
            12 => "dbPointer",
            13 => "javascript",
            14 => "symbol",
            15 => "javascriptWithScope",
            16 => "int",
            17 => "timestamp",
            18 => "long",
            19 => "decimal",
            -1 => "minKey",
            127 => "maxKey",
            _ => return Err(error("invalid BSON type code")),
        },
        _ => return Err(error("unsupported BSON type")),
    };
    if ![
        "double",
        "string",
        "object",
        "array",
        "binData",
        "undefined",
        "objectId",
        "bool",
        "date",
        "null",
        "regex",
        "dbPointer",
        "javascript",
        "symbol",
        "javascriptWithScope",
        "int",
        "timestamp",
        "long",
        "decimal",
        "minKey",
        "maxKey",
        "number",
    ]
    .contains(&name)
    {
        return Err(error("invalid BSON type alias"));
    }
    Ok(name)
}
fn type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Undefined => "undefined",
        Bson::Null => "null",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Double(_) => "double",
        Bson::Decimal128(_) => "decimal",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Binary(_) => "binData",
        Bson::RegularExpression(_) => "regex",
        Bson::Timestamp(_) => "timestamp",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Symbol(_) => "symbol",
        Bson::DbPointer(_) => "dbPointer",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}
pub(super) fn matches(doc: &Document, filter: &Document) -> Result<bool> {
    validate_filter(filter)?;
    matches_vars(doc, filter, &Vars::new())
}
fn matches_vars(doc: &Document, filter: &Document, vars: &Vars) -> Result<bool> {
    for (key, condition) in filter {
        let passed = match key.as_str() {
            "$and" | "$or" | "$nor" => {
                let a = array(condition)?;
                if a.is_empty() {
                    return Err(error("logical operator requires a nonempty array"));
                }
                let mut flags = vec![];
                for f in a {
                    flags.push(matches_vars(doc, object(f)?, vars)?);
                }
                match key.as_str() {
                    "$and" => flags.iter().all(|v| *v),
                    "$or" => flags.iter().any(|v| *v),
                    _ => !flags.iter().any(|v| *v),
                }
            }
            "$expr" => truth(&expr(condition, doc, vars)?),
            _ if key.starts_with('$') => {
                return Err(error(format!("unsupported query operator: {key}")));
            }
            _ => matches_values(&values(doc, key), condition, vars)?,
        };
        if !passed {
            return Ok(false);
        }
    }
    Ok(true)
}

fn set_path(doc: &mut Document, path: &str, value: Bson) -> Result<()> {
    fn put(container: &mut Bson, parts: &[&str], value: Bson) -> Result<()> {
        match container {
            Bson::Document(d) => {
                if parts.len() == 1 {
                    d.insert(parts[0], value);
                    return Ok(());
                }
                if !d.contains_key(parts[0]) {
                    d.insert(parts[0], Document::new());
                }
                put(
                    d.get_mut(parts[0]).expect("field inserted"),
                    &parts[1..],
                    value,
                )
            }
            Bson::Array(a) => {
                let i = parts[0]
                    .parse::<usize>()
                    .map_err(|_| error("array update needs an explicit index"))?;
                if i > 1_000_000 {
                    return Err(error("array index too large"));
                }
                a.resize(a.len().max(i + 1), Bson::Null);
                if parts.len() == 1 {
                    a[i] = value;
                    Ok(())
                } else {
                    put(&mut a[i], &parts[1..], value)
                }
            }
            _ => Err(error("cannot create a nested field within a scalar")),
        }
    }
    if path.is_empty() || path.split('.').any(|p| p.is_empty() || p.starts_with('$')) {
        return Err(error("invalid field path"));
    }
    let mut root = Bson::Document(std::mem::take(doc));
    let result = put(&mut root, &path.split('.').collect::<Vec<_>>(), value);
    *doc = root.as_document().expect("root remains document").clone();
    result
}
fn unset_path(doc: &mut Document, path: &str) -> Result<()> {
    fn remove(value: &mut Bson, parts: &[&str]) -> Result<()> {
        match value {
            Bson::Document(d) => {
                if parts.len() == 1 {
                    d.remove(parts[0]);
                } else if let Some(v) = d.get_mut(parts[0]) {
                    remove(v, &parts[1..])?;
                }
            }
            Bson::Array(a) => {
                let i = parts[0]
                    .parse::<usize>()
                    .map_err(|_| error("array update needs an explicit index"))?;
                if let Some(v) = a.get_mut(i) {
                    if parts.len() == 1 {
                        *v = Bson::Null
                    } else {
                        remove(v, &parts[1..])?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    if path.split('.').any(|p| p.is_empty() || p.starts_with('$')) {
        return Err(error("invalid field path"));
    }
    let mut root = Bson::Document(std::mem::take(doc));
    let result = remove(&mut root, &path.split('.').collect::<Vec<_>>());
    *doc = root.as_document().expect("root remains document").clone();
    result
}
fn project_vars(doc: &Document, projection: &Document, vars: &Vars) -> Result<Document> {
    validate_projection(projection)?;
    if projection.is_empty() {
        return Ok(doc.clone());
    }
    let included = projection
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| projection_flag(v) != Some(false));
    let excluded = projection
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| projection_flag(v) == Some(false));
    if included && excluded {
        return Err(error("cannot mix inclusion and exclusion projection"));
    }
    let inclusion = included || (!excluded && projection.get("_id").is_some_and(truth));
    let mut result = if inclusion {
        Document::new()
    } else {
        doc.clone()
    };
    if inclusion
        && projection.get("_id").is_none_or(truth)
        && let Some(v) = doc.get("_id")
    {
        result.insert("_id", v.clone());
    }
    for (key, value) in projection {
        match projection_flag(value) {
            Some(false) => projection_remove(&mut result, &key.split('.').collect::<Vec<_>>()),
            Some(true) => include_path(&mut result, doc, &key.split('.').collect::<Vec<_>>()),
            None => {
                if let Bson::String(path) = value
                    && path.starts_with('$')
                    && !path.starts_with("$$")
                    && values(doc, &path[1..]).is_empty()
                {
                    continue;
                }
                let value = expr(value, doc, vars)?;
                if matches!(value, Bson::Undefined) {
                    unset_path(&mut result, key)?;
                } else {
                    set_path(&mut result, key, value)?;
                }
            }
        }
    }
    Ok(result)
}
pub(super) fn project(doc: &Document, projection: &Document) -> Result<Document> {
    project_vars(doc, projection, &Vars::new())
}
pub(super) fn sort(docs: &mut [Document], spec: &Document) -> Result<()> {
    let mut keys = vec![];
    for (k, v) in spec {
        let n = integer(v)?;
        if n != 1 && n != -1 {
            return Err(error("sort direction must be 1 or -1"));
        }
        keys.push((k, n));
    }
    docs.sort_by(|a, b| {
        for (k, direction) in &keys {
            let av = sort_value(a, k, *direction);
            let bv = sort_value(b, k, *direction);
            let ordering = compare(&av, &bv);
            if ordering != Ordering::Equal {
                return if *direction == 1 {
                    ordering
                } else {
                    ordering.reverse()
                };
            }
        }
        Ordering::Equal
    });
    Ok(())
}
fn sort_value(doc: &Document, key: &str, direction: i64) -> Bson {
    let mut all = vec![];
    for v in values(doc, key) {
        if let Bson::Array(a) = v {
            all.extend(a.iter())
        } else {
            all.push(v)
        }
    }
    let value = if direction == 1 {
        all.into_iter().min_by(|a, b| compare(a, b))
    } else {
        all.into_iter().max_by(|a, b| compare(a, b))
    };
    value.cloned().unwrap_or(Bson::Null)
}

fn args<'a>(value: &'a Bson, n: usize, op: &str) -> Result<&'a Vec<Bson>> {
    let args = array(value)?;
    if args.len() != n {
        return Err(error(format!("{op} requires {n} arguments")));
    }
    Ok(args)
}
fn number_result(n: BigDecimal, prefer_double: bool) -> Result<Bson> {
    if prefer_double {
        return Ok(Bson::Double(
            n.to_string()
                .parse::<f64>()
                .map_err(|_| error("numeric overflow"))?,
        ));
    }
    if let Ok(n) = n.to_string().parse::<i32>() {
        return Ok(Bson::Int32(n));
    }
    if let Ok(n) = n.to_string().parse::<i64>() {
        return Ok(Bson::Int64(n));
    }
    Ok(Bson::Double(
        n.to_string()
            .parse::<f64>()
            .map_err(|_| error("numeric overflow"))?,
    ))
}
fn typed_number_result(n: BigDecimal, operands: &[&Bson], force_double: bool) -> Result<Bson> {
    if operands.iter().any(|v| matches!(v, Bson::Decimal128(_))) {
        return Ok(Bson::Decimal128(
            bson::Decimal128::from_str(&n.to_string())
                .map_err(|_| error("decimal arithmetic overflow"))?,
        ));
    }
    if force_double || operands.iter().any(|v| matches!(v, Bson::Double(_))) {
        return number_result(n, true);
    }
    if operands.iter().any(|v| matches!(v, Bson::Int64(_)))
        && let Ok(n) = n.to_string().parse::<i64>()
    {
        return Ok(Bson::Int64(n));
    }
    number_result(n, false)
}
fn expr(value: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    match value {
        Bson::String(s) if s.starts_with("$$") => {
            let name = &s[2..];
            let (base, path) = name.split_once('.').unwrap_or((name, ""));
            let v = match base {
                "ROOT" | "CURRENT" => Bson::Document(doc.clone()),
                "REMOVE" => Bson::Undefined,
                _ => vars
                    .get(base)
                    .cloned()
                    .ok_or_else(|| error(format!("undefined variable: {base}")))?,
            };
            Ok(if path.is_empty() {
                v
            } else {
                v.as_document()
                    .and_then(|d| get(d, path))
                    .cloned()
                    .unwrap_or(Bson::Undefined)
            })
        }
        Bson::String(s) if s.starts_with('$') => Ok(field_expression(doc, &s[1..])),
        Bson::Array(a) => Ok(Bson::Array(
            a.iter()
                .map(|v| {
                    expr(v, doc, vars).map(|v| {
                        if matches!(v, Bson::Undefined) {
                            Bson::Null
                        } else {
                            v
                        }
                    })
                })
                .collect::<Result<_>>()?,
        )),
        Bson::Document(d) if d.keys().any(|k| k.starts_with('$')) => {
            if d.len() != 1 {
                return Err(error(
                    "an expression object must contain exactly one operator",
                ));
            }
            let (op, input) = d.iter().next().expect("nonempty expression");
            match op.as_str() {
                "$literal" => Ok(input.clone()),
                "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$cmp" => {
                    let a = args(input, 2, op)?;
                    let a0 = expr(&a[0], doc, vars)?;
                    let a1 = expr(&a[1], doc, vars)?;
                    let c = compare(&a0, &a1);
                    if op == "$cmp" {
                        return Ok(Bson::Int32(match c {
                            Ordering::Less => -1,
                            Ordering::Equal => 0,
                            Ordering::Greater => 1,
                        }));
                    }
                    Ok(Bson::Boolean(match op.as_str() {
                        "$eq" => c.is_eq(),
                        "$ne" => c.is_ne(),
                        "$gt" => c.is_gt(),
                        "$gte" => c.is_ge(),
                        "$lt" => c.is_lt(),
                        _ => c.is_le(),
                    }))
                }
                "$and" | "$or" => {
                    let a = array(input)?;
                    let mut result = op == "$and";
                    for v in a {
                        let yes = truth(&expr(v, doc, vars)?);
                        if op == "$and" {
                            result &= yes
                        } else {
                            result |= yes
                        }
                    }
                    Ok(result.into())
                }
                "$not" => {
                    let a = args(input, 1, op)?;
                    Ok((!truth(&expr(&a[0], doc, vars)?)).into())
                }
                "$ifNull" => {
                    let a = array(input)?;
                    if a.len() < 2 {
                        return Err(error("$ifNull requires at least two arguments"));
                    }
                    for v in a {
                        let v = expr(v, doc, vars)?;
                        if !matches!(v, Bson::Null | Bson::Undefined) {
                            return Ok(v);
                        }
                    }
                    Ok(Bson::Null)
                }
                "$cond" => {
                    let (condition, yes, no) = if let Bson::Array(a) = input {
                        if a.len() != 3 {
                            return Err(error("$cond requires three arguments"));
                        }
                        (&a[0], &a[1], &a[2])
                    } else {
                        let d = object(input)?;
                        only_fields(d, &["if", "then", "else"])?;
                        (
                            required(d, "if")?,
                            required(d, "then")?,
                            required(d, "else")?,
                        )
                    };
                    expr(
                        if truth(&expr(condition, doc, vars)?) {
                            yes
                        } else {
                            no
                        },
                        doc,
                        vars,
                    )
                }
                "$switch" => {
                    let d = object(input)?;
                    only_fields(d, &["branches", "default"])?;
                    for branch in array(required(d, "branches")?)? {
                        let b = object(branch)?;
                        only_fields(b, &["case", "then"])?;
                        if truth(&expr(required(b, "case")?, doc, vars)?) {
                            return expr(required(b, "then")?, doc, vars);
                        }
                    }
                    expr(required(d, "default")?, doc, vars)
                }
                "$add" | "$multiply" | "$subtract" | "$divide" | "$mod" => {
                    let a = array(input)?;
                    if matches!(op.as_str(), "$subtract" | "$divide" | "$mod") && a.len() != 2 {
                        return Err(error("arithmetic expression requires two arguments"));
                    }
                    let mut n = if op == "$multiply" {
                        BigDecimal::from(1)
                    } else {
                        BigDecimal::from(0)
                    };
                    let mut double = false;
                    let mut operands = vec![];
                    for (i, v) in a.iter().enumerate() {
                        let v = expr(v, doc, vars)?;
                        if matches!(v, Bson::Null | Bson::Undefined) {
                            return Ok(Bson::Null);
                        }
                        double |= matches!(v, Bson::Double(_));
                        operands.push(v.clone());
                        let v = numeric(&v)
                            .ok_or_else(|| error("arithmetic requires numeric values"))?;
                        if i == 0 && matches!(op.as_str(), "$subtract" | "$divide" | "$mod") {
                            n = v;
                            continue;
                        }
                        n = match op.as_str() {
                            "$add" => n + v,
                            "$multiply" => n * v,
                            "$subtract" => n - v,
                            "$divide" => {
                                if v == 0 {
                                    return Err(error("division by zero"));
                                }
                                double = true;
                                n / v
                            }
                            _ => {
                                if v == 0 {
                                    return Err(error("division by zero"));
                                }
                                n % v
                            }
                        };
                    }
                    typed_number_result(n, &operands.iter().collect::<Vec<_>>(), double)
                }
                "$concat" => {
                    let mut out = String::new();
                    for v in array(input)? {
                        let v = expr(v, doc, vars)?;
                        if matches!(v, Bson::Null | Bson::Undefined) {
                            return Ok(Bson::Null);
                        }
                        out.push_str(string(&v)?);
                    }
                    Ok(out.into())
                }
                "$concatArrays" | "$setUnion" => {
                    let mut out = vec![];
                    for v in array(input)? {
                        let v = expr(v, doc, vars)?;
                        if matches!(v, Bson::Null | Bson::Undefined) {
                            return Ok(Bson::Null);
                        }
                        for v in array(&v)? {
                            if op == "$concatArrays" || !out.iter().any(|x| equal(x, v)) {
                                out.push(v.clone());
                            }
                        }
                    }
                    Ok(out.into())
                }
                "$map" | "$filter" => {
                    let d = object(input)?;
                    let fields = if op == "$map" {
                        &["input", "as", "in"][..]
                    } else {
                        &["input", "as", "cond", "limit"][..]
                    };
                    only_fields(d, fields)?;
                    let input = expr(required(d, "input")?, doc, vars)?;
                    if matches!(input, Bson::Null | Bson::Undefined) {
                        return Ok(Bson::Null);
                    }
                    let a = array(&input)?;
                    let name = d.get_str("as").unwrap_or("this");
                    let mut bound = vars.clone();
                    let mut out = vec![];
                    let limit = if let Some(limit) = d.get("limit") {
                        let limit = expr(limit, doc, vars)?;
                        if matches!(limit, Bson::Null | Bson::Undefined) {
                            usize::MAX
                        } else {
                            let n = integer(&limit)?;
                            if n < 1 {
                                return Err(error("$filter limit must be positive"));
                            }
                            usize::try_from(n).map_err(|_| error("invalid limit"))?
                        }
                    } else {
                        usize::MAX
                    };
                    for v in a {
                        bound.insert(name.into(), v.clone());
                        if op == "$map" {
                            out.push(expr(required(d, "in")?, doc, &bound)?);
                        } else if truth(&expr(required(d, "cond")?, doc, &bound)?) {
                            out.push(v.clone());
                            if out.len() >= limit {
                                break;
                            }
                        }
                    }
                    Ok(out.into())
                }
                "$let" => {
                    let d = object(input)?;
                    only_fields(d, &["vars", "in"])?;
                    let mut bound = vars.clone();
                    for (k, v) in object(required(d, "vars")?)? {
                        bound.insert(k.clone(), expr(v, doc, vars)?);
                    }
                    expr(required(d, "in")?, doc, &bound)
                }
                "$objectToArray" => {
                    let input = expr(input, doc, vars)?;
                    if matches!(input, Bson::Null | Bson::Undefined) {
                        return Ok(Bson::Null);
                    }
                    Ok(Bson::Array(
                        object(&input)?
                            .iter()
                            .map(|(k, v)| Bson::Document(doc! {"k":k,"v":v}))
                            .collect(),
                    ))
                }
                "$arrayToObject" => {
                    let input = expr(input, doc, vars)?;
                    if matches!(input, Bson::Null | Bson::Undefined) {
                        return Ok(Bson::Null);
                    }
                    let mut out = Document::new();
                    for v in array(&input)? {
                        let (k, v) = match v {
                            Bson::Array(pair) if pair.len() == 2 => (string(&pair[0])?, &pair[1]),
                            Bson::Document(pair) if pair.len() == 2 => {
                                (string(required(pair, "k")?)?, required(pair, "v")?)
                            }
                            _ => return Err(error("$arrayToObject requires key/value pairs")),
                        };
                        if k.contains('\0') {
                            return Err(error("invalid object key"));
                        }
                        out.insert(k, v.clone());
                    }
                    Ok(out.into())
                }
                "$mergeObjects" => {
                    let input = if let Bson::Array(a) = input {
                        a.clone()
                    } else {
                        vec![input.clone()]
                    };
                    let mut out = Document::new();
                    for value in input {
                        let value = expr(&value, doc, vars)?;
                        if matches!(value, Bson::Null | Bson::Undefined) {
                            continue;
                        }
                        out.extend(object(&value)?.clone());
                    }
                    Ok(out.into())
                }
                "$size" => {
                    let v = expr(input, doc, vars)?;
                    Ok(Bson::Int32(
                        i32::try_from(array(&v)?.len()).map_err(|_| error("array too large"))?,
                    ))
                }
                "$isArray" => Ok(matches!(expr(input, doc, vars)?, Bson::Array(_)).into()),
                "$in" => {
                    let a = args(input, 2, op)?;
                    let value = expr(&a[0], doc, vars)?;
                    let values = expr(&a[1], doc, vars)?;
                    Ok(array(&values)?.iter().any(|v| equal(v, &value)).into())
                }
                "$arrayElemAt" => {
                    let a = args(input, 2, op)?;
                    let value = expr(&a[0], doc, vars)?;
                    if matches!(value, Bson::Null | Bson::Undefined) {
                        return Ok(Bson::Null);
                    }
                    let avalues = array(&value)?;
                    let i = integer(&expr(&a[1], doc, vars)?)?;
                    let i = if i < 0 {
                        i64::try_from(avalues.len()).unwrap_or(i64::MAX) + i
                    } else {
                        i
                    };
                    Ok(usize::try_from(i)
                        .ok()
                        .and_then(|i| avalues.get(i))
                        .cloned()
                        .unwrap_or(Bson::Undefined))
                }
                "$first" | "$last" => {
                    let value = expr(input, doc, vars)?;
                    if matches!(value, Bson::Null | Bson::Undefined) {
                        return Ok(Bson::Null);
                    }
                    let a = array(&value)?;
                    Ok(if op == "$first" { a.first() } else { a.last() }
                        .cloned()
                        .unwrap_or(Bson::Undefined))
                }
                "$type" => {
                    if let Bson::String(s) = input
                        && s.starts_with('$')
                        && !s.starts_with("$$")
                        && values(doc, &s[1..]).is_empty()
                    {
                        return Ok("missing".into());
                    }
                    let value = expr(input, doc, vars)?;
                    if matches!(value, Bson::Undefined)
                        && matches!(input,Bson::String(s) if s.starts_with('$'))
                    {
                        return Ok("missing".into());
                    }
                    Ok(type_name(&value).into())
                }
                "$toInt" => {
                    let value = expr(input, doc, vars)?;
                    let n = match value {
                        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
                        Bson::Boolean(b) => i32::from(b),
                        Bson::String(s) => s
                            .parse::<i32>()
                            .map_err(|_| error("cannot convert string to int"))?,
                        _ => numeric(&value)
                            .ok_or_else(|| error("cannot convert value to int"))?
                            .with_scale(0)
                            .to_string()
                            .parse::<i32>()
                            .map_err(|_| error("int conversion overflow"))?,
                    };
                    Ok(Bson::Int32(n))
                }
                "$dateFromString" => date_from_string(object(input)?, doc, vars),
                "$dateToString" => date_to_string(object(input)?, doc, vars),
                "$toString" => {
                    let value = expr(input, doc, vars)?;
                    Ok(match value {
                        Bson::Null | Bson::Undefined => Bson::Null,
                        Bson::String(s) => s.into(),
                        Bson::Int32(n) => n.to_string().into(),
                        Bson::Int64(n) => n.to_string().into(),
                        Bson::Double(n) => n.to_string().into(),
                        Bson::Boolean(n) => n.to_string().into(),
                        Bson::ObjectId(n) => n.to_hex().into(),
                        _ => return Err(error("unsupported conversion to string")),
                    })
                }
                _ => Err(error(format!("unsupported expression: {op}"))),
            }
        }
        Bson::Document(d) => {
            let mut out = Document::new();
            for (k, v) in d {
                let v = expr(v, doc, vars)?;
                if !matches!(v, Bson::Undefined) {
                    out.insert(k, v);
                }
            }
            Ok(out.into())
        }
        _ => Ok(value.clone()),
    }
}
fn field_expression(doc: &Document, path: &str) -> Bson {
    fn descend(value: &Bson, parts: &[&str]) -> Bson {
        if parts.is_empty() {
            return value.clone();
        }
        match value {
            Bson::Document(d) => d
                .get(parts[0])
                .map(|v| descend(v, &parts[1..]))
                .unwrap_or(Bson::Undefined),
            Bson::Array(a) => Bson::Array(
                a.iter()
                    .map(|v| descend(v, parts))
                    .filter(|v| !matches!(v, Bson::Undefined))
                    .collect(),
            ),
            _ => Bson::Undefined,
        }
    }
    let parts = path.split('.').collect::<Vec<_>>();
    doc.get(parts[0])
        .map(|v| descend(v, &parts[1..]))
        .unwrap_or(Bson::Undefined)
}
fn required<'a>(doc: &'a Document, key: &str) -> Result<&'a Bson> {
    doc.get(key)
        .ok_or_else(|| error(format!("missing required field: {key}")))
}
fn only_fields(doc: &Document, allowed: &[&str]) -> Result<()> {
    for k in doc.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(error(format!("unsupported option: {k}")));
        }
    }
    Ok(())
}

pub(super) fn update(old: &Document, update: &Bson, inserting: bool) -> Result<Document> {
    validate_update(update)?;
    let mut candidate = old.clone();
    if let Bson::Array(stages) = update {
        for stage in stages {
            let d = object(stage)?;
            if d.len() != 1 {
                return Err(error("update stage must have one operator"));
            }
            let (op, value) = d.iter().next().expect("one operator");
            match op.as_str() {
                "$set" | "$addFields" => {
                    let original = candidate.clone();
                    for (k, v) in object(value)? {
                        let v = expr(v, &original, &Vars::new())?;
                        if matches!(v, Bson::Undefined) {
                            unset_path(&mut candidate, k)?
                        } else {
                            set_path(&mut candidate, k, v)?;
                        }
                    }
                }
                "$unset" => match value {
                    Bson::String(k) => unset_path(&mut candidate, k)?,
                    Bson::Array(a) => {
                        for k in a {
                            unset_path(&mut candidate, string(k)?)?
                        }
                    }
                    _ => return Err(error("$unset requires a string or array")),
                },
                "$project" => candidate = project(&candidate, object(value)?)?,
                "$replaceRoot" => {
                    let d = object(value)?;
                    only_fields(d, &["newRoot"])?;
                    candidate =
                        object(&expr(required(d, "newRoot")?, &candidate, &Vars::new())?)?.clone();
                }
                "$replaceWith" => {
                    candidate = object(&expr(value, &candidate, &Vars::new())?)?.clone()
                }
                _ => return Err(error(format!("unsupported update stage: {op}"))),
            }
        }
    } else {
        let updates = object(update)?;
        if !updates.keys().any(|k| k.starts_with('$')) {
            candidate = updates.clone();
            if !candidate.contains_key("_id")
                && let Some(id) = old.get("_id")
            {
                candidate.insert("_id", id.clone());
            }
        } else {
            for (op, changes) in updates {
                for (path, value) in object(changes)? {
                    match op.as_str() {
                        "$set" => set_path(&mut candidate, path, value.clone())?,
                        "$setOnInsert" => {
                            if inserting {
                                set_path(&mut candidate, path, value.clone())?;
                            }
                        }
                        "$unset" => unset_path(&mut candidate, path)?,
                        "$inc" | "$mul" => {
                            let n = numeric(value).ok_or_else(|| {
                                super::Error::new(14, "numeric update requires a number")
                            })?;
                            let previous = get(&candidate, path);
                            let before = if let Some(v) = previous {
                                numeric(v).ok_or_else(|| {
                                    super::Error::new(
                                        14,
                                        "cannot apply numeric update to a non-number",
                                    )
                                })?
                            } else {
                                BigDecimal::from(0)
                            };
                            let result = if op == "$inc" { before + n } else { before * n };
                            let mut operands = vec![value];
                            if let Some(v) = previous {
                                operands.push(v);
                            }
                            // Modifier arithmetic rejects signed-integer overflow;
                            // aggregation expressions instead promote to double.
                            if operands
                                .iter()
                                .all(|v| matches!(v, Bson::Int32(_) | Bson::Int64(_)))
                                && !(BigDecimal::from(i64::MIN)..=BigDecimal::from(i64::MAX))
                                    .contains(&result)
                            {
                                return Err(error("integer update overflow"));
                            }
                            let result = typed_number_result(result, &operands, false)?;
                            set_path(&mut candidate, path, result)?;
                        }
                        "$min" | "$max" => {
                            if get(&candidate, path).is_none_or(|v| {
                                if op == "$min" {
                                    compare(v, value).is_gt()
                                } else {
                                    compare(v, value).is_lt()
                                }
                            }) {
                                set_path(&mut candidate, path, value.clone())?;
                            }
                        }
                        "$push" | "$addToSet" => {
                            let mut current = match get(&candidate, path) {
                                None => vec![],
                                Some(Bson::Array(a)) => a.clone(),
                                _ => {
                                    return Err(super::Error::new(
                                        2,
                                        "array update requires an array",
                                    ));
                                }
                            };
                            let modifiers = value
                                .as_document()
                                .filter(|d| d.keys().any(|k| k.starts_with('$')));
                            let append = if let Some(d) = modifiers {
                                let allowed = if op == "$push" {
                                    &["$each", "$position", "$slice", "$sort"][..]
                                } else {
                                    &["$each"][..]
                                };
                                only_fields(d, allowed)?;
                                array(required(d, "$each")?)?.clone()
                            } else {
                                vec![value.clone()]
                            };
                            if op == "$addToSet" {
                                for v in append {
                                    if !current.iter().any(|item| equal(item, &v)) {
                                        current.push(v);
                                    }
                                }
                            } else {
                                let position = if let Some(v) =
                                    modifiers.and_then(|d| d.get("$position"))
                                {
                                    let i = integer(v)?;
                                    if i < 0 {
                                        current.len().saturating_sub(
                                            usize::try_from(i.unsigned_abs()).unwrap_or(usize::MAX),
                                        )
                                    } else {
                                        usize::try_from(i).unwrap_or(usize::MAX).min(current.len())
                                    }
                                } else {
                                    current.len()
                                };
                                current.splice(position..position, append);
                                if let Some(sort_spec) = modifiers.and_then(|d| d.get("$sort")) {
                                    if let Bson::Document(spec) = sort_spec {
                                        let mut documents = current
                                            .iter()
                                            .map(|v| object(v).cloned())
                                            .collect::<Result<Vec<_>>>()?;
                                        sort(&mut documents, spec)?;
                                        current =
                                            documents.into_iter().map(Bson::Document).collect();
                                    } else {
                                        let direction = integer(sort_spec)?;
                                        if direction != 1 && direction != -1 {
                                            return Err(error("invalid $push sort direction"));
                                        }
                                        current.sort_by(|a, b| {
                                            if direction == 1 {
                                                compare(a, b)
                                            } else {
                                                compare(b, a)
                                            }
                                        });
                                    }
                                }
                                if let Some(slice) = modifiers.and_then(|d| d.get("$slice")) {
                                    let n = integer(slice)?;
                                    if n >= 0 {
                                        current.truncate(usize::try_from(n).unwrap_or(usize::MAX));
                                    } else {
                                        let keep =
                                            usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
                                        if current.len() > keep {
                                            current.drain(..current.len() - keep);
                                        }
                                    }
                                }
                            }
                            set_path(&mut candidate, path, current.into())?;
                        }
                        "$pull" | "$pullAll" => {
                            if let Some(v) = get(&candidate, path) {
                                let a = array(v)?;
                                let mut kept = vec![];
                                for v in a {
                                    let remove = if op == "$pullAll" {
                                        array(value)?.iter().any(|wanted| equal(v, wanted))
                                    } else if let Some(filter) = value
                                        .as_document()
                                        .filter(|d| !d.keys().any(|k| k.starts_with('$')))
                                    {
                                        if let Some(d) = v.as_document() {
                                            matches(d, filter)?
                                        } else {
                                            false
                                        }
                                    } else {
                                        matches_values(&[v], value, &Vars::new())?
                                    };
                                    if !remove {
                                        kept.push(v.clone());
                                    }
                                }
                                set_path(&mut candidate, path, kept.into())?;
                            }
                        }
                        "$pop" => {
                            let n = integer(value)?;
                            if n != 1 && n != -1 {
                                return Err(error("$pop must be 1 or -1"));
                            }
                            if let Some(v) = get(&candidate, path) {
                                let mut a = array(v)?.clone();
                                if !a.is_empty() {
                                    if n == 1 {
                                        a.pop();
                                    } else {
                                        a.remove(0);
                                    }
                                }
                                set_path(&mut candidate, path, a.into())?;
                            }
                        }
                        "$rename" => {
                            let to = string(value)?;
                            if to == path
                                || to.starts_with(&format!("{path}."))
                                || path.starts_with(&format!("{to}."))
                            {
                                return Err(error("invalid rename destination"));
                            }
                            if let Some(v) = get(&candidate, path).cloned() {
                                unset_path(&mut candidate, path)?;
                                set_path(&mut candidate, to, v)?;
                            }
                        }
                        "$currentDate" => {
                            if value != &Bson::Boolean(true)
                                && value != &Bson::Document(doc! {"$type":"date"})
                            {
                                return Err(error("only date $currentDate is supported"));
                            }
                            set_path(&mut candidate, path, Bson::DateTime(bson::DateTime::now()))?;
                        }
                        _ => unreachable!("operators validated"),
                    }
                }
            }
        }
    }
    if let Some(old_id) = old.get("_id")
        && candidate.get("_id").is_none_or(|id| !equal(id, old_id))
    {
        return Err(super::Error::new(66, "immutable _id field changed"));
    }
    Ok(candidate)
}

pub(super) fn aggregate(
    docs: Vec<Document>,
    pipeline: &[Bson],
    collections: &BTreeMap<String, Vec<Document>>,
) -> Result<Vec<Document>> {
    aggregate_vars(docs, pipeline, collections, &Vars::new())
}
fn aggregate_vars(
    mut docs: Vec<Document>,
    pipeline: &[Bson],
    collections: &BTreeMap<String, Vec<Document>>,
    vars: &Vars,
) -> Result<Vec<Document>> {
    validate_pipeline(pipeline)?;
    for stage in pipeline {
        let stage = object(stage)?;
        let (op, value) = stage.iter().next().expect("stage validated");
        match op.as_str() {
            "$match" => {
                let filter = object(value)?;
                let mut next = vec![];
                for d in docs {
                    if matches_vars(&d, filter, vars)? {
                        next.push(d);
                    }
                }
                docs = next;
            }
            "$sort" => sort(&mut docs, object(value)?)?,
            "$limit" => {
                let n = integer(value)?;
                if n <= 0 {
                    return Err(error("$limit must be positive"));
                }
                docs.truncate(usize::try_from(n).unwrap_or(usize::MAX));
            }
            "$skip" => {
                let n = integer(value)?;
                if n < 0 {
                    return Err(error("$skip must be nonnegative"));
                }
                let n = usize::try_from(n).unwrap_or(usize::MAX).min(docs.len());
                docs.drain(..n);
            }
            "$project" => {
                let spec = object(value)?;
                docs = docs
                    .iter()
                    .map(|d| project_vars(d, spec, vars))
                    .collect::<Result<_>>()?;
            }
            "$set" | "$addFields" => {
                let spec = object(value)?;
                docs = docs
                    .iter()
                    .map(|d| {
                        let mut next = d.clone();
                        for (k, v) in spec {
                            let v = expr(v, d, vars)?;
                            if matches!(v, Bson::Undefined) {
                                unset_path(&mut next, k)?;
                            } else {
                                set_path(&mut next, k, v)?;
                            }
                        }
                        Ok(next)
                    })
                    .collect::<Result<_>>()?;
            }
            "$unset" => {
                let keys = match value {
                    Bson::String(s) => vec![s.as_str()],
                    Bson::Array(a) => a.iter().map(string).collect::<Result<_>>()?,
                    _ => return Err(error("$unset requires a field or array")),
                };
                for d in &mut docs {
                    for k in &keys {
                        unset_path(d, k)?;
                    }
                }
            }
            "$replaceRoot" | "$replaceWith" => {
                let replacement = if op == "$replaceRoot" {
                    let d = object(value)?;
                    only_fields(d, &["newRoot"])?;
                    required(d, "newRoot")?
                } else {
                    value
                };
                docs = docs
                    .iter()
                    .map(|d| Ok(object(&expr(replacement, d, vars)?)?.clone()))
                    .collect::<Result<_>>()?;
            }
            "$group" => {
                docs = group(docs, object(value)?, vars)?;
            }
            "$count" => {
                let name = string(value)?;
                if name.is_empty() || name.starts_with('$') || name.contains('.') {
                    return Err(error("invalid count field"));
                }
                docs = if docs.is_empty() {
                    vec![]
                } else {
                    let mut d = Document::new();
                    d.insert(
                        name,
                        number_result(BigDecimal::from(docs.len() as u64), false)?,
                    );
                    vec![d]
                };
            }
            "$lookup" => {
                let spec = object(value)?;
                only_fields(
                    spec,
                    &[
                        "from",
                        "as",
                        "localField",
                        "foreignField",
                        "let",
                        "pipeline",
                    ],
                )?;
                let from = string(required(spec, "from")?)?;
                let out = string(required(spec, "as")?)?;
                let foreign = collections.get(from).cloned().unwrap_or_default();
                let local_field = spec.get("localField").map(string).transpose()?;
                let foreign_field = spec.get("foreignField").map(string).transpose()?;
                if local_field.is_some() != foreign_field.is_some() {
                    return Err(error("$lookup requires both localField and foreignField"));
                }
                if local_field.is_none() && !spec.contains_key("pipeline") {
                    return Err(error("$lookup requires a pipeline or join fields"));
                }
                let pipeline = spec.get("pipeline").map(array).transpose()?;
                let mut next = vec![];
                for mut d in docs {
                    let mut bound = vars.clone();
                    if let Some(bindings) = spec.get("let") {
                        for (k, v) in object(bindings)? {
                            bound.insert(k.clone(), expr(v, &d, vars)?);
                        }
                    }
                    let mut joined = foreign.clone();
                    if let (Some(local), Some(foreign)) = (local_field, foreign_field) {
                        let local_values = values(&d, local);
                        let local_values = if local_values.is_empty() {
                            vec![Bson::Null]
                        } else {
                            local_values.into_iter().cloned().collect()
                        };
                        let mut matching = vec![];
                        for item in joined {
                            let mut yes = false;
                            for v in &local_values {
                                if let Bson::Array(a) = v {
                                    for element in a {
                                        yes |= matches_values(
                                            &values(&item, foreign),
                                            element,
                                            &bound,
                                        )?;
                                    }
                                } else {
                                    yes |= matches_values(&values(&item, foreign), v, &bound)?;
                                }
                            }
                            if yes {
                                matching.push(item);
                            }
                        }
                        joined = matching;
                    }
                    if let Some(pipeline) = pipeline {
                        joined = aggregate_vars(joined, pipeline, collections, &bound)?;
                    }
                    set_path(
                        &mut d,
                        out,
                        joined
                            .into_iter()
                            .map(Bson::Document)
                            .collect::<Vec<_>>()
                            .into(),
                    )?;
                    next.push(d);
                }
                docs = next;
            }
            "$unionWith" => {
                let (name, pipeline) = if let Bson::String(name) = value {
                    (name.as_str(), None)
                } else {
                    let spec = object(value)?;
                    only_fields(spec, &["coll", "pipeline"])?;
                    (
                        string(required(spec, "coll")?)?,
                        spec.get("pipeline").map(array).transpose()?,
                    )
                };
                let other = collections.get(name).cloned().unwrap_or_default();
                docs.extend(if let Some(pipeline) = pipeline {
                    aggregate_vars(other, pipeline, collections, vars)?
                } else {
                    other
                });
            }
            "$unwind" => {
                let (path, preserve, index) = if let Bson::String(path) = value {
                    (path.as_str(), false, None)
                } else {
                    let spec = object(value)?;
                    only_fields(
                        spec,
                        &["path", "preserveNullAndEmptyArrays", "includeArrayIndex"],
                    )?;
                    (
                        string(required(spec, "path")?)?,
                        spec.get_bool("preserveNullAndEmptyArrays").unwrap_or(false),
                        spec.get("includeArrayIndex").map(string).transpose()?,
                    )
                };
                let path = path
                    .strip_prefix('$')
                    .ok_or_else(|| error("$unwind path must start with $"))?;
                let mut next = vec![];
                for d in docs {
                    match get(&d, path) {
                        Some(Bson::Array(a)) if !a.is_empty() => {
                            for (i, v) in a.iter().enumerate() {
                                let mut row = d.clone();
                                set_path(&mut row, path, v.clone())?;
                                if let Some(index) = index {
                                    set_path(&mut row, index, Bson::Int64(i as i64))?;
                                }
                                next.push(row);
                            }
                        }
                        Some(Bson::Array(_)) => {
                            if preserve {
                                let mut row = d.clone();
                                unset_path(&mut row, path)?;
                                if let Some(index) = index {
                                    set_path(&mut row, index, Bson::Null)?;
                                }
                                next.push(row);
                            }
                        }
                        None | Some(Bson::Null) => {
                            if preserve {
                                let mut row = d;
                                if let Some(index) = index {
                                    set_path(&mut row, index, Bson::Null)?;
                                }
                                next.push(row);
                            }
                        }
                        _ => {
                            let mut row = d;
                            if let Some(index) = index {
                                set_path(&mut row, index, Bson::Null)?;
                            }
                            next.push(row);
                        }
                    }
                }
                docs = next;
            }
            _ => unreachable!("stage validated"),
        }
    }
    Ok(docs)
}
fn group(docs: Vec<Document>, spec: &Document, vars: &Vars) -> Result<Vec<Document>> {
    let id = required(spec, "_id")?;
    let mut groups: Vec<(Bson, Vec<Document>)> = vec![];
    for d in docs {
        let key = expr(id, &d, vars)?;
        let key = if matches!(key, Bson::Undefined) {
            Bson::Null
        } else {
            key
        };
        if let Some((_, rows)) = groups.iter_mut().find(|(k, _)| equal(k, &key)) {
            rows.push(d);
        } else {
            groups.push((key, vec![d]));
        }
    }
    let mut out = vec![];
    for (key, rows) in groups {
        let mut row = doc! {"_id":key};
        for (name, accumulator) in spec {
            if name == "_id" {
                continue;
            }
            if name.starts_with('$') || name.contains('.') {
                return Err(error("invalid group field"));
            }
            let acc = object(accumulator)?;
            if acc.len() != 1 {
                return Err(error("an accumulator must contain one operator"));
            }
            let (op, input) = acc.iter().next().expect("one accumulator");
            let values = rows
                .iter()
                .map(|d| expr(input, d, vars))
                .collect::<Result<Vec<_>>>()?;
            let result = match op.as_str() {
                "$sum" | "$avg" => {
                    let mut sum = BigDecimal::from(0);
                    let mut count = 0u64;
                    for value in &values {
                        if let Some(n) = numeric(value) {
                            sum += n;
                            count += 1;
                        }
                    }
                    if op == "$avg" {
                        if count == 0 {
                            Bson::Null
                        } else {
                            typed_number_result(
                                sum / BigDecimal::from(count),
                                &values.iter().collect::<Vec<_>>(),
                                true,
                            )?
                        }
                    } else {
                        typed_number_result(sum, &values.iter().collect::<Vec<_>>(), false)?
                    }
                }
                "$min" | "$max" => {
                    let values = values
                        .iter()
                        .filter(|v| !matches!(v, Bson::Null | Bson::Undefined));
                    if op == "$min" {
                        values.min_by(|a, b| compare(a, b))
                    } else {
                        values.max_by(|a, b| compare(a, b))
                    }
                    .cloned()
                    .unwrap_or(Bson::Null)
                }
                "$first" => values
                    .first()
                    .filter(|v| !matches!(v, Bson::Undefined))
                    .cloned()
                    .unwrap_or(Bson::Null),
                "$last" => values
                    .last()
                    .filter(|v| !matches!(v, Bson::Undefined))
                    .cloned()
                    .unwrap_or(Bson::Null),
                "$push" => values
                    .into_iter()
                    .filter(|v| !matches!(v, Bson::Undefined))
                    .collect::<Vec<_>>()
                    .into(),
                "$addToSet" => {
                    let mut unique = vec![];
                    for v in values {
                        if matches!(v, Bson::Undefined) {
                            continue;
                        }
                        if !unique.iter().any(|x| equal(x, &v)) {
                            unique.push(v);
                        }
                    }
                    unique.into()
                }
                "$count" => {
                    if !object(input)?.is_empty() {
                        return Err(error("$count accumulator requires an empty object"));
                    }
                    number_result(BigDecimal::from(rows.len() as u64), false)?
                }
                "$mergeObjects" => {
                    let mut d = Document::new();
                    for v in values {
                        if matches!(v, Bson::Null | Bson::Undefined) {
                            continue;
                        }
                        d.extend(object(&v)?.clone());
                    }
                    d.into()
                }
                _ => return Err(error(format!("unsupported accumulator: {op}"))),
            };
            row.insert(name, result);
        }
        out.push(row);
    }
    Ok(out)
}

/// Validate syntax before evaluating against data, including an empty collection.
pub(super) fn validate_filter(filter: &Document) -> Result<()> {
    for (k, v) in filter {
        match k.as_str() {
            "$and" | "$or" | "$nor" => {
                let a = array(v)?;
                if a.is_empty() {
                    return Err(error("logical operator requires a nonempty array"));
                }
                for v in a {
                    validate_filter(object(v)?)?;
                }
            }
            "$expr" => validate_expr(v)?,
            _ if k.starts_with('$') => {
                return Err(error(format!("unsupported query operator: {k}")));
            }
            _ => validate_condition(v)?,
        }
    }
    Ok(())
}
fn validate_condition(value: &Bson) -> Result<()> {
    if let Some(d) = value
        .as_document()
        .filter(|d| d.keys().any(|k| k.starts_with('$')))
    {
        for (op, v) in d {
            match op.as_str() {
                "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {}
                "$in" | "$nin" | "$all" => {
                    for item in array(v)? {
                        if item
                            .as_document()
                            .is_some_and(|d| d.keys().any(|k| k.starts_with('$')))
                        {
                            return Err(error(
                                "cannot nest query operators within membership arrays",
                            ));
                        }
                    }
                }
                "$not" => validate_condition(v)?,
                "$elemMatch" => {
                    let filter = object(v)?;
                    if filter.keys().all(|k| k.starts_with('$')) {
                        validate_condition(v)?;
                    } else {
                        validate_filter(filter)?;
                    }
                }
                "$exists" => {
                    if v.as_bool().is_none() {
                        return Err(error("$exists requires a boolean"));
                    }
                }
                "$size" => {
                    if integer(v)? < 0 {
                        return Err(error("$size must be nonnegative"));
                    }
                }
                "$regex" => {
                    let (pattern, options) = match v {
                        Bson::String(p) => (p.as_str(), d.get_str("$options").unwrap_or("")),
                        Bson::RegularExpression(r) => (r.pattern.as_str(), r.options.as_str()),
                        _ => return Err(error("invalid regex")),
                    };
                    regex_match(&Bson::Null, pattern, options)?;
                }
                "$options" => {
                    string(v)?;
                    if !d.contains_key("$regex") {
                        return Err(error("$options requires $regex"));
                    }
                }
                "$type" => {
                    query_type(v)?;
                }
                _ => return Err(error(format!("unsupported query operator: {op}"))),
            }
        }
    }
    Ok(())
}
fn validate_expr(value: &Bson) -> Result<()> {
    match value {
        Bson::Array(a) => {
            for v in a {
                validate_expr(v)?;
            }
        }
        Bson::Document(d) => {
            if let Some((op, input)) = d.iter().find(|(k, _)| k.starts_with('$')) {
                if d.len() != 1 {
                    return Err(error("expression requires exactly one operator"));
                }
                if op == "$literal" {
                    return Ok(());
                }
                if !matches!(
                    op.as_str(),
                    "$eq"
                        | "$ne"
                        | "$gt"
                        | "$gte"
                        | "$lt"
                        | "$lte"
                        | "$cmp"
                        | "$and"
                        | "$or"
                        | "$not"
                        | "$ifNull"
                        | "$cond"
                        | "$switch"
                        | "$add"
                        | "$multiply"
                        | "$subtract"
                        | "$divide"
                        | "$mod"
                        | "$concat"
                        | "$concatArrays"
                        | "$setUnion"
                        | "$map"
                        | "$filter"
                        | "$let"
                        | "$objectToArray"
                        | "$arrayToObject"
                        | "$mergeObjects"
                        | "$size"
                        | "$isArray"
                        | "$in"
                        | "$arrayElemAt"
                        | "$first"
                        | "$last"
                        | "$type"
                        | "$toString"
                        | "$toInt"
                        | "$dateFromString"
                        | "$dateToString"
                ) {
                    return Err(error(format!("unsupported expression: {op}")));
                }
                match op.as_str() {
                    "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$cmp" | "$subtract"
                    | "$divide" | "$mod" | "$in" | "$arrayElemAt" => {
                        args(input, 2, op)?;
                    }
                    "$not" => {
                        args(input, 1, op)?;
                    }
                    "$and" | "$or" | "$add" | "$multiply" | "$concat" | "$concatArrays"
                    | "$setUnion" => {
                        array(input)?;
                    }
                    "$ifNull" => {
                        if array(input)?.len() < 2 {
                            return Err(error("$ifNull requires at least two arguments"));
                        }
                    }
                    "$map" | "$filter" => {
                        let d = object(input)?;
                        only_fields(
                            d,
                            if op == "$map" {
                                &["input", "as", "in"]
                            } else {
                                &["input", "as", "cond", "limit"]
                            },
                        )?;
                        required(d, "input")?;
                        required(d, if op == "$map" { "in" } else { "cond" })?;
                        if let Some(name) = d.get("as") {
                            string(name)?;
                        }
                    }
                    "$cond" => {
                        if matches!(input, Bson::Array(_)) {
                            args(input, 3, op)?;
                        } else {
                            let d = object(input)?;
                            only_fields(d, &["if", "then", "else"])?;
                            required(d, "if")?;
                            required(d, "then")?;
                            required(d, "else")?;
                        }
                    }
                    "$let" => {
                        let d = object(input)?;
                        only_fields(d, &["vars", "in"])?;
                        object(required(d, "vars")?)?;
                        required(d, "in")?;
                    }
                    "$switch" => {
                        let d = object(input)?;
                        only_fields(d, &["branches", "default"])?;
                        for branch in array(required(d, "branches")?)? {
                            let branch = object(branch)?;
                            only_fields(branch, &["case", "then"])?;
                            required(branch, "case")?;
                            required(branch, "then")?;
                        }
                    }
                    "$dateFromString" | "$dateToString" => {
                        let d = object(input)?;
                        only_fields(
                            d,
                            if op == "$dateFromString" {
                                &["dateString", "format", "timezone", "onError", "onNull"]
                            } else {
                                &["date", "format", "timezone", "onNull"]
                            },
                        )?;
                        required(
                            d,
                            if op == "$dateFromString" {
                                "dateString"
                            } else {
                                "date"
                            },
                        )?;
                    }
                    _ => {}
                }
                validate_expr(input)?;
            } else {
                for (_, v) in d {
                    validate_expr(v)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
fn validate_pipeline(pipeline: &[Bson]) -> Result<()> {
    for stage in pipeline {
        let d = object(stage)?;
        if d.len() != 1 {
            return Err(error("pipeline stage requires exactly one operator"));
        }
        let (op, value) = d.iter().next().expect("one stage");
        match op.as_str() {
            "$match" => validate_filter(object(value)?)?,
            "$sort" => {
                sort(&mut [], object(value)?)?;
            }
            "$limit" => {
                if integer(value)? <= 0 {
                    return Err(error("$limit must be positive"));
                }
            }
            "$skip" => {
                if integer(value)? < 0 {
                    return Err(error("$skip must be nonnegative"));
                }
            }
            "$project" => {
                let d = object(value)?;
                if d.is_empty() {
                    return Err(error("$project requires at least one field"));
                }
                validate_projection(d)?;
            }
            "$set" | "$addFields" => {
                nonconflicting_paths(object(value)?.keys().map(String::as_str))?;
                for (_, v) in object(value)? {
                    validate_expr(v)?;
                }
            }
            "$replaceRoot" => {
                let d = object(value)?;
                only_fields(d, &["newRoot"])?;
                validate_expr(required(d, "newRoot")?)?;
            }
            "$replaceWith" => validate_expr(value)?,
            "$group" => {
                let d = object(value)?;
                validate_expr(required(d, "_id")?)?;
                for (k, v) in d {
                    if k == "_id" {
                        continue;
                    }
                    if k.starts_with('$') || k.contains('.') {
                        return Err(error("invalid group field"));
                    }
                    let acc = object(v)?;
                    if acc.len() != 1 {
                        return Err(error("accumulator requires exactly one operator"));
                    }
                    let (op, input) = acc.iter().next().expect("one accumulator");
                    if !matches!(
                        op.as_str(),
                        "$sum"
                            | "$avg"
                            | "$min"
                            | "$max"
                            | "$first"
                            | "$last"
                            | "$push"
                            | "$addToSet"
                            | "$count"
                            | "$mergeObjects"
                    ) {
                        return Err(error(format!("unsupported accumulator: {op}")));
                    }
                    if op == "$count" && !object(input)?.is_empty() {
                        return Err(error("$count accumulator requires an empty object"));
                    }
                    validate_expr(input)?;
                }
            }
            "$lookup" => {
                let d = object(value)?;
                only_fields(
                    d,
                    &[
                        "from",
                        "as",
                        "localField",
                        "foreignField",
                        "let",
                        "pipeline",
                    ],
                )?;
                string(required(d, "from")?)?;
                string(required(d, "as")?)?;
                if let Some(p) = d.get("pipeline") {
                    validate_pipeline(array(p)?)?;
                }
                if let Some(bindings) = d.get("let") {
                    for (_, v) in object(bindings)? {
                        validate_expr(v)?;
                    }
                }
            }
            "$unionWith" => {
                if !matches!(value, Bson::String(_)) {
                    let d = object(value)?;
                    only_fields(d, &["coll", "pipeline"])?;
                    string(required(d, "coll")?)?;
                    if let Some(p) = d.get("pipeline") {
                        validate_pipeline(array(p)?)?;
                    }
                }
            }
            "$count" => {
                string(value)?;
            }
            "$unset" => match value {
                Bson::String(_) => {}
                Bson::Array(a) => {
                    for v in a {
                        string(v)?;
                    }
                }
                _ => return Err(error("$unset requires a field or array")),
            },
            "$unwind" => {
                if !matches!(value, Bson::String(_)) {
                    let d = object(value)?;
                    only_fields(
                        d,
                        &["path", "preserveNullAndEmptyArrays", "includeArrayIndex"],
                    )?;
                    string(required(d, "path")?)?;
                    if let Some(v) = d.get("preserveNullAndEmptyArrays")
                        && v.as_bool().is_none()
                    {
                        return Err(error("preserveNullAndEmptyArrays must be boolean"));
                    }
                    if let Some(v) = d.get("includeArrayIndex") {
                        valid_path(string(v)?)?;
                    }
                }
            }
            _ => return Err(error(format!("unsupported pipeline stage: {op}"))),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn null_arrays_and_numeric_queries() {
        let missing = doc! {};
        let null = doc! {"v":Bson::Null};
        let array = doc! {"v":[1,3,doc!{"n":5}]};
        assert!(matches(&missing, &doc! {"v":Bson::Null}).unwrap());
        assert!(!matches(&missing, &doc! {"v":{"$ne":Bson::Null}}).unwrap());
        assert!(matches(&null, &doc! {"v":{"$exists":true}}).unwrap());
        assert!(matches(&array, &doc! {"v":3_i64}).unwrap());
        assert!(matches(&array, &doc! {"v.n":{"$gte":5}}).unwrap());
        assert!(!equal(
            &Bson::Int64(9_007_199_254_740_993),
            &Bson::Double(9_007_199_254_740_992.0)
        ));
    }
    #[test]
    fn update_failure_preserves_input_and_pipeline_uses_old_fields() {
        let original = doc! {"_id":1,"a":2,"b":3,"items":[1]};
        assert!(
            update(
                &original,
                &doc! {"$set":{"a":10},"$inc":{"items":1}}.into(),
                false
            )
            .is_err()
        );
        assert_eq!(original.get_i32("a").unwrap(), 2);
        let updated = update(
            &original,
            &Bson::Array(vec![Bson::Document(doc! {"$set":{"a":"$b","b":"$a"}})]),
            false,
        )
        .unwrap();
        assert_eq!(updated.get_i32("a").unwrap(), 3);
        assert_eq!(updated.get_i32("b").unwrap(), 2);
    }
    #[test]
    fn correlated_lookup_group_and_object_expressions() {
        let rows = vec![doc! {"_id":1,"key":"x","data":{"a":2,"b":4}}];
        let collections = BTreeMap::from([(
            "other".into(),
            vec![doc! {"key":"x","amount":3}, doc! {"key":"z","amount":8}],
        )]);
        let pipeline=vec![doc!{"$lookup":{"from":"other","let":{"k":"$key"},"pipeline":[{"$match":{"$expr":{"$eq":["$key","$$k"]}}},{"$group":{"_id":Bson::Null,"total":{"$sum":"$amount"}}}],"as":"joined"}}.into(),doc!{"$project":{"_id":0,"joined":1,"mapped":{"$arrayToObject":{"$map":{"input":{"$objectToArray":"$data"},"as":"pair","in":{"k":"$$pair.k","v":{"$multiply":["$$pair.v",2]}}}}}}}.into()];
        let out = aggregate(rows, &pipeline, &collections).unwrap();
        assert_eq!(out[0].get_document("mapped").unwrap(), &doc! {"a":4,"b":8});
        assert_eq!(
            out[0].get_array("joined").unwrap(),
            &vec![Bson::Document(doc! {"_id":Bson::Null,"total":3})]
        );
    }
}

fn timezone(value: Option<Bson>) -> Result<time::UtcOffset> {
    let Some(value) = value else {
        return Ok(time::UtcOffset::UTC);
    };
    let zone = string(&value)?;
    if matches!(zone, "UTC" | "GMT" | "Z" | "Etc/UTC") {
        return Ok(time::UtcOffset::UTC);
    }
    let sign = match zone.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Err(error("only UTC and numeric timezone offsets are supported")),
    };
    let digits = zone[1..].replace(':', "");
    if digits.len() != 2 && digits.len() != 4 {
        return Err(error("invalid timezone offset"));
    }
    let hours = digits[..2]
        .parse::<i8>()
        .map_err(|_| error("invalid timezone offset"))?;
    let minutes = if digits.len() == 4 {
        digits[2..]
            .parse::<i8>()
            .map_err(|_| error("invalid timezone offset"))?
    } else {
        0
    };
    if hours > 23 || minutes > 59 {
        return Err(error("invalid timezone offset"));
    }
    time::UtcOffset::from_hms(sign * hours, sign * minutes, 0)
        .map_err(|_| error("invalid timezone offset"))
}
fn date_format(format: &str) -> Result<String> {
    let mut out = String::new();
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            if c == '[' {
                out.push_str("[[")
            } else {
                out.push(c)
            }
            continue;
        }
        out.push_str(
            match chars
                .next()
                .ok_or_else(|| error("incomplete date format"))?
            {
                '%' => "%",
                'Y' => "[year]",
                'm' => "[month]",
                'd' => "[day]",
                'H' => "[hour]",
                'M' => "[minute]",
                'S' => "[second]",
                'L' => "[subsecond digits:3]",
                'z' => "[offset_hour sign:mandatory][offset_minute]",
                _ => return Err(error("unsupported date format specifier")),
            },
        );
    }
    Ok(out)
}
fn date_from_string(spec: &Document, doc: &Document, vars: &Vars) -> Result<Bson> {
    only_fields(
        spec,
        &["dateString", "format", "timezone", "onError", "onNull"],
    )?;
    let input = expr(required(spec, "dateString")?, doc, vars)?;
    if matches!(input, Bson::Null | Bson::Undefined) {
        return spec
            .get("onNull")
            .map(|v| expr(v, doc, vars))
            .unwrap_or(Ok(Bson::Null));
    }
    let parse = || -> Result<Bson> {
        let text = string(&input)?;
        let zone = timezone(
            spec.get("timezone")
                .map(|v| expr(v, doc, vars))
                .transpose()?,
        )?;
        let parsed = if let Some(format) = spec.get("format") {
            let format = expr(format, doc, vars)?;
            let format = string(&format)?;
            let translated = date_format(format)?;
            let items = time::format_description::parse_borrowed::<2>(&translated)
                .map_err(|_| error("invalid date format"))?;
            if format.contains("%z") {
                if spec.contains_key("timezone") {
                    return Err(error("dateString timezone conflicts with timezone option"));
                }
                time::OffsetDateTime::parse(text, &items)
                    .map_err(|_| error("cannot parse date string"))?
            } else {
                time::PrimitiveDateTime::parse(text, &items)
                    .or_else(|_| time::Date::parse(text, &items).map(time::Date::midnight))
                    .map_err(|_| error("cannot parse date string"))?
                    .assume_offset(zone)
            }
        } else if let Ok(parsed) =
            time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        {
            if spec.contains_key("timezone") {
                return Err(error("dateString timezone conflicts with timezone option"));
            }
            parsed
        } else {
            let format = time::format_description::parse_borrowed::<2>("[year]-[month]-[day]")
                .expect("static date format");
            if let Ok(date) = time::Date::parse(text, &format) {
                date.midnight().assume_offset(zone)
            } else {
                let format = time::format_description::parse_borrowed::<2>(
                    "[year]-[month]-[day]T[hour]:[minute]:[second]",
                )
                .expect("static date format");
                time::PrimitiveDateTime::parse(text, &format)
                    .map_err(|_| error("cannot parse date string"))?
                    .assume_offset(zone)
            }
        };
        let millis = parsed.unix_timestamp_nanos() / 1_000_000;
        Ok(Bson::DateTime(bson::DateTime::from_millis(
            i64::try_from(millis).map_err(|_| error("date overflow"))?,
        )))
    };
    match parse() {
        Ok(v) => Ok(v),
        Err(e) => spec
            .get("onError")
            .map(|v| expr(v, doc, vars))
            .unwrap_or(Err(e)),
    }
}
fn date_to_string(spec: &Document, doc: &Document, vars: &Vars) -> Result<Bson> {
    only_fields(spec, &["date", "format", "timezone", "onNull"])?;
    let input = expr(required(spec, "date")?, doc, vars)?;
    if matches!(input, Bson::Null | Bson::Undefined) {
        return spec
            .get("onNull")
            .map(|v| expr(v, doc, vars))
            .unwrap_or(Ok(Bson::Null));
    }
    let millis = match input {
        Bson::DateTime(d) => d.timestamp_millis(),
        Bson::Timestamp(t) => i64::from(t.time) * 1000,
        Bson::ObjectId(id) => id.timestamp().timestamp_millis(),
        _ => return Err(error("$dateToString requires a date")),
    };
    let zone = timezone(
        spec.get("timezone")
            .map(|v| expr(v, doc, vars))
            .transpose()?,
    )?;
    let date = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .map_err(|_| error("date out of supported range"))?
        .to_offset(zone);
    let format = spec.get("format").map(|v| expr(v, doc, vars)).transpose()?;
    let default = if zone.is_utc() {
        "%Y-%m-%dT%H:%M:%S.%LZ"
    } else {
        "%Y-%m-%dT%H:%M:%S.%L"
    };
    let format = format.as_ref().map(string).transpose()?.unwrap_or(default);
    let translated = date_format(format)?;
    let items = time::format_description::parse_borrowed::<2>(&translated)
        .map_err(|_| error("invalid date format"))?;
    Ok(date
        .format(&items)
        .map_err(|_| error("cannot format date"))?
        .into())
}

fn projection_flag(value: &Bson) -> Option<bool> {
    match value {
        Bson::Boolean(b) => Some(*b),
        v => numeric(v).map(|n| n != 0),
    }
}
fn projection_remove(doc: &mut Document, parts: &[&str]) {
    if parts.len() == 1 {
        doc.remove(parts[0]);
        return;
    }
    if let Some(v) = doc.get_mut(parts[0]) {
        fn descend(value: &mut Bson, parts: &[&str]) {
            match value {
                Bson::Document(d) => projection_remove(d, parts),
                Bson::Array(a) => {
                    for v in a {
                        descend(v, parts)
                    }
                }
                _ => {}
            }
        }
        descend(v, &parts[1..]);
    }
}
fn include_path(target: &mut Document, source: &Document, parts: &[&str]) {
    let Some(value) = source.get(parts[0]) else {
        return;
    };
    if parts.len() == 1 {
        target.insert(parts[0], value.clone());
        return;
    }
    fn descend(target: &mut Bson, source: &Bson, parts: &[&str]) {
        match source {
            Bson::Document(source) => {
                if !matches!(target, Bson::Document(_)) {
                    *target = Document::new().into();
                }
                include_path(
                    target.as_document_mut().expect("document initialized"),
                    source,
                    parts,
                );
            }
            Bson::Array(source) => {
                if !matches!(target, Bson::Array(_)) {
                    *target = Bson::Array(vec![]);
                }
                let target = target.as_array_mut().expect("array initialized");
                target.resize(source.len(), Bson::Document(Document::new()));
                for (i, v) in source.iter().enumerate() {
                    descend(&mut target[i], v, parts);
                }
            }
            _ => {}
        }
    }
    if !target.contains_key(parts[0]) {
        target.insert(parts[0], Bson::Null);
    }
    descend(
        target.get_mut(parts[0]).expect("field inserted"),
        value,
        &parts[1..],
    );
}
fn valid_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path
            .split('.')
            .any(|part| part.is_empty() || part.starts_with('$') || part.contains('\0'))
    {
        Err(error("unsupported or invalid field path"))
    } else {
        Ok(())
    }
}
fn nonconflicting_paths<'a>(paths: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut previous: Vec<&str> = vec![];
    for path in paths {
        valid_path(path)?;
        for p in &previous {
            if path == *p
                || path.starts_with(&format!("{p}."))
                || p.starts_with(&format!("{path}."))
            {
                return Err(super::Error::new(40, "conflicting paths"));
            }
        }
        previous.push(path);
    }
    Ok(())
}
pub(super) fn validate_projection(spec: &Document) -> Result<()> {
    nonconflicting_paths(spec.keys().map(String::as_str))?;
    let mut include = false;
    let mut exclude = false;
    for (k, v) in spec {
        if k != "_id" {
            if projection_flag(v) == Some(false) {
                exclude = true
            } else {
                include = true
            }
        }
        if projection_flag(v).is_none() {
            validate_expr(v)?;
        }
    }
    if include && exclude {
        return Err(error("cannot mix inclusion and exclusion projection"));
    }
    Ok(())
}
pub(super) fn validate_update(value: &Bson) -> Result<()> {
    if let Bson::Array(stages) = value {
        for stage in stages {
            let d = object(stage)?;
            if d.len() != 1 {
                return Err(error("update stage requires one operator"));
            }
            let (op, value) = d.iter().next().expect("one update stage");
            match op.as_str() {
                "$set" | "$addFields" => {
                    let d = object(value)?;
                    nonconflicting_paths(d.keys().map(String::as_str))?;
                    for (_, v) in d {
                        validate_expr(v)?;
                    }
                }
                "$unset" => match value {
                    Bson::String(path) => valid_path(path)?,
                    Bson::Array(a) => {
                        for p in a {
                            valid_path(string(p)?)?
                        }
                    }
                    _ => return Err(error("$unset requires a field or array")),
                },
                "$project" => validate_projection(object(value)?)?,
                "$replaceRoot" => {
                    let d = object(value)?;
                    only_fields(d, &["newRoot"])?;
                    validate_expr(required(d, "newRoot")?)?;
                }
                "$replaceWith" => validate_expr(value)?,
                _ => return Err(error(format!("unsupported update stage: {op}"))),
            }
        }
        return Ok(());
    }
    let updates = object(value)?;
    if !updates.keys().any(|k| k.starts_with('$')) {
        return Ok(());
    }
    let mut paths = vec![];
    for (op, value) in updates {
        let changes = object(value)?;
        for (path, v) in changes {
            paths.push(path.as_str());
            match op.as_str() {
                "$set" | "$unset" | "$setOnInsert" | "$min" | "$max" => {}
                "$inc" | "$mul" => {
                    if numeric(v).is_none() {
                        return Err(super::Error::new(14, "numeric update requires a number"));
                    }
                }
                "$push" | "$addToSet" => {
                    if let Some(d) = v
                        .as_document()
                        .filter(|d| d.keys().any(|k| k.starts_with('$')))
                    {
                        let allowed = if op == "$push" {
                            &["$each", "$position", "$slice", "$sort"][..]
                        } else {
                            &["$each"][..]
                        };
                        only_fields(d, allowed)?;
                        array(required(d, "$each")?)?;
                        if let Some(v) = d.get("$position") {
                            integer(v)?;
                        }
                        if let Some(v) = d.get("$slice") {
                            integer(v)?;
                        }
                        if let Some(v) = d.get("$sort") {
                            if let Bson::Document(d) = v {
                                sort(&mut [], d)?;
                            } else {
                                let n = integer(v)?;
                                if n != 1 && n != -1 {
                                    return Err(error("invalid $push sort direction"));
                                }
                            }
                        }
                    }
                }
                "$pull" => {
                    if v.as_document()
                        .is_some_and(|d| d.keys().any(|k| k.starts_with('$')))
                    {
                        validate_condition(v)?;
                    } else if let Some(d) = v.as_document() {
                        validate_filter(d)?;
                    }
                }
                "$pullAll" => {
                    array(v)?;
                }
                "$pop" => {
                    let n = integer(v)?;
                    if n != 1 && n != -1 {
                        return Err(error("$pop must be 1 or -1"));
                    }
                }
                "$rename" => {
                    let to = string(v)?;
                    valid_path(to)?;
                    paths.push(to);
                }
                "$currentDate" => {
                    if v != &Bson::Boolean(true) && v != &Bson::Document(doc! {"$type":"date"}) {
                        return Err(error("only date $currentDate is supported"));
                    }
                }
                _ => return Err(error(format!("unsupported update operator: {op}"))),
            }
        }
        if changes.is_empty()
            && !matches!(
                op.as_str(),
                "$set"
                    | "$unset"
                    | "$setOnInsert"
                    | "$min"
                    | "$max"
                    | "$inc"
                    | "$mul"
                    | "$push"
                    | "$addToSet"
                    | "$pull"
                    | "$pullAll"
                    | "$pop"
                    | "$rename"
                    | "$currentDate"
            )
        {
            return Err(error(format!("unsupported update operator: {op}")));
        }
    }
    nonconflicting_paths(paths.into_iter())
}

fn literal_condition(actual: &[&Bson], expected: &Bson) -> Result<bool> {
    if actual.is_empty() {
        return Ok(matches!(expected, Bson::Null));
    }
    for value in actual {
        if value_eq(value, expected)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    #[test]
    fn dates_offsets_conversion_and_nested_projection() {
        let input = doc! {"_id":1,"items":[{"x":1,"secret":2},{"x":3,"secret":4}],"date":"2025-01-01T10:30:00+10:30"};
        let projected = project(&input, &doc! {"_id":0,"items.x":1}).unwrap();
        assert_eq!(projected, doc! {"items":[{"x":1},{"x":3}]});
        let hidden = project(&input, &doc! {"items.secret":0}).unwrap();
        assert_eq!(
            hidden.get_array("items").unwrap(),
            &vec![Bson::Document(doc! {"x":1}), Bson::Document(doc! {"x":3})]
        );
        let date = expr(
            &doc! {"$dateFromString":{"dateString":"$date"}}.into(),
            &input,
            &Vars::new(),
        )
        .unwrap();
        assert_eq!(
            date,
            Bson::DateTime(bson::DateTime::from_millis(1_735_689_600_000))
        );
        assert_eq!(
            expr(
                &doc! {"$dateToString":{"date":date,"format":"%Y-%m-%d %H:%M","timezone":"-05:00"}}
                    .into(),
                &input,
                &Vars::new()
            )
            .unwrap(),
            Bson::String("2024-12-31 19:00".into())
        );
        assert_eq!(
            expr(&doc! {"$toInt":-2.9}.into(), &input, &Vars::new()).unwrap(),
            Bson::Int32(-2)
        );
        assert!(
            expr(
                &doc! {"$dateFromString":{"dateString":"2025-01-01","timezone":"Australia/Sydney"}}
                    .into(),
                &input,
                &Vars::new()
            )
            .is_err()
        );
    }
    #[test]
    fn unsupported_syntax_rejected_without_rows_or_matches() {
        assert!(
            aggregate(
                vec![],
                &[doc! {"$project":{"v":{"$map":{"input":[],"in":1,"surprise":true}}}}.into()],
                &BTreeMap::new()
            )
            .is_err()
        );
        assert!(
            aggregate(
                vec![],
                &[doc! {"$project":{"v":{"$eq":[1]}}}.into()],
                &BTreeMap::new()
            )
            .is_err()
        );
        assert!(validate_update(&doc! {"$unknown":{}}.into()).is_err());
        assert!(validate_update(&doc! {"$push":{"a":{"$each":[],"surprise":1}}}.into()).is_err());
        assert!(validate_update(&doc! {"$set":{"a":1},"$inc":{"a.x":1}}.into()).is_err());
        assert!(validate_projection(&doc! {"a":1,"a.x":1}).is_err());
    }
    #[test]
    fn numeric_types_and_immutable_fields_survive_updates() {
        let source = doc! {"_id":1,"n":2_i64};
        let updated = update(&source, &doc! {"$inc":{"n":1}}.into(), false).unwrap();
        assert_eq!(updated.get("n"), Some(&Bson::Int64(3)));
        assert!(
            update(
                &source,
                &vec![Bson::Document(doc! {"$unset":"_id"})].into(),
                false
            )
            .is_err()
        );
        assert!(compare(&Bson::Double(f64::NAN), &Bson::Int32(0)).is_lt());
        assert!(equal(
            &Bson::Double(f64::INFINITY),
            &Bson::Double(f64::INFINITY)
        ));
    }
    #[test]
    fn missing_projection_fields_remain_missing_and_array_fields_remain_arrays() {
        let source = doc! {"items":[{"v":2}]};
        let projected=project(&source,&doc!{"_id":0,"v":"$missing","kind":{"$type":"$missing"},"values":["$missing"],"nested":{"absent":"$missing"},"path":"$items.v"}).unwrap();
        assert_eq!(
            projected,
            doc! {"kind":"missing","values":[Bson::Null],"nested":{},"path":[2]}
        );
        assert!(!matches(&source, &doc! {"$expr":{"$eq":["$missing",Bson::Null]}}).unwrap());
    }
}
