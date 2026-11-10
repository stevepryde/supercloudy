use super::{Point, Result, bad, fields, object, point_key};
use serde_json::{Map, Value};

fn path_values<'a>(payload: &'a Map<String, Value>, path: &str) -> Vec<&'a Value> {
    fn descend<'a>(map: &'a Map<String, Value>, parts: &[&str], out: &mut Vec<&'a Value>) {
        let part = parts[0];
        let key = part.strip_suffix("[]").unwrap_or(part);
        let Some(value) = map.get(key) else {
            return;
        };
        let targets: Vec<&Value> = if part.ends_with("[]") {
            value
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default()
        } else {
            vec![value]
        };
        for value in targets {
            if parts.len() == 1 {
                out.push(value);
            } else if let Some(map) = value.as_object() {
                descend(map, &parts[1..], out);
            }
        }
    }
    let mut out = Vec::new();
    let parts = path.split('.').collect::<Vec<_>>();
    descend(payload, &parts, &mut out);
    out
}
pub(super) fn values<'a>(payload: &'a Map<String, Value>, path: &str) -> Vec<&'a Value> {
    path_values(payload, path)
        .into_iter()
        .flat_map(|value| {
            value
                .as_array()
                .map_or_else(|| vec![value], |a| a.iter().collect())
        })
        .collect()
}
fn conditions(value: &Value) -> Result<Vec<&Value>> {
    if let Some(a) = value.as_array() {
        Ok(a.iter().collect())
    } else if value.is_object() {
        Ok(vec![value])
    } else {
        Err(bad("filter clause must be an object or array"))
    }
}
pub(super) fn validate(value: &Value) -> Result<()> {
    validate_depth(value, 0)
}
fn validate_depth(value: &Value, depth: usize) -> Result<()> {
    if depth > 32 {
        return Err(bad("filter nesting exceeds 32"));
    }
    fields(value, &["must", "must_not", "should", "min_should"])?;
    for key in ["must", "must_not", "should"] {
        if let Some(v) = value.get(key) {
            for c in conditions(v)? {
                validate_condition(c, depth + 1)?;
            }
        }
    }
    if let Some(v) = value.get("min_should") {
        fields(v, &["conditions", "min_count"])?;
        if v["min_count"].as_u64().is_none() {
            return Err(bad("invalid min_should count"));
        }
        for c in v["conditions"]
            .as_array()
            .ok_or_else(|| bad("min_should conditions required"))?
        {
            validate_condition(c, depth + 1)?;
        }
    }
    Ok(())
}
fn validate_condition(v: &Value, depth: usize) -> Result<()> {
    let o = object(v)?;
    if ["must", "must_not", "should", "min_should"]
        .iter()
        .any(|k| o.contains_key(*k))
    {
        return validate_depth(v, depth);
    }
    if let Some(ids) = v.get("has_id") {
        fields(v, &["has_id"])?;
        for id in ids
            .as_array()
            .ok_or_else(|| bad("has_id must be an array"))?
        {
            point_key(id)?;
        }
        return Ok(());
    }
    if let Some(name) = v.get("has_vector") {
        fields(v, &["has_vector"])?;
        if !name.is_string() {
            return Err(bad("has_vector must be a string"));
        }
        return Ok(());
    }
    for key in ["is_empty", "is_null"] {
        if let Some(field) = v.get(key) {
            fields(v, &[key])?;
            fields(field, &["key"])?;
            if !field["key"].is_string() {
                return Err(bad("condition key required"));
            }
            return Ok(());
        }
    }
    fields(v, &["key", "match", "range", "values_count"])?;
    if v["key"].as_str().is_none_or(str::is_empty) {
        return Err(bad("condition key required"));
    }
    if ["match", "range", "values_count"]
        .iter()
        .filter(|k| v.get(**k).is_some())
        .count()
        != 1
    {
        return Err(bad("exactly one field condition required"));
    }
    if let Some(m) = v.get("match") {
        fields(m, &["value", "any", "except"])?;
        if object(m)?.len() != 1 {
            return Err(bad("invalid match condition"));
        }
        for (k, v) in object(m)? {
            if k == "value" {
                if !(v.is_boolean() || v.is_string() || v.as_i64().is_some()) {
                    return Err(bad("match requires keyword, integer or boolean"));
                }
            } else {
                let a = v
                    .as_array()
                    .ok_or_else(|| bad("match any/except requires an array"))?;
                if !(a.iter().all(Value::is_string) || a.iter().all(|v| v.as_i64().is_some())) {
                    return Err(bad("match any/except requires keywords or integers"));
                }
            }
        }
    }
    for key in ["range", "values_count"] {
        if let Some(r) = v.get(key) {
            fields(r, &["gt", "gte", "lt", "lte"])?;
            if object(r)?.values().any(|v| v.as_f64().is_none()) {
                return Err(bad("range bounds must be numbers"));
            }
        }
    }
    Ok(())
}
pub(super) fn matches(f: &Value, p: &Point) -> bool {
    for key in ["must", "must_not", "should"] {
        if let Some(v) = f.get(key) {
            let conditions = conditions(v).unwrap_or_default();
            let count = conditions.iter().filter(|v| condition(v, p)).count();
            if (key == "must" && count != conditions.len())
                || (key == "must_not" && count != 0)
                || (key == "should" && !conditions.is_empty() && count == 0)
            {
                return false;
            }
        }
    }
    if let Some(v) = f.get("min_should") {
        let count = v["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| condition(v, p))
            .count();
        if count < (v["min_count"].as_u64().unwrap() as usize) {
            return false;
        }
    }
    true
}
fn range(r: &Value, n: f64) -> bool {
    r.as_object().unwrap().iter().all(|(op, v)| {
        let v = v.as_f64().unwrap();
        match op.as_str() {
            "gt" => n > v,
            "gte" => n >= v,
            "lt" => n < v,
            "lte" => n <= v,
            _ => false,
        }
    })
}
fn condition(c: &Value, p: &Point) -> bool {
    if c.get("key").is_some() {
        let vals = values(&p.payload, c["key"].as_str().unwrap());
        if let Some(m) = c.get("match") {
            return vals.into_iter().any(|v| {
                if let Some(w) = m.get("value") {
                    v == w
                } else if let Some(any) = m.get("any") {
                    any.as_array().unwrap().contains(v)
                } else {
                    !v.is_null() && !m["except"].as_array().unwrap().contains(v)
                }
            });
        }
        if let Some(r) = c.get("range") {
            return vals.iter().any(|v| v.as_f64().is_some_and(|n| range(r, n)));
        }
        return path_values(&p.payload, c["key"].as_str().unwrap())
            .into_iter()
            .any(|value| {
                let count = match value {
                    Value::Null => 0,
                    Value::Array(values) => values.len(),
                    _ => 1,
                };
                range(&c["values_count"], count as f64)
            });
    }
    if let Some(ids) = c.get("has_id") {
        return ids
            .as_array()
            .unwrap()
            .iter()
            .any(|id| point_key(id).ok() == point_key(&p.id).ok());
    }
    if let Some(name) = c["has_vector"].as_str() {
        return p.vectors.contains_key(name);
    }
    if let Some(v) = c.get("is_empty") {
        let vals = path_values(&p.payload, v["key"].as_str().unwrap());
        return vals.is_empty()
            || vals
                .iter()
                .all(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty));
    }
    if let Some(v) = c.get("is_null") {
        return path_values(&p.payload, v["key"].as_str().unwrap())
            .iter()
            .any(|v| v.is_null());
    }
    matches(c, p)
}
