use super::{Collection, Result, bad, fields, invalid, object};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone)]
pub(super) enum Vector {
    Dense(Vec<f32>),
    Sparse { indices: Vec<u32>, values: Vec<f32> },
}
impl Vector {
    pub(super) fn json(&self) -> Value {
        match self {
            Self::Dense(v) => json!(v),
            Self::Sparse { indices, values } => json!({"indices":indices,"values":values}),
        }
    }
}
pub(super) fn validate_schema(b: &Value) -> Result<()> {
    if let Some(v) = b.get("vectors") {
        let o = object(v)?;
        if o.contains_key("size") {
            validate_dense(v)?;
        } else {
            for config in o.values() {
                validate_dense(config)?;
            }
        }
    }
    if let Some(v) = b.get("sparse_vectors") {
        for config in object(v)?.values() {
            fields(config, &["modifier", "index"])?;
            if config
                .get("modifier")
                .is_some_and(|v| v != "idf" && v != "none")
            {
                return Err(bad("unsupported sparse modifier"));
            }
            if let Some(i) = config.get("index") {
                fields(i, &["on_disk", "full_scan_threshold", "datatype"])?;
                if i.get("on_disk").is_some_and(|v| !v.is_boolean()) {
                    return Err(bad("on_disk must be boolean"));
                }
                if i.get("datatype").is_some_and(|v| v != "float32") {
                    return Err(bad("only float32 sparse vectors are supported"));
                }
            }
        }
    }
    if let (Some(d), Some(s)) = (
        b.get("vectors").and_then(Value::as_object),
        b.get("sparse_vectors").and_then(Value::as_object),
    ) && d.keys().any(|k| s.contains_key(k))
    {
        return Err(bad("dense and sparse vector names conflict"));
    }
    Ok(())
}
fn validate_dense(v: &Value) -> Result<()> {
    fields(
        v,
        &["size", "distance", "on_disk", "datatype", "hnsw_config"],
    )?;
    if v["size"].as_u64().is_none_or(|n| n == 0 || n > 65536) {
        return Err(bad("vector size must be in 1..65536"));
    }
    if !["Cosine", "Dot"].iter().any(|s| v["distance"] == *s) {
        return Err(bad("invalid vector distance"));
    }
    if v.get("datatype").is_some_and(|v| v != "float32") {
        return Err(bad("only float32 dense vectors are supported"));
    }
    if v.get("on_disk").is_some_and(|v| !v.is_boolean()) {
        return Err(bad("on_disk must be boolean"));
    }
    if let Some(h) = v.get("hnsw_config")
        && !object(h)?.is_empty()
    {
        return Err(bad("HNSW configuration is unsupported"));
    }
    Ok(())
}
fn dense_config<'a>(params: &'a Value, name: &str) -> Option<&'a Value> {
    let v = &params["vectors"];
    if v.get("size").is_some() {
        if name.is_empty() { Some(v) } else { None }
    } else {
        v.get(name)
    }
}
pub(super) fn has_name(params: &Value, name: &str) -> bool {
    dense_config(params, name).is_some() || params["sparse_vectors"].get(name).is_some()
}
pub(super) fn decode_all(v: &Value, params: &Value) -> Result<BTreeMap<String, Vector>> {
    let mut out = BTreeMap::new();
    if v.is_array() {
        out.insert("".into(), decode(v, "", params, false)?);
    } else {
        for (name, value) in object(v)? {
            out.insert(name.clone(), decode(value, name, params, false)?);
        }
    }
    Ok(out)
}
fn floats(v: &Value) -> Result<Vec<f32>> {
    v.as_array()
        .ok_or_else(|| bad("vector values must be an array"))?
        .iter()
        .map(|v| {
            v.as_f64()
                .filter(|v| v.is_finite() && v.abs() <= f32::MAX as f64)
                .map(|v| v as f32)
                .ok_or_else(|| bad("vector values must be finite numbers"))
        })
        .collect()
}
pub(super) fn decode(v: &Value, name: &str, params: &Value, query: bool) -> Result<Vector> {
    if let Some(config) = dense_config(params, name) {
        let mut values = floats(v)?;
        if values.len() != config["size"].as_u64().unwrap() as usize {
            return Err(bad(format!(
                "Vector dimension error: expected dim: {}, got {}",
                config["size"],
                values.len()
            )));
        }
        if config["distance"] == "Cosine" {
            let squared = values.iter().map(|v| v * v).sum::<f32>();
            if squared >= f32::EPSILON && (squared - 1.0).abs() > 1.0e-6 {
                let norm = squared.sqrt();
                for v in &mut values {
                    *v /= norm;
                }
            }
        }
        return Ok(Vector::Dense(values));
    }
    if params["sparse_vectors"].get(name).is_none() {
        return Err(bad(format!("Not existing vector name error: {name}")));
    }
    let (indices, values) = if v.get("text").is_some() {
        fields(v, &["text", "model", "options"])?;
        if v["model"] != "qdrant/bm25" {
            return Err(bad("only local model qdrant/bm25 is supported"));
        }
        super::bm25::embed(
            v["text"]
                .as_str()
                .ok_or_else(|| bad("text must be a string"))?,
            v.get("options"),
            query,
        )
        .map_err(bad)?
    } else {
        fields(v, &["indices", "values"])?;
        let indices = v["indices"]
            .as_array()
            .ok_or_else(|| bad("sparse indices required"))?
            .iter()
            .map(|v| {
                v.as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .ok_or_else(|| bad("sparse index must be uint32"))
            })
            .collect::<Result<Vec<_>>>()?;
        let values = floats(&v["values"])?;
        (indices, values)
    };
    if indices.len() != values.len() {
        return Err(bad("sparse indices and values must have the same length"));
    }
    if indices.iter().collect::<BTreeSet<_>>().len() != indices.len() {
        return Err(invalid("sparse indices must be unique"));
    }
    let mut zipped = indices.into_iter().zip(values).collect::<Vec<_>>();
    zipped.sort_by_key(|v| v.0);
    let (indices, values) = zipped.into_iter().unzip();
    Ok(Vector::Sparse { indices, values })
}
pub(super) fn score(query: &Vector, point: &Vector, name: &str, c: &Collection) -> Result<f32> {
    match (query, point) {
        (Vector::Dense(q), Vector::Dense(p)) => {
            let config = dense_config(&c.config["params"], name)
                .ok_or_else(|| bad("unknown vector name"))?;
            match config["distance"].as_str().unwrap() {
                "Cosine" | "Dot" => Ok(q.iter().zip(p).map(|(q, p)| q * p).sum()),
                _ => unreachable!(),
            }
        }
        (
            Vector::Sparse {
                indices: qi,
                values: qv,
            },
            Vector::Sparse {
                indices: pi,
                values: pv,
            },
        ) => {
            let idf = c.config["params"]["sparse_vectors"][name]["modifier"] == "idf";
            let mut score = 0.0;
            for (index, q) in qi.iter().zip(qv) {
                if let Ok(i) = pi.binary_search(index) {
                    let modifier = if idf {
                        let n = c.sparse_records.iter().filter(|record| matches!(record.get(name),Some(Vector::Sparse{indices,..})if indices.binary_search(index).is_ok())).count() as f32;
                        let total = c.sparse_counts.get(name).copied().unwrap_or(0) as f32;
                        ((total - n + 0.5) / (n + 0.5) + 1.0).ln()
                    } else {
                        1.0
                    };
                    score += q * modifier * pv[i];
                }
            }
            Ok(score)
        }
        _ => Err(bad("vector type mismatch")),
    }
}
