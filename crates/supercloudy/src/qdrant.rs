//! Isolated in-memory subset of the Qdrant HTTP API.
//! All mutations are immediately visible; indexes are metadata and search is exact.
mod bm25;
mod filter;
mod vectors;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

type Store = Arc<Mutex<BTreeMap<String, Collection>>>;
type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
struct Error(StatusCode, String);
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            self.0,
            axum::Json(json!({"status":{"error":self.1},"time":0.0})),
        )
            .into_response()
    }
}
fn bad(message: impl Into<String>) -> Error {
    Error(
        StatusCode::BAD_REQUEST,
        format!("Bad request: {}", message.into()),
    )
}
fn invalid(message: impl Into<String>) -> Error {
    Error(
        StatusCode::UNPROCESSABLE_ENTITY,
        format!("Validation error in JSON body: {}", message.into()),
    )
}
fn missing(name: &str) -> Error {
    Error(
        StatusCode::NOT_FOUND,
        format!("Not found: Collection `{name}` doesn't exist!"),
    )
}
fn ok(value: Value) -> Response {
    axum::Json(json!({"result":value,"status":"ok","time":0.0})).into_response()
}
fn object(value: &Value) -> Result<&Map<String, Value>> {
    value.as_object().ok_or_else(|| bad("expected an object"))
}
fn fields(value: &Value, names: &[&str]) -> Result<()> {
    for key in object(value)?.keys() {
        if !names.contains(&key.as_str()) {
            return Err(bad(format!("unsupported field: {key}")));
        }
    }
    Ok(())
}
fn parse(body: Bytes) -> Result<Value> {
    serde_json::from_slice(&body).map_err(|_| bad("invalid JSON body"))
}
fn positive(value: Option<&Value>, default: usize) -> Result<usize> {
    match value {
        None => Ok(default),
        Some(v) => v
            .as_u64()
            .filter(|v| *v > 0)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| invalid("limit must be a positive integer")),
    }
}
fn options(query: &HashMap<String, String>) -> Result<bool> {
    for (k, v) in query {
        match k.as_str() {
            "wait" if v == "true" || v == "false" => {}
            "ordering" if ["weak", "medium", "strong"].contains(&v.as_str()) => {}
            _ => return Err(bad(format!("unsupported query option: {k}"))),
        }
    }
    Ok(query.get("wait").is_some_and(|v| v == "true"))
}

/// Owns one listener and its server task. Dropping it cancels the server.
pub struct Server {
    address: SocketAddr,
    connections: Connections,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}
impl Server {
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) -> std::io::Result<()> {
        close_connections(&self.connections);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(std::io::Error::other)??;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        close_connections(&self.connections);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
pub async fn start(port: u16) -> std::io::Result<Server> {
    let listener =
        TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)).await?;
    let address = listener.local_addr()?;
    let (stop, rx) = oneshot::channel();
    let connections = Arc::new(Mutex::new(ConnectionRegistry {
        closed: false,
        streams: BTreeMap::new(),
    }));
    let owned = OwnedListener {
        listener,
        connections: connections.clone(),
        next: 0,
    };
    let task = tokio::spawn(async move {
        axum::serve(owned, router())
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
    });
    Ok(Server {
        address,
        connections,
        stop: Some(stop),
        task: Some(task),
    })
}
// Retain shutdown handles for accepted sockets so partial HTTP requests cannot
// outlive the server owner. Each connection removes its handle when it closes.
type Connections = Arc<Mutex<ConnectionRegistry>>;
struct ConnectionRegistry {
    closed: bool,
    streams: BTreeMap<u64, std::net::TcpStream>,
}
fn close_connections(connections: &Connections) {
    let mut registry = connections.lock().unwrap();
    registry.closed = true;
    for stream in registry.streams.values() {
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
}
struct OwnedListener {
    listener: TcpListener,
    connections: Connections,
    next: u64,
}
struct OwnedStream {
    stream: tokio::net::TcpStream,
    connections: Connections,
    id: u64,
}
impl Drop for OwnedStream {
    fn drop(&mut self) {
        self.connections.lock().unwrap().streams.remove(&self.id);
    }
}
impl tokio::io::AsyncRead for OwnedStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for OwnedStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
impl axum::serve::Listener for OwnedListener {
    type Io = OwnedStream;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, address)) => {
                    let Ok(stream) = stream.into_std() else {
                        continue;
                    };
                    let Ok(handle) = stream.try_clone() else {
                        continue;
                    };
                    let Ok(stream) = tokio::net::TcpStream::from_std(stream) else {
                        continue;
                    };
                    let id = self.next;
                    self.next = self.next.wrapping_add(1);
                    let mut registry = self.connections.lock().unwrap();
                    if registry.closed {
                        let _ = handle.shutdown(std::net::Shutdown::Both);
                    } else {
                        registry.streams.insert(id, handle);
                    }
                    drop(registry);
                    return (
                        OwnedStream {
                            stream,
                            connections: self.connections.clone(),
                            id,
                        },
                        address,
                    );
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
    }
    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}
/// Creates a fresh isolated namespace, suitable for mounting on its own listener.
pub fn router() -> Router {
    Router::new()
        .route(
            "/",
            get(|| async {
                axum::Json(json!({"title":"qdrant - vector search engine","version":"1.18.3"}))
            }),
        )
        .route("/collections", get(list))
        .route(
            "/collections/{name}",
            put(create).get(inspect).delete(remove).patch(update),
        )
        .route("/collections/{name}/exists", get(exists))
        .route("/collections/{name}/index", put(index))
        .route(
            "/collections/{name}/index/{field}",
            axum::routing::delete(delete_index),
        )
        .route("/collections/{name}/points", put(upsert).post(retrieve))
        .route("/collections/{name}/points/{id}", get(retrieve_one))
        .route("/collections/{name}/points/delete", post(delete_points))
        .route("/collections/{name}/points/query", post(query_points))
        .route("/collections/{name}/points/query/batch", post(batch_query))
        .route("/collections/{name}/points/count", post(count))
        .route("/collections/{name}/points/scroll", post(scroll))
        .fallback(|| async { Error(StatusCode::NOT_FOUND, "Not found".into()) })
        .layer(axum::extract::DefaultBodyLimit::max(32 * 1024 * 1024))
        .with_state(Arc::new(Mutex::new(BTreeMap::new())))
}

#[derive(Clone)]
struct Point {
    slot: usize,
    id: Value,
    vectors: BTreeMap<String, vectors::Vector>,
    payload: Map<String, Value>,
}
#[derive(Clone)]
struct Collection {
    config: Value,
    points: BTreeMap<String, Point>,
    indexes: BTreeMap<String, Value>,
    operation: u64,
    sparse_records: Vec<BTreeMap<String, vectors::Vector>>,
    sparse_counts: BTreeMap<String, usize>,
}
impl Collection {
    fn new(body: Value) -> Result<Self> {
        fields(
            &body,
            &[
                "vectors",
                "sparse_vectors",
                "metadata",
                "on_disk_payload",
                "shard_number",
                "replication_factor",
                "write_consistency_factor",
                "quantization_config",
            ],
        )?;
        for key in [
            "shard_number",
            "replication_factor",
            "write_consistency_factor",
        ] {
            if let Some(v) = body.get(key)
                && v.as_u64() != Some(1)
            {
                return Err(bad(format!("{key} supports only 1")));
            }
        }
        if body
            .get("quantization_config")
            .is_some_and(|v| !v.is_null())
        {
            return Err(bad("quantization is unsupported"));
        }
        vectors::validate_schema(&body)?;
        if let Some(v) = body.get("metadata")
            && !v.is_null()
        {
            object(v)?;
        }
        let mut params = json!({"vectors":body.get("vectors").cloned().unwrap_or(json!({})),"shard_number":1,"replication_factor":1,"write_consistency_factor":1,"on_disk_payload":true});
        for key in ["sparse_vectors", "on_disk_payload"] {
            if let Some(v) = body.get(key) {
                params[key] = v.clone();
            }
        }
        if !params["on_disk_payload"].is_boolean() {
            return Err(bad("on_disk_payload must be boolean"));
        }
        let config = json!({"params":params,"hnsw_config":{"m":16,"ef_construct":100,"full_scan_threshold":10000,"max_indexing_threads":0,"on_disk":false},"optimizer_config":{"deleted_threshold":0.2,"vacuum_min_vector_number":1000,"default_segment_number":0,"max_segment_size":null,"memmap_threshold":null,"indexing_threshold":10000,"flush_interval_sec":5,"max_optimization_threads":null,"prevent_unoptimized":null},"wal_config":{"wal_capacity_mb":32,"wal_segments_ahead":0,"wal_retain_closed":1},"quantization_config":null,"metadata":body.get("metadata").cloned().unwrap_or(Value::Null)});
        Ok(Self {
            config,
            points: BTreeMap::new(),
            indexes: BTreeMap::new(),
            operation: 0,
            sparse_records: Vec::new(),
            sparse_counts: BTreeMap::new(),
        })
    }
    fn info(&self) -> Value {
        let mut indexes = Map::new();
        for (key, schema) in &self.indexes {
            let mut entry = schema.clone();
            entry["points"] = json!(
                self.points
                    .values()
                    .filter(|p| filter::values(&p.payload, key)
                        .iter()
                        .any(|v| indexed_value(schema["data_type"].as_str().unwrap(), v)))
                    .count()
            );
            indexes.insert(key.clone(), entry);
        }
        json!({"status":"green","optimizer_status":"ok","indexed_vectors_count":0,"points_count":self.points.len(),"segments_count":1,"config":self.config,"payload_schema":indexes,"update_queue":{"length":0}})
    }
    fn operation(&mut self, wait: bool) -> Value {
        let id = self.operation;
        self.operation += 1;
        json!({"operation_id":id,"status":if wait{"completed"}else{"acknowledged"}})
    }
}
fn indexed_value(kind: &str, value: &Value) -> bool {
    match kind {
        "keyword" => value.is_string(),
        "integer" => {
            value.as_i64().is_some()
                || value.as_f64().is_some_and(|number| {
                    number.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&number)
                })
        }
        "float" => value.as_f64().is_some_and(f64::is_finite),
        "bool" => value.is_boolean(),
        "uuid" => value
            .as_str()
            .is_some_and(|s| bson::Uuid::parse_str(s).is_ok()),
        "datetime" => value.as_str().is_some_and(indexed_datetime),
        _ => false,
    }
}
fn indexed_datetime(value: &str) -> bool {
    // Use the same date parser family as the pinned server, including leap
    // seconds and its accepted UTC, offset, naive-time and date-only forms.
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
        || value.parse::<chrono::DateTime<chrono::Utc>>().is_ok()
        || ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%.f%#z"]
            .iter()
            .any(|format| chrono::DateTime::parse_from_str(value, format).is_ok())
        || [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
        ]
        .iter()
        .any(|format| chrono::NaiveDateTime::parse_from_str(value, format).is_ok())
        || chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}
async fn list(State(state): State<Store>) -> Response {
    ok(
        json!({"collections":state.lock().unwrap().keys().map(|name|json!({"name":name})).collect::<Vec<_>>()}),
    )
}
async fn exists(State(state): State<Store>, Path(name): Path<String>) -> Response {
    ok(json!({"exists":state.lock().unwrap().contains_key(&name)}))
}
async fn create(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let c = Collection::new(parse(body)?)?;
    let mut state = state.lock().unwrap();
    if state.contains_key(&name) {
        return Err(Error(
            StatusCode::CONFLICT,
            format!("Wrong input: Collection `{name}` already exists!"),
        ));
    }
    state.insert(name, c);
    Ok(ok(json!(true)))
}
async fn inspect(State(state): State<Store>, Path(name): Path<String>) -> Result<Response> {
    Ok(ok(state
        .lock()
        .unwrap()
        .get(&name)
        .ok_or_else(|| missing(&name))?
        .info()))
}
async fn remove(State(state): State<Store>, Path(name): Path<String>) -> Response {
    ok(json!(state.lock().unwrap().remove(&name).is_some()))
}
async fn update(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let body = parse(body)?;
    fields(&body, &["metadata"])?;
    let mut state = state.lock().unwrap();
    let c = state.get_mut(&name).ok_or_else(|| missing(&name))?;
    if let Some(v) = body.get("metadata")
        && !v.is_null()
    {
        let updates = object(v)?;
        if !c.config["metadata"].is_object() {
            c.config["metadata"] = json!({});
        }
        let metadata = c.config["metadata"].as_object_mut().unwrap();
        for (key, value) in updates {
            if value.is_null() {
                metadata.remove(key);
            } else {
                metadata.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(ok(json!(true)))
}
async fn index(
    State(state): State<Store>,
    Path(name): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let wait = options(&q)?;
    let b = parse(body)?;
    fields(&b, &["field_name", "field_schema"])?;
    let field = b["field_name"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad("field_name required"))?;
    let schema = &b["field_schema"];
    let kind = schema
        .as_str()
        .or_else(|| schema["type"].as_str())
        .ok_or_else(|| bad("field_schema required"))?;
    if !["keyword", "integer", "float", "bool", "uuid", "datetime"].contains(&kind) {
        return Err(bad("unsupported field schema"));
    }
    if schema.is_object() {
        fields(
            schema,
            &[
                "type",
                "is_tenant",
                "lookup",
                "range",
                "on_disk",
                "enable_hnsw",
            ],
        )?;
        for (k, v) in object(schema)? {
            if k != "type" && !v.is_boolean() {
                return Err(bad("index options must be boolean"));
            }
        }
    }
    let mut result = json!({"data_type":kind,"points":0});
    if schema.is_object() {
        result["params"] = schema.clone();
    }
    let mut state = state.lock().unwrap();
    let c = state.get_mut(&name).ok_or_else(|| missing(&name))?;
    c.indexes.insert(field.into(), result);
    Ok(ok(c.operation(wait)))
}
async fn delete_index(
    State(state): State<Store>,
    Path((name, field)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response> {
    let wait = options(&q)?;
    let mut s = state.lock().unwrap();
    let c = s.get_mut(&name).ok_or_else(|| missing(&name))?;
    c.indexes.remove(&field);
    Ok(ok(c.operation(wait)))
}
fn point_key(id: &Value) -> Result<String> {
    if let Some(n) = id.as_u64() {
        return Ok(format!("n{n:020}"));
    }
    if let Some(s) = id.as_str() {
        let shape = s.len() == 32
            || (s.len() == 36
                && s.as_bytes().iter().enumerate().all(|(i, b)| {
                    if [8, 13, 18, 23].contains(&i) {
                        *b == b'-'
                    } else {
                        b.is_ascii_hexdigit()
                    }
                }));
        let compact = s.replace('-', "");
        if shape && compact.len() == 32 && compact.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Ok(format!("s{}", compact.to_ascii_lowercase()));
        }
    }
    Err(bad("point id must be an unsigned integer or UUID"))
}
fn decode_point(value: &Value, c: &Collection) -> Result<Point> {
    fields(value, &["id", "vector", "payload"])?;
    point_key(&value["id"])?;
    let payload = match value.get("payload") {
        None | Some(Value::Null) => Map::new(),
        Some(v) => object(v)?.clone(),
    };
    let vectors = vectors::decode_all(&value["vector"], &c.config["params"])?;
    let id = if let Some(s) = value["id"].as_str() {
        let raw = s.replace('-', "").to_ascii_lowercase();
        json!(format!(
            "{}-{}-{}-{}-{}",
            &raw[..8],
            &raw[8..12],
            &raw[12..16],
            &raw[16..20],
            &raw[20..]
        ))
    } else {
        value["id"].clone()
    };
    Ok(Point {
        slot: 0,
        id,
        vectors,
        payload,
    })
}
async fn upsert(
    State(state): State<Store>,
    Path(name): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let wait = options(&q)?;
    let b = parse(body)?;
    fields(&b, &["points", "batch"])?;
    if b.get("batch").is_some() {
        return Err(bad("batch upsert is unsupported; use points"));
    }
    let entries = b["points"]
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| bad("points must be nonempty"))?;
    let mut s = state.lock().unwrap();
    let c = s.get_mut(&name).ok_or_else(|| missing(&name))?;
    let points = entries
        .iter()
        .map(|v| decode_point(v, c))
        .collect::<Result<Vec<_>>>()?;
    for mut p in points {
        let key = point_key(&p.id)?;
        let old = c.points.get(&key);
        p.slot = old.map_or(c.sparse_records.len(), |old| old.slot);
        let previous = old.map(|old| &old.vectors);
        for name in c.config["params"]["sparse_vectors"]
            .as_object()
            .into_iter()
            .flat_map(|o| o.keys())
        {
            let before = previous.and_then(|v| v.get(name));
            let after = p.vectors.get(name);
            let nonempty =
                matches!(after,Some(vectors::Vector::Sparse{indices,..})if !indices.is_empty());
            let count = c.sparse_counts.entry(name.clone()).or_default();
            if nonempty && before.is_none() {
                *count += 1;
            }
            if !nonempty
                && matches!(before,Some(vectors::Vector::Sparse{indices,..})if !indices.is_empty())
            {
                *count = count.saturating_sub(1);
            }
        }
        let record = p
            .vectors
            .iter()
            .filter(|(_, v)| matches!(v, vectors::Vector::Sparse { .. }))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if p.slot == c.sparse_records.len() {
            c.sparse_records.push(record);
        } else {
            c.sparse_records[p.slot] = record;
        }
        c.points.insert(key, p);
    }
    Ok(ok(c.operation(wait)))
}
fn payload_paths(selector: &Value) -> Result<(bool, Vec<String>)> {
    if let Some(keys) = selector.as_array() {
        return Ok((
            true,
            keys.iter()
                .map(|k| {
                    k.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| bad("payload keys must be strings"))
                })
                .collect::<Result<_>>()?,
        ));
    }
    fields(selector, &["include", "exclude"])?;
    let o = object(selector)?;
    if o.len() != 1 {
        return Err(bad(
            "payload selector requires exactly one of include or exclude",
        ));
    }
    let include = o.contains_key("include");
    let keys = if include {
        &selector["include"]
    } else {
        &selector["exclude"]
    };
    let keys = keys
        .as_array()
        .ok_or_else(|| bad("payload selector keys must be an array"))?;
    let paths = keys
        .iter()
        .map(|k| {
            k.as_str()
                .map(str::to_owned)
                .ok_or_else(|| bad("payload keys must be strings"))
        })
        .collect::<Result<_>>()?;
    Ok((include, paths))
}
fn validate_selection(
    c: &Collection,
    payload: Option<&Value>,
    vector: Option<&Value>,
) -> Result<()> {
    if let Some(v) = payload
        && !v.is_boolean()
    {
        let (_, paths) = payload_paths(v)?;
        payload_parts(&paths)?;
    }
    if let Some(v) = vector
        && !v.is_boolean()
    {
        for name in v.as_array().ok_or_else(|| bad("invalid vector selector"))? {
            let name = name
                .as_str()
                .ok_or_else(|| bad("vector names must be strings"))?;
            if !vectors::has_name(&c.config["params"], name) {
                return Err(bad(format!("Not existing vector name error: {name}")));
            }
        }
    }
    Ok(())
}
enum PayloadPart {
    Field(String),
    Each,
}
fn payload_parts(paths: &[String]) -> Result<Vec<Vec<PayloadPart>>> {
    paths
        .iter()
        .map(|path| {
            let mut parts = Vec::new();
            for field in path.split('.') {
                let name = field.strip_suffix("[]").unwrap_or(field);
                if name.contains(['[', ']']) {
                    return Err(bad(
                        "payload selectors support object fields and [] array traversal",
                    ));
                }
                parts.push(PayloadPart::Field(name.to_owned()));
                if field.ends_with("[]") {
                    parts.push(PayloadPart::Each);
                }
            }
            Ok(parts)
        })
        .collect()
}
fn project_payload(value: &mut Value, paths: &[&[PayloadPart]], include: bool) -> bool {
    if paths.iter().any(|path| path.is_empty()) {
        return include;
    }
    match value {
        Value::Object(map) => map.retain(|key, value| {
            let nested = paths
                .iter()
                .filter_map(|path| match path.first() {
                    Some(PayloadPart::Field(name)) if name == key => Some(&path[1..]),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if nested.is_empty() {
                !include
            } else {
                project_payload(value, &nested, include)
            }
        }),
        Value::Array(values) => {
            let nested = paths
                .iter()
                .filter_map(|path| {
                    if matches!(path.first(), Some(PayloadPart::Each)) {
                        Some(&path[1..])
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if nested.is_empty() {
                if include {
                    values.clear();
                }
            } else {
                values.retain_mut(|value| project_payload(value, &nested, include));
            }
        }
        _ => {}
    }
    true
}
fn select_payload(point: &Point, selector: Option<&Value>) -> Result<Option<Value>> {
    match selector {
        None | Some(Value::Bool(false)) => Ok(None),
        Some(Value::Bool(true)) => Ok(Some(json!(point.payload))),
        Some(v) => {
            let (include, paths) = payload_paths(v)?;
            let mut payload = json!(point.payload);
            let parts = payload_parts(&paths)?;
            project_payload(
                &mut payload,
                &parts.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                include,
            );
            Ok(Some(payload))
        }
    }
}
fn project(p: &Point, payload: Option<&Value>, vector: Option<&Value>) -> Result<Value> {
    let mut out = json!({"id":p.id});
    if let Some(payload) = select_payload(p, payload)? {
        out["payload"] = payload;
    }
    let names = match vector {
        None | Some(Value::Bool(false)) => None,
        Some(Value::Bool(true)) => Some(p.vectors.keys().cloned().collect::<Vec<_>>()),
        Some(Value::Array(a)) => Some(
            a.iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| bad("vector names must be strings"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => return Err(bad("invalid vector selector")),
    };
    if let Some(names) = names {
        let mut v = Map::new();
        for name in names {
            if let Some(vector) = p.vectors.get(&name) {
                v.insert(name, vector.json());
            }
        }
        out["vector"] = if v.len() == 1 && v.contains_key("") {
            v.remove("").unwrap()
        } else {
            json!(v)
        };
    }
    Ok(out)
}
async fn retrieve(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let b = parse(body)?;
    fields(&b, &["ids", "with_payload", "with_vector"])?;
    let ids = b["ids"].as_array().ok_or_else(|| bad("ids required"))?;
    let s = state.lock().unwrap();
    let c = s.get(&name).ok_or_else(|| missing(&name))?;
    validate_selection(c, b.get("with_payload"), b.get("with_vector"))?;
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        let key = point_key(id)?;
        if !seen.insert(key.clone()) {
            continue;
        }
        if let Some(p) = c.points.get(&key) {
            out.push(project(p, b.get("with_payload"), b.get("with_vector"))?);
        }
    }
    Ok(ok(json!(out)))
}
async fn retrieve_one(
    State(state): State<Store>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Response> {
    let id = id.parse::<u64>().map_or_else(|_| json!(id), |n| json!(n));
    let s = state.lock().unwrap();
    let c = s.get(&name).ok_or_else(|| missing(&name))?;
    let p = c.points.get(&point_key(&id)?).ok_or_else(|| {
        Error(
            StatusCode::NOT_FOUND,
            format!("Not found: No point with id {id} found"),
        )
    })?;
    Ok(ok(project(
        p,
        Some(&Value::Bool(true)),
        Some(&Value::Bool(true)),
    )?))
}
async fn delete_points(
    State(state): State<Store>,
    Path(name): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response> {
    let wait = options(&q)?;
    let b = parse(body)?;
    fields(&b, &["points", "filter"])?;
    if b.get("points").is_some() == b.get("filter").is_some() {
        return Err(bad("supply exactly one of points or filter"));
    }
    if let Some(f) = b.get("filter") {
        filter::validate(f)?;
    }
    let mut s = state.lock().unwrap();
    let c = s.get_mut(&name).ok_or_else(|| missing(&name))?;
    let keys = if let Some(ids) = b.get("points") {
        ids.as_array()
            .ok_or_else(|| bad("points must be an array"))?
            .iter()
            .map(point_key)
            .collect::<Result<Vec<_>>>()?
    } else {
        c.points
            .iter()
            .filter(|(_, p)| filter::matches(&b["filter"], p))
            .map(|(k, _)| k.clone())
            .collect()
    };
    for k in keys {
        c.points.remove(&k);
    }
    Ok(ok(c.operation(wait)))
}
fn run_query(c: &Collection, b: &Value) -> Result<Value> {
    validate_selection(c, b.get("with_payload"), b.get("with_vector"))?;
    fields(
        b,
        &[
            "query",
            "using",
            "filter",
            "limit",
            "offset",
            "with_payload",
            "with_vector",
            "score_threshold",
            "params",
        ],
    )?;
    if let Some(params) = b.get("params") {
        fields(params, &["exact", "hnsw_ef"])?;
        if params.get("exact").is_some_and(|v| !v.is_boolean()) {
            return Err(bad("exact must be boolean"));
        }
        if params
            .get("hnsw_ef")
            .is_some_and(|v| v.as_u64().is_none_or(|n| n == 0))
        {
            return Err(bad("hnsw_ef must be positive integer"));
        }
    }
    let using = match b.get("using") {
        None => "",
        Some(v) => v.as_str().ok_or_else(|| bad("using must be string"))?,
    };
    let limit = positive(b.get("limit"), 10)?;
    let offset = match b.get("offset") {
        None => 0,
        Some(v) => v.as_u64().ok_or_else(|| bad("invalid offset"))? as usize,
    };
    if let Some(f) = b.get("filter") {
        filter::validate(f)?;
    }
    let mut exclude = None;
    let query = match b.get("query") {
        None => None,
        Some(v) if v.is_number() || v.is_string() => {
            let key = point_key(v)?;
            let p = c.points.get(&key).ok_or_else(|| {
                Error(
                    StatusCode::NOT_FOUND,
                    "Not found: query point not found".into(),
                )
            })?;
            exclude = Some(key);
            Some(
                p.vectors
                    .get(using)
                    .ok_or_else(|| bad("query point has no vector"))?
                    .clone(),
            )
        }
        Some(v) => Some(vectors::decode(v, using, &c.config["params"], true)?),
    };
    let threshold = match b.get("score_threshold") {
        None => None,
        Some(v) => Some(v.as_f64().ok_or_else(|| bad("invalid score_threshold"))? as f32),
    };
    let mut out = Vec::new();
    for (k, p) in &c.points {
        if exclude.as_ref() == Some(k) || b.get("filter").is_some_and(|f| !filter::matches(f, p)) {
            continue;
        }
        let score = if let Some(q) = &query {
            let Some(v) = p.vectors.get(using) else {
                continue;
            };
            let score = vectors::score(q, v, using, c)?;
            if matches!(q, vectors::Vector::Sparse { .. }) && score == 0.0 {
                continue;
            }
            score
        } else {
            1.0
        };
        if threshold.is_some_and(|t| score < t) {
            continue;
        }
        out.push((score, p));
    }
    if query.is_some() {
        out.sort_by(|a, b| {
            b.0.total_cmp(&a.0).then_with(|| {
                point_key(&a.1.id)
                    .unwrap()
                    .cmp(&point_key(&b.1.id).unwrap())
            })
        });
    }
    let mut points = Vec::new();
    for (score, p) in out.into_iter().skip(offset).take(limit) {
        let mut v = project(p, b.get("with_payload"), b.get("with_vector"))?;
        v["score"] = json!(score);
        v["version"] = json!(0);
        points.push(v);
    }
    Ok(json!({"points":points}))
}
async fn query_points(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let b = parse(body)?;
    let s = state.lock().unwrap();
    Ok(ok(run_query(
        s.get(&name).ok_or_else(|| missing(&name))?,
        &b,
    )?))
}
async fn batch_query(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let b = parse(body)?;
    fields(&b, &["searches"])?;
    let searches = b["searches"]
        .as_array()
        .ok_or_else(|| bad("searches required"))?;
    let s = state.lock().unwrap();
    let c = s.get(&name).ok_or_else(|| missing(&name))?;
    Ok(ok(json!(
        searches
            .iter()
            .map(|b| run_query(c, b))
            .collect::<Result<Vec<_>>>()?
    )))
}
async fn count(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let b = parse(body)?;
    fields(&b, &["filter", "exact"])?;
    if b.get("exact").is_some_and(|v| !v.is_boolean()) {
        return Err(bad("exact must be boolean"));
    }
    if let Some(f) = b.get("filter") {
        filter::validate(f)?;
    }
    let s = state.lock().unwrap();
    let c = s.get(&name).ok_or_else(|| missing(&name))?;
    Ok(ok(
        json!({"count":c.points.values().filter(|p|b.get("filter").is_none_or(|f|filter::matches(f,p))).count()}),
    ))
}
async fn scroll(
    State(state): State<Store>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Response> {
    let b = parse(body)?;
    fields(
        &b,
        &["filter", "limit", "offset", "with_payload", "with_vector"],
    )?;
    if let Some(f) = b.get("filter") {
        filter::validate(f)?;
    }
    let limit = positive(b.get("limit"), 10)?;
    let offset = b.get("offset").map(point_key).transpose()?;
    let s = state.lock().unwrap();
    let c = s.get(&name).ok_or_else(|| missing(&name))?;
    validate_selection(c, b.get("with_payload"), b.get("with_vector"))?;
    let mut entries = c
        .points
        .iter()
        .filter(|(k, p)| {
            offset.as_ref().is_none_or(|o| *k >= o)
                && b.get("filter").is_none_or(|f| filter::matches(f, p))
        })
        .take(limit + 1)
        .collect::<Vec<_>>();
    let next = if entries.len() > limit {
        Some(entries.pop().unwrap().1.id.clone())
    } else {
        None
    };
    let points = entries
        .into_iter()
        .map(|(_, p)| {
            project(
                p,
                b.get("with_payload").or(Some(&Value::Bool(true))),
                b.get("with_vector"),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ok(json!({"points":points,"next_page_offset":next})))
}
