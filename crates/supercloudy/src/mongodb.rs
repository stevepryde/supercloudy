//! A loopback-only, in-memory subset of the MongoDB wire protocol.
//!
//! No authentication, persistence, sharding, replication or external calls occur.
mod query;

use bson::{Bson, Document, doc};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

const MAX_MESSAGE: usize = 48 * 1024 * 1024;

/// An owned MongoDB listener and its connections. Dropping it cancels all work.
pub struct Server {
    address: SocketAddr,
    task: Option<JoinHandle<()>>,
}
impl Server {
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Start an isolated server on IPv4 loopback. Port zero selects an available port.
pub async fn start(port: u16) -> io::Result<Server> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let state = Arc::new(Mutex::new(State::default()));
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => { let state = state.clone(); connections.spawn(async move { let _ = connection(stream, state, address).await; }); },
                    Err(_) => break,
                },
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Ok(Server {
        address,
        task: Some(task),
    })
}

#[derive(Debug, Clone)]
pub(super) struct Error {
    code: i32,
    message: String,
}
impl Error {
    pub(super) fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    fn document(&self) -> Document {
        let name = match self.code {
            2 => "BadValue",
            9 => "FailedToParse",
            26 => "NamespaceNotFound",
            27 => "IndexNotFound",
            43 => "CursorNotFound",
            59 => "CommandNotFound",
            66 => "ImmutableField",
            72 => "InvalidOptions",
            11000 => "DuplicateKey",
            112 => "WriteConflict",
            117 => "ConflictingOperationInProgress",
            251 => "NoSuchTransaction",
            225 => "TransactionTooOld",
            _ => "OperationFailed",
        };
        let mut result =
            doc! { "ok": 0.0, "code": self.code, "codeName": name, "errmsg": &self.message };
        if self.code == 112 || self.code == 251 {
            result.insert("errorLabels", vec!["TransientTransactionError"]);
        }
        result
    }
}
type Result<T> = std::result::Result<T, Error>;
fn bad(message: impl Into<String>) -> Error {
    Error::new(2, message)
}
fn integer(value: Option<&Bson>, default: i64) -> Result<i64> {
    match value {
        None => Ok(default),
        Some(Bson::Int32(v)) => Ok(i64::from(*v)),
        Some(Bson::Int64(v)) => Ok(*v),
        Some(_) => Err(bad("integer required")),
    }
}
fn string<'a>(document: &'a Document, key: &str) -> Result<&'a str> {
    document
        .get_str(key)
        .map_err(|_| bad(format!("{key} must be a string")))
}
fn object(document: &Document, key: &str) -> Result<Document> {
    match document.get(key) {
        None => Ok(Document::new()),
        Some(Bson::Document(v)) => Ok(v.clone()),
        _ => Err(bad(format!("{key} must be an object"))),
    }
}
fn array(document: &Document, key: &str) -> Result<Vec<Bson>> {
    document
        .get_array(key)
        .cloned()
        .map_err(|_| bad(format!("{key} must be an array")))
}
fn boolean(document: &Document, key: &str, default: bool) -> Result<bool> {
    match document.get(key) {
        None => Ok(default),
        Some(Bson::Boolean(v)) => Ok(*v),
        _ => Err(bad(format!("{key} must be boolean"))),
    }
}
fn options(document: &Document, allowed: &[&str]) -> Result<()> {
    for key in document.keys() {
        if key.starts_with('$') && key != "$db" && key != "$readPreference" {
            return Err(Error::new(72, format!("unsupported option {key}")));
        }
        if !allowed.contains(&key.as_str())
            && ![
                "$db",
                "$readPreference",
                "lsid",
                "txnNumber",
                "autocommit",
                "startTransaction",
                "readConcern",
                "writeConcern",
                "maxTimeMS",
                "comment",
                "apiVersion",
                "apiStrict",
                "apiDeprecationErrors",
            ]
            .contains(&key.as_str())
        {
            return Err(Error::new(72, format!("unsupported option {key}")));
        }
    }
    if let Some(value) = document.get("readConcern") {
        let concern = value
            .as_document()
            .ok_or_else(|| bad("readConcern must be object"))?;
        options(concern, &["level"])?;
        if let Ok(level) = concern.get_str("level")
            && !["local", "available", "majority", "snapshot"].contains(&level)
        {
            return Err(Error::new(72, "unsupported readConcern"));
        }
    }
    if let Some(concern) = document.get("writeConcern") {
        let concern = concern
            .as_document()
            .ok_or_else(|| bad("writeConcern must be object"))?;
        options(concern, &["w", "wtimeout", "wtimeoutMS", "j"])?;
        if let Some(w) = concern.get("w")
            && w != &Bson::String("majority".into())
            && !query::equal(w, &Bson::Int32(0))
            && !query::equal(w, &Bson::Int32(1))
        {
            return Err(Error::new(
                72,
                "only writeConcern w=0, w=1 or majority supported",
            ));
        }
        if boolean(concern, "j", false)? {
            return Err(Error::new(72, "journal durability unavailable in memory"));
        }
    }
    if let Some(version) = document.get("apiVersion")
        && version != &Bson::String("1".into())
    {
        return Err(Error::new(323, "only API version 1 supported"));
    }
    if document.contains_key("maxTimeMS") && integer(document.get("maxTimeMS"), 0)? < 0 {
        return Err(bad("maxTimeMS must be nonnegative"));
    }
    Ok(())
}

#[derive(Clone)]
struct Index {
    name: String,
    keys: Document,
    unique: bool,
    sparse: bool,
    partial: Option<Document>,
    ttl: Option<i64>,
}
impl Index {
    fn id() -> Self {
        Self {
            name: "_id_".into(),
            keys: doc! {"_id":1},
            unique: true,
            sparse: false,
            partial: None,
            ttl: None,
        }
    }
    fn document(&self) -> Document {
        let mut d = doc! { "v": 2, "key": self.keys.clone(), "name": &self.name };
        if self.unique && self.name != "_id_" {
            d.insert("unique", true);
        }
        if self.sparse {
            d.insert("sparse", true);
        }
        if let Some(p) = &self.partial {
            d.insert("partialFilterExpression", p.clone());
        }
        if let Some(ttl) = self.ttl {
            d.insert("expireAfterSeconds", ttl);
        }
        d
    }
}
#[derive(Clone)]
struct Collection {
    docs: Vec<Document>,
    indexes: Vec<Index>,
}
impl Default for Collection {
    fn default() -> Self {
        Self {
            docs: vec![],
            indexes: vec![Index::id()],
        }
    }
}
#[derive(Clone, Default)]
struct Store {
    collections: BTreeMap<String, Collection>,
    versions: BTreeMap<String, Vec<(Bson, u64)>>,
}
impl Store {
    fn version(&self, ns: &str, id: &Bson) -> u64 {
        self.versions
            .get(ns)
            .and_then(|entries| entries.iter().find(|(key, _)| query::equal(key, id)))
            .map_or(0, |(_, version)| *version)
    }
    fn set_version(&mut self, ns: &str, id: Bson, revision: u64) {
        let entries = self.versions.entry(ns.to_owned()).or_default();
        if let Some((_, version)) = entries.iter_mut().find(|(key, _)| query::equal(key, &id)) {
            *version = revision;
        } else {
            entries.push((id, revision));
        }
    }
}
#[derive(Clone, Default)]
struct CursorOwner {
    session: Option<Vec<u8>>,
    transaction: Option<Vec<u8>>,
}
impl CursorOwner {
    fn from_command(command: &Document) -> Result<Self> {
        let session = command
            .get("lsid")
            .map(|id| bson::to_vec(&doc! {"s":id.clone()}).map_err(|_| bad("invalid session")))
            .transpose()?;
        let transaction = if command.get("autocommit") == Some(&Bson::Boolean(false)) {
            session_key(command)?
        } else {
            None
        };
        Ok(Self {
            session,
            transaction,
        })
    }
}
#[derive(Clone)]
struct Transaction {
    base: Store,
    working: Store,
    touched: BTreeSet<String>,
}
struct WriteEvent {
    namespace: String,
    id: Bson,
}
struct RetryStatement {
    command: String,
    response: Document,
}
struct Cursor {
    namespace: String,
    remaining: Vec<Document>,
    owner: CursorOwner,
}
#[derive(Default)]
struct State {
    store: Store,
    transactions: BTreeMap<Vec<u8>, Transaction>,
    completed: BTreeSet<Vec<u8>>,
    started: BTreeSet<Vec<u8>>,
    retries: BTreeMap<Vec<u8>, RetryStatement>,
    cursors: BTreeMap<i64, Cursor>,
    retired_cursors: BTreeMap<i64, (String, CursorOwner)>,
    next_cursor: i64,
    session_numbers: BTreeMap<Vec<u8>, i64>,
    cursor_owner: CursorOwner,
    next_revision: u64,
    write_events: Vec<WriteEvent>,
}

fn session_key(command: &Document) -> Result<Option<Vec<u8>>> {
    let Some(session) = command.get("lsid") else {
        return Ok(None);
    };
    if !matches!(session, Bson::Document(_)) {
        return Err(bad("invalid session id"));
    }
    let Some(txn) = command.get("txnNumber") else {
        return Ok(None);
    };
    let txn = integer(Some(txn), 0)?;
    if txn < 0 {
        return Err(bad("txnNumber must be nonnegative"));
    }
    bson::to_vec(&doc! {"s":session.clone(),"n":txn})
        .map(Some)
        .map_err(|_| bad("invalid session"))
}
fn namespace(command: &Document, name: &str) -> Result<String> {
    let db = string(command, "$db")?;
    let coll = string(command, name)?;
    if db.is_empty() || coll.is_empty() || coll.contains('\0') {
        return Err(bad("invalid namespace"));
    }
    Ok(format!("{db}.{coll}"))
}
fn split_ns(ns: &str) -> (&str, &str) {
    ns.split_once('.').unwrap_or(("", ns))
}
fn collection<'a>(store: &'a Store, ns: &str) -> &'a [Document] {
    store.collections.get(ns).map_or(&[], |c| c.docs.as_slice())
}

async fn connection(
    mut stream: TcpStream,
    state: Arc<Mutex<State>>,
    address: SocketAddr,
) -> io::Result<()> {
    loop {
        let mut header = [0u8; 16];
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let length = i32::from_le_bytes(header[0..4].try_into().unwrap());
        if length < 16 || length as usize > MAX_MESSAGE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid MongoDB message length",
            ));
        }
        let request_id = i32::from_le_bytes(header[4..8].try_into().unwrap());
        let opcode = i32::from_le_bytes(header[12..16].try_into().unwrap());
        let mut body = vec![0; length as usize - 16];
        stream.read_exact(&mut body).await?;
        let decoded = decode(opcode, &body);
        let (response, no_reply) = match decoded {
            Ok((command, no_reply)) => {
                let result = state.lock().unwrap().execute(command, address);
                (result.unwrap_or_else(|e| e.document()), no_reply)
            }
            Err(e) => (e.document(), false),
        };
        if no_reply {
            continue;
        }
        let bson = bson::to_vec(&response).map_err(io::Error::other)?;
        let mut reply = Vec::new();
        if opcode == 2004 {
            reply.extend_from_slice(&((36 + bson.len()) as i32).to_le_bytes());
            reply.extend_from_slice(&0i32.to_le_bytes());
            reply.extend_from_slice(&request_id.to_le_bytes());
            reply.extend_from_slice(&1i32.to_le_bytes());
            reply.extend_from_slice(&0i32.to_le_bytes());
            reply.extend_from_slice(&0i64.to_le_bytes());
            reply.extend_from_slice(&0i32.to_le_bytes());
            reply.extend_from_slice(&1i32.to_le_bytes());
            reply.extend_from_slice(&bson);
        } else {
            reply.extend_from_slice(&((21 + bson.len()) as i32).to_le_bytes());
            reply.extend_from_slice(&0i32.to_le_bytes());
            reply.extend_from_slice(&request_id.to_le_bytes());
            reply.extend_from_slice(&2013i32.to_le_bytes());
            reply.extend_from_slice(&0i32.to_le_bytes());
            reply.push(0);
            reply.extend_from_slice(&bson);
        }
        stream.write_all(&reply).await?;
    }
}
fn read_document(bytes: &[u8], offset: &mut usize) -> Result<Document> {
    let start = *offset;
    let length = bytes
        .get(start..start + 4)
        .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| bad("truncated BSON"))?;
    if length < 5 {
        return Err(bad("invalid BSON size"));
    }
    let end = start
        .checked_add(length as usize)
        .ok_or_else(|| bad("BSON overflow"))?;
    let data = bytes.get(start..end).ok_or_else(|| bad("truncated BSON"))?;
    *offset = end;
    Document::from_reader(&mut std::io::Cursor::new(data)).map_err(|_| bad("invalid BSON"))
}
fn decode(opcode: i32, body: &[u8]) -> Result<(Document, bool)> {
    if opcode == 2004 {
        let end = body
            .get(4..)
            .and_then(|b| b.iter().position(|v| *v == 0))
            .map(|i| i + 4)
            .ok_or_else(|| bad("invalid query namespace"))?;
        let ns = std::str::from_utf8(&body[4..end]).map_err(|_| bad("invalid namespace"))?;
        let mut offset = end + 1 + 8;
        let mut command = read_document(body, &mut offset)?;
        command.insert("$db", ns.split('.').next().unwrap_or("admin"));
        return Ok((command, false));
    }
    if opcode != 2013 {
        return Err(Error::new(59, "unsupported MongoDB opcode"));
    }
    let flags = body
        .get(..4)
        .map(|v| u32::from_le_bytes(v.try_into().unwrap()))
        .ok_or_else(|| bad("missing flags"))?;
    if flags & 0xffff & !3 != 0 {
        return Err(bad("unsupported OP_MSG flags"));
    }
    if flags & 1 != 0 {
        return Err(Error::new(72, "checksummed OP_MSG frames are unsupported"));
    }
    let end = body
        .len()
        .checked_sub(if flags & 1 != 0 { 4 } else { 0 })
        .ok_or_else(|| bad("invalid message"))?;
    let mut offset = 4;
    let mut command = None;
    let mut sequences = vec![];
    while offset < end {
        let kind = body[offset];
        offset += 1;
        match kind {
            0 => {
                if command.is_some() {
                    return Err(bad("duplicate body"));
                }
                command = Some(read_document(&body[..end], &mut offset)?);
            }
            1 => {
                let start = offset;
                let size = body
                    .get(offset..offset + 4)
                    .map(|v| i32::from_le_bytes(v.try_into().unwrap()))
                    .ok_or_else(|| bad("invalid sequence"))?;
                if size < 5 {
                    return Err(bad("invalid sequence size"));
                }
                let finish = start + size as usize;
                if finish > end {
                    return Err(bad("truncated sequence"));
                }
                offset += 4;
                let nul = body[offset..finish]
                    .iter()
                    .position(|v| *v == 0)
                    .ok_or_else(|| bad("invalid sequence identifier"))?;
                let name = std::str::from_utf8(&body[offset..offset + nul])
                    .map_err(|_| bad("invalid sequence identifier"))?
                    .to_owned();
                offset += nul + 1;
                let mut documents = vec![];
                while offset < finish {
                    documents.push(Bson::Document(read_document(&body[..finish], &mut offset)?));
                }
                sequences.push((name, documents));
            }
            _ => return Err(bad("unsupported section kind")),
        }
    }
    let mut command = command.ok_or_else(|| bad("missing command body"))?;
    for (name, values) in sequences {
        if command.contains_key(&name) {
            return Err(bad("duplicate sequence field"));
        }
        command.insert(name, values);
    }
    Ok((command, flags & 2 != 0))
}

impl State {
    fn expire(&mut self) {
        let now = bson::DateTime::now().timestamp_millis();
        let mut removed = Vec::new();
        for (namespace, collection) in &mut self.store.collections {
            collection.docs.retain(|doc| {
                let expired = collection.indexes.iter().any(|index| {
                    let Some(ttl) = index.ttl else { return false };
                    if let Some(partial) = &index.partial
                        && !query::matches(doc, partial).unwrap_or(false)
                    {
                        return false;
                    }
                    let Some(key) = index.keys.keys().next() else {
                        return false;
                    };
                    fn expired(value: &Bson, now: i64, ttl: i64) -> bool {
                        match value {
                            Bson::DateTime(date) => {
                                date.timestamp_millis()
                                    .saturating_add(ttl.saturating_mul(1000))
                                    <= now
                            }
                            Bson::Array(values) => values.iter().any(|v| expired(v, now, ttl)),
                            _ => false,
                        }
                    }
                    query::get(doc, key).is_some_and(|v| expired(v, now, ttl))
                });
                if expired {
                    removed.push((namespace.clone(), doc.get("_id").unwrap().clone()));
                }
                !expired
            });
        }
        for (namespace, id) in removed {
            self.next_revision += 1;
            self.store.set_version(&namespace, id, self.next_revision);
        }
    }
    fn execute(&mut self, command: Document, address: SocketAddr) -> Result<Document> {
        self.expire();
        let name = command
            .keys()
            .next()
            .ok_or_else(|| bad("empty command"))?
            .clone();
        if ["hello", "isMaster", "ismaster"].contains(&name.as_str()) {
            return Ok(
                doc! {"ok":1.0,"isWritablePrimary":true,"ismaster":true,"helloOk":true,"minWireVersion":0,"maxWireVersion":25,"maxBsonObjectSize":16_777_216,"maxMessageSizeBytes":50_331_648,"maxWriteBatchSize":100_000,"logicalSessionTimeoutMinutes":30,"connectionId":1,"setName":"rs0","hosts":[address.to_string()],"primary":address.to_string(),"me":address.to_string(),"localTime":bson::DateTime::now(),"readOnly":false},
            );
        }
        if [
            "ping",
            "endSessions",
            "connectionStatus",
            "buildInfo",
            "getParameter",
        ]
        .contains(&name.as_str())
        {
            return Ok(match name.as_str() {
                "buildInfo" => {
                    doc! {"ok":1.0,"version":"8.0.28","versionArray":[8,0,28,0],"bits":64}
                }
                "getParameter" => return Err(Error::new(72, "server parameters are not emulated")),
                "connectionStatus" => {
                    doc! {"ok":1.0,"authInfo":{"authenticatedUsers":[],"authenticatedUserRoles":[]}}
                }
                _ => doc! {"ok":1.0},
            });
        }
        let key = session_key(&command)?;
        if let (Some(session), Some(number)) = (command.get("lsid"), command.get("txnNumber")) {
            let session =
                bson::to_vec(&doc! {"lsid":session.clone()}).map_err(|_| bad("invalid session"))?;
            let number = integer(Some(number), 0)?;
            if self
                .session_numbers
                .get(&session)
                .is_some_and(|highest| number < *highest)
            {
                return Err(Error::new(
                    225,
                    "transaction number is older than this session's latest transaction",
                ));
            }
            self.session_numbers.insert(session, number);
        }
        if name == "commitTransaction" {
            options(&command, &["commitTransaction"])?;
            let key = key.ok_or_else(|| bad("commit requires session and transaction number"))?;
            if self.completed.contains(&key) {
                return Ok(doc! {"ok":1.0});
            }
            let transaction = self
                .transactions
                .remove(&key)
                .ok_or_else(|| Error::new(251, "transaction does not exist"))?;
            self.purge_transaction_cursors(&key);
            self.commit(transaction)?;
            self.completed.insert(key);
            return Ok(doc! {"ok":1.0});
        }
        if name == "abortTransaction" {
            options(&command, &["abortTransaction"])?;
            let key = key.ok_or_else(|| bad("abort requires session and transaction number"))?;
            self.purge_transaction_cursors(&key);
            if self.transactions.remove(&key).is_none() {
                return Err(Error::new(251, "transaction does not exist"));
            }
            return Ok(doc! {"ok":1.0});
        }
        let transactional = match command.get("autocommit") {
            None => false,
            Some(Bson::Boolean(false)) => true,
            _ => return Err(bad("autocommit must be false")),
        };
        let write = [
            "insert",
            "update",
            "delete",
            "findAndModify",
            "findandmodify",
        ]
        .contains(&name.as_str());
        if transactional {
            let key = key.ok_or_else(|| bad("transaction requires session and number"))?;
            if boolean(&command, "startTransaction", false)? {
                if !self.started.insert(key.clone()) {
                    return Err(Error::new(117, "transaction number already used"));
                }
                self.transactions.insert(
                    key.clone(),
                    Transaction {
                        base: self.store.clone(),
                        working: self.store.clone(),
                        touched: BTreeSet::new(),
                    },
                );
            }
            let mut tx = self
                .transactions
                .remove(&key)
                .ok_or_else(|| Error::new(251, "transaction does not exist"))?;
            if ![
                "insert",
                "update",
                "delete",
                "findAndModify",
                "findandmodify",
                "find",
                "aggregate",
                "count",
                "distinct",
                "getMore",
                "killCursors",
            ]
            .contains(&name.as_str())
            {
                return Err(Error::new(263, "operation unsupported in transaction"));
            }
            let mut result = self.operation(&mut tx.working, &command, &name);
            if write && let Ok(ns) = namespace(&command, &name) {
                tx.touched.insert(ns);
            }
            if let Ok(response) = &result {
                if let Ok(errors) = response.get_array("writeErrors") {
                    let error = errors[0].as_document().unwrap();
                    result = Err(Error::new(
                        error.get_i32("code").unwrap_or(2),
                        error
                            .get_str("errmsg")
                            .unwrap_or("transaction write failed"),
                    ));
                } else if write {
                    let committed = std::mem::take(&mut self.store);
                    self.store = committed.clone();
                    let validation = self.commit(tx.clone());
                    self.store = committed;
                    if let Err(error) = validation {
                        result = Err(error);
                    } else {
                        self.transactions.insert(key.clone(), tx);
                    }
                } else {
                    self.transactions.insert(key.clone(), tx);
                }
            }
            if result.is_err() {
                self.purge_transaction_cursors(&key);
            }
            return result;
        }
        if write && let Some(key) = key {
            return self.retryable(&key, &command, &name);
        }
        self.execute_committed(&command, &name)
    }
    fn execute_committed(&mut self, command: &Document, name: &str) -> Result<Document> {
        let mut store = std::mem::take(&mut self.store);
        let result = self.operation(&mut store, command, name);
        self.store = store;
        result
    }
    fn retryable(&mut self, session: &[u8], command: &Document, name: &str) -> Result<Document> {
        let field = match name {
            "insert" => Some("documents"),
            "update" => Some("updates"),
            "delete" => Some("deletes"),
            _ => None,
        };
        let values = if let Some(field) = field {
            let values = array(command, field)?;
            if values.is_empty() {
                return Err(bad("write batch must not be empty"));
            }
            values
        } else {
            vec![Bson::Null]
        };
        if command.contains_key("stmtIds") && command.contains_key("stmtId") {
            return Err(bad("stmtId and stmtIds cannot be combined"));
        }
        let ids = if command.contains_key("stmtIds") {
            let ids = array(command, "stmtIds")?;
            if ids.len() != values.len() {
                return Err(bad("stmtIds must match write batch length"));
            }
            ids.iter()
                .map(|v| integer(Some(v), 0))
                .collect::<Result<Vec<_>>>()?
        } else {
            let first = integer(command.get("stmtId"), 0)?;
            (0..values.len())
                .map(|i| {
                    first
                        .checked_add(i as i64)
                        .ok_or_else(|| bad("statement id overflow"))
                })
                .collect::<Result<Vec<_>>>()?
        };
        if ids.iter().any(|id| *id < 0 || *id > i32::MAX as i64) {
            return Err(bad("statement ids must be nonnegative int32 values"));
        }
        let mut unique = BTreeSet::new();
        if ids.iter().any(|id| !unique.insert(*id)) {
            return Err(bad("statement ids must be distinct within a batch"));
        }
        let ordered = boolean(command, "ordered", true)?;
        let mut n = 0i64;
        let mut modified = 0i64;
        let mut errors = vec![];
        let mut upserted = vec![];
        for (index, (value, id)) in values.into_iter().zip(ids).enumerate() {
            if name == "update"
                && let Some(spec) = value.as_document()
                && boolean(spec, "multi", false)?
            {
                return Err(Error::new(72, "multi writes are not retryable"));
            }
            let mut key = session.to_vec();
            key.extend_from_slice(&id.to_le_bytes());
            let response = if let Some(previous) = self.retries.get(&key) {
                if previous.command != name {
                    return Err(Error::new(
                        50911,
                        "statement id already used by another command",
                    ));
                }
                previous.response.clone()
            } else {
                let mut single = command.clone();
                single.remove("stmtId");
                single.remove("stmtIds");
                if let Some(field) = field {
                    single.insert(field, vec![value]);
                }
                let response = self.execute_committed(&single, name)?;
                if !response.contains_key("writeErrors") {
                    self.retries.insert(
                        key,
                        RetryStatement {
                            command: name.to_owned(),
                            response: response.clone(),
                        },
                    );
                }
                response
            };
            if field.is_none() {
                return Ok(response);
            }
            n += integer(response.get("n"), 0)?;
            modified += integer(response.get("nModified"), 0)?;
            if let Ok(values) = response.get_array("upserted") {
                for value in values {
                    let mut value = value
                        .as_document()
                        .cloned()
                        .ok_or_else(|| bad("invalid upsert result"))?;
                    value.insert("index", index as i32);
                    upserted.push(value);
                }
            }
            if let Ok(values) = response.get_array("writeErrors") {
                for value in values {
                    let mut value = value
                        .as_document()
                        .cloned()
                        .ok_or_else(|| bad("invalid write error"))?;
                    value.insert("index", index as i32);
                    errors.push(value);
                }
                if ordered {
                    break;
                }
            }
        }
        let mut result = write_response(n as i32, errors);
        if name == "update" {
            result.insert("nModified", modified as i32);
        }
        if !upserted.is_empty() {
            result.insert("upserted", upserted);
        }
        Ok(result)
    }
    fn purge_transaction_cursors(&mut self, key: &[u8]) {
        let ids: Vec<_> = self
            .cursors
            .iter()
            .filter_map(|(id, cursor)| {
                (cursor.owner.transaction.as_deref() == Some(key)).then_some(*id)
            })
            .collect();
        for id in ids {
            let cursor = self.cursors.remove(&id).unwrap();
            self.retired_cursors
                .insert(id, (cursor.namespace, cursor.owner));
        }
    }
    fn commit(&mut self, tx: Transaction) -> Result<()> {
        let mut candidate = self.store.clone();
        for ns in &tx.touched {
            let original = collection(&tx.base, ns);
            let working = collection(&tx.working, ns);
            let mut ids: Vec<Bson> = vec![];
            for id in tx
                .working
                .versions
                .get(ns)
                .into_iter()
                .flatten()
                .map(|(id, _)| id)
                .chain(original.iter().chain(working).filter_map(|d| d.get("_id")))
            {
                if !ids.iter().any(|existing| query::equal(existing, id)) {
                    ids.push(id.clone());
                }
            }
            for id in ids {
                let before_version = tx.base.version(ns, &id);
                let after_version = tx.working.version(ns, &id);
                if before_version == after_version {
                    continue;
                }
                if candidate.version(ns, &id) != before_version {
                    return Err(Error::new(112, "snapshot write conflict"));
                }
                let after = working
                    .iter()
                    .find(|d| d.get("_id").is_some_and(|v| query::equal(v, &id)));
                if after.is_some() || candidate.collections.contains_key(ns) {
                    let current = candidate.collections.entry(ns.clone()).or_default();
                    if let Some(position) = current
                        .docs
                        .iter()
                        .position(|d| d.get("_id").is_some_and(|v| query::equal(v, &id)))
                    {
                        current.docs.remove(position);
                    }
                    if let Some(after) = after {
                        current.docs.push(after.clone());
                    }
                }
                candidate.set_version(ns, id, after_version);
            }
            if let Some(current) = candidate.collections.get(ns) {
                validate_indexes(current)?;
            }
        }
        self.store = candidate;
        Ok(())
    }
    fn cursor(
        &mut self,
        ns: String,
        mut docs: Vec<Document>,
        size: i64,
        single: bool,
        first: bool,
    ) -> Result<Document> {
        if size < 0 {
            return Err(bad("batchSize must be nonnegative"));
        }
        let mut count = 0;
        let mut bytes = 1024;
        for doc in docs.iter().take(size as usize) {
            let length = bson::to_vec(doc)
                .map_err(|_| bad("invalid cursor document"))?
                .len()
                + 16;
            if count > 0 && bytes + length > 16 * 1024 * 1024 {
                break;
            }
            bytes += length;
            count += 1;
        }
        let rest = docs.split_off(count);
        let id = if !single && !rest.is_empty() {
            self.next_cursor += 1;
            self.cursors.insert(
                self.next_cursor,
                Cursor {
                    namespace: ns.clone(),
                    remaining: rest,
                    owner: self.cursor_owner.clone(),
                },
            );
            self.next_cursor
        } else {
            0
        };
        let mut cursor = doc! {"id":id,"ns":ns};
        cursor.insert(
            if first { "firstBatch" } else { "nextBatch" },
            docs.into_iter().map(Bson::Document).collect::<Vec<_>>(),
        );
        Ok(doc! {"ok":1.0,"cursor":cursor})
    }
    fn operation(&mut self, store: &mut Store, c: &Document, name: &str) -> Result<Document> {
        let ns = if ["insert", "update", "findAndModify", "findandmodify"].contains(&name) {
            namespace(c, name).ok()
        } else {
            None
        };
        let existed = ns
            .as_ref()
            .is_some_and(|ns| store.collections.contains_key(ns));
        let before = if [
            "insert",
            "update",
            "delete",
            "findAndModify",
            "findandmodify",
            "drop",
            "dropDatabase",
        ]
        .contains(&name)
        {
            Some(store.collections.clone())
        } else {
            None
        };
        self.write_events.clear();
        self.cursor_owner = CursorOwner::from_command(c)?;
        let result = self.command(store, c, name);
        self.cursor_owner = CursorOwner::default();
        if let Some(before) = before {
            let namespaces: BTreeSet<_> = before
                .keys()
                .chain(store.collections.keys())
                .cloned()
                .collect();
            for namespace in namespaces {
                let old = before
                    .get(&namespace)
                    .map_or(&[][..], |c| c.docs.as_slice());
                let new = collection(store, &namespace);
                let mut ids = vec![];
                for id in old.iter().chain(new).filter_map(|d| d.get("_id")) {
                    if !ids.iter().any(|v| query::equal(v, id)) {
                        ids.push(id.clone());
                    }
                }
                let changed: Vec<_> = ids
                    .into_iter()
                    .filter(|id| {
                        self.write_events.iter().any(|event| {
                            event.namespace == namespace && query::equal(&event.id, id)
                        }) || old
                            .iter()
                            .find(|d| d.get("_id").is_some_and(|v| query::equal(v, id)))
                            != new
                                .iter()
                                .find(|d| d.get("_id").is_some_and(|v| query::equal(v, id)))
                    })
                    .collect();
                for id in changed {
                    self.next_revision += 1;
                    store.set_version(&namespace, id, self.next_revision);
                }
            }
        }
        if let Some(ns) = ns
            && !existed
            && store
                .collections
                .get(&ns)
                .is_some_and(|coll| coll.docs.is_empty())
        {
            store.collections.remove(&ns);
        }
        result
    }
    fn command(&mut self, store: &mut Store, c: &Document, name: &str) -> Result<Document> {
        match name {
            "insert" => {
                options(
                    c,
                    &[
                        "insert",
                        "documents",
                        "ordered",
                        "bypassDocumentValidation",
                        "stmtId",
                        "stmtIds",
                    ],
                )?;
                if boolean(c, "bypassDocumentValidation", false)? {
                    return Err(Error::new(72, "document validation bypass unsupported"));
                }
                let ns = namespace(c, name)?;
                let values = array(c, "documents")?;
                if values.is_empty() {
                    return Err(bad("documents must not be empty"));
                }
                let ordered = boolean(c, "ordered", true)?;
                let coll = store.collections.entry(ns.clone()).or_default();
                let mut n = 0;
                let mut errors = vec![];
                for (i, value) in values.into_iter().enumerate() {
                    let result = (|| {
                        let mut document = value
                            .as_document()
                            .cloned()
                            .ok_or_else(|| bad("insert requires documents"))?;
                        prepare_document(&mut document)?;
                        let id = document.get("_id").unwrap().clone();
                        let mut candidate = coll.clone();
                        candidate.docs.push(document);
                        validate_indexes(&candidate)?;
                        *coll = candidate;
                        self.write_events.push(WriteEvent {
                            namespace: ns.clone(),
                            id,
                        });
                        Ok(())
                    })();
                    match result {
                        Ok(()) => n += 1,
                        Err(e) => {
                            errors.push(write_error(i, e));
                            if ordered {
                                break;
                            }
                        }
                    }
                }
                Ok(write_response(n, errors))
            }
            "find" => {
                options(
                    c,
                    &[
                        "find",
                        "filter",
                        "projection",
                        "sort",
                        "skip",
                        "limit",
                        "batchSize",
                        "singleBatch",
                        "hint",
                        "returnKey",
                        "showRecordId",
                        "tailable",
                        "awaitData",
                        "noCursorTimeout",
                        "allowPartialResults",
                        "allowDiskUse",
                        "let",
                    ],
                )?;
                for flag in [
                    "returnKey",
                    "showRecordId",
                    "tailable",
                    "awaitData",
                    "noCursorTimeout",
                    "allowPartialResults",
                ] {
                    if boolean(c, flag, false)? {
                        return Err(Error::new(72, format!("unsupported option {flag}")));
                    }
                }
                if c.contains_key("let") {
                    return Err(Error::new(72, "find let unsupported"));
                }
                let ns = namespace(c, name)?;
                check_hint(store, &ns, c.get("hint"))?;
                let filter = object(c, "filter")?;
                query::validate_filter(&filter)?;
                let projection = object(c, "projection")?;
                query::validate_projection(&projection)?;
                let mut docs = vec![];
                for d in collection(store, &ns) {
                    if query::matches(d, &filter)? {
                        docs.push(d.clone())
                    }
                }
                query::sort(&mut docs, &object(c, "sort")?)?;
                let skip = integer(c.get("skip"), 0)?;
                if skip < 0 {
                    return Err(bad("skip must be nonnegative"));
                }
                docs = docs.into_iter().skip(skip as usize).collect();
                let limit = integer(c.get("limit"), 0)?;
                if limit != 0 {
                    docs.truncate(limit.unsigned_abs().min(usize::MAX as u64) as usize)
                }
                docs = docs
                    .iter()
                    .map(|d| query::project(d, &projection))
                    .collect::<Result<_>>()?;
                self.cursor(
                    ns,
                    docs,
                    integer(c.get("batchSize"), 101)?,
                    boolean(c, "singleBatch", false)? || limit < 0,
                    true,
                )
            }
            "getMore" => {
                options(c, &["getMore", "collection", "batchSize"])?;
                let id = integer(c.get("getMore"), 0)?;
                let batch_size = integer(c.get("batchSize"), 101)?;
                if batch_size < 0 {
                    return Err(bad("batchSize must be nonnegative"));
                }
                let requested = format!("{}.{}", string(c, "$db")?, string(c, "collection")?);
                let owner = self
                    .cursors
                    .get(&id)
                    .map(|c| &c.owner)
                    .or_else(|| self.retired_cursors.get(&id).map(|(_, owner)| owner))
                    .ok_or_else(|| Error::new(43, "cursor not found"))?;
                if owner.session != self.cursor_owner.session {
                    return Err(Error::new(
                        50738,
                        "getMore requires the cursor's original session",
                    ));
                }
                if owner.transaction.is_some() && self.cursor_owner.transaction.is_none() {
                    return Err(Error::new(
                        50740,
                        "cannot run a transaction cursor outside its transaction",
                    ));
                }
                if owner.transaction != self.cursor_owner.transaction {
                    return Err(Error::new(
                        50741,
                        "getMore requires the cursor's original transaction",
                    ));
                }
                let cursor = self
                    .cursors
                    .remove(&id)
                    .ok_or_else(|| Error::new(43, "cursor not found"))?;
                if cursor.namespace != requested {
                    self.cursors.insert(id, cursor);
                    return Err(bad("cursor namespace mismatch"));
                }
                self.cursor(cursor.namespace, cursor.remaining, batch_size, false, false)
            }
            "killCursors" => {
                options(c, &["killCursors", "cursors"])?;
                let ns = namespace(c, name)?;
                let mut killed = vec![];
                let mut unknown = vec![];
                for id in array(c, "cursors")? {
                    let n = integer(Some(&id), 0)?;
                    if self.cursors.get(&n).is_some_and(|v| v.namespace == ns)
                        || self
                            .retired_cursors
                            .get(&n)
                            .is_some_and(|(namespace, _)| *namespace == ns)
                    {
                        self.cursors.remove(&n);
                        self.retired_cursors.remove(&n);
                        killed.push(Bson::Int64(n));
                    } else {
                        unknown.push(Bson::Int64(n));
                    }
                }
                Ok(
                    doc! {"ok":1.0,"cursorsKilled":killed,"cursorsNotFound":unknown,"cursorsAlive":[],"cursorsUnknown":[]},
                )
            }
            "update" => {
                options(
                    c,
                    &[
                        "update",
                        "updates",
                        "ordered",
                        "bypassDocumentValidation",
                        "let",
                        "stmtId",
                        "stmtIds",
                    ],
                )?;
                if c.contains_key("let") || boolean(c, "bypassDocumentValidation", false)? {
                    return Err(Error::new(72, "unsupported update options"));
                }
                let ns = namespace(c, name)?;
                let specs = array(c, "updates")?;
                if specs.is_empty() {
                    return Err(bad("updates must not be empty"));
                }
                let ordered = boolean(c, "ordered", true)?;
                let coll = store.collections.entry(ns.clone()).or_default();
                let mut n = 0;
                let mut modified = 0;
                let mut errors = vec![];
                let mut upserted = vec![];
                for (i, spec) in specs.into_iter().enumerate() {
                    let result = (|| {
                        let spec = spec
                            .as_document()
                            .ok_or_else(|| bad("update must be object"))?;
                        options(spec, &["q", "u", "multi", "upsert", "hint"])?;
                        check_collection_hint(coll, spec.get("hint"))?;
                        let q = object(spec, "q")?;
                        query::validate_filter(&q)?;
                        let update = spec.get("u").ok_or_else(|| bad("missing update"))?;
                        query::validate_update(update)?;
                        let multi = boolean(spec, "multi", false)?;
                        let mut matched = vec![];
                        for (position, d) in coll.docs.iter().enumerate() {
                            if query::matches(d, &q)? {
                                matched.push(position);
                                if !multi {
                                    break;
                                }
                            }
                        }
                        if matched.is_empty() && boolean(spec, "upsert", false)? {
                            let seed = upsert_seed(&q)?;
                            let mut inserted = query::update(&seed, update, true)?;
                            prepare_document(&mut inserted)?;
                            let id = inserted.get("_id").unwrap().clone();
                            let mut candidate = coll.clone();
                            candidate.docs.push(inserted);
                            validate_indexes(&candidate)?;
                            *coll = candidate;
                            self.write_events.push(WriteEvent {
                                namespace: ns.clone(),
                                id: id.clone(),
                            });
                            n += 1;
                            upserted.push(doc! {"index":i as i32,"_id":id});
                        } else {
                            for position in matched {
                                let old = &coll.docs[position];
                                let mut new = query::update(old, update, false)?;
                                prepare_document(&mut new)?;
                                check_immutable(old, &new)?;
                                let changed = *old != new;
                                let id = old.get("_id").unwrap().clone();
                                let mut candidate = coll.clone();
                                candidate.docs[position] = new;
                                validate_indexes(&candidate)?;
                                *coll = candidate;
                                if changed {
                                    self.write_events.push(WriteEvent {
                                        namespace: ns.clone(),
                                        id,
                                    });
                                }
                                n += 1;
                                if changed {
                                    modified += 1
                                }
                            }
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        errors.push(write_error(i, e));
                        if ordered {
                            break;
                        }
                    }
                }
                let mut response = write_response(n, errors);
                response.insert("nModified", modified);
                if !upserted.is_empty() {
                    response.insert("upserted", upserted);
                }
                Ok(response)
            }
            "delete" => {
                options(
                    c,
                    &["delete", "deletes", "ordered", "let", "stmtId", "stmtIds"],
                )?;
                if c.contains_key("let") {
                    return Err(Error::new(72, "delete let unsupported"));
                }
                let ns = namespace(c, name)?;
                let specs = array(c, "deletes")?;
                if specs.is_empty() {
                    return Err(bad("deletes must not be empty"));
                }
                let ordered = boolean(c, "ordered", true)?;
                let mut n = 0;
                let mut errors = vec![];
                for (i, spec) in specs.into_iter().enumerate() {
                    let result = (|| {
                        let spec = spec
                            .as_document()
                            .ok_or_else(|| bad("delete must be object"))?;
                        options(spec, &["q", "limit", "hint"])?;
                        let q = object(spec, "q")?;
                        query::validate_filter(&q)?;
                        let limit = integer(spec.get("limit"), 0)?;
                        if limit != 0 && limit != 1 {
                            return Err(bad("delete limit must be zero or one"));
                        }
                        if let Some(coll) = store.collections.get_mut(&ns) {
                            check_collection_hint(coll, spec.get("hint"))?;
                            let mut positions = vec![];
                            for (p, d) in coll.docs.iter().enumerate() {
                                if query::matches(d, &q)? {
                                    positions.push(p);
                                    if limit == 1 {
                                        break;
                                    }
                                }
                            }
                            n += positions.len() as i32;
                            for p in positions.into_iter().rev() {
                                let removed = coll.docs.remove(p);
                                self.write_events.push(WriteEvent {
                                    namespace: ns.clone(),
                                    id: removed.get("_id").unwrap().clone(),
                                });
                            }
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        errors.push(write_error(i, e));
                        if ordered {
                            break;
                        }
                    }
                }
                Ok(write_response(n, errors))
            }
            "findAndModify" | "findandmodify" => {
                options(
                    c,
                    &[
                        name,
                        "query",
                        "sort",
                        "remove",
                        "update",
                        "new",
                        "fields",
                        "upsert",
                        "bypassDocumentValidation",
                        "hint",
                        "let",
                        "stmtId",
                    ],
                )?;
                if c.contains_key("let") || boolean(c, "bypassDocumentValidation", false)? {
                    return Err(Error::new(72, "unsupported findAndModify options"));
                }
                let ns = namespace(c, name)?;
                query::validate_projection(&object(c, "fields")?)?;
                if let Some(update) = c.get("update") {
                    query::validate_update(update)?;
                }
                let coll = store.collections.entry(ns.clone()).or_default();
                check_collection_hint(coll, c.get("hint"))?;
                let q = object(c, "query")?;
                query::validate_filter(&q)?;
                let mut docs = vec![];
                for d in &coll.docs {
                    if query::matches(d, &q)? {
                        docs.push(d.clone())
                    }
                }
                query::sort(&mut docs, &object(c, "sort")?)?;
                let old = docs.first().cloned();
                let remove = boolean(c, "remove", false)?;
                let new = boolean(c, "new", false)?;
                let upsert = boolean(c, "upsert", false)?;
                if remove && (c.contains_key("update") || upsert || new) {
                    return Err(bad("remove cannot be combined with update, new or upsert"));
                }
                let mut candidate = coll.clone();
                let mut updated = None;
                let mut inserted = false;
                if let Some(old) = &old {
                    let position = candidate
                        .docs
                        .iter()
                        .position(|d| d.get("_id") == old.get("_id"))
                        .unwrap();
                    if remove {
                        candidate.docs.remove(position);
                    } else {
                        let mut value = query::update(
                            old,
                            c.get("update").ok_or_else(|| bad("missing update"))?,
                            false,
                        )?;
                        prepare_document(&mut value)?;
                        check_immutable(old, &value)?;
                        candidate.docs[position] = value.clone();
                        updated = Some(value);
                    }
                } else if upsert {
                    let mut value = query::update(
                        &upsert_seed(&q)?,
                        c.get("update").ok_or_else(|| bad("missing update"))?,
                        true,
                    )?;
                    prepare_document(&mut value)?;
                    inserted = true;
                    candidate.docs.push(value.clone());
                    updated = Some(value);
                } else if !remove && !c.contains_key("update") {
                    return Err(bad("missing update"));
                }
                validate_indexes(&candidate)?;
                let selected = if new { updated.as_ref() } else { old.as_ref() };
                let value = selected
                    .map(|d| query::project(d, &object(c, "fields")?))
                    .transpose()?
                    .map(Bson::Document)
                    .unwrap_or(Bson::Null);
                if old != updated
                    && let Some(doc) = old.as_ref().or(updated.as_ref())
                {
                    self.write_events.push(WriteEvent {
                        namespace: ns.clone(),
                        id: doc.get("_id").unwrap().clone(),
                    });
                }
                *coll = candidate;
                let mut last =
                    doc! {"n":i32::from(old.is_some()||inserted),"updatedExisting":old.is_some()};
                if inserted {
                    last.insert(
                        "upserted",
                        updated.as_ref().unwrap().get("_id").unwrap().clone(),
                    );
                }
                Ok(doc! {"ok":1.0,"value":value,"lastErrorObject":last})
            }
            "count" => {
                options(c, &["count", "query", "limit", "skip", "hint"])?;
                let ns = namespace(c, name)?;
                check_hint(store, &ns, c.get("hint"))?;
                let q = object(c, "query")?;
                query::validate_filter(&q)?;
                let mut count = 0i64;
                for d in collection(store, &ns) {
                    if query::matches(d, &q)? {
                        count += 1
                    }
                }
                let skip = integer(c.get("skip"), 0)?;
                if skip < 0 {
                    return Err(bad("negative skip"));
                }
                count = (count - skip).max(0);
                let limit = integer(c.get("limit"), 0)?
                    .checked_abs()
                    .ok_or_else(|| bad("limit is out of range"))?;
                if limit > 0 {
                    count = count.min(limit)
                }
                Ok(doc! {"ok":1.0,"n":count})
            }
            "distinct" => {
                options(c, &["distinct", "key", "query"])?;
                let ns = namespace(c, name)?;
                let key = string(c, "key")?;
                let filter = object(c, "query")?;
                query::validate_filter(&filter)?;
                let mut values = vec![];
                for d in collection(store, &ns) {
                    if !query::matches(d, &filter)? {
                        continue;
                    }
                    for value in query::values(d, key) {
                        let members = match value {
                            Bson::Array(v) => v.clone(),
                            v => vec![v.clone()],
                        };
                        for value in members {
                            if !values.iter().any(|v| query::equal(v, &value)) {
                                values.push(value)
                            }
                        }
                    }
                }
                Ok(doc! {"ok":1.0,"values":values})
            }
            "aggregate" => {
                options(
                    c,
                    &[
                        "aggregate",
                        "pipeline",
                        "cursor",
                        "allowDiskUse",
                        "hint",
                        "bypassDocumentValidation",
                        "let",
                    ],
                )?;
                if c.contains_key("let") || boolean(c, "bypassDocumentValidation", false)? {
                    return Err(Error::new(72, "unsupported aggregate options"));
                }
                let ns = namespace(c, name)?;
                check_hint(store, &ns, c.get("hint"))?;
                let db = split_ns(&ns).0;
                let collections = store
                    .collections
                    .iter()
                    .filter_map(|(ns, c)| {
                        let (database, name) = split_ns(ns);
                        (db == database).then(|| (name.to_owned(), c.docs.clone()))
                    })
                    .collect();
                let docs = query::aggregate(
                    collection(store, &ns).to_vec(),
                    &array(c, "pipeline")?,
                    &collections,
                )?;
                let cursor = object(c, "cursor")?;
                options(&cursor, &["batchSize"])?;
                self.cursor(
                    ns,
                    docs,
                    integer(cursor.get("batchSize"), 101)?,
                    false,
                    true,
                )
            }
            "create" => {
                options(c, &["create"])?;
                let ns = namespace(c, name)?;
                if store.collections.contains_key(&ns) {
                    return Err(Error::new(48, "collection already exists"));
                }
                store.collections.insert(ns, Collection::default());
                Ok(doc! {"ok":1.0})
            }
            "drop" => {
                options(c, &["drop"])?;
                let ns = namespace(c, name)?;
                let collection = store
                    .collections
                    .remove(&ns)
                    .ok_or_else(|| Error::new(26, "namespace not found"))?;
                Ok(doc! {"ok":1.0,"ns":ns,"nIndexesWas":collection.indexes.len() as i32})
            }
            "dropDatabase" => {
                options(c, &["dropDatabase"])?;
                let db = string(c, "$db")?;
                store.collections.retain(|ns, _| split_ns(ns).0 != db);
                Ok(doc! {"ok":1.0,"dropped":db})
            }
            "listDatabases" => {
                options(
                    c,
                    &["listDatabases", "nameOnly", "filter", "authorizedDatabases"],
                )?;
                let mut names = BTreeSet::new();
                for ns in store.collections.keys() {
                    names.insert(split_ns(ns).0);
                }
                let filter = object(c, "filter")?;
                query::validate_filter(&filter)?;
                let mut databases = vec![];
                for name in names {
                    let d = doc! {"name":name,"sizeOnDisk":0i64,"empty":false};
                    if query::matches(&d, &filter)? {
                        databases.push(d)
                    }
                }
                Ok(doc! {"ok":1.0,"databases":databases,"totalSize":0i64,"totalSizeMb":0i64})
            }
            "listCollections" => {
                options(
                    c,
                    &[
                        "listCollections",
                        "filter",
                        "cursor",
                        "nameOnly",
                        "authorizedCollections",
                    ],
                )?;
                let db = string(c, "$db")?;
                let filter = object(c, "filter")?;
                query::validate_filter(&filter)?;
                let mut docs = vec![];
                for ns in store.collections.keys() {
                    let (database, name) = split_ns(ns);
                    if database == db {
                        let d = doc! {"name":name,"type":"collection","options":{},"info":{"readOnly":false},"idIndex":Index::id().document()};
                        if query::matches(&d, &filter)? {
                            docs.push(d)
                        }
                    }
                }
                let cursor = object(c, "cursor")?;
                self.cursor(
                    format!("{db}.$cmd.listCollections"),
                    docs,
                    integer(cursor.get("batchSize"), 101)?,
                    false,
                    true,
                )
            }
            "createIndexes" => {
                options(c, &["createIndexes", "indexes", "commitQuorum"])?;
                if c.contains_key("commitQuorum") {
                    return Err(Error::new(72, "index commitQuorum unsupported"));
                }
                let ns = namespace(c, name)?;
                let coll = store.collections.entry(ns.clone()).or_default();
                let before = coll.indexes.len();
                let mut candidate = coll.clone();
                for spec in array(c, "indexes")? {
                    let spec = spec
                        .as_document()
                        .ok_or_else(|| bad("index specification must be object"))?;
                    options(
                        spec,
                        &[
                            "key",
                            "name",
                            "unique",
                            "sparse",
                            "partialFilterExpression",
                            "v",
                            "background",
                            "expireAfterSeconds",
                        ],
                    )?;
                    let keys = object(spec, "key")?;
                    if keys.is_empty() {
                        return Err(bad("empty index key"));
                    }
                    for value in keys.values() {
                        if !matches!(value, Bson::Int32(1 | -1) | Bson::Int64(1 | -1)) {
                            return Err(Error::new(
                                72,
                                "only ascending/descending indexes are supported",
                            ));
                        }
                    }
                    let name = string(spec, "name")?.to_owned();
                    let index = Index {
                        name: name.clone(),
                        keys,
                        unique: boolean(spec, "unique", false)?,
                        sparse: boolean(spec, "sparse", false)?,
                        partial: spec
                            .get("partialFilterExpression")
                            .map(|v| {
                                v.as_document()
                                    .cloned()
                                    .ok_or_else(|| bad("partial filter must be object"))
                            })
                            .transpose()?,
                        ttl: spec
                            .get("expireAfterSeconds")
                            .map(|v| integer(Some(v), 0))
                            .transpose()?,
                    };
                    if index.sparse && index.partial.is_some() {
                        return Err(Error::new(
                            67,
                            "sparse and partial indexes cannot be combined",
                        ));
                    }
                    if spec
                        .get("v")
                        .is_some_and(|v| !query::equal(v, &Bson::Int32(2)))
                    {
                        return Err(Error::new(72, "only index version 2 supported"));
                    }
                    if let Some(ttl) = index.ttl
                        && (ttl < 0 || index.keys.len() != 1 || index.keys.contains_key("_id"))
                    {
                        return Err(Error::new(
                            72,
                            "TTL requires one non-_id field and nonnegative expireAfterSeconds",
                        ));
                    }
                    if let Some(existing) = candidate.indexes.iter().find(|i| i.name == name) {
                        if existing.document() != index.document() {
                            return Err(Error::new(85, "index options conflict"));
                        }
                        continue;
                    }
                    if let Some(filter) = &index.partial {
                        query::matches(&Document::new(), filter)?;
                    }
                    candidate.indexes.push(index);
                }
                validate_indexes(&candidate)?;
                *coll = candidate;
                Ok(
                    doc! {"ok":1.0,"numIndexesBefore":before as i32,"numIndexesAfter":coll.indexes.len() as i32,"createdCollectionAutomatically":false},
                )
            }
            "listIndexes" => {
                options(c, &["listIndexes", "cursor", "includeBuildUUIDs"])?;
                let ns = namespace(c, name)?;
                let coll = store
                    .collections
                    .get(&ns)
                    .ok_or_else(|| Error::new(26, "namespace not found"))?;
                let docs = coll.indexes.iter().map(Index::document).collect();
                let cursor = object(c, "cursor")?;
                self.cursor(
                    ns,
                    docs,
                    integer(cursor.get("batchSize"), 101)?,
                    false,
                    true,
                )
            }
            "dropIndexes" => {
                options(c, &["dropIndexes", "index"])?;
                let ns = namespace(c, name)?;
                let coll = store
                    .collections
                    .get_mut(&ns)
                    .ok_or_else(|| Error::new(26, "namespace not found"))?;
                let before = coll.indexes.len();
                let target = c.get("index").ok_or_else(|| bad("missing index"))?;
                if target == &Bson::String("_id_".into()) {
                    return Err(Error::new(72, "cannot drop _id index"));
                }
                if target == &Bson::String("*".into()) {
                    coll.indexes.retain(|i| i.name == "_id_");
                } else {
                    let position = coll
                        .indexes
                        .iter()
                        .position(|i| {
                            target == &Bson::String(i.name.clone())
                                || target == &Bson::Document(i.keys.clone())
                        })
                        .ok_or_else(|| Error::new(27, "index not found"))?;
                    if coll.indexes[position].name == "_id_" {
                        return Err(Error::new(72, "cannot drop _id index"));
                    }
                    coll.indexes.remove(position);
                }
                Ok(doc! {"ok":1.0,"nIndexesWas":before as i32})
            }
            _ => Err(Error::new(59, format!("unsupported command {name}"))),
        }
    }
}

fn prepare_document(document: &mut Document) -> Result<()> {
    if !document.contains_key("_id") {
        document.insert("_id", bson::oid::ObjectId::new());
    }
    if matches!(
        document.get("_id"),
        Some(Bson::Array(_) | Bson::RegularExpression(_))
    ) {
        return Err(bad("invalid _id type"));
    }
    if bson::to_vec(document)
        .map_err(|_| bad("invalid document"))?
        .len()
        > 16 * 1024 * 1024
    {
        return Err(bad("document exceeds maximum BSON size"));
    }
    for key in document.keys() {
        if key.starts_with('$') {
            return Err(bad("top-level field names cannot start with $"));
        }
    }
    Ok(())
}
fn check_immutable(old: &Document, new: &Document) -> Result<()> {
    match (old.get("_id"), new.get("_id")) {
        (Some(a), Some(b)) if query::equal(a, b) => Ok(()),
        _ => Err(Error::new(66, "_id is immutable")),
    }
}
fn upsert_seed(filter: &Document) -> Result<Document> {
    fn collect(filter: &Document, fields: &mut Document) -> Result<()> {
        for (key, value) in filter {
            if key == "$and" {
                for filter in value.as_array().ok_or_else(|| bad("$and requires array"))? {
                    collect(
                        filter
                            .as_document()
                            .ok_or_else(|| bad("$and requires objects"))?,
                        fields,
                    )?;
                }
            } else if !key.starts_with('$') {
                let value = match value {
                    Bson::Document(d) if d.len() == 1 && d.contains_key("$eq") => {
                        d.get("$eq").unwrap()
                    }
                    Bson::Document(d) if d.keys().any(|k| k.starts_with('$')) => continue,
                    value => value,
                };
                if let Some(old) = fields.get(key)
                    && !query::equal(old, value)
                {
                    return Err(bad("conflicting equality predicates for upsert"));
                }
                fields.insert(key, value.clone());
            }
        }
        Ok(())
    }
    let mut fields = Document::new();
    collect(filter, &mut fields)?;
    query::update(
        &Document::new(),
        &Bson::Document(doc! {"$set":fields}),
        true,
    )
}

fn write_error(index: usize, error: Error) -> Document {
    doc! {"index":index as i32,"code":error.code,"errmsg":error.message}
}
fn write_response(n: i32, errors: Vec<Document>) -> Document {
    let mut d = doc! {"ok":1.0,"n":n};
    if !errors.is_empty() {
        d.insert("writeErrors", errors);
    }
    d
}
fn check_hint(store: &Store, ns: &str, hint: Option<&Bson>) -> Result<()> {
    if let Some(hint) = hint {
        let coll = store
            .collections
            .get(ns)
            .ok_or_else(|| bad("hint does not correspond to an index"))?;
        check_collection_hint(coll, Some(hint))?;
    }
    Ok(())
}
fn check_collection_hint(coll: &Collection, hint: Option<&Bson>) -> Result<()> {
    if let Some(hint) = hint {
        if hint == &Bson::Document(doc! {"$natural":1})
            || hint == &Bson::Document(doc! {"$natural":-1})
        {
            return Ok(());
        }
        if !coll.indexes.iter().any(|i| {
            hint == &Bson::String(i.name.clone()) || hint == &Bson::Document(i.keys.clone())
        }) {
            return Err(bad("hint does not correspond to an index"));
        }
    }
    Ok(())
}
fn validate_indexes(coll: &Collection) -> Result<()> {
    for index in &coll.indexes {
        let mut entries: Vec<Vec<Bson>> = vec![];
        for document in &coll.docs {
            if let Some(filter) = &index.partial
                && !query::matches(document, filter)?
            {
                continue;
            }
            let fields: Vec<_> = index
                .keys
                .keys()
                .map(|key| query::values(document, key))
                .collect();
            if index.sparse && fields.iter().all(Vec::is_empty) {
                continue;
            }
            let mut tuples = vec![vec![]];
            let mut arrays = 0;
            for field in fields {
                let multikey = field.len() > 1 || field.iter().any(|v| matches!(v, Bson::Array(_)));
                if multikey {
                    arrays += 1;
                    if arrays > 1 {
                        return Err(Error::new(
                            171,
                            "compound indexes spanning multiple array paths are unsupported",
                        ));
                    }
                }
                let values = if field.is_empty() {
                    vec![Bson::Null]
                } else {
                    field
                        .into_iter()
                        .flat_map(|value| match value {
                            Bson::Array(values) if values.is_empty() => vec![Bson::Undefined],
                            Bson::Array(values) => values.clone(),
                            v => vec![v.clone()],
                        })
                        .collect()
                };
                let mut expanded = vec![];
                for tuple in &tuples {
                    for value in &values {
                        let mut tuple = tuple.clone();
                        tuple.push(value.clone());
                        expanded.push(tuple);
                    }
                }
                tuples = expanded;
            }
            if index.unique {
                for tuple in &tuples {
                    if entries
                        .iter()
                        .any(|entry| entry.iter().zip(tuple).all(|(a, b)| query::equal(a, b)))
                    {
                        return Err(Error::new(
                            11000,
                            format!("duplicate key for index {}", index.name),
                        ));
                    }
                }
                for tuple in tuples {
                    if !entries
                        .iter()
                        .any(|entry| entry.iter().zip(&tuple).all(|(a, b)| query::equal(a, b)))
                    {
                        entries.push(tuple);
                    }
                }
            }
        }
    }
    Ok(())
}
