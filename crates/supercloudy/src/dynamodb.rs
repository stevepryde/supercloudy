mod api;
mod batches;
pub(crate) mod error;
mod expressions;
pub(crate) mod faults;
mod operations;
mod storage;
mod transactions;
mod value;

#[cfg(test)]
mod properties;

use error::{Error, Result};
use faults::{Effect, Faults};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub(crate) struct Service {
    database: Arc<RwLock<storage::Database>>,
    faults: Faults,
}
pub(crate) fn operation(target: Option<&str>) -> Option<&str> {
    target
        .and_then(|t| t.strip_prefix("DynamoDB_20120810."))
        .filter(|op| {
            matches!(
                *op,
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
            )
        })
}
impl Service {
    pub fn with_scheduler(faults: Faults) -> Self {
        Self {
            database: Arc::default(),
            faults,
        }
    }
    fn fault(&self, op: &str, raw: &Value) -> Result<batches::Deferred> {
        match self.faults.effect(op, raw) {
            Some(Effect::Error(e)) => Err(e),
            Some(Effect::BatchDeferral(positions)) => Ok(positions),
            None => Ok(Default::default()),
        }
    }
    pub async fn dispatch(&self, op: &str, body: &[u8]) -> Result<Value> {
        let raw: Value = serde_json::from_slice(body)
            .map_err(|_| Error::new("SerializationException", "Invalid JSON body"))?;
        match op {
            "TransactWriteItems" => {
                let request = transactions::decode_write(&raw)?;
                let mut db = self.database.write().await;
                let plan = transactions::prepare_write(&db, &request)?;
                self.fault(op, &raw)?;
                Ok(transactions::commit(&mut db, &request, plan))
            }
            "TransactGetItems" => {
                let requests = transactions::decode_get(&raw)?;
                let db = self.database.read().await;
                let response = transactions::get(&db, &requests)?;
                self.fault(op, &raw)?;
                Ok(response)
            }
            "BatchWriteItem" => {
                let request = batches::decode_write(&raw, body.len())?;
                let mut db = self.database.write().await;
                let plan = batches::prepare_write(&db, &request)?;
                let deferred = self.fault(op, &raw)?;
                Ok(batches::commit(&mut db, &request, plan, &deferred))
            }
            "BatchGetItem" => {
                let request = batches::decode_get(&raw)?;
                let db = self.database.read().await;
                let response = batches::get(&db, &request, &Default::default())?;
                let deferred = self.fault(op, &raw)?;
                if deferred.is_empty() {
                    Ok(response)
                } else {
                    batches::get(&db, &request, &deferred)
                }
            }
            _ => {
                let request = operations::decode(op, body)?;
                if matches!(op, "DescribeTable" | "GetItem" | "Query" | "Scan") {
                    let db = self.database.read().await;
                    let response = operations::read(&db, op, &request)?;
                    self.fault(op, &raw)?;
                    Ok(response)
                } else {
                    let mut db = self.database.write().await;
                    match op {
                        "CreateTable" => {
                            let table = operations::prepare_create_table(&db, &request)?;
                            self.fault(op, &raw)?;
                            let response =
                                serde_json::json!({"TableDescription":table.description()});
                            db.tables.insert(request.table_name.clone(), table);
                            Ok(response)
                        }
                        "DeleteTable" => {
                            if !db.tables.contains_key(&request.table_name) {
                                return Err(Error::new(
                                    "ResourceNotFoundException",
                                    "Requested table does not exist",
                                ));
                            }
                            self.fault(op, &raw)?;
                            operations::write(&mut db, op, &request)
                        }
                        _ => {
                            let table =
                                db.tables.get_mut(&request.table_name).ok_or_else(|| {
                                    Error::new(
                                        "ResourceNotFoundException",
                                        "Requested table does not exist",
                                    )
                                })?;
                            let prepared = operations::prepare_write(table, op, &request)?;
                            self.fault(op, &raw)?;
                            if let Some(item) = prepared.candidate {
                                table.items.insert(prepared.key, item);
                            } else {
                                table.items.remove(&prepared.key);
                            }
                            Ok(prepared.response)
                        }
                    }
                }
            }
        }
    }
}
