mod api;
mod error;
mod expressions;
mod operations;
mod storage;
mod value;

use error::Result;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub(crate) struct Service(Arc<RwLock<storage::Database>>);
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
            )
        })
}
impl Service {
    pub async fn dispatch(&self, op: &str, body: &[u8]) -> Result<Value> {
        let request = operations::decode(op, body)?;
        if matches!(op, "DescribeTable" | "GetItem" | "Query") {
            operations::read(&*self.0.read().await, op, &request)
        } else {
            operations::write(&mut *self.0.write().await, op, &request)
        }
    }
}
