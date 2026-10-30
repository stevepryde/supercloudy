use serde_json::{Map, Value};

#[derive(Debug)]
pub(crate) struct Error {
    pub code: &'static str,
    pub message: String,
    pub status: u16,
    pub details: Map<String, Value>,
}
pub(crate) type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            status: 400,
            details: Map::new(),
        }
    }
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new("ValidationException", message)
    }
    pub fn transaction_canceled(reasons: Vec<Value>) -> Self {
        let codes = reasons
            .iter()
            .map(|reason| reason["Code"].as_str().expect("cancellation reason code"))
            .collect::<Vec<_>>()
            .join(", ");
        Self::new(
            "TransactionCanceledException",
            format!(
                "Transaction cancelled, please refer cancellation reasons for specific reasons [{codes}]"
            ),
        )
        .with_detail("CancellationReasons", Value::Array(reasons))
    }
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    pub fn with_detail(mut self, name: impl Into<String>, value: Value) -> Self {
        self.details.insert(name.into(), value);
        self
    }
}
