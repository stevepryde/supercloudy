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
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    pub fn with_detail(mut self, name: impl Into<String>, value: Value) -> Self {
        self.details.insert(name.into(), value);
        self
    }
}
