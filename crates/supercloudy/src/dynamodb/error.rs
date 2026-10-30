#[derive(Debug)]
pub(crate) struct Error {
    pub code: &'static str,
    pub message: String,
}
pub(crate) type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new("ValidationException", message)
    }
}
