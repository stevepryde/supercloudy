//! Local cloud service HTTP server.

mod dynamodb;
mod fault_config;
mod faults;
mod openai;
mod protocol;
mod routing;
mod s3;
pub mod server;
mod ssm;
