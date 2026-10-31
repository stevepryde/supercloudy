//! Local cloud service HTTP server.

mod dynamodb;
mod fault_config;
mod faults;
mod firebase;
mod lambda;
mod openai;
mod protocol;
mod routing;
mod s3;
pub mod server;
mod ssm;
