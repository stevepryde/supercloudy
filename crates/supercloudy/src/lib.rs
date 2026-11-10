//! Local in-memory cloud and database protocol emulators.

mod dynamodb;
mod fault_config;
mod faults;
mod firebase;
mod lambda;
pub mod mongodb;
mod openai;
mod protocol;
mod routing;
mod s3;
pub mod server;
mod ssm;

pub mod qdrant;
