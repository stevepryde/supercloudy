//! Explicit Docker-backed oracle: cargo test --test mongodb_conformance -- --ignored
#[path = "mongodb_cases/mod.rs"]
mod cases;
#[path = "database_matrix/mod.rs"]
mod matrix;
use bson::Bson;
use testcontainers::{
    ImageExt,
    core::{CmdWaitFor, ExecCommand},
    runners::AsyncRunner,
};
use testcontainers_modules::mongo::Mongo;
#[tokio::test]
#[ignore = "requires Docker and pulls pinned MongoDB 8.0.28 replica-set image"]
async fn mongodb_8_replica_set_matches_emulator() {
    let container = Mongo::new()
        .with_tag("8.0.28")
        .with_cmd([
            "--replSet",
            "rs0",
            "--bind_ip_all",
            "--setParameter",
            "ttlMonitorSleepSecs=1",
        ])
        .start()
        .await
        .expect("MongoDB Testcontainer must start; Docker is required for this explicit suite");
    container
        .exec(
            ExecCommand::new([
                "mongosh",
                "--quiet",
                "--eval",
                "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let host = container.get_host().await.unwrap();
    let real = cases::client(&format!("mongodb://{host}:{port}/?directConnection=true")).await;
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let emulated = cases::client(&format!(
        "mongodb://{}/?directConnection=true",
        server.address()
    ))
    .await;
    assert_eq!(matrix::mongo(&emulated).await, matrix::mongo(&real).await);
    // Every scenario uses the same real driver commands and synthetic fixtures.
    for scenario in 0..14 {
        real.database("conformance").drop().await.unwrap();
        emulated.database("conformance").drop().await.unwrap();
        let (actual, expected) = match scenario {
            0 => (cases::crud(&emulated).await, cases::crud(&real).await),
            1 => (cases::indexes(&emulated).await, cases::indexes(&real).await),
            2 => (
                cases::aggregation(&emulated).await,
                cases::aggregation(&real).await,
            ),
            3 => (
                cases::transactions(&emulated).await,
                cases::transactions(&real).await,
            ),
            4 => (
                cases::concurrency(&emulated).await,
                cases::concurrency(&real).await,
            ),
            5 => (
                cases::query_semantics(&emulated).await,
                cases::query_semantics(&real).await,
            ),
            6 => (
                cases::update_semantics(&emulated).await,
                cases::update_semantics(&real).await,
            ),
            7 => (
                cases::metadata_and_cursors(&emulated).await,
                cases::metadata_and_cursors(&real).await,
            ),
            8 => (cases::ttl(&emulated).await, cases::ttl(&real).await),
            9 => (
                cases::disjoint_transactions(&emulated).await,
                cases::disjoint_transactions(&real).await,
            ),
            10 => (
                cases::large_documents(&emulated).await,
                cases::large_documents(&real).await,
            ),
            13 => (
                cases::retryable_writes(&emulated).await,
                cases::retryable_writes(&real).await,
            ),
            11 => (
                cases::transaction_aba_conflicts(&emulated).await,
                cases::transaction_aba_conflicts(&real).await,
            ),
            _ => (
                cases::cursor_ownership(&emulated).await,
                cases::cursor_ownership(&real).await,
            ),
        };
        assert_eq!(
            Bson::Document(actual),
            Bson::Document(expected),
            "scenario {scenario}"
        );
    }
    server.shutdown().await.unwrap();
}
