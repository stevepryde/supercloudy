#[path = "mongodb_cases/mod.rs"]
mod cases;
use bson::{Document, doc};

async fn setup() -> (supercloudy::mongodb::Server, mongodb::Client) {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let client = cases::client(&format!(
        "mongodb://{}/?directConnection=true",
        server.address()
    ))
    .await;
    (server, client)
}
#[tokio::test]
async fn real_driver_crud_cursors_and_updates() {
    let (server, client) = setup().await;
    let result = cases::crud(&client).await;
    assert_eq!(result.get_array("rows").unwrap().len(), 3);
    assert_eq!(result.get("nullCount").unwrap().as_i64(), Some(3));
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_unique_compound_sparse_partial_indexes() {
    let (server, client) = setup().await;
    cases::indexes(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_aggregation_and_expressions() {
    let (server, client) = setup().await;
    let result = cases::aggregation(&client).await;
    let grouped = result.get_array("grouped").unwrap();
    assert_eq!(grouped.len(), 2);
    assert_eq!(
        cases::canonical(grouped[0].as_document().unwrap().get("total").unwrap()),
        bson::Bson::Int64(6)
    );
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_snapshot_transactions_rollback_and_conflicts() {
    let (server, client) = setup().await;
    let result = cases::transactions(&client).await;
    let docs = result.get_array("accounts").unwrap();
    assert_eq!(
        docs[0].as_document().unwrap().get_i32("balance").unwrap(),
        8
    );
    assert_eq!(
        docs[1].as_document().unwrap().get_i32("balance").unwrap(),
        23
    );
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_atomic_concurrent_writes() {
    let (server, client) = setup().await;
    cases::concurrency(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn isolation_shutdown_ttl_and_unsupported_operations() {
    let (server, client) = setup().await;
    let (other, other_client) = setup().await;
    let coll = client.database("conformance").collection::<Document>("ttl");
    coll.insert_one(doc! {"_id":1,"at":bson::DateTime::from_millis(1)})
        .await
        .unwrap();
    cases::command(&client,doc!{"createIndexes":"ttl","indexes":[{"key":{"at":1},"name":"at_ttl","expireAfterSeconds":0}]}).await;
    assert_eq!(coll.count_documents(doc! {}).await.unwrap(), 0);
    assert!(
        other_client
            .database("conformance")
            .list_collection_names()
            .await
            .unwrap()
            .is_empty()
    );
    let error = client
        .database("conformance")
        .run_command(doc! {"aggregate":"ttl","pipeline":[{"$out":"bad"}],"cursor":{}})
        .await
        .unwrap_err();
    assert_eq!(cases::error_code(error), 2);
    let address = server.address();
    server.shutdown().await.unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn real_driver_bson_query_semantics() {
    let (server, client) = setup().await;
    let result = cases::query_semantics(&client).await;
    assert_eq!(result.get_array("filters").unwrap().len(), 15);
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_update_modifiers_upsert_and_failed_write_atomicity() {
    let (server, client) = setup().await;
    let result = cases::update_semantics(&client).await;
    let rows = result.get_array("rows").unwrap();
    assert_eq!(rows[0].as_document().unwrap().get_i32("n").unwrap(), 6);
    assert_eq!(rows[1].as_document().unwrap().get_i32("n").unwrap(), 2);
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_metadata_cursor_kill_and_errors() {
    let (server, client) = setup().await;
    cases::metadata_and_cursors(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_disjoint_snapshot_transactions_merge() {
    let (server, client) = setup().await;
    cases::disjoint_transactions(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_partial_ttl_dates_arrays_and_non_dates() {
    let (server, client) = setup().await;
    cases::ttl(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_large_documents_are_split_across_cursor_batches() {
    let (server, client) = setup().await;
    cases::large_documents(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_session_retryable_writes_are_not_reapplied() {
    let (server, client) = setup().await;
    cases::retryable_writes(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_transaction_detects_update_and_reinsert_aba() {
    let (server, client) = setup().await;
    cases::transaction_aba_conflicts(&client).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_driver_cursor_session_ownership_and_transaction_cleanup() {
    let (server, client) = setup().await;
    cases::cursor_ownership(&client).await;
    server.shutdown().await.unwrap();
}
