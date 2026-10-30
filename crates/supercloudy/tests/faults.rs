use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region, retry::RetryConfig},
    types::{
        AttributeDefinition, AttributeValue as Av, BillingMode, KeySchemaElement, KeyType,
        KeysAndAttributes, Put, PutRequest, ScalarAttributeType, TransactWriteItem, WriteRequest,
    },
};
use serde_json::{Value, json};
use std::collections::HashMap;
use supercloudy::server::{FaultConfig, router_with_faults};
use tokio::{net::TcpListener, task::JoinHandle};

struct Emulator {
    client: Client,
    task: JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Emulator {
    async fn new(rules: Value, attempts: u32) -> Self {
        let config: FaultConfig = serde_json::from_value(json!({"rules": rules})).unwrap();
        let router = router_with_faults(config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
            .endpoint_url(endpoint)
            .retry_config(RetryConfig::standard().with_max_attempts(attempts))
            .build();
        Self {
            client: Client::from_conf(config),
            task,
        }
    }
    async fn table(&self, table: &str) {
        self.client
            .create_table()
            .table_name(table)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
    async fn get(&self, table: &str, id: &str) -> Option<HashMap<String, Av>> {
        self.client
            .get_item()
            .table_name(table)
            .set_key(Some(key(id)))
            .send()
            .await
            .unwrap()
            .item
    }
}
fn key(id: &str) -> HashMap<String, Av> {
    HashMap::from([("pk".into(), Av::S(id.into()))])
}
fn write(id: &str) -> WriteRequest {
    WriteRequest::builder()
        .put_request(
            PutRequest::builder()
                .set_item(Some(key(id)))
                .build()
                .unwrap(),
        )
        .build()
}

#[test]
fn incompatible_faults_are_rejected_before_startup() {
    for rule in [
        json!({"operation":"Unknown","first_request":1,"repeat":1,"effect":{"type":"throttle"}}),
        json!({"operation":"PutItem","first_request":1,"repeat":1,"effect":{"type":"batch_deferral","positions":{"synthetic-items":[0]}}}),
        json!({"operation":"GetItem","first_request":1,"repeat":1,"effect":{"type":"transaction_canceled","reasons":["TransactionConflict"]}}),
        json!({"operation":"PutItem","first_request":0,"repeat":1,"effect":{"type":"throttle"}}),
    ] {
        let config = serde_json::from_value(json!({"rules":[rule]})).unwrap();
        assert!(router_with_faults(config).is_err());
    }
    assert!(
        serde_json::from_value::<FaultConfig>(json!({
            "rules":[], "enabled":true
        }))
        .is_err()
    );
}

#[tokio::test]
async fn throttle_counts_only_valid_matching_requests_and_prevents_mutation() {
    let e = Emulator::new(
        json!([{
            "operation":"PutItem", "table":"synthetic-items", "first_request":2,"repeat":1,
            "effect":{"type":"throttle"}
        }]),
        1,
    )
    .await;
    e.table("synthetic-items").await;
    e.table("synthetic-other").await;
    let invalid = e
        .client
        .put_item()
        .table_name("synthetic-items")
        .item("pk", Av::N("7".into()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        invalid.as_service_error().unwrap().meta().code(),
        Some("ValidationException")
    );
    e.client
        .put_item()
        .table_name("synthetic-other")
        .set_item(Some(key("other")))
        .send()
        .await
        .unwrap();
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key("first")))
        .send()
        .await
        .unwrap();
    let error = e
        .client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key("deferred")))
        .send()
        .await
        .unwrap_err();
    assert!(
        error
            .as_service_error()
            .unwrap()
            .is_provisioned_throughput_exceeded_exception()
    );
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    assert!(e.get("synthetic-items", "deferred").await.is_none());
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key("deferred")))
        .send()
        .await
        .unwrap();
    assert!(e.get("synthetic-items", "deferred").await.is_some());
}

#[tokio::test]
async fn internal_errors_decode_and_sdk_retry_applies_increment_once() {
    let e = Emulator::new(
        json!([{
            "operation":"UpdateItem","first_request":1,"repeat":1,
            "effect":{"type":"internal_server_error"}
        }]),
        3,
    )
    .await;
    e.table("synthetic-items").await;
    e.client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key("counter")))
        .update_expression("ADD visits :one")
        .expression_attribute_values(":one", Av::N("1".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.get("synthetic-items", "counter").await.unwrap()["visits"],
        Av::N("1".into())
    );

    let e = Emulator::new(
        json!([{
            "operation":"PutItem","first_request":1,"repeat":2,
            "effect":{"type":"internal_server_error"}
        }]),
        1,
    )
    .await;
    e.table("synthetic-items").await;
    for _ in 0..2 {
        let error = e
            .client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(key("item")))
            .send()
            .await
            .unwrap_err();
        assert!(error.as_service_error().unwrap().is_internal_server_error());
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 500);
        assert!(e.get("synthetic-items", "item").await.is_none());
    }
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key("item")))
        .send()
        .await
        .unwrap();
    assert!(e.get("synthetic-items", "item").await.is_some());
}

#[tokio::test]
async fn transaction_fault_reasons_are_aligned_and_no_write_is_published() {
    let e = Emulator::new(
        json!([{
            "operation":"TransactWriteItems","table":"synthetic-items","first_request":1,"repeat":1,
            "effect":{"type":"transaction_canceled","reasons":["None","TransactionConflict"]}
        }]),
        1,
    )
    .await;
    e.table("synthetic-items").await;
    // A rule failing action index 1 cannot apply to a one-action request;
    // it must remain available for the next compatible transaction.
    e.client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("synthetic-items")
                        .set_item(Some(key("short")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(e.get("synthetic-items", "short").await.is_some());
    let actions = ["first", "second", "third"]
        .into_iter()
        .map(|id| {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("synthetic-items")
                        .set_item(Some(key(id)))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect::<Vec<_>>();
    let error = e
        .client
        .transact_write_items()
        .set_transact_items(Some(actions.clone()))
        .send()
        .await
        .unwrap_err();
    let modeled = error.as_service_error().unwrap();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(canceled) = modeled else {
        panic!("unexpected transaction error: {modeled:?}");
    };
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    assert_eq!(
        canceled.message(),
        Some(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [None, TransactionConflict, None]"
        )
    );
    assert_eq!(
        canceled
            .cancellation_reasons()
            .iter()
            .map(|reason| reason.code())
            .collect::<Vec<_>>(),
        [Some("None"), Some("TransactionConflict"), Some("None")]
    );
    for id in ["first", "second", "third"] {
        assert!(e.get("synthetic-items", id).await.is_none());
    }
    e.client
        .transact_write_items()
        .set_transact_items(Some(actions))
        .send()
        .await
        .unwrap();
    for id in ["first", "second", "third"] {
        assert!(e.get("synthetic-items", id).await.is_some());
    }
}

#[tokio::test]
async fn batch_deferrals_return_original_requests_and_make_partial_progress() {
    let e = Emulator::new(
        json!([
            {"operation":"BatchWriteItem","first_request":1,"repeat":1,
             "effect":{"type":"batch_deferral","positions":{"synthetic-items":[0,2]}}},
            {"operation":"BatchGetItem","first_request":1,"repeat":1,
             "effect":{"type":"batch_deferral","positions":{"synthetic-items":[1]}}}
        ]),
        1,
    )
    .await;
    e.table("synthetic-items").await;
    e.table("synthetic-other").await;
    let writes = vec![write("first"), write("second"), write("third")];
    let output = e
        .client
        .batch_write_item()
        .request_items("synthetic-items", writes.clone())
        .request_items("synthetic-other", vec![write("other")])
        .send()
        .await
        .unwrap();
    let remaining = output.unprocessed_items.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        remaining["synthetic-items"],
        [writes[0].clone(), writes[2].clone()]
    );
    assert!(e.get("synthetic-items", "first").await.is_none());
    assert!(e.get("synthetic-items", "third").await.is_none());
    assert!(e.get("synthetic-items", "second").await.is_some());
    assert!(e.get("synthetic-other", "other").await.is_some());
    let retry = e
        .client
        .batch_write_item()
        .set_request_items(Some(remaining))
        .send()
        .await
        .unwrap();
    assert!(retry.unprocessed_items.unwrap().is_empty());

    let reads = KeysAndAttributes::builder()
        .keys(key("first"))
        .keys(key("second"))
        .keys(key("third"))
        .consistent_read(true)
        .projection_expression("pk")
        .build()
        .unwrap();
    let output = e
        .client
        .batch_get_item()
        .request_items("synthetic-items", reads)
        .send()
        .await
        .unwrap();
    let remaining = output.unprocessed_keys.unwrap();
    assert_eq!(remaining["synthetic-items"].keys(), [key("second")]);
    assert_eq!(remaining["synthetic-items"].consistent_read(), Some(true));
    assert_eq!(
        remaining["synthetic-items"].projection_expression(),
        Some("pk")
    );
    let mut found = output.responses.unwrap().remove("synthetic-items").unwrap();
    let retry = e
        .client
        .batch_get_item()
        .set_request_items(Some(remaining))
        .send()
        .await
        .unwrap();
    assert!(retry.unprocessed_keys.unwrap().is_empty());
    found.extend(retry.responses.unwrap().remove("synthetic-items").unwrap());
    let mut ids = found
        .iter()
        .map(|item| item["pk"].as_s().unwrap().as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, ["first", "second", "third"]);
}
