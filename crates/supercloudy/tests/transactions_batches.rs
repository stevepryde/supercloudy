use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    types::{
        AttributeDefinition, AttributeValue as Av, BillingMode, ConditionCheck, Delete,
        DeleteRequest, Get, KeySchemaElement, KeyType, KeysAndAttributes, Put, PutRequest,
        ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TransactGetItem,
        TransactWriteItem, Update, WriteRequest,
    },
};
use std::collections::HashMap;
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
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, supercloudy::server::router())
                .await
                .unwrap();
        });
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
            .endpoint_url(endpoint)
            .build();
        Self {
            client: Client::from_conf(config),
            task,
        }
    }
    async fn table(&self, name: &str, numeric: bool, composite: bool) {
        let mut b = self
            .client
            .create_table()
            .table_name(name)
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
                    .attribute_type(if numeric {
                        ScalarAttributeType::N
                    } else {
                        ScalarAttributeType::S
                    })
                    .build()
                    .unwrap(),
            );
        if composite {
            b = b
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("sk")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("sk")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                );
        }
        b.send().await.unwrap();
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
fn s(v: &str) -> Av {
    Av::S(v.into())
}
fn n(v: &str) -> Av {
    Av::N(v.into())
}
fn key(id: &str) -> HashMap<String, Av> {
    HashMap::from([("pk".into(), s(id))])
}
fn item(id: &str, value: &str) -> HashMap<String, Av> {
    HashMap::from([("pk".into(), s(id)), ("revision".into(), n(value))])
}
fn tx_put(table: &str, id: &str) -> TransactWriteItem {
    TransactWriteItem::builder()
        .put(
            Put::builder()
                .table_name(table)
                .set_item(Some(item(id, "1")))
                .build()
                .unwrap(),
        )
        .build()
}
fn increment(table: &str, id: &str) -> TransactWriteItem {
    TransactWriteItem::builder()
        .update(
            Update::builder()
                .table_name(table)
                .set_key(Some(key(id)))
                .update_expression("SET revision = revision + :one")
                .expression_attribute_values(":one", n("1"))
                .build()
                .unwrap(),
        )
        .build()
}
fn batch_put(id: &str) -> WriteRequest {
    WriteRequest::builder()
        .put_request(
            PutRequest::builder()
                .set_item(Some(item(id, "1")))
                .build()
                .unwrap(),
        )
        .build()
}

#[tokio::test]
async fn transactions_stage_all_actions_and_return_ordered_cancellation_images() {
    let e = Emulator::new().await;
    e.table("synthetic-ledger", false, false).await;
    e.table("synthetic-guards", false, false).await;
    for (table, id) in [("synthetic-ledger", "a"), ("synthetic-guards", "b")] {
        e.client
            .put_item()
            .table_name(table)
            .set_item(Some(item(id, "1")))
            .send()
            .await
            .unwrap();
    }
    let actions = vec![
        tx_put("synthetic-ledger", "new"),
        increment("synthetic-ledger", "a"),
        TransactWriteItem::builder()
            .condition_check(
                ConditionCheck::builder()
                    .table_name("synthetic-guards")
                    .set_key(Some(key("b")))
                    .condition_expression("revision = :expected")
                    .expression_attribute_values(":expected", n("0"))
                    .return_values_on_condition_check_failure(
                        ReturnValuesOnConditionCheckFailure::AllOld,
                    )
                    .build()
                    .unwrap(),
            )
            .build(),
    ];
    let error = e
        .client
        .transact_write_items()
        .set_transact_items(Some(actions))
        .send()
        .await
        .unwrap_err();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(failure) = error.as_service_error().unwrap() else { panic!("expected transaction cancellation: {error:?}"); };
    assert_eq!(
        failure
            .cancellation_reasons()
            .iter()
            .map(|r| r.code().unwrap())
            .collect::<Vec<_>>(),
        vec!["None", "None", "ConditionalCheckFailed"]
    );
    assert_eq!(
        failure.cancellation_reasons()[2].item().unwrap()["revision"],
        n("1")
    );
    assert_eq!(
        e.get("synthetic-ledger", "a").await.unwrap()["revision"],
        n("1")
    );
    assert!(e.get("synthetic-ledger", "new").await.is_none());
    e.client
        .transact_write_items()
        .transact_items(tx_put("synthetic-ledger", "new"))
        .transact_items(increment("synthetic-ledger", "a"))
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name("synthetic-guards")
                        .set_key(Some(key("b")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.get("synthetic-ledger", "a").await.unwrap()["revision"],
        n("2")
    );
    assert!(e.get("synthetic-guards", "b").await.is_none());
    let output = e
        .client
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("synthetic-ledger")
                        .set_key(Some(key("a")))
                        .projection_expression("#r")
                        .expression_attribute_names("#r", "revision")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("synthetic-guards")
                        .set_key(Some(key("b")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("synthetic-ledger")
                        .set_key(Some(key("new")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(output.responses().len(), 3);
    assert_eq!(
        output.responses()[0].item().unwrap(),
        &HashMap::from([("revision".into(), n("2"))])
    );
    assert!(output.responses()[1].item().is_none());
    assert_eq!(output.responses()[2].item().unwrap()["pk"], s("new"));
}

#[tokio::test]
async fn token_replay_does_not_reapply_updates_and_changed_parameters_fail() {
    let e = Emulator::new().await;
    e.table("synthetic-tokens", false, false).await;
    e.client
        .put_item()
        .table_name("synthetic-tokens")
        .set_item(Some(item("a", "1")))
        .send()
        .await
        .unwrap();
    let b = e
        .client
        .transact_write_items()
        .client_request_token("stable-operation")
        .transact_items(increment("synthetic-tokens", "a"));
    b.clone().send().await.unwrap();
    b.send().await.unwrap();
    assert_eq!(
        e.get("synthetic-tokens", "a").await.unwrap()["revision"],
        n("2")
    );
    let error = e
        .client
        .transact_write_items()
        .client_request_token("stable-operation")
        .transact_items(tx_put("synthetic-tokens", "b"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("IdempotentParameterMismatchException")
    );
    assert!(e.get("synthetic-tokens", "b").await.is_none());
}

#[tokio::test]
async fn transaction_duplicate_numeric_keys_and_action_limits_reject_before_mutation() {
    let e = Emulator::new().await;
    e.table("synthetic-numeric", true, false).await;
    e.table("synthetic-counts", false, false).await;
    let mut actions = Vec::new();
    for value in ["1", "1.0"] {
        actions.push(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("synthetic-numeric")
                        .item("pk", n(value))
                        .build()
                        .unwrap(),
                )
                .build(),
        );
    }
    let error = e
        .client
        .transact_write_items()
        .set_transact_items(Some(actions))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert!(
        e.client
            .get_item()
            .table_name("synthetic-numeric")
            .key("pk", n("1"))
            .send()
            .await
            .unwrap()
            .item
            .is_none()
    );
    let actions = (0..101)
        .map(|i| tx_put("synthetic-counts", &format!("row-{i}")))
        .collect::<Vec<_>>();
    let error = e
        .client
        .transact_write_items()
        .set_transact_items(Some(actions.clone()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert!(e.get("synthetic-counts", "row-0").await.is_none());
    e.client
        .transact_write_items()
        .set_transact_items(Some(actions[..100].to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.client
            .scan()
            .table_name("synthetic-counts")
            .send()
            .await
            .unwrap()
            .count(),
        100
    );
    let reads = (0..101)
        .map(|i| {
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("synthetic-counts")
                        .set_key(Some(key(&format!("row-{i}"))))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        e.client
            .transact_get_items()
            .set_transact_items(Some(reads.clone()))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
    assert_eq!(
        e.client
            .transact_get_items()
            .set_transact_items(Some(reads[..100].to_vec()))
            .send()
            .await
            .unwrap()
            .responses()
            .len(),
        100
    );
}

#[tokio::test]
async fn transaction_validation_and_aggregate_sizes_leave_no_partial_writes() {
    let e = Emulator::new().await;
    e.table("synthetic-sizes", false, false).await;
    let invalid = TransactWriteItem::builder()
        .update(
            Update::builder()
                .table_name("synthetic-sizes")
                .set_key(Some(key("bad")))
                .update_expression("SET revision = :v")
                .expression_attribute_values(":v", s(&"x".repeat(400 * 1024)))
                .build()
                .unwrap(),
        )
        .build();
    let error = e
        .client
        .transact_write_items()
        .transact_items(tx_put("synthetic-sizes", "good"))
        .transact_items(invalid)
        .send()
        .await
        .unwrap_err();
    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(failure) = error.as_service_error().unwrap() else { panic!("expected transaction cancellation: {error:?}"); };
    let reasons = failure.cancellation_reasons();
    assert_eq!(reasons[1].code(), Some("ValidationError"));
    assert!(e.get("synthetic-sizes", "good").await.is_none());
    let actions = (0..11)
        .map(|i| {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("synthetic-sizes")
                        .item("pk", s(&format!("row-{i}")))
                        .item("payload", s(&"x".repeat(390 * 1024)))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        e.client
            .transact_write_items()
            .set_transact_items(Some(actions))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
    assert!(e.get("synthetic-sizes", "row-0").await.is_none());
    for i in 0..11 {
        e.client
            .put_item()
            .table_name("synthetic-sizes")
            .item("pk", s(&format!("row-{i}")))
            .item("payload", s(&"x".repeat(390 * 1024)))
            .send()
            .await
            .unwrap();
    }
    let reads = (0..11)
        .map(|i| {
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("synthetic-sizes")
                        .set_key(Some(key(&format!("row-{i}"))))
                        .build()
                        .unwrap(),
                )
                .build()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        e.client
            .transact_get_items()
            .set_transact_items(Some(reads))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
}

#[tokio::test]
async fn transaction_snapshot_reads_never_observe_half_a_commit() {
    let e = Emulator::new().await;
    e.table("synthetic-snapshot", false, false).await;
    e.client
        .transact_write_items()
        .transact_items(tx_put("synthetic-snapshot", "a"))
        .transact_items(tx_put("synthetic-snapshot", "b"))
        .send()
        .await
        .unwrap();
    let client = e.client.clone();
    let writer = tokio::spawn(async move {
        for _ in 0..30 {
            client
                .transact_write_items()
                .transact_items(increment("synthetic-snapshot", "a"))
                .transact_items(increment("synthetic-snapshot", "b"))
                .send()
                .await
                .unwrap();
        }
    });
    for _ in 0..30 {
        let b = e.client.transact_get_items();
        let b = ["a", "b"].into_iter().fold(b, |b, id| {
            b.transact_items(
                TransactGetItem::builder()
                    .get(
                        Get::builder()
                            .table_name("synthetic-snapshot")
                            .set_key(Some(key(id)))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
        });
        let output = b.send().await.unwrap();
        assert_eq!(
            output.responses()[0].item().unwrap()["revision"],
            output.responses()[1].item().unwrap()["revision"]
        );
    }
    writer.await.unwrap();
    assert_eq!(
        e.get("synthetic-snapshot", "a").await.unwrap()["revision"],
        n("31")
    );
}

#[tokio::test]
async fn batches_validate_all_requests_and_support_multi_table_gets_and_deletes() {
    let e = Emulator::new().await;
    e.table("synthetic-batch-a", false, false).await;
    e.table("synthetic-batch-b", false, false).await;
    let writes = (0..26)
        .map(|i| batch_put(&format!("row-{i}")))
        .collect::<Vec<_>>();
    assert_eq!(
        e.client
            .batch_write_item()
            .set_request_items(Some(HashMap::from([(
                "synthetic-batch-a".into(),
                writes.clone()
            )])))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
    assert!(e.get("synthetic-batch-a", "row-0").await.is_none());
    let output = e
        .client
        .batch_write_item()
        .set_request_items(Some(HashMap::from([
            ("synthetic-batch-a".into(), writes[..24].to_vec()),
            ("synthetic-batch-b".into(), vec![batch_put("other")]),
        ])))
        .send()
        .await
        .unwrap();
    assert!(output.unprocessed_items().is_none_or(HashMap::is_empty));
    let duplicate = vec![batch_put("fresh"), batch_put("fresh")];
    assert_eq!(
        e.client
            .batch_write_item()
            .set_request_items(Some(HashMap::from([(
                "synthetic-batch-a".into(),
                duplicate
            )])))
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
    assert!(e.get("synthetic-batch-a", "fresh").await.is_none());
    let output = e
        .client
        .batch_get_item()
        .request_items(
            "synthetic-batch-a",
            KeysAndAttributes::builder()
                .keys(key("row-0"))
                .keys(key("missing"))
                .projection_expression("#r")
                .expression_attribute_names("#r", "revision")
                .consistent_read(true)
                .build()
                .unwrap(),
        )
        .request_items(
            "synthetic-batch-b",
            KeysAndAttributes::builder()
                .keys(key("other"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let responses = output.responses().unwrap();
    assert_eq!(
        responses["synthetic-batch-a"],
        vec![HashMap::from([("revision".into(), n("1"))])]
    );
    assert_eq!(responses["synthetic-batch-b"][0]["pk"], s("other"));
    e.client
        .batch_write_item()
        .request_items(
            "synthetic-batch-a",
            vec![
                WriteRequest::builder()
                    .delete_request(
                        DeleteRequest::builder()
                            .set_key(Some(key("row-0")))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            ],
        )
        .send()
        .await
        .unwrap();
    assert!(e.get("synthetic-batch-a", "row-0").await.is_none());
    let keys = (0..101)
        .map(|i| key(&format!("row-{i}")))
        .collect::<Vec<_>>();
    assert_eq!(
        e.client
            .batch_get_item()
            .request_items(
                "synthetic-batch-a",
                KeysAndAttributes::builder()
                    .set_keys(Some(keys.clone()))
                    .build()
                    .unwrap()
            )
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
    assert_eq!(
        e.client
            .batch_get_item()
            .request_items(
                "synthetic-batch-a",
                KeysAndAttributes::builder()
                    .set_keys(Some(keys[..100].to_vec()))
                    .build()
                    .unwrap()
            )
            .send()
            .await
            .unwrap()
            .responses()
            .unwrap()["synthetic-batch-a"]
            .len(),
        23
    );
    assert_eq!(
        e.client
            .batch_get_item()
            .request_items(
                "synthetic-batch-a",
                KeysAndAttributes::builder()
                    .keys(key("row-1"))
                    .keys(key("row-1"))
                    .build()
                    .unwrap()
            )
            .send()
            .await
            .unwrap_err()
            .as_service_error()
            .unwrap()
            .code(),
        Some("ValidationException")
    );
}

#[tokio::test]
async fn batch_get_size_bound_returns_original_read_options_for_retry() {
    let e = Emulator::new().await;
    e.table("synthetic-partition", false, true).await;
    let mut keys = Vec::new();
    for i in 0..4 {
        let k = HashMap::from([
            ("pk".into(), s("shared")),
            ("sk".into(), s(&format!("row-{i}"))),
        ]);
        let mut value = k.clone();
        value.insert("payload".into(), s(&"x".repeat(300 * 1024)));
        e.client
            .put_item()
            .table_name("synthetic-partition")
            .set_item(Some(value))
            .send()
            .await
            .unwrap();
        keys.push(k);
    }
    let output = e
        .client
        .batch_get_item()
        .request_items(
            "synthetic-partition",
            KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .projection_expression("#s")
                .expression_attribute_names("#s", "sk")
                .consistent_read(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(output.responses().unwrap()["synthetic-partition"].len(), 3);
    let pending = output.unprocessed_keys().unwrap();
    assert_eq!(pending["synthetic-partition"].keys().len(), 1);
    assert_eq!(
        pending["synthetic-partition"].projection_expression(),
        Some("#s")
    );
    assert_eq!(pending["synthetic-partition"].consistent_read(), Some(true));
    let retry = e
        .client
        .batch_get_item()
        .set_request_items(Some(pending.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        retry.responses().unwrap()["synthetic-partition"][0]["sk"],
        s("row-3")
    );
    assert!(retry.unprocessed_keys().is_none_or(HashMap::is_empty));
}

#[tokio::test]
async fn batch_response_and_wire_limits_return_retryable_work_or_validation() {
    let e = Emulator::new().await;
    e.table("synthetic-response-size", false, false).await;
    // Distinct logical partitions exercise the aggregate response boundary,
    // rather than the smaller per-partition read boundary.
    for first in [0, 20, 40] {
        let writes = (first..first + 20)
            .map(|i| {
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .item("pk", s(&format!("row-{i}")))
                            .item("payload", s(&"x".repeat(300 * 1024)))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        e.client
            .batch_write_item()
            .request_items("synthetic-response-size", writes)
            .send()
            .await
            .unwrap();
    }
    let output = e
        .client
        .batch_get_item()
        .request_items(
            "synthetic-response-size",
            KeysAndAttributes::builder()
                .set_keys(Some((0..60).map(|i| key(&format!("row-{i}"))).collect()))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let rows = &output.responses().unwrap()["synthetic-response-size"];
    assert_eq!(rows.len(), 54);
    let pending = output.unprocessed_keys().unwrap();
    assert_eq!(pending["synthetic-response-size"].keys().len(), 6);
    let retry = e
        .client
        .batch_get_item()
        .set_request_items(Some(pending.clone()))
        .send()
        .await
        .unwrap();
    let identities = rows
        .iter()
        .chain(&retry.responses().unwrap()["synthetic-response-size"])
        .map(|row| row["pk"].as_s().unwrap().clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(identities, (0..60).map(|i| format!("row-{i}")).collect());
    assert!(retry.unprocessed_keys().is_none_or(HashMap::is_empty));

    // JSON escaping crosses both the native 16 MiB wire limit and the
    // server body collection ceiling, while decoded items remain below 400 KiB.
    for character in ["\n", "\u{0001}"] {
        let writes = (0..25)
            .map(|i| {
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .item("pk", s(&format!("escaped-{i}")))
                            .item("payload", s(&character.repeat(350 * 1024)))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        let error = e
            .client
            .batch_write_item()
            .request_items("synthetic-response-size", writes)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert!(
            e.get("synthetic-response-size", "escaped-0")
                .await
                .is_none()
        );
    }
}
