use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    types::{
        AttributeDefinition, AttributeValue as Av, BillingMode, GlobalSecondaryIndex,
        KeySchemaElement, KeyType, LocalSecondaryIndex, Projection, ProjectionType,
        ScalarAttributeType, Select,
    },
};
use std::collections::{BTreeSet, HashMap};
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
    async fn table(&self, indexed: bool) {
        let mut request = self
            .client
            .create_table()
            .table_name("synthetic-records")
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(key_schema("pk", KeyType::Hash))
            .key_schema(key_schema("sk", KeyType::Range))
            .attribute_definitions(definition("pk"))
            .attribute_definitions(definition("sk"));
        if indexed {
            request = request
                .attribute_definitions(definition("group_key"))
                .attribute_definitions(definition("rank_key"))
                .global_secondary_indexes(
                    GlobalSecondaryIndex::builder()
                        .index_name("by-group")
                        .key_schema(key_schema("group_key", KeyType::Hash))
                        .key_schema(key_schema("rank_key", KeyType::Range))
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::Include)
                                .non_key_attributes("label")
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .global_secondary_indexes(
                    GlobalSecondaryIndex::builder()
                        .index_name("all-by-group")
                        .key_schema(key_schema("group_key", KeyType::Hash))
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .local_secondary_indexes(
                    LocalSecondaryIndex::builder()
                        .index_name("by-rank")
                        .key_schema(key_schema("pk", KeyType::Hash))
                        .key_schema(key_schema("rank_key", KeyType::Range))
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::KeysOnly)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                );
        }
        request.send().await.unwrap();
    }
    async fn put(&self, partition: &str, sort: &str, extra: &[(&str, Av)]) {
        let mut item = key(partition, sort);
        item.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
        self.client
            .put_item()
            .table_name("synthetic-records")
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
}
fn key_schema(name: &str, kind: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(kind)
        .build()
        .unwrap()
}
fn definition(name: &str) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap()
}
fn s(value: &str) -> Av {
    Av::S(value.into())
}
fn key(pk: &str, sk: &str) -> HashMap<String, Av> {
    HashMap::from([("pk".into(), s(pk)), ("sk".into(), s(sk))])
}
fn string(item: &HashMap<String, Av>, name: &str) -> String {
    item[name].as_s().unwrap().clone()
}

#[tokio::test]
async fn index_projection_sparse_updates_and_invalid_candidates() {
    let e = Emulator::new().await;
    e.table(true).await;
    e.put(
        "p",
        "a",
        &[
            ("group_key", s("g")),
            ("rank_key", s("r")),
            ("label", s("shown")),
            ("detail_text", s("secret-value")),
        ],
    )
    .await;
    e.put("p", "b", &[("label", s("sparse"))]).await;
    let description = e
        .client
        .describe_table()
        .table_name("synthetic-records")
        .send()
        .await
        .unwrap()
        .table
        .unwrap();
    assert_eq!(description.global_secondary_indexes().len(), 2);
    assert_eq!(description.local_secondary_indexes().len(), 1);
    let result = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-group")
        .key_condition_expression("group_key = :g")
        .expression_attribute_values(":g", s("g"))
        .send()
        .await
        .unwrap();
    assert_eq!(result.items().len(), 1);
    assert_eq!(result.items()[0].len(), 5);
    assert!(!result.items()[0].contains_key("detail_text"));
    let all = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("all-by-group")
        .key_condition_expression("group_key = :g")
        .expression_attribute_values(":g", s("g"))
        .send()
        .await
        .unwrap();
    assert_eq!(string(&all.items()[0], "detail_text"), "secret-value");
    let invalid = e
        .client
        .update_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "a")))
        .update_expression("SET rank_key = :n")
        .expression_attribute_values(":n", Av::N("4".into()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        invalid.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    let stored = e
        .client
        .get_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "a")))
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(string(&stored, "rank_key"), "r");
    e.client
        .update_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "a")))
        .update_expression("REMOVE group_key")
        .send()
        .await
        .unwrap();
    let result = e
        .client
        .scan()
        .table_name("synthetic-records")
        .index_name("by-group")
        .send()
        .await
        .unwrap();
    assert!(result.items().is_empty());
}

#[tokio::test]
async fn local_index_can_fetch_base_attributes_and_global_index_rejects_them() {
    let e = Emulator::new().await;
    e.table(true).await;
    e.put(
        "p",
        "a",
        &[
            ("group_key", s("g")),
            ("rank_key", s("r")),
            ("detail_text", s("kept")),
        ],
    )
    .await;
    let local = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .consistent_read(true)
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("p"))
        .projection_expression("detail_text")
        .send()
        .await
        .unwrap();
    assert_eq!(
        local.items()[0],
        HashMap::from([("detail_text".into(), s("kept"))])
    );
    let default = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("p"))
        .send()
        .await
        .unwrap();
    assert!(!default.items()[0].contains_key("detail_text"));
    let all = e
        .client
        .scan()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .select(Select::AllAttributes)
        .send()
        .await
        .unwrap();
    assert_eq!(string(&all.items()[0], "detail_text"), "kept");
    for strong in [false, true] {
        let request = e
            .client
            .query()
            .table_name("synthetic-records")
            .index_name("by-group")
            .key_condition_expression("group_key = :g")
            .expression_attribute_values(":g", s("g"));
        let error = if strong {
            request.consistent_read(true).send().await.unwrap_err()
        } else {
            request
                .projection_expression("detail_text")
                .send()
                .await
                .unwrap_err()
        };
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
    }
}

#[tokio::test]
async fn filters_project_nested_values_and_keep_evaluated_cursors() {
    let e = Emulator::new().await;
    e.table(false).await;
    let document = Av::M(HashMap::from([
        ("title".into(), s("nested")),
        ("omit".into(), s("x")),
    ]));
    e.put(
        "p",
        "a",
        &[
            ("keep_row", Av::Bool(false)),
            ("document", document.clone()),
        ],
    )
    .await;
    e.put(
        "p",
        "b",
        &[("keep_row", Av::Bool(true)), ("document", document)],
    )
    .await;
    let first = e
        .client
        .query()
        .table_name("synthetic-records")
        .key_condition_expression("pk = :p")
        .filter_expression("keep_row = :yes")
        .expression_attribute_values(":p", s("p"))
        .expression_attribute_values(":yes", Av::Bool(true))
        .projection_expression("#doc.#title")
        .expression_attribute_names("#doc", "document")
        .expression_attribute_names("#title", "title")
        .limit(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.count(), 0);
    assert_eq!(first.scanned_count(), 1);
    assert_eq!(first.last_evaluated_key(), Some(&key("p", "a")));
    let second = e
        .client
        .query()
        .table_name("synthetic-records")
        .key_condition_expression("pk = :p")
        .filter_expression("keep_row = :yes")
        .expression_attribute_values(":p", s("p"))
        .expression_attribute_values(":yes", Av::Bool(true))
        .projection_expression("#doc.#title")
        .expression_attribute_names("#doc", "document")
        .expression_attribute_names("#title", "title")
        .set_exclusive_start_key(first.last_evaluated_key)
        .send()
        .await
        .unwrap();
    assert_eq!(second.count(), 1);
    assert_eq!(
        second.items()[0],
        HashMap::from([(
            "document".into(),
            Av::M(HashMap::from([("title".into(), s("nested"))]))
        )])
    );
    let count = e
        .client
        .scan()
        .table_name("synthetic-records")
        .select(Select::Count)
        .filter_expression("keep_row = :yes")
        .expression_attribute_values(":yes", Av::Bool(true))
        .send()
        .await
        .unwrap();
    assert_eq!((count.count(), count.scanned_count()), (1, 2));
    assert!(count.items.is_none());
    let projected = e
        .client
        .get_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "b")))
        .projection_expression("#doc.#title")
        .expression_attribute_names("#doc", "document")
        .expression_attribute_names("#title", "title")
        .send()
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(projected, second.items()[0]);
    let overlap = e
        .client
        .get_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "b")))
        .projection_expression("#doc, #doc.#title")
        .expression_attribute_names("#doc", "document")
        .expression_attribute_names("#title", "title")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        overlap.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
}

#[tokio::test]
async fn index_cursors_preserve_ties_reverse_order_and_deleted_rows() {
    let e = Emulator::new().await;
    e.table(true).await;
    for sk in ["a", "b", "c", "d"] {
        e.put(
            "p",
            sk,
            &[
                ("group_key", s("g")),
                ("rank_key", s("same")),
                ("label", s(sk)),
            ],
        )
        .await;
    }
    for forward in [true, false] {
        let mut cursor = None;
        let mut labels = vec![];
        loop {
            let page = e
                .client
                .query()
                .table_name("synthetic-records")
                .index_name("by-group")
                .key_condition_expression("group_key = :g")
                .expression_attribute_values(":g", s("g"))
                .projection_expression("label")
                .limit(1)
                .scan_index_forward(forward)
                .set_exclusive_start_key(cursor)
                .send()
                .await
                .unwrap();
            labels.extend(page.items().iter().map(|item| string(item, "label")));
            cursor = page.last_evaluated_key;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            labels,
            if forward {
                vec!["a", "b", "c", "d"]
            } else {
                vec!["d", "c", "b", "a"]
            }
        );
    }
    let first = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-group")
        .key_condition_expression("group_key = :g")
        .expression_attribute_values(":g", s("g"))
        .limit(1)
        .send()
        .await
        .unwrap();
    e.client
        .delete_item()
        .table_name("synthetic-records")
        .set_key(Some(key("p", "a")))
        .send()
        .await
        .unwrap();
    let rest = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-group")
        .key_condition_expression("group_key = :g")
        .expression_attribute_values(":g", s("g"))
        .set_exclusive_start_key(first.last_evaluated_key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        rest.items()
            .iter()
            .map(|i| string(i, "label"))
            .collect::<Vec<_>>(),
        vec!["b", "c", "d"]
    );
}

#[tokio::test]
async fn physical_pages_use_decoded_data_size_and_segments_cover_each_item_once() {
    let e = Emulator::new().await;
    e.table(true).await;
    // Decoded binary data totals 1.2 MiB; base64 wire size is larger. Exactly
    // three records fit in a physical page, independent of the requested limit.
    for sk in ["a", "b", "c", "d"] {
        e.put(
            "large",
            sk,
            &[
                (
                    "payload",
                    Av::B(aws_sdk_dynamodb::primitives::Blob::new(vec![
                        0u8;
                        300 * 1024
                    ])),
                ),
                ("group_key", s("large-group")),
                ("rank_key", s(sk)),
            ],
        )
        .await;
    }
    let first = e
        .client
        .query()
        .table_name("synthetic-records")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("large"))
        .projection_expression("sk")
        .limit(100)
        .send()
        .await
        .unwrap();
    assert_eq!(first.scanned_count(), 3);
    assert_eq!(first.items().len(), 3);
    assert_eq!(first.last_evaluated_key(), Some(&key("large", "c")));
    let second = e
        .client
        .query()
        .table_name("synthetic-records")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("large"))
        .projection_expression("sk")
        .set_exclusive_start_key(first.last_evaluated_key)
        .send()
        .await
        .unwrap();
    assert_eq!(second.scanned_count(), 1);
    assert!(second.last_evaluated_key.is_none());
    for (index, expected) in [("all-by-group", 3), ("by-group", 4)] {
        let result = e
            .client
            .query()
            .table_name("synthetic-records")
            .index_name(index)
            .key_condition_expression("group_key = :g")
            .expression_attribute_values(":g", s("large-group"))
            .projection_expression("sk")
            .limit(100)
            .send()
            .await
            .unwrap();
        assert_eq!(result.scanned_count(), expected);
        assert_eq!(result.last_evaluated_key.is_some(), expected == 3);
    }
    let keys_only = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("large"))
        .send()
        .await
        .unwrap();
    assert_eq!(keys_only.scanned_count(), 4);
    let fetched = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("large"))
        .projection_expression("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.scanned_count(), 3);
    for i in 0..18 {
        e.put(&format!("partition-{i}"), "row", &[]).await;
    }
    let expected: BTreeSet<_> = ["a", "b", "c", "d"]
        .map(|sk| format!("large/{sk}"))
        .into_iter()
        .chain((0..18).map(|i| format!("partition-{i}/row")))
        .collect();
    let mut actual = BTreeSet::new();
    for segment in 0..5 {
        let mut cursor = None;
        loop {
            let page = e
                .client
                .scan()
                .table_name("synthetic-records")
                .segment(segment)
                .total_segments(5)
                .projection_expression("pk, sk")
                .limit(2)
                .set_exclusive_start_key(cursor)
                .send()
                .await
                .unwrap();
            for item in page.items() {
                assert!(actual.insert(format!("{}/{}", string(item, "pk"), string(item, "sk"))));
            }
            cursor = page.last_evaluated_key;
            if cursor.is_none() {
                break;
            }
        }
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn schema_projection_and_local_entry_size_validation_are_atomic() {
    let e = Emulator::new().await;
    for projection in [
        Projection::builder()
            .projection_type(ProjectionType::Include)
            .build(),
        Projection::builder()
            .projection_type(ProjectionType::KeysOnly)
            .non_key_attributes("label")
            .build(),
        Projection::builder()
            .projection_type(ProjectionType::Include)
            .non_key_attributes("pk")
            .build(),
    ] {
        let error = e
            .client
            .create_table()
            .table_name("invalid-records")
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(key_schema("pk", KeyType::Hash))
            .attribute_definitions(definition("pk"))
            .attribute_definitions(definition("group_key"))
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("by-group")
                    .key_schema(key_schema("group_key", KeyType::Hash))
                    .projection(projection)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        let missing = e
            .client
            .describe_table()
            .table_name("invalid-records")
            .send()
            .await
            .unwrap_err();
        assert!(
            missing
                .as_service_error()
                .unwrap()
                .is_resource_not_found_exception()
        );
    }
    let invalid_local = e
        .client
        .create_table()
        .table_name("invalid-records")
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key_schema("pk", KeyType::Hash))
        .attribute_definitions(definition("pk"))
        .attribute_definitions(definition("rank_key"))
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("by-rank")
                .key_schema(key_schema("pk", KeyType::Hash))
                .key_schema(key_schema("rank_key", KeyType::Range))
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        invalid_local.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    e.table(true).await;
    // The base item fits 400 KiB by itself; its local index entry pushes their
    // combined size over the limit. A rejected candidate leaves no base row.
    let long = "x".repeat(500);
    let error = e
        .client
        .put_item()
        .table_name("synthetic-records")
        .set_item(Some(key(&long, &long)))
        .item("rank_key", s(&long))
        .item(
            "payload",
            Av::B(aws_sdk_dynamodb::primitives::Blob::new(vec![0u8; 406_900])),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    let absent = e
        .client
        .get_item()
        .table_name("synthetic-records")
        .set_key(Some(key(&long, &long)))
        .send()
        .await
        .unwrap();
    assert!(absent.item.is_none());
    let wrong_partial = e
        .client
        .put_item()
        .table_name("synthetic-records")
        .set_item(Some(key("p", "s")))
        .item("group_key", Av::Bool(true))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        wrong_partial.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    let empty = e
        .client
        .scan()
        .table_name("synthetic-records")
        .send()
        .await
        .unwrap();
    assert!(empty.items().is_empty());
}

#[tokio::test]
async fn batch_and_transaction_commits_update_indexes_and_failed_candidates_do_not() {
    use aws_sdk_dynamodb::types::{Delete, PutRequest, TransactWriteItem, Update, WriteRequest};
    let e = Emulator::new().await;
    e.table(true).await;
    let put = |sort: &str, rank: Av| {
        let mut item = key("p", sort);
        item.extend([("group_key".into(), s("g")), ("rank_key".into(), rank)]);
        WriteRequest::builder()
            .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
            .build()
    };
    let error = e
        .client
        .batch_write_item()
        .request_items(
            "synthetic-records",
            vec![put("a", s("r")), put("b", Av::N("1".into()))],
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert_eq!(
        e.client
            .scan()
            .table_name("synthetic-records")
            .send()
            .await
            .unwrap()
            .count(),
        0
    );
    e.client
        .batch_write_item()
        .request_items(
            "synthetic-records",
            vec![put("a", s("r")), put("b", s("r"))],
        )
        .send()
        .await
        .unwrap();
    let query = |group: &str| {
        e.client
            .query()
            .table_name("synthetic-records")
            .index_name("by-group")
            .key_condition_expression("group_key = :g")
            .expression_attribute_values(":g", s(group))
    };
    assert_eq!(query("g").send().await.unwrap().count(), 2);
    let move_group = || {
        TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name("synthetic-records")
                    .set_key(Some(key("p", "a")))
                    .update_expression("SET group_key = :g")
                    .expression_attribute_values(":g", s("moved"))
                    .build()
                    .unwrap(),
            )
            .build()
    };
    let invalid = TransactWriteItem::builder()
        .update(
            Update::builder()
                .table_name("synthetic-records")
                .set_key(Some(key("p", "b")))
                .update_expression("SET rank_key = :n")
                .expression_attribute_values(":n", Av::N("1".into()))
                .build()
                .unwrap(),
        )
        .build();
    let error = e
        .client
        .transact_write_items()
        .transact_items(move_group())
        .transact_items(invalid)
        .send()
        .await
        .unwrap_err();
    assert!(
        error
            .as_service_error()
            .unwrap()
            .is_transaction_canceled_exception()
    );
    assert_eq!(query("g").send().await.unwrap().count(), 2);
    assert_eq!(query("moved").send().await.unwrap().count(), 0);
    e.client
        .transact_write_items()
        .transact_items(move_group())
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name("synthetic-records")
                        .set_key(Some(key("p", "b")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(query("g").send().await.unwrap().count(), 0);
    let moved = query("moved").send().await.unwrap();
    assert_eq!(moved.items().len(), 1);
    assert_eq!(string(&moved.items()[0], "sk"), "a");
    let local = e
        .client
        .query()
        .table_name("synthetic-records")
        .index_name("by-rank")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("p"))
        .send()
        .await
        .unwrap();
    assert_eq!(local.items().len(), 1);
    assert_eq!(string(&local.items()[0], "sk"), "a");
}
