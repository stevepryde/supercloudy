use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue as Av, BillingMode, KeySchemaElement, KeyType,
        ReturnValue, ScalarAttributeType, Select, TableStatus,
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
    async fn table(&self, name: &str, sort: Option<ScalarAttributeType>) {
        let mut create = self
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
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            );
        if let Some(tag) = sort {
            create = create
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
                        .attribute_type(tag)
                        .build()
                        .unwrap(),
                );
        }
        create.send().await.unwrap();
    }
    async fn get(&self, table: &str, key: HashMap<String, Av>) -> Option<HashMap<String, Av>> {
        self.client
            .get_item()
            .table_name(table)
            .set_key(Some(key))
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
fn map(values: &[(&str, Av)]) -> HashMap<String, Av> {
    values
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}
fn key() -> HashMap<String, Av> {
    map(&[("pk", s("item-1"))])
}

#[tokio::test]
async fn table_lifecycle_and_modeled_errors() {
    let e = Emulator::new().await;
    let c = &e.client;
    let missing = c
        .describe_table()
        .table_name("synthetic-items")
        .send()
        .await
        .unwrap_err();
    assert!(
        missing
            .as_service_error()
            .unwrap()
            .is_resource_not_found_exception()
    );
    e.table("synthetic-items", Some(ScalarAttributeType::S))
        .await;
    let table = c
        .describe_table()
        .table_name("synthetic-items")
        .send()
        .await
        .unwrap()
        .table
        .unwrap();
    assert_eq!(table.table_status(), Some(&TableStatus::Active));
    assert_eq!(table.key_schema().len(), 2);
    let duplicate = c
        .create_table()
        .table_name("synthetic-items")
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
        .unwrap_err();
    assert!(
        duplicate
            .as_service_error()
            .unwrap()
            .is_resource_in_use_exception()
    );
    c.put_item()
        .table_name("synthetic-items")
        .item("pk", s("item-1"))
        .item("sk", s("a"))
        .send()
        .await
        .unwrap();
    let deleted = c
        .delete_table()
        .table_name("synthetic-items")
        .send()
        .await
        .unwrap()
        .table_description
        .unwrap();
    assert_eq!(deleted.table_name(), Some("synthetic-items"));
    let err = c
        .get_item()
        .table_name("synthetic-items")
        .key("pk", s("item-1"))
        .key("sk", s("a"))
        .send()
        .await
        .unwrap_err();
    assert!(
        err.as_service_error()
            .unwrap()
            .is_resource_not_found_exception()
    );
    e.table("synthetic-items", Some(ScalarAttributeType::S))
        .await;
    assert!(
        e.get(
            "synthetic-items",
            map(&[("pk", s("item-1")), ("sk", s("a"))])
        )
        .await
        .is_none()
    );
}

#[tokio::test]
async fn typed_item_roundtrips_replacement_and_deletion() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    assert!(e.get("synthetic-items", key()).await.is_none());
    let item = map(&[
        ("pk", s("item-1")),
        ("text", s("hello")),
        ("decimal", n("12345678901234567890123456789012345678")),
        ("binary", Av::B(Blob::new([0, 255, 128]))),
        ("flag", Av::Bool(true)),
        ("nothing", Av::Null(true)),
        ("strings", Av::Ss(vec!["a".into(), "b".into()])),
        ("numbers", Av::Ns(vec!["1".into(), "2".into()])),
        ("binaries", Av::Bs(vec![Blob::new([255]), Blob::new([0])])),
        ("list", Av::L(vec![n("2"), Av::Null(true)])),
        ("document", Av::M(map(&[("nested", s("value"))]))),
    ]);
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    let got = e.get("synthetic-items", key()).await.unwrap();
    for name in [
        "pk", "text", "decimal", "binary", "flag", "nothing", "list", "document",
    ] {
        assert_eq!(got[name], item[name]);
    }
    assert_eq!(got["strings"].as_ss().unwrap().len(), 2);
    assert_eq!(
        got["numbers"].as_ns().unwrap(),
        &vec!["1".to_string(), "2".to_string()]
    );
    let mut binaries = got["binaries"].as_bs().unwrap().clone();
    binaries.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    assert_eq!(binaries, vec![Blob::new([0]), Blob::new([255])]);
    let old = c
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key()))
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(old["text"], s("hello"));
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), key());
    assert_eq!(
        c.delete_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .unwrap()
            .attributes
            .unwrap(),
        key()
    );
    assert!(
        c.delete_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .unwrap()
            .attributes
            .is_none()
    );
    assert!(e.get("synthetic-items", key()).await.is_none());
}

#[tokio::test]
async fn failing_conditions_are_modeled_and_leave_state_unchanged() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    let original = map(&[
        ("pk", s("item-1")),
        ("revision", n("1")),
        ("keep", s("original")),
    ]);
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .unwrap();
    let err = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET revision = :next")
        .condition_expression("revision = :expected")
        .expression_attribute_values(":next", n("2"))
        .expression_attribute_values(":expected", n("0"))
        .send()
        .await
        .unwrap_err();
    assert!(
        err.as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ConditionalCheckFailedException")
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    let err = c
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(key()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
        .unwrap_err();
    assert!(
        err.as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
    let err = c
        .delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("revision <> :one")
        .expression_attribute_values(":one", n("1.0"))
        .send()
        .await
        .unwrap_err();
    assert!(
        err.as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    c.delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("revision = :one")
        .expression_attribute_values(":one", n("1E0"))
        .send()
        .await
        .unwrap();
    let err = c
        .delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .unwrap_err();
    assert!(
        err.as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
}

#[tokio::test]
async fn update_creation_return_values_and_operand_snapshot() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    let created = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET a = :one, b = :two, c = :three")
        .expression_attribute_values(":one", n("1"))
        .expression_attribute_values(":two", n("2"))
        .expression_attribute_values(":three", n("3"))
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(created["a"], n("1"));
    assert_eq!(created["pk"], s("item-1"));
    let old = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE a SET b = a, c = b")
        .return_values(ReturnValue::AllOld)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(old, created);
    let got = e.get("synthetic-items", key()).await.unwrap();
    assert!(!got.contains_key("a"));
    assert_eq!(got["b"], n("1"));
    assert_eq!(got["c"], n("2"));
    let old = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET b = b + :one REMOVE c")
        .expression_attribute_values(":one", n("1"))
        .return_values(ReturnValue::UpdatedOld)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(old, map(&[("b", n("1")), ("c", n("2"))]));
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET b = b - :one, fresh = :one REMOVE absent")
        .expression_attribute_values(":one", n("1"))
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(new, map(&[("b", n("1")), ("fresh", n("1"))]));
}

#[tokio::test]
async fn update_documents_sets_and_list_functions() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[
            ("pk", s("item-1")),
            (
                "doc",
                Av::M(map(&[("a.b", s("before")), ("keep", Av::Bool(true))])),
            ),
            ("a.b", s("literal")),
            ("entries", Av::L(vec![s("a"), s("b"), s("c"), s("d")])),
            ("tags", Av::Ss(vec!["red".into(), "blue".into()])),
            ("numberSet", Av::Ns(vec!["1".into(), "2".into()])),
            ("binarySet", Av::Bs(vec![Blob::new([0])])),
        ])))
        .send()
        .await
        .unwrap();
    let got=c.update_item().table_name("synthetic-items").set_key(Some(key())).update_expression("SET #doc.#dot = :after, #dot = :literal, revision = if_not_exists(revision, :zero) + :inc REMOVE entries[0], entries[2] ADD tags :tags, numberSet :numbers, binarySet :binaries")
        .expression_attribute_names("#doc","doc").expression_attribute_names("#dot","a.b")
        .expression_attribute_values(":after",s("after")).expression_attribute_values(":literal",s("changed"))
        .expression_attribute_values(":zero",n("0")).expression_attribute_values(":inc",n("0.1"))
        .expression_attribute_values(":tags",Av::Ss(vec!["green".into()])).expression_attribute_values(":numbers",Av::Ns(vec!["2.0".into(),"3".into()])).expression_attribute_values(":binaries",Av::Bs(vec![Blob::new([255])]))
        .return_values(ReturnValue::AllNew).send().await.unwrap().attributes.unwrap();
    assert_eq!(
        got["doc"],
        Av::M(map(&[("a.b", s("after")), ("keep", Av::Bool(true))]))
    );
    assert_eq!(got["a.b"], s("changed"));
    assert_eq!(got["revision"], n("0.1"));
    assert_eq!(got["entries"], Av::L(vec![s("b"), s("d")]));
    assert_eq!(got["tags"].as_ss().unwrap().len(), 3);
    assert_eq!(got["numberSet"].as_ns().unwrap().len(), 3);
    assert_eq!(got["binarySet"].as_bs().unwrap().len(), 2);
    let got=c.update_item().table_name("synthetic-items").set_key(Some(key())).update_expression("SET entries = list_append(entries, :tail), revision = if_not_exists(revision, :zero) + :inc DELETE tags :tags, numberSet :numbers, binarySet :binaries")
        .expression_attribute_values(":tail",Av::L(vec![s("e")])).expression_attribute_values(":zero",n("0")).expression_attribute_values(":inc",n("0.2"))
        .expression_attribute_values(":tags",Av::Ss(vec!["red".into(),"blue".into(),"green".into()])).expression_attribute_values(":numbers",Av::Ns(vec!["1.0".into()])).expression_attribute_values(":binaries",Av::Bs(vec![Blob::new([0])]))
        .return_values(ReturnValue::AllNew).send().await.unwrap().attributes.unwrap();
    assert_eq!(got["revision"], n("0.3"));
    assert_eq!(got["entries"], Av::L(vec![s("b"), s("d"), s("e")]));
    assert!(!got.contains_key("tags"));
    assert_eq!(got["numberSet"], Av::Ns(vec!["2".into(), "3".into()]));
    assert_eq!(got["binarySet"], Av::Bs(vec![Blob::new([255])]));
}

#[tokio::test]
async fn nested_list_append_is_rejected_but_nested_if_not_exists_is_supported() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let original = map(&[("pk", s("item-1")), ("entries", Av::L(vec![n("1")]))]);
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .send()
        .await
        .unwrap();
    for expression in [
        "SET entries = if_not_exists(entries, list_append(:v, :v))",
        "SET entries = list_append(list_append(:v, :v), :v)",
        "SET entries = list_append(:v, list_append(:v, :v))",
    ] {
        let error = e
            .client
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression(expression)
            .expression_attribute_values(":v", Av::L(vec![n("2")]))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException"),
            "{expression}"
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    }
    e.client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression(
            "SET fresh = list_append(if_not_exists(absent, if_not_exists(another_missing, :v)), :v)",
        )
        .expression_attribute_values(":v", Av::L(vec![n("2")]))
        .send()
        .await
        .unwrap();
    let got = e.get("synthetic-items", key()).await.unwrap();
    assert_eq!(got["entries"], Av::L(vec![n("1")]));
    assert_eq!(got["fresh"], Av::L(vec![n("2"), n("2")]));
}

#[tokio::test]
async fn condition_grammar_functions_aliases_and_missing_values() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[
            ("pk", s("item-1")),
            ("payload", s("prefix-tail")),
            ("revision", n("3")),
            ("nothing", Av::Null(true)),
            ("tags", Av::Ss(vec!["blue".into()])),
        ])))
        .send()
        .await
        .unwrap();
    c.update_item().table_name("synthetic-items").set_key(Some(key())).update_expression("SET checked = :yes")
        .condition_expression("(revision BETWEEN :low AND :high AND revision IN (:low, :high)) AND NOT attribute_exists(absent) AND attribute_exists(nothing) AND attribute_type(nothing, :null) AND begins_with(#p, :prefix) AND contains(#p, :tail) AND size(#p) >= :length AND contains(tags, :blue) AND (revision < :low OR revision >= :high)")
        .expression_attribute_names("#p","payload").expression_attribute_values(":yes",Av::Bool(true)).expression_attribute_values(":low",n("1")).expression_attribute_values(":high",n("3")).expression_attribute_values(":null",s("NULL")).expression_attribute_values(":prefix",s("prefix")).expression_attribute_values(":tail",s("tail")).expression_attribute_values(":length",n("11")).expression_attribute_values(":blue",s("blue")).send().await.unwrap();
    assert_eq!(
        e.get("synthetic-items", key()).await.unwrap()["checked"],
        Av::Bool(true)
    );
    c.update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET checked = :v")
        .condition_expression("absent <> :v")
        .expression_attribute_values(":v", n("0"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.get("synthetic-items", key()).await.unwrap()["checked"],
        n("0")
    );
    // AND binds more tightly than OR.
    c.delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression(
            "attribute_exists(pk) OR attribute_exists(absent) AND attribute_not_exists(pk)",
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_attributes_compare_unequal_in_conditional_puts() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    for condition in ["#absent <> :v", ":v <> #absent", "doc.#absent <> :v"] {
        e.client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(map(&[
                ("pk", s("item-1")),
                ("doc", Av::M(map(&[("keep", n("1"))]))),
                ("checked", s(condition)),
            ])))
            .condition_expression(condition)
            .expression_attribute_names("#absent", "missing")
            .expression_attribute_values(":v", n("0"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            e.get("synthetic-items", key()).await.unwrap()["checked"],
            s(condition)
        );
    }
}

#[tokio::test]
async fn invalid_condition_literal_types_are_rejected_before_writing() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let original = map(&[
        ("pk", s("item-1")),
        ("payload", s("abc")),
        ("revision", n("1")),
    ]);
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .send()
        .await
        .unwrap();
    for (expression, operands) in [
        ("payload < :v", map(&[(":v", Av::M(HashMap::new()))])),
        (":v > payload", map(&[(":v", Av::Bool(true))])),
        ("payload <= :v", map(&[(":v", Av::Null(true))])),
        ("payload > :v", map(&[(":v", Av::L(vec![]))])),
        ("payload >= :v", map(&[(":v", Av::Ss(vec!["x".into()]))])),
        (
            "absent BETWEEN :low AND :high",
            map(&[(":low", s("a")), (":high", n("3"))]),
        ),
        (
            "payload BETWEEN :low AND :high",
            map(&[
                (":low", Av::M(HashMap::new())),
                (":high", Av::M(HashMap::new())),
            ]),
        ),
    ] {
        let mut values = operands;
        values.insert(":next".into(), n("2"));
        let error = e
            .client
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression("SET revision = :next")
            .condition_expression(format!("attribute_exists(pk) OR {expression}"))
            .set_expression_attribute_values(Some(values))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException"),
            "{expression}"
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    }
    // Stored types remain an evaluation mismatch, rather than invalid input.
    let error = e
        .client
        .delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("payload < :v")
        .expression_attribute_values(":v", n("3"))
        .send()
        .await
        .unwrap_err();
    assert!(
        error
            .as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
}

#[tokio::test]
async fn reversed_between_bounds_are_validation_errors_and_preserve_items() {
    let e = Emulator::new().await;
    e.table("synthetic-items", Some(ScalarAttributeType::N))
        .await;
    let original = map(&[("pk", s("item-1")), ("sk", n("3"))]);
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .send()
        .await
        .unwrap();
    for (condition, low, high) in [
        ("sk BETWEEN :low AND :high", n("5"), n("1")),
        ("absent BETWEEN :low AND :high", s("z"), s("a")),
        (":low BETWEEN sk AND :high", n("5"), n("1")),
        (
            "attribute_exists(pk) OR sk BETWEEN :low AND :high",
            n("5"),
            n("1"),
        ),
    ] {
        let err = e
            .client
            .delete_item()
            .table_name("synthetic-items")
            .set_key(Some(original.clone()))
            .condition_expression(condition)
            .expression_attribute_values(":low", low)
            .expression_attribute_values(":high", high)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(
            e.get("synthetic-items", original.clone()).await.unwrap(),
            original
        );
    }
    // Invalid literal bounds must also fail on an empty Query result.
    let err = e
        .client
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p AND sk BETWEEN :low AND :high")
        .expression_attribute_values(":p", s("empty"))
        .expression_attribute_values(":low", n("5"))
        .expression_attribute_values(":high", n("1"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
}

#[tokio::test]
async fn item_size_limit_rejects_oversized_puts_and_updates_atomically() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    // pk + item-1 + payload = 15 UTF-8 bytes; values fill the remaining 400 KiB.
    for payload in [
        s(&"é".repeat(204_792)), // 409,584 value bytes, one byte below the limit.
        Av::B(Blob::new(vec![0; 409_585])), // Exactly the limit, before base64 encoding.
    ] {
        let item = map(&[("pk", s("item-1")), ("payload", payload)]);
        e.client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(item.clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), item);
    }
    let original = e.get("synthetic-items", key()).await.unwrap();
    for payload in [
        s(&"x".repeat(500 * 1024)),
        Av::B(Blob::new(vec![0; 409_586])), // One byte over, counting names and key.
        Av::M(map(&[("x", Av::L(vec![s(&"x".repeat(409_577))]))])),
        Av::Ss(vec!["x".repeat(204_793), "y".repeat(204_793)]),
    ] {
        let err = e
            .client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(map(&[("pk", s("item-1")), ("payload", payload)])))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    }
    let err = e
        .client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET extra = :v")
        .expression_attribute_values(":v", Av::Bool(true))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    let err = e
        .client
        .put_item()
        .table_name("synthetic-items")
        .item("pk", s("absent-item"))
        .item("payload", s(&"x".repeat(500 * 1024)))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert!(
        e.get("synthetic-items", map(&[("pk", s("absent-item"))]))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn empty_attribute_names_are_rejected_without_changing_items() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let original = map(&[("pk", s("item-1")), ("payload", s("original"))]);
    e.client
        .put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .send()
        .await
        .unwrap();
    for invalid in [
        map(&[("pk", s("item-1")), ("", s("invalid"))]),
        map(&[("pk", s("item-1")), ("doc", Av::M(map(&[("", n("1"))])))]),
    ] {
        let error = e
            .client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(invalid))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    }
    let error = e
        .client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET doc = :v")
        .expression_attribute_values(":v", Av::M(map(&[("", n("1"))])))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
}

#[tokio::test]
async fn document_paths_allow_32_dereferences_and_reject_33() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    for lists in [false, true] {
        let nested = |mut value| {
            for _ in 0..32 {
                value = if lists {
                    Av::L(vec![value])
                } else {
                    Av::M(map(&[("x", value)]))
                };
            }
            value
        };
        e.client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(map(&[("pk", s("item-1")), ("doc", nested(n("1")))])))
            .send()
            .await
            .unwrap();
        let path = format!(
            "doc{}",
            if lists {
                "[0]".repeat(32)
            } else {
                ".x".repeat(32)
            }
        );
        e.client
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression(format!("SET {path} = :next"))
            .condition_expression(format!("{path} = :old"))
            .expression_attribute_values(":next", n("2"))
            .expression_attribute_values(":old", n("1"))
            .send()
            .await
            .unwrap();
        let expected = map(&[("pk", s("item-1")), ("doc", nested(n("2")))]);
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), expected);
        let too_deep = format!("{path}{}", if lists { "[0]" } else { ".x" });
        let error = e
            .client
            .delete_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .condition_expression(format!("attribute_not_exists({too_deep})"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn negative_numbers_and_number_sets_respect_the_item_size_boundary() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    // Names and key cost 21 bytes. -1 costs 3 bytes; {-1, -12} costs 6.
    for (number, payload_len) in [
        (n("-1"), 409_576),
        (Av::Ns(vec!["-1".into(), "-12".into()]), 409_573),
    ] {
        let item = map(&[
            ("pk", s("item-1")),
            ("number", number.clone()),
            ("payload", Av::B(Blob::new(vec![0; payload_len]))),
        ]);
        e.client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
        let original = e.get("synthetic-items", key()).await.unwrap();

        let oversized_payload = Av::B(Blob::new(vec![0; payload_len + 1]));
        let err = e
            .client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(map(&[
                ("pk", s("item-1")),
                ("number", number),
                ("payload", oversized_payload.clone()),
            ])))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);

        let err = e
            .client
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression("SET payload = :v")
            .expression_attribute_values(":v", oversized_payload)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    }
}

#[tokio::test]
async fn numeric_query_comparisons_order_and_pages() {
    let e = Emulator::new().await;
    e.table("synthetic-items", Some(ScalarAttributeType::N))
        .await;
    let c = &e.client;
    for sk in [
        "10",
        "2",
        "-1",
        "3",
        "10000000000000000000000000000000000000",
    ] {
        c.put_item()
            .table_name("synthetic-items")
            .item("pk", s("group"))
            .item("sk", n(sk))
            .send()
            .await
            .unwrap();
    }
    c.put_item()
        .table_name("synthetic-items")
        .item("pk", s("other"))
        .item("sk", n("2"))
        .send()
        .await
        .unwrap();
    for (condition, expected) in [
        ("#sk = :v", vec!["3"]),
        ("#sk < :v", vec!["-1", "2"]),
        ("#sk <= :v", vec!["-1", "2", "3"]),
        (
            "#sk > :v",
            vec!["10", "10000000000000000000000000000000000000"],
        ),
        (
            "#sk >= :v",
            vec!["3", "10", "10000000000000000000000000000000000000"],
        ),
    ] {
        let got = c
            .query()
            .table_name("synthetic-items")
            .key_condition_expression(format!("#pk = :pk AND {condition}"))
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_names("#sk", "sk")
            .expression_attribute_values(":pk", s("group"))
            .expression_attribute_values(":v", n("3.0"))
            .send()
            .await
            .unwrap();
        let keys: Vec<_> = got
            .items()
            .iter()
            .map(|i| i["sk"].as_n().unwrap().as_str())
            .collect();
        assert_eq!(keys, expected);
        assert_eq!(got.count(), got.scanned_count());
    }
    let between = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p AND sk BETWEEN :a AND :b")
        .expression_attribute_values(":p", s("group"))
        .expression_attribute_values(":a", n("2"))
        .expression_attribute_values(":b", n("10"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        between
            .items()
            .iter()
            .map(|i| i["sk"].as_n().unwrap().as_str())
            .collect::<Vec<_>>(),
        vec!["2", "3", "10"]
    );
    let first = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("group"))
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(
        first
            .items()
            .iter()
            .map(|i| i["sk"].as_n().unwrap().as_str())
            .collect::<Vec<_>>(),
        vec!["-1", "2"]
    );
    let cursor = first.last_evaluated_key.unwrap();
    c.delete_item()
        .table_name("synthetic-items")
        .set_key(Some(cursor.clone()))
        .send()
        .await
        .unwrap();
    let second = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("group"))
        .set_exclusive_start_key(Some(cursor))
        .send()
        .await
        .unwrap();
    assert_eq!(
        second
            .items()
            .iter()
            .map(|i| i["sk"].as_n().unwrap().as_str())
            .collect::<Vec<_>>(),
        vec!["3", "10", "10000000000000000000000000000000000000"]
    );
    assert!(second.last_evaluated_key.is_none());
    let reverse = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("group"))
        .scan_index_forward(false)
        .limit(1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        reverse.items()[0]["sk"],
        n("10000000000000000000000000000000000000")
    );
    let remaining = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("group"))
        .scan_index_forward(false)
        .set_exclusive_start_key(reverse.last_evaluated_key)
        .select(Select::Count)
        .send()
        .await
        .unwrap();
    assert_eq!(remaining.count(), 3);
    assert!(remaining.items.is_none());
}

#[tokio::test]
async fn query_accepts_reversed_key_comparison_operands() {
    let e = Emulator::new().await;
    e.table("synthetic-items", Some(ScalarAttributeType::N))
        .await;
    for (partition, sort) in [
        ("item-1", "1"),
        ("item-1", "2"),
        ("item-1", "3"),
        ("other", "2"),
    ] {
        e.client
            .put_item()
            .table_name("synthetic-items")
            .set_item(Some(map(&[("pk", s(partition)), ("sk", n(sort))])))
            .send()
            .await
            .unwrap();
    }
    for (condition, uses_sort, expected) in [
        (":p = #pk", false, vec![n("1"), n("2"), n("3")]),
        (":p = #pk AND :v = sk", true, vec![n("2")]),
        (":p = #pk AND :v < sk", true, vec![n("3")]),
        (":p = #pk AND :v <= sk", true, vec![n("2"), n("3")]),
        (":p = #pk AND :v > sk", true, vec![n("1")]),
        (":v >= sk AND :p = #pk", true, vec![n("1"), n("2")]),
    ] {
        let mut query = e
            .client
            .query()
            .table_name("synthetic-items")
            .key_condition_expression(condition)
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_values(":p", s("item-1"));
        if uses_sort {
            query = query.expression_attribute_values(":v", n("2"));
        }
        let page = query.send().await.unwrap();
        let actual = page
            .items()
            .iter()
            .map(|item| item["sk"].clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{condition}");
    }
}

#[tokio::test]
async fn string_and_binary_query_prefixes_and_order() {
    let e = Emulator::new().await;
    let c = &e.client;
    for (table, tag, keys, prefix, expected) in [
        (
            "synthetic-text",
            ScalarAttributeType::S,
            vec![s("b"), s("a2"), s("a1"), s("é")],
            s("a"),
            vec![s("a1"), s("a2")],
        ),
        (
            "synthetic-binary",
            ScalarAttributeType::B,
            vec![
                Av::B(Blob::new([255])),
                Av::B(Blob::new([128, 2])),
                Av::B(Blob::new([128, 1])),
                Av::B(Blob::new([0])),
            ],
            Av::B(Blob::new([128])),
            vec![Av::B(Blob::new([128, 1])), Av::B(Blob::new([128, 2]))],
        ),
    ] {
        e.table(table, Some(tag)).await;
        for sk in keys {
            c.put_item()
                .table_name(table)
                .item("pk", s("group"))
                .item("sk", sk)
                .send()
                .await
                .unwrap();
        }
        let got = c
            .query()
            .table_name(table)
            .key_condition_expression("pk = :p AND begins_with(sk, :prefix)")
            .expression_attribute_values(":p", s("group"))
            .expression_attribute_values(":prefix", prefix)
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.items()
                .iter()
                .map(|i| i["sk"].clone())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn decimal_keys_normalize_without_float_rounding_and_invalid_values_fail() {
    let e = Emulator::new().await;
    e.table("synthetic-items", Some(ScalarAttributeType::N))
        .await;
    let c = &e.client;
    for sk in ["1", "1.0", "1E0"] {
        c.put_item()
            .table_name("synthetic-items")
            .item("pk", s("group"))
            .item("sk", n(sk))
            .send()
            .await
            .unwrap();
    }
    for sk in ["9007199254740992", "9007199254740993"] {
        c.put_item()
            .table_name("synthetic-items")
            .item("pk", s("group"))
            .item("sk", n(sk))
            .send()
            .await
            .unwrap();
    }
    let got = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("group"))
        .send()
        .await
        .unwrap();
    assert_eq!(got.count(), 3);
    assert_eq!(got.items()[1]["sk"], n("9007199254740992"));
    assert_eq!(got.items()[2]["sk"], n("9007199254740993"));
    for value in [
        n("123456789012345678901234567890123456789"),
        n("1e126"),
        n("1e-131"),
        n("NaN"),
        Av::Ss(vec![]),
        Av::Ns(vec!["1".into(), "1.0".into()]),
    ] {
        let err = c
            .put_item()
            .table_name("synthetic-items")
            .item("pk", s("group"))
            .item("sk", n("2"))
            .item("bad", value)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
    }
}

#[tokio::test]
async fn invalid_updates_are_atomic_and_unsupported_queries_are_rejected() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    let original = map(&[("pk", s("item-1")), ("revision", n("1"))]);
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(original.clone()))
        .send()
        .await
        .unwrap();
    for expr in [
        "SET revision = :v, revision = :v",
        "SET pk = :v",
        "SET revision = :v, doc.nested = :v",
        "SET revision = :v + absent",
        "SET #missing = :v",
        "SET size = :v",
        "ADD revision :v SET revision = :v",
        "SET revision = :v * :v",
    ] {
        let err = c
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression(expr)
            .expression_attribute_values(":v", n("2"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException"),
            "{expr}"
        );
        assert_eq!(
            e.get("synthetic-items", key()).await.unwrap(),
            original,
            "{expr}"
        );
    }
    let err = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET revision = :v")
        .expression_attribute_values(":v", n("2"))
        .expression_attribute_values(":unused", n("3"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    let err = c
        .delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("contains(pk, pk)")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert_eq!(e.get("synthetic-items", key()).await.unwrap(), original);
    for condition in [
        "pk > :p",
        "attribute_exists(pk)",
        "revision = :p",
        "pk = :p OR pk = :p",
    ] {
        let err = c
            .query()
            .table_name("synthetic-items")
            .key_condition_expression(condition)
            .expression_attribute_values(":p", s("item-1"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
    }
    let err = c
        .query()
        .table_name("synthetic-items")
        .key_condition_expression("pk = :p")
        .expression_attribute_values(":p", s("item-1"))
        .filter_expression("attribute_exists(pk)")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
}

#[tokio::test]
async fn concurrent_conditional_updates_have_one_winner() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[("pk", s("item-1")), ("revision", n("0"))])))
        .send()
        .await
        .unwrap();
    let update = || {
        c.update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression("SET revision = :next")
            .condition_expression("revision = :old")
            .expression_attribute_values(":next", n("1"))
            .expression_attribute_values(":old", n("0"))
            .send()
    };
    let (a, b) = tokio::join!(update(), update());
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let failure = a.err().or(b.err()).unwrap();
    assert!(
        failure
            .as_service_error()
            .unwrap()
            .is_conditional_check_failed_exception()
    );
    assert_eq!(
        e.get("synthetic-items", key()).await.unwrap()["revision"],
        n("1")
    );
}

#[tokio::test]
async fn list_descendants_use_original_indices_before_sibling_removals() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    let entries = Av::L(vec![
        Av::M(map(&[("x", n("1"))])),
        Av::M(map(&[("x", n("2")), ("y", n("3"))])),
    ]);
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[("pk", s("item-1")), ("entries", entries)])))
        .send()
        .await
        .unwrap();
    let got = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE entries[0], entries[1].y SET entries[1].x = :v")
        .expression_attribute_values(":v", n("9"))
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(got["entries"], Av::L(vec![Av::M(map(&[("x", n("9"))]))]));
    let got = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET entries[2] = :v REMOVE entries[1]")
        .expression_attribute_values(":v", s("appended"))
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(
        got["entries"],
        Av::L(vec![Av::M(map(&[("x", n("9"))])), s("appended")])
    );
}

#[tokio::test]
async fn updated_list_return_values_compact_elements_and_select_nested_paths() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let numbers = Av::L(vec![n("10"), n("20"), n("30")]);
    for (expression, values, original, expected_old, expected_new) in [
        (
            "SET entries[1] = :v",
            map(&[(":v", n("99"))]),
            numbers.clone(),
            Av::L(vec![n("20")]),
            Av::L(vec![n("99")]),
        ),
        (
            "SET entries[2] = :v, entries[0] = :w",
            map(&[(":v", n("99")), (":w", n("88"))]),
            numbers,
            Av::L(vec![n("10"), n("30")]),
            Av::L(vec![n("88"), n("99")]),
        ),
        (
            "SET entries[1].x = :v, entries[0].y = :w, entries[1].y = :w",
            map(&[(":v", n("99")), (":w", n("88"))]),
            Av::L(vec![
                Av::M(map(&[("x", n("10")), ("y", n("20"))])),
                Av::M(map(&[("x", n("30")), ("y", n("40")), ("keep", s("yes"))])),
                Av::M(map(&[("keep", s("untouched"))])),
            ]),
            Av::L(vec![
                Av::M(map(&[("y", n("20"))])),
                Av::M(map(&[("x", n("30")), ("y", n("40"))])),
            ]),
            Av::L(vec![
                Av::M(map(&[("y", n("88"))])),
                Av::M(map(&[("x", n("99")), ("y", n("88"))])),
            ]),
        ),
    ] {
        for (mode, expected) in [
            (ReturnValue::UpdatedOld, expected_old),
            (ReturnValue::UpdatedNew, expected_new),
        ] {
            e.client
                .put_item()
                .table_name("synthetic-items")
                .set_item(Some(map(&[
                    ("pk", s("item-1")),
                    ("entries", original.clone()),
                ])))
                .send()
                .await
                .unwrap();
            let got = e
                .client
                .update_item()
                .table_name("synthetic-items")
                .set_key(Some(key()))
                .update_expression(expression)
                .set_expression_attribute_values(Some(values.clone()))
                .return_values(mode)
                .send()
                .await
                .unwrap()
                .attributes
                .unwrap();
            assert_eq!(got, map(&[("entries", expected)]), "{expression}");
        }
    }
}

#[tokio::test]
async fn remove_only_update_leaves_a_missing_item_absent() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    for mode in [
        ReturnValue::None,
        ReturnValue::AllOld,
        ReturnValue::AllNew,
        ReturnValue::UpdatedOld,
        ReturnValue::UpdatedNew,
    ] {
        let got = e
            .client
            .update_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .update_expression("REMOVE absent, another_missing")
            .return_values(mode)
            .send()
            .await
            .unwrap();
        assert!(got.attributes.is_none());
        assert!(e.get("synthetic-items", key()).await.is_none());
    }
    e.client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET fresh = :v REMOVE absent")
        .expression_attribute_values(":v", n("1"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        e.get("synthetic-items", key()).await.unwrap(),
        map(&[("pk", s("item-1")), ("fresh", n("1"))])
    );
    let got = e
        .client
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE fresh")
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .unwrap();
    assert_eq!(got.attributes, Some(key()));
    assert_eq!(e.get("synthetic-items", key()).await, Some(key()));
}

#[tokio::test]
async fn nested_return_values_select_only_paths_present_in_the_image() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[
            ("pk", s("item-1")),
            ("doc", Av::M(map(&[("a", n("1")), ("keep", s("retained"))]))),
            ("entries", Av::L(vec![s("a"), s("b"), s("c")])),
        ])))
        .send()
        .await
        .unwrap();
    let old = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET doc.fresh = :v")
        .expression_attribute_values(":v", n("2"))
        .return_values(ReturnValue::UpdatedOld)
        .send()
        .await
        .unwrap();
    assert!(old.attributes.is_none());
    let old = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET doc.a = :v")
        .expression_attribute_values(":v", n("9"))
        .return_values(ReturnValue::UpdatedOld)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(old, map(&[("doc", Av::M(map(&[("a", n("1"))])))]));
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET doc.a = :v, doc.fresh = :v")
        .expression_attribute_values(":v", n("9"))
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(
        new,
        map(&[("doc", Av::M(map(&[("a", n("9")), ("fresh", n("9"))])))])
    );
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE doc.a")
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap();
    assert!(new.attributes.is_none());
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("SET entries[99] = :v")
        .expression_attribute_values(":v", s("d"))
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap();
    assert!(new.attributes.is_none());
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE entries[3]")
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap();
    assert!(new.attributes.is_none());
    let new = c
        .update_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .update_expression("REMOVE entries[1]")
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(new, map(&[("entries", Av::L(vec![s("c")]))]));
    let got = e.get("synthetic-items", key()).await.unwrap();
    assert_eq!(
        got["doc"],
        Av::M(map(&[("fresh", n("9")), ("keep", s("retained"))]))
    );
}

#[tokio::test]
async fn conditions_handle_unicode_size_and_nonmatching_stored_types() {
    let e = Emulator::new().await;
    e.table("synthetic-items", None).await;
    let c = &e.client;
    c.put_item()
        .table_name("synthetic-items")
        .set_item(Some(map(&[
            ("pk", s("item-1")),
            ("word", s("é😀")),
            ("num", n("4")),
        ])))
        .send()
        .await
        .unwrap();
    c.update_item().table_name("synthetic-items").set_key(Some(key())).update_expression("SET checked = :yes")
        .condition_expression("size(word) = :three AND (attribute_type(num, :kind) OR begins_with(num, :text)) AND (attribute_type(num, :kind) OR size(num) > :zero)")
        .expression_attribute_values(":yes",Av::Bool(true)).expression_attribute_values(":three",n("3")).expression_attribute_values(":kind",s("N")).expression_attribute_values(":text",s("4")).expression_attribute_values(":zero",n("0")).send().await.unwrap();
    for (condition, value) in [("size(word) = :v", n("6")), ("contains(num, :v)", n("4"))] {
        let err = c
            .delete_item()
            .table_name("synthetic-items")
            .set_key(Some(key()))
            .condition_expression(condition)
            .expression_attribute_values(":v", value)
            .send()
            .await
            .unwrap_err();
        assert!(
            err.as_service_error()
                .unwrap()
                .is_conditional_check_failed_exception()
        );
    }
    let err = c
        .delete_item()
        .table_name("synthetic-items")
        .set_key(Some(key()))
        .condition_expression("begins_with(word, :v)")
        .expression_attribute_values(":v", n("4"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("ValidationException")
    );
    assert_eq!(
        e.get("synthetic-items", key()).await.unwrap()["checked"],
        Av::Bool(true)
    );
}

#[tokio::test]
async fn query_limit_boundaries_and_cursor_scope_match_dynamodb() {
    let e = Emulator::new().await;
    e.table("synthetic-items", Some(ScalarAttributeType::S))
        .await;
    let c = &e.client;
    for sk in ["b", "c"] {
        c.put_item()
            .table_name("synthetic-items")
            .item("pk", s("group"))
            .item("sk", s(sk))
            .send()
            .await
            .unwrap();
    }
    let query = || {
        c.query()
            .table_name("synthetic-items")
            .key_condition_expression("pk = :p AND sk >= :b")
            .expression_attribute_values(":p", s("group"))
            .expression_attribute_values(":b", s("b"))
    };
    let first = query().limit(2).send().await.unwrap();
    assert_eq!(first.count(), 2);
    assert_eq!(first.last_evaluated_key.as_ref().unwrap()["sk"], s("c"));
    let empty = query()
        .set_exclusive_start_key(first.last_evaluated_key)
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(empty.count(), 0);
    assert!(empty.last_evaluated_key.is_none());
    for cursor in [
        map(&[("pk", s("group"))]),
        map(&[("pk", s("group")), ("sk", s("b")), ("extra", s("value"))]),
        map(&[("pk", s("other")), ("sk", s("b"))]),
        map(&[("pk", s("group")), ("sk", s("a"))]),
    ] {
        let err = query()
            .set_exclusive_start_key(Some(cursor))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
    }
}
