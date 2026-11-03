//! In-process tests can hold the private database guard and race real tasks.
//! SDK sequence tests exercise the wire boundary separately.
use super::Service;
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn config() -> Config {
    let mut c = Config::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        c.cases = 32;
    }
    if std::env::var_os("PROPTEST_RNG_SEED").is_none() {
        c.rng_seed = RngSeed::Fixed(0x5a08_0208);
    }
    c.failure_persistence = Some(Box::new(FileFailurePersistence::Direct(
        "proptest-regressions/dynamodb-snapshots.txt",
    )));
    c
}
async fn call(s: &Service, op: &str, body: Value) -> Value {
    s.dispatch(op, &serde_json::to_vec(&body).unwrap())
        .await
        .unwrap()
}
async fn database(rows: u8) -> Service {
    let s = Service::default();
    call(&s,"CreateTable",json!({"TableName":"synthetic-snapshot","BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}]})).await;
    for k in 0..rows {
        call(&s,"PutItem",json!({"TableName":"synthetic-snapshot","Item":{"pk":{"S":"p"},"sk":{"N":k.to_string()},"revision":{"N":"0"}}})).await;
    }
    s
}
fn read_request(query: bool) -> Value {
    if query {
        json!({"TableName":"synthetic-snapshot","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"p"}}})
    } else {
        json!({"TableName":"synthetic-snapshot"})
    }
}

fn table_request() -> Value {
    json!({"TableName":"synthetic-schema", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]})
}

async fn create_result(request: Value, accepted: bool) {
    let service = Service::default();
    let result = service
        .dispatch("CreateTable", &serde_json::to_vec(&request).unwrap())
        .await;
    if accepted {
        result.expect("valid table configuration must be accepted");
        call(
            &service,
            "DescribeTable",
            json!({"TableName":"synthetic-schema"}),
        )
        .await;
    } else {
        assert_eq!(result.unwrap_err().code, "ValidationException", "{request}");
        assert_eq!(
            service
                .dispatch("DescribeTable", br#"{"TableName":"synthetic-schema"}"#)
                .await
                .unwrap_err()
                .code,
            "ResourceNotFoundException"
        );
    }
}

#[tokio::test]
async fn wire_create_and_describe_dispatch_supported_targets() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        axum::serve(listener, crate::server::router())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    // Exercise wire dispatch before the SDK sequence property: a broken target
    // must fail directly, without SDK retries or property shrinking delaying it.
    for (target, request, description) in [
        (
            "DynamoDB_20120810.CreateTable",
            table_request(),
            "TableDescription",
        ),
        (
            "DynamoDB_20120810.DescribeTable",
            json!({"TableName":"synthetic-schema"}),
            "Table",
        ),
    ] {
        let response = client
            .post(&endpoint)
            .header("x-amz-target", target)
            .body(serde_json::to_vec(&request).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{target}");
        let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body[description]["TableName"], "synthetic-schema");
    }
}

#[tokio::test]
async fn create_table_rejects_isolated_key_schema_and_local_index_errors() {
    for (schema, definitions) in [
        (
            json!([{"AttributeName":"pk","KeyType":"INVALID"}]),
            json!([{"AttributeName":"pk","AttributeType":"S"}]),
        ),
        (
            json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"INVALID"}]),
            json!([{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]),
        ),
        (
            json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"pk","KeyType":"RANGE"}]),
            json!([{"AttributeName":"pk","AttributeType":"S"}]),
        ),
    ] {
        let mut request = table_request();
        request["KeySchema"] = schema;
        request["AttributeDefinitions"] = definitions;
        create_result(request, false).await;
    }
    for (partition, sort, throughput) in [
        ("other", "alt", None),
        ("pk", "sk", None),
        (
            "pk",
            "alt",
            Some(json!({"ReadCapacityUnits":1,"WriteCapacityUnits":1})),
        ),
    ] {
        let mut request = table_request();
        let mut index = json!({"IndexName":"synthetic-index",
            "KeySchema":[{"AttributeName":partition,"KeyType":"HASH"},{"AttributeName":sort,"KeyType":"RANGE"}],
            "Projection":{"ProjectionType":"ALL"}});
        if let Some(throughput) = throughput {
            index["ProvisionedThroughput"] = throughput;
        }
        request["LocalSecondaryIndexes"] = json!([index]);
        for name in [partition, sort] {
            if name != "pk" && name != "sk" {
                request["AttributeDefinitions"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"AttributeName":name,"AttributeType":"S"}));
            }
        }
        create_result(request, false).await;
    }
}

#[tokio::test]
async fn create_table_accepts_index_count_limits_and_rejects_outside_them() {
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-secondary-indexes
    for (local, count, accepted) in [
        (false, 20, true),
        (false, 21, false),
        (false, 0, false),
        (true, 5, true),
        (true, 6, false),
        (true, 0, false),
    ] {
        let mut request = table_request();
        let indexes = (0..count).map(|number| {
            let keys = if local {
                json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"alt","KeyType":"RANGE"}])
            } else {
                json!([{"AttributeName":"pk","KeyType":"HASH"}])
            };
            json!({"IndexName":format!("synthetic-index-{number}"), "KeySchema":keys,
                "Projection":{"ProjectionType":"ALL"}})
        }).collect::<Vec<_>>();
        request[if local {
            "LocalSecondaryIndexes"
        } else {
            "GlobalSecondaryIndexes"
        }] = json!(indexes);
        if local && count > 0 {
            request["AttributeDefinitions"]
                .as_array_mut()
                .unwrap()
                .push(json!({"AttributeName":"alt","AttributeType":"S"}));
        }
        create_result(request, accepted).await;
    }
}

#[tokio::test]
async fn create_table_validates_include_projection_boundaries_and_names() {
    // INCLUDE allows 20 attributes per index and 100 across all indexes.
    // Names, including projected names, have a 255-byte limit.
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html
    let mut valid = table_request();
    valid["GlobalSecondaryIndexes"] = json!((0..5).map(|index| json!({
        "IndexName":format!("synthetic-index-{index}"),
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":
            (0..20).map(|attribute| format!("field_{index}_{attribute}")).collect::<Vec<_>>()}
    })).collect::<Vec<_>>());
    valid["GlobalSecondaryIndexes"][0]["Projection"]["NonKeyAttributes"][0] =
        json!("a".repeat(255));
    create_result(valid.clone(), true).await;
    let mut over_total = valid;
    over_total["GlobalSecondaryIndexes"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "IndexName":"synthetic-extra", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["extra"]}
        }));
    create_result(over_total, false).await;
    for attributes in [
        json!([]),
        json!((0..21).map(|i| format!("field_{i}")).collect::<Vec<_>>()),
        json!(["duplicate", "duplicate"]),
        json!([""]),
        json!(["a".repeat(256)]),
        json!(["pk"]),
    ] {
        let mut request = table_request();
        request["GlobalSecondaryIndexes"] = json!([{"IndexName":"synthetic-index",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":attributes}}]);
        create_result(request, false).await;
    }
}

#[tokio::test]
async fn create_table_requires_matching_scalar_definitions_and_bounded_key_names() {
    for (name, accepted) in [
        ("a".repeat(255), true),
        ("a".repeat(256), false),
        (String::new(), false),
    ] {
        let mut request = table_request();
        request["KeySchema"] = json!([{"AttributeName":name,"KeyType":"HASH"}]);
        request["AttributeDefinitions"] = json!([{"AttributeName":name,"AttributeType":"S"}]);
        create_result(request, accepted).await;
    }
    for definitions in [
        json!([{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"}]),
        json!([{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"other","AttributeType":"S"}]),
        json!([{"AttributeName":"pk","AttributeType":"BOOL"},{"AttributeName":"sk","AttributeType":"S"}]),
    ] {
        let mut request = table_request();
        request["AttributeDefinitions"] = definitions;
        create_result(request, false).await;
    }
}

#[tokio::test]
async fn create_table_billing_and_index_throughput_are_validated_before_publication() {
    let mut invalid_mode = table_request();
    invalid_mode["BillingMode"] = json!("UNKNOWN");
    create_result(invalid_mode, false).await;
    for indexed in [false, true] {
        let mut on_demand = table_request();
        if indexed {
            on_demand["GlobalSecondaryIndexes"] = json!([{
                "IndexName":"synthetic-index", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
                "Projection":{"ProjectionType":"ALL"},
                "ProvisionedThroughput":{"ReadCapacityUnits":1,"WriteCapacityUnits":1}
            }]);
        } else {
            on_demand["ProvisionedThroughput"] =
                json!({"ReadCapacityUnits":1,"WriteCapacityUnits":1});
        }
        create_result(on_demand, false).await;
        for (throughput, accepted) in [
            (None, false),
            (Some((0, 1)), false),
            (Some((-1, 1)), false),
            (Some((1, 0)), false),
            (Some((1, -1)), false),
            (Some((1, 1)), true),
        ] {
            let mut request = table_request();
            request["BillingMode"] = json!("PROVISIONED");
            let throughput = throughput.map(|(read, write)| {
                json!({
                    "ReadCapacityUnits":read, "WriteCapacityUnits":write
                })
            });
            if indexed {
                request["ProvisionedThroughput"] =
                    json!({"ReadCapacityUnits":1,"WriteCapacityUnits":1});
                let mut index = json!({"IndexName":"synthetic-index",
                    "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}], "Projection":{"ProjectionType":"ALL"}});
                if let Some(throughput) = throughput {
                    index["ProvisionedThroughput"] = throughput;
                }
                request["GlobalSecondaryIndexes"] = json!([index]);
            } else if let Some(throughput) = throughput {
                request["ProvisionedThroughput"] = throughput;
            }
            create_result(request, accepted).await;
        }
    }
}

proptest! {
    #![proptest_config(config())]
    #[test]
    fn query_scan_share_read_lock(rows in 1u8..12) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let s=database(rows).await;
            // A second reader must make progress while this guard remains held.
            // An exclusive Query/Scan dispatch deadlocks until the timeout fires.
            let guard=s.database.read().await;
            for (op,query) in [("Query",true),("Scan",false)] {
                let response=tokio::time::timeout(Duration::from_secs(1),s.dispatch(op,&serde_json::to_vec(&read_request(query)).unwrap())).await
                    .expect("Query/Scan blocked another reader").unwrap();
                assert_eq!(response["Count"],json!(rows));
                assert_eq!(response["Items"].as_array().unwrap().len(),usize::from(rows));
            }
            drop(guard);
        });
    }
    #[test]
    fn concurrent_writer_preserves_read_snapshots(
        rows in 2u8..9,
        revisions in prop::collection::vec(1i16..1000,2..25),
        reads in prop::collection::vec(0u8..3,8..40)
    ) {
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap().block_on(async {
            // Bound the complete race, including writer readiness and shutdown,
            // so a lock regression fails the property instead of hanging it.
            tokio::time::timeout(Duration::from_secs(5), async {
            let s=database(rows).await;
            let mut committed:BTreeSet<i16>=revisions.iter().copied().collect(); committed.insert(0);
            let writer=s.clone();
            let stop=Arc::new(AtomicBool::new(false));
            let writer_stop=stop.clone();
            let (ready,started)=tokio::sync::oneshot::channel();
            let task=tokio::spawn(async move {
                let mut ready=Some(ready);
                // Keep writing throughout the read window on another worker.
                // A per-row unlock/relock can now race without test-only yields
                // inside dispatch or inside the injected defect.
                for revision in revisions.into_iter().cycle() {
                    if writer_stop.load(Ordering::Relaxed) {break;}
                    let actions:Vec<_>=(0..rows).map(|k|json!({"Update":{"TableName":"synthetic-snapshot",
                        "Key":{"pk":{"S":"p"},"sk":{"N":k.to_string()}},"UpdateExpression":"SET revision = :v",
                        "ExpressionAttributeValues":{":v":{"N":revision.to_string()}}}})).collect();
                    call(&writer,"TransactWriteItems",json!({"TransactItems":actions})).await;
                    if let Some(ready)=ready.take() {let _=ready.send(());}
                    tokio::task::yield_now().await;
                }
            });
            started.await.expect("writer stopped before its first commit");
            for kind in [0,1,2].into_iter().chain(reads) {
                let items=if kind==2 {
                    let gets:Vec<_>=(0..rows).map(|k|json!({"Get":{"TableName":"synthetic-snapshot","Key":{"pk":{"S":"p"},"sk":{"N":k.to_string()}}}})).collect();
                    call(&s,"TransactGetItems",json!({"TransactItems":gets})).await["Responses"].as_array().unwrap().iter().map(|v|v["Item"].clone()).collect::<Vec<_>>()
                } else {
                    let response=call(&s,if kind==0 {"Query"} else {"Scan"},read_request(kind==0)).await;
                    assert_eq!(response["Count"],json!(rows));
                    response["Items"].as_array().unwrap().clone()
                };
                assert_eq!(items.len(),usize::from(rows));
                let snapshot:BTreeSet<_>=items.iter().map(|i|i["revision"]["N"].as_str().unwrap().parse::<i16>().unwrap()).collect();
                assert_eq!(snapshot.len(),1,"torn read across a transaction: {items:?}");
                assert!(committed.contains(snapshot.first().unwrap()),"uncommitted revision");
                tokio::task::yield_now().await;
            }
            stop.store(true,Ordering::Relaxed);
            task.await.unwrap();
            }).await.expect("concurrent snapshot read/write deadlocked");
        });
    }
}

#[tokio::test]
async fn batch_get_accepts_exact_partition_and_response_size_limits() {
    let s = Service::default();
    call(&s, "CreateTable", json!({
        "TableName":"synthetic-batch-boundary", "BillingMode":"PAY_PER_REQUEST",
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
        "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]
    })).await;
    // AWS sizes strings by UTF-8 attribute-name plus value bytes:
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
    // Here names take 11 bytes and the two key values take 3: each item is 256 KiB.
    let payload = "x".repeat(256 * 1024 - 14);
    let mut keys = Vec::new();
    for sk in ["0", "1", "2", "3"] {
        let key = json!({"pk":{"S":"pp"},"sk":{"S":sk}});
        keys.push(key.clone());
        let mut item = key;
        item["payload"] = json!({"S":payload});
        call(
            &s,
            "PutItem",
            json!({"TableName":"synthetic-batch-boundary","Item":item}),
        )
        .await;
    }
    let response = call(
        &s,
        "BatchGetItem",
        json!({"RequestItems":{
            "synthetic-batch-boundary":{"Keys":keys}
        }}),
    )
    .await;
    // Four 256 KiB items exactly fill the per-partition page, without deferral.
    assert_eq!(
        response["Responses"]["synthetic-batch-boundary"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(response["UnprocessedKeys"].as_object().unwrap().is_empty());
    drop(response);

    let mut keys = Vec::new();
    for i in 0..64 {
        let key = json!({"pk":{"S":format!("{i:02}")},"sk":{"S":"0"}});
        keys.push(key.clone());
        let mut item = key;
        item["payload"] = json!({"S":payload});
        call(
            &s,
            "PutItem",
            json!({"TableName":"synthetic-batch-boundary","Item":item}),
        )
        .await;
    }
    let response = call(
        &s,
        "BatchGetItem",
        json!({"RequestItems":{
            "synthetic-batch-boundary":{"Keys":keys}
        }}),
    )
    .await;
    // Distinct partitions isolate the inclusive 16 MiB aggregate response cap.
    let rows = response["Responses"]["synthetic-batch-boundary"]
        .as_array()
        .unwrap();
    assert_eq!(rows.len(), 64);
    assert!(response["UnprocessedKeys"].as_object().unwrap().is_empty());
    assert_eq!(
        rows.iter()
            .map(|row| row["pk"]["S"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>(),
        (0..64).map(|i| format!("{i:02}")).collect()
    );
}

#[tokio::test]
async fn updates_handle_existing_numbers_missing_sets_and_ordered_list_appends() {
    let s = database(1).await;
    for (expression, values) in [
        (
            "SET entries = :v",
            json!({":v":{"L":[{"S":"a"},{"S":"b"}]}}),
        ),
        ("ADD revision :v", json!({":v":{"N":"5"}})),
        ("ADD revision :v", json!({":v":{"N":"3"}})),
        ("ADD tags :v", json!({":v":{"SS":["red"]}})),
        ("DELETE missingSet :v", json!({":v":{"SS":["red"]}})),
        (
            "SET entries[4] = :four, entries[3] = :three",
            json!({":four":{"S":"four"},":three":{"S":"three"}}),
        ),
    ] {
        call(
            &s,
            "UpdateItem",
            json!({
                "TableName":"synthetic-snapshot", "Key":{"pk":{"S":"p"},"sk":{"N":"0"}},
                "UpdateExpression":expression, "ExpressionAttributeValues":values
            }),
        )
        .await;
    }
    let read = json!({"TableName":"synthetic-snapshot","Key":{"pk":{"S":"p"},"sk":{"N":"0"}}});
    let expected = json!({
        "pk":{"S":"p"}, "sk":{"N":"0"}, "revision":{"N":"8"},
        "entries":{"L":[{"S":"a"},{"S":"b"},{"S":"three"},{"S":"four"}]},
        "tags":{"SS":["red"]}
    });
    assert_eq!(call(&s, "GetItem", read.clone()).await["Item"], expected);
    let invalid = json!({
        "TableName":"synthetic-snapshot", "Key":{"pk":{"S":"p"},"sk":{"N":"0"}},
        "UpdateExpression":"REMOVE unwritten.child"
    });
    let error = s
        .dispatch("UpdateItem", &serde_json::to_vec(&invalid).unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.code, "ValidationException");
    assert_eq!(call(&s, "GetItem", read).await["Item"], expected);
}

#[tokio::test]
async fn table_names_and_return_modes_validate_before_publication() {
    let s = database(1).await;
    // AWS table names contain 3..=255 ASCII letters/digits or '_', '-', '.'.
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-naming-rules
    for (name, accepted) in [
        ("abc".to_owned(), true),
        ("s".repeat(255), true),
        ("ab".to_owned(), false),
        ("s".repeat(256), false),
        ("bad/name".to_owned(), false),
    ] {
        let request = json!({
            "TableName":name, "BillingMode":"PAY_PER_REQUEST",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]
        });
        let result = s
            .dispatch("CreateTable", &serde_json::to_vec(&request).unwrap())
            .await;
        if accepted {
            result.unwrap();
            assert_eq!(
                call(&s, "DescribeTable", json!({"TableName":name})).await["Table"]["TableName"],
                name
            );
        } else {
            assert_eq!(result.unwrap_err().code, "ValidationException");
        }
    }
    for (operation, request) in [
        (
            "UpdateItem",
            json!({
                "TableName":"synthetic-snapshot", "Key":{"pk":{"S":"p"},"sk":{"N":"0"}},
                "UpdateExpression":"SET revision = :v", "ExpressionAttributeValues":{":v":{"N":"7"}},
                "ReturnValues":"INVALID"
            }),
        ),
        (
            "PutItem",
            json!({
                "TableName":"synthetic-snapshot", "Item":{"pk":{"S":"p"},"sk":{"N":"0"},"revision":{"N":"7"}},
                "ReturnValues":"ALL_NEW"
            }),
        ),
    ] {
        assert_eq!(
            s.dispatch(operation, &serde_json::to_vec(&request).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
        assert_eq!(
            call(
                &s,
                "GetItem",
                json!({"TableName":"synthetic-snapshot","Key":{"pk":{"S":"p"},"sk":{"N":"0"}}})
            )
            .await["Item"]["revision"],
            json!({"N":"0"})
        );
    }
}

#[tokio::test]
async fn page_rejects_incompatible_parameters_segments_and_select_options() {
    let service = database(1).await;
    for (operation, options) in [
        ("Query", json!({"Segment":0,"TotalSegments":1})),
        (
            "Scan",
            json!({"KeyConditionExpression":"attribute_exists(pk)"}),
        ),
        ("Scan", json!({"ScanIndexForward":false})),
        ("Scan", json!({"Segment":1,"TotalSegments":1})),
        ("Scan", json!({"Segment":0,"TotalSegments":1_000_001})),
        ("Scan", json!({"Select":"INVALID"})),
        (
            "Scan",
            json!({"Select":"ALL_ATTRIBUTES","ProjectionExpression":"revision"}),
        ),
        ("Scan", json!({"Select":"SPECIFIC_ATTRIBUTES"})),
        ("Scan", json!({"Select":"ALL_PROJECTED_ATTRIBUTES"})),
    ] {
        let mut request = read_request(operation == "Query");
        request
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        assert_eq!(
            service
                .dispatch(operation, &serde_json::to_vec(&request).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException",
            "{operation}: {request}"
        );
    }
    let response = call(&service,"Scan",json!({"TableName":"synthetic-snapshot",
        "Segment":0,"TotalSegments":1,"ProjectionExpression":"revision","Select":"SPECIFIC_ATTRIBUTES"})).await;
    assert_eq!(response["Items"], json!([{"revision":{"N":"0"}}]));
    call(
        &service,
        "Scan",
        json!({"TableName":"synthetic-snapshot","Segment":0,"TotalSegments":1_000_000}),
    )
    .await;
}

#[tokio::test]
async fn page_segment_assignments_match_fixed_fnv_vectors_and_cursor_ownership() {
    // Independent FNV-1a64 vectors (fnv 1.0.7):
    // Sa=09462f07b5d5f4b9, Sb=09462c07b5d5efa0, Sc=09462d07b5d5f153,
    // N1=09207b07b5b5a6a0, N2=09207e07b5b5abb9,
    // B00=0912d407b5aa007f, Bff=0913c907b5aba0ce. Segment is hash modulo 7.
    for (kind, rows) in [
        (
            "S",
            vec![
                (json!({"S":"a"}), "a", 1),
                (json!({"S":"b"}), "b", 6),
                (json!({"S":"c"}), "c", 2),
            ],
        ),
        (
            "N",
            vec![
                (json!({"N":"1.00"}), "one", 1),
                (json!({"N":"2e0"}), "two", 3),
            ],
        ),
        (
            "B",
            vec![
                (json!({"B":"AA=="}), "zero", 6),
                (json!({"B":"/w=="}), "ff", 6),
            ],
        ),
    ] {
        let service = Service::default();
        call(
            &service,
            "CreateTable",
            json!({"TableName":"synthetic-segments","BillingMode":"PAY_PER_REQUEST",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":kind}]}),
        )
        .await;
        for (key, marker, _) in &rows {
            call(&service,"PutItem",json!({"TableName":"synthetic-segments","Item":{"pk":key,"fixture_id":{"S":marker}}})).await;
        }
        for segment in 0..7 {
            let response = call(
                &service,
                "Scan",
                json!({"TableName":"synthetic-segments","Segment":segment,"TotalSegments":7,
                "ProjectionExpression":"#id","ExpressionAttributeNames":{"#id":"fixture_id"}}),
            )
            .await;
            let actual: BTreeSet<_> = response["Items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["fixture_id"]["S"].as_str().unwrap())
                .collect();
            let expected: BTreeSet<_> = rows
                .iter()
                .filter(|(_, _, assigned)| *assigned == segment)
                .map(|(_, marker, _)| *marker)
                .collect();
            assert_eq!(actual, expected, "{kind} segment {segment}");
        }
        let request = json!({"TableName":"synthetic-segments","Segment":0,"TotalSegments":7,
            "ExclusiveStartKey":{"pk":rows[0].0}});
        assert_eq!(
            service
                .dispatch("Scan", &serde_json::to_vec(&request).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
    }
}

#[tokio::test]
async fn page_global_index_all_attributes_obeys_the_index_projection() {
    let service = Service::default();
    let mut request = table_request();
    request["GlobalSecondaryIndexes"] = json!(["ALL","KEYS_ONLY"].map(|projection|json!({
        "IndexName":format!("synthetic-{projection}"),"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "Projection":{"ProjectionType":projection}
    })));
    call(&service, "CreateTable", request).await;
    let item = json!({"pk":{"S":"p"},"sk":{"S":"s"},"payload":{"S":"synthetic"}});
    call(
        &service,
        "PutItem",
        json!({"TableName":"synthetic-schema","Item":item}),
    )
    .await;
    for operation in ["Query", "Scan"] {
        let mut request = json!({"TableName":"synthetic-schema","IndexName":"synthetic-ALL","Select":"ALL_ATTRIBUTES"});
        if operation == "Query" {
            request["KeyConditionExpression"] = json!("pk = :p");
            request["ExpressionAttributeValues"] = json!({":p":{"S":"p"}});
        }
        assert_eq!(
            call(&service, operation, request.clone()).await["Items"],
            json!([item])
        );
        request["IndexName"] = json!("synthetic-KEYS_ONLY");
        assert_eq!(
            service
                .dispatch(operation, &serde_json::to_vec(&request).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
    }
}

#[tokio::test]
async fn page_key_conditions_accept_maximum_keys_and_reject_unsupported_comparisons() {
    let service = Service::default();
    call(&service, "CreateTable", table_request()).await;
    let partition = "p".repeat(2048);
    let sort = "s".repeat(1024);
    let item = json!({"pk":{"S":partition},"sk":{"S":sort}});
    call(
        &service,
        "PutItem",
        json!({"TableName":"synthetic-schema","Item":item}),
    )
    .await;
    let mut request = json!({"TableName":"synthetic-schema","KeyConditionExpression":"pk = :p AND sk = :s",
        "ExpressionAttributeValues":{":p":{"S":partition},":s":{"S":sort}}});
    assert_eq!(
        call(&service, "Query", request.clone()).await["Items"],
        json!([item])
    );
    for (condition, value) in [
        ("pk = :p AND sk <> :s", "different".to_owned()),
        ("pk = :p AND sk = :s", "s".repeat(1025)),
    ] {
        request["KeyConditionExpression"] = json!(condition);
        request["ExpressionAttributeValues"][":s"] = json!({"S":value});
        assert_eq!(
            service
                .dispatch("Query", &serde_json::to_vec(&request).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
    }
}

#[tokio::test]
async fn transaction_options_validate_types_and_token_lengths_before_writing() {
    let service = database(1).await;
    let mut request = json!({"TransactItems":[{"Update":{
        "TableName":"synthetic-snapshot", "Key":{"pk":{"S":"p"},"sk":{"N":"0"}},
        "UpdateExpression":"SET revision = :v", "ExpressionAttributeValues":{":v":{"N":"1"}},
        "ReturnValuesOnConditionCheckFailure":"NONE"
    }}]});
    for value in [Value::Null, json!(true), json!(1), json!([])] {
        let mut invalid = request.clone();
        invalid["TransactItems"][0]["Update"]["ReturnValuesOnConditionCheckFailure"] = value;
        assert_eq!(
            service
                .dispatch("TransactWriteItems", &serde_json::to_vec(&invalid).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
    }
    for token in [String::new(), "t".repeat(37)] {
        let mut invalid = request.clone();
        invalid["ClientRequestToken"] = json!(token);
        assert_eq!(
            service
                .dispatch("TransactWriteItems", &serde_json::to_vec(&invalid).unwrap())
                .await
                .unwrap_err()
                .code,
            "ValidationException"
        );
    }
    let read = json!({"TableName":"synthetic-snapshot","Key":{"pk":{"S":"p"},"sk":{"N":"0"}}});
    assert_eq!(
        call(&service, "GetItem", read.clone()).await["Item"]["revision"],
        json!({"N":"0"})
    );
    request["ClientRequestToken"] = json!("t".repeat(36));
    call(&service, "TransactWriteItems", request).await;
    assert_eq!(
        call(&service, "GetItem", read).await["Item"]["revision"],
        json!({"N":"1"})
    );
}

#[tokio::test]
async fn transaction_four_mib_limits_are_inclusive_and_overflow_is_atomic() {
    let service = Service::default();
    call(&service, "CreateTable", table_request()).await;
    // Sixteen 256 KiB items exactly fill the transaction limit. Attribute names
    // take 11 bytes and key values 3 bytes, independently of JSON wire encoding.
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
    let payload = "x".repeat(256 * 1024 - 14);
    let mut puts = Vec::new();
    let mut gets = Vec::new();
    for number in 0..16 {
        let key = json!({"pk":{"S":"p"},"sk":{"S":format!("{number:02}")}});
        let mut item = key.clone();
        item["payload"] = json!({"S":payload});
        puts.push(json!({"Put":{"TableName":"synthetic-schema","Item":item}}));
        gets.push(json!({"Get":{"TableName":"synthetic-schema","Key":key}}));
    }
    call(
        &service,
        "TransactWriteItems",
        json!({"TransactItems":puts}),
    )
    .await;
    let response = call(&service, "TransactGetItems", json!({"TransactItems":gets})).await;
    assert_eq!(response["Responses"].as_array().unwrap().len(), 16);
    drop(response);
    puts[0]["Put"]["Item"]["payload"] = json!({"S":format!("{payload}x")});
    let error = service
        .dispatch(
            "TransactWriteItems",
            &serde_json::to_vec(&json!({"TransactItems":puts})).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "ValidationException");
    let stored = call(
        &service,
        "GetItem",
        json!({"TableName":"synthetic-schema","Key":{"pk":{"S":"p"},"sk":{"S":"00"}}}),
    )
    .await;
    assert!(stored["Item"]["payload"]["S"].as_str().unwrap() == payload);
    drop(stored);
    call(
        &service,
        "PutItem",
        json!({"TableName":"synthetic-schema","Item":{"pk":{"S":"p"},"sk":{"S":"16"}}}),
    )
    .await;
    gets.push(
        json!({"Get":{"TableName":"synthetic-schema","Key":{"pk":{"S":"p"},"sk":{"S":"16"}}}}),
    );
    assert_eq!(
        service
            .dispatch(
                "TransactGetItems",
                &serde_json::to_vec(&json!({"TransactItems":gets})).unwrap()
            )
            .await
            .unwrap_err()
            .code,
        "ValidationException"
    );
}

#[tokio::test]
async fn transaction_tokens_survive_unrelated_commits_and_expire_lazily() {
    let service = database(1).await;
    let token = "t".repeat(36);
    let original = json!({"ClientRequestToken":token,"TransactItems":[{"Update":{
        "TableName":"synthetic-snapshot", "Key":{"pk":{"S":"p"},"sk":{"N":"0"}},
        "UpdateExpression":"ADD revision :one", "ExpressionAttributeValues":{":one":{"N":"1.00"}}
    }}]});
    call(&service, "TransactWriteItems", original.clone()).await;
    call(&service,"TransactWriteItems",json!({"TransactItems":[{"Put":{
        "TableName":"synthetic-snapshot","Item":{"pk":{"S":"p"},"sk":{"N":"1"},"revision":{"N":"0"}}
    }}]})).await;
    let mut replay = original;
    replay["TransactItems"][0]["Update"]["ExpressionAttributeValues"][":one"] = json!({"N":"1e0"});
    call(&service, "TransactWriteItems", replay.clone()).await;
    let read = json!({"TableName":"synthetic-snapshot","Key":{"pk":{"S":"p"},"sk":{"N":"0"}}});
    assert_eq!(
        call(&service, "GetItem", read.clone()).await["Item"]["revision"],
        json!({"N":"1"})
    );
    // Age an existing record without sleeping or replacing the production clock.
    service
        .database
        .write()
        .await
        .tokens
        .get_mut(&token)
        .unwrap()
        .created = std::time::Instant::now() - Duration::from_secs(601);
    replay["TransactItems"][0]["Update"]["ExpressionAttributeValues"][":one"] = json!({"N":"2"});
    call(&service, "TransactWriteItems", replay).await;
    assert_eq!(
        call(&service, "GetItem", read).await["Item"]["revision"],
        json!({"N":"3"})
    );
}
