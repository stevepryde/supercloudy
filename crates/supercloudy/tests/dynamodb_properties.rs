use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, Region},
    types::{
        AttributeDefinition, AttributeValue as Av, BillingMode, ConditionCheck, Delete,
        DeleteRequest, Get, KeySchemaElement, KeyType, KeysAndAttributes, Put, PutRequest,
        ReturnValue, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TransactGetItem,
        TransactWriteItem, Update, WriteRequest,
    },
};
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed},
};
use std::collections::{BTreeMap, HashMap};
use tokio::{net::TcpListener, task::JoinHandle};

const TABLE: &str = "synthetic-properties";
type Key = (u8, u8);
type Item = HashMap<String, Av>;
type Model = BTreeMap<Key, i64>;

// Independent integer/map semantics are intentionally narrower than the emulator.
// Every step checks the complete stored table, including rejected writes.
#[derive(Clone, Debug)]
enum Guard {
    Always,
    Absent,
    Present,
    Equal(i64),
}
impl Guard {
    fn accepts(&self, old: Option<i64>) -> bool {
        match self {
            Self::Always => true,
            Self::Absent => old.is_none(),
            Self::Present => old.is_some(),
            Self::Equal(v) => old == Some(*v),
        }
    }
    fn wire(
        &self,
    ) -> (
        Option<String>,
        Option<HashMap<String, String>>,
        Option<Item>,
    ) {
        let expression = match self {
            Self::Always => return (None, None, None),
            Self::Absent => "attribute_not_exists(#v)",
            Self::Present => "attribute_exists(#v)",
            Self::Equal(_) => "#v = :want",
        };
        let values = if let Self::Equal(v) = self {
            Some(HashMap::from([(":want".into(), n(*v))]))
        } else {
            None
        };
        (
            Some(expression.into()),
            Some(HashMap::from([("#v".into(), "revision".into())])),
            values,
        )
    }
}
#[derive(Clone, Debug)]
struct Action {
    kind: u8,
    value: i64,
    guard: Guard,
}
#[derive(Clone, Debug)]
enum Op {
    Put(Key, i64, Guard),
    Get(Key),
    Update(Key, i64, Guard),
    Delete(Key, Guard),
    Query {
        partition: u8,
        low: u8,
        high: u8,
        forward: bool,
        limit: i32,
        threshold: i64,
    },
    Scan {
        threshold: i64,
        limit: i32,
    },
    Transaction(BTreeMap<Key, Action>),
    TransactGet(Vec<Key>),
    BatchWrite(BTreeMap<Key, Option<i64>>),
    BatchGet(Vec<Key>),
}
fn keys() -> impl Strategy<Value = Key> {
    (0u8..3, 0u8..5)
}
fn guards() -> impl Strategy<Value = Guard> {
    prop_oneof![
        Just(Guard::Always),
        Just(Guard::Absent),
        Just(Guard::Present),
        (-12i64..13).prop_map(Guard::Equal)
    ]
}
fn ops() -> impl Strategy<Value = Op> {
    prop_oneof![
        (keys(), -12i64..13, guards()).prop_map(|(k, v, g)| Op::Put(k, v, g)),
        keys().prop_map(Op::Get),
        (keys(), -4i64..5, guards()).prop_map(|(k, v, g)| Op::Update(k, v, g)),
        (keys(), guards()).prop_map(|(k, g)| Op::Delete(k, g)),
        (0u8..3, 0u8..5, 0u8..5, any::<bool>(), 1i32..5, -12i64..13).prop_map(
            |(partition, a, b, forward, limit, threshold)| Op::Query {
                partition,
                low: a.min(b),
                high: a.max(b),
                forward,
                limit,
                threshold
            }
        ),
        (-12i64..13, 1i32..5).prop_map(|(threshold, limit)| Op::Scan { threshold, limit }),
        prop::collection::btree_map(
            keys(),
            (0u8..4, -8i64..9, guards()).prop_map(|(kind, value, guard)| Action {
                kind,
                value,
                guard
            }),
            1..5
        )
        .prop_map(Op::Transaction),
        prop::collection::vec(keys(), 1..8).prop_map(Op::TransactGet),
        prop::collection::btree_map(keys(), prop::option::of(-12i64..13), 1..6)
            .prop_map(Op::BatchWrite),
        prop::collection::btree_set(keys(), 1..8)
            .prop_map(|s| Op::BatchGet(s.into_iter().collect())),
    ]
}
fn config() -> Config {
    let mut c = Config::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        c.cases = 32;
    }
    if std::env::var_os("PROPTEST_RNG_SEED").is_none() {
        c.rng_seed = RngSeed::Fixed(0xd1a0_0208);
    }
    c.failure_persistence = Some(Box::new(FileFailurePersistence::Direct(
        "tests/dynamodb_properties.proptest-regressions",
    )));
    c
}
#[track_caller]
fn success<T, E>(r: Result<T, E>) -> T {
    r.unwrap_or_else(|_| panic!("local DynamoDB request failed"))
}
fn n(v: i64) -> Av {
    Av::N(v.to_string())
}
fn key((p, s): Key) -> Item {
    HashMap::from([
        ("pk".into(), Av::S(format!("p{p}"))),
        ("sk".into(), n(i64::from(s))),
    ])
}
fn item(k: Key, v: i64) -> Item {
    let mut i = key(k);
    i.insert("revision".into(), n(v));
    i
}
fn model_items(model: &Model) -> Vec<Item> {
    model.iter().map(|(k, v)| item(*k, *v)).collect()
}
fn sorted(mut items: Vec<Item>) -> Vec<Item> {
    items.sort_by_key(|i| {
        (
            i["pk"].as_s().unwrap().clone(),
            i["sk"].as_n().unwrap().parse::<u8>().unwrap(),
        )
    });
    items
}
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
        let client = Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .behavior_version_latest()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
                .endpoint_url(endpoint)
                .build(),
        );
        let mut create = client
            .create_table()
            .table_name(TABLE)
            .billing_mode(BillingMode::PayPerRequest);
        for (name, tag, kind) in [
            ("pk", ScalarAttributeType::S, KeyType::Hash),
            ("sk", ScalarAttributeType::N, KeyType::Range),
        ] {
            create = create
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(name)
                        .attribute_type(tag)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(name)
                        .key_type(kind)
                        .build()
                        .unwrap(),
                );
        }
        success(create.send().await);
        Self { client, task }
    }
    async fn all(&self) -> Vec<Item> {
        success(self.client.scan().table_name(TABLE).send().await)
            .items
            .unwrap_or_default()
    }
}
fn update_values(values: Option<Item>, delta: i64) -> Item {
    let mut values = values.unwrap_or_default();
    values.insert(":zero".into(), n(0));
    values.insert(":delta".into(), n(delta));
    values
}
fn update_names(names: Option<HashMap<String, String>>) -> HashMap<String, String> {
    let mut names = names.unwrap_or_default();
    names.insert("#v".into(), "revision".into());
    names
}
fn tx_action(k: Key, a: &Action) -> TransactWriteItem {
    let (condition, names, values) = a.guard.wire();
    let b = TransactWriteItem::builder();
    match a.kind {
        0 => b.put(
            Put::builder()
                .table_name(TABLE)
                .set_item(Some(item(k, a.value)))
                .set_condition_expression(condition)
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                )
                .build()
                .unwrap(),
        ),
        1 => b.update(
            Update::builder()
                .table_name(TABLE)
                .set_key(Some(key(k)))
                .update_expression("SET #v = if_not_exists(#v, :zero) + :delta")
                .set_condition_expression(condition)
                .set_expression_attribute_names(Some(update_names(names)))
                .set_expression_attribute_values(Some(update_values(values, a.value)))
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                )
                .build()
                .unwrap(),
        ),
        2 => b.delete(
            Delete::builder()
                .table_name(TABLE)
                .set_key(Some(key(k)))
                .set_condition_expression(condition)
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                )
                .build()
                .unwrap(),
        ),
        _ => {
            // ConditionCheck always needs an expression; Always uses a tautology
            // as a request, while the independent model simply accepts it.
            b.condition_check(
                ConditionCheck::builder()
                    .table_name(TABLE)
                    .set_key(Some(key(k)))
                    .condition_expression(condition.unwrap_or_else(|| {
                        "attribute_exists(#v) OR attribute_not_exists(#v)".into()
                    }))
                    .set_expression_attribute_names(Some(update_names(names)))
                    .set_expression_attribute_values(values)
                    .return_values_on_condition_check_failure(
                        ReturnValuesOnConditionCheckFailure::AllOld,
                    )
                    .build()
                    .unwrap(),
            )
        }
    }
    .build()
}

async fn run(steps: Vec<Op>, initial: Model) {
    let e = Emulator::new().await;
    let c = &e.client;
    let mut model = initial;
    for (k, v) in &model {
        success(
            c.put_item()
                .table_name(TABLE)
                .set_item(Some(item(*k, *v)))
                .send()
                .await,
        );
    }
    for op in steps {
        match op {
            Op::Put(k, v, g) => {
                let old = model.get(&k).copied();
                let ok = g.accepts(old);
                let (cond, names, values) = g.wire();
                let r = c
                    .put_item()
                    .table_name(TABLE)
                    .set_item(Some(item(k, v)))
                    .set_condition_expression(cond)
                    .set_expression_attribute_names(names)
                    .set_expression_attribute_values(values)
                    .return_values(ReturnValue::AllOld)
                    .return_values_on_condition_check_failure(
                        ReturnValuesOnConditionCheckFailure::AllOld,
                    )
                    .send()
                    .await;
                if ok {
                    assert_eq!(success(r).attributes, old.map(|v| item(k, v)));
                    model.insert(k, v);
                } else {
                    let err = r.expect_err("condition must reject put");
                    let aws_sdk_dynamodb::operation::put_item::PutItemError::ConditionalCheckFailedException(f) = err.as_service_error().unwrap() else { panic!("wrong put error") };
                    assert_eq!(f.item, old.map(|v| item(k, v)));
                }
            }
            Op::Get(k) => {
                assert_eq!(
                    success(
                        c.get_item()
                            .table_name(TABLE)
                            .set_key(Some(key(k)))
                            .send()
                            .await
                    )
                    .item,
                    model.get(&k).map(|v| item(k, *v))
                );
            }
            Op::Update(k, delta, g) => {
                let old = model.get(&k).copied();
                let ok = g.accepts(old);
                let (cond, names, values) = g.wire();
                let r = c
                    .update_item()
                    .table_name(TABLE)
                    .set_key(Some(key(k)))
                    .update_expression("SET #v = if_not_exists(#v, :zero) + :delta")
                    .set_condition_expression(cond)
                    .set_expression_attribute_names(Some(update_names(names)))
                    .set_expression_attribute_values(Some(update_values(values, delta)))
                    .return_values(ReturnValue::AllNew)
                    .return_values_on_condition_check_failure(
                        ReturnValuesOnConditionCheckFailure::AllOld,
                    )
                    .send()
                    .await;
                if ok {
                    let v = old.unwrap_or(0) + delta;
                    assert_eq!(success(r).attributes, Some(item(k, v)));
                    model.insert(k, v);
                } else {
                    let err = r.expect_err("condition must reject update");
                    let aws_sdk_dynamodb::operation::update_item::UpdateItemError::ConditionalCheckFailedException(f) = err.as_service_error().unwrap() else { panic!("wrong update error") };
                    assert_eq!(f.item, old.map(|v| item(k, v)));
                }
            }
            Op::Delete(k, g) => {
                let old = model.get(&k).copied();
                let ok = g.accepts(old);
                let (cond, names, values) = g.wire();
                let r = c
                    .delete_item()
                    .table_name(TABLE)
                    .set_key(Some(key(k)))
                    .set_condition_expression(cond)
                    .set_expression_attribute_names(names)
                    .set_expression_attribute_values(values)
                    .return_values(ReturnValue::AllOld)
                    .return_values_on_condition_check_failure(
                        ReturnValuesOnConditionCheckFailure::AllOld,
                    )
                    .send()
                    .await;
                if ok {
                    assert_eq!(success(r).attributes, old.map(|v| item(k, v)));
                    model.remove(&k);
                } else {
                    let err = r.expect_err("condition must reject delete");
                    let aws_sdk_dynamodb::operation::delete_item::DeleteItemError::ConditionalCheckFailedException(f) = err.as_service_error().unwrap() else { panic!("wrong delete error") };
                    assert_eq!(f.item, old.map(|v| item(k, v)));
                }
            }
            Op::Query {
                partition,
                low,
                high,
                forward,
                limit,
                threshold,
            } => {
                let mut evaluated: Vec<_> = model
                    .iter()
                    .filter(|((p, s), _)| *p == partition && (low..=high).contains(s))
                    .map(|(k, v)| (*k, *v))
                    .collect();
                if !forward {
                    evaluated.reverse();
                }
                let mut cursor = None;
                let mut offset = 0;
                loop {
                    let r = success(
                        c.query()
                            .table_name(TABLE)
                            .key_condition_expression("pk = :p AND sk BETWEEN :lo AND :hi")
                            .expression_attribute_values(":p", Av::S(format!("p{partition}")))
                            .expression_attribute_values(":lo", n(i64::from(low)))
                            .expression_attribute_values(":hi", n(i64::from(high)))
                            .filter_expression("#v >= :min")
                            .expression_attribute_names("#v", "revision")
                            .expression_attribute_values(":min", n(threshold))
                            .scan_index_forward(forward)
                            .limit(limit)
                            .set_exclusive_start_key(cursor)
                            .send()
                            .await,
                    );
                    let page: Vec<_> = evaluated.iter().skip(offset).take(limit as usize).collect();
                    let want: Vec<_> = page
                        .iter()
                        .filter(|(_, v)| *v >= threshold)
                        .map(|(k, v)| item(*k, *v))
                        .collect();
                    assert_eq!(r.items.unwrap_or_default(), want);
                    assert_eq!(r.count, want.len() as i32);
                    assert_eq!(r.scanned_count, page.len() as i32);
                    offset += page.len();
                    let expected_cursor = if page.len() == limit as usize {
                        page.last().map(|(k, _)| key(*k))
                    } else {
                        None
                    };
                    assert_eq!(r.last_evaluated_key, expected_cursor);
                    cursor = r.last_evaluated_key;
                    if cursor.is_none() {
                        break;
                    }
                    assert!(offset <= evaluated.len(), "query failed to advance");
                }
            }
            Op::Scan { threshold, limit } => {
                let evaluated: Vec<_> = model.iter().collect();
                let mut offset = 0;
                let mut cursor = None;
                loop {
                    let r = success(
                        c.scan()
                            .table_name(TABLE)
                            .filter_expression("#v >= :min")
                            .projection_expression("pk, sk")
                            .expression_attribute_names("#v", "revision")
                            .expression_attribute_values(":min", n(threshold))
                            .limit(limit)
                            .set_exclusive_start_key(cursor)
                            .send()
                            .await,
                    );
                    let page: Vec<_> = evaluated.iter().skip(offset).take(limit as usize).collect();
                    let want: Vec<_> = page
                        .iter()
                        .filter(|(_, v)| **v >= threshold)
                        .map(|(k, _)| key(**k))
                        .collect();
                    assert_eq!(r.items.unwrap_or_default(), want);
                    assert_eq!(r.count, want.len() as i32);
                    assert_eq!(r.scanned_count, page.len() as i32);
                    offset += page.len();
                    let expected_cursor = if page.len() == limit as usize {
                        page.last().map(|(k, _)| key(**k))
                    } else {
                        None
                    };
                    assert_eq!(r.last_evaluated_key, expected_cursor);
                    cursor = r.last_evaluated_key;
                    if cursor.is_none() {
                        break;
                    }
                    assert!(offset <= evaluated.len(), "scan failed to advance");
                }
            }
            Op::Transaction(actions) => {
                let accepted: Vec<_> = actions
                    .iter()
                    .map(|(k, a)| a.guard.accepts(model.get(k).copied()))
                    .collect();
                let r = c
                    .transact_write_items()
                    .set_transact_items(Some(
                        actions.iter().map(|(k, a)| tx_action(*k, a)).collect(),
                    ))
                    .send()
                    .await;
                if accepted.iter().all(|v| *v) {
                    success(r);
                    for (k, a) in actions {
                        match a.kind {
                            0 => {
                                model.insert(k, a.value);
                            }
                            1 => {
                                let v = model.get(&k).copied().unwrap_or(0) + a.value;
                                model.insert(k, v);
                            }
                            2 => {
                                model.remove(&k);
                            }
                            _ => {}
                        }
                    }
                } else {
                    let err = r.expect_err("transaction must reject");
                    let aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(f)=err.as_service_error().unwrap() else {panic!("wrong transaction error")};
                    let codes: Vec<_> = accepted
                        .iter()
                        .map(|ok| {
                            if *ok {
                                "None"
                            } else {
                                "ConditionalCheckFailed"
                            }
                        })
                        .collect();
                    assert_eq!(f.message(),Some(format!("Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",codes.join(", ")).as_str()));
                    assert_eq!(f.cancellation_reasons().len(), actions.len());
                    for (((k, _), ok), reason) in
                        actions.iter().zip(accepted).zip(f.cancellation_reasons())
                    {
                        assert_eq!(
                            reason.code(),
                            Some(if ok { "None" } else { "ConditionalCheckFailed" })
                        );
                        assert_eq!(
                            reason.message(),
                            if ok {
                                None
                            } else {
                                Some("The conditional request failed")
                            }
                        );
                        assert_eq!(
                            reason.item,
                            if ok {
                                None
                            } else {
                                model.get(k).map(|v| item(*k, *v))
                            }
                        );
                    }
                }
            }
            Op::TransactGet(ks) => {
                let r = success(
                    c.transact_get_items()
                        .set_transact_items(Some(
                            ks.iter()
                                .map(|k| {
                                    TransactGetItem::builder()
                                        .get(
                                            Get::builder()
                                                .table_name(TABLE)
                                                .set_key(Some(key(*k)))
                                                .build()
                                                .unwrap(),
                                        )
                                        .build()
                                })
                                .collect(),
                        ))
                        .send()
                        .await,
                );
                assert_eq!(r.responses().len(), ks.len());
                for (k, response) in ks.iter().zip(r.responses()) {
                    assert_eq!(response.item, model.get(k).map(|v| item(*k, *v)));
                }
            }
            Op::BatchWrite(actions) => {
                let writes = actions
                    .iter()
                    .map(|(k, v)| match v {
                        Some(v) => WriteRequest::builder()
                            .put_request(
                                PutRequest::builder()
                                    .set_item(Some(item(*k, *v)))
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                        None => WriteRequest::builder()
                            .delete_request(
                                DeleteRequest::builder()
                                    .set_key(Some(key(*k)))
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    })
                    .collect();
                let r = success(
                    c.batch_write_item()
                        .request_items(TABLE, writes)
                        .send()
                        .await,
                );
                assert!(r.unprocessed_items().is_none_or(HashMap::is_empty));
                for (k, v) in actions {
                    if let Some(v) = v {
                        model.insert(k, v);
                    } else {
                        model.remove(&k);
                    }
                }
            }
            Op::BatchGet(ks) => {
                let r = success(
                    c.batch_get_item()
                        .request_items(
                            TABLE,
                            KeysAndAttributes::builder()
                                .set_keys(Some(ks.iter().map(|k| key(*k)).collect()))
                                .build()
                                .unwrap(),
                        )
                        .send()
                        .await,
                );
                assert!(r.unprocessed_keys().is_none_or(HashMap::is_empty));
                let got = r
                    .responses()
                    .and_then(|m| m.get(TABLE))
                    .cloned()
                    .unwrap_or_default();
                let want = ks
                    .iter()
                    .filter_map(|k| model.get(k).map(|v| item(*k, *v)))
                    .collect();
                assert_eq!(sorted(got), sorted(want));
            }
        }
        assert_eq!(
            sorted(e.all().await),
            model_items(&model),
            "stored state diverged"
        );
    }
}
proptest! {
    #![proptest_config(config())]
    #[test]
    fn dynamodb_sequences_match_reference(
        initial in prop::collection::btree_map(keys(), -12i64..13, 0..8),
        steps in prop::collection::vec(ops(), 1..35)
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(run(steps,initial));
    }
}
