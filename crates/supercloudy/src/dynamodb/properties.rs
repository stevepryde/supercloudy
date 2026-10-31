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
        });
    }
}
