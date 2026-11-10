#![allow(dead_code)]
use bson::{Bson, Document, doc};
use mongodb::Client;

pub async fn client(uri: &str) -> Client {
    let mut options = mongodb::options::ClientOptions::parse(uri).await.unwrap();
    options.server_selection_timeout = Some(std::time::Duration::from_secs(10));
    Client::with_options(options).unwrap()
}
pub async fn rows(client: &Client, collection: &str) -> Vec<Document> {
    let collection = client
        .database("conformance")
        .collection::<Document>(collection);
    let mut cursor = collection
        .find(doc! {})
        .sort(doc! {"_id":1})
        .batch_size(1)
        .await
        .unwrap();
    let mut docs = vec![];
    while cursor.advance().await.unwrap() {
        docs.push(cursor.deserialize_current().unwrap());
    }
    docs
}
pub async fn command(client: &Client, command: Document) -> Document {
    client
        .database("conformance")
        .run_command(command)
        .await
        .unwrap()
}
pub fn error_code(error: mongodb::error::Error) -> i32 {
    match *error.kind {
        mongodb::error::ErrorKind::Command(e) => e.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(e)) => e.code,
        mongodb::error::ErrorKind::InsertMany(e) => e.write_errors.unwrap()[0].code,
        _ => panic!("unexpected error: {error}"),
    }
}

pub async fn crud(client: &Client) -> Document {
    command(client,doc!{"insert":"items","documents":[{"_id":1,"team":"a","score":2,"tags":["x"],"nested":{"n":1}},{"_id":2,"team":"a","score":4,"optional":null},{"_id":3,"team":"b","score":8}]}).await;
    let result=command(client,doc!{"update":"items","updates":[{"q":{"_id":1},"u":{"$inc":{"score":3},"$set":{"nested.n":9},"$push":{"tags":{"$each":["y","z"],"$position":0}}}}]}).await;
    assert_eq!(result.get_i32("nModified").unwrap(), 1);
    let projection=command(client,doc!{"find":"items","filter":{"score":{"$gte":4}},"sort":{"score":-1},"projection":{"_id":0,"team":1,"nested.n":1},"singleBatch":true}).await;
    let count = command(client, doc! {"count":"items","query":{"optional":null}}).await;
    let distinct = command(client, doc! {"distinct":"items","key":"team"}).await;
    let before=command(client,doc!{"findAndModify":"items","query":{"team":"a"},"sort":{"score":-1},"update":{"$inc":{"score":1}},"new":false}).await;
    command(client,doc!{"update":"items","updates":[{"q":{"_id":4},"u":[{"$set":{"score":{"$add":[{"$ifNull":["$score",0]},7]},"team":"c"}}],"upsert":true}]}).await;
    command(
        client,
        doc! {"delete":"items","deletes":[{"q":{"_id":2},"limit":1}]},
    )
    .await;
    doc! {"rows":rows(client,"items").await,"projection":projection.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone(),"nullCount":count.get("n").unwrap().as_i64().or_else(||count.get("n").unwrap().as_i32().map(i64::from)).unwrap(),"distinct":distinct.get_array("values").unwrap().clone(),"before":before.get_document("value").unwrap().clone()}
}

pub async fn indexes(client: &Client) -> Document {
    command(client,doc!{"createIndexes":"indexed","indexes":[{"key":{"tenant":1,"email":1},"name":"tenant_email","unique":true},{"key":{"external":1},"name":"external","unique":true,"sparse":true},{"key":{"activeKey":1},"name":"active","unique":true,"partialFilterExpression":{"active":true}}]}).await;
    command(client,doc!{"insert":"indexed","documents":[{"_id":1,"tenant":"a","email":"x","active":true,"activeKey":"same"},{"_id":2,"tenant":"b","email":"x","active":false,"activeKey":"same"}]}).await;
    let duplicate = client
        .database("conformance")
        .collection::<Document>("indexed")
        .insert_one(doc! {"_id":3,"tenant":"a","email":"x"})
        .await
        .unwrap_err();
    assert_eq!(error_code(duplicate), 11000);
    let failed=command(client,doc!{"update":"indexed","updates":[{"q":{"_id":2},"u":{"$set":{"active":true,"other":"must-not-appear"}}}]}).await;
    assert_eq!(
        failed.get_array("writeErrors").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("code")
            .unwrap(),
        11000
    );
    let snapshot = rows(client, "indexed").await;
    command(
        client,
        doc! {"dropIndexes":"indexed","index":"tenant_email"},
    )
    .await;
    command(
        client,
        doc! {"insert":"indexed","documents":[{"_id":3,"tenant":"a","email":"x"}]},
    )
    .await;
    doc! {"beforeDrop":snapshot,"afterDrop":rows(client,"indexed").await}
}

pub async fn aggregation(client: &Client) -> Document {
    command(client,doc!{"insert":"sales","documents":[{"_id":1,"team":"a","amount":2,"values":[1,2,3],"obj":{"x":1,"y":2}},{"_id":2,"team":"a","amount":4,"values":[4]},{"_id":3,"team":"b","amount":9,"values":[]}]}).await;
    command(client,doc!{"insert":"teams","documents":[{"_id":"a","label":"Alpha"},{"_id":"b","label":"Beta"}]}).await;
    let grouped=command(client,doc!{"aggregate":"sales","pipeline":[{"$group":{"_id":"$team","total":{"$sum":"$amount"},"average":{"$avg":"$amount"},"first":{"$first":"$amount"},"last":{"$last":"$amount"}}},{"$sort":{"_id":1}},{"$lookup":{"from":"teams","let":{"team":"$_id"},"pipeline":[{"$match":{"$expr":{"$eq":["$_id","$$team"]}}}],"as":"teamInfo"}}],"cursor":{}}).await;
    let expressions=command(client,doc!{"aggregate":"sales","pipeline":[{"$match":{"_id":1}},{"$project":{"_id":0,"mapped":{"$map":{"input":"$values","as":"v","in":{"$multiply":["$$v",2]}}},"filtered":{"$filter":{"input":"$values","as":"v","cond":{"$gt":["$$v",1]}}},"roundtrip":{"$arrayToObject":{"$objectToArray":"$obj"}},"joined":{"$concatArrays":["$values",[4]]},"branch":{"$cond":[{"$eq":["$team","a"]},"yes","no"]},"literal":{"$literal":"$team"},"missing":"$missing","missingType":{"$type":"$missing"},"missingArray":["$missing"]}}],"cursor":{}}).await;
    let union=command(client,doc!{"aggregate":"sales","pipeline":[{"$match":{"_id":1}},{"$project":{"_id":0,"name":"$team"}},{"$unionWith":{"coll":"teams","pipeline":[{"$match":{"_id":"b"}},{"$replaceRoot":{"newRoot":{"name":"$label"}}}]}},{"$sort":{"name":1}}],"cursor":{}}).await;
    doc! {"grouped":grouped.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone(),"expressions":expressions.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone(),"union":union.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone()}
}

pub async fn transactions(client: &Client) -> Document {
    let coll = client
        .database("conformance")
        .collection::<Document>("accounts");
    coll.insert_many([doc! {"_id":1,"balance":10}, doc! {"_id":2,"balance":20}])
        .await
        .unwrap();
    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    coll.update_one(doc! {"_id":1}, doc! {"$inc":{"balance":-3}})
        .session(&mut session)
        .await
        .unwrap();
    coll.update_one(doc! {"_id":2}, doc! {"$inc":{"balance":3}})
        .session(&mut session)
        .await
        .unwrap();
    assert_eq!(
        rows(client, "accounts").await[0]
            .get_i32("balance")
            .unwrap(),
        10
    );
    session.commit_transaction().await.unwrap();
    session.start_transaction().await.unwrap();
    coll.delete_one(doc! {"_id":2})
        .session(&mut session)
        .await
        .unwrap();
    session.abort_transaction().await.unwrap();
    session.start_transaction().await.unwrap();
    let snap = coll
        .find_one(doc! {"_id":1})
        .session(&mut session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snap.get_i32("balance").unwrap(), 7);
    coll.update_one(doc! {"_id":1}, doc! {"$inc":{"balance":1}})
        .await
        .unwrap();
    let repeated = coll
        .find_one(doc! {"_id":1})
        .session(&mut session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated.get_i32("balance").unwrap(), 7);
    let conflict = coll
        .update_one(doc! {"_id":1}, doc! {"$inc":{"balance":2}})
        .session(&mut session)
        .await;
    let conflict = conflict.unwrap_err();
    assert!(conflict.contains_label("TransientTransactionError"));
    assert_eq!(error_code(conflict), 112);
    let _ = session.abort_transaction().await;
    session.start_transaction().await.unwrap();
    coll.update_one(doc! {"_id":1}, doc! {"$inc":{"balance":100}})
        .session(&mut session)
        .await
        .unwrap();
    let duplicate = coll
        .insert_one(doc! {"_id":2,"balance":0})
        .session(&mut session)
        .await
        .unwrap_err();
    assert_eq!(error_code(duplicate), 11000);
    assert_eq!(
        error_code(session.commit_transaction().await.unwrap_err()),
        251
    );
    doc! {"accounts":rows(client,"accounts").await}
}

pub async fn concurrency(client: &Client) -> Document {
    let coll = client
        .database("conformance")
        .collection::<Document>("counter");
    coll.insert_one(doc! {"_id":1,"n":0}).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let coll = coll.clone();
        tasks.spawn(async move {
            coll.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
                .await
                .unwrap();
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap()
    }
    let mut tasks = tokio::task::JoinSet::new();
    for winner in 0..8 {
        let coll = coll.clone();
        tasks.spawn(async move {
            coll.update_one(
                doc! {"_id":1,"n":20},
                doc! {"$set":{"n":21,"winner":winner}},
            )
            .await
            .unwrap()
            .modified_count
        });
    }
    let mut winners = 0;
    while let Some(result) = tasks.join_next().await {
        winners += result.unwrap();
    }
    assert_eq!(winners, 1);
    let doc = coll.find_one(doc! {"_id":1}).await.unwrap().unwrap();
    assert_eq!(doc.get_i32("n").unwrap(), 21);
    doc! {"n":doc.get("n").unwrap().clone(),"winners":winners as i64}
}

pub fn canonical(value: &Bson) -> Bson {
    match value {
        Bson::Int32(n) => Bson::Int64(i64::from(*n)),
        Bson::Array(values) => Bson::Array(values.iter().map(canonical).collect()),
        Bson::Document(d) => {
            Bson::Document(d.iter().map(|(k, v)| (k.clone(), canonical(v))).collect())
        }
        other => other.clone(),
    }
}

pub async fn query_semantics(client: &Client) -> Document {
    command(client,doc!{"insert":"queries","documents":[{"_id":1,"n":2,"optional":null,"arr":[1,2],"children":[{"x":1,"y":5},{"x":2,"y":6}],"text":"Alpha"},{"_id":2,"n":2i64,"arr":[3],"text":"beta"},{"_id":3,"n":2.5,"optional":1,"arr":[],"text":"ALPINE"},{"_id":4,"n":"2","optional":false,"text":"other"}]}).await;
    let filters = [
        doc! {"n":2.0},
        doc! {"n":{"$gte":2,"$lt":3}},
        doc! {"optional":null},
        doc! {"optional":{"$exists":false}},
        doc! {"optional":{"$ne":null}},
        doc! {"arr":{"$all":[1,2]}},
        doc! {"arr":{"$size":0}},
        doc! {"children":{"$elemMatch":{"x":2,"y":{"$gt":5}}}},
        doc! {"children.x":2},
        doc! {"text":{"$regex":"^alp","$options":"i"}},
        doc! {"$or":[{"_id":1},{"_id":4}]},
        doc! {"$nor":[{"_id":1},{"_id":4}]},
        doc! {"$expr":{"$and":[{"$lt":["$n",3]},{"$gt":["$n",2]}]}},
        doc! {"n":{"$in":[2,"2"]}},
        doc! {"arr":{"$not":{"$size":1}}},
    ];
    let mut result = vec![];
    for filter in filters {
        let response=command(client,doc!{"find":"queries","filter":filter,"sort":{"_id":1},"projection":{"_id":1},"singleBatch":true}).await;
        result.push(Bson::Array(
            response
                .get_document("cursor")
                .unwrap()
                .get_array("firstBatch")
                .unwrap()
                .clone(),
        ));
    }
    let projection=command(client,doc!{"find":"queries","filter":{"_id":1},"projection":{"_id":0,"children.x":1},"singleBatch":true}).await;
    doc! {"filters":result,"projection":projection.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone()}
}

pub async fn update_semantics(client: &Client) -> Document {
    command(client,doc!{"insert":"updates","documents":[{"_id":1,"n":2,"arr":[3,1],"old":"v","obsolete":true}]}).await;
    for update in [
        doc! {"$mul":{"n":3},"$min":{"low":5},"$max":{"high":9},"$rename":{"old":"renamed"},"$unset":{"obsolete":""}},
        doc! {"$push":{"arr":{"$each":[4,2],"$sort":1,"$slice":3}}},
        doc! {"$addToSet":{"arr":{"$each":[2,5]}}},
        doc! {"$pull":{"arr":{"$gt":3}}},
        doc! {"$pop":{"arr":-1}},
    ] {
        command(
            client,
            doc! {"update":"updates","updates":[{"q":{"_id":1},"u":update}]},
        )
        .await;
    }
    let upsert = doc! {"q":{"_id":2,"nested.x":4},"u":{"$inc":{"n":1},"$setOnInsert":{"created":true}},"upsert":true};
    command(client, doc! {"update":"updates","updates":[upsert.clone()]}).await;
    command(client, doc! {"update":"updates","updates":[upsert]}).await;
    let coll = client
        .database("conformance")
        .collection::<Document>("updates");
    for (update, expected) in [
        (doc! {"$set":{"_id":99}}, 66),
        (doc! {"$set":{"a":1,"a.b":2}}, 40),
    ] {
        let err = coll.update_one(doc! {"_id":1}, update).await.unwrap_err();
        assert_eq!(error_code(err), expected);
    }
    let before = rows(client, "updates").await;
    // Both servers must reject the malformed projection before publishing this write.
    assert!(client.database("conformance").run_command(doc!{"findAndModify":"updates","query":{"_id":1},"update":{"$inc":{"n":100}},"fields":{"n":1,"arr":0}}).await.is_err());
    assert_eq!(rows(client, "updates").await, before);
    doc! {"rows":before}
}

pub async fn metadata_and_cursors(client: &Client) -> Document {
    let db = client.database("conformance");
    db.create_collection("catalog").await.unwrap();
    command(
        client,
        doc! {"insert":"catalog","documents":[{"_id":1},{"_id":2},{"_id":3}]},
    )
    .await;
    let first = command(client, doc! {"find":"catalog","batchSize":0}).await;
    let cursor = first.get_document("cursor").unwrap();
    assert!(cursor.get_array("firstBatch").unwrap().is_empty());
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    let next = command(
        client,
        doc! {"getMore":id,"collection":"catalog","batchSize":1},
    )
    .await;
    let cursor = next.get_document("cursor").unwrap();
    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 1);
    let id = cursor.get_i64("id").unwrap();
    let killed = command(client, doc! {"killCursors":"catalog","cursors":[id]}).await;
    assert_eq!(killed.get_array("cursorsKilled").unwrap().len(), 1);
    let error = db
        .run_command(doc! {"getMore":id,"collection":"catalog"})
        .await
        .unwrap_err();
    assert_eq!(error_code(error), 43);
    let indexes = command(client, doc! {"listIndexes":"catalog"}).await;
    let names = db.list_collection_names().await.unwrap();
    db.collection::<Document>("catalog").drop().await.unwrap();
    assert!(db.list_collection_names().await.unwrap().is_empty());
    doc! {"names":names,"indexes":indexes.get_document("cursor").unwrap().get_array("firstBatch").unwrap().clone()}
}

pub async fn ttl(client: &Client) -> Document {
    command(client,doc!{"createIndexes":"expiry","indexes":[{"key":{"at":1},"name":"expires","expireAfterSeconds":0i64,"partialFilterExpression":{"enabled":true}}]}).await;
    let future = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() + 3_600_000);
    command(client,doc!{"insert":"expiry","documents":[{"_id":1,"enabled":true,"at":bson::DateTime::from_millis(1)},{"_id":2,"enabled":false,"at":bson::DateTime::from_millis(1)},{"_id":3,"enabled":true,"at":future},{"_id":4,"enabled":true,"at":[future,bson::DateTime::from_millis(1)]},{"_id":5,"enabled":true,"at":"not a date"}]}).await;
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if client
                .database("conformance")
                .collection::<Document>("expiry")
                .count_documents(doc! {})
                .await
                .unwrap()
                == 3
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("eligible TTL rows must eventually expire");
    let docs = rows(client, "expiry").await;
    assert_eq!(
        docs.iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect::<Vec<_>>(),
        vec![2, 3, 5]
    );
    // Expiry is an external write: a stale transaction cannot resurrect the row.
    command(client,doc!{"createIndexes":"expiry_transaction","indexes":[{"key":{"at":1},"name":"expires","expireAfterSeconds":0}]}).await;
    let coll = client
        .database("conformance")
        .collection::<Document>("expiry_transaction");
    coll.insert_one(doc! {"_id":1,"v":0,"at":bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis()+1500)})
        .await.unwrap();
    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    assert!(
        coll.find_one(doc! {"_id":1})
            .session(&mut session)
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while coll.count_documents(doc! {}).await.unwrap() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("TTL must delete the snapshot row");
    let error = coll
        .update_one(doc! {"_id":1}, doc! {"$set":{"v":1}})
        .session(&mut session)
        .await
        .unwrap_err();
    assert!(error.contains_label("TransientTransactionError"));
    assert_eq!(error_code(error), 112);
    let _ = session.abort_transaction().await;
    assert!(coll.find_one(doc! {"_id":1}).await.unwrap().is_none());
    // Future clock values vary across servers; compare identities and exact stored types.
    doc! {"remaining":docs.iter().map(|d|d.get("_id").unwrap().clone()).collect::<Vec<_>>()}
}

pub async fn disjoint_transactions(client: &Client) -> Document {
    let coll = client
        .database("conformance")
        .collection::<Document>("disjoint");
    coll.insert_many([doc! {"_id":1,"n":0}, doc! {"_id":2,"n":0}])
        .await
        .unwrap();
    let mut first = client.start_session().await.unwrap();
    let mut second = client.start_session().await.unwrap();
    first.start_transaction().await.unwrap();
    second.start_transaction().await.unwrap();
    coll.find_one(doc! {"_id":1})
        .session(&mut first)
        .await
        .unwrap();
    coll.find_one(doc! {"_id":2})
        .session(&mut second)
        .await
        .unwrap();
    coll.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
        .session(&mut first)
        .await
        .unwrap();
    coll.update_one(doc! {"_id":2}, doc! {"$inc":{"n":2}})
        .session(&mut second)
        .await
        .unwrap();
    first.commit_transaction().await.unwrap();
    second.commit_transaction().await.unwrap();
    doc! {"rows":rows(client,"disjoint").await}
}

pub async fn large_documents(client: &Client) -> Document {
    let coll = client
        .database("conformance")
        .collection::<Document>("large");
    let payload = "x".repeat(9 * 1024 * 1024);
    coll.insert_many((0..3).map(|id| doc! {"_id":id,"value":payload.clone()}))
        .await
        .unwrap();
    let first = command(
        client,
        doc! {"find":"large","batchSize":100,"sort":{"_id":1}},
    )
    .await;
    let cursor = first.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        1,
        "16 MiB cursor batches must split 9 MiB documents"
    );
    let mut docs = first_batch.clone();
    let mut id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    while id != 0 {
        let next = command(
            client,
            doc! {"getMore":id,"collection":"large","batchSize":100},
        )
        .await;
        let cursor = next.get_document("cursor").unwrap();
        id = cursor.get_i64("id").unwrap();
        docs.extend(cursor.get_array("nextBatch").unwrap().clone());
    }
    let ids: Vec<_> = docs
        .iter()
        .map(|d| {
            let d = d.as_document().unwrap();
            assert_eq!(d.get_str("value").unwrap().len(), payload.len());
            d.get_i32("_id").unwrap()
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2]);
    doc! {"firstBatchLength":first_batch.len() as i64,"ids":ids}
}

pub async fn retryable_writes(client: &Client) -> Document {
    let mut session = client.start_session().await.unwrap();
    let db = client.database("conformance");
    let insert = doc! {"insert":"retry","documents":[{"_id":1,"n":0}],"txnNumber":41i64};
    for _ in 0..2 {
        let result = db
            .run_command(insert.clone())
            .session(&mut session)
            .await
            .unwrap();
        assert_eq!(result.get_i32("n").unwrap(), 1);
        assert!(!result.contains_key("writeErrors"));
    }
    let update =
        doc! {"update":"retry","updates":[{"q":{"_id":1},"u":{"$inc":{"n":1}}}],"txnNumber":42i64};
    for _ in 0..2 {
        let result = db
            .run_command(update.clone())
            .session(&mut session)
            .await
            .unwrap();
        assert_eq!(result.get_i32("n").unwrap(), 1);
    }
    let stale=db.run_command(doc!{"update":"retry","updates":[{"q":{"_id":1},"u":{"$inc":{"n":10}}}],"txnNumber":41i64}).session(&mut session).await.unwrap_err();
    assert_eq!(error_code(stale), 225);
    let docs = rows(client, "retry").await;
    assert_eq!(docs[0].get_i32("n").unwrap(), 1);
    for (ids, documents) in [
        (vec![0], vec![doc! {"_id":2,"n":0}]),
        (vec![1], vec![doc! {"_id":3,"n":0}]),
        (vec![0, 2], vec![doc! {"_id":2,"n":0}, doc! {"_id":4,"n":0}]),
    ] {
        let result = db
            .run_command(
                doc! {"insert":"retry","documents":documents,"stmtIds":ids,"txnNumber":43i64},
            )
            .session(&mut session)
            .await
            .unwrap();
        assert!(!result.contains_key("writeErrors"));
    }
    for (ids, targets) in [(vec![0, 1], vec![1, 2]), (vec![1, 2], vec![2, 3])] {
        let updates: Vec<_> = targets
            .into_iter()
            .map(|id| doc! {"q":{"_id":id},"u":{"$inc":{"n":1}}})
            .collect();
        let result = db
            .run_command(doc! {"update":"retry","updates":updates,"stmtIds":ids,"txnNumber":44i64})
            .session(&mut session)
            .await
            .unwrap();
        assert_eq!(result.get_i32("n").unwrap(), 2);
    }
    for (ids, targets) in [(vec![0], vec![4]), (vec![0, 1], vec![4, 3])] {
        let expected = ids.len() as i32;
        let deletes: Vec<_> = targets
            .into_iter()
            .map(|id| doc! {"q":{"_id":id},"limit":1})
            .collect();
        let result = db
            .run_command(doc! {"delete":"retry","deletes":deletes,"stmtIds":ids,"txnNumber":45i64})
            .session(&mut session)
            .await
            .unwrap();
        assert_eq!(result.get_i32("n").unwrap(), expected);
    }
    let docs = rows(client, "retry").await;
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].get_i32("n").unwrap(), 2);
    assert_eq!(docs[1].get_i32("n").unwrap(), 1);
    doc! {"rows":docs}
}

pub async fn transaction_aba_conflicts(client: &Client) -> Document {
    let coll = client.database("conformance").collection::<Document>("aba");
    coll.insert_many((1..=5).map(|id| doc! {"_id":id,"v":0}))
        .await
        .unwrap();
    for id in 1..=5 {
        let mut session = client.start_session().await.unwrap();
        session.start_transaction().await.unwrap();
        let snapshot = coll
            .find_one(doc! {"_id":id})
            .session(&mut session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.get_i32("v").unwrap(), 0);
        match id {
            1 => {
                coll.update_one(doc! {"_id":id}, doc! {"$set":{"v":1}})
                    .await
                    .unwrap();
                coll.update_one(doc! {"_id":id}, doc! {"$set":{"v":0}})
                    .await
                    .unwrap();
            }
            2 => {
                coll.delete_one(doc! {"_id":id}).await.unwrap();
                coll.insert_one(doc! {"_id":id,"v":0}).await.unwrap();
            }
            3 => {
                let response=command(client,doc!{"update":"aba","updates":[{"q":{"_id":id},"u":{"$set":{"v":1}}},{"q":{"_id":id},"u":{"$set":{"v":0}}}]}).await;
                assert_eq!(response.get_i32("nModified").unwrap(), 2);
            }
            4 => {
                coll.update_one(doc! {"_id":id}, doc! {"$set":{"v":1}})
                    .await
                    .unwrap();
            }
            _ => {
                let noop = coll
                    .update_one(doc! {"_id":id}, doc! {"$set":{"v":0}})
                    .await
                    .unwrap();
                assert_eq!(noop.modified_count, 0);
            }
        }
        let write = coll
            .update_one(doc! {"_id":id}, doc! {"$set":{"v":if id==4{0}else{2}}})
            .session(&mut session)
            .await;
        if id == 4 || id == 5 {
            let result = write.unwrap();
            assert_eq!(result.modified_count, if id == 4 { 0 } else { 1 });
            session.commit_transaction().await.unwrap();
        } else {
            let error = write.unwrap_err();
            assert!(error.contains_label("TransientTransactionError"));
            assert_eq!(error_code(error), 112);
            let _ = session.abort_transaction().await;
        }
    }
    doc! {"rows":rows(client,"aba").await}
}

pub async fn cursor_ownership(client: &Client) -> Document {
    let db = client.database("conformance");
    let coll = db.collection::<Document>("owned_cursors");
    coll.insert_many([doc! {"_id":1}, doc! {"_id":2}, doc! {"_id":3}])
        .await
        .unwrap();
    let mut session = client.start_session().await.unwrap();
    let mut other = client.start_session().await.unwrap();
    let mut codes = vec![];
    let first = db
        .run_command(doc! {"find":"owned_cursors","batchSize":1,"sort":{"_id":1}})
        .session(&mut session)
        .await
        .unwrap();
    let id = first.get_document("cursor").unwrap().get_i64("id").unwrap();
    assert_ne!(id, 0);
    let wrong = db
        .run_command(doc! {"getMore":id,"collection":"owned_cursors","batchSize":1})
        .session(&mut other)
        .await
        .unwrap_err();
    codes.push(error_code(wrong));
    let next = db
        .run_command(doc! {"getMore":id,"collection":"owned_cursors","batchSize":1})
        .session(&mut session)
        .await
        .unwrap();
    assert_eq!(
        next.get_document("cursor")
            .unwrap()
            .get_array("nextBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("_id")
            .unwrap(),
        2
    );
    for action in ["commit", "abort", "failed-write"] {
        session.start_transaction().await.unwrap();
        let first = db
            .run_command(doc! {"find":"owned_cursors","batchSize":1})
            .session(&mut session)
            .await
            .unwrap();
        let id = first.get_document("cursor").unwrap().get_i64("id").unwrap();
        assert_ne!(id, 0);
        match action {
            "commit" => session.commit_transaction().await.unwrap(),
            "abort" => session.abort_transaction().await.unwrap(),
            _ => {
                let duplicate = coll
                    .insert_one(doc! {"_id":1})
                    .session(&mut session)
                    .await
                    .unwrap_err();
                assert_eq!(error_code(duplicate), 11000);
                assert_eq!(
                    error_code(session.commit_transaction().await.unwrap_err()),
                    251
                );
            }
        }
        let gone = db
            .run_command(doc! {"getMore":id,"collection":"owned_cursors"})
            .session(&mut session)
            .await
            .unwrap_err();
        codes.push(error_code(gone));
    }
    // A terminal transaction number cannot reopen its retired cursor state.
    let mut raw_session = client.start_session().await.unwrap();
    let raw_find = doc! {"find":"owned_cursors","batchSize":1,"txnNumber":100i64,"autocommit":false,"startTransaction":true};
    let first = db
        .run_command(raw_find.clone())
        .session(&mut raw_session)
        .await
        .unwrap();
    let id = first.get_document("cursor").unwrap().get_i64("id").unwrap();
    client
        .database("admin")
        .run_command(doc! {"abortTransaction":1,"txnNumber":100i64,"autocommit":false})
        .session(&mut raw_session)
        .await
        .unwrap();
    let restart = db
        .run_command(raw_find)
        .session(&mut raw_session)
        .await
        .unwrap_err();
    assert_eq!(error_code(restart), 117);
    let gone = db
        .run_command(
            doc! {"getMore":id,"collection":"owned_cursors","txnNumber":100i64,"autocommit":false},
        )
        .session(&mut raw_session)
        .await
        .unwrap_err();
    assert_eq!(error_code(gone), 251);
    doc! {"errorCodes":codes,"rows":rows(client,"owned_cursors").await}
}
