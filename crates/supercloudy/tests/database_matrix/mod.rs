use bson::{Bson, Document, doc};

pub async fn mongo(client: &mongodb::Client) -> Vec<(String, Vec<Document>)> {
    let db = client.database("synthetic_matrix");
    db.drop().await.unwrap();
    let collection = db.collection::<Document>("values");
    collection.insert_many([
        doc! {"_id":1,"v":Bson::Null,"n":2,"arr":[1,2],"objects":[{"n":1,"keep":"a"},{"n":5,"keep":"b"}],"labels":["alpha"]},
        doc! {"_id":2,"v":0,"n":Bson::Int64(3),"arr":[3],"objects":[{"n":2}],"labels":["beta"]},
        doc! {"_id":3,"n":4.5,"arr":[],"objects":[],"labels":[]},
        doc! {"_id":4,"v":"alpha","n":Bson::Decimal128("2.0".parse().unwrap()),"arr":[Bson::Null,Bson::Int32(1)],"objects":[{"n":5}],"labels":["alpha","beta"]},
    ]).await.unwrap();
    let mut results = Vec::new();
    for (name, filter) in [
        ("null-or-missing", doc! {"v":Bson::Null}),
        ("not-null", doc! {"v":{"$ne":Bson::Null}}),
        ("missing", doc! {"v":{"$exists":false}}),
        (
            "array-element-range",
            doc! {"arr":{"$elemMatch":{"$gt":1,"$lt":3}}},
        ),
        ("array-all", doc! {"arr":{"$all":[1,2]}}),
        ("nested-array-path", doc! {"objects.n":{"$gte":5}}),
        (
            "negated-array-membership",
            doc! {"labels":{"$not":{"$in":["alpha"]}}},
        ),
        ("cross-numeric-equality", doc! {"$expr":{"$eq":["$n",2]}}),
        ("regex", doc! {"v":{"$regex":"^ALP","$options":"i"}}),
        (
            "regex-array",
            doc! {"labels":bson::Regex{pattern:"^alp".into(),options:String::new()}},
        ),
        (
            "regex-membership-array",
            doc! {"labels":{"$in":[bson::Regex{pattern:"^alp".into(),options:String::new()}]}},
        ),
        ("array-numeric-type", doc! {"arr":{"$type":"number"}}),
    ] {
        let mut cursor = collection
            .find(filter)
            .sort(doc! {"_id":1})
            .projection(doc! {"_id":1})
            .await
            .unwrap();
        let mut rows = Vec::new();
        while cursor.advance().await.unwrap() {
            rows.push(cursor.deserialize_current().unwrap());
        }
        results.push((name.into(), rows));
    }
    for (name, pipeline) in [
        (
            "nested-array-projection",
            vec![doc! {"$project":{"objects.n":1}}],
        ),
        (
            "missing-expression",
            vec![doc! {"$project":{"copy":"$v","kind":{"$type":"$v"},"numberKind":{"$type":"$n"}}}],
        ),
        (
            "numeric-promotion",
            vec![doc! {"$project":{"value":{"$add":["$n",1]}}}],
        ),
        (
            "date-conversions",
            vec![doc! {"$project":{
                "parsed":{"$dateFromString":{"dateString":"2025-02-03T04:05:06.789Z"}},
                "offsetParsed":{"$dateFromString":{"dateString":"2025-02-03 09:35:06","format":"%Y-%m-%d %H:%M:%S","timezone":"+05:30"}},
                "formatted":{"$dateToString":{"date":bson::DateTime::from_millis(1_738_555_506_789),"timezone":"-05:00"}},
                "integer":{"$toInt":"$n"}
            }}],
        ),
        ("count-type", vec![doc! {"$count":"n"}]),
        (
            "count-accumulator-type",
            vec![doc! {"$group":{"_id":Bson::Null,"n":{"$count":{}}}}],
        ),
        (
            "expression-spectrum",
            vec![
                doc! {"$match":{"_id":1}},
                doc! {"$project":{
                    "comparison":{"$cmp":["$n",3]},
                    "logic":{"$not":[{"$or":[{"$gt":["$n",5]},{"$lt":["$n",0]}]}]},
                    "choice":{"$switch":{"branches":[{"case":{"$eq":["$n",2]},"then":"two"}],"default":"other"}},
                    "arithmetic":{"$mod":[{"$subtract":[{"$multiply":["$n",5]},1]},4]},
                    "quotient":{"$divide":["$n",4]},
                    "text":{"$concat":["value-",{"$toString":"$n"}]},
                    "uniqueSize":{"$size":{"$setUnion":["$arr",[2,3]]}},
                    "bound":{"$let":{"vars":{"n":{"$add":["$n",3]}},"in":{"$multiply":["$$n",2]}}},
                    "merged":{"$mergeObjects":[{"a":1},{"a":2,"b":3}]},
                    "isArray":{"$isArray":"$arr"},
                    "member":{"$in":[2,"$arr"]},
                    "element":{"$arrayElemAt":["$arr",-1]},
                    "first":{"$first":"$arr"},"last":{"$last":"$arr"}
                }},
            ],
        ),
        (
            "unwind-preserved-index",
            vec![
                doc! {"$unwind":{"path":"$v","preserveNullAndEmptyArrays":true,"includeArrayIndex":"idx"}},
                doc! {"$project":{"v":1,"idx":1}},
            ],
        ),
    ] {
        let mut pipeline = pipeline;
        pipeline.push(doc! {"$sort":{"_id":1}});
        let mut cursor = collection.aggregate(pipeline).await.unwrap();
        let mut rows = Vec::new();
        while cursor.advance().await.unwrap() {
            rows.push(cursor.deserialize_current().unwrap());
        }
        results.push((name.into(), rows));
    }
    collection
        .update_many(doc! {}, doc! {"$inc":{"n":1}})
        .await
        .unwrap();
    collection
        .update_one(doc! {"_id":1}, doc! {"$inc":{"n":Bson::Int64(1)}})
        .await
        .unwrap();
    let mut cursor = collection
        .find(doc! {})
        .sort(doc! {"_id":1})
        .projection(doc! {"n":1})
        .await
        .unwrap();
    let mut rows = Vec::new();
    while cursor.advance().await.unwrap() {
        rows.push(cursor.deserialize_current().unwrap());
    }
    results.push(("stored-numeric-promotion".into(), rows));
    let invalid = collection
        .find(doc! {"arr":{"$type":"invalid-type-alias"}})
        .await
        .unwrap_err();
    assert!(
        matches!(*invalid.kind, mongodb::error::ErrorKind::Command(ref error) if error.code==2)
    );

    let overflow = db.collection::<Document>("overflow");
    overflow
        .insert_one(doc! {"_id":1,"n":i64::MAX})
        .await
        .unwrap();
    for update in [
        doc! {"$set":{"touched":true},"$inc":{"n":1i64}},
        doc! {"$set":{"touched":true},"$mul":{"n":2}},
    ] {
        let error = overflow
            .update_one(doc! {"_id":1}, update)
            .await
            .unwrap_err();
        assert!(
            matches!(*error.kind,mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(ref error)) if error.code==2)
        );
        let stored = overflow.find_one(doc! {"_id":1}).await.unwrap().unwrap();
        assert_eq!(
            stored,
            doc! {"_id":1,"n":i64::MAX},
            "failed numeric modifier must leave the whole document unchanged"
        );
    }
    db.drop().await.unwrap();
    results
}
