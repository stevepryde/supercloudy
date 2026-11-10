use reqwest::{Client, Method};
use serde_json::{Value, json};

pub struct Http {
    pub base: String,
    client: Client,
}
impl Http {
    pub fn new(base: String) -> Self {
        Self {
            base,
            client: Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap(),
        }
    }
    pub async fn call(&self, method: Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base, path));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("loopback HTTP request");
        let status = response.status().as_u16();
        let body = response.json().await.expect("Qdrant JSON envelope");
        (status, body)
    }
    pub async fn success(&self, method: Method, path: &str, body: Option<Value>) -> Value {
        let (status, body) = self.call(method, path, body).await;
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(body["status"], "ok");
        body["result"].clone()
    }
}
fn normalize(mut value: Value) -> Value {
    match &mut value {
        Value::Array(batch) => {
            for result in batch {
                *result = normalize(result.take());
            }
        }
        Value::Object(result) => {
            if let Some(Value::Array(points)) = result.get_mut("points") {
                for point in points {
                    if let Some(point) = point.as_object_mut() {
                        point.remove("version");
                    }
                }
            }
        }
        _ => {}
    }
    value
}
/// Qdrant stores/scorers use f32. SIMD and scalar reductions can differ by a
/// few ulps, so only computed scores/vector components use 2e-5 tolerance.
/// IDs, payloads (including floats), counts and configuration compare exactly.
pub fn compare(actual: &Value, expected: &Value) {
    compare_value(actual, expected, false)
}
fn compare_value(actual: &Value, expected: &Value, computed: bool) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b))
            if (a.is_u64() && b.is_u64()) || (a.is_i64() && b.is_i64()) =>
        {
            assert_eq!(a, b)
        }
        (Value::Number(a), Value::Number(b)) if computed => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            assert!((a - b).abs() <= 2e-5_f64.max(b.abs() * 2e-5), "{a} != {b}");
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{actual} != {expected}");
            for (a, b) in a.iter().zip(b) {
                compare_value(a, b, computed);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(
                a.keys().collect::<std::collections::BTreeSet<_>>(),
                b.keys().collect::<std::collections::BTreeSet<_>>(),
                "{actual} != {expected}"
            );
            let point = a.contains_key("id");
            for (k, a) in a {
                if k == "payload" {
                    assert_eq!(a, &b[k]);
                } else {
                    compare_value(
                        a,
                        &b[k],
                        computed || (point && (k == "score" || k == "vector")),
                    );
                }
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
pub async fn scenario(h: &Http) -> Value {
    let mut evidence = Vec::new();
    let schema = json!({"vectors":{"semantic":{"size":3,"distance":"Cosine"}},"sparse_vectors":{"terms":{"modifier":"idf"}},"metadata":{"kind":"synthetic","keep":true,"remove":1,"nested":{"a":1,"b":2}}});
    assert_eq!(
        h.success(Method::PUT, "/collections/documents", Some(schema.clone()))
            .await,
        true
    );
    let info = h.success(Method::GET, "/collections/documents", None).await;
    assert_eq!(info["config"]["params"]["vectors"], schema["vectors"]);
    assert_eq!(
        info["config"]["params"]["sparse_vectors"],
        schema["sparse_vectors"]
    );
    assert_eq!(info["config"]["metadata"], schema["metadata"]);
    for (field, schema) in [
        ("tenant", json!({"type":"keyword","is_tenant":true})),
        ("revision", json!("integer")),
    ] {
        h.success(
            Method::PUT,
            "/collections/documents/index?wait=true",
            Some(json!({"field_name":field,"field_schema":schema})),
        )
        .await;
    }
    let points = json!({"points":[
    {"id":1,"vector":{"semantic":[3.0,0.0,0.0],"terms":{"indices":[7,11],"values":[1.0,2.0]}},"payload":{"tenant":"a","revision":1,"version":9007199254740993_u64,"tags":["red","shared"],"nested":{"rank":2},"active":true}},
    {"id":2,"vector":{"semantic":[4.0,3.0,0.0],"terms":{"indices":[7,19],"values":[3.0,1.0]}},"payload":{"tenant":"a","revision":2,"tags":["blue","shared"],"nested":{"rank":4},"active":false}},
    {"id":3,"vector":{"semantic":[0.0,1.0,0.0],"terms":{"indices":[19],"values":[2.0]}},"payload":{"tenant":"b","revision":3,"tags":["red"],"active":true}}
    ]});
    h.success(
        Method::PUT,
        "/collections/documents/points?wait=true",
        Some(points),
    )
    .await;
    let info = h.success(Method::GET, "/collections/documents", None).await;
    assert_eq!(info["points_count"], 3);
    evidence.push(info["payload_schema"].clone());
    evidence.push(info["config"]["params"].clone());
    let retrieved = h
        .success(
            Method::POST,
            "/collections/documents/points",
            Some(json!({"ids":[1,2,99],"with_payload":true,"with_vector":true})),
        )
        .await;
    assert_eq!(retrieved.as_array().unwrap().len(), 2);
    compare(
        &retrieved[0],
        &json!({"id":1,"vector":{"semantic":[1,0,0],"terms":{"indices":[7,11],"values":[1,2]}},"payload":{"tenant":"a","revision":1,"version":9007199254740993_u64,"tags":["red","shared"],"nested":{"rank":2},"active":true}}),
    );
    evidence.push(retrieved);
    let selected=h.success(Method::POST,"/collections/documents/points",Some(json!({"ids":[1],"with_payload":{"exclude":["tags","nested","active","version"]},"with_vector":["terms"]}))).await;
    assert_eq!(selected[0]["payload"], json!({"tenant":"a","revision":1}));
    assert!(selected[0]["vector"].get("semantic").is_none());
    evidence.push(selected);
    let filters = [
        json!({"must":[{"key":"tenant","match":{"value":"a"}},{"key":"revision","range":{"gte":2}}]}),
        json!({"must":{"key":"tags","match":{"any":["red","blue"]}},"must_not":[{"key":"tenant","match":{"value":"b"}}],"should":[{"key":"active","match":{"value":true}},{"key":"revision","range":{"gt":1}}]}),
        json!({"must":[{"key":"nested.rank","range":{"lt":3}}]}),
        json!({"should":[{"has_id":[1]},{"key":"tenant","match":{"value":"b"}}]}),
        json!({"must":[{"key":"tags","match":{"except":["shared","blue"]}}]}),
        json!({"min_should":{"conditions":[{"key":"tags","match":{"value":"red"}},{"key":"active","match":{"value":true}}],"min_count":2}}),
    ];
    for (filter, ids) in filters.into_iter().zip([
        vec![2],
        vec![1, 2],
        vec![1],
        vec![1, 3],
        vec![1, 3],
        vec![1, 3],
    ]) {
        let result=h.success(Method::POST,"/collections/documents/points/query",Some(json!({"query":[1,0,0],"using":"semantic","filter":filter,"with_payload":["tenant"],"with_vector":false}))).await;
        assert_eq!(
            result["points"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            ids
        );
        evidence.push(normalize(result));
    }
    let dense=h.success(Method::POST,"/collections/documents/points/query",Some(json!({"query":[1,0,0],"using":"semantic","limit":1,"offset":1,"score_threshold":0.5}))).await;
    assert_eq!(dense["points"][0]["id"], 2);
    compare_value(&dense["points"][0]["score"], &json!(0.8), true);
    evidence.push(normalize(dense));
    let sparse=h.success(Method::POST,"/collections/documents/points/query",Some(json!({"query":{"indices":[7,11],"values":[1,1]},"using":"terms","with_payload":true}))).await;
    assert_eq!(sparse["points"][0]["id"], 1);
    compare_value(
        &sparse["points"][0]["score"],
        &json!((4.0_f64 / 2.5).ln() + 2.0 * (4.0_f64 / 1.5).ln()),
        true,
    );
    evidence.push(normalize(sparse));
    let batch=h.success(Method::POST,"/collections/documents/points/query/batch",Some(json!({"searches":[{"query":[0,1,0],"using":"semantic","limit":2},{"query":{"indices":[19],"values":[1]},"using":"terms","filter":{"must":[{"key":"tenant","match":{"value":"a"}}]}}]}))).await;
    assert_eq!(batch[0]["points"][0]["id"], 3);
    assert_eq!(batch[1]["points"][0]["id"], 2);
    evidence.push(normalize(batch));
    // A bad vector later in an upsert must not publish its valid predecessor.
    let(status,error)=h.call(Method::PUT,"/collections/documents/points?wait=true",Some(json!({"points":[{"id":4,"vector":{"semantic":[1,0,0]}},{"id":5,"vector":{"semantic":[1,0]}}]}))).await;
    assert_eq!(status, 400);
    assert!(error["status"]["error"].is_string());
    evidence.push(json!({"dimension_error_status":status}));
    assert_eq!(
        h.success(
            Method::POST,
            "/collections/documents/points/count",
            Some(json!({"exact":true}))
        )
        .await["count"],
        3
    );
    h.success(
        Method::POST,
        "/collections/documents/points/delete?wait=true",
        Some(json!({"points":[2,99]})),
    )
    .await;
    let after = h
        .success(
            Method::POST,
            "/collections/documents/points/query",
            Some(json!({"query":{"indices":[7],"values":[1]},"using":"terms"})),
        )
        .await;
    compare_value(
        &after["points"][0]["score"],
        &json!((4.0_f64 / 2.5).ln()),
        true,
    );
    evidence.push(normalize(after));
    h.success(
        Method::POST,
        "/collections/documents/points/delete?wait=true",
        Some(json!({"filter":{"must":[{"key":"tenant","match":{"value":"a"}}]}})),
    )
    .await;
    evidence.push(
        h.success(
            Method::POST,
            "/collections/documents/points/scroll",
            Some(json!({"with_payload":true,"with_vector":true})),
        )
        .await,
    );
    h.success(
        Method::PATCH,
        "/collections/documents",
        Some(json!({"metadata":{"kind":"updated","remove":null,"nested":{"b":3}}})),
    )
    .await;
    assert_eq!(
        h.success(Method::GET, "/collections/documents", None).await["config"]["metadata"],
        json!({"kind":"updated","keep":true,"nested":{"b":3}})
    );
    h.success(
        Method::PATCH,
        "/collections/documents",
        Some(json!({"metadata":null})),
    )
    .await;
    evidence.push(
        h.success(Method::GET, "/collections/documents", None).await["config"]["metadata"].clone(),
    );
    let (status, missing) = h.call(Method::GET, "/collections/absent", None).await;
    assert_eq!(status, 404);
    assert!(missing["status"]["error"].is_string());
    evidence.push(json!({"missing_collection_status":status}));
    h.success(Method::DELETE, "/collections/documents", None)
        .await;
    assert_eq!(
        h.success(Method::GET, "/collections/documents/exists", None)
            .await["exists"],
        false
    );
    json!(evidence)
}
pub async fn bm25_scenario(h: &Http) -> Value {
    h.success(
        Method::PUT,
        "/collections/texts",
        Some(json!({"vectors":{},"sparse_vectors":{"lexical":{"modifier":"idf"}}})),
    )
    .await;
    let docs = [
        "The quick brown fox jumps over the lazy dog.",
        "Running runners run running! Rust services, databases.",
        "A fox runs quickly; café naïve 東京 １２３.",
        "the and or is a",
    ];
    let points=docs.into_iter().enumerate().map(|(i,text)|json!({"id":i+1,"vector":{"lexical":{"text":text,"model":"qdrant/bm25"}},"payload":{"position":i}})).collect::<Vec<_>>();
    h.success(
        Method::PUT,
        "/collections/texts/points?wait=true",
        Some(json!({"points":points})),
    )
    .await;
    let stored = h
        .success(
            Method::POST,
            "/collections/texts/points",
            Some(json!({"ids":[1,2,3,4],"with_vector":true})),
        )
        .await;
    assert_eq!(stored[3]["vector"]["lexical"]["indices"], json!([]));
    let mut evidence = vec![stored];
    for text in [
        "fox",
        "runs running",
        "the fox fox",
        "café 東京",
        "unseenword",
        "the and",
    ] {
        let result=h.success(Method::POST,"/collections/texts/points/query",Some(json!({"query":{"text":text,"model":"qdrant/bm25"},"using":"lexical","with_payload":true}))).await;
        if text == "runs running" {
            assert_eq!(result["points"][0]["id"], 2);
        }
        if text == "unseenword" || text == "the and" {
            assert!(result["points"].as_array().unwrap().is_empty());
        }
        evidence.push(normalize(result));
    }
    // Explicit options alter both embeddings and query processing.
    h.success(Method::PUT,"/collections/texts/points?wait=true",Some(json!({"points":[{"id":5,"vector":{"lexical":{"text":"repeat repeat zebra", "model":"qdrant/bm25", "options":{"k":1.8,"b":0.4,"avg_len":20}}}}]}))).await;
    evidence.push(
        h.success(
            Method::POST,
            "/collections/texts/points",
            Some(json!({"ids":[5],"with_vector":true})),
        )
        .await,
    );
    let query = h
        .success(
            Method::POST,
            "/collections/texts/points/query",
            Some(json!({"query":{"text":"repeat zebra","model":"qdrant/bm25"},"using":"lexical"})),
        )
        .await;
    assert_eq!(query["points"][0]["id"], 5);
    evidence.push(normalize(query));
    json!(evidence)
}

pub async fn auxiliary_scenario(h: &Http) -> Value {
    let mut evidence = Vec::new();
    h.success(
        Method::PUT,
        "/collections/aux",
        Some(json!({"vectors":{"size":2,"distance":"Dot"},"metadata":{"suite":"auxiliary"}})),
    )
    .await;
    let uuid = "A0B1C2D3-E4F5-4678-9ABC-DEF012345678";
    h.success(Method::PUT,"/collections/aux/points?wait=true",Some(json!({"points":[
 {"id":1,"vector":[2,0],"payload":{"nested":{"keep":1,"drop":2},"array":[{"name":"one","number":1},{"name":"two","number":2}],"list":[1,2],"null":null,"empty":[],"group":"red"}},
 {"id":2,"vector":[0,3],"payload":{"nested":{"keep":2},"list":[3],"group":"blue"}},
 {"id":3,"vector":{},"payload":{"group":"green"}},
 {"id":uuid,"vector":[1,1],"payload":{"group":"uuid"}}
 ]}))).await;
    let info = h.success(Method::GET, "/collections/aux", None).await;
    assert_eq!(info["points_count"], 4);
    let list = h.success(Method::GET, "/collections", None).await;
    assert!(
        list["collections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["name"] == "aux")
    );
    let p = h
        .success(
            Method::GET,
            "/collections/aux/points/A0B1C2D3E4F546789ABCDEF012345678",
            None,
        )
        .await;
    assert_eq!(p["id"], "a0b1c2d3-e4f5-4678-9abc-def012345678");
    evidence.push(p);
    for selector in [
        json!(["nested.keep", "array[].name"]),
        json!({"exclude":["nested.drop","array[].number","list","null","empty","group"]}),
    ] {
        evidence.push(
            h.success(
                Method::POST,
                "/collections/aux/points",
                Some(json!({"ids":[1],"with_payload":selector})),
            )
            .await,
        );
    }
    let default_retrieve = h
        .success(
            Method::POST,
            "/collections/aux/points",
            Some(json!({"ids":[1]})),
        )
        .await;
    assert!(default_retrieve[0].get("payload").is_none());
    assert!(default_retrieve[0].get("vector").is_none());
    evidence.push(default_retrieve);
    let queries = [
        json!({"query":[1,0],"with_vector":true,"params":{"exact":true,"hnsw_ef":20}}),
        json!({"query":1,"with_payload":true}),
        json!({"limit":2}),
    ];
    for query in queries {
        evidence.push(normalize(
            h.success(Method::POST, "/collections/aux/points/query", Some(query))
                .await,
        ));
    }
    let filters = [
        (json!({"must":[{"has_vector":""}]}), 3),
        (json!({"must":[{"is_empty":{"key":"empty"}}]}), 4),
        (json!({"must":[{"is_null":{"key":"null"}}]}), 1),
        (
            json!({"must":[{"key":"list","values_count":{"gt":1,"lte":2}}]}),
            1,
        ),
        (
            json!({"must":[{"should":[{"key":"group","match":{"value":"blue"}},{"has_id":[1]}]}],"must_not":{"has_id":[2]}}),
            1,
        ),
        (
            json!({"must":[{"key":"array[].number","range":{"gte":2}}]}),
            1,
        ),
    ];
    for (filter, count) in filters {
        let result = h
            .success(
                Method::POST,
                "/collections/aux/points/count",
                Some(json!({"filter":filter,"exact":true})),
            )
            .await;
        assert_eq!(result["count"], count);
        evidence.push(result);
    }
    let page = h
        .success(
            Method::POST,
            "/collections/aux/points/scroll",
            Some(json!({"limit":2})),
        )
        .await;
    assert_eq!(page["next_page_offset"], 3);
    evidence.push(page.clone());
    let next = h
        .success(
            Method::POST,
            "/collections/aux/points/scroll",
            Some(json!({"limit":2,"offset":page["next_page_offset"]})),
        )
        .await;
    assert!(next["next_page_offset"].is_null());
    evidence.push(next);
    h.success(
        Method::PUT,
        "/collections/aux/index?wait=true",
        Some(json!({"field_name":"group","field_schema":"keyword"})),
    )
    .await;
    h.success(
        Method::DELETE,
        "/collections/aux/index/group?wait=true",
        None,
    )
    .await;
    assert!(
        h.success(Method::GET, "/collections/aux", None).await["payload_schema"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    for (method, path, body) in [
        (Method::GET, "/collections/aux/points/999", None),
        (
            Method::POST,
            "/collections/aux/points",
            Some(json!({"ids":[1],"with_vector":["absent"]})),
        ),
        (Method::PUT, "/collections/aux", Some(json!({"vectors":{}}))),
    ] {
        let (status, error) = h.call(method, path, body).await;
        assert!(error["status"]["error"].is_string());
        evidence.push(json!({"status":status}));
    }
    // Point replacement replaces all vector/payload state and changes search.
    h.success(
        Method::PUT,
        "/collections/aux/points?wait=true",
        Some(json!({"points":[{"id":1,"vector":[0,4],"payload":{"replacement":true}}]})),
    )
    .await;
    let replaced = h
        .success(Method::GET, "/collections/aux/points/1", None)
        .await;
    assert_eq!(replaced["payload"], json!({"replacement":true}));
    evidence.push(replaced);
    evidence.push(h.success(Method::DELETE, "/collections/aux", None).await);
    evidence.push(h.success(Method::DELETE, "/collections/aux", None).await);
    json!(evidence)
}

pub async fn sparse_statistics_scenario(h: &Http) -> Value {
    h.success(Method::PUT,"/collections/statistics",Some(json!({"vectors":{"dense":{"size":1,"distance":"Dot"}},"sparse_vectors":{"idf":{"modifier":"idf"},"plain":{}}}))).await;
    let mut evidence = Vec::new();
    async fn put(h: &Http, id: u64, vector: Value) {
        h.success(
            Method::PUT,
            "/collections/statistics/points?wait=true",
            Some(json!({"points":[{"id":id,"vector":vector}]})),
        )
        .await;
    }
    async fn query(h: &Http) -> Value {
        normalize(
            h.success(
                Method::POST,
                "/collections/statistics/points/query",
                Some(json!({"query":{"indices":[1],"values":[1]},"using":"idf"})),
            )
            .await,
        )
    }
    put(
        h,
        1,
        json!({"idf":{"indices":[1],"values":[1]},"plain":{"indices":[5,2],"values":[-2,3]}}),
    )
    .await;
    evidence.push(query(h).await);
    put(h, 2, json!({"dense":[1]})).await;
    evidence.push(query(h).await);
    put(h, 3, json!({"idf":{"indices":[],"values":[]}})).await;
    evidence.push(query(h).await);
    put(h, 4, json!({"idf":{"indices":[1],"values":[0]}})).await;
    evidence.push(query(h).await);
    put(h, 5, json!({"idf":{"indices":[2],"values":[1]}})).await;
    evidence.push(query(h).await);
    h.success(
        Method::POST,
        "/collections/statistics/points/delete?wait=true",
        Some(json!({"points":[4,5]})),
    )
    .await;
    evidence.push(query(h).await);
    put(h, 1, json!({"idf":{"indices":[2],"values":[1]}})).await;
    assert!(query(h).await["points"].as_array().unwrap().is_empty());
    put(
        h,
        1,
        json!({"idf":{"indices":[1],"values":[1]},"plain":{"indices":[5,2],"values":[-2,3]}}),
    )
    .await;
    evidence.push(query(h).await);
    put(h, 2, json!({"idf":{"indices":[1],"values":[2]}})).await;
    evidence.push(query(h).await);
    put(h, 3, json!({"idf":{"indices":[1],"values":[3]}})).await;
    evidence.push(query(h).await);
    put(h, 5, json!({"idf":{"indices":[1],"values":[4]}})).await;
    evidence.push(query(h).await);
    for query in [
        json!({"query":{"indices":[2,5],"values":[1,1]},"using":"plain"}),
        json!({"query":{"indices":[5],"values":[1]},"using":"plain"}),
    ] {
        evidence.push(normalize(
            h.success(
                Method::POST,
                "/collections/statistics/points/query",
                Some(query),
            )
            .await,
        ));
    }
    evidence.push(
        h.success(
            Method::POST,
            "/collections/statistics/points",
            Some(json!({"ids":[1],"with_vector":["plain"]})),
        )
        .await,
    );
    let (status, error) = h
        .call(
            Method::PUT,
            "/collections/statistics/points?wait=true",
            Some(json!({"points":[{"id":10,"vector":{"plain":{"indices":[1,1],"values":[1,2]}}}]})),
        )
        .await;
    assert_eq!(status, 422);
    assert!(error["status"]["error"].is_string());
    evidence.push(json!({"duplicate_status":status}));
    h.success(Method::DELETE, "/collections/statistics", None)
        .await;
    json!(evidence)
}

pub async fn precision_scenario(h: &Http) -> Value {
    h.success(
        Method::PUT,
        "/collections/precision",
        Some(json!({"vectors":{"size":2,"distance":"Cosine"}})),
    )
    .await;
    h.success(Method::PUT,"/collections/precision/points?wait=true",Some(json!({"points":[
 {"id":9007199254740993_u64,"vector":[0.00001,0],"payload":{"version":7,"id":9007199254740993_u64,"score":0.123456789}},
 {"id":9007199254740994_u64,"vector":[0,0]},
 {"id":9007199254740995_u64,"vector":[1.0000001,0]}
 ]}))).await;
    let stored=h.success(Method::POST,"/collections/precision/points",Some(json!({"ids":[9007199254740993_u64,9007199254740994_u64,9007199254740995_u64],"with_payload":true,"with_vector":true}))).await;
    assert_eq!(stored[0]["id"], 9007199254740993_u64);
    assert_eq!(stored[0]["payload"]["version"], 7);
    // Qdrant deliberately leaves almost-zero and already-normalized vectors alone.
    assert!(stored[0]["vector"][0].as_f64().unwrap() < 0.0001);
    assert_eq!(stored[1]["vector"], json!([0.0, 0.0]));
    let query = normalize(
        h.success(
            Method::POST,
            "/collections/precision/points/query",
            Some(json!({"query":[1,0],"with_payload":true,"with_vector":true})),
        )
        .await,
    );
    assert_eq!(query["points"][0]["id"], 9007199254740995_u64);
    assert_eq!(query["points"][1]["payload"]["version"], 7);
    let by_id = normalize(
        h.success(
            Method::POST,
            "/collections/precision/points/query",
            Some(json!({"query":9007199254740993_u64})),
        )
        .await,
    );
    h.success(Method::DELETE, "/collections/precision", None)
        .await;
    json!([stored, query, by_id])
}

pub async fn preprocessing_scenario(h: &Http) -> Value {
    let variants = [
        (
            json!({"language":"none","tokenizer":"whitespace","lowercase":false,"stopwords":{"custom":["DROP"]}}),
            "The The DROP Running! running! A",
            "The Running!",
        ),
        (
            json!({"min_token_len":3,"max_token_len":5,"stopwords":{"languages":["english"],"custom":["zebra"]}}),
            "Running runners apples a THE zebra",
            "running zebra",
        ),
        (
            json!({"tokenizer":"prefix","min_token_len":2,"max_token_len":4}),
            "theory running runs",
            "the",
        ),
        (
            json!({"language":"none","stemmer":{"type":"snowball","language":"french"}}),
            "chevaux cheval",
            "chevaux",
        ),
    ];
    let mut evidence = Vec::new();
    for (i, (options, text, query)) in variants.into_iter().enumerate() {
        let path = format!("/collections/preprocessing_{i}");
        h.success(
            Method::PUT,
            &path,
            Some(json!({"sparse_vectors":{"lexical":{"modifier":"idf"}}})),
        )
        .await;
        h.success(Method::PUT,&format!("{path}/points?wait=true"),Some(json!({"points":[{"id":1,"vector":{"lexical":{"text":text,"model":"qdrant/bm25","options":options}}}]}))).await;
        evidence.push(
            h.success(
                Method::POST,
                &format!("{path}/points"),
                Some(json!({"ids":[1],"with_vector":true})),
            )
            .await,
        );
        let result=normalize(h.success(Method::POST,&format!("{path}/points/query"),Some(json!({"query":{"text":query,"model":"qdrant/bm25","options":options},"using":"lexical"}))).await);
        assert_eq!(result["points"][0]["id"], 1);
        evidence.push(result);
        h.success(Method::DELETE, &path, None).await;
    }
    json!(evidence)
}

pub async fn filter_boundary_scenario(h: &Http) -> Value {
    h.success(
        Method::PUT,
        "/collections/filter_boundaries",
        Some(json!({"vectors":{}})),
    )
    .await;
    let payloads = [
        json!({}),
        json!({"v":null}),
        json!({"v":[]}),
        json!({"v":[null]}),
        json!({"v":[null,null]}),
        json!({"v":[null,1]}),
        json!({"v":1}),
        json!({"x":1}),
        json!({"obj":[{"x":1}]}),
        json!({"obj":{"x":1}}),
    ];
    let points = payloads
        .into_iter()
        .enumerate()
        .map(|(i, payload)| json!({"id":10+i,"vector":{},"payload":payload}))
        .collect::<Vec<_>>();
    h.success(
        Method::PUT,
        "/collections/filter_boundaries/points?wait=true",
        Some(json!({"points":points})),
    )
    .await;
    let cases = [
        (
            json!({"must":[{"key":"obj.x","match":{"value":1}}]}),
            vec![19],
        ),
        (
            json!({"must":[{"key":"obj[].x","match":{"value":1}}]}),
            vec![18],
        ),
        (json!({"must":[{"is_null":{"key":"v"}}]}), vec![11]),
        (
            json!({"must":[{"is_empty":{"key":"v"}}]}),
            vec![10, 11, 12, 17, 18, 19],
        ),
        (
            json!({"must":[{"key":"v","values_count":{"gte":0}}]}),
            vec![11, 12, 13, 14, 15, 16],
        ),
        (
            json!({"must":[{"key":"v","values_count":{"gte":1}}]}),
            vec![13, 14, 15, 16],
        ),
        (
            json!({"must":[{"key":"v","values_count":{"gte":2}}]}),
            vec![14, 15],
        ),
        (
            json!({"should":[]}),
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        ),
    ];
    let mut evidence = Vec::new();
    for (filter, expected) in cases {
        let result = h
            .success(
                Method::POST,
                "/collections/filter_boundaries/points/scroll",
                Some(json!({"filter":filter,"with_payload":false})),
            )
            .await;
        assert_eq!(
            result["points"]
                .as_array()
                .unwrap()
                .iter()
                .map(|point| point["id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            expected,
            "filter: {filter}"
        );
        evidence.push(result);
    }
    h.success(
        Method::PUT,
        "/collections/filter_boundaries/points?wait=true",
        Some(json!({"points":[
            {"id":20,"vector":{},"payload":{"v":[null,"a"]}},
            {"id":21,"vector":{},"payload":{"v":true}},
            {"id":22,"vector":{},"payload":{"v":{}}},
            {"id":23,"vector":{},"payload":{"v":"a"}},
            {"id":24,"vector":{},"payload":{"v":"b"}}
        ]})),
    )
    .await;
    for (matcher, expected) in [
        (json!({"except":["a"]}), vec![15, 16, 21, 22, 24]),
        (json!({"except":[]}), vec![15, 16, 20, 21, 22, 23, 24]),
        (json!({"any":[]}), vec![]),
        (json!({"any":[1]}), vec![15, 16]),
        (json!({"except":[1]}), vec![20, 21, 22, 23, 24]),
    ] {
        let result=h.success(Method::POST,"/collections/filter_boundaries/points/scroll",Some(json!({"limit":100,"filter":{"must":[{"key":"v","match":matcher}]},"with_payload":false}))).await;
        assert_eq!(
            result["points"]
                .as_array()
                .unwrap()
                .iter()
                .map(|point| point["id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            expected,
            "match: {matcher}"
        );
        evidence.push(result);
    }
    for matcher in [json!({"any":["a",1]}), json!({"except":["a",1]})] {
        let (status, error) = h
            .call(
                Method::POST,
                "/collections/filter_boundaries/points/scroll",
                Some(json!({"filter":{"must":[{"key":"v","match":matcher}]}})),
            )
            .await;
        assert_eq!(status, 400);
        assert!(error["status"]["error"].is_string());
        evidence.push(json!({"mixed_match_status":status}));
    }
    h.success(Method::DELETE, "/collections/filter_boundaries", None)
        .await;
    json!(evidence)
}

pub async fn selector_boundary_scenario(h: &Http) -> Value {
    h.success(
        Method::PUT,
        "/collections/selector_boundaries",
        Some(json!({"vectors":{}})),
    )
    .await;
    let payload =
        json!({"a":1,"b":null,"c":{},"d":[{"x":1,"y":2},{"y":3},"scalar",null],"nested":{"x":1}});
    h.success(
        Method::PUT,
        "/collections/selector_boundaries/points?wait=true",
        Some(json!({"points":[{"id":1,"vector":{},"payload":payload}]})),
    )
    .await;
    let mut removed_x = payload.clone();
    removed_x["d"][0].as_object_mut().unwrap().remove("x");
    let mut empty_d = payload.clone();
    empty_d["d"] = json!([]);
    let cases = [
        (json!({"include":["a.x"]}), json!({"a":1})),
        (json!({"include":["b.x"]}), json!({"b":null})),
        (json!({"include":["c.x"]}), json!({"c":{}})),
        (json!({"include":["nested.no"]}), json!({"nested":{}})),
        (json!({"include":["d.x"]}), json!({"d":[]})),
        (
            json!({"include":["d[].x"]}),
            json!({"d":[{"x":1},{},"scalar",null]}),
        ),
        (json!({"include":["a[]"]}), json!({"a":1})),
        (json!({"exclude":["a.x"]}), payload.clone()),
        (json!({"exclude":["d.x"]}), payload.clone()),
        (json!({"exclude":["a[]"]}), payload.clone()),
        (json!({"exclude":["d[].x"]}), removed_x),
        (json!({"include":["d[]"]}), json!({"d":payload["d"]})),
        (json!({"exclude":["d[]"]}), empty_d),
    ];
    let mut evidence = Vec::new();
    for (selector, expected) in cases {
        let result = h
            .success(
                Method::POST,
                "/collections/selector_boundaries/points",
                Some(json!({"ids":[1],"with_payload":selector})),
            )
            .await;
        assert_eq!(result[0]["payload"], expected, "selector: {selector}");
        evidence.push(result);
    }
    h.success(
        Method::PUT,
        "/collections/selector_boundaries/points?wait=true",
        Some(json!({"points":[{"id":2,"vector":{}},{"id":3,"vector":{}}]})),
    )
    .await;
    let retrieved = h
        .success(
            Method::POST,
            "/collections/selector_boundaries/points",
            Some(json!({"ids":[2,2,3,1]})),
        )
        .await;
    assert_eq!(retrieved, json!([{"id":2},{"id":3},{"id":1}]));
    evidence.push(retrieved);
    h.success(Method::DELETE, "/collections/selector_boundaries", None)
        .await;
    json!(evidence)
}

pub async fn index_type_scenario(h: &Http) -> Value {
    h.success(
        Method::PUT,
        "/collections/index_types",
        Some(json!({"vectors":{}})),
    )
    .await;
    let uuid = "a0b1c2d3-e4f5-4678-9abc-def012345678";
    let date = "2024-01-02T03:04:05Z";
    let points = json!({"points":[
    {"id":1,"vector":{},"payload":{"keyword":"alpha","integer":1,"float":1.5,"bool":true,"uuid":uuid,"datetime":date}},
    {"id":2,"vector":{},"payload":{"keyword":2,"integer":"2","float":"2.5","bool":1,"uuid":"invalid","datetime":"invalid"}},
    {"id":3,"vector":{},"payload":{"keyword":[null,"beta","beta",1],"integer":[null,2,3,"two"],"float":[null,2,2.5],"bool":[null,false,true,"false"],"uuid":[null,uuid,uuid,"invalid"],"datetime":[null,date,date,"invalid"]}},
    {"id":4,"vector":{},"payload":{"keyword":[null,2,true],"integer":[null,"2",false],"float":[null,"2",false],"bool":[null,1,"true"],"uuid":[null,"invalid",1],"datetime":[null,"invalid",1]}},
    {"id":5,"vector":{},"payload":{"keyword":null,"integer":null,"float":null,"bool":null,"uuid":null,"datetime":null}},
    {"id":6,"vector":{},"payload":{"keyword":[],"integer":[],"float":[],"bool":[],"uuid":[],"datetime":[]}},
    {"id":7,"vector":{},"payload":{}}
    ]});
    h.success(
        Method::PUT,
        "/collections/index_types/points?wait=true",
        Some(points),
    )
    .await;
    for kind in ["keyword", "integer", "float", "bool", "uuid", "datetime"] {
        h.success(
            Method::PUT,
            "/collections/index_types/index?wait=true",
            Some(json!({"field_name":kind,"field_schema":kind})),
        )
        .await;
    }
    let indexes = h
        .success(Method::GET, "/collections/index_types", None)
        .await["payload_schema"]
        .clone();
    for kind in ["keyword", "integer", "float", "bool", "uuid", "datetime"] {
        assert_eq!(indexes[kind]["points"], 2, "index kind: {kind}");
    }
    h.success(Method::PUT,"/collections/index_types/points?wait=true",Some(json!({"points":[{"id":8,"vector":{},"payload":{"datetime":"2024-01-02"}},{"id":9,"vector":{},"payload":{"datetime":"2024-01-02 03:04"}},{"id":10,"vector":{},"payload":{"datetime":"2024-01-02T03:04:05+0900"}},{"id":11,"vector":{},"payload":{"datetime":"2024-02-30T00:00:00Z"}},{"id":12,"vector":{},"payload":{"datetime":"2024-01-02T00:00:00+aéx"}}]}))).await;
    let dates = h
        .success(Method::GET, "/collections/index_types", None)
        .await["payload_schema"]["datetime"]
        .clone();
    assert_eq!(dates["points"], 5);
    // These forms are accepted by the pinned server's numeric/date parsers.
    h.success(Method::PUT,"/collections/index_types/points?wait=true",Some(json!({"points":[
        {"id":13,"vector":{},"payload":{"datetime":"2024-01-02T03:04:60Z","integer":1.0}},
        {"id":14,"vector":{},"payload":{"datetime":"2024-1-2T03:04:05Z","integer":0.0}},
        {"id":15,"vector":{},"payload":{"datetime":"2024-01-02 03:04:05 UTC","integer":9223372036854775808u64}},
        {"id":16,"vector":{},"payload":{"integer":1.1}},
        {"id":17,"vector":{},"payload":{"integer":-1.1}},
        {"id":18,"vector":{},"payload":{"integer":1e30}},
        {"id":19,"vector":{},"payload":{"integer":-1e30}},
        {"id":20,"vector":{},"payload":{"integer":u64::MAX}}
    ]}))).await;
    let boundaries = h
        .success(Method::GET, "/collections/index_types", None)
        .await["payload_schema"]
        .clone();
    assert_eq!(boundaries["integer"]["points"], 5);
    assert_eq!(boundaries["datetime"]["points"], 8);
    h.success(
        Method::POST,
        "/collections/index_types/points/delete?wait=true",
        Some(json!({"points":[1]})),
    )
    .await;
    let deleted = h
        .success(Method::GET, "/collections/index_types", None)
        .await["payload_schema"]
        .clone();
    for kind in ["keyword", "float", "bool", "uuid"] {
        assert_eq!(deleted[kind]["points"], 1);
    }
    assert_eq!(deleted["integer"]["points"], 4);
    h.success(Method::DELETE, "/collections/index_types", None)
        .await;
    json!([indexes, dates, boundaries, deleted])
}
