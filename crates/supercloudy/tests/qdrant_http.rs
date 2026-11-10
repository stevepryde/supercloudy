#[path = "qdrant_scenarios/mod.rs"]
mod scenarios;
use reqwest::Method;
use serde_json::json;
use supercloudy::qdrant;

#[tokio::test]
async fn stateful_http_contract() {
    let server = qdrant::start(0).await.unwrap();
    let h = scenarios::Http::new(format!("http://{}", server.address()));
    scenarios::scenario(&h).await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn local_text_inference() {
    let server = qdrant::start(0).await.unwrap();
    scenarios::bm25_scenario(&scenarios::Http::new(format!(
        "http://{}",
        server.address()
    )))
    .await;
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn isolation_replacement_pagination_and_rejection() {
    let first = qdrant::start(0).await.unwrap();
    let second = qdrant::start(0).await.unwrap();
    let h = scenarios::Http::new(format!("http://{}", first.address()));
    let other = scenarios::Http::new(format!("http://{}", second.address()));
    h.success(
        Method::PUT,
        "/collections/items",
        Some(json!({"vectors":{"size":2,"distance":"Cosine"}})),
    )
    .await;
    assert_eq!(
        other
            .success(Method::GET, "/collections/items/exists", None)
            .await["exists"],
        false
    );
    let (status, error) = h
        .call(
            Method::PUT,
            "/collections/items",
            Some(json!({"vectors":{}})),
        )
        .await;
    assert_eq!(status, 409);
    assert!(error["status"]["error"].is_string());
    h.success(Method::PUT,"/collections/items/points?wait=true",Some(json!({"points":[{"id":1,"vector":[1,0],"payload":{"old":true}},{"id":2,"vector":[0,1]},{"id":3,"vector":[-1,0]}]}))).await;
    h.success(
        Method::PUT,
        "/collections/items/points?wait=true",
        Some(json!({"points":[{"id":1,"vector":[0,4],"payload":{"new":true}}]})),
    )
    .await;
    let p = h
        .success(Method::GET, "/collections/items/points/1", None)
        .await;
    assert_eq!(p["payload"], json!({"new":true}));
    assert_eq!(p["vector"], json!([0.0, 1.0]));
    let page = h
        .success(
            Method::POST,
            "/collections/items/points/scroll",
            Some(json!({"limit":2})),
        )
        .await;
    assert_eq!(page["next_page_offset"], 3);
    let next = h
        .success(
            Method::POST,
            "/collections/items/points/scroll",
            Some(json!({"offset":page["next_page_offset"],"limit":2})),
        )
        .await;
    assert_eq!(next["points"][0]["id"], 3);
    assert!(next["next_page_offset"].is_null());
    for (path, body) in [
        (
            "/collections/items/points/query",
            json!({"query":[1,0],"using":"unknown"}),
        ),
        (
            "/collections/items/points/query",
            json!({"query":[1,0],"prefetch":[]}),
        ),
        (
            "/collections/items/points/query",
            json!({"filter":{"must":[{"key":"new","match":{"text":"unsupported"}}]}}),
        ),
        (
            "/collections/items/points",
            json!({"points":[{"id":"invalid","vector":[1,0]}]}),
        ),
    ] {
        let (status, error) = h
            .call(
                if path.ends_with("/points") {
                    Method::PUT
                } else {
                    Method::POST
                },
                path,
                Some(body),
            )
            .await;
        assert_eq!(status, 400, "{error}");
        assert!(error["status"]["error"].is_string());
    }
    let (status, error) = h
        .call(
            Method::PUT,
            "/collections/unsupported",
            Some(json!({"vectors":{},"shard_number":2})),
        )
        .await;
    assert_eq!(status, 400);
    assert!(error["status"]["error"].is_string());
    assert_eq!(
        h.success(Method::GET, "/collections/unsupported/exists", None)
            .await["exists"],
        false
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}
#[tokio::test]
async fn concurrent_writes_and_owned_shutdown() {
    let server = qdrant::start(0).await.unwrap();
    let address = server.address();
    let h = scenarios::Http::new(format!("http://{address}"));
    h.success(
        Method::PUT,
        "/collections/items",
        Some(json!({"vectors":{"size":2,"distance":"Cosine"}})),
    )
    .await;
    let mut tasks = tokio::task::JoinSet::new();
    for id in 0..24 {
        let base = h.base.clone();
        tasks.spawn(async move {
            scenarios::Http::new(base)
                .success(
                    Method::PUT,
                    "/collections/items/points?wait=true",
                    Some(json!({"points":[{"id":id,"vector":[1,0]}]})),
                )
                .await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        h.success(
            Method::POST,
            "/collections/items/points/count",
            Some(json!({"exact":true}))
        )
        .await["count"],
        24
    );
    server.shutdown().await.unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    let replacement = qdrant::start(address.port()).await.unwrap();
    drop(replacement);
    tokio::task::yield_now().await;
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn auxiliary_endpoints_and_sparse_statistics() {
    let server = qdrant::start(0).await.unwrap();
    let h = scenarios::Http::new(format!("http://{}", server.address()));
    scenarios::auxiliary_scenario(&h).await;
    scenarios::sparse_statistics_scenario(&h).await;
    scenarios::precision_scenario(&h).await;
    scenarios::preprocessing_scenario(&h).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_requests_stop_with_server_owner() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for graceful in [true, false] {
        let server = qdrant::start(0).await.unwrap();
        let address = server.address();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /collections HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut received = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let n =
                tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buffer))
                    .await
                    .expect("loopback HTTP response")
                    .unwrap();
            assert!(n > 0);
            received.extend_from_slice(&buffer[..n]);
            if received.windows(11).any(|w| w == b"\"time\":0.0}") {
                break;
            }
        }
        stream.write_all(b"POST /collections/items/points/query HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").await.unwrap();
        if graceful {
            tokio::time::timeout(std::time::Duration::from_secs(2), server.shutdown())
                .await
                .expect("shutdown must cancel partial requests")
                .unwrap();
        } else {
            drop(server);
        }
        let mut tail = Vec::new();
        let closed = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read_to_end(&mut tail),
        )
        .await
        .expect("owned connection must close");
        if let Err(error) = closed {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
}

#[tokio::test]
async fn filter_array_null_missing_and_empty_should_boundaries() {
    let server = qdrant::start(0).await.unwrap();
    let h = scenarios::Http::new(format!("http://{}", server.address()));
    scenarios::filter_boundary_scenario(&h).await;
    scenarios::selector_boundary_scenario(&h).await;
    scenarios::index_type_scenario(&h).await;
    server.shutdown().await.unwrap();
}
