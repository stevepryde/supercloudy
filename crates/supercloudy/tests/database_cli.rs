//! Exercise the published process boundary, including all three listeners and
//! SIGTERM. Service suites separately cover database behavior.
#![cfg(unix)]

use bson::doc;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    process::Command,
    time::timeout,
};

#[tokio::test]
async fn cli_serves_all_protocols_and_releases_ports_on_sigterm() {
    timeout(Duration::from_secs(20), async {
        let mut child = Command::new(env!("CARGO_BIN_EXE_supercloudy"))
            .args([
                "up",
                "--port",
                "0",
                "--mongodb-port",
                "0",
                "--qdrant-port",
                "0",
                "--bucket",
                "synthetic-database-cli",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let http = lines
            .next_line()
            .await
            .unwrap()
            .unwrap()
            .strip_prefix("Listening on ")
            .unwrap()
            .to_owned();
        let mongo = lines
            .next_line()
            .await
            .unwrap()
            .unwrap()
            .strip_prefix("MongoDB on ")
            .unwrap()
            .to_owned();
        let qdrant = lines
            .next_line()
            .await
            .unwrap()
            .unwrap()
            .strip_prefix("Qdrant on ")
            .unwrap()
            .to_owned();

        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let object = format!("{http}/synthetic-database-cli/record");
        assert!(
            client
                .put(&object)
                .body("stored")
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        assert_eq!(
            client
                .get(&object)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "stored"
        );

        let database = mongodb::Client::with_uri_str(&mongo).await.unwrap();
        database
            .database("cli")
            .collection::<bson::Document>("records")
            .insert_one(doc! {"_id":1,"value":"stored"})
            .await
            .unwrap();
        assert_eq!(
            database
                .database("cli")
                .collection::<bson::Document>("records")
                .find_one(doc! {"_id":1})
                .await
                .unwrap()
                .unwrap()
                .get_str("value")
                .unwrap(),
            "stored"
        );

        let collection = format!("{qdrant}/collections/synthetic-cli");
        let response = client
            .put(&collection)
            .json(&serde_json::json!({"vectors":{"size":2,"distance":"Cosine"}}))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        assert!(
            client
                .get(&collection)
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        database.shutdown().await;

        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().unwrap().to_string()])
                .status()
                .await
                .unwrap()
                .success()
        );
        assert!(child.wait().await.unwrap().success());
        for endpoint in [
            http.strip_prefix("http://").unwrap(),
            mongo
                .strip_prefix("mongodb://")
                .unwrap()
                .split('/')
                .next()
                .unwrap(),
            qdrant.strip_prefix("http://").unwrap(),
        ] {
            assert!(
                TcpStream::connect(endpoint).await.is_err(),
                "listener survived shutdown: {endpoint}"
            );
        }
    })
    .await
    .expect("CLI did not complete database requests and shut down");
}

#[tokio::test]
async fn cli_bind_failure_exits_without_announcing_readiness() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = timeout(
        Duration::from_secs(10),
        Command::new(env!("CARGO_BIN_EXE_supercloudy"))
            .args([
                "up",
                "--port",
                "0",
                "--mongodb-port",
                "0",
                "--qdrant-port",
                &occupied.local_addr().unwrap().port().to_string(),
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("bind failure left CLI running")
    .unwrap();
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "CLI announced readiness before binding all requested services"
    );
}
