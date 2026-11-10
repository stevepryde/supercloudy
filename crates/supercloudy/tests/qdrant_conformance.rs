#[path = "qdrant_scenarios/mod.rs"]
mod scenarios;
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

#[tokio::test]
#[ignore = "requires Docker; explicitly runs the pinned real Qdrant server"]
async fn real_server_conformance() {
    let container = GenericImage::new("qdrant/qdrant", "v1.18.3-unprivileged")
        .with_exposed_port(6333.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Qdrant HTTP listening on 6333"))
        .start()
        .await
        .expect("start pinned Qdrant container (Docker is required)");
    let port = container.get_host_port_ipv4(6333).await.unwrap();
    let host = container.get_host().await.unwrap();
    let real = scenarios::Http::new(format!("http://{host}:{port}"));
    let emulated = supercloudy::qdrant::start(0).await.unwrap();
    let local = scenarios::Http::new(format!("http://{}", emulated.address()));
    let expected = scenarios::scenario(&real).await;
    let actual = scenarios::scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::bm25_scenario(&real).await;
    let actual = scenarios::bm25_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::auxiliary_scenario(&real).await;
    let actual = scenarios::auxiliary_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::sparse_statistics_scenario(&real).await;
    let actual = scenarios::sparse_statistics_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::precision_scenario(&real).await;
    let actual = scenarios::precision_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::preprocessing_scenario(&real).await;
    let actual = scenarios::preprocessing_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::filter_boundary_scenario(&real).await;
    let actual = scenarios::filter_boundary_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::selector_boundary_scenario(&real).await;
    let actual = scenarios::selector_boundary_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    let expected = scenarios::index_type_scenario(&real).await;
    let actual = scenarios::index_type_scenario(&local).await;
    scenarios::compare(&actual, &expected);
    emulated.shutdown().await.unwrap();
}
