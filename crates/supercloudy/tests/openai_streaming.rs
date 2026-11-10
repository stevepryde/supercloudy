use reqwest::Client;
use serde_json::{Value, json};
use supercloudy::server::{Config, ServiceConfig, router_with_services};
use tokio::{net::TcpListener, task::JoinHandle};

struct Emulator {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Emulator {
    async fn new(config: Value) -> Self {
        let router = router_with_services(
            Config::default(),
            ServiceConfig {
                openai: Some(serde_json::from_value(config).unwrap()),
                ..Default::default()
            },
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, task }
    }
    async fn post(&self, input: Value, stream: bool) -> reqwest::Response {
        Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(&self.url)
            .bearer_auth("test")
            .body(
                json!({"model":"synthetic-model", "input":input, "stream":stream,
                "tools":[{"type":"function", "name":"calculator", "parameters":{"type":"object"}}]})
                .to_string(),
            )
            .send()
            .await
            .unwrap()
    }
}
fn fixtures() -> Value {
    serde_json::from_str(include_str!("../examples/codex-fixtures.json")).unwrap()
}
async fn events(response: reqwest::Response) -> Vec<Value> {
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert!(response.headers().contains_key("x-request-id"));
    response
        .text()
        .await
        .unwrap()
        .split("\n\n")
        .filter(|frame| !frame.is_empty())
        .map(|frame| {
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap();
            serde_json::from_str(data).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn streamed_tool_round_trip_and_new_turn_ignore_earlier_history() {
    let e = Emulator::new(fixtures()).await;
    let mut history = vec![
        json!({"type":"additional_tools", "role":"developer", "tools":[
            {"type":"function", "name":"calculator", "description":"Synthetic calculator", "parameters":{"type":"object"}}
        ]}),
        json!({"role":"user", "content":"Calculate six times seven"}),
    ];
    let first = events(e.post(json!(history), true).await).await;
    let call = first
        .iter()
        .find(|event| event["type"] == "response.output_item.done")
        .unwrap()["item"]
        .clone();
    assert_eq!(call["type"], "function_call");
    assert_eq!(call["name"], "calculator");
    let arguments: Value = serde_json::from_str(call["arguments"].as_str().unwrap()).unwrap();
    let result = arguments["left"].as_i64().unwrap() * arguments["right"].as_i64().unwrap();
    history.push(call.clone());
    history.push(
        json!({"type":"function_call_output", "call_id":call["call_id"],
        "output":[{"type":"input_text", "text":result.to_string()}]}),
    );
    let second = events(e.post(json!(history), true).await).await;
    let text = second
        .iter()
        .filter(|event| event["type"] == "response.output_text.delta")
        .map(|event| event["delta"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(text, "The result is 42.");
    let final_response = &second.last().unwrap()["response"];
    assert_eq!(final_response["status"], "completed");
    assert_eq!(final_response["output"][0]["content"][0]["text"], text);
    history.push(final_response["output"][0].clone());
    history.push(json!({"role":"user", "content":"Calculate six times seven again"}));
    let third = e.post(json!(history), false).await;
    assert_eq!(third.status(), 200);
    let body: Value = serde_json::from_str(&third.text().await.unwrap()).unwrap();
    assert_eq!(body["output"][0]["type"], "function_call");
    assert_ne!(body["output"][0]["call_id"], call["call_id"]);
    // Earlier prompts, developer text and opaque image URLs cannot select a fixture.
    let no_match = e.post(json!([
        {"role":"user", "content":"Say hello"},
        {"role":"developer", "content":"Say hello"},
        {"role":"user", "content":[{"type":"input_image", "image_url":"https://example.test/Say hello"}]}
    ]), false).await;
    assert_eq!(no_match.status(), 400);
    let body: Value = serde_json::from_str(&no_match.text().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "fixture_not_found");
    // A malformed typed item must not fall through as a user message and panic
    // later when matching a function result.
    for malformed in [
        json!({"type":"function_call_output", "role":"user", "content":"Say hello", "output":42}),
        json!({"type":"additional_tools", "role":"developer", "tools":null}),
        json!({"type":"reasoning", "role":"user"}),
    ] {
        assert_eq!(e.post(json!([malformed]), false).await.status(), 400);
    }
}

#[tokio::test]
async fn fallback_requires_actual_user_input() {
    let e = Emulator::new(json!({"conversation_fixtures":[{"output_text":"fallback"}]})).await;
    for item in [
        json!({"role":"developer", "content":"hello"}),
        json!({"role":"assistant", "content":"hello"}),
        json!({"type":"reasoning", "role":"user"}),
        json!({"type":"additional_tools", "role":"user", "tools":[]}),
        json!({"type":"function_call_output", "call_id":"call_synthetic", "output":"42"}),
    ] {
        assert_eq!(e.post(json!([item]), false).await.status(), 400);
    }
    // An image-only user message is still a user turn, without searchable text.
    assert_eq!(e.post(json!([{"role":"user", "content":[{"type":"input_image", "image_url":"https://example.test/image"}]}]), false).await.status(), 200);
}

#[tokio::test]
async fn structured_fixtures_also_stream_serialized_json_text() {
    let e = Emulator::new(
        json!({"fixtures":[{"format_name":"example", "output":{"items":["synthetic"]}}]}),
    )
    .await;
    let response = Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(&e.url)
        .bearer_auth("test")
        .body(
            json!({"model":"synthetic-model", "input":"sample", "stream":true,
            "text":{"format":{"type":"json_schema", "name":"example", "schema":{"type":"object"}}}})
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let stream = events(response).await;
    let text = stream
        .iter()
        .filter(|event| event["type"] == "response.output_text.delta")
        .map(|event| event["delta"].as_str().unwrap())
        .collect::<String>();
    let output: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(output["items"][0], "synthetic");
    assert_eq!(stream.last().unwrap()["response"]["status"], "completed");
}

#[tokio::test]
async fn conversational_faults_keep_stream_transport_and_named_faults_stay_scoped() {
    let mut config = fixtures();
    config["faults"] = json!([
        {"format_name":"unrelated", "first_request":1, "repeat":5, "effect":"throttle"},
        {"first_request":1, "repeat":1, "effect":"incomplete"}
    ]);
    let e = Emulator::new(config).await;
    assert_eq!(e.post(json!("No fixture"), true).await.status(), 400);
    let incomplete = events(e.post(json!("Say hello"), true).await).await;
    assert_eq!(incomplete.last().unwrap()["type"], "response.incomplete");
    assert_eq!(
        incomplete.last().unwrap()["response"]["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    let complete = events(e.post(json!("Say hello"), true).await).await;
    assert_eq!(complete.last().unwrap()["type"], "response.completed");
    assert_eq!(
        complete.last().unwrap()["response"]["output"][0]["content"][0]["text"],
        "Hello from the local fixture."
    );
}

#[test]
fn ambiguous_conversation_fixtures_fail_before_serving() {
    for fixture in [
        json!({"output_text":"hello", "function_call":{"name":"calculator", "arguments":"{}"}}),
        json!({"input_contains":"" , "output_text":"hello"}),
        json!({"input_contains":"hello", "function_output_contains":"42", "output_text":"hello"}),
        json!({"function_call":{"name":"calculator", "arguments":"not JSON"}}),
        json!({"function_call":{"name":"calculator", "arguments":"[]"}}),
        json!({"function_call":{"name":"", "arguments":"{}"}}),
        json!({"input_contains":"hello"}),
    ] {
        let config = serde_json::from_value(json!({"conversation_fixtures":[fixture]})).unwrap();
        assert!(
            router_with_services(
                Config::default(),
                ServiceConfig {
                    openai: Some(config),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}
