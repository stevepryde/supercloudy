//! Opt-in compatibility test against an installed, real Codex app-server.
//! No account, cloud model, ambient configuration, or real credentials are used.
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use supercloudy::server::{Config, ServiceConfig, router_with_services};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Workspace(PathBuf);
impl Workspace {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("supercloudy-codex-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(root.join("home/codex")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        Self(root)
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct AppServer {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    pending: VecDeque<Value>,
    next_id: u64,
}
impl AppServer {
    async fn start(workspace: &Workspace, base_url: &str) -> Self {
        let binary = std::env::var_os("CODEX_BIN").unwrap_or_else(|| "codex".into());
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", workspace.0.join("home"))
            .env("CODEX_HOME", workspace.0.join("home/codex"))
            .env("SUPERCLOUDY_API_KEY", "test")
            .current_dir(workspace.0.join("workspace"))
            .arg("app-server")
            .arg("--listen")
            .arg("stdio://");
        for setting in [
            "model=\"synthetic-model\"".to_owned(),
            "model_provider=\"supercloudy\"".to_owned(),
            "model_providers.supercloudy.name=\"supercloudy\"".to_owned(),
            format!("model_providers.supercloudy.base_url=\"{base_url}\""),
            "model_providers.supercloudy.wire_api=\"responses\"".to_owned(),
            "model_providers.supercloudy.env_key=\"SUPERCLOUDY_API_KEY\"".to_owned(),
            "model_providers.supercloudy.requires_openai_auth=false".to_owned(),
            "model_providers.supercloudy.request_max_retries=0".to_owned(),
            "model_providers.supercloudy.stream_max_retries=0".to_owned(),
            "cli_auth_credentials_store=\"file\"".to_owned(),
            "analytics.enabled=false".to_owned(),
            "check_for_update_on_startup=false".to_owned(),
        ] {
            command.arg("-c").arg(setting);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("install Codex or set CODEX_BIN");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut server = Self {
            child,
            stdin,
            stdout,
            pending: VecDeque::new(),
            next_id: 1,
        };
        server
            .rpc(
                "initialize",
                json!({"clientInfo":{"name":"supercloudy_smoke", "version":"1"},
            "capabilities":{"experimentalApi":true}}),
            )
            .await;
        server
            .send(json!({"method":"initialized", "params":{}}))
            .await;
        server
    }
    async fn send(&mut self, value: Value) {
        self.stdin
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
        self.stdin.flush().await.unwrap();
    }
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(30), self.stdout.next_line())
            .await
            .expect("app-server did not produce an event within 30 seconds")
            .unwrap()
            .expect("app-server exited");
        serde_json::from_str(&line).expect("app-server JSON line")
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"id":id,"method":method,"params":params}))
            .await;
        loop {
            let event = self.read().await;
            if event["id"] == id && event.get("method").is_none() {
                assert!(event.get("error").is_none(), "{method} failed: {event}");
                return event["result"].clone();
            }
            self.pending.push_back(event);
        }
    }
    async fn turn(&mut self, thread_id: &str) {
        self.rpc(
            "turn/start",
            json!({"threadId":thread_id,
            "input":[{"type":"text","text":"Calculate six times seven", "text_elements":[]}]}),
        )
        .await;
        let mut calls = 0;
        let mut text = String::new();
        loop {
            let event = if let Some(event) = self.pending.pop_front() {
                event
            } else {
                self.read().await
            };
            match event["method"].as_str() {
                Some("item/tool/call") => {
                    assert_eq!(event["params"]["tool"], "calculator");
                    let arguments = &event["params"]["arguments"];
                    let result =
                        arguments["left"].as_i64().unwrap() * arguments["right"].as_i64().unwrap();
                    self.send(json!({"id":event["id"], "result":{"success":true,
                        "contentItems":[{"type":"inputText","text":result.to_string()}]}}))
                        .await;
                    calls += 1;
                    assert_eq!(calls, 1, "fixture replayed a completed function call");
                }
                Some("item/agentMessage/delta") => {
                    text.push_str(event["params"]["delta"].as_str().unwrap())
                }
                Some("turn/completed") => {
                    assert_eq!(event["params"]["turn"]["status"], "completed", "{event}");
                    break;
                }
                Some("error") => panic!("app-server turn error: {event}"),
                _ if event.get("method").is_some() && event.get("id").is_some() => {
                    panic!("unexpected server request: {event}");
                }
                _ => {}
            }
        }
        assert_eq!(
            calls, 1,
            "Codex did not dispatch the fixture's dynamic tool"
        );
        assert_eq!(text, "The result is 42.");
    }
    async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires an installed Codex binary; set CODEX_BIN if not on PATH"]
async fn real_codex_dynamic_tool_and_resumed_thread() {
    let workspace = Workspace::new();
    let router = router_with_services(
        Config::default(),
        ServiceConfig {
            openai: Some(
                serde_json::from_str(include_str!("../examples/codex-fixtures.json")).unwrap(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let serving = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut first = AppServer::start(&workspace, &base_url).await;
        let started = first.rpc("thread/start", json!({"cwd":workspace.0.join("workspace"),
            "approvalPolicy":"never", "sandbox":"read-only", "dynamicTools":[{
                "type":"function", "name":"calculator", "description":"Multiply two integers",
                "inputSchema":{"type":"object", "properties":{"left":{"type":"integer"}, "right":{"type":"integer"}},
                    "required":["left","right"], "additionalProperties":false}
            }]})).await;
        let thread_id = started["thread"]["id"].as_str().unwrap().to_owned();
        first.turn(&thread_id).await;
        first.stop().await;
        let mut second = AppServer::start(&workspace, &base_url).await;
        second.rpc("thread/resume", json!({"threadId":thread_id, "cwd":workspace.0.join("workspace"),
            "approvalPolicy":"never", "sandbox":"read-only"})).await;
        second.turn(&thread_id).await;
        second.stop().await;
    }).await.expect("real Codex smoke exceeded 90 seconds");
    serving.abort();
}
