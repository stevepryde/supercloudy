//! Buffered, deterministic Responses SSE. No timing or tokenization simulation.
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

pub(super) fn response(completed: Value, id: &str) -> Response {
    let mut events = Vec::new();
    let mut started = completed.clone();
    started["status"] = json!("in_progress");
    started["output"] = json!([]);
    started["incomplete_details"] = Value::Null;
    started["usage"] = Value::Null;
    events.push(json!({"type":"response.created", "response":started}));
    events.push(json!({"type":"response.in_progress", "response":started}));
    for item in completed["output"].as_array().expect("response output") {
        let mut added = item.clone();
        added["status"] = json!("in_progress");
        if item["type"] == "message" {
            added["content"] = json!([]);
        } else {
            added["arguments"] = json!("");
        }
        events.push(json!({"type":"response.output_item.added", "output_index":0, "item":added}));
        if item["type"] == "message" {
            let part = &item["content"][0];
            let mut empty = part.clone();
            empty["text"] = json!("");
            events.push(
                json!({"type":"response.content_part.added", "output_index":0,
                "item_id":item["id"], "content_index":0, "part":empty}),
            );
            events.push(
                json!({"type":"response.output_text.delta", "output_index":0,
                "item_id":item["id"], "content_index":0, "delta":part["text"], "logprobs":[]}),
            );
            events.push(json!({"type":"response.output_text.done", "output_index":0,
                "item_id":item["id"], "content_index":0, "text":part["text"], "logprobs":[]}));
            events.push(
                json!({"type":"response.content_part.done", "output_index":0,
                "item_id":item["id"], "content_index":0, "part":part}),
            );
        } else {
            events.push(
                json!({"type":"response.function_call_arguments.delta", "output_index":0,
                "item_id":item["id"], "delta":item["arguments"]}),
            );
            events.push(
                json!({"type":"response.function_call_arguments.done", "output_index":0,
                "item_id":item["id"], "arguments":item["arguments"]}),
            );
        }
        events.push(json!({"type":"response.output_item.done", "output_index":0, "item":item}));
    }
    let event_type = if completed["status"] == "incomplete" {
        "response.incomplete"
    } else {
        "response.completed"
    };
    events.push(json!({"type":event_type, "response":completed}));
    let mut body = String::new();
    for (sequence, mut event) in events.into_iter().enumerate() {
        event["sequence_number"] = json!(sequence);
        body.push_str(&format!(
            "event: {}\ndata: {}\n\n",
            event["type"].as_str().unwrap(),
            event
        ));
    }
    (
        [
            ("content-type", "text/event-stream"),
            ("cache-control", "no-cache"),
            ("x-request-id", id),
        ],
        body,
    )
        .into_response()
}
