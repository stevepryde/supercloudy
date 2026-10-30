use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn protocol_errors_preserve_aws_envelopes_and_target_precedence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, supercloudy::server::router())
            .await
            .unwrap();
    });
    for (method, path, target, body, code, namespace) in [
        (
            "POST",
            "/",
            Some("DynamoDB_20120810.NotAnOperation"),
            "{",
            "UnknownOperationException",
            "com.amazon.coral.service",
        ),
        (
            "POST",
            "/",
            None,
            "{}",
            "UnknownOperationException",
            "com.amazon.coral.service",
        ),
        (
            "POST",
            "/",
            Some("DynamoDB_20111205.GetItem"),
            "{}",
            "UnknownOperationException",
            "com.amazon.coral.service",
        ),
        (
            "GET",
            "/",
            Some("DynamoDB_20120810.GetItem"),
            "{}",
            "UnknownOperationException",
            "com.amazon.coral.service",
        ),
        (
            "POST",
            "/other",
            Some("DynamoDB_20120810.GetItem"),
            "{}",
            "UnknownOperationException",
            "com.amazon.coral.service",
        ),
        (
            "POST",
            "/",
            Some("DynamoDB_20120810.GetItem"),
            "{",
            "SerializationException",
            "com.amazonaws.dynamodb.v20120810",
        ),
        (
            "POST",
            "/",
            Some("DynamoDB_20120810.GetItem"),
            "[]",
            "SerializationException",
            "com.amazonaws.dynamodb.v20120810",
        ),
        (
            "POST",
            "/",
            Some("DynamoDB_20120810.GetItem"),
            "{\"TableName\":7}",
            "SerializationException",
            "com.amazonaws.dynamodb.v20120810",
        ),
        (
            "POST",
            "/",
            Some("DynamoDB_20120810.GetItem"),
            "{\"TableName\":\"synthetic-items\",\"Key\":{\"pk\":{\"S\":\"one\"}}}",
            "ResourceNotFoundException",
            "com.amazonaws.dynamodb.v20120810",
        ),
    ] {
        let mut connection = TcpStream::connect(address).await.unwrap();
        let target_header = target
            .map(|t| format!("X-Amz-Target: {t}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/x-amz-json-1.0\r\n{target_header}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        connection.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connection.read_to_string(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.starts_with("HTTP/1.1 400 "));
        assert!(headers.contains("content-type: application/x-amz-json-1.0\r\n"));
        assert!(headers.contains("x-amzn-requestid: "));
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["__type"], json!(format!("{namespace}#{code}")));
        assert!(body["message"].is_string());
    }
    server.abort();
}
