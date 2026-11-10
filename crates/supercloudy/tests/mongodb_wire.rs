use bson::{Bson, Document, doc, spec::BinarySubtype};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
async fn send(
    stream: &mut TcpStream,
    command: Document,
    sequence: Option<(&str, Vec<Document>)>,
    flags: u32,
) -> Document {
    let mut body = flags.to_le_bytes().to_vec();
    body.push(0);
    body.extend(bson::to_vec(&command).unwrap());
    if flags & 1 != 0 {
        body.extend([0xff, 0xff, 0xff, 0xff]);
    }
    if let Some((name, docs)) = sequence {
        body.push(1);
        let mut section = vec![0; 4];
        section.extend(name.as_bytes());
        section.push(0);
        for doc in docs {
            section.extend(bson::to_vec(&doc).unwrap())
        }
        let length = section.len() as i32;
        section[..4].copy_from_slice(&length.to_le_bytes());
        body.extend(section);
    }
    let mut frame = ((16 + body.len()) as i32).to_le_bytes().to_vec();
    frame.extend(17i32.to_le_bytes());
    frame.extend(0i32.to_le_bytes());
    frame.extend(2013i32.to_le_bytes());
    frame.extend(body);
    stream.write_all(&frame).await.unwrap();
    if flags & 2 != 0 {
        return Document::new();
    }
    let mut header = [0; 16];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(i32::from_le_bytes(header[8..12].try_into().unwrap()), 17);
    let length = i32::from_le_bytes(header[..4].try_into().unwrap());
    let mut body = vec![0; length as usize - 16];
    stream.read_exact(&mut body).await.unwrap();
    Document::from_reader(&mut std::io::Cursor::new(&body[5..])).unwrap()
}
#[tokio::test]
async fn document_sequences_unacknowledged_writes_and_retryable_writes() {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let session =
        Bson::Document(doc! {"id":bson::Binary{subtype:BinarySubtype::Uuid,bytes:vec![1;16]}});
    let command = doc! {"insert":"items","$db":"test","lsid":session.clone(),"txnNumber":1i64};
    let response = send(
        &mut stream,
        command.clone(),
        Some(("documents", vec![doc! {"_id":1,"n":0}])),
        0,
    )
    .await;
    assert_eq!(response.get_i32("n").unwrap(), 1);
    let repeated = send(
        &mut stream,
        command,
        Some(("documents", vec![doc! {"_id":1,"n":0}])),
        0,
    )
    .await;
    assert_eq!(repeated, response);
    let update = doc! {"update":"items","updates":[{"q":{"_id":1},"u":{"$inc":{"n":1}}}],"lsid":session,"txnNumber":2i64,"$db":"test"};
    send(&mut stream, update.clone(), None, 0).await;
    send(&mut stream, update, None, 0).await;
    send(
        &mut stream,
        doc! {"insert":"items","documents":[{"_id":2}],"writeConcern":{"w":0},"$db":"test"},
        None,
        2,
    )
    .await;
    let found = send(
        &mut stream,
        doc! {"find":"items","sort":{"_id":1},"$db":"test"},
        None,
        0,
    )
    .await;
    let batch = found
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].as_document().unwrap().get_i32("n").unwrap(), 1);
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn malformed_frames_are_closed_and_unknown_commands_do_not_break_connection() {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let mut malformed = TcpStream::connect(server.address()).await.unwrap();
    let mut frame = vec![0; 16];
    frame[..4].copy_from_slice(&15i32.to_le_bytes());
    malformed.write_all(&frame).await.unwrap();
    let mut byte = [0];
    assert_eq!(malformed.read(&mut byte).await.unwrap(), 0);
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let unknown = send(&mut stream, doc! {"unknownCommand":1,"$db":"test"}, None, 0).await;
    assert_eq!(unknown.get_i32("code").unwrap(), 59);
    assert_eq!(
        send(&mut stream, doc! {"ping":1,"$db":"test"}, None, 0)
            .await
            .get_f64("ok")
            .unwrap(),
        1.0
    );
    let bad = send(
        &mut stream,
        doc! {"insert":"items","documents":[{"_id":1}],"$db":"test"},
        Some(("documents", vec![doc! {"_id":2}])),
        0,
    )
    .await;
    assert_eq!(bad.get_f64("ok").unwrap(), 0.0);
    let found = send(&mut stream, doc! {"find":"items","$db":"test"}, None, 0).await;
    assert!(
        found
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .is_empty()
    );
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn drop_cancels_existing_connections_and_releases_port() {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let address = server.address();
    let mut stream = TcpStream::connect(address).await.unwrap();
    send(&mut stream, doc! {"ping":1,"$db":"test"}, None, 0).await;
    drop(server);
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn corrupt_checksummed_frames_are_rejected_before_writes() {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let mut stream = TcpStream::connect(server.address()).await.unwrap();
    let response = send(
        &mut stream,
        doc! {"insert":"items","documents":[{"_id":1}],"$db":"test"},
        None,
        1,
    )
    .await;
    assert_eq!(response.get_i32("code").unwrap(), 72);
    let found = send(&mut stream, doc! {"find":"items","$db":"test"}, None, 0).await;
    assert!(
        found
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .is_empty()
    );
    server.shutdown().await.unwrap();
}
