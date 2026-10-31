use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
};
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed, TestCaseError, TestCaseResult},
};
use std::collections::BTreeMap;
use tokio::{net::TcpListener, runtime::Builder, task::JoinHandle};

const BUCKET: &str = "synthetic-properties";

fn config() -> Config {
    let mut config = Config::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        config.cases = 32;
    }
    if std::env::var_os("PROPTEST_RNG_SEED").is_none() {
        config.rng_seed = RngSeed::Fixed(0x5333_6279_7465_7301);
    }
    config.failure_persistence = Some(Box::new(FileFailurePersistence::Direct(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/s3_properties.proptest-regressions"
    ))));
    config
}

// Avoid rendering signed SDK requests in property failures; generated inputs
// and the persisted seed contain only synthetic data.
fn request<T, E>(result: Result<T, E>) -> Result<T, TestCaseError> {
    result.map_err(|_| TestCaseError::fail("local S3 request failed"))
}

struct Emulator {
    client: Client,
    task: JoinHandle<()>,
}

impl Drop for Emulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Emulator {
    async fn new() -> Result<Self, TestCaseError> {
        let router = request(supercloudy::server::router_with_config(
            supercloudy::server::Config {
                buckets: vec![BUCKET.into()],
                faults: Default::default(),
            },
        ))?;
        let listener = request(TcpListener::bind("127.0.0.1:0").await)?;
        let endpoint = format!("http://{}", request(listener.local_addr())?);
        let client = Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
                .endpoint_url(endpoint)
                .force_path_style(true)
                .build(),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Ok(Self { client, task })
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> TestCaseResult {
        request(
            self.client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(ByteStream::from(bytes.to_vec()))
                .send()
                .await,
        )?;
        Ok(())
    }

    async fn read(&self, key: &str, expected: Option<&[u8]>) -> TestCaseResult {
        let result = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await;
        if let Some(expected) = expected {
            let output = request(result)?;
            prop_assert_eq!(output.content_length(), Some(expected.len() as i64));
            let actual = request(output.body.collect().await)?.into_bytes();
            prop_assert_eq!(actual.as_ref(), expected);
        } else {
            let error = result.err().ok_or_else(|| {
                TestCaseError::fail("GET returned a deleted or never-written object")
            })?;
            prop_assert_eq!(error.raw_response().map(|r| r.status().as_u16()), Some(404));
            prop_assert_eq!(
                error.as_service_error().and_then(|e| e.code()),
                Some("NoSuchKey")
            );
        }
        Ok(())
    }

    async fn range(&self, key: &str, bytes: &[u8], range: &Range) -> TestCaseResult {
        let result = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .range(range.header())
            .send()
            .await;
        // Select individual indexed bytes, independently of the server's range
        // parser and inclusive slice arithmetic.
        let selected: Vec<_> = bytes
            .iter()
            .copied()
            .enumerate()
            .filter(|(index, _)| match range {
                Range::Closed(start, end) => *index >= *start && *index <= *end,
                Range::Open(start) => *index >= *start,
                Range::Suffix(count) => bytes.len() - *index <= *count,
                Range::Malformed(_) => false,
            })
            .collect();
        if let (Some(first), Some(last)) = (selected.first(), selected.last()) {
            let output = request(result)?;
            let expected_header = format!("bytes {}-{}/{}", first.0, last.0, bytes.len());
            prop_assert_eq!(output.content_range(), Some(expected_header.as_str()));
            prop_assert_eq!(output.content_length(), Some(selected.len() as i64));
            let actual = request(output.body.collect().await)?.into_bytes();
            let expected: Vec<_> = selected.into_iter().map(|(_, byte)| byte).collect();
            prop_assert_eq!(actual.as_ref(), expected.as_slice());
        } else {
            let error = result.err().ok_or_else(|| {
                TestCaseError::fail("GET accepted an invalid or unsatisfiable byte range")
            })?;
            let (status, code) = if matches!(range, Range::Malformed(_)) {
                (400, "InvalidArgument")
            } else {
                (416, "InvalidRange")
            };
            prop_assert_eq!(
                error.raw_response().map(|r| r.status().as_u16()),
                Some(status)
            );
            prop_assert_eq!(error.as_service_error().and_then(|e| e.code()), Some(code));
        }
        Ok(())
    }

    async fn list(
        &self,
        model: &BTreeMap<String, Vec<u8>>,
        prefix: &str,
        page_size: usize,
    ) -> TestCaseResult {
        let expected: Vec<_> = model
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, bytes)| (Some(key.as_str()), Some(bytes.len() as i64)))
            .collect();
        let mut token = None;
        let mut offset = 0;
        loop {
            let output = request(
                self.client
                    .list_objects_v2()
                    .bucket(BUCKET)
                    .prefix(prefix)
                    .max_keys(page_size as i32)
                    .set_continuation_token(token)
                    .send()
                    .await,
            )?;
            let end = (offset + page_size).min(expected.len());
            let actual: Vec<_> = output
                .contents()
                .iter()
                .map(|o| (o.key(), o.size()))
                .collect();
            prop_assert_eq!(actual.as_slice(), &expected[offset..end]);
            prop_assert_eq!(output.key_count(), Some((end - offset) as i32));
            prop_assert_eq!(output.is_truncated(), Some(end < expected.len()));
            if end == expected.len() {
                prop_assert!(output.next_continuation_token().is_none());
                return Ok(());
            }
            token = Some(
                output
                    .next_continuation_token()
                    .ok_or_else(|| {
                        TestCaseError::fail("truncated listing omitted its continuation token")
                    })?
                    .to_owned(),
            );
            offset = end;
        }
    }
}

#[derive(Clone, Debug)]
enum Range {
    Closed(usize, usize),
    Open(usize),
    Suffix(usize),
    Malformed(&'static str),
}

impl Range {
    fn header(&self) -> String {
        match self {
            Self::Closed(start, end) => format!("bytes={start}-{end}"),
            Self::Open(start) => format!("bytes={start}-"),
            Self::Suffix(count) => format!("bytes=-{count}"),
            Self::Malformed(value) => (*value).into(),
        }
    }
}

fn ranges() -> impl Strategy<Value = Range> {
    prop_oneof![
        (0usize..130, 0usize..130).prop_map(|(start, end)| Range::Closed(start, end)),
        (0usize..130).prop_map(Range::Open),
        (0usize..130).prop_map(Range::Suffix),
        prop::sample::select(vec!["bytes=", "bytes=1-2,4-5", "bytes=x-3", "items=0-1"])
            .prop_map(Range::Malformed),
    ]
}

#[derive(Clone, Debug)]
enum Operation {
    Put(usize, Vec<u8>),
    Get(usize),
    Delete(usize),
    Range(usize, Range),
    List(usize, usize),
}

fn operations() -> impl Strategy<Value = Vec<Operation>> {
    prop::collection::vec(
        prop_oneof![
            4 => (0usize..6, prop::collection::vec(any::<u8>(), 0..97))
                .prop_map(|(key, bytes)| Operation::Put(key, bytes)),
            2 => (0usize..6).prop_map(Operation::Get),
            2 => (0usize..6).prop_map(Operation::Delete),
            2 => (0usize..6, ranges()).prop_map(|(key, range)| Operation::Range(key, range)),
            2 => (0usize..5, 1usize..4).prop_map(|(prefix, size)| Operation::List(prefix, size)),
        ],
        1..25,
    )
}

fn key_suffix() -> impl Strategy<Value = String> {
    // XML-safe Unicode scalar values plus URL punctuation; fixed surrounding
    // segments keep the resulting keys nonempty and below S3's size limit.
    prop::collection::vec(
        prop_oneof![
            prop::char::range(' ', '~'),
            prop::char::range('\u{a1}', '\u{d7ff}'),
            prop::char::range('\u{e000}', '\u{fffd}'),
            prop::char::range('\u{10000}', '\u{10ffff}'),
        ],
        0..6,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn s3_operation_sequences_match_btree_model(suffix in key_suffix(), ops in operations()) {
        let runtime = request(Builder::new_current_thread().enable_all().build())?;
        runtime.block_on(async {
            let emulator = Emulator::new().await?;
            let keys = [
                format!("plain-{suffix}"),
                format!("nested/{suffix}/"),
                format!("雪/{suffix}/é"),
                format!("%2F/{suffix}/%25"),
                format!("space {suffix}/a+b?#&<>\""),
                format!("/{suffix}//leaf"),
            ];
            let prefixes = ["", "nested/", "雪/", "%2F/", "absent/"];
            let mut model = BTreeMap::new();
            for op in ops {
                match op {
                    Operation::Put(index, bytes) => {
                        let key = &keys[index];
                        emulator.put(key, &bytes).await?;
                        model.insert(key.clone(), bytes);
                        emulator.read(key, model.get(key).map(Vec::as_slice)).await?;
                    }
                    Operation::Get(index) => {
                        emulator.read(&keys[index], model.get(&keys[index]).map(Vec::as_slice)).await?;
                    }
                    Operation::Delete(index) => {
                        let key = &keys[index];
                        // Repeated deletion must succeed and keep the object missing.
                        for _ in 0..2 {
                            request(emulator.client.delete_object().bucket(BUCKET).key(key).send().await)?;
                        }
                        model.remove(key);
                        emulator.read(key, None).await?;
                    }
                    Operation::Range(index, range) => {
                        let key = &keys[index];
                        if let Some(bytes) = model.get(key) {
                            emulator.range(key, bytes, &range).await?;
                        } else {
                            emulator.read(key, None).await?;
                        }
                    }
                    Operation::List(prefix, size) => {
                        emulator.list(&model, prefixes[prefix], size).await?;
                    }
                }
            }
            for key in &keys {
                emulator.read(key, model.get(key).map(Vec::as_slice)).await?;
            }
            for prefix in prefixes {
                emulator.list(&model, prefix, 1).await?;
            }
            Ok::<(), TestCaseError>(())
        })?;
    }

    #[test]
    fn s3_byte_ranges_match_indexed_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..97),
        start in 0usize..130,
        end in 0usize..130,
        suffix in 0usize..130,
    ) {
        let runtime = request(Builder::new_current_thread().enable_all().build())?;
        runtime.block_on(async {
            let emulator = Emulator::new().await?;
            emulator.put("range/雪%2F", &bytes).await?;
            emulator.read("range/雪%2F", Some(&bytes)).await?;
            // Each case covers every form and its boundaries, including the
            // inclusive first/last byte that exposes range off-by-one defects.
            for range in [
                Range::Closed(start, end),
                Range::Open(start),
                Range::Suffix(suffix),
                Range::Closed(0, 0),
                Range::Closed(bytes.len().saturating_sub(1), bytes.len() + 5),
                Range::Open(bytes.len()),
                Range::Closed(2, 1),
                Range::Suffix(0),
                Range::Suffix(bytes.len() + 1),
                Range::Malformed("bytes=0-1,3-4"),
            ] {
                emulator.range("range/雪%2F", &bytes, &range).await?;
            }
            // Failed range requests do not mutate the full object.
            emulator.read("range/雪%2F", Some(&bytes)).await?;
            emulator.put("empty", &[]).await?;
            emulator.read("empty", Some(&[])).await?;
            for range in [Range::Closed(0, 0), Range::Open(0), Range::Suffix(1)] {
                emulator.range("empty", &[], &range).await?;
            }
            Ok::<(), TestCaseError>(())
        })?;
    }
}
