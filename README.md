# supercloudy

An in-memory DynamoDB and S3 emulator speaking AWS JSON 1.0 and REST/XML. Use
normal AWS SDK clients against one local endpoint. No cloud account or disk storage
is needed.

```sh
cargo run -- up --port 8000 --bucket synthetic-images
```

Or install the CLI:

```sh
cargo install --path crates/supercloudy
supercloudy up --port 8000
```

The server binds to `127.0.0.1`. `--port 0` selects an available port and prints
its address. Stop with Ctrl-C; all tables, buckets and objects disappear on exit.

Configure your SDK with the endpoint and dummy credentials only:

```sh
export AWS_ENDPOINT_URL=http://127.0.0.1:8000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

For a Rust `aws-sdk-dynamodb` client, explicit configuration avoids ambient profiles:

```rust,no_run
use aws_sdk_dynamodb::{Client, config::{Credentials, Region}};

let config = aws_sdk_dynamodb::Config::builder()
    .behavior_version_latest()
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("test", "test", None, None, "synthetic"))
    .endpoint_url("http://127.0.0.1:8000")
    .build();
let client = Client::from_conf(config);
```

Supported operations:

- `CreateTable`, `DescribeTable`, `DeleteTable`: simple or composite primary keys;
  tables become active immediately.
- `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`: typed attributes, conditional
  writes, aliases, and atomic updates. Missing reads omit `Item`.
- `Query`: partition-key equality, sort-key `=`, `<`, `<=`, `>`, `>=`, inclusive
  `BETWEEN`, and string/binary `begins_with`. Ascending/reverse ordering, `Limit`,
  continuation keys, filters, projections, and `Select=COUNT` are supported.
- `Scan`: table/index reads, filters, projections, and segmented scans.
- Global and local secondary indexes: sparse membership, ordered reads, and
  `ALL`, `KEYS_ONLY`, and `INCLUDE` projections. Query/Scan pages honor both
  `Limit` and the 1 MiB evaluated-data boundary.
- `TransactWriteItems` / `TransactGetItems`: atomic writes and snapshot reads,
  up to 100 actions, ordered cancellation reasons, and ten-minute write tokens.
- `BatchGetItem` / `BatchWriteItem`: multi-table reads/writes and retryable
  unprocessed subsets, with DynamoDB count and size limits.

Conditions support comparisons, `BETWEEN`, `IN`, `AND`, `OR`, `NOT`, parentheses,
`attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`,
`contains`, and `size`. Updates support `SET`, `REMOVE`, `ADD`, `DELETE`, document
paths, arithmetic, `if_not_exists`, and `list_append`. Use expression name aliases
for reserved words and literal attribute names containing dots.

`PutItem`/`DeleteItem` support `NONE` and `ALL_OLD`; `UpdateItem` additionally supports
`ALL_NEW`, `UPDATED_OLD`, and `UPDATED_NEW`. Failed conditions return the modeled
`ConditionalCheckFailedException` and leave stored state unchanged.
`ReturnValuesOnConditionCheckFailure=ALL_OLD` includes the previous item.

All AttributeValue types are supported, including recursive maps/lists and sets.
Numbers use exact decimal arithmetic with DynamoDB's 38 significant digit and
magnitude constraints. Numeric key identity ignores spelling (`1` equals `1.0`).

This is a scoped emulator. Streams, TTL processing, IAM, capacity simulation,
persistence, account/region isolation, online index changes, and legacy request
forms remain excluded. Index visibility is immediate. Table size/capacity
metadata is not simulated. Unimplemented operations return
`UnknownOperationException`; unsupported options return `ValidationException`.

## S3

Create buckets at startup with repeatable `--bucket <name>`, or use `CreateBucket`.
`PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `CopyObject`, and
`ListObjectsV2` are supported. Object writes and copies are atomic.

Both addressing styles reach the same bucket:

```text
http://127.0.0.1:8000/synthetic-images/photo.jpg
http://synthetic-images.localhost:8000/photo.jpg
```

For SDK default virtual-host addressing, set the endpoint to
`http://localhost:8000`; your resolver must resolve `<bucket>.localhost` to loopback.
The server does not provide DNS. An IP endpoint works with ordinary SDK endpoint
resolution; clients can also explicitly request path style. No consumer path-style
configuration seam is required when wildcard localhost resolves locally.

S3 supports single byte ranges (closed, open-ended, suffix), content type, user
metadata, up to ten upload tags, and declared `aws:kms` encryption attributes.
KMS attributes are stored and echoed; no encryption or KMS calls occur. S3 stores
tags and exposes the count on GET/HEAD (`x-amz-tagging-count` for nonempty sets);
the separate object-tagging APIs are excluded.
SHA256 and CRC32 upload checksums (including SDK streaming trailers) and Content-MD5
are validated before publication. Checksum-enabled full reads return the stored
checksum. ETags are quoted MD5 digests, including simulated SSE objects.

Copy accepts slash-preserving or fully percent-encoded sources. Metadata/content
type and tags independently support `COPY` and `REPLACE`. Lists support prefixes,
MaxKeys (up to 1,000), opaque bucket/prefix-bound continuation tokens, and URL
encoding. A page is coherent; traversal is not a snapshot across requests.

SDK presigned PUT/GET URLs are accepted and expiry is checked. Signatures and IAM
are **not authenticated**. GET supports `response-content-disposition`.
Multipart uploads, ACLs, versioning, conditional requests, delimiter/start-after
listing, and other unimplemented options return explicit S3 errors. S3 errors use
REST/XML; HEAD error responses have no body.

Objects have a 64 MiB decoded size limit. The separate 65 MiB wire body limit
allows up to 1 MiB of AWS chunk framing/trailers for maximum-size objects.
Overflow returns `EntityTooLarge` and preserves any existing object. DynamoDB's
independent JSON body limit remains 32 MiB.

Routing inspects the authority/Host, method, raw path/query and target before body
collection. Precedence is any `X-Amz-Target`, then explicit SigV4 service scope,
then `<bucket>.localhost`, then non-root path-style S3. Unknown DynamoDB targets
stay JSON errors; unsupported signing services cannot fall through to S3.
Conflicting/malformed explicit signing scopes fail before execution.

Future services reserve the following spaces on the shared base host. These
reservations are documentation only; current non-root paths still reach S3:

- SSM: `POST /` with target `AmazonSSM.<operation>`, at target precedence.
- Lambda: all methods on raw `/2015-03-31/functions` and its `/...` descendants,
  including `POST /2015-03-31/functions/{function}/invocations`.
- Plain HTTP stubs: explicitly configured exact method/raw-path pairs only,
  such as `GET /certificates/synthetic.pem`.

When activated, future AWS scopes select their service. After target/scope/bucket
host selection, Lambda paths precede configured HTTP pairs, which precede the
path-style S3 fallback. Unsupported operations stay with their owning service.
Raw reserved paths are matched without decoding or normalization. These future
paths can collide with base-host path-style bucket/key requests; use bucket-host
addressing or explicit S3 signing for such keys. Bucket hosts retain S3 object
paths, including service-looking paths, subject to target/scope precedence.
No future service or stub handlers are implemented.

To exercise a running server with synthetic data and dummy credentials:

```sh
cargo run --example s3_smoke -- http://localhost:8000 synthetic-images
```

## Deterministic faults

Ordinary startup is fault-free. Explicitly load ordered rules to exercise retries:

```sh
supercloudy up --port 8137 --faults rules.json
```

```json
{"rules":[{"operation":"PutItem","table":"synthetic-items","first_request":1,"repeat":2,"effect":{"type":"throttle"}}]}
```

S3 rules use `"service":"s3"` with optional `bucket` and `key` selectors and
`throttle` (HTTP 503 `SlowDown`) or `internal_server_error` (HTTP 500 `InternalError`)
effects. Copy selectors identify the destination. Omitting `service` preserves
DynamoDB rules. Cross-service selectors and effects fail startup validation.

Rules count fully validated matching requests per instance; the first eligible
rule wins. A transaction cancellation rule applies only when the request has a
non-`None` reason position; shorter requests leave that rule available.
SDK retries count as new requests. Effects are `throttle` (HTTP 400),
`internal_server_error` (HTTP 500), `transaction_canceled` with ordered `reasons`
(for example `["None","TransactionConflict"]`), and `batch_deferral` with
zero-based per-table `positions` (for example `{"synthetic-items":[0,2]}`).
Errors precede mutation; batch deferral processes only the other actions.
Invalid configurations fail startup. Use isolated instances and serial requests
for repeatable fault scenarios. Library callers can use
`server::router_with_faults(server::FaultConfig)`; `server::router()` has no faults.

See [the architecture contract](docs/ARCHITECTURE.md) for behavior and boundaries.

## Development

Use current stable Rust (validated with 1.99.1), rustfmt, Clippy, Bash, Git, find,
and awk. Run from the repository root:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
scripts/secret-scan.sh
```

Behavior tests use `aws-sdk-dynamodb`, `aws-sdk-s3`, and reqwest with local endpoints, synthetic
fixtures, and credentials `test`. Each test spawns its own server on a random
loopback port. No cloud account, Docker, or Python is required.

The secret scan checks regular files including ignored files, the Git index,
and every commit reachable from local Git refs. It excludes `.git` metadata and
`target` directories, rejects credential filenames and common secret formats,
and reports locations without printing matches. This pattern guard cannot prove
that arbitrary private data is absent. Use a complete clone when auditing history.
The scanner also runs in `cargo test`.

Enable the optional pre-commit hook with:

```sh
git config core.hooksPath .githooks
```

## License

[MIT](LICENSE), Copyright (c) 2026 Steve Pryde.
