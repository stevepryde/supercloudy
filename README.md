# supercloudy

An in-memory DynamoDB, S3 and SSM emulator with local OpenAI Responses and Firebase ID token stubs.
Use ordinary AWS SDKs and HTTP clients against one loopback endpoint. No cloud
account is needed; service handlers make no outbound network calls.

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

Routing selects service ownership before reading bodies: any `X-Amz-Target`,
then explicit SigV4 scope, then `<bucket>.localhost`, then the enabled exact
`POST /v1/responses` and Firebase certificate/mint reservations, then non-root
path-style S3. SSM target errors
use JSON 1.1; DynamoDB retains JSON 1.0. Unknown operations stay with their service.
Malformed/conflicting signing scopes fail before execution. GET/PUT, encoded or
trailing-slash Responses paths remain S3. Explicit S3 signing and valid bucket
hosts retain S3 ownership of service-looking paths. The full reservation table,
including future inactive Lambda paths, is in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

To exercise a running server with synthetic data and dummy credentials:

```sh
cargo run --example s3_smoke -- http://localhost:8000 synthetic-images
```

## SSM and local Responses fixtures

Load synthetic JSON configuration once at startup:

```sh
supercloudy up --port 8000 --bucket synthetic-images \
  --ssm-parameters crates/supercloudy/examples/ssm-parameters.json \
  --openai-fixtures crates/supercloudy/examples/openai-fixtures.json
```

SSM config:

```json
{"parameters":[{"name":"/example/provider-key","type":"SecureString","value":"test","encrypted_value":"synthetic-ciphertext"}]}
```

`GetParameter` supports String, StringList and SecureString. Missing parameters
return modeled `ParameterNotFound`. SecureString returns `encrypted_value` unless
`WithDecryption` is true, then `value`. This is fixture selection, not encryption.
No parameter writes, ARN/label/version lookup or KMS calls are supported.

Responses config:

```json
{"fixtures":[{"format_name":"example_extraction","input_contains":"sample","output":{"items":["synthetic"]}}],"faults":[]}
```

Fixtures match exact `text.format.name` and optional case-sensitive substring in
an individual input text entry; first match wins. Schema text and opaque image/file
fields never participate. Output JSON becomes one `output_text` string in a completed
Responses envelope. Consumer schema/parser validation belongs in consumer tests.
No match returns 400 `fixture_not_found`. Every stub response has `x-request-id`.

```sh
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://127.0.0.1:8000 ssm get-parameter \
  --name /example/provider-key --with-decryption
curl -i http://127.0.0.1:8000/v1/responses \
  -H 'Authorization: Bearer test' -H 'Content-Type: application/json' \
  -d '{"model":"synthetic-model","input":"sample","text":{"format":{"type":"json_schema","name":"example_extraction","strict":true,"schema":{"type":"object"}}}}'
```

One nonempty bearer is required; missing, empty or duplicate headers return 401.
The stub never validates or forwards tokens. Streaming, background mode and tools
are unsupported. SSM bodies are limited to 64 KiB; Responses JSON to 32 MiB,
including base64 content. Invalid config fails before binding without logging values.
Omitting `--ssm-parameters` gives an empty store; omitting `--openai-fixtures`
disables the reservation. An empty fixture list enables it with no matches.
Keep local config under ignored `.supercloudy/`; use only synthetic values and
credential value `test`, including in ignored files.

Responses faults belong to the fixtures config and preserve the AWS fault format:

```json
{"fixtures":[{"format_name":"example_extraction","output":{"items":[]}}],"faults":[{"format_name":"example_extraction","first_request":1,"repeat":2,"effect":"throttle"}]}
```

Effects are `throttle` (429), `internal_server_error` (500), and `incomplete`
(200, status incomplete, empty output, reason max_output_tokens). Optional
`format_name` selects one format; omission selects all matched fixtures. Only
requests passing auth, size, validation and fixture lookup advance counters.
Library callers use `server::router_with_services(Config, ServiceConfig)`;
existing router functions and Config literals retain their defaults.

## Local Firebase ID tokens

Start an issuer with an optional default project:

```sh
cargo run -- up --port 8000 --firebase-project-id synthetic-project
```

`--firebase` enables it without a default (supply `project_id` when minting).
`--firebase-cert-max-age 60` sets the certificate cache lifetime, default 3600
seconds. Both configuration flags also enable the service. Library callers set
`ServiceConfig.firebase = Some(server::FirebaseConfig { project_id:
Some("synthetic-project".into()), ..Default::default() })`.

Point your client's **certificate URL** setting at:

```text
http://127.0.0.1:8000/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com
```

Keep its expected project/audience `synthetic-project` and issuer
`https://securetoken.google.com/synthetic-project`. Use normal RS256 verification
with a certificate selected by token kid. The endpoint returns Google's JSON
`{kid: PEM certificate}` shape and `Cache-Control: public, max-age=N`.
If a client has no certificate URL setting, it needs that configuration seam.

Mint a synthetic ID token (the response contains a bearer token; keep it out of
logs). This example prints only token length and public metadata:

```sh
curl -sS http://127.0.0.1:8000/mint \
  -H 'Content-Type: application/json' \
  -d '{"sub":"synthetic-user","email":"user@example.test","email_verified":true,"extra_claims":{"role":"reader"}}' \
  | jq '{token_length:(.id_token|length),project_id,kid,iat,exp}'
```

The JSON response has `id_token`, `project_id`, `kid`, `iat`, and `exp`. Pass
`id_token` to the client's existing ID token authentication flow. `sub` and
`email` are required; `email_verified` defaults to true. Supply `project_id` to
override the configured project. `extra_claims` cannot replace identity/issuer
claims. Optional `ttl` is positive seconds (default 3600); `iat` and `exp` are
unsigned Unix seconds. `ttl` and `exp` cannot be combined. Mint bodies are limited
to 64 KiB; malformed/invalid requests return 400 and oversized bodies return 413.

Add `"variant":"expired"`, `"variant":"bad_kid"` or `"variant":"wrong_aud"`
to mint tokens rejected for expiry, kid lookup or audience. The expired variant
is one hour past expiry (beyond a 60-second verifier leeway) and cannot combine
with time overrides. `"kid":"chosen-header-kid"` changes only the JWT header;
it cannot combine with `bad_kid`. Explicit time overrides can also produce
invalid tokens. `email_verified:false` supports verification rejection tests.

RSA keys and self-signed certificates are generated in memory at startup and
never persisted. Restarting changes the key and kid; clear a client's certificate
cache after restart. There is no OAuth flow, refresh token or outbound Google
request. Use synthetic users only.

When enabled, only the exact base-host `GET` certificate path and `POST /mint`
are reserved. AWS targets/signing scopes and bucket hosts still win. Query
parameters do not change reservations; wrong methods, trailing slashes and
percent-encoded paths retain S3 ownership. Omitting all Firebase flags disables
both reservations. See [the architecture contract](docs/ARCHITECTURE.md).

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

Behavior tests use `aws-sdk-dynamodb`, `aws-sdk-s3`, `aws-sdk-ssm`, and reqwest with local endpoints, synthetic
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
