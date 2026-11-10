# supercloudy

An in-memory DynamoDB, S3, SSM, Lambda Invoke, MongoDB and Qdrant emulator with local OpenAI Responses and Firebase ID token stubs.
Use ordinary SDKs and HTTP clients against loopback endpoints. No cloud
account is needed. Lambda Invoke can forward to explicitly configured loopback HTTP targets; other services make no outbound network calls.

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
Lambda paths, `POST /v1/responses` and Firebase certificate/mint reservations, then non-root
path-style S3. SSM target errors
use JSON 1.1; DynamoDB retains JSON 1.0. Unknown operations stay with their service.
Malformed/conflicting signing scopes fail before execution. GET/PUT, encoded or
trailing-slash Responses paths remain S3. Explicit S3 signing and valid bucket
hosts retain S3 ownership of service-looking paths. The full reservation table,
including Lambda paths, is in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

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
The stub never validates or forwards tokens. `stream: true` returns Responses SSE;
background mode is unsupported. Structured fixtures do not accept tools; the
conversational fixtures below support canned function calls. SSM bodies are limited to 64 KiB; Responses JSON to 32 MiB,
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

### Local Codex and conversational SSE

The same endpoint supports deterministic text and function-call fixtures, including
real `codex app-server` clients. No model or account is needed:

```sh
cargo run -- up --port 8000 \
  --openai-fixtures crates/supercloudy/examples/codex-fixtures.json
```

The additive `conversation_fixtures` list is used when `text.format` is absent or
`{"type":"text"}`. It may coexist with structured `fixtures`:

```json
{
  "conversation_fixtures": [
    {"input_contains":"Calculate six times seven","function_call":{"name":"calculator","arguments":"{\"left\":6,\"right\":7}"}},
    {"function_output_contains":"42","output_text":"The result is 42."},
    {"input_contains":"Say hello","output_text":"Hello from the local fixture."}
  ]
}
```

Each fixture has exactly one output: `output_text` or `function_call`, whose
`arguments` is a JSON-object string. It may have at most one selector:

- `input_contains` searches text blocks in the most recent user message.
- `function_output_contains` searches the most recent function result, provided
  no newer user message exists. String outputs and `input_text` arrays work.
- Neither selector means a fallback for user input, not for function results.

First matching fixture wins. Earlier turns, assistant messages, instructions,
tool definitions, and opaque image/file fields cannot select a conversational
fixture. This prevents a completed call from replaying when its result is sent,
and allows the same prompt to make a fresh call after a thread is resumed.
`reasoning` and `additional_tools` history items are accepted as opaque metadata.
Tool declarations are accepted but not executed or checked against fixture names.
Your app-server client must register and answer any configured dynamic tools.

For a CLI text-only smoke, use an isolated home and a synthetic provider:

```sh
local_home=$(mktemp -d)
mkdir -p "$local_home/codex" "$local_home/workspace"
env -i PATH="$PATH" HOME="$local_home" CODEX_HOME="$local_home/codex" \
  SUPERCLOUDY_API_KEY=test codex exec --skip-git-repo-check --ephemeral \
  -C "$local_home/workspace" -s read-only \
  -c 'model="synthetic-model"' \
  -c 'model_provider="supercloudy"' \
  -c 'model_providers.supercloudy.name="supercloudy"' \
  -c 'model_providers.supercloudy.base_url="http://127.0.0.1:8000/v1"' \
  -c 'model_providers.supercloudy.wire_api="responses"' \
  -c 'model_providers.supercloudy.env_key="SUPERCLOUDY_API_KEY"' \
  -c 'model_providers.supercloudy.requires_openai_auth=false' \
  -c 'cli_auth_credentials_store="file"' \
  -c 'analytics.enabled=false' \
  'Say hello'
```

Expected final text: `Hello from the local fixture.` The temporary directory is
safe to remove when finished. Use the same provider settings with `codex
app-server --listen stdio://` for app-server integrations. Only the loopback
`/v1/responses` HTTP endpoint is implemented; WebSocket transport, compaction,
model discovery and hosted tools are not emulated.

SSE includes response creation, item/content start and completion, text or
function-argument deltas, and a final `response.completed` (or injected
`response.incomplete`). Events are buffered, with no timing/tokenization
simulation; usage counts are synthetic zeros. The `incomplete` fault preserves
SSE transport. Throttle/server faults return ordinary HTTP 429/500 JSON errors.
Faults without `format_name` apply to both structured and conversational fixtures;
named faults apply only to the corresponding structured format.

Run the optional real-binary compatibility test separately from the ordinary
Rust suite. It starts its own loopback emulator, clears inherited environment,
creates a temporary empty home, executes a calculator tool, then restarts
app-server and resumes the thread for another tool round trip:

```sh
cargo test --test codex_app_server -- --ignored --nocapture
# Or choose an installed binary explicitly:
CODEX_BIN=/path/to/codex cargo test --test codex_app_server -- --ignored --nocapture
```

Verified with `codex-cli 0.163.0`. The optional test is ignored by default because
the normal suite must not require Codex. It does not install or download Codex.

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

## Local Lambda Invoke

Load a function map with `--lambda-functions <json-file>`:

```json
{
  "functions": {
    "synthetic-echo": {"url":"http://127.0.0.1:9001/invoke"},
    "synthetic-canned": {"response":"{\"ok\":true}"},
    "synthetic-failure": {"response":"{\"errorMessage\":\"sample failure\"}","function_error":"Unhandled"}
  },
  "max_concurrency":16,
  "shutdown_timeout_ms":5000,
  "faults":[]
}
```

```sh
supercloudy up --port 8137 --lambda-functions .supercloudy/functions.json
```

Use an ordinary Lambda SDK with the emulator endpoint and credentials `test`.
`POST /2015-03-31/functions/{name}/invocations` forwards the raw body by HTTP POST
as `application/octet-stream`; the target must accept raw bytes. URLs must be
loopback HTTP, without credentials or fragments. Proxies and redirects are
disabled. Each forward has a 30-second timeout (5-second connect timeout).
Canned `response` strings are returned as literal UTF-8 bytes.

`RequestResponse` (default) returns 200 and the target body. A target
`X-Amz-Function-Error` is preserved; a non-2xx target response supplies `Unhandled`
when the header is absent. These are handler failures, still HTTP 200. Connection,
timeout, truncated/oversized response failures are Invoke API errors (502
`ServiceException`), without a function-error header. Unknown names return 404
`ResourceNotFoundException`. `DryRun` returns 204 without contacting the target.
This follows the [Invoke API status/function-error distinction](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html).

`Event` returns 202 only after reserving a concurrency slot and registering work
in the runtime-owned task set. It returns before the target completes; disconnecting
the caller does not cancel accepted work. The shared concurrency bound covers
both invocation types; saturation returns 429 `TooManyRequestsException` without
acceptance. There is no waiting queue, persistence or async retry simulation.
Async handler/transport failures have no result channel and are not retried.
RequestResponse payloads and all handler responses are limited to 6 MiB; Event
payloads to 1 MiB. Bodies remain opaque, including non-JSON bytes. Query options,
version/alias resolution, logs, IAM and management APIs are unsupported; map exact
function names (including qualified names if needed).

Ctrl-C or SIGTERM stops Lambda admission and drains accepted Events for
`shutdown_timeout_ms` (default 5000) while the shared HTTP listener remains live
for their DynamoDB/S3 calls. Remaining tasks are cancelled and joined, then HTTP
acceptance stops and in-flight HTTP requests finish; state is discarded. Library hosts should keep
`server::runtime_with_services(Config, ServiceConfig)` alive, serve
`runtime.router()`, and call `runtime.shutdown().await` before closing the shared
HTTP listener.
`ServiceConfig.lambda = Some(...)` enables the base-host path reservation; a
Lambda SigV4 scope always retains Lambda ownership. The router-only constructors
remain available, but dropping a router cancels its owned work without a drain.

Invoke faults use the same ordered scheduler as the other services. Configure
`faults` in the function-map file, for example:

```json
{"functions":{"synthetic-worker":{"response":"{}"}},"faults":[{"function_name":"synthetic-worker","first_request":1,"repeat":1,"effect":"throttle"}]}
```

Optional `function_name` selects one function. Effects are `throttle` (429),
`service_error` (500 `ServiceException`), and `function_error` (200 plus
`Unhandled`, or an asynchronously discarded handler failure for Event). Counters
advance after function/type/body validation and capacity reservation; unknown
names, DryRun and capacity rejection do not consume them. First eligible rule
wins, all matching counters advance, and instances are isolated. Use ignored
`.supercloudy/` for local configuration and synthetic fixtures only.

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

## MongoDB and Qdrant

Enable either database independently, alongside the existing HTTP services:

```sh
supercloudy up --port 8000 --mongodb-port 27017 --qdrant-port 6333
```

MongoDB clients connect to `mongodb://127.0.0.1:27017/?directConnection=true`.
Qdrant HTTP clients use `http://127.0.0.1:6333`. No real credentials are needed.
Each database has its own listener and in-memory state; Qdrant paths do not
interfere with S3 paths. Use port `0` to allocate a free port. Startup prints all
enabled addresses only after their listeners bind successfully. Ctrl-C or SIGTERM
stops the process and discards its data.

Rust tests can start an isolated service directly without a subprocess:

```rust,no_run
# async fn example() -> std::io::Result<()> {
let mongo = supercloudy::mongodb::start(0).await?;
let mongo_uri = format!("mongodb://{}/?directConnection=true", mongo.address());
let qdrant = supercloudy::qdrant::start(0).await?;
let qdrant_url = format!("http://{}", qdrant.address());
// Configure ordinary database clients with these endpoints.
mongo.shutdown().await?;
qdrant.shutdown().await?;
# Ok(())
# }
```

Keep the server handle alive for the fixture lifetime. Dropping it cancels the
owned server. Services do not share state between instances. The supported
operations and deliberate differences from production servers are documented in
[the database compatibility contract](docs/plans/database-emulators.md) and
[the architecture reference](docs/ARCHITECTURE.md#database-services).

### Real-server conformance

Ordinary `cargo test` needs no Docker. Explicit conformance targets use
Testcontainers to run synthetic scenarios against both Supercloudy and pinned
real servers:

```sh
cargo test --locked --test mongodb_conformance -- --ignored
cargo test --locked --test qdrant_conformance -- --ignored
```

These commands require Docker and permission to pull the pinned images. Missing
Docker or failed server startup fails the run. Tests own and clean up their
containers; there are no cloud accounts or shared production databases. CI runs
these targets on pull requests, main pushes, version tags, and scheduled/manual
runs, separately from the Docker-free default suite.

MongoDB uses `mongo:8.0.28` as a single-node replica set, with its TTL monitor
interval shortened to one second for the expiration scenario. Qdrant uses
`qdrant/qdrant:v1.18.3-unprivileged`. Compatibility is measured against those
versions. Passing conformance covers the exercised API contract; it does not
establish parity for distributed operation, durability, approximate search,
performance, or unimplemented APIs.

## Development

Use current stable Rust (validated with 1.99.1), rustfmt, Clippy, Bash, Git, find,
and awk. Run from the repository root:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
scripts/secret-scan.sh
```

Behavior tests use `aws-sdk-dynamodb`, `aws-sdk-s3`, `aws-sdk-ssm`, `aws-sdk-lambda`, and reqwest with local endpoints, synthetic
fixtures, and credentials `test`. Each test spawns its own server on a random
loopback port. No cloud account, Docker, or Python is required.

Property tests use `proptest` as a development dependency. DynamoDB and S3
operation sequences run through Rust SDK clients on real loopback listeners and
compare responses and stored state with plain in-test maps. In-process properties
exercise private expression/XML/JSON parsers and database snapshot/lock behavior.
The snapshot properties race transactional writers and readers on separate runtime
workers; each page must contain one committed revision. A Tokio deadline bounds
the snapshot race. XML parser properties run in supervised child processes so a
synchronous parser loop fails the test; their deadlines scale with the case count.

SDK sequence/range and lock properties default to 32 cases each; parser properties
default to 96. Each suite has a fixed RNG seed for reproducible ordinary runs.
For a deeper run, or a different reproducible seed:

```sh
PROPTEST_CASES=1000 cargo test --lib --test dynamodb_properties --test s3_properties
PROPTEST_CASES=1000 PROPTEST_RNG_SEED=208 cargo test --lib --test dynamodb_properties --test s3_properties
```

Failures shrink to a smaller input and persist seeds in
`crates/supercloudy/proptest-regressions/` or the corresponding
`tests/*.proptest-regressions` file. Keep regression seeds when fixing a real bug.
Generated parser inputs include structured nesting as well as arbitrary text and
bytes; the mixed NOT/grouping regression covers a previously aborting stack overflow.

### Mutation testing

Install the pinned tool and run the baseline plus mutation tests:

```sh
cargo install cargo-mutants --version 26.2.0 --locked
bash scripts/mutants.sh
```

The runner uses the complete workspace test suite, including SDK and property
tests. It defaults to four mutation workers sharing cargo-mutants' CPU jobserver;
use `-j N` or `CARGO_MUTANTS_JOBS=N` to choose another count.
Test timeouts adapt to five times the measured baseline with a 240-second
minimum for scheduling variation. There are no repository-specific exclusions.
Results stay in a printed temporary directory outside the checkout because the
secret scanner also examines ignored files and mutation logs contain altered
synthetic fixtures. Use `--output /tmp/supercloudy-mutants` to retain a chosen
location outside the checkout.

Extra cargo-mutants arguments pass through, including focused runs:

```sh
scripts/mutants.sh --shard 1/4 -j 2
git diff > /tmp/supercloudy-changes.diff
scripts/mutants.sh --in-diff /tmp/supercloudy-changes.diff
scripts/mutants.sh --list
```

Unexplained survivors fail the runner. Prefer a behavioral regression test; for
an equivalent mutation or a documented scope boundary, add the exact line from
`mutants.out/missed.txt`, a literal tab, and a specific reason to
[`mutants-survivors.txt`](mutants-survivors.txt). Listed mutants still run.
Baseline failures, timeouts, and tool errors retain their failing exit status.
Remove obsolete list entries when code changes; locations and replacements must
match exactly, so an old exception cannot permit a different mutant.

In the printed `mutants.out` directory, `caught.txt` lists mutations detected by
failing tests, `missed.txt` lists surviving mutations, `timeout.txt` lists runs
that exceeded the test deadline, and `unviable.txt` lists mutations that did not
compile. `outcomes.json` contains counts and individual outcomes; `log/` and
`diff/` show each run and its code change. Inspect timeouts separately; they
are failures of the runner, not confirmed test kills.

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

## CI

Pushes to `main` and pull requests run formatting, Clippy, tests and the secret
scan. Mutation tests run manually or weekly on Mondays at 03:23 UTC; they report a
notice and skip until `scripts/mutants.sh` is available.

## License

[MIT](LICENSE), Copyright (c) 2026 Steve Pryde.
