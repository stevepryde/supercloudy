# supercloudy

An in-memory DynamoDB emulator speaking the real AWS JSON 1.0 protocol. Use normal
AWS SDK clients against a local endpoint. No cloud account or disk storage is needed.

```sh
cargo run -- up --port 8000
```

Or install the CLI:

```sh
cargo install --path crates/supercloudy
supercloudy up --port 8000
```

The server binds to `127.0.0.1`. `--port 0` selects an available port and prints
its address. Stop with Ctrl-C; all tables and items disappear on exit.

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

## Deterministic faults

Ordinary startup is fault-free. Explicitly load ordered rules to exercise retries:

```sh
supercloudy up --port 8137 --faults rules.json
```

```json
{"rules":[{"operation":"PutItem","table":"synthetic-items","first_request":1,"repeat":2,"effect":{"type":"throttle"}}]}
```

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
cargo clippy --all-targets -- -D warnings
cargo test
scripts/secret-scan.sh
```

Behavior tests use `aws-sdk-dynamodb` with explicit local endpoints, synthetic
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
