# supercloudy architecture

Status: in-memory DynamoDB behavior milestone.

## Scope

supercloudy is an independent Rust process serving a subset of DynamoDB's real
JSON wire protocol. Consumers use ordinary AWS SDKs against a local endpoint.
There are no consumer dependencies, persistent storage, cloud calls, or cloud
credentials. Fixtures and examples are synthetic; dummy credentials are `test`.

The `supercloudy up --port <u16>` CLI binds `127.0.0.1`, defaults to port 8000,
prints its actual address, and serves until interrupted. Port 0 selects a random
available port. Bind errors exit nonzero.

## Modules

One workspace package, `crates/supercloudy`, exposes a binary and server library.
Every crate inherits the workspace MIT license. Cargo.lock is retained.

| Module | Responsibility |
| --- | --- |
| `main` | CLI, loopback listener, Ctrl-C shutdown |
| `server` | HTTP routing, target selection, bounded body collection |
| `protocol` | AWS JSON response envelope and request IDs |
| `dynamodb/api` | Request decoding structures |
| `dynamodb/value` | Tagged AttributeValues, exact decimals, scalar key ordering |
| `dynamodb/expressions` | Tokenizer, parser, condition/update evaluation, paths |
| `dynamodb/storage` | Table schemas, ordered items, metadata |
| `dynamodb/operations` | Table, item, and Query behavior |
| `dynamodb/error` | Domain errors |

Domain code does not depend on Axum. There is no backend trait, mock interface,
service registry, or persistence adapter. Runtime dependencies are Axum, Tokio,
Clap, Serde/serde_json, base64, and BigDecimal. The AWS SDK is a dev dependency.
`dynamodb/reserved.txt` contains DynamoDB's public
[reserved word list](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html).

## HTTP and encoding

Requests use `POST /`, `Content-Type: application/x-amz-json-1.0`, and
`X-Amz-Target: DynamoDB_20120810.<Operation>`. Target selection precedes body
decoding. Missing/unknown targets, wrong paths/methods, and unimplemented
operations return HTTP 400 `UnknownOperationException` with namespace
`com.amazon.coral.service#`. Supported targets decode JSON; malformed bodies
return `SerializationException`, and invalid or unsupported domain inputs return
`ValidationException`.

All responses have AWS JSON 1.0 content type and a process-local
`x-amzn-RequestId`. Errors contain `__type` and `message`; modeled DynamoDB errors
use `com.amazonaws.dynamodb.v20120810#`. Missing tables, duplicate tables, and
failed conditions map to `ResourceNotFoundException`, `ResourceInUseException`,
and `ConditionalCheckFailedException`, respectively (HTTP 400). Error compatibility
compares codes/effects, not incidental message text or request ID spelling.
See [AWS wire encoding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html)
and [errors](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html).

The server does not authenticate signatures, read credentials, or isolate
accounts/regions. One router instance owns one namespace. Dummy client signing
is accepted. Table ARN metadata uses a synthetic account.

AttributeValues require exactly one tag: `S`, `N`, `B`, `BOOL`, `NULL`, `SS`, `NS`,
`BS`, `L`, or `M`. Binary values decode from base64 to bytes. Null is distinct from
missing. Sets reject empty or semantically duplicate members. Maps and lists
preserve nested types. Numbers use arbitrary precision BigDecimal with 38-digit
precision and nonzero magnitudes from 1E-130 through 9.999…E125; arithmetic is
exact and validated before commit. Numeric equality normalizes spelling.

Primary keys have declared type `S`, `N`, or `B`, with an optional sort key.
Structured key tuples avoid concatenation collisions. Ordering is UTF-8 byte
order for strings, unsigned byte order for binary, mathematical order for numbers.
See [AttributeValue](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html)
and [DynamoDB constraints](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html).

## Operations and expressions

CreateTable validates simple/composite schemas; tables become active immediately.
DescribeTable returns metadata. DeleteTable removes a table and its items atomically.
GetItem omits `Item` for a missing item. PutItem replaces the complete item.
UpdateItem can create an absent item using its supplied key. DeleteItem of an
absent item succeeds unless a condition fails.

The tokenizer and recursive descent parser share paths and operands, with
separate condition, update, and key-condition validation. Unsupported syntax
fails explicitly. Aliases resolve to literal attribute segments: `#a` mapped to
`a.b` denotes one name; `#a.#b` is nested. Missing/unused aliases and values,
reserved unaliased words, invalid types, overlapping update paths, and key
mutations are rejected.

Conditions support comparisons, inclusive `BETWEEN`, `IN`, `AND`, `OR`, `NOT`,
parentheses, `attribute_exists`, `attribute_not_exists`, `attribute_type`,
`begins_with`, `contains`, and `size`. Missing paths are explicit evaluator state.
See [condition grammar](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html).

Updates support `SET`, `REMOVE`, `ADD`, `DELETE`, numeric arithmetic,
`if_not_exists`, `list_append`, and document paths. All right-hand operands use
the pre-update item. A separate candidate is built, validated, then committed.
List removals use original indices. See
[AWS update semantics](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html).

ReturnValues supports `NONE` (default) and `ALL_OLD` for PutItem/DeleteItem;
UpdateItem also supports `ALL_NEW`, `UPDATED_OLD`, and `UPDATED_NEW`.
`ReturnValuesOnConditionCheckFailure` is excluded.
UPDATED modes select attributes whose exact touched path exists in the chosen
image; a selected nested path returns its whole top-level document. Condition
functions treat nonmatching stored types as false or undefined, while invalid
literal operand types are rejected. String `size` follows UTF-16 code units.

Query requires partition-key equality and optionally a sort-key comparison
(`=`, `<`, `<=`, `>`, `>=`), inclusive `BETWEEN`, or string/binary `begins_with`.
Non-key references and general condition predicates are invalid. Results sort
ascending by key, reversed for `ScanIndexForward=false`. Positive `Limit`,
`ExclusiveStartKey`, `LastEvaluatedKey`, and `Select=COUNT` are supported.
Continuation compares keys even after deletion of the last item in an earlier
page. A supplied cursor must satisfy the key condition. Reaching an explicit
Limit returns a cursor even if the next page will be empty.
Without filters, Count and ScannedCount agree. Each page has a coherent
read, with no snapshot across pages. The 1 MiB page boundary is not implemented.
See [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html).

## Atomicity

An `Arc<tokio::sync::RwLock<Database>>` owns tables directly. Each table owns schema
metadata and a `BTreeMap<KeyTuple, Item>`. One write guard covers table lookup,
condition evaluation, candidate construction, validation, and commit, including
DeleteTable. Failed conditions and invalid updates leave state untouched.
Concurrent conditional updates expecting the same old value have one winner.
Reads clone response data under a read guard. Guards release before serialization
and network I/O. All reads see committed state immediately.

## Exclusions and validation

No persistence, indexes, transactions, batch APIs, Scan, streams, TTL, IAM,
capacity simulation, or account separation. Filters, projections, legacy request
forms (`Expected`, `AttributeUpdates`, `KeyConditions`), and other unsupported
options return ValidationException rather than changing results silently.
Table byte-size/capacity statistics are not simulated.

Required checks are listed in [AGENTS.md](../AGENTS.md). Integration scenarios use
Rust `aws-sdk-dynamodb` against independent real loopback listeners on random
ports, with explicit endpoint_url and dummy credentials. They exercise every
operation, typed values, conditions and error decoding, nested updates,
pre-update operands, numeric identity/order, range/prefix queries, pagination,
failed-write atomicity, and concurrent conditional writes. Raw HTTP checks cover
protocol envelopes. No test uses a cloud account, Docker, or Python.

Expected behavior comes from AWS semantics and independently observable stored
state, not implementation-derived constants. An external reference is optional
only for a concrete compatibility question; normal cargo test is self-contained.
The secret scanner checks the working tree, index, and available Git history,
including ignored credential files, and redacts matching content.
