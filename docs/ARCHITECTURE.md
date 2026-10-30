# supercloudy architecture

Status: in-memory DynamoDB and S3 behavior milestones.

## Scope

supercloudy is an independent Rust process serving subsets of DynamoDB's JSON
wire protocol and S3's REST/XML protocol on one listener. Consumers use ordinary AWS SDKs against a local endpoint.
There are no consumer dependencies, persistent storage, cloud calls, or cloud
credentials. Fixtures and examples are synthetic; dummy credentials are `test`.

The `supercloudy up --port <u16>` CLI binds `127.0.0.1`, defaults to port 8000,
prints its actual address, and serves until interrupted. Repeatable `--bucket`
flags initialize S3 buckets. Port 0 selects a random
available port. Bind errors exit nonzero.

## Modules

One workspace package, `crates/supercloudy`, exposes a binary and server library.
Every crate inherits the workspace MIT license. Cargo.lock is retained.

| Module | Responsibility |
| --- | --- |
| `main` | CLI, loopback listener, Ctrl-C shutdown |
| `server` | Validated configuration, isolated service construction, request entry point |
| `routing` | Target/signing-scope/REST precedence before body consumption |
| `s3` | Atomic in-memory bucket/object operations, ranges, checksums, listing |
| `s3/http` | URI/header decoding, presign expiry, bounded aws-chunked payload decoding |
| `s3/protocol` | REST/XML responses, HTTP metadata and errors |
| `faults` | Shared rule validation, ordered scheduling and per-instance counters |
| `protocol` | AWS JSON response envelope and request IDs |
| `dynamodb/api` | Request decoding structures |
| `dynamodb/value` | Tagged AttributeValues, exact decimals, scalar key ordering |
| `dynamodb/expressions` | Tokenizer, parser, condition/update evaluation, paths |
| `dynamodb/storage` | Table schemas, ordered items, metadata |
| `dynamodb/operations` | Table/item preparation, indexed reads, shared pagination |
| `dynamodb/transactions` | Staged atomic writes, snapshot Gets, successful token records |
| `dynamodb/batches` | Whole-request validation, size limits, unprocessed subsets |
| `dynamodb/faults` | DynamoDB fault eligibility and modeled-error adapter |
| `dynamodb/error` | Domain errors |

Domain code does not depend on Axum. There is no backend trait, mock interface,
service registry, or persistence adapter. Runtime dependencies include Axum, Tokio, Clap, Serde/serde_json, base64,
BigDecimal, quick-xml, time/httpdate, percent-encoding, and checksum algorithms.
AWS SDKs and reqwest are dev dependencies.
`dynamodb/reserved.txt` contains DynamoDB's public
[reserved word list](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html).

## Service routing and S3

Any `X-Amz-Target` owns dispatch before REST hints, preserving DynamoDB protocol
errors. Without it, explicit SigV4 signing scope selects S3 or rejects unsupported
services. A `<bucket>.localhost` authority then selects S3, followed by non-root
path-style requests. Unsigned root requests retain the unknown-operation envelope.
Conflicting or malformed routing information fails before execution. No registry,
plugin trait, or speculative service implementation is introduced.

S3 owns a separate ordered bucket/object map and lock. Put, Copy (including across
buckets), and Delete validate before publication under that lock. Reads clone
committed object data and release guards before XML serialization or network I/O.
Faults share one ordered scheduler while each service owns eligibility and errors.

The detailed S3 protocol contract, addressing, supported operations, checksum and
presign semantics, and extension points are recorded in the retained
[service routing design](plans/service-routing-s3.md). Both services remain entirely
in memory. S3 does not authenticate signatures, perform encryption, or contact KMS.
Virtual-host addressing requires the client's resolver to reach wildcard localhost;
HTTP routing cannot supply DNS.

## HTTP and encoding

DynamoDB requests use `POST /`, `Content-Type: application/x-amz-json-1.0`, and
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
UpdateItem can create an absent item using its supplied key; a REMOVE-only
update leaves an absent item absent. DeleteItem of an
absent item succeeds unless a condition fails. PutItem and UpdateItem reject
candidate items larger than 400 KiB with ValidationException before committing.
Item size counts UTF-8 attribute names and strings, decoded binary bytes, scalar
and set values, and map/list element overhead, rather than JSON wire size.
See [AWS item sizing](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html).

The tokenizer and recursive descent parser share paths and operands, with
separate condition, update, and key-condition validation. Unsupported syntax
fails explicitly. Aliases resolve to literal attribute segments: `#a` mapped to
`a.b` denotes one name; `#a.#b` is nested. Missing/unused aliases and values,
reserved unaliased words, invalid types, overlapping update paths, and key
mutations are rejected. Attribute names cannot be empty, including within maps.
Document paths allow up to 32 dereference operators (dots or list indices),
excluding the root attribute name.

Conditions support comparisons, inclusive `BETWEEN`, `IN`, `AND`, `OR`, `NOT`,
parentheses, `attribute_exists`, `attribute_not_exists`, `attribute_type`,
`begins_with`, `contains`, and `size`. Missing paths are explicit evaluator state.
A missing path compares unequal to a present value with `<>`. Reversed
`BETWEEN` bounds return ValidationException, including on a missing target path.
See [condition grammar](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html).

Updates support `SET`, `REMOVE`, `ADD`, `DELETE`, numeric arithmetic,
`if_not_exists`, `list_append`, and document paths. All right-hand operands use
the pre-update item. A separate candidate is built, validated, then committed.
List removals use original indices. See
[AWS update semantics](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html).
`if_not_exists` can nest inside update functions; `list_append` cannot.

ReturnValues supports `NONE` (default) and `ALL_OLD` for PutItem/DeleteItem;
UpdateItem also supports `ALL_NEW`, `UPDATED_OLD`, and `UPDATED_NEW`.
`ReturnValuesOnConditionCheckFailure` supports `NONE` and `ALL_OLD`; failures
include the previous item when requested.
UPDATED modes select attributes whose exact touched path exists in the chosen
image; nested map paths return only the selected leaves within their enclosing
maps, omitting untouched siblings. Selected list elements are compacted in source
index order, selecting only touched paths within nested maps and lists. Updating
an entire map or list returns its complete value.
Condition functions treat nonmatching stored types as false or undefined, while invalid
literal operand types are rejected. String `size` follows UTF-16 code units.

Query requires partition-key equality and optionally a sort-key comparison
(`=`, `<`, `<=`, `>`, `>=`), inclusive `BETWEEN`, or string/binary `begins_with`.
Non-key references and general condition predicates are invalid. Key comparison
operands can place the key on either side of the comparator. Results sort
ascending by key, reversed for `ScanIndexForward=false`. Positive `Limit`,
`ExclusiveStartKey`, `LastEvaluatedKey`, and `Select=COUNT` are supported.
Continuation compares keys even after deletion of the last item in an earlier
page. A supplied cursor must match the selected partition and key schema. Reaching an explicit
Limit returns a cursor even if the next page will be empty.
Filters run after evaluation and before projection. Count measures matches;
ScannedCount measures evaluated rows. Query and Scan stop at positive Limit or
1 MiB of decoded evaluated data, before filters/projections. An empty filtered
page can have a continuation key. Cursors contain the union of base/index keys
and compare complete ordered tuples even after cursor-row deletion. Each page
has a coherent read, with no snapshot across pages.
See [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html).

## Indexes, scans, and read selection

CreateTable accepts conventional GSIs and LSIs with `ALL`, `KEYS_ONLY`, or
`INCLUDE` projections. Attribute definitions cover exactly the union of key
attributes. LSIs require the table partition key and an alternate sort key.
Index views derive from committed base rows under the read lock; they sort by
(index key, table key), preserving duplicate index values. Missing index keys
make rows sparse; present invalid index keys reject every write path before
mutation. Combined base-item and LSI entry size is bounded at 400 KiB. INCLUDE
allows 1–20 attributes per index and at most 100 across the table.

Index reads default to ALL_PROJECTED_ATTRIBUTES. GSIs reject strong reads and
requests for unprojected attributes; LSIs can fetch requested base attributes
and support strong reads. LSI base fetch page sizing counts complete fetched
items individually rounded to 4 KiB plus aggregate rounded index data. Other
read paths count decoded item/index entry sizes. Select supports COUNT,
SPECIFIC_ATTRIBUTES, and valid ALL selections. Get and transaction/batch Gets
support nested projections with shared alias and path validation.

Scan supports filters over any attribute and needs no key condition. Segment
and TotalSegments must be paired, with total 1..1,000,000 and segment 0..total.
Partition assignment uses FNV-1a over the selected access path partition key:
its scalar tag byte followed by UTF-8 string, canonical decimal string, or raw
binary bytes, modulo total segments. Identical partitions stay together; scans
are disjoint and collectively complete. Assignment does not simulate physical
AWS partitions. A continuation key must belong to the selected segment.

## Atomicity

An `Arc<tokio::sync::RwLock<Database>>` owns tables directly. Each table owns schema
metadata and a `BTreeMap<KeyTuple, Item>`. One write guard covers table lookup,
condition evaluation, candidate construction, validation, and commit, including
DeleteTable. Failed conditions and invalid updates leave state untouched.
Concurrent conditional updates expecting the same old value have one winner.
Reads clone response data under a read guard. Guards release before serialization
and network I/O. All reads see committed state immediately.

## Transactions and batches

TransactWriteItems accepts 1–100 Put/Update/Delete/ConditionCheck actions.
Duplicate normalized table/key targets fail before mutation. Every condition
uses pre-transaction state; only touched candidate items are prepared, validated,
then published under one write guard. TransactionCanceledException contains one
CancellationReasons entry per action (None, ConditionalCheckFailed, or
ValidationError), with old Item when requested. Malformed requests, duplicate
targets, and aggregate sizes exceeding 4 MiB fail with ValidationException.
Successful ClientRequestToken requests replay for ten minutes without reapplying
writes; changed requests return IdempotentParameterMismatchException. Records
use monotonic time, expire lazily, and share the commit guard. No contention is
simulated. TransactGetItems reads 1–100 ordered base-table Gets under one read
guard, preserves empty slots, and enforces a 4 MiB aggregate limit.

BatchGetItem accepts up to 100 distinct keys; BatchWriteItem accepts up to 25
distinct PutRequest/DeleteRequest actions. All request schemas/candidates validate
before processing. BatchGet omits missing rows and preserves per-table read
options in UnprocessedKeys when 16 MiB response or 1 MiB logical-partition limits
are reached. BatchWrite enforces 400 KiB items and a 16 MiB wire request.
Unconstrained calls return empty unprocessed maps. Deferred writes do not undo
processed writes. Requests never require top-level TableName for multi-item APIs.

## Explicit fault configuration

`up --faults <json-file>` and `server::router_with_faults` accept explicit ordered
rules; ordinary CLI/router construction enables none. Each DynamoDB rule selects an
operation, optional table, positive first_request ordinal and repeat count, and
an effect. Counters belong to one instance and advance only after complete
validation; failed conditions do not advance them. All matching counters advance,
and the first eligible rule wins. A transaction cancellation rule is eligible only
when the request contains at least one configured failing action position;
shorter requests neither trigger nor consume that rule. Retries are new requests. Serial isolated
scenarios provide deterministic order.

Effects are modeled ProvisionedThroughputExceededException (HTTP 400),
InternalServerError (HTTP 500), aligned TransactionCanceledException reasons,
and original-position batch deferral. Whole-request errors precede publication;
deferral returns untouched original requests/keys. Incompatible configurations
fail startup. There is no ambient activation, request-header override, admin
endpoint, delay, executable hook, or background scheduler. Structured domain
errors carry status and details directly into the AWS envelope.

## Exclusions and validation

No persistence, streams, TTL, IAM, capacity simulation, account separation,
online index backfill/UpdateTable, multi-attribute GSI schemas, physical partition
simulation, or 10 GiB item-collection simulation. Legacy request forms
(`Expected`, `AttributeUpdates`, `KeyConditions`) and unsupported options return
ValidationException rather than changing results silently.
Table byte-size/capacity statistics are not simulated.

Required checks are listed in [AGENTS.md](../AGENTS.md). Integration scenarios use
Rust `aws-sdk-dynamodb` and `aws-sdk-s3` against independent real loopback listeners on random
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
