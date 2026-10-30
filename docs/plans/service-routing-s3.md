# Service routing and in-memory S3

Status: implemented design contract. Validation evidence belongs in the change review; the consumer throwaway runs are separate acceptance work.

## Purpose and scope

One listener must serve the existing DynamoDB implementation and scoped S3 operations through ordinary AWS SDK clients. State remains local to one router instance, in memory, with no cloud calls, signature authentication, persistence, or account/region isolation.

The S3 operation set is CreateBucket, PutObject, GetObject, HeadObject, DeleteObject, CopyObject, and ListObjectsV2. Multipart uploads, ACLs, versioning, bucket policy/lifecycle APIs, standalone object-tagging operations, SSM, Lambda, and plain HTTP stub execution remain excluded. Their routing extension points require no service registry, plugin trait, or backend abstraction.

Existing DynamoDB behavior remains governed by [ARCHITECTURE.md](../ARCHITECTURE.md). Implementation must update its DynamoDB-only scope/routing sections and the README with the supported surface, bucket flag, addressing examples and limitations. The README and architecture contract describe the implemented surface.

## 1. Construction and ownership

1. `server::router()` and `router_with_faults(FaultConfig)` must retain their signatures and isolated-instance behavior. Their routers expose both services; default S3 state contains no buckets.
2. Add `server::Config` containing fault configuration and initial bucket names, and `router_with_config(Config) -> Result<Router, ConfigError>`. Configuration errors occur before listening. Repeatable `up --bucket <name>` supplies those buckets; repeated names create one bucket. No new config-file format or environment-variable seam is needed.
3. DynamoDB and S3 own separate stores and locks. S3 uses an ordered bucket/object map behind one `Arc<RwLock<_>>`. Each object stores immutable bytes, content type, user metadata, tags, checksum information, ETag, modification time, and optional declared SSE-KMS attributes.
4. Put, Copy, and Delete publish atomically. Copy reads its source and replaces its destination under the same S3 write guard, including copies between buckets. Validation and injected errors precede mutation. Locks release before serialization or network I/O.

Keep modules concrete:

| Boundary | Responsibility |
| --- | --- |
| `server` | Configuration, shared state, single request entry point |
| `routing` | Classify headers/URI into a concrete route |
| existing `protocol` | Preserve DynamoDB JSON envelopes |
| `s3` | Operation selection, validation, storage, execution |
| `s3/http` | URI/header decoding, bounded body and aws-chunked decoding |
| `s3/protocol` | S3 XML and HTTP responses |
| shared `faults` | Rule validation, matching, ordered counters and effects |

A small `Route` enum and explicit `match` suffice. S3 domain errors and stored objects must not depend on Axum. Dependencies added during implementation should be limited to protocol necessities: XML parsing/serialization, percent encoding, date handling, and checksum algorithms; SDK and reqwest clients remain dev dependencies. Prefer established crates to handwritten XML or cryptographic primitives; select versions against the repository toolchain and MIT licensing policy.

## 2. Deterministic routing

Routing must inspect the original method, URI, authority/Host, headers, and query before consuming the body. Apply these rules in order:

1. **Any `X-Amz-Target` owns target dispatch.** `DynamoDB_20120810.<operation>` uses the existing selector. Unknown targets/operations and wrong methods/paths retain HTTP 400 JSON `UnknownOperationException`, before body decoding. Other hints cannot reinterpret these requests as S3.
2. **Without a target, parse SigV4 service scope.** Read `Credential` from `Authorization: AWS4-HMAC-SHA256 ...` or once-decoded `X-Amz-Credential`. Service `s3` selects S3. Service `dynamodb` selects DynamoDB, which still requires its target. Other services are unsupported and cannot fall through into S3.
3. **Without a service scope, a bucket authority selects S3.** Support `<bucket>.localhost[:port]`, including dotted bucket names. Normalize hostname case and strip the port using an authority parser. Arbitrary dotted hosts and IP addresses are not bucket names. No wildcard DNS server or AWS hostname rewriting is introduced.
4. **Otherwise a non-root path selects path-style S3.** A root request without an S3 signal retains the existing unknown-operation response. In particular, unsigned `POST /` does not become an S3 operation.

Target precedence preserves current DynamoDB behavior even when another hint disagrees. Without a target, conflicting header/query credential scopes, malformed explicit SigV4 scopes, or ambiguous duplicate routing fields return HTTP 400 without execution. Use S3 `InvalidRequest` when an S3 hint identifies the protocol; otherwise use the existing unknown-operation envelope. Credentials are routing information only and must never be logged. Non-SigV4 authorization is not interpreted as a service scope.

S3 operation selection follows service selection. Unsupported S3 methods/subresources return S3 errors and cannot fall through to another service. Dispatch depends on method, bucket/key presence and operation selectors, never on whether a bucket currently exists.

Host determines addressing: `bucket.localhost/key` means bucket `bucket`, key `key`; `/another/key` on that host means key `another/key`. On `localhost` or a loopback IP, `/bucket/key` is path-style. Bucket roots accept `/bucket` and `/bucket/`. Use HTTP authority where available, otherwise Host; conflicting supplied authorities are invalid.

Preserve the raw path until bucket/key parsing. Split the path-style bucket delimiter before decoding its components. Percent-decode once, preserve literal `+`, repeated slashes, and key case; never apply filesystem normalization. Reject malformed escapes or invalid UTF-8. Decode URI query values once with literal `+` preserved. Tagging uses its own form-encoding parser.

**Extension seam:** future SSM adds the known JSON target prefix `AmazonSSM.` at target dispatch. Future Lambda adds its signing scope and exact REST patterns such as `/2015-03-31/functions/.../invocations`. Future explicitly configured HTTP stubs match exact method/path after AWS hints and before the weak path-style fallback. Explicitly signed S3 requests and bucket hosts retain object-path ownership, even when a key resembles another service's path. No future handlers or stub configuration are implemented now.

## 3. Buckets and object operations

Buckets must exist before object operations. Missing buckets return `NoSuchBucket`; absent object reads return `NoSuchKey`. Startup initialization and CreateBucket share ordinary general-purpose bucket-name validation. Access-point ARNs and directory buckets are outside scope.

CreateBucket accepts an empty body or conventional `CreateBucketConfiguration` XML with a location constraint. Location does not partition state. It returns HTTP 200 with `Location`; recreating an existing bucket succeeds without clearing objects. This is a documented single-owner emulator simplification, consistent with the default region's existing-bucket success behavior; it does not simulate regional redirects or ownership conflicts. [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)

| Operation | Request and observable response |
| --- | --- |
| PutObject | `PUT` object without CopySource; replace bytes and attributes, HTTP 200, quoted ETag and applicable checksum/SSE headers |
| GetObject | `GET` object; HTTP 200 and exact bytes, content length/type, ETag, Last-Modified and metadata |
| HeadObject | Same object headers as a full GET, HTTP 200, no body |
| DeleteObject | `DELETE` object; HTTP 204, no body; absent key in an existing bucket succeeds |
| CopyObject | `PUT` object with `x-amz-copy-source`; HTTP 200 XML `CopyObjectResult` with ETag and LastModified |
| ListObjectsV2 | `GET` bucket with `list-type=2`; HTTP 200 XML `ListBucketResult` |

Default content type is `application/octet-stream`. User metadata uses `x-amz-meta-*`, with case-insensitive header names. Unmodeled transport/client headers may be ignored; unsupported selectors and behavior-changing S3 options fail explicitly. In particular, recognize subresources before ordinary object operations so `?uploads`, `?uploadId`, `?tagging`, or `?versionId` cannot accidentally mutate an object.

ETags are quoted lowercase MD5 digests of stored bytes for this single-part emulator, including simulated SSE. This is an emulator convention, not a claim about real SSE-KMS ETags. Modification times change on successful Put/Copy.

GetObject must support one closed, open-ended, or suffix byte range. `bytes=0-4` returns five bytes; `bytes=0-11` returns twelve when available. Clamp an excessive end to the last byte. Return HTTP 206, selected Content-Length, `Content-Range: bytes start-end/total`, and `Accept-Ranges: bytes`. Unsatisfiable ranges, including any range on an empty object, return 416 `InvalidRange` and `Content-Range: bytes */total`. Malformed or multiple ranges return 400 `InvalidArgument`. Do not attach a full-object checksum to a partial response. [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

HEAD responses, including errors, have no body; successful HEAD preserves object Content-Length. Tests must expect status-based SDK HEAD errors, not an XML `NoSuchKey` body. Range on HEAD and conditional request headers are outside the required call surface and return explicit `NotImplemented` instead of being silently ignored. No condition evaluator is added now; if conditional requests are subsequently included, false supported conditions must return 412 `PreconditionFailed` before mutation. [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

## 4. Copy, metadata, tags and SSE

CopySource accepts slash-preserving and fully percent-encoded bucket/key forms, with an optional leading slash. Separate any raw query selector first, then decode the path once **before** splitting bucket from key. Never decode the extracted key again. Thus `%252F` remains a literal `%2F` key fragment, `%2F` becomes a slash, and encoded `?` remains key data. Reject version selectors and unsupported source forms.

`x-amz-metadata-directive` defaults to `COPY`, copying metadata and content type. `REPLACE` uses only destination metadata and content type, with the ordinary default when omitted. Tagging is independent: `x-amz-tagging-directive` defaults to `COPY`; `REPLACE` uses the supplied set or clears it when absent. Invalid directives leave the destination unchanged. A missing source bucket/key produces the corresponding S3 error. [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)

PutObject accepts `x-amz-tagging` as form-encoded pairs: `+` becomes space and percent escapes decode once. Store the parsed set; reject malformed encoding, duplicate/empty keys, or more than ten tags. GET/HEAD expose `x-amz-tagging-count` for nonempty sets, **not** an invented tag-value response header. Exact tag values require GetObjectTagging, which remains excluded. Test parsing and copy/replacement of tag values at the domain boundary; test counts and copy effects through HTTP. [HeadObject tag metadata](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

Put/Copy accept `x-amz-server-side-encryption: aws:kms` and optional `x-amz-server-side-encryption-aws-kms-key-id`. Preserve and echo supplied declarations on write responses and GET/HEAD. No encryption, KMS lookup, key validation, default key generation, or authorization occurs. Copy applies destination encryption declarations independently of source metadata. Other SSE modes are unsupported.

## 5. Checksums and request bodies

Support explicit SHA256 and CRC32 checksums. SHA256 fulfills the requested surface; CRC32 supports ordinary current Rust SDK uploads, whose default must stay enabled in compatibility tests. Values use base64; CRC32 uses a four-byte big-endian digest. [SDK data integrity defaults](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html)

Recognize `x-amz-sdk-checksum-algorithm`, `x-amz-checksum-sha256`, `x-amz-checksum-crc32`, and announced trailers. Validate actual decoded bytes before mutation. Malformed checksum encoding returns `InvalidDigest`, a digest mismatch returns `BadDigest`, and inconsistent declarations return `InvalidRequest`. Unsupported algorithms fail explicitly. Store accepted algorithm/value; do not invent a checksum when none was requested. Copy preserves source checksum unless a supported destination `x-amz-checksum-algorithm` requests recomputation; include the resulting checksum in its XML response. GET/HEAD with `x-amz-checksum-mode: ENABLED` return the stored full-object checksum. [S3 upload integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)

HTTP transfer framing belongs to the HTTP stack. `Content-Encoding: aws-chunked` is separate payload encoding and must be decoded before storage/hashing. Support the SDK's signed/unsigned streaming markers, chunk extensions, terminal chunk, and announced checksum trailers; parse but do not authenticate chunk signatures. Validate decoded length and reject malformed/truncated framing or missing trailers. Strip only `aws-chunked` from stored representation encoding. Explicit `Content-MD5`, if supplied, must be validated against decoded bytes rather than ignored. [PutObject integrity](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)

The existing 32 MiB collected-wire-body limit remains the bounded input limit; framing consumes part of it. S3 overflow returns `EntityTooLarge`, without mutation. DynamoDB retains its current limit and distinct BatchWriteItem/serialization errors. A premature body termination or declared decoded-length mismatch fails without publication. `x-amz-content-sha256` is signing/transport information, separate from the S3 object checksum.

## 6. Listing and continuation

ListObjectsV2 filters by decoded prefix and orders keys by UTF-8 bytes. Default page size is 1,000; cap larger `max-keys` at 1,000, accept zero as an empty nontruncated result, and reject malformed/negative values. Include Name, Prefix, MaxKeys, KeyCount, IsTruncated and Contents entries with Key, LastModified, ETag, Size and StorageClass `STANDARD`. Echo supplied ContinuationToken and return NextContinuationToken only when more matching keys exist. [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)

Use an opaque, versioned base64url cursor containing bucket, prefix, and last emitted key. Reject malformed tokens or reuse with another bucket/prefix. Resume strictly after the cursor key even if it was deleted. Pages have coherent reads but no cross-page snapshot. Tokens are neither authentication nor persistence guarantees; no cursor registry or signing key is required.

Support `encoding-type=url`, percent-encoding returned key/prefix fields and echoing EncodingType while leaving continuation tokens opaque. Reject unsupported listing options, including delimiter and start-after. More than 1,000 objects must paginate without duplicates/omissions in an unchanged bucket.

## 7. Presigned requests

Accept SDK-generated PUT/GET presigned URLs, including 900-second expiry, through ordinary HTTP clients. Recognize algorithm, credential, date, expiry, signed-headers, signature and optional security-token query parameters; they do not select another object operation.

Any SigV4 presign marker requires the complete conventional query shape, preventing partial auth queries from bypassing expiry handling. Require `AWS4-HMAC-SHA256`, UTC `YYYYMMDDTHHMMSSZ`, and expiry `1..=604800`. Reject at or after expiry with HTTP 403 `AccessDenied`; malformed fields return HTTP 400 `InvalidArgument`. Evaluate expiry once at receipt, before reading an upload body. Signatures, credentials, signed-header contents and IAM are not authenticated. No clock-skew or temporary-credential policy is simulated. [SigV4 query parameters](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)

Normalize supported S3 operation parameters whether the SDK leaves them in headers or hoists them into the presigned query, including checksums, metadata, tagging and SSE declarations. Decode once and reject conflicting duplicates; do not interpret arbitrary query keys as headers. The client's presigned request supplies required headers as well as the URI. Declared content length/type are processed as normal request fields, without pretending the signature protects them.

GET `response-content-disposition` overrides that response's Content-Disposition after one URI decode, without mutating the object. Reject values invalid for an HTTP header. Test a generated URL with a filename requiring percent encoding.

## 8. Protocol and deterministic faults

Use real REST/XML shapes: `CopyObjectResult` and `ListBucketResult` use the S3 namespace `http://s3.amazonaws.com/doc/2006-03-01/`; timestamps use UTC ISO 8601 in XML and HTTP dates in headers. Put/Create/Delete success bodies are empty. XML `<Error>` contains Code, Message, Resource and RequestId; serializers must escape key/message text. Include `application/xml` on XML bodies and `x-amz-request-id`/`x-amz-id-2` on S3 responses. Do not echo auth queries in Resource/errors. Downloads use stored Content-Type. Preserve DynamoDB JSON namespaces, status mappings and request IDs.

Use 404 `NoSuchBucket`/`NoSuchKey`, 400 validation/checksum errors, 416 `InvalidRange`, 501 `NotImplemented` for excluded S3 operations/options, 503 `SlowDown` for injected throttle, and 500 `InternalError` for injected internal failure. HEAD suppresses error bodies. Error compatibility concerns code/status/effect, not incidental message text or request-ID spelling.

Extend fault rules with optional `service` defaulting to `dynamodb`, and optional S3 `bucket`/`key` selectors. Existing JSON files retain their meaning and existing server reexports remain available. Reject cross-service selectors/effects at startup. S3 supports only `throttle` and `internal_server_error`; Copy selectors identify the destination.

Reuse one ordered scheduling/counter implementation with service adapters; no independent S3 scheduler. Move DynamoDB-specific table extraction, transaction eligibility, batch handling and error mapping into its adapter while preserving their behavior. Fully validated matching requests advance all matching counters; the first eligible rule wins. Failed checksum/range/presign/domain validation consumes no rule. Retries count as new requests. S3 faults precede mutation, and services cannot consume each other's rules. Release the short fault-counter lock before any await; lock ordering stays service store then fault counters.

## 9. Acceptance and delivery boundaries

All integration checks use real random-port loopback listeners, explicit SDK endpoint/region configuration, synthetic fixtures and credentials `test`. No cloud, Docker or Python is required. Follow the ownership/cleanup pattern in `tests/sdk.rs`.

- Run the unchanged DynamoDB suite against the shared router, including target-before-body errors and both services interleaved on one listener.
- Use `aws-sdk-s3` for CreateBucket, CLI/config precreated buckets, object replacement/deletion, metadata/content type, both required ranges plus unsatisfiable/empty ranges, COPY/REPLACE, tag counts, SSE echo, explicit SHA256, default CRC32 and checksum-enabled reads. Consume downloaded bodies so SDK validation executes. Check modeled missing-object errors and missing buckets.
- Verify both explicit path style and SDK-default addressing with `force_path_style` unset. Use `http://localhost:<port>` for virtual hosting and a test connector/DNS override mapping the synthetic `<bucket>.localhost` to the actual loopback listener, preserving Host. reqwest Host tests complement, rather than replace, SDK endpoint-resolution tests. Also exercise the IP endpoint's actual SDK behavior. OS wildcard localhost resolution must not be assumed.
- Exercise actual SDK aws-chunked uploads and controlled malformed framing/checksum requests. Rejected overwrites must preserve prior bytes and attributes. Check bad checksums against known independent digest vectors, not a duplicate implementation.
- Use reqwest for addressing/routing conflicts, XML escaping/errors, HEAD's empty body, and SDK-generated presigned PUT then GET with all required headers. Include checksum/metadata/tag/SSE presigned uploads, response disposition, and an expired URL generated with past signing time; no sleep is needed.
- Copy sources cover retained slashes, encoded key slashes, fully encoded bucket/key, spaces, `+`, Unicode and literal percent escapes. Assert destination bytes/type/metadata and unchanged source, not decoder internals.
- List at least 1,005 matching objects plus other prefixes. Traverse default SDK pages and smaller MaxKeys pages, test zero/empty results, invalid/reused cursors and deletion of a cursor key between pages.
- Exercise old and S3 fault rules together: invalid requests before eligible ones, SDK retry success, isolated instances and unchanged objects after injected errors. Existing DynamoDB fault semantics must remain green.
- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and `scripts/secret-scan.sh` from the repository root.

Tests compare observable bytes, HTTP status, SDK results and resulting state with the requirements or published semantics. Do not validate by scanning source text, mirroring implementation calculations, or asserting declared constants. Exact tag-value parsing/copy checks belong at the domain boundary because scoped HTTP operations expose only counts.

Separate consumer acceptance (outside this implementation run) includes throwaway local image promotion (presign PUT, Head, range sniff, Copy, Delete) and document upload/Head checksum-SSE checks through representative consumers. Record only sanitized outcomes in the implementation review; never commit consumer code, names, configuration, or payloads. This design does not claim those runs occurred or authorize consumer changes.

Keep fixtures inline/in memory where possible. No new fixture/state directory is required; any introduced local fixture/state directory must receive an appropriate `.gitignore` guard, and secret scanning must still include ignored files. Never copy private data or real credentials. The implementation must document that virtual-host routing cannot fix a host resolver that cannot reach `<bucket>.localhost`; any consumer fallback is a separate decision based on observed SDK/resolver behavior.

Preserve SDK defaults while testing; do not disable checksum features or force path style globally to manufacture compatibility. Local consumer workflows remain separate evidence and must not be inferred from emulator tests.
