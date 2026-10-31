# SSM GetParameter and OpenAI Responses stub

Status: implemented contract; related contracts: [architecture](../ARCHITECTURE.md), [routing](service-routing-s3.md).
Scope: immutable synthetic parameters and Responses fixtures on the existing listener;
no cloud calls, real credentials, persistence, consumer schemas, or consumer code changes.

## 1. Ownership and routing

Classify from headers, authority, method and raw URI **before reading any body**.
Extend concrete `Route`/`Services`; preserve existing DynamoDB/S3 behavior in this order:

1. Any `X-Amz-Target` owns target dispatch. Recognize the exact family prefixes
   `AmazonSSM.` and `DynamoDB_20120810.` before checking supported operations.
   Each family owns unknown operations and wrong method/path errors, even on bucket
   hosts or with conflicting signing hints. Both accept operations only at `POST /`.
   Duplicate/malformed targets and unknown families retain the existing generic
   HTTP 400 `UnknownOperationException`; never fall through to REST/body decoding.
2. Without a target, explicit SigV4 scope owns dispatch: `s3`, `ssm`, `dynamodb`.
   SSM without its target returns its own unknown-operation error. Preserve rejection
   of malformed/conflicting scopes and unsupported services; bearer auth is not SigV4.
3. A `<bucket>.localhost` authority owns every S3 object path.
4. Lambda's documented reservation stays inactive until its implementation.
   When OpenAI fixtures are configured, reserve exactly `POST /v1/responses` on the
   shared base host, before weak path-style S3. Ignore query parameters for matching.
   No prefix, percent-decoded, trailing-slash or other-method reservation is implied.
5. Remaining non-root paths retain path-style S3; unsigned root retains its error.

Thus an unsigned base-host `POST /v1/responses` collides with bucket `v1`, key
`responses`, only while the stub is enabled. Explicit S3 signing or bucket-host
addressing retains S3 ownership. GET/PUT `/v1/responses` remain S3 operations.
The architecture reservation table records this enablement rule and precedence.
Bucket `v1` is below S3's minimum name length; use a valid bucket host to store
service-looking keys. Explicit S3 signing preserves XML errors on the exact pair.

## 2. Construction and configuration

Private `ssm` and `openai` services have their own HTTP/protocol adapters and
immutable per-instance data. Clones share handles; no outer service lock is needed.
Existing router entry points and `server::Config` literals use empty parameters
and a disabled stub. `router_with_services(Config, ServiceConfig)` adds the new
configuration; `Some(OpenAiConfig)` enables the stub, even with no fixtures.
Load configuration once before binding; reject bad JSON, unknown config fields,
duplicate parameter names, invalid types and invalid fault ordinals without logging values.

`up --ssm-parameters <json-file>` loads this format (absent means an empty map):

```json
{"parameters":[{"name":"/example/provider-key","type":"SecureString","value":"test","encrypted_value":"synthetic-ciphertext"}]}
```

Types are `String`, `StringList`, `SecureString`; `value` is a string, including a
comma-separated StringList. SecureString requires `encrypted_value`; other types
reject it. This is synthetic ciphertext supplied by the fixture, not encryption.
Version is 1; no writes, history, labels, ARN lookup or KMS calls.
Keep temporary local config under ignored `.supercloudy/`. Reviewed synthetic
examples live under `crates/supercloudy/examples/`;
guard any additional local fixture/state directory before introducing it. Never log bodies
or authorization; all credential examples and SDK signing/bearer values must be `test`.

## 3. SSM wire contract

`AmazonSSM.GetParameter` uses AWS JSON 1.1 (`application/x-amz-json-1.1`) and
`x-amzn-RequestId`, including errors; do not reuse DynamoDB's namespace/JSON 1.0 codec.
Require string `Name` and optional boolean `WithDecryption` (default false).
Trim surrounding spaces; reject empty/interior-space names, unsupported selectors
(ARNs, labels, versions), names longer than 2048 bytes and characters outside
ASCII letters/digits and `/_.-` with HTTP 400 `ValidationException`.
Malformed JSON returns `SerializationException`;
unknown operations or wrong method/path return HTTP 400 `UnknownOperationException`.
Success is HTTP 200 with `{"Parameter":{"Name":"/example/provider-key",
"Type":"SecureString","Value":"test","Version":1,"DataType":"text"}}`.
For SecureString, true selects `value`; false/omitted selects `encrypted_value`.
For String/StringList the flag has no effect. No claim of encryption or IAM parity.
Missing names return HTTP 400 `{"__type":"ParameterNotFound","message":"Parameter not found"}`;
use unqualified SSM error codes and test modeled SDK decoding, not incidental prose.
Protocol basis: [GetParameter](https://docs.aws.amazon.com/systems-manager/latest/APIReference/API_GetParameter.html).

## 4. Generic Responses fixtures and envelope

`up --openai-fixtures <json-file>` enables the stub, including an empty fixture list:

```json
{"fixtures":[{"format_name":"example_extraction","input_contains":"sample","output":{"items":["synthetic"]}}],"faults":[{"format_name":"example_extraction","first_request":1,"repeat":1,"effect":"incomplete"}]}
```

`faults` defaults to empty; `input_contains` is optional and must be nonempty if set.
First matching fixture in file order wins: exact `text.format.name`, then optional
case-sensitive substring within any input text string (string `input` or individual
`input[].content[]` entries with `type:input_text`, or message content strings). Never search serialized JSON,
schema text, image URLs or file bytes; substring matching does not cross text entries.
Require a JSON object, nonempty model, supported input, and `text.format` with
`type:json_schema`, nonempty name and object schema. Accept strict, store:false,
max_output_tokens and reasoning; accept input_image/data-URL and input_file/PDF blocks
as opaque input, without fetching or decoding. Reject stream:true, background:true or nonempty tools
explicitly with HTTP 400. No-match is HTTP 400 with code `fixture_not_found`.
Fixtures supply arbitrary JSON `output`, serialized once into an output text string;
schema execution and consumer parser acceptance belong in consumer tests, not this repo.

Without AWS selectors, defer non-SigV4 Authorization validation to the selected service,
including duplicate bearer headers; OpenAI requires one nonempty bearer before collection.
Missing/empty/wrong-scheme/duplicate bearer headers return HTTP 401:
`{"error":{"message":"Bearer token required","type":"authentication_error","param":null,"code":"invalid_api_key"}}`.
Presence only is checked; no real key validation. All stub responses use JSON and a
fresh `x-request-id`, including auth, parse, size, no-match and injected errors.
Other request errors use the same error object with `type:invalid_request_error`.

Success is HTTP 200: `id:resp_<id>`, `object:response`, numeric `created_at`,
`status:completed`, requested `model`, `error:null`, `incomplete_details:null`,
and exactly one output message with `id:msg_<id>`, `type:message`, `role:assistant`,
`status:completed`, `content:[{"type":"output_text","text":"<serialized output>","annotations":[]}]`.
No storage or token accounting. References: [Responses](https://developers.openai.com/api/reference/python/resources/responses/methods/create), [auth/request IDs](https://developers.openai.com/api/reference/overview).

## 5. Fault and body boundaries

Per-service rule validation, matching, effects and rendering live in `<service>/faults.rs`.
Shared `faults.rs` keeps only ordered scheduling, ordinal validation and counters.
Existing config/schema plumbing is in `fault_config.rs` behind unchanged re-exports,
with service validation assembled outside the scheduler. **FaultRule/FaultEffect compatibility
redesign is deferred**: retain existing fields, variants, constructors and `--faults` JSON.
OpenAI's service-local `faults` array above avoids extending those public types.
Its optional `format_name` selects a schema; absent selects all matched fixtures.
Effects: `throttle` returns 429 (`rate_limit_error`/`rate_limit_exceeded`),
`internal_server_error` returns 500 (`server_error`/`internal_server_error`), using the
error envelope above; `incomplete` returns HTTP 200, `status:incomplete`, `output:[]`,
`error:null`, `incomplete_details:{"reason":"max_output_tokens"}` in the Responses envelope.
No SSM fault modes are required in this slice; any later SSM rules belong in `ssm/faults.rs`.
Reuse scheduling mechanics with per-service/per-instance counters. Positive, non-overflowing
ordinals, all matching counters advance, first eligible wins, retries count anew.
Select only after auth, body bounds, decoding, request validation and successful fixture
lookup; failures consume no rule. Release scheduler locks before constructing responses.

SSM collects at most 64 KiB; overflow is HTTP 400 `SerializationException`.
OpenAI collects at most 32 MiB of JSON wire bytes, including base64 input; overflow is
HTTP 413 `invalid_request_error`/`request_too_large`. Count actual collected bytes even
without Content-Length; no decompression. These are emulator limits, not provider claims.
Ownership/operation rejection precedes collection; collection precedes faults and locks.
Retain DynamoDB's 32 MiB and S3's 65 MiB wire/64 MiB decoded limits unchanged.

## 6. Validation boundaries

Rust AWS SDK and reqwest tests run against random-port loopback listeners using
synthetic fixtures and dummy credentials `test`. They exercise modeled SSM errors,
all parameter types/decryption modes, mixed-service precedence, raw paths,
unfinished rejected bodies, fixture order/text matching, request IDs, fault recovery,
counter isolation and actual body limits with and without Content-Length.

S3 decoded-length cases use valid framing to isolate length mismatches. Oversized
writes preserve stored bytes/metadata and the next matching request's fault.
The full DynamoDB/S3 suites remain required, alongside formatting, Clippy and the
secret scanner. Consumer parser/schema acceptance is a separate consumer-side check.
