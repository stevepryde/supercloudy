# Database emulator contract

MongoDB and Qdrant extend the existing independent, in-memory service emulator.
They are intended for application behavior tests using ordinary database clients.
Compatibility is bounded by the supported operations documented in
`docs/ARCHITECTURE.md`; it does not imply complete product, deployment, performance,
or durability equivalence.

## Requirements

1. Each enabled database must own an isolated store and loopback listener.
   MongoDB uses its binary wire protocol; Qdrant uses a separate HTTP listener so
   its paths cannot change existing S3 routing. Port zero must select an available
   port. Dropping or shutting down an owned server must stop its work.
2. CLI flags `--mongodb-port` and `--qdrant-port` must independently enable the
   services. Omitting them preserves existing behavior. Failed startup must exit
   with an error and release listeners already acquired. Ctrl-C/SIGTERM must shut
   down all enabled services. No database service may make external calls.
3. MongoDB must support normal Rust-driver connections, BSON values, CRUD,
   cursors, the documented query/update/aggregation operators, and index
   constraints. Writes must preserve single-document atomicity. Multi-document
   transactions must provide a snapshot, atomic commit, rollback, and conflict
   detection. Failed uniqueness checks must not publish the failed write.
4. Qdrant must store points and compute results from that state: named dense and
   sparse vectors, cosine similarity, sparse scoring and IDF, documented payload
   filters, query/batch query, retrieval, deletion, and collection/index metadata.
   `qdrant/bm25` text inputs must follow the pinned server's document/query
   tokenization and weighting. No canned query results or consumer-specific logic.
5. Unsupported operations and semantic options must fail explicitly. Metadata
   about supported capabilities must reflect implemented behavior. Authentication,
   distributed replication, durable storage and approximate indexing are outside
   this local emulator contract.
6. The default Rust suite must exercise real loopback protocols without Docker
   or cloud accounts. Synthetic fixtures and dummy credential value `test` are
   mandatory. Testcontainers must be development-only.
7. Explicit conformance suites must run the same independent scenarios against
   the emulator and pinned real MongoDB and Qdrant containers. Docker or startup
   failures must fail a requested run. Comparisons must cover returned values,
   resulting state, error codes, transaction outcomes, concurrent writes, filter
   isolation, and ranking. Normalize only nondeterministic metadata; justify
   floating-point tolerance. Avoid ambiguous ties in ranking comparisons.
8. Ordinary regression tests must exercise observed behavior rather than inspect
   implementation text. Every repaired compatibility defect should have a small
   reproducible behavioral case. Passing conformance demonstrates the exercised
   contract, not every possible behavior of either database.

## Acceptance

- Normal formatting, Clippy, Rust tests and repository secret scan pass.
- Explicit MongoDB and Qdrant conformance targets pass against pinned containers.
- The CLI serves the existing HTTP API and both databases together, and releases
  their ports on termination.
- Supported operations, exclusions, test commands, server versions and numerical
  comparison policy are documented with the implementation.
