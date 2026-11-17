# Decisions

## D-1: Keep custom emulators; do not adopt AWS Emulator as a replacement

Decided 2026-11-17, evaluated against AWS Emulator v1.0.483.

- supercloudy keeps its own DynamoDB, S3, SSM and Lambda Invoke implementations
  and does not depend on, wrap or delegate to AWS Emulator.
- AWS Emulator is not a conformance oracle for supercloudy; real services (or
  recordings of them) remain the reference.
- Consumers that need AWS services supercloudy does not implement may run AWS
  Emulator alongside it via per-service endpoints (`AWS_ENDPOINT_URL_<SERVICE>`).
  supercloudy does not add mocks solely to cover those services.

Rationale: deterministic, ordinal fault injection is supercloudy's core value and
AWS Emulator has none for these services (only time-window FIS faults, wired to
EC2); its fidelity on the operations supercloudy covers is lower (published scores:
DynamoDB 85% behavioral / 65% wire, SSM 51% / 42%); its Lambda runs only in-process
Python/Node packages, not loopback HTTP targets; and it would require Docker or a
Linux-only ~100 GB build, contrary to the Docker-free test suite.

Revisit if AWS Emulator gains deterministic per-request fault injection for these
services.
