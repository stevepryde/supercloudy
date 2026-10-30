# Repository rules

This is a public repository. Use only synthetic fixtures and the dummy credential
value `test`. Never add real credentials, private code, private data, or internal
agent content. Do not copy code or data from other projects.

Keep the emulator independent of its consumers. Follow
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for scope and protocol contracts.
The DynamoDB behavior milestone is implemented in memory.

All crates inherit the workspace MIT license. Do not commit plaintext environment
files or credential files. Do not deploy, force-push, or terminate unrelated
processes. Do not commit or push unless the user explicitly requests it.

## Required checks

Run from the repository root before handing off changes:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
scripts/secret-scan.sh
```

Tests must exercise observable behavior. Keep SDK fixtures synthetic, use dummy
credentials `test`, and direct SDK requests only to the local emulator. Do not
require cloud accounts, Docker, or Python for the Rust behavioral suite.

