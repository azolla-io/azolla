# Azolla Client Libraries - AI Agent Guide

This guide highlights the essentials an AI coding agent needs when working on the Azolla client libraries. Use it as a quick reference and defer to each crate's README or project documentation for deeper detail.

## Layout
- `clients/rust/azolla-client`: primary Rust client crate
- `clients/rust/azolla-macros`: proc-macro crate providing `#[azolla_task]`
- `clients/python`: Python package with async client, worker runtime, and CLI utilities

## Rust Client Overview
The Rust client follows a two-crate architecture so procedural macros remain optional:
- `azolla-macros`: exposes the `azolla_task` attribute and depends on `syn`, `quote`, and `proc-macro2`
- `azolla-client`: async gRPC client that optionally enables macros via the `macros` feature

### Key Modules (`azolla-client/src`)
- `lib.rs`: top-level exports and feature-gated re-exports
- `client.rs`: tonic-based client with connection and retry logic
- `error.rs`: error hierarchy (`AzollaError`, `TaskError`)
- `retry_policy.rs`: retry/backoff strategies
- `task.rs`: task traits and helpers
- `worker.rs`: single-invocation worker runtime reporting back to the shepherd

### Must-Know Practices
- Run `cargo fmt`, `cargo clippy --all-targets --all-features`, and `cargo test --all-features -- --test-threads=1`
- Keep proc-macro functionality behind the `macros` feature and update both crates together when versions change
- Prefer inline format arguments in macros (`format!("Value: {value}")`) to avoid clippy warnings

## Python Client Overview
The Python client provides an asyncio-first SDK with structured modules under `clients/python/src/azolla/`.

### Key Modules
- `client.py`: async gRPC client and connection management
- `worker.py`: worker runtime and task execution helpers
- `task.py`: decorators and task definitions
- `retry.py`: retry policies with backoff strategies
- `types.py` & `exceptions.py`: shared enums, result types, and error hierarchy
- `_grpc/`: generated protobuf bindings (excluded from linting)

### Must-Know Practices
- Activate the repo virtualenv before running tooling (`source ./venv/bin/activate` from the project root)
- Run `ruff check`, `black --check`, `mypy src`, and sequential `pytest` suites as documented in the Python README
- Keep `__all__` exports sorted, maintain full type hints, and never lint generated `_grpc` files

## Shared Workflows
- Follow the project-wide pre-commit checklist (format, clippy/lints, `cargo test --all-features -- --test-threads=1`)
- Client integration tests assume the orchestrator components are available; use OS-level timeouts when invoking them
- When adjusting protocol definitions, update `proto/` in the main project first and regenerate client bindings as needed

## Release & Publishing
- Prefer the language-specific `release.sh` scripts (`clients/rust/release.sh`, `clients/python/release.sh`) for coordinated releases
- Rust crates must publish `azolla-macros` before `azolla-client`, keeping versions in lockstep
- The Python package relies on `_version.py`; ensure the version matches the release argument before publishing

## Protocol Buffer Sync
- Both clients mirror the main project's `.proto` files; keep them aligned before code changes land
- Rust regeneration occurs via `build.rs` automatically during builds
- Python regeneration uses `grpcio-tools`; run the helper script in `clients/python/scripts` when proto files change

## AI Agent Checklist
- Mirror existing patterns; prefer minimal changes outside the target functionality
- Maintain rigorous testing (unit + integration) and document notable behaviors in README files when necessary
- Coordinate version bumps, documentation updates, and proto syncs whenever client APIs change
- Ask for confirmation before creating commits; never bypass project formatting or linting steps
