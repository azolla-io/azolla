# Azolla

## Project Overview

Azolla is a reliable, high-performance async task orchestration platform built in Rust. It delivers enterprise-grade task orchestration with breakthrough simplicity, achieving sub-millisecond latency and 10K+ tasks/sec throughput while running on just PostgreSQL—no complex infrastructure required.

**Core Architecture**: Azolla implements a distributed architecture consisting of three main components:

- **Orchestrator**: The central coordinator that interacts with clients, manages task scheduling, and interacts with Shepherds
- **Shepherd**: Worker daemon that manages life cycle of Worker processes
- **Worker**: Individual task execution processes spawned by Shepherds

## Project Structure

### Overview

```
azolla/
├── src/
│   ├── bin/              # Executable binaries
│   ├── orchestrator/     # Orchestrator components
│   ├── shepherd/         # Shepherd and Worker components
│   ├── lib.rs           # Core library
│   └── proto.rs         # Protocol definitions
├── clients/             # Client libraries for different languages
│   ├── README.md        # Developer guide for client library development
│   ├── rust/            # Rust client implementation
│   │   ├── azolla-client/   # Main client library crate
│   │   ├── azolla-macros/   # Procedural macro support crate
│   │   └── release.sh       # Automated release script for crates.io
│   └── python/          # Python client implementation (see clients/AGENTS.md)
├── config/              # Configuration files
├── examples/            # Usage examples for client libraries  
├── proto/               # gRPC protocol definitions
├── tests/               # Tests
│   ├── integrations/    # Integration tests
└── migrations/          # Database schema migrations
```

### Major Components

#### Orchestrator (`src/orchestrator/`)
*Central coordinator and task scheduler*

- **main.rs**: Application entry point and server setup
- **client_service.rs**: Public API for task submission (`ClientService`)
- **cluster_service.rs**: Internal Shepherd communication (`ClusterService`)
- **event_stream.rs**: Efficient append-only event persistence with PostgreSQL
- **taskset.rs**: High-performance in-memory task scheduling (TaskSet™)
- **shepherd_manager.rs**: Manages shepherd life cycle and task dispatching
- **engine.rs**: Grouping core components of Orchestrator
- **scheduler.rs**: Task/workflow scheduler
- **db.rs**: Database operations and connection management

#### Shepherd (`src/shepherd/`)
*Worker daemon that executes tasks with concurrency control*

- **mod.rs**: Main shepherd module and instance management
- **config.rs**: Configuration loading and validation
- **task_manager.rs**: Manages concurrent task execution
- **stream_handler.rs**: Maintains connection with orchestrator
- **worker_service.rs**: Spawns and manages worker processes

#### Binaries (`src/bin/`)
*Executable components*

- **azolla-shepherd.rs**: Shepherd daemon entry point
- **azolla-worker.rs**: Example Rust-based Worker implementation
- **benchmark.rs**: Performance testing utilities
- **merge_events.rs**: Event processing utilities

#### Protocol Definitions (`proto/`)
*gRPC service definitions*

- **orchestrator.proto**: Client and cluster service definitions
- **shepherd.proto**: Worker service definitions  
- **common.proto**: Shared message types

#### Client Libraries (`clients/`)
*Multi-language client libraries for Azolla integration*

- **README.md**: Comprehensive developer guide for client library development, publishing, and maintenance
- **rust/**: Complete Rust client implementation with two-crate architecture
  - **azolla-client/**: Main client library with gRPC client, retry policies, and optional proc macro support
  - **azolla-macros/**: Procedural macro crate providing `#[azolla_task]` attribute for type-safe task definitions
  - **release.sh**: Automated script for coordinated publishing to crates.io with version validation and testing
- **python/**: Async Python client implementation; see `clients/AGENTS.md` for development guidance

## Build and Test Commands

### Development Setup
```bash
# Start PostgreSQL database for development
make dev-up

# Build the application
make build
# or
cargo build

# Build for release
cargo build --release
```

### Running the Application
```bash
# Start orchestrator (terminal 1)
cargo run --bin azolla-orchestrator

# Start shepherd (terminal 2) 
cargo run --bin azolla-shepherd
```

### Docker Commands
```bash
# Development database only
make dev-up    # Start PostgreSQL for local testing
make dev-down  # Stop PostgreSQL for local testing
make dev-clean # Stop and remove all containers/volumes
```

### Utility Commands
```bash
# Clean build artifacts
make clean
# or
cargo clean

# Merge events (data processing)
cargo run --release --bin merge_events
```

## Code Style Guidelines

### Rust-Specific Rules

**IMPORTANT**: Always use inline format arguments in Rust print statements to avoid clippy errors.

#### Format String Usage Rules

❌ **Never use old-style format:**
```rust
println!("Value: {}", variable);
println!("User {} has {} items", name, count);
format!("Error: {}", error_msg);
```

✅ **Always use inline format arguments:**
```rust
println!("Value: {variable}");
println!("User {name} has {count} items");
format!("Error: {error_msg}");
```

This prevents the clippy error: `variables can be used directly in the format! string`

#### Apply to All Rust Format Macros
This rule applies to:
- `println!`, `print!`
- `format!`, `format_args!`
- `write!`, `writeln!`
- `panic!`, `assert!`, etc.

## Testing Instructions

### Test Organization

The test suite is organized into several categories. For any Rust-based test
invocation below, run with `RUSTFLAGS=-D warnings` to mirror CI's "warnings as
errors" configuration:

```bash
RUSTFLAGS=-D warnings cargo test ...
```

**Runtime cap:** keep every test command under 60 seconds by calling it through `timeout 60s <command>` (or `gtimeout 60s` on macOS). This prevents agents from waiting indefinitely on stuck tests.

#### Unit Tests
Located within individual modules using `#[cfg(test)]` blocks:
```bash
# Run unit tests only (CI parity)
RUSTFLAGS=-D warnings cargo test --lib
```

#### Integration Tests
Located in `tests/integration/`:
- `task_execution.rs`: End-to-end task execution scenarios
- `shepherd_management.rs`: Shepherd lifecycle and management
- `life_cycle.rs`: Complete system lifecycle tests
- `retry_mechanism.rs`: Error handling and retry logic
 - `group_routing.rs`: Domain shepherd-group routing behavior

```bash
# Run integration tests (sequentially) with feature flag
# Always wrap test invocations with a 60s timeout to avoid hangs
# Linux:
RUSTFLAGS=-D warnings timeout 60s cargo test --features test-harness -- --test-threads=1

# macOS (coreutils):
RUSTFLAGS=-D warnings gtimeout 60s cargo test --features test-harness -- --test-threads=1

# Or run the integration test harness binary explicitly
RUSTFLAGS=-D warnings timeout 60s cargo test --test integration_tests_main --features test-harness -- --test-threads=1
```

#### Rust Client Tests
Located in `clients/rust/azolla-client/`:
- **Unit tests**: Library tests in `src/` modules
- **Integration tests**: End-to-end tests in `tests/integration/`
- **Unit test suite**: Organized tests in `tests/unit/`

```bash
# Run all Rust client tests (must run sequentially to avoid port conflicts)
RUSTFLAGS=-D warnings cargo test -- --test-threads=1

# Note: See "Pre-Commit Validation" section below for strict CI-equivalent linting

# Run tests with all features enabled (including macros)
RUSTFLAGS=-D warnings cargo test --all-features -- --test-threads=1

# Run only unit tests (fast)
RUSTFLAGS=-D warnings cargo test --lib

# Run only integration tests (requires orchestrator startup)
RUSTFLAGS=-D warnings cargo test --test integration_tests -- --test-threads=1

# Run only unit test suite
RUSTFLAGS=-D warnings cargo test --test unit_tests

# Note: See "Pre-Commit Validation" section below for complete CI-equivalent commands
```

**Important Notes for Rust Client Tests:**
- **Sequential execution required**: Integration tests must use `--test-threads=1` to prevent port conflicts
- **Feature flags**: Some tests require `--all-features` to enable macro support
- **Strict linting**: CI uses `-D warnings` to treat all warnings as errors
- **Orchestrator dependency**: Integration tests automatically start/stop orchestrator processes

#### Pre-Commit Validation (CI-Equivalent)
To ensure your changes pass CI, run these commands before committing:

```bash
# STEP 1: Auto-format code (fixes formatting issues)
cargo fmt --all
cd clients/rust/azolla-macros && cargo fmt --all
cd clients/rust/azolla-client && cargo fmt --all
cd ../../../  # Return to project root

# STEP 2: Verify formatting matches CI exactly (strict check - must pass)
cargo fmt --all -- --check
cd clients/rust/azolla-macros && cargo fmt --all -- --check
cd clients/rust/azolla-client && cargo fmt --all -- --check
cd ../../../  # Return to project root

# STEP 2.5: Check for compilation errors with all features
cargo check --all-targets --all-features

# STEP 3: Run strict linting (CI equivalent - treats warnings as errors)
cargo clippy --all-targets --all-features -- -D warnings

# STEP 4: Run client crate linting (CI equivalent)
cd clients/rust/azolla-macros && cargo clippy --all-targets --all-features -- -D warnings
cd clients/rust/azolla-client && cargo clippy --all-targets --all-features -- -D warnings
cd ../../../  # Return to project root

# STEP 5: Run all tests sequentially (required for integration tests)
cargo test --all-features -- --test-threads=1

# STEP 6: Verify macro functionality specifically
cargo test --features macros -- --test-threads=1
```

**CI Strictness:**
- **Formatting enforcement**: CI uses `cargo fmt --check` which fails if any code is not perfectly formatted
- **Warnings as errors**: `-D warnings` flag treats all warnings as compilation errors
- **All features tested**: CI runs with `--all-features` to test macro functionality
- **Clippy rules**: Stricter clippy rules than default local setup
- **Feature gates**: Code using `azolla_task` must be behind `#[cfg(feature = "macros")]`
- **Client crate isolation**: Each client crate (azolla-client, azolla-macros) has separate formatting/linting checks

### Test Environment Setup

#### Database Requirements
Most integration tests require PostgreSQL:
```bash
# Start testing PostgreSQL database
make dev-up

# Run tests with database (e.g. `cargo test`)
```

#### Environment Variables
```bash
# Set database URL for tests
export DATABASE_URL="postgresql://localhost:5432/azolla"

# Enable test harness features
cargo test --features test-harness
```


For language-specific workflows—including Python client development and testing—refer to `clients/AGENTS.md`.

### Test Best Practices

#### Writing New Tests
1. **Document test purpose**: Each test should have a clear comment explaining what it tests
2. **Test expected behavior**: Describe what should happen when the test passes
3. **Use descriptive names**: Test names should indicate what functionality is being validated
4. **Clean up resources**: Ensure tests clean up any created resources
5. **Use appropriate test categories**: Unit tests for logic, integration tests for system behavior

### Separate Test Pipelines

The project uses separate CI pipelines for different components:

#### Rust Client Tests (`rust-client-tests` job)
- **Location**: `clients/rust/azolla-client` and `clients/rust/azolla-macros`
- **Coverage requirement**: 80% minimum
- **Runs**: Formatting, linting, unit tests, integration tests, and coverage analysis
- **Parallel execution**: Runs in parallel with main test suite
- **Triggers**: Changes to `clients/rust/**` paths

#### Main Application Tests (`test-and-coverage` job)
- **Location**: Core Azolla application (excluding client crates)
- **Coverage requirement**: 64% minimum
- **Excludes**: `azolla-client` and `azolla-macros` crates to avoid duplication
- **Includes**: PostgreSQL integration tests with test harness

### Integration Test Patterns (test_harness.rs)

Use the IntegrationTestEnvironment for end-to-end tests:

- Ensure worker binary is available when tasks execute real workers:
  - `harness.ensure_worker_binary().await?`
- Start shepherds via harness and wait for registration:
  - `let _shepherd = harness.start_shepherd().await?;`
  - `harness.wait_for_shepherd_registration(uuid, Duration::from_secs(5))`
- Query task and attempt status via harness helpers:
  - `get_task_status`, `get_task_attempts`
- Query event metadata for routing/dispatch assertions:
  - `wait_for_attempt_started_metadata(&task_id, domain, Duration::from_secs(5))`

Timeouts:
- Do not add outer timeouts in test code. Instead, always run integration tests with the `timeout 60s` (or `gtimeout 60s`) wrapper noted above to enforce the global 60-second cap.
- New integration tests should be designed to complete well within 30 seconds.

Execution:
- Run with `--features test-harness` and `--test-threads=1` to avoid port and container contention.

## Commit Instructions

This command performs an intelligent git commit by analyzing changes and following best practices.

### Instructions

1. **Analyze current state**: Run `git status` and `git diff` to understand all changes
2. **Remove unnecessary comments**: Make sure no new comments are added for simple logic or self-explanatory code.
3. **Stage relevant files**: Add untracked and modified files that should be committed
4. **Linting**: Follow the "Pre-Commit Validation (CI-Equivalent)" section above for complete commands
5. **Generate commit message**: Create a commit message with a concise one-sentence title describing the main change. No other information should be added to the commit message.
6. **Create commit**: Create the commit, never use the `--no-verify` option to bypass hooks and handle all pre-commit failures.

### Git Commit Message Standards
- **Never make a git commit or push without my confirmation**
- **Never add Claude Code promotion message**: Do not include any promotional text about Claude Code in commit messages
- **Follow conventional commit format**: Use the format `<type>: <description>` as a single-line title
- **Supported types**: fix, feat, build, chore, ci, docs, style, refactor, perf, test, and others
- **Focus on the most impactful changes, ignore changes that are easy to understand or self-explanatory**
- **ALWAYS ask for user confirmation before committing**
- Wait for explicit approval ("yes", "commit", "proceed", etc.) before executing commit
