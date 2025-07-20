# Integration Tests

This directory contains comprehensive end-to-end integration tests for the Azolla orchestrator system.

## Test Organization

### 📋 task_execution.rs
Tests basic task creation, execution, and result handling workflow.

**Coverage:**
- Task creation via ClientService API
- Shepherd registration and task dispatch  
- Worker execution and result reporting
- Task status transitions (CREATED → STARTED → SUCCEEDED)
- Attempt tracking and timing verification

### 🔄 retry_mechanism.rs  
Tests task retry policies, automatic retry execution, and race condition handling.

**Coverage:**
- End-to-end retry execution with configurable delays
- Retry policy enforcement (max_attempts, wait strategies)
- Status transitions during retry lifecycle
- Race condition handling when multiple tasks with different retry timings
- SchedulerActor retry scheduling optimization

### 🐑 shepherd_management.rs
Tests shepherd lifecycle, registration, and multi-shepherd coordination.

**Coverage:**  
- Single shepherd startup and configuration
- Shepherd registration with orchestrator
- Load tracking and capacity management
- Multiple shepherds in cluster configuration
- Resource isolation and port management

## Running Tests

**⚠️ Important:** Integration tests must be run sequentially due to resource contention (ports, database containers):

```bash
# Run all integration tests
cargo test --test integration_tests_main --features test-harness -- --test-threads=1

# Run specific test module
cargo test --features test-harness integration::task_execution --test-threads=1

# Run specific test
cargo test --features test-harness integration::retry_mechanism::test_task_retry_handling
```

## Test Requirements

- **test-harness feature**: Must be enabled for all integration tests
- **PostgreSQL container**: Tests use testcontainers to spin up isolated databases
- **Available ports**: Tests dynamically allocate ports for orchestrator and shepherd services  
- **Worker binary**: `azolla-worker` must be built and available at `./target/debug/azolla-worker`

## Test Architecture

Each test follows this pattern:

1. **Setup**: Create `IntegrationTestEnvironment` with isolated database
2. **Shepherd**: Start and register shepherd instance(s)
3. **Task Creation**: Submit tasks via ClientService gRPC API
4. **Execution**: Wait for task processing and verify behavior
5. **Verification**: Assert on task status, attempts, and system state
6. **Cleanup**: Proper shutdown of all components

## Debugging

Enable debug logging for detailed test output:

```bash
RUST_LOG=debug cargo test --features test-harness integration::retry_mechanism::test_task_retry_handling -- --nocapture
```

Common issues:
- **Port conflicts**: Tests running in parallel - use `--test-threads=1`
- **Database issues**: Ensure Docker is running for testcontainers
- **Worker binary**: Run `cargo build --bin azolla-worker` if missing