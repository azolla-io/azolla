# Python Integration Tests

This directory contains end-to-end integration tests for the Azolla Python client library.

## Test Structure

### Components

1. **Test Worker** (`bin/test_worker.py`)
   - Python implementation that mirrors `src/bin/azolla-worker.rs`
   - Implements test tasks: echo, always_fail, flaky, math_add, count_args
   - Supports both single task execution and continuous service modes

2. **Integration Tests** (`test_e2e_orchestrator.py`)
   - Tests three key scenarios:
     - Task succeeds on first attempt (echo_task)
     - Task succeeds after retries (flaky_task)
     - Task fails after exhausting attempts (always_fail_task)
   - Validates complete integration between Python client, Rust orchestrator, and Python worker

3. **Process Management Utilities** (`utils.py`)
   - Manages orchestrator and worker processes
   - Handles port allocation and health checks
   - Provides logging and cleanup capabilities

## Test Scenarios

### 1. Success on First Attempt
Tests that the echo task returns input unchanged without retries.

### 2. Success After Retry
Tests that the flaky task fails on first attempt but succeeds on retry using file-based state tracking.

### 3. Failure After Exhausting Attempts
Tests that the always_fail task properly fails after all retry attempts are exhausted.

## Requirements

The integration tests require:
- Rust toolchain (to build the orchestrator binary)
- PostgreSQL service running (for orchestrator)
- Python dependencies: `pip install -e ".[dev,testing,integration]"`

## Running Tests

### Local Development
```bash
# Build the orchestrator first
cargo build

# Run integration tests
cd clients/python
pytest tests/integration/ -v
```

### CI/CD
The integration tests run automatically in GitHub Actions with:
- PostgreSQL service container
- Rust orchestrator binary build
- Multi-worker load balancing tests
- Process lifecycle management

## Architecture

```
┌─────────────────┐    ┌───────────────────┐    ┌─────────────────┐
│  Python Client  │───▶│ Rust Orchestrator │◀───│ Python Worker   │
│  (Submit Tasks) │    │   (Manage Tasks)  │    │ (Execute Tasks) │
└─────────────────┘    └───────────────────┘    └─────────────────┘
        │                        │                        │
        └────── Integration Test Environment ──────────────┘
```

The tests validate the complete round-trip communication and ensure compatibility between Python and Rust components.