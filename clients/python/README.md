# Azolla Python Client

A modern, type-safe Python client library for [Azolla](https://github.com/azolla-io/azolla) distributed task processing.

## Features

- 🚀 **Modern Python**: Built for Python 3.9+ with full type hints and async/await support
- 🔒 **Type Safe**: Powered by Pydantic v2 for automatic validation and IDE support
- 🎯 **Dual Approach**: Choose between convenient decorators or explicit class-based tasks
- ⚡ **High Performance**: Efficient gRPC communication with connection pooling
- 🔄 **Robust Retry Logic**: Configurable retry policies with exponential backoff
- 🎛️ **Production Ready**: Comprehensive logging, monitoring, and error handling

## Installation

```bash
pip install azolla
```

For enhanced performance, install optional dependencies:

```bash
pip install azolla[performance]  # uvloop + orjson for faster execution
pip install azolla[monitoring]   # Prometheus metrics and OpenTelemetry
pip install azolla[all]         # Everything included
```

## Quick Start

### Define Tasks

Choose between two approaches for defining tasks:

#### 🎯 Decorator Approach (Recommended)

```python
from azolla import azolla_task

@azolla_task
async def send_email(to: str, subject: str, body: str) -> dict:
    """Send an email notification."""
    # Your email sending logic here
    return {
        "message_id": "msg_123",
        "sent_to": to,
        "status": "delivered"
    }
```

#### 🏗️ Class Approach (Advanced)

```python
from azolla import Task
from pydantic import BaseModel

class SendEmailTask(Task):
    class Args(BaseModel):
        to: str
        subject: str
        body: str
        
    async def execute(self, args: Args) -> dict:
        # Your email sending logic here
        return {
            "message_id": "msg_123", 
            "sent_to": args.to,
            "status": "delivered"
        }
```

### Submit Tasks (Client)

```python
import asyncio
from azolla import Client
from azolla.retry import RetryPolicy, ExponentialBackoff

async def main():
    # Connect to Azolla orchestrator
    client = await Client.connect("http://localhost:52710")

    async with client:
        
        # Submit task with retry policy
        handle = await (
            client.submit_task(send_email, {
                "to": "user@example.com",
                "subject": "Welcome!",
                "body": "Welcome to our platform!"
            })
            .retry_policy(RetryPolicy(
                max_attempts=3,
                backoff=ExponentialBackoff(initial=1.0)
            ))
            .submit()
        )
        
        # Wait for result
        result = await handle.wait()
        if result.success:
            print(f"✅ Email sent: {result.value}")
        else:
            print(f"❌ Failed: {result.error}")

asyncio.run(main())
```

### Process Tasks (Worker)

```python
import asyncio
from azolla import Worker, WorkerInvocation

async def main():
    worker = Worker.builder().register_task(send_email).build()

    invocation = WorkerInvocation.from_json(
        task_id="demo",
        task_name="send_email",
        args_json='["user@example.com", "Welcome!", "Welcome to our platform!"]',
        kwargs_json='{}',
        shepherd_endpoint="http://127.0.0.1:50052",
    )

    execution = await worker.execute(invocation)
    print(execution.value)

asyncio.run(main())
```

### CLI Worker

You can also run workers from the command line:

```bash
# Start worker and import task modules
azolla-worker \
    --task-id "$TASK_ID" \
    --name "$TASK_NAME" \
    --args '$TASK_ARGS_JSON' \
    --kwargs '$TASK_KWARGS_JSON' \
    --shepherd-endpoint "$SHEPHERD_URL" \
    --task-modules my_app.tasks my_app.notifications
```

## Advanced Features

### Custom Retry Policies

```python
from azolla.retry import RetryPolicy, ExponentialBackoff, LinearBackoff

# Exponential backoff with jitter
exponential_retry = RetryPolicy(
    max_attempts=5,
    backoff=ExponentialBackoff(
        initial=1.0,
        multiplier=2.0, 
        max_delay=60.0,
        jitter=True
    ),
    retry_on=[ConnectionError, TaskTimeoutError],
    stop_on_codes=["INVALID_EMAIL"]
)

# Linear backoff
linear_retry = RetryPolicy(
    max_attempts=3,
    backoff=LinearBackoff(initial=2.0, increment=1.0)
)
```


### Error Handling

Azolla provides structured error handling with automatic exception wrapping for consistent error reporting.

#### Recommended: Use TaskError and Subclasses

```python
from azolla.exceptions import TaskError, TaskValidationError, TaskTimeoutError

@azolla_task
async def validate_data(data: dict) -> dict:
    """Validate input data."""
    if not data.get("email"):
        # Non-retryable validation error
        raise TaskValidationError("Email is required")

    if external_service_down():
        # Retryable error with custom type
        raise TaskError(
            "External service unavailable",
            error_code="SERVICE_DOWN",
            error_type="ServiceUnavailable",
            retryable=True
        )

    if processing_time_exceeded():
        # Timeout error (retryable by default)
        raise TaskTimeoutError("Data processing took too long")

    return {"status": "valid", "data": data}
```

#### Automatic Exception Wrapping

**Any non-TaskError exceptions are automatically wrapped in TaskError** to ensure consistent error handling:

```python
@azolla_task
async def risky_operation() -> dict:
    """Example showing automatic wrapping."""

    # These exceptions will be automatically wrapped:
    if invalid_input():
        raise ValueError("Invalid input data")  # → TaskError(error_type="ValueError", retryable=True)

    if network_issue():
        raise ConnectionError("Network unavailable")  # → TaskError(error_type="ConnectionError", retryable=True)

    # This is preserved as-is (recommended):
    if business_logic_error():
        raise TaskError("Business rule violation", retryable=False)

    return {"status": "success"}
```

#### Exception Behavior Summary

| Exception Type | Behavior | Retryable Default | Best Practice |
|---------------|----------|-------------------|---------------|
| `TaskError` | Preserved as-is | **`True`** | ✅ **Recommended** |
| `TaskValidationError` | Preserved as-is | `False` | ✅ Use for invalid inputs |
| `TaskTimeoutError` | Preserved as-is | `True` | ✅ Use for timeouts |
| `ValueError`, `TypeError`, etc. | **Wrapped in TaskError** | `True` | ⚠️ Consider using TaskError instead |

#### Custom Error Types

```python
from azolla.exceptions import TaskError

class DatabaseError(TaskError):
    """Custom database-related error."""
    def __init__(self, message: str, **extra_data):
        super().__init__(
            message,
            error_code="DATABASE_ERROR",
            error_type="DatabaseError",
            retryable=True,
            **extra_data
        )

@azolla_task
async def query_database() -> dict:
    try:
        result = await db.query("SELECT * FROM users")
        return {"data": result}
    except DatabaseConnectionTimeout:
        # Custom error with additional context
        raise DatabaseError("Database connection timeout",
                          database="users_db",
                          retry_count=3)
```

### Monitoring and Observability

```python
# Install monitoring dependencies
# pip install azolla[monitoring]

import logging
from azolla._internal.utils import setup_logging

# Configure structured logging  
setup_logging("INFO")

# Optional extras (`azolla[monitoring]`) provide building blocks such as
# Prometheus exporters and OpenTelemetry SDKs. Instrumentation is left to your
# application so you can record the metrics and traces that matter.
```

## Configuration

### Client Configuration

```python
from azolla import ClientConfig, Client

config = ClientConfig(
    endpoint="http://production-orchestrator:52710",
    domain="production",
    timeout=60.0,
    max_message_size=16 * 1024 * 1024  # 16MB
)

client = Client(config)
```

### Worker Invocation

Workers execute a single task per process. The shepherd spawns the worker with the
task metadata and the worker reports the result back via gRPC:

```python
from azolla import Worker, WorkerInvocation

worker = (
    Worker.builder()
    .register_task(greet_user)  # @azolla_task-decorated function
    .register_task(SendEmailTask())
    .build()
)

invocation = WorkerInvocation.from_json(
    task_id="demo-123",
    task_name="greet_user",
    args_json='["Alice"]',
    kwargs_json='{"language": "fr"}',
    shepherd_endpoint="http://127.0.0.1:50052",
)

execution = await worker.execute(invocation)
print(execution.value)

# When running under a shepherd, call run_invocation to report the result back:
# await worker.run_invocation(invocation)
```

## Examples

See the [`../../examples/python/`](../../examples/python/) directory for complete examples:

- [`basic_client_example.py`](../../examples/python/basic_client_example.py) - Basic client usage and task submission
- [`basic_worker_example.py`](../../examples/python/basic_worker_example.py) - Basic worker setup and task handling

## Development

### Running Tests

Install development dependencies:

```bash
pip install -e ".[dev,testing]"
```

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=azolla --cov-report=term-missing

# Run only unit tests
pytest tests/unit/

# Run integration tests (requires Azolla orchestrator)
pytest tests/integration/
```

### Testing Your Tasks

Use standard pytest coroutines to test your task functions or Task classes directly.

```python
import pytest

@pytest.mark.asyncio
async def test_send_email_task():
    result = await send_email(
        to="test@example.com",
        subject="Test",
        body="Hello",
    )
    assert result["sent_to"] == "test@example.com"
```

### Code Quality

This project uses several tools to maintain code quality:

```bash
# Linting and formatting
ruff check src tests
black src tests

# Type checking
mypy src

# Security scanning
bandit -r src/
```

## Release Process

To release a new version to PyPI:

1. **Ensure all tests pass** on your feature branch
2. **Merge to main branch** and ensure CI passes
3. **Update version** in `src/azolla/_version.py`
4. **Run release script** from main branch:
   ```bash
   ./release.sh 0.1.3  # Replace with your version
   ```

The release script will:
- ✅ Verify you're on main branch (where tests have passed)
- ✅ Update proto files from main project
- ✅ Build and verify the package
- ✅ Test publish to TestPyPI (if configured)
- ✅ Publish to production PyPI
- ✅ Create a git tag
- ✅ Verify installation works

**Note**: The release script only runs from main branch to ensure all tests have passed before release.

## API Reference

### Core Classes

- **`Client`** - Submit tasks to Azolla orchestrator
- **`Worker`** - Process tasks from Azolla orchestrator  
- **`Task`** - Base class for task implementations
- **`TaskHandle`** - Handle to submitted task for result retrieval
- **`TaskResult`** - Result of task execution with status and data

### Decorators

- **`@azolla_task`** - Convert async function to Azolla task

### Exceptions

- **`TaskError`** - Base exception for task execution errors (preserves retryable flag)
- **`TaskValidationError`** - Invalid task arguments (non-retryable by default)
- **`TaskTimeoutError`** - Task execution timeout (retryable by default)
- **`ConnectionError`** - Connection to orchestrator failed
- **`WorkerError`** - Worker-specific errors
- **`SerializationError`** - Serialization/deserialization errors

**Note**: Non-TaskError exceptions (e.g., `ValueError`, `TypeError`) are automatically wrapped in `TaskError` to ensure consistent error handling and proper retry behavior.

### Retry Policies

- **`RetryPolicy`** - Configurable retry behavior
- **`ExponentialBackoff`** - Exponential delay with jitter
- **`LinearBackoff`** - Linear delay increase
- **`FixedBackoff`** - Fixed delay between retries

## Requirements

- Python 3.9+
- gRPC dependencies (`grpcio`, `grpcio-status`) 
- Pydantic v2 for validation
- Optional: `uvloop` for performance, `orjson` for faster JSON

## Contributing

Contributions welcome! Please see the [main Azolla repository](https://github.com/azolla-io/azolla) for contribution guidelines.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

Note on generated code: The Python package checks in generated gRPC stubs under `src/azolla/_grpc/` to simplify installation for end users. Regenerate them with `clients/python/scripts/generate_proto.py` when protocol files change.
