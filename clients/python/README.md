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
    async with Client.connect("http://localhost:52710") as client:
        
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
from azolla import Worker

async def main():
    # Create worker
    worker = (
        Worker.builder()
        .orchestrator("localhost:52710")
        .domain("production")
        .shepherd_group("email-workers")
        .max_concurrency(10)
        .register_task(send_email)  # Register your tasks
        .build()
    )
    
    print(f"Starting worker with {worker.task_count()} tasks")
    
    # Start processing (runs until shutdown)
    await worker.start()

asyncio.run(main())
```

### CLI Worker

You can also run workers from the command line:

```bash
# Start worker and import task modules
azolla-worker \
    --orchestrator localhost:52710 \
    --domain production \
    --shepherd-group email-workers \
    --max-concurrency 10 \
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
    retry_on=[ConnectionError, TimeoutError],
    stop_on_codes=["INVALID_EMAIL"]
)

# Linear backoff
linear_retry = RetryPolicy(
    max_attempts=3,
    backoff=LinearBackoff(initial=2.0, increment=1.0)
)
```

### Task Context and Metadata

```python
from azolla import TaskContext

@azolla_task  
async def process_order(order_id: str, context: TaskContext = None) -> dict:
    """Process an order with context information."""
    print(f"Processing order {order_id} (attempt {context.attempt_number})")
    
    if context.is_final_attempt():
        print("This is the final retry attempt!")
    
    # Your processing logic
    return {"order_id": order_id, "status": "processed"}
```

### Error Handling

```python
from azolla.exceptions import TaskError, ValidationError

@azolla_task
async def validate_data(data: dict) -> dict:
    """Validate input data."""
    if not data.get("email"):
        # Non-retryable error
        raise ValidationError("Email is required")
    
    if external_service_down():
        # Retryable error
        raise TaskError(
            "External service unavailable", 
            error_code="SERVICE_DOWN",
            retryable=True
        )
    
    return {"status": "valid", "data": data}
```

### Monitoring and Observability

```python
# Install monitoring dependencies
# pip install azolla[monitoring]

import logging
from azolla._internal.utils import setup_logging

# Configure structured logging  
setup_logging("INFO")

# Your tasks will automatically include:
# - Execution time tracking
# - Attempt number logging
# - Error categorization
# - Performance metrics (if prometheus-client installed)
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

### Worker Configuration

```python  
from azolla import WorkerConfig, Worker

config = WorkerConfig(
    orchestrator_endpoint="localhost:52710",
    domain="production", 
    shepherd_group="data-processors",
    max_concurrency=20,
    heartbeat_interval=30.0,
    reconnect_delay=1.0,
    max_reconnect_delay=60.0
)

worker = Worker(config)
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

```python
import pytest
from azolla.testing import TaskTester

@pytest.mark.asyncio
async def test_send_email_task():
    """Test email task execution."""
    tester = TaskTester(send_email)
    
    result = await tester.execute({
        "to": "test@example.com",
        "subject": "Test", 
        "body": "Hello"
    })
    
    assert result.success
    assert result.value["sent_to"] == "test@example.com"
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

- **`TaskError`** - Base exception for task execution errors
- **`ValidationError`** - Invalid task arguments  
- **`TimeoutError`** - Task execution timeout
- **`ConnectionError`** - Connection to orchestrator failed

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