# Azolla Python Client Library Design

## Overview

The `azolla` Python client library provides a Pythonic, production-ready interface for building distributed task processing applications. Following proven patterns from the Rust client, it offers both convenient decorator-based task definitions and explicit class-based implementations, designed for seamless PyPI distribution.

## Design Principles

1. **Pythonic First**: Leverage Python idioms, type hints, and async/await patterns
2. **Production Ready**: Follow PyPI best practices with proper versioning, dependencies, and testing
3. **Developer Experience**: Prioritize ease of use, clear error messages, and excellent IDE support  
4. **Dual Approach**: Support both convenient (`@azolla_task`) and explicit (`Task` class) patterns
5. **Type Safety**: Use Pydantic v2 for validation with automatic casting for IDE support
6. **Async Native**: Built on asyncio with proper event loop management

## Architecture Overview

```python
# Convenient approach - decorator magic
@azolla_task
async def process_order(order_id: str, priority: int = 1) -> dict:
    return {"status": "processed", "order_id": order_id}

# Explicit approach - class inheritance  
class ProcessOrderTask(Task):
    class Args(BaseModel):
        order_id: str
        priority: int = 1
    
    async def execute(self, args: Args) -> dict:
        return {"status": "processed", "order_id": args.order_id}
```

## Package Structure (PyPI Ready)

```
clients/python/
├── pyproject.toml           # PEP 518 build system
├── README.md               # PyPI description
├── LICENSE                 # Apache 2.0
├── CHANGELOG.md            # Version history
├── .github/
│   └── workflows/
│       ├── test.yml        # CI/CD pipeline
│       └── publish.yml     # PyPI publishing
├── src/
│   └── azolla/
│       ├── __init__.py     # Public API exports
│       ├── py.typed        # PEP 561 typing support
│       ├── _version.py     # Single source of truth for version
│       ├── client.py       # Client for task submission
│       ├── worker.py       # Worker for task execution  
│       ├── task.py         # Task base class and decorator
│       ├── exceptions.py   # Exception hierarchy (renamed from errors.py)
│       ├── retry.py        # Retry policy implementation
│       ├── types.py        # Type definitions
│       ├── _grpc/          # Generated gRPC code
│       │   ├── __init__.py
│       │   └── *.py        # Generated files
│       └── _internal/      # Private utilities
│           ├── __init__.py
│           └── utils.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py         # Pytest configuration
│   ├── unit/
│   ├── integration/
│   └── test_*.py
├── examples/
│   ├── __init__.py
│   ├── basic_usage.py
│   ├── advanced_retry.py
│   └── production_worker.py
├── docs/                   # Sphinx documentation
│   ├── conf.py
│   ├── index.rst
│   └── api/
└── scripts/
    ├── generate_proto.py   # Proto generation
    └── build_release.py    # Release automation
```

## Core API Design

### 1. Task Definitions

#### Decorator Approach (Convenient)

```python
from azolla import azolla_task
from azolla.exceptions import TaskError

@azolla_task
async def send_email(to: str, subject: str, body: str) -> dict:
    """Send an email notification."""
    if not to or "@" not in to:
        raise TaskError("Invalid email address", error_code="INVALID_EMAIL")
    
    # Email sending logic here
    return {
        "message_id": "msg_123",
        "sent_to": to,
        "timestamp": "2024-01-01T00:00:00Z"
    }

# The decorator automatically creates:
# - SendEmailTask class
# - Pydantic Args model from function signature  
# - Type-safe execute method
```

#### Class Approach (Explicit)

```python
from azolla import Task
from azolla.exceptions import TaskError
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime

class SendEmailTask(Task):
    class Args(BaseModel):
        to: str
        subject: str
        body: str
        template_id: Optional[str] = None
        
        @field_validator('to')  # Pydantic v2 syntax
        @classmethod
        def validate_email(cls, v: str) -> str:
            if not v or "@" not in v:
                raise ValueError("Invalid email address")
            return v
    
    async def execute(self, args: Args) -> dict:
        """Args is automatically cast to SendEmailTask.Args type."""
        # Full IDE auto-completion on args.to, args.subject, etc.
        if args.template_id:
            body = await self.load_template(args.template_id, args.body)
        else:
            body = args.body
            
        # Email sending logic
        return {
            "message_id": f"msg_{hash(args.to)}",
            "sent_to": args.to,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def load_template(self, template_id: str, body: str) -> str:
        # Custom methods available in explicit approach
        return f"Template {template_id}: {body}"
```

### 2. Task Execution & Results

```python
from typing import Any, Optional, Union, Generic, TypeVar
from pydantic import BaseModel
from enum import Enum

T = TypeVar('T')

class TaskStatus(str, Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskResult(BaseModel, Generic[T]):
    """Represents the result of task execution."""
    task_id: str
    status: TaskStatus
    value: Optional[T] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_type: Optional[str] = None
    execution_time: Optional[float] = None
    attempt_number: int = 1
    max_attempts: Optional[int] = None
    
    @property
    def success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED
        
    @property
    def failed(self) -> bool:
        """Check if task failed."""
        return self.status == TaskStatus.FAILED

class TaskError(Exception):
    """Base exception for task execution errors."""
    def __init__(
        self, 
        message: str, 
        error_code: str = "TASK_ERROR",
        error_type: Optional[str] = None,
        retryable: bool = True,
        **extra_data: Any
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code  
        self.error_type = error_type or self.__class__.__name__
        self.retryable = retryable
        self.extra_data = extra_data
        
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "message": self.message,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "retryable": self.retryable,
            **self.extra_data
        }

# Specific error types
class ValidationError(TaskError):
    """Raised when task arguments are invalid."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="VALIDATION_ERROR", retryable=False, **extra_data)

class TimeoutError(TaskError):
    """Raised when task execution times out."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="TIMEOUT_ERROR", retryable=True, **extra_data)

class ResourceError(TaskError):
    """Raised when required resources are unavailable."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="RESOURCE_ERROR", retryable=True, **extra_data)
```

### 3. Client API

```python
from azolla import Client
from azolla.retry import RetryPolicy, ExponentialBackoff

# Create client
client = Client(
    orchestrator_endpoint="http://localhost:52710",
    domain="production",
    timeout=30.0
)

# Submit tasks with type safety
submission = client.submit_task(send_email, {
    "to": "user@example.com",
    "subject": "Welcome!",
    "body": "Welcome to our platform"
})

# Configure retry policy  
submission.with_retry(
    RetryPolicy(
        max_attempts=3,
        backoff=ExponentialBackoff(initial=1.0, max_delay=60.0),
        retry_on=[TaskError],
        stop_on_codes=["INVALID_EMAIL"]
    )
)

# Submit and get handle
handle = await submission.submit()

# Wait for result
result = await handle.wait()
if result.success:
    print(f"Email sent: {result.value}")
else:
    print(f"Failed: {result.error}")
```

### 4. Worker API

```python
from azolla import Worker
import asyncio

async def main():
    # Build worker
    worker = (Worker.builder()
             .orchestrator("http://localhost:52710")
             .domain("production") 
             .shepherd_group("email-workers")
             .max_concurrency(10)
             .register_task(send_email)  # Decorator-created task
             .register_task(ProcessOrderTask)  # Explicit task class
             .build())
    
    print(f"Starting worker with {worker.task_count} tasks")
    
    # Start worker in existing event loop
    await worker.start()
    
    # Worker runs until shutdown signal
    await worker.wait_for_shutdown()

# User manages the event loop
if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Features

### 1. Custom Argument Parsing

```python
class ProcessDataTask(Task):
    class Args(BaseModel):
        data: list[dict]
        operation: str
        
    async def execute(self, args: Args) -> dict:
        # Process data based on operation
        if args.operation == "aggregate":
            return {"result": sum(item.get("value", 0) for item in args.data)}
        elif args.operation == "filter":
            return {"result": [item for item in args.data if item.get("active", False)]}
        else:
            raise TaskError(f"Unknown operation: {args.operation}")
    
    @classmethod  
    def parse_args(cls, json_args: list) -> Args:
        """Custom parsing for legacy format compatibility."""
        if len(json_args) == 2:
            # Legacy format: [data_array, operation_string]
            return cls.Args(data=json_args[0], operation=json_args[1])
        else:
            # New format: single Args object
            return cls.Args.parse_obj(json_args[0])
```

### 2. Context and Dependency Injection

```python
from azolla import TaskContext

class DatabaseTask(Task):
    def __init__(self, db_pool=None):
        self.db_pool = db_pool or get_default_pool()
    
    class Args(BaseModel):
        query: str
        params: dict = {}
    
    async def execute(self, args: Args, context: TaskContext) -> dict:
        """Context provides task metadata."""
        print(f"Executing task {context.task_id}, attempt {context.attempt_number}")
        
        async with self.db_pool.acquire() as conn:
            result = await conn.fetch(args.query, **args.params)
            return {"rows": len(result), "data": [dict(row) for row in result]}

# Register with dependency injection
worker = (Worker.builder()
         .register_task(DatabaseTask(db_pool=my_pool))
         .build())
```

### 3. Error Handling & Retry Policies

```python
from azolla.retry import RetryPolicy, LinearBackoff, ExponentialBackoff

# Pythonic retry configuration
@azolla_task
async def flaky_api_call(url: str, max_retries: int = 3) -> dict:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code in [500, 502, 503]:
            raise TaskError(
                f"Server error: {e.response.status_code}", 
                error_code="SERVER_ERROR",
                retryable=True
            )
        else:
            raise TaskError(
                f"Client error: {e.response.status_code}",
                error_code="CLIENT_ERROR", 
                retryable=False
            )

# Configure retries at submission time
submission = client.submit_task(flaky_api_call, {"url": "https://api.example.com/data"})
submission.with_retry(
    RetryPolicy(
        max_attempts=5,
        backoff=ExponentialBackoff(initial=1.0, multiplier=2.0, max_delay=30.0),
        retry_on=[TaskError],
        stop_on_codes=["CLIENT_ERROR"]
    )
)
```

## Implementation Details

### 1. Base Task Class with Auto-Casting

```python
from typing import TypeVar, get_type_hints
from pydantic import BaseModel
import inspect

T = TypeVar('T', bound=BaseModel)

class Task:
    """Base class for all tasks with automatic argument casting."""
    
    def __init_subclass__(cls):
        """Set up automatic Args type detection."""
        if hasattr(cls, 'Args'):
            cls._args_type = cls.Args
        super().__init_subclass__()
    
    async def execute(self, args: BaseModel) -> Any:
        """Override this method in subclasses."""
        raise NotImplementedError
    
    async def _execute_with_casting(self, raw_args: dict) -> Any:
        """Internal method that handles automatic casting."""
        if hasattr(self, '_args_type'):
            typed_args = self._args_type.parse_obj(raw_args)
        else:
            typed_args = raw_args
        
        return await self.execute(typed_args)
    
    @classmethod
    def parse_args(cls, json_args: list) -> BaseModel:
        """Parse JSON arguments into typed arguments."""
        if not json_args:
            return cls._args_type()
        elif len(json_args) == 1:
            return cls._args_type.parse_obj(json_args[0])
        else:
            # Multiple args - treat as array
            return cls._args_type.parse_obj(json_args)
    
    def name(self) -> str:
        """Task name for registration."""
        return self.__class__.__name__.replace('Task', '').lower()
```

### 2. Decorator Implementation

```python
import functools
from typing import get_type_hints, get_origin, get_args
from pydantic import BaseModel, create_model

def azolla_task(func):
    """Decorator that converts async functions into Task classes."""
    
    # Extract function signature
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    
    # Create Pydantic model from function parameters
    fields = {}
    for param_name, param in sig.parameters.items():
        param_type = type_hints.get(param_name, str)
        default_value = param.default if param.default != param.empty else ...
        fields[param_name] = (param_type, default_value)
    
    # Generate Args model
    args_model_name = f"{func.__name__.title().replace('_', '')}Args"
    args_model = create_model(args_model_name, **fields)
    
    # Generate Task class
    class GeneratedTask(Task):
        Args = args_model
        _original_func = func
        
        async def execute(self, args: args_model) -> Any:
            # Convert args back to function parameters
            kwargs = args.dict()
            return await self._original_func(**kwargs)
        
        def name(self) -> str:
            return func.__name__
    
    # Set up the magic: make the function behave like a task
    task_instance = GeneratedTask()
    
    # Copy function metadata
    functools.update_wrapper(task_instance, func)
    task_instance.__name__ = func.__name__
    task_instance.__class__.__name__ = f"{func.__name__.title().replace('_', '')}Task"
    
    return task_instance
```

## Testing Support

```python
from azolla.testing import MockClient, TaskTester

# Unit test individual tasks
async def test_send_email_task():
    tester = TaskTester(send_email)
    
    result = await tester.execute({
        "to": "test@example.com", 
        "subject": "Test",
        "body": "Hello"
    })
    
    assert result.success
    assert result.value["sent_to"] == "test@example.com"

# Integration test with mock client
async def test_task_submission():
    mock_client = MockClient()
    
    submission = mock_client.submit_task(send_email, {
        "to": "test@example.com",
        "subject": "Test", 
        "body": "Hello"
    })
    
    handle = await submission.submit()
    result = await handle.wait()
    
    assert result.success
```

## Migration & Compatibility

### From Celery
```python
# Celery task
@celery.task
def send_email_celery(to, subject, body):
    # existing logic
    return {"status": "sent"}

# Azolla equivalent
@azolla_task  
async def send_email(to: str, subject: str, body: str) -> dict:
    # same logic, now with type safety and async
    return {"status": "sent"}
```

## Dependencies & PyPI Configuration

### pyproject.toml

```toml
[build-system]
requires = ["hatchling>=1.13.0"]
build-backend = "hatchling.build"

[project]
name = "azolla"
dynamic = ["version"]
description = "Python client library for Azolla distributed task processing"
readme = "README.md"
license = {text = "Apache-2.0"}
authors = [
    {name = "Azolla Team", email = "team@azolla.io"},
]
maintainers = [
    {name = "Azolla Team", email = "team@azolla.io"},
]
keywords = ["distributed", "tasks", "async", "worker", "queue"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10", 
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: System :: Distributed Computing",
    "Typing :: Typed",
]
requires-python = ">=3.9"
dependencies = [
    "pydantic>=2.0.0,<3.0.0",
    "grpcio>=1.50.0",
    "grpcio-status>=1.50.0",
    "typing-extensions>=4.5.0;python_version<'3.11'",
]

[project.optional-dependencies]
# Performance optimizations
performance = [
    "uvloop>=0.17.0;sys_platform!='win32'",
    "orjson>=3.8.0",
]
# Development tools
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.0.0",
    "mypy>=1.5.0",
    "ruff>=0.1.0",
    "black>=23.0.0",
    "pre-commit>=3.0.0",
]
# Documentation
docs = [
    "sphinx>=7.0.0",
    "sphinx-rtd-theme>=1.3.0",
    "sphinx-autodoc-typehints>=1.24.0",
]
# Monitoring and observability
monitoring = [
    "prometheus-client>=0.17.0",
    "opentelemetry-api>=1.20.0",
    "structlog>=23.0.0",
]
# Testing utilities
testing = [
    "pytest-mock>=3.11.0",
    "factory-boy>=3.3.0",
    "freezegun>=1.2.0",
]
# Complete development environment
all = [
    "azolla[performance,dev,docs,monitoring,testing]"
]

[project.urls]
Homepage = "https://github.com/azolla-io/azolla"
Documentation = "https://docs.azolla.io/python-client"
Repository = "https://github.com/azolla-io/azolla.git"
"Bug Tracker" = "https://github.com/azolla-io/azolla/issues"
Changelog = "https://github.com/azolla-io/azolla/blob/main/clients/python/CHANGELOG.md"

[project.scripts]
azolla-worker = "azolla.cli:worker_main"

[tool.hatch.version]
path = "src/azolla/_version.py"

[tool.hatch.build.targets.sdist]
include = [
    "/src",
    "/tests",
    "/README.md",
    "/LICENSE",
    "/CHANGELOG.md",
]

[tool.hatch.build.targets.wheel]
packages = ["src/azolla"]

# Ruff configuration (linting & formatting)
[tool.ruff]
target-version = "py39"
line-length = 100
select = [
    "E",  # pycodestyle errors
    "W",  # pycodestyle warnings  
    "F",  # pyflakes
    "I",  # isort
    "B",  # flake8-bugbear
    "C4", # flake8-comprehensions
    "UP", # pyupgrade
]
ignore = [
    "E501",  # Line too long (handled by formatter)
    "B008",  # Do not perform function calls in argument defaults
]

[tool.ruff.per-file-ignores]
"tests/**/*.py" = [
    "B018", # Found useless expression
]

[tool.black]
line-length = 100
target-version = ['py39', 'py310', 'py311', 'py312']

[tool.mypy]
python_version = "3.9"
strict = true
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
disallow_incomplete_defs = true
check_untyped_defs = true
disallow_untyped_decorators = true

[tool.pytest.ini_options]
minversion = "7.0"
addopts = "-ra -q --strict-markers --strict-config"
testpaths = ["tests"]
asyncio_mode = "auto"
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests as integration tests",
]

[tool.coverage.run]
source = ["src"]
branch = true

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "if self.debug:",
    "if settings.DEBUG",
    "raise AssertionError",
    "raise NotImplementedError",
    "if 0:",
    "if __name__ == .__main__.:",
    "class .*\\bProtocol\\):",
    "@(abc\\.)?abstractmethod",
]
```

### Version Management

```python
# src/azolla/_version.py
"""Single source of truth for package version."""
__version__ = "0.1.0"
__version_info__ = tuple(int(i) for i in __version__.split("."))
```

### Public API Exports

```python
# src/azolla/__init__.py
"""Azolla Python Client Library.

A modern, type-safe Python client for Azolla distributed task processing.
"""

from azolla._version import __version__, __version_info__
from azolla.client import Client, ClientConfig
from azolla.worker import Worker, WorkerConfig
from azolla.task import Task, azolla_task, TaskContext
from azolla.exceptions import (
    TaskError,
    ValidationError,
    TimeoutError,
    ResourceError,
    AzollaError,
)
from azolla.retry import RetryPolicy, ExponentialBackoff, LinearBackoff
from azolla.types import TaskResult, TaskStatus

__all__ = [
    # Version info
    "__version__",
    "__version_info__",
    # Core classes
    "Client",
    "ClientConfig", 
    "Worker",
    "WorkerConfig",
    "Task",
    "TaskContext",
    # Decorators
    "azolla_task",
    # Exceptions
    "TaskError",
    "ValidationError", 
    "TimeoutError",
    "ResourceError",
    "AzollaError",
    # Retry policies
    "RetryPolicy",
    "ExponentialBackoff",
    "LinearBackoff",
    # Types
    "TaskResult",
    "TaskStatus",
]
```

### Requirements

- **Python**: 3.9+ (Modern Python with better type hints)
- **Core**: `pydantic>=2.0`, `grpcio>=1.50.0`, `grpcio-status>=1.50.0`
- **Optional**: `uvloop` (performance), `prometheus-client` (metrics), `orjson` (faster JSON)

## Testing & Quality Assurance

### Test Structure

```python
# tests/conftest.py
"""Shared test fixtures and configuration."""
import pytest
import asyncio
from typing import AsyncGenerator
from azolla import Client, Worker
from azolla.testing import MockOrchestrator

@pytest.fixture
async def mock_orchestrator() -> AsyncGenerator[MockOrchestrator, None]:
    """Provide a mock orchestrator for testing."""
    orchestrator = MockOrchestrator()
    await orchestrator.start()
    try:
        yield orchestrator
    finally:
        await orchestrator.stop()

@pytest.fixture
async def client(mock_orchestrator: MockOrchestrator) -> Client:
    """Provide a test client connected to mock orchestrator."""
    return await Client.connect(mock_orchestrator.endpoint)

# tests/unit/test_task.py
"""Unit tests for task functionality."""
import pytest
from azolla import azolla_task, Task, ValidationError
from azolla.testing import TaskTester

@azolla_task
async def example_task(name: str, count: int = 1) -> dict:
    """Example task for testing."""
    return {"message": f"Hello {name}!", "count": count}

class TestTaskDecorator:
    """Test azolla_task decorator functionality."""
    
    async def test_decorator_creates_task_class(self) -> None:
        """Test that decorator creates a proper Task class."""
        assert hasattr(example_task, '__azolla_task_class__')
        task_class = example_task.__azolla_task_class__
        assert issubclass(task_class, Task)
    
    async def test_task_execution(self) -> None:
        """Test basic task execution."""
        tester = TaskTester(example_task)
        result = await tester.execute({"name": "World", "count": 3})
        
        assert result.success
        assert result.value == {"message": "Hello World!", "count": 3}
    
    async def test_validation_error(self) -> None:
        """Test that validation errors are properly handled."""
        tester = TaskTester(example_task)
        
        with pytest.raises(ValidationError):
            await tester.execute({"count": "invalid"})  # Wrong type

# tests/integration/test_client_worker.py
"""Integration tests for client-worker communication."""
import pytest
from azolla import Client, Worker, azolla_task

@azolla_task
async def integration_test_task(value: int) -> dict:
    """Task for integration testing."""
    return {"doubled": value * 2}

class TestClientWorkerIntegration:
    """Test full client-worker integration."""
    
    async def test_task_submission_and_execution(
        self, 
        mock_orchestrator, 
        client: Client
    ) -> None:
        """Test complete task lifecycle."""
        # Start worker
        worker = (
            Worker.builder()
            .orchestrator(mock_orchestrator.endpoint)
            .register_task(integration_test_task)
            .build()
        )
        
        # Submit task
        result = await client.submit_task(
            integration_test_task,
            {"value": 21}
        ).submit().wait()
        
        assert result.success
        assert result.value == {"doubled": 42}
```

### CI/CD Pipeline

```yaml
# .github/workflows/test.yml
name: Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        python-version: ["3.9", "3.10", "3.11", "3.12", "3.13"]
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e ".[dev,testing]"
    
    - name: Lint with ruff
      run: ruff check src tests
    
    - name: Format check with black
      run: black --check src tests
    
    - name: Type check with mypy
      run: mypy src
    
    - name: Test with pytest
      run: |
        pytest tests/ \
          --cov=azolla \
          --cov-report=xml \
          --cov-report=term-missing \
          -v
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      if: matrix.os == 'ubuntu-latest' && matrix.python-version == '3.11'
      with:
        file: ./coverage.xml

  security:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Run security checks
      run: |
        pip install safety bandit
        safety check
        bandit -r src/

# .github/workflows/publish.yml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: "3.11"
    
    - name: Install build tools
      run: |
        python -m pip install --upgrade pip
        pip install build twine
    
    - name: Build package
      run: python -m build
    
    - name: Verify package
      run: |
        twine check dist/*
        pip install dist/*.whl
        python -c "import azolla; print(azolla.__version__)"
    
    - name: Publish to PyPI
      env:
        TWINE_USERNAME: __token__
        TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
      run: twine upload dist/*
```

### Quality Assurance Tools

```python
# scripts/quality_check.py
"""Quality assurance script for development."""
import subprocess
import sys
from pathlib import Path

def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"Running {description}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ {description} failed:")
        print(result.stdout)
        print(result.stderr)
        return False
    else:
        print(f"✅ {description} passed")
        return True

def main() -> None:
    """Run all quality checks."""
    checks = [
        (["ruff", "check", "src", "tests"], "Linting"),
        (["black", "--check", "src", "tests"], "Code formatting"),
        (["mypy", "src"], "Type checking"),
        (["pytest", "tests/", "--cov=azolla", "-v"], "Tests"),
        (["safety", "check"], "Security vulnerabilities"),
        (["bandit", "-r", "src/"], "Security analysis"),
    ]
    
    failed_checks = []
    for cmd, description in checks:
        if not run_command(cmd, description):
            failed_checks.append(description)
    
    if failed_checks:
        print(f"\n❌ {len(failed_checks)} checks failed:")
        for check in failed_checks:
            print(f"  - {check}")
        sys.exit(1)
    else:
        print(f"\n✅ All {len(checks)} quality checks passed!")

if __name__ == "__main__":
    main()
```

### Performance Benchmarks

```python
# tests/benchmarks/test_performance.py
"""Performance benchmarks for the Azolla client."""
import pytest
import asyncio
import time
from azolla import azolla_task
from azolla.testing import TaskTester

@azolla_task
async def benchmark_task(data: dict) -> dict:
    """Simple task for benchmarking."""
    return {"processed": len(data)}

class TestPerformance:
    """Performance benchmark tests."""
    
    @pytest.mark.slow
    async def test_task_execution_throughput(self) -> None:
        """Benchmark task execution throughput."""
        tester = TaskTester(benchmark_task)
        test_data = {"key": "value" * 100}
        
        num_tasks = 1000
        start_time = time.perf_counter()
        
        tasks = [
            tester.execute(test_data)
            for _ in range(num_tasks)
        ]
        results = await asyncio.gather(*tasks)
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        throughput = num_tasks / duration
        
        print(f"Executed {num_tasks} tasks in {duration:.2f}s")
        print(f"Throughput: {throughput:.2f} tasks/second")
        
        # Verify all tasks completed successfully
        assert all(result.success for result in results)
        # Expect reasonable performance (adjust based on requirements)
        assert throughput > 500  # At least 500 tasks/second
```

## Publishing Checklist

### Pre-Release Validation

- [ ] All tests pass on supported Python versions (3.9-3.13)
- [ ] Type checking passes with mypy strict mode
- [ ] Code formatting consistent with black + ruff
- [ ] Security scan passes (bandit + safety)
- [ ] Documentation builds successfully
- [ ] Version number updated in `_version.py`
- [ ] CHANGELOG.md updated with release notes
- [ ] All dependencies pinned with minimum versions

### PyPI Publishing Steps

1. **Test on TestPyPI first**:
   ```bash
   python -m build
   twine upload --repository testpypi dist/*
   pip install --index-url https://test.pypi.org/simple/ azolla
   ```

2. **Production release**:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   # GitHub Actions will handle PyPI publishing
   ```

3. **Post-release verification**:
   ```bash
   pip install azolla
   python -c "import azolla; print(azolla.__version__)"
   ```

## Performance Considerations

1. **Pydantic v2 Performance**: ~2-5x faster than v1, validation adds ~5-25μs per task
2. **Memory Usage**: Task classes are lightweight, decorator approach creates minimal overhead
3. **Async Performance**: Built on asyncio, supports high concurrency with optional uvloop
4. **gRPC Efficiency**: Uses connection pooling and streaming for optimal network performance
5. **JSON Performance**: Optional orjson support for 2-3x faster serialization

This design provides a production-ready, Pythonic interface following PyPI best practices while maintaining the proven patterns from the Rust implementation. The comprehensive testing, CI/CD, and quality assurance ensure reliable releases suitable for enterprise adoption.