# Azolla Client Libraries - AI Agent Guide

This document provides comprehensive guidance for AI coding agents working with Azolla client libraries. It covers architecture, development patterns, and publishing procedures for both Rust and Python clients.

## Directory Structure

```
clients/
├── AGENTS.md                      # This file - AI agent guide
├── rust/                          # Rust client implementation
│   ├── azolla-client/             # Main client library crate
│   │   ├── Cargo.toml            # Package metadata and dependencies
│   │   ├── README.md             # User-facing documentation
│   │   ├── build.rs              # Build script for protobuf generation
│   │   ├── src/
│   │   │   ├── lib.rs            # Main library exports
│   │   │   ├── client.rs         # gRPC client implementation
│   │   │   ├── error.rs          # Error types and handling
│   │   │   ├── retry_policy.rs   # Retry policy implementation
│   │   │   ├── task.rs           # Task trait and utilities
│   │   │   └── worker.rs         # Single-invocation worker runtime
│   │   ├── LICENSE              # Apache 2.0 license
│   │   └── release.sh            # Automated publishing script
│   └── azolla-macros/            # Procedural macro support crate
│       ├── Cargo.toml           # Proc macro package metadata
│       ├── README.md            # Macro-specific documentation
│       ├── src/lib.rs           # Proc macro implementation
│       └── LICENSE             # Apache 2.0 license
└── python/                       # Python client implementation
    ├── src/azolla/               # Main package source
    │   ├── __init__.py          # Package exports and __all__
    │   ├── _version.py          # Version information
    │   ├── client.py            # gRPC client implementation
    │   ├── worker.py            # Worker implementation
    │   ├── task.py              # Task definition and decorator
    │   ├── types.py             # Common types and enums
    │   ├── exceptions.py        # Error hierarchy
    │   ├── retry.py             # Retry policies with backoff strategies
    │   ├── cli.py               # Command-line interface
    │   ├── _grpc/               # Generated gRPC code (excluded from linting)
    │   └── _internal/           # Internal utilities
    ├── tests/                    # Test suite
    │   ├── unit/                # Unit tests
    │   └── integration/         # Integration tests with orchestrator
    ├── pyproject.toml           # Package metadata and tool configuration
    ├── README.md                # User-facing documentation
    ├── LICENSE                  # Apache 2.0 license
    └── release.sh               # Automated publishing script
```

## Rust Client Architecture

The Rust client uses a **two-crate architecture** following Rust ecosystem best practices:

### azolla-macros (Proc Macro Crate)
- **Purpose**: Provides `#[azolla_task]` procedural macro
- **Type**: `proc-macro = true` crate
- **Dependencies**: `syn`, `quote`, `proc-macro2`
- **Exports**: Only the `azolla_task` proc macro attribute

### azolla-client (Main Library Crate)  
- **Purpose**: Complete client library with optional proc macro support
- **Type**: Regular library crate
- **Dependencies**: `tonic`, `tokio`, `serde_json`, optional `azolla-macros`
- **Features**: `macros` feature enables proc macro support
- **Worker runtime**: Executes a single task per process and reports results back to the shepherd via `ReportResult`

### Feature-Based Integration

```toml
# azolla-client/Cargo.toml
[features]
default = []
macros = ["azolla-macros"]  # Enables proc macro support

[dependencies]
azolla-macros = { version = "0.1.0", optional = true }
```

Users can choose their integration level:
```toml
# Client-only usage
azolla-client = "0.1.0"

# Full development with proc macros  
azolla-client = { version = "0.1.0", features = ["macros"] }
```

## Python Client Architecture

The Python client follows **modern Python packaging standards** with a clean, type-safe API:

### Package Structure
- **pyproject.toml**: Modern Python packaging with Hatchling build system
- **src/azolla/**: Source layout following PEP 420 namespace packages
- **Type hints**: Full type annotations for Python 3.9+
- **Async/await**: Native asyncio support throughout
- **Pydantic v2**: For data validation and serialization

### Core Modules

#### azolla/__init__.py
- **Exports**: All public APIs with sorted `__all__` list
- **Import organization**: Alphabetized imports following import guidelines
- **Version info**: Re-exports from `_version.py`

#### azolla/client.py
- **Client class**: Main gRPC client with connection management
- **TaskHandle**: Handle for submitted tasks with result waiting
- **Context managers**: `async with Client.connect()` pattern
- **Connection pooling**: Efficient gRPC channel management

#### azolla/worker.py
- **Worker class**: Task execution engine with shepherd registration
- **WorkerConfig**: Configuration for worker behavior
- **Task registry**: Dynamic task registration system
- **Graceful shutdown**: Proper cleanup on SIGTERM/SIGINT

#### azolla/task.py
- **@azolla_task decorator**: Convert functions to Azolla tasks
- **Task base class**: For class-based task definitions

#### azolla/retry.py
- **RetryPolicy**: Configurable retry behavior
- **Backoff strategies**: Exponential, Linear, Fixed backoff
- **Jitter support**: Cryptographically secure randomness
- **Exception handling**: Retryable vs non-retryable errors

#### azolla/types.py
- **TaskResult**: Success/failure result wrapper
- **TaskStatus**: Enumeration of task states
- **Common types**: Shared data structures

#### azolla/exceptions.py
- **Error hierarchy**: From base AzollaError to specific errors
- **gRPC integration**: Automatic gRPC status code mapping
- **Type safety**: Proper exception inheritance

### Development Guidelines

#### Type Safety
```python
# Always use type hints
async def submit_task(self, task_func: Callable, args: Dict[str, Any]) -> TaskHandle:
    ...

# Use generic types where appropriate
from typing import TypeVar, Generic
T = TypeVar('T')
class TaskHandle(Generic[T]):
    ...
```

#### Async Patterns
```python
# Use context managers for resource management
async with Client.connect("http://localhost:52710") as client:
    handle = await client.submit_task(my_task, {"arg": "value"})
    result = await handle.wait()

# Prefer async/await over callbacks
result = await handle.wait()  # Good
handle.on_complete(callback)  # Avoid
```

#### Error Handling
```python
# Use specific exception types
try:
    result = await handle.wait()
except ConnectionError:
    # Handle connection issues
except TaskError as e:
    # Handle task execution errors
    if e.retryable:
        # Can retry
```

## Development Procedures

### Setting Up Development Environment

#### Rust Client
```bash
# Clone the repository
git clone https://github.com/azolla-io/azolla.git
cd azolla/clients/rust

# Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build both crates
cd azolla-macros && cargo build
cd ../azolla-client && cargo build

# Run tests
cd ../azolla-macros && cargo test
cd ../azolla-client && cargo test

# Test feature combinations
cd azolla-client
cargo test --no-default-features        # Client-only mode
cargo test --features macros            # With proc macros
```

#### Python Client
```bash
# Clone the repository
git clone https://github.com/azolla-io/azolla.git
cd azolla/clients/python

# Install Python 3.9+ (if not already installed)
# Use pyenv for version management
pyenv install 3.9.23
pyenv local 3.9.23

# Set up development environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -e ".[dev,testing,integration]"

# Run linting and type checking
ruff check src tests                    # Linting
black --check src tests                 # Format checking
mypy src                               # Type checking

# Run tests
pytest tests/unit/                     # Unit tests only
pytest tests/integration/              # Integration tests (requires orchestrator)
pytest --cov=azolla --cov-report=term-missing  # With coverage
```

### Code Organization Guidelines

#### Rust Client Structure

##### azolla-macros/src/lib.rs
- Single file containing the `azolla_task` proc macro implementation
- Generates PascalCase task structs (e.g., `GreetUserTask` from `greet_user`)
- Handles typed argument extraction from JSON using `FromJsonValue` trait
- Provides compile-time type checking and validation

##### azolla-client/src/ Structure
- **lib.rs**: Main library exports and feature-gated re-exports
- **client.rs**: Core gRPC client implementation with retry logic
- **error.rs**: Comprehensive error types (`AzollaError`, `TaskError`)
- **retry_policy.rs**: Configurable retry policies with exponential backoff
- **task.rs**: Task trait definition and execution context
- **worker.rs**: Single-task worker runtime with shepherd result reporting

#### Python Client Structure

##### src/azolla/ Organization
- **__init__.py**: Alphabetized exports with sorted `__all__` list
- **client.py**: Async gRPC client with connection pooling
- **worker.py**: Task execution engine with lifecycle management
- **task.py**: Decorator and base class for task definitions
- **types.py**: Common types (TaskResult, TaskStatus, etc.)
- **exceptions.py**: Error hierarchy with gRPC status mapping
- **retry.py**: Backoff strategies with secure randomness
- **cli.py**: Command-line interface for workers
- **_grpc/**: Generated protobuf code (excluded from linting)
- **_internal/**: Internal utilities and helpers

##### Python Code Standards
- **Type hints**: Required for all public APIs
- **Docstrings**: Google-style docstrings for all public functions
- **Import sorting**: Use isort/ruff for alphabetized imports
- **Error handling**: Specific exception types, avoid bare except
- **Async/await**: Prefer over callbacks and blocking calls
- **Context managers**: For resource management (connections, files)
- **Pydantic models**: For data validation and serialization

### Adding New Features

#### Rust Client
1. **For proc macro changes**: Edit `azolla-macros/src/lib.rs`
2. **For client features**: Add to appropriate module in `azolla-client/src/`
3. **For new features**: Add feature flag to `azolla-client/Cargo.toml`
4. **Update documentation**: Both README files and inline docs
5. **Add tests**: Unit tests and integration tests
6. **Update examples**: Ensure examples demonstrate new features

#### Python Client
1. **For core features**: Add to appropriate module in `src/azolla/`
2. **For new exceptions**: Add to `exceptions.py` with proper hierarchy
3. **For CLI features**: Extend `cli.py` with new commands
4. **Update exports**: Add to `__init__.py` `__all__` list (keep sorted)
5. **Add tests**: Unit tests in `tests/unit/`, integration in `tests/integration/`
6. **Type safety**: Ensure full type hints and mypy compliance
7. **Documentation**: Update README.md and inline docstrings

### Tool Configuration

#### Python Linting and Formatting
The Python client uses modern tooling configured in `pyproject.toml`:

```toml
# Ruff for linting (replaces flake8, isort, pyupgrade)
[tool.ruff]
extend-exclude = ["src/azolla/_grpc"]  # Skip generated gRPC files

# Black for formatting
[tool.black]
extend-exclude = "src/azolla/_grpc/"   # Skip generated gRPC files

# MyPy for type checking
[tool.mypy]
exclude = ["src/azolla/_grpc/"]        # Skip generated gRPC files
```

**Important**: Always exclude `src/azolla/_grpc/` from linting tools as this contains generated protobuf code.

## Publishing Procedures

### Publishing Rust Client to crates.io

The Rust client follows a **coordinated release process** where both crates are published with the same version number.

### Pre-Publishing Checklist

```bash
# 1. Update version numbers in both Cargo.toml files
# azolla-macros/Cargo.toml
version = "0.1.1"

# azolla-client/Cargo.toml  
version = "0.1.1"
azolla-macros = "0.1.1"  # Update dependency version

# 2. Update CHANGELOG.md files (if they exist)
# 3. Run full test suite
cd azolla-macros && cargo test && cd ../azolla-client && cargo test

# 4. Verify packaging
cd azolla-macros && cargo package --list
cd ../azolla-client && cargo package --list

# 5. Test locally with published dependency structure
# (temporarily change azolla-macros dependency to version instead of path)
```

### Publishing Process

**Important**: Always publish `azolla-macros` first since `azolla-client` depends on it.

```bash
# 1. Login to crates.io (one-time setup)
cargo login <your-api-token>

# 2. Publish azolla-macros first
cd azolla-macros
cargo publish

# Wait 2-3 minutes for crates.io propagation

# 3. Update azolla-client dependency to published version
cd ../azolla-client
# Edit Cargo.toml: azolla-macros = "0.1.1" (remove path)
cargo check  # Verify it works with published version

# 4. Publish azolla-client
cargo publish

# 5. Revert azolla-client Cargo.toml back to path dependency for development
# azolla-macros = { path = "../azolla-macros" }
```

### Version Synchronization Strategy

Both crates **always use the same version number**:
- If only `azolla-macros` changes: bump both to 0.1.1
- If only `azolla-client` changes: bump both to 0.1.1  
- If both change: bump both to 0.1.1

This ensures predictable compatibility and simple user experience.

### Release Automation

A release script is provided to automate the coordinated publishing process:

```bash
# From clients/rust/ directory
./release.sh 0.1.1
```

The script automatically:
1. **Validates** version format, environment setup, and version consistency
2. **Updates** proto files from the main project to ensure synchronization
3. **Updates** azolla-client dependency to use published version
4. **Tests** all feature combinations thoroughly
5. **Publishes** azolla-macros first, then azolla-client after crates.io propagation
6. **Reverts** to development path dependencies
7. **Creates** git tag for the release

#### Usage Requirements

Before running the release script:

```bash
# Ensure you're in the right directory
cd clients/rust/

# Ensure git working directory is clean
git status

# Login to crates.io (one-time setup)  
cargo login <your-api-token>

# Run the release
./release.sh 0.1.1
```

#### Post-Release Steps

After the script completes:

```bash
# Push tag to repository
git push origin v0.1.1

# Verify publication
open https://crates.io/crates/azolla-client
open https://crates.io/crates/azolla-macros
```

#### Script Features

- **Pre-flight checks**: Validates environment, git status, and crates.io access
- **Proto synchronization**: Ensures client proto files match main project
- **Comprehensive testing**: Tests all feature combinations before publishing
- **Safe publishing**: Waits for crates.io propagation between publications
- **Development restoration**: Automatically reverts to path dependencies
- **Git integration**: Creates tagged releases
- **Error handling**: Stops on any failure with clear error messages

### Publishing Python Client to PyPI

The Python client uses **semantic versioning** with automated release script for PyPI publishing.

#### Pre-Publishing Checklist

```bash
# 1. Update version in src/azolla/_version.py
__version__ = "0.1.3"

# 2. Ensure all tests pass
cd clients/python
pytest tests/ --cov=azolla --cov-fail-under=51

# 3. Ensure linting passes
ruff check src tests
black --check src tests
mypy src

# 4. Update proto files from main project
cp ../../proto/*.proto src/azolla/_grpc/proto/

# 5. Verify package builds
python -m build
twine check dist/*
```

#### Publishing Process

**Requirements**: 
- PyPI account with `~/.pypirc` configured
- `build` and `twine` packages installed
- Clean git working directory

```bash
# Manual publishing steps
cd clients/python

# Build package
python -m build

# Check package
twine check dist/*

# Test publish to TestPyPI (optional)
twine upload --repository testpypi dist/*

# Publish to PyPI
twine upload dist/*

# Create git tag
git tag "python-v0.1.3" -m "Python client release v0.1.3"
```

#### Automated Release Script

```bash
# From clients/python/ directory
./release.sh 0.1.3
```

The script automatically:
1. **Validates** version format and environment setup
2. **Updates** proto files from main project
3. **Runs** comprehensive tests and linting
4. **Builds** and validates package
5. **Tests** on TestPyPI (if configured)
6. **Publishes** to production PyPI
7. **Verifies** installation from PyPI
8. **Creates** git tag for the release

#### Release Script Requirements

```bash
# Ensure you're on main branch (tests have passed)
git branch --show-current  # Should show "main"

# Ensure version matches in _version.py
grep '__version__' src/azolla/_version.py

# Configure PyPI authentication
cat ~/.pypirc  # Should contain [pypi] and optionally [testpypi] sections

# Run the release
./release.sh 0.1.3
```

#### Post-Release Steps

```bash
# Push tag to repository
git push origin python-v0.1.3

# Verify publication
open https://pypi.org/project/azolla/0.1.3/

# Test installation
pip install azolla==0.1.3
python -c "import azolla; print(azolla.__version__)"
```

#### Package Distribution Features

- **Modern packaging**: Uses `pyproject.toml` with Hatchling build system
- **Type information**: Includes `py.typed` marker for PEP 561 compliance
- **Multiple environments**: Supports Python 3.9-3.13
- **Optional dependencies**: Performance, monitoring, development extras
- **CLI entry point**: `azolla-worker` command for worker processes
- **Comprehensive metadata**: Keywords, classifiers, project URLs

## Protocol Buffer Management

Both client libraries use the same protocol definitions from the main project.

### Updating Protocols

#### Rust Client
1. **Update proto files**: Edit main project `proto/*.proto` files
2. **Sync to client**: Release script copies proto files automatically
3. **Regenerate code**: `cargo build` automatically runs `build.rs`
4. **Update client code**: Modify Rust code to match new protocols
5. **Test compatibility**: Ensure backward/forward compatibility
6. **Version bump**: Protocol changes usually require version bump

#### Python Client  
1. **Update proto files**: Edit main project `proto/*.proto` files
2. **Sync to client**: Release script copies proto files automatically
3. **Regenerate code**: Currently manual process using `grpcio-tools`
4. **Update client code**: Modify Python code to match new protocols
5. **Exclude from linting**: Generated files are excluded via `pyproject.toml`
6. **Version bump**: Protocol changes usually require version bump

### gRPC Code Generation

#### For Python Client
```bash
# From clients/python/ directory
python -m grpc_tools.protoc \
    --python_out=src/azolla/_grpc \
    --pyi_out=src/azolla/_grpc \
    --grpc_python_out=src/azolla/_grpc \
    --proto_path=src/azolla/_grpc/proto \
    src/azolla/_grpc/proto/*.proto
```

**Important**: Generated files in `src/azolla/_grpc/` are automatically excluded from linting tools (ruff, black, mypy) via `pyproject.toml` configuration.

## AI Agent Guidelines

When working with Azolla client libraries as an AI agent:

### General Principles
1. **Follow existing patterns**: Study the codebase structure before making changes
2. **Maintain type safety**: Rust and Python both emphasize type safety
3. **Test thoroughly**: Both unit and integration tests are required
4. **Document changes**: Update README files and inline documentation
5. **Use release scripts**: Prefer automated release scripts over manual publishing

### Language-Specific Guidelines

#### When Working with Rust
- Use `cargo fmt` and `cargo clippy` for formatting and linting
- Follow the two-crate architecture (azolla-client + azolla-macros)
- Test all feature combinations (`--no-default-features`, `--features macros`)
- Update version numbers in both crates simultaneously
- Use the `./release.sh` script for publishing

#### When Working with Python  
- Use `ruff`, `black`, and `mypy` for code quality
- Keep `__all__` lists sorted alphabetically within logical groups
- Exclude `src/azolla/_grpc/` from all linting tools
- Use `pytest` with coverage requirements (>80%)
- Follow async/await patterns consistently
- Use the `./release.sh` script for PyPI publishing

### Common Pitfalls to Avoid
1. **Don't lint generated gRPC code**: Always exclude `_grpc/` directories
2. **Don't break version synchronization**: Keep related packages in sync
3. **Don't skip integration tests**: They ensure client/server compatibility  
4. **Don't forget proto updates**: Sync protocol files during releases
5. **Don't publish without tests**: Both clients require passing test suites
