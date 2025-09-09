# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-09-09

### Added
- Initial release of Azolla Python client library
- `@azolla_task` decorator for convenient task definition
- `Task` base class for explicit task implementation
- `Client` for task submission with builder pattern API
- `Worker` for task processing with gRPC streaming
- Comprehensive retry policies with exponential/linear/fixed backoff strategies
- Type-safe argument validation using Pydantic v2
- Async/await support throughout the API
- CLI worker with `azolla-worker` command
- Full test suite with unit and integration tests
- Production-ready error handling and logging
- PyPI-compatible package structure
- Comprehensive documentation and examples

### Features
- Python 3.9+ support with full type hints
- gRPC-based communication with Azolla orchestrator
- Automatic connection management and reconnection
- Configurable concurrency limits and heartbeat intervals  
- Support for shepherd groups and domains
- Task context with retry attempt tracking
- Graceful shutdown handling with signal support
- Optional performance optimizations (uvloop, orjson)
- Optional monitoring support (Prometheus, OpenTelemetry)

[Unreleased]: https://github.com/azolla-io/azolla/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/azolla-io/azolla/releases/tag/v0.1.0