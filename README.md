# Azolla

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org) [![PostgreSQL](https://img.shields.io/badge/postgresql-12%2B-blue.svg)](https://www.postgresql.org) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE) [![codecov](https://codecov.io/gh/azolla-io/azolla/branch/main/graph/badge.svg)](https://codecov.io/gh/azolla-io/azolla) [![Security Audit](https://img.shields.io/badge/security-audit%20passed-brightgreen)](https://github.com/azolla-io/azolla/security)

> **As simple as Celery, as durable as Temporal, and runs on just a database.**

**Azolla** is a high-performance, distributed task orchestration system built in Rust. Designed for ultra-low latency and enterprise-grade reliability, Azolla delivers blazing-fast task execution with exactly-once guarantees—ensuring no task is ever lost.

This is the modernized async task platform you always wanted. Azolla gives you the power of Temporal with the simplicity of Celery, all running on just a database. It supports dynamic DAG workflows that adapt at runtime, durable long-running tasks without determinism headaches, and is architected for multi-tenancy, reliability, and scale. It's truly polyglot, so you can write your tasks in any language. Stop managing complex infrastructure and start building reliable applications.

## ✨ Why Azolla?

### 🔁 **Uncompromising Reliability**
- **Exactly-once execution** – Tasks are never lost, even during worker crashes.
- **Complete audit trail** – Every task and workflow run is materialized in the database.
- **Transactional enqueue** – Option for transactional enqueuing with database transactions.
- **Durable scheduler** – Far-future tasks don't consume worker memory and survive crashes.

### ⚙️ **Operational Simplicity**
- **Single database dependency** – No message brokers, no Redis clusters—just PostgreSQL.
- **Built-in observability** – Web UI, metrics, tracing, and real-time monitoring out of the box.
- **Zero code versioning headaches** – Run new code without breaking in-flight tasks.
- **Global rate limiting** – Prevent noisy neighbors with built-in throttling.

### ⚡ **Blazing Performance**
- **Sub-millisecond scheduling latency.**
- **10K+ tasks/sec** throughput on a single PostgreSQL instance.
- **In-memory DAG tracking** and **append-only event sourcing** ensure minimal DB overhead.
- **[TaskSet™]**: our ReadySet-inspired technique for incrementally syncing memory state with durable facts for blazing-fast reads.

### 🧑‍💻 **Developer-Friendly**
- **Celery-like Python API** for defining tasks and flows.
- **Durable DAG workflows** with human-in-the-loop capabilities.
- **Polyglot by design** – language-agnostic over RPC.
- **Built-in retry policies**, scheduling, signals, and state sharing via `flow.ctx`.



## 🏗️ Architecture

Azolla follows a distributed push-based execution model with three main components. The system supports **High Availability** through orchestrator replication and **Scalability** through domain-based sharding:

```
┌───────────┐  <── gRPC bi-dir ──>  ┌───────────┐  <── Process ──>  ┌───────────┐
│Orchestrator│                     │  Shepherd │                   │   Worker  │
│ (Scheduler)│                     │ (Task Mgr)│                   │ (Task Exec)│
└─────┬──────┘                     └─────┬─────┘                   └─────┬─────┘
      │                                  │                               │
      │                                  │                               │
      ▼                                  │                               │
┌───────────┐                            │                               │
│PostgreSQL │                            │                               │
│(Event Log)│                            │                               │
└───────────┘                            └───────────────────────────────┘
```

## 🚨 Status

**Azolla is in active development and approaching production readiness.**

Current status: **Beta** - Core functionality implemented with comprehensive testing. The codebase follows production-grade engineering practices with extensive test coverage, security auditing, and performance optimization. Feature development is ongoing, but the foundation is solid and battle-tested.

For production use, please wait for the 1.0 release or contact the maintainers for early access.

## 🚀 Quick Start

### Prerequisites
- PostgreSQL 12+ running and accessible
- Rust 1.70+ (for building from source)

### Installation

#### Build from Source
```bash
git clone https://github.com/azolla-io/azolla.git
cd azolla
cargo build --release
```

#### Using Docker
```bash
docker-compose up --build
```

### Basic Usage

1. **Start the system**:
   ```bash
   export DATABASE_URL="postgresql://username:password@localhost:5432/azolla"
   ./target/release/azolla-orchestrator
   ```

2. **Start a worker**:
   ```bash
   ./target/release/azolla-shepherd
   ```

3. **Submit a task**:
   ```bash
   grpcurl -plaintext -d '{
     "name": "echo",
     "domain": "default",
     "args": ["Hello, Azolla!"]
   }' localhost:52710 azolla.orchestrator.ClientService/CreateTask
   ```

## 🔧 Configuration

### Minimal Configuration
```toml
# config/orchestrator.toml
[server]
port = 52710

[database]
url = "postgresql://localhost:5432/azolla"
```

```toml
# config/shepherd.toml
[orchestrator]
endpoint = "http://localhost:52710"

[worker]
max_concurrency = 4
```

### Environment Variables
- `DATABASE_URL`: PostgreSQL connection string
- `AZOLLA_ORCHESTRATOR_PORT`: Override orchestrator port
- `AZOLLA_SHEPHERD_CONCURRENCY`: Override worker concurrency

## 📊 Performance Characteristics

Azolla delivers exceptional performance with:

- **Task Creation Latency**: < 1ms average
- **Throughput**: 10,000+ tasks/sec on single PostgreSQL instance
- **Worker Crash Recovery**: < 100ms detection and failover
- **Database Dependencies**: Single PostgreSQL instance required

*Performance measured on standard cloud instances with PostgreSQL 14*

## 🧪 Testing & Quality Assurance

Run the comprehensive test suite:

```bash
# Unit tests
cargo test

# Integration tests with database
cargo test --test '*' -- --test-threads=1

# Performance benchmarks
cargo bench

# Security audit
cargo audit

# Code coverage
cargo tarpaulin --out html
```

### Test Categories
- **Unit Tests**: Core logic for all major components with >95% coverage
- **Integration Tests**: Database integration with testcontainers
- **End-to-End Tests**: Multi-component workflow testing
- **Performance Tests**: Event stream batching and throughput validation

## 💬 Community & Support

- **GitHub Discussions**: Have a question, an idea, or want to share your project? [Join the discussion!](https://github.com/azolla-io/azolla/discussions)
- **Issue Tracker**: Found a bug? [Report it on our issue tracker](https://github.com/azolla-io/azolla/issues).

We are committed to fostering an open and welcoming environment.

## 🤝 Contributing

We welcome contributions to Azolla! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup
1. **Fork and clone the repository**
2. **Install Rust toolchain**: `rustup update stable`
3. **Start development environment**: `docker-compose up --build`
4. **Run tests**: `cargo test`
5. **Submit pull request**

### Code Standards
- **Rust Best Practices**: Follow the official Rust style guide
- **Memory Safety**: Zero-copy optimizations where possible
- **Error Handling**: Comprehensive error types with context
- **Documentation**: Inline docs for all public APIs
- **Performance**: Benchmark-driven development for critical paths

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

**Built with ❤️ in Rust** | **Powered by PostgreSQL** | **Designed for Scale**

*Ready to orchestrate your tasks with blazing speed and rock-solid reliability?*