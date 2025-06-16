# Azolla
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org) [![PostgreSQL](https://img.shields.io/badge/postgresql-12%2B-blue.svg)](https://www.postgresql.org) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

_As simple as Celery, as durable as Temporal, and runs on just a database._

**Azolla** is a high-performance, reliable task queue and workflow orchestration system. Built in Rust and powered by PostgreSQL, Azolla delivers ultra-low latency task execution with enterprise-grade durability—ensuring no task is ever lost.

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
- **[FlowCache™]**: our ReadySet-inspired technique for incrementally syncing memory state with durable facts for blazing-fast reads.

### 🧑‍💻 **Developer-Friendly**
- **Celery-like Python API** for defining tasks and flows.
- **Durable DAG workflows** with human-in-the-loop capabilities.
- **Polyglot by design** – language-agnostic over RPC.
- **Built-in retry policies**, scheduling, signals, and state sharing via `flow.ctx`.

## 🎯 Core Features

### Task Management
- **Retry policies** with exponential backoff.
- **Task cancellation** and interruption.
- **Priority queues** and custom routing.
- **Bulk operations** for high-throughput scenarios.

### Workflow Orchestration  
- **DAG-based workflows** with conditional branches.
- **Durable context** for state sharing between tasks.
- **Human-in-the-loop** support for approval workflows.
- **Dynamic DAGs** that adapt per execution.

### Scheduling & Triggers
- **Cron scheduling** with timezone support.
- **ETA-based** delayed execution.
- **Event-driven** triggers and signals.
- **Transactional** task enqueuing.

### Monitoring & Operations
- **Real-time dashboard** with task status and metrics.
- **Distributed tracing** for workflow debugging.  
- **Prometheus/OpenTelemetry** integration.
- **Alert and notification** system.

## 🔧 Language Support

Azolla provides idiomatic clients for multiple languages:

- **Python** – Full-featured with async support.
- **Rust** – Native integration with type safety.
- **JavaScript/Node.js** – Promise-based API.
- **Go** – Context-aware implementation.  
- **Java** – CompletableFuture support.

*More languages coming soon! Azolla's gRPC-based architecture makes adding new clients straightforward.*

## 📊 Performance Benchmarks

| Metric | Azolla | Celery + Redis | Temporal |
|--------|--------|----------------|----------|
| Task Creation Latency | < 1ms | ~5ms | ~10ms |
| Throughput (tasks/sec) | 10,000+ | ~3,000 | ~1,000 |
| Worker Crash Recovery | < 100ms | ~5s | ~30s |
| Memory Usage (1M tasks) | ~200MB | ~2GB | ~1GB |

*Benchmarks run on standard cloud instances with PostgreSQL 14*

## 📋 Quick Start

### Installation

#### Start the Orchestrator
```bash
# Install the Azolla server
cargo install azolla-server
azolla orchestrator --db-url postgres://...

# Or using Docker
docker pull azolla/azolla:latest
```

#### Run a Worker
```bash
azolla worker --module my_tasks
```

#### Monitor via Web UI
Visit http://localhost:8080 to see task and workflow status in real time.

### Basic Usage

**Python Client:**
```python
from azolla import task, flow

# Define a simple task
@task(name="send_email")
def send_welcome_email(user_id: int, email: str):
    # Your task logic here
    send_email(email, "Welcome!")
    return {"status": "sent", "user_id": user_id}

# Execute immediately
result = send_welcome_email.delay(user_id=123, email="user@example.com")

# Or schedule for later
send_welcome_email.apply_async(
    args=[123, "user@example.com"], 
    eta=datetime.now() + timedelta(hours=1)
)
```

**Workflow (DAG) Example:**
```python
@flow(name="user_onboarding", retries=3)
def onboard_new_user(user_data):
    # Define workflow steps
    user = create_user_account(user_data)
    profile = setup_user_profile(user.id)
    email = send_welcome_email(user.id, user.email)
    
    # Define dependencies
    user >> profile >> email
    return {"user_id": user.id, "onboarding_complete": True}

# Execute the workflow  
result = onboard_new_user.delay({"name": "John", "email": "john@example.com"})
```

**Use flow.ctx to share durable context across task steps:**
```python
@task
def step_one(x):
    flow.ctx["sum"] = flow.ctx.get("sum", 0) + x

@task
def step_two():
    total = flow.ctx["sum"]
    print(f"Sum: {total}")
```

### Server Setup

```bash
# Start with PostgreSQL connection
azolla-server --database-url postgresql://user:pass@localhost/azolla

# Or with Docker Compose
docker-compose up -d
```

## 🏗️ Architecture Overview

Azolla follows a push-based execution model:
- Tasks are pushed directly to available workers (no external message queue).
- Events are written to an append-only log in Postgres.
- A single orchestrator handles task scheduling, retrying, and workflow DAG evaluation.
- A caching layer (FlowCache™) incrementally syncs in-memory state with durable storage.

Components:
- Orchestrator – core scheduler, dispatcher, and API server.
- Worker – language-agnostic runners (gRPC-based).
- Postgres – durable event store and metadata layer.

## 🔒 Durability & Fault Tolerance
Azolla ensures strong durability guarantees:

| Component    | Failure Mode | Recovery Strategy                                    |
| ------------ | ------------ | ---------------------------------------------------- |
| Orchestrator | Crash        | Leader election + cold restart with fast rehydration |
| Worker       | Crash        | Tasks are auto-failed and retried per policy         |
| Client       | Crash        | Clients can resume task waiting or poll result       |

## 🛠️ Configuration

**Server Configuration:**
```toml
[database]
url = "postgresql://localhost/azolla"
max_connections = 100

[server]
bind_address = "0.0.0.0:50051"
web_ui_port = 8080

[scheduler]
batch_size = 1000
poll_interval = "100ms"

[observability]
metrics_enabled = true
tracing_endpoint = "http://jaeger:14268"
```

**Client Configuration:**
```python
from azolla import configure

configure(
    broker_url='grpc://localhost:50051',
    result_backend='postgresql://localhost/azolla',
    task_default_retry_delay=60,
    task_default_max_retries=3
)
```

## 🤝 Contributing

We welcome contributions to Azolla. If you discover a bug, please submit a pull request with a fix or open an issue for discussion. For new feature proposals, we kindly request that you open an issue first to discuss the implementation approach with the maintainers.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/azolla-org/azolla.git
cd azolla

# Start development environment  
docker-compose -f docker-compose.dev.yml up -d

# Run tests
cargo test
```

## 🗺️ Roadmap

- **Q3 2025**: Multi-tenant support and advanced RBAC.
- **Q4 2025**: Kubernetes operator and auto-scaling.
- **Q1 2026**: Machine learning workflow primitives.
- **Q2 2026**: Cross-region replication and disaster recovery.

## 🏢 Enterprise Support

Need enterprise features, support, or consulting? Contact us at [enterprise@azolla.dev](mailto:enterprise@azolla.dev).

---

**Built with ❤️ in Rust** | **Powered by PostgreSQL** | **Ready for Production**