# Azolla

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org) [![PostgreSQL](https://img.shields.io/badge/postgresql-12%2B-blue.svg)](https://www.postgresql.org) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

<div align="center">

**🚀 Stop fighting brittle task queues. Start building with Azolla.**

[**⭐ Star this repo**](https://github.com/azolla-io/azolla) | [**🍴 Fork & contribute**](https://github.com/azolla-io/azolla/fork) | [**📖 Read the docs**](docs/)

*Built with ❤️ in Rust | Ridiculously simple, yet high-performance and reliable*

</div>

> **The reliable, high-performance, yet simple async task platform**

**Azolla** delivers enterprise-grade task orchestration with breakthrough simplicity. Built in Rust, it achieves **sub-millisecond latency** and **10K+ tasks/sec** throughput while running on just PostgreSQL—no complex infrastructure required.

> ⚠️ **Development Status**: Azolla is under active development and ready for testing. Production readiness is expected in the coming months.

---

## 🚀 Why Azolla?

**Simple**: Only depend on PostgreSQL. No Redis, no message broker, no etcd/ZooKeeper.  
**Fast**: Sub-millisecond scheduling with 10K+ tasks/sec throughput.  
**Reliable**: Exactly-once execution with complete audit trails.  
**Polyglot**: Write tasks in any language via high-performance gRPC.  
**Scalable**: Horizontally scalable with domain based sharding and isolation.

## ⚡ Core Innovations

### **EventStream** — Durable High-Throughput Events
Append-only event sourcing delivers blazing-fast writes with zero data loss. Every task execution is captured in an immutable log, ensuring complete durability and full audit trails.

### **TaskSet** — Lightning-Fast Scheduling  
Inspired by ReadySet, TaskSet incrementally synchronizes in-memory state with durable database facts. This hybrid approach eliminates database latency while maintaining full persistence.

### **Push-Based Dispatching** — Ultra-Low Latency
Workers receive tasks instantly through optimized push delivery, eliminating polling overhead. Tasks are dispatched in sub-millisecond timeframes.

### **Polyglot gRPC Interface** — True Multi-Language Support
Write tasks in any language through our high-performance gRPC API. **Python and Rust officially supported**, with easy extension to other languages. Seamlessly integrates with your existing microservice architecture—no rewrites, no vendor lock-in, just drop-in orchestration for your current services.

### **Shepherd-Managed Workers** — Fault-Isolated Execution
Azolla’s shepherd spawns a dedicated worker process per task, giving you fault containment, per-task isolation, and comprehensive crash visibility—the shepherd remains healthy even when user code panics, and it records the failure for later inspection.

---

## 🏃 Quick Start

### 1. Start Azolla
```bash
# Prerequisites: PostgreSQL 12+
export DATABASE_URL="postgresql://localhost:5432/azolla"

# Start orchestrator
cargo run --bin azolla-orchestrator

# Start worker (separate terminal)
cargo run --bin azolla-shepherd
```

### 2. Write and Submit Your First Task

**Rust Example:**

```rust
// Define a task with the proc macro
use azolla_client::{azolla_task, TaskError};
use serde_json::{json, Value};

#[azolla_task]
async fn greet_user(name: String, age: u32) -> Result<Value, TaskError> {
    Ok(json!({
        "greeting": format!("Hello {name}! You are {age} years old."),
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

// Submit the task from a client
use azolla_client::{Client, TaskExecutionResult};

#[tokio::main]
async fn main() -> Result<(), azolla_client::AzollaError> {
    let client = Client::connect("http://localhost:52710").await?;
    
    let task = client
        .submit_task("greet_user")
        .args(("Alice".to_string(), 25u32))?
        .submit()
        .await?;
    
    match task.wait().await? {
        TaskExecutionResult::Success(result) => println!("✅ {}", result),
        TaskExecutionResult::Failed(error) => println!("❌ {}", error),
    }
    
    Ok(())
}
```

**Python Example:**
```python
import asyncio
from azolla import Client, azolla_task, Worker
from azolla.retry import RetryPolicy, ExponentialBackoff

# Define a task with the decorator
@azolla_task
async def greet_user(name: str, age: int) -> dict:
    return {
        "greeting": f"Hello {name}! You are {age} years old.",
        "timestamp": "2025-01-15T10:30:00Z"
    }

# Submit the task from a client
async def main():
    async with Client.connect("http://localhost:52710") as client:
        # Submit task with retry policy
        handle = await (
            client.submit_task(greet_user, {
                "name": "Alice", 
                "age": 25
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
            print(f"✅ {result.value}")
        else:
            print(f"❌ {result.error}")

asyncio.run(main())
```

**[📖 Full Documentation](clients/) | [🦀 Rust Client Guide](clients/rust/README.md) | [🐍 Python Client Guide](clients/python/README.md)**

---

## 🔥 Performance

| Metric | Performance |
|--------|-------------|
| **Task Creation** | < 1ms average |
| **Throughput** | 10,000+ tasks/sec |
| **Crash Recovery** | < 100ms |
| **Dependencies** | PostgreSQL only |

*Measured on standard cloud instances*

---

## 🏗️ Architecture

```
┌─ EXECUTION LAYER ─────────────────────────────────────┐
│  Workers (Rust/Python/Any Language)                   │
│  ⚡ Execute tasks with sub-ms latency                  │
└────────────┬──────────────────────────────────────────┘
             │ gRPC Push-Based Dispatch
┌─ ORCHESTRATION LAYER ─────────────────────────────────┐  
│  🧠 EventStream: Durable event sourcing               │
│  ⚡ TaskSet™: In-memory scheduling with persistence    │
│  📡 Push-based dispatch for instant delivery          │
└────────────┬──────────────────────────────────────────┘
             │ High-performance writes
┌─ PERSISTENCE LAYER ───────────────────────────────────┐
│  🗃️ PostgreSQL: Single source of truth                │
│  📊 Indexed task states & event log                   │
└───────────────────────────────────────────────────────┘
```

---

## 🛠️ Use Cases

- **Microservices Orchestration**: Coordinate complex workflows across services
- **Data Processing Pipelines**: ETL jobs with reliability guarantees  
- **Background Job Processing**: User uploads, email sending, report generation
- **Multi-Language Teams**: Rust performance with Python/JS task logic

---

## 🚀 Get Started

### Installation

```bash
# Build from source
git clone https://github.com/azolla-io/azolla.git
cd azolla
cargo build --release
```

```bash
# Or use Docker
docker-compose up --build
```

### Client Libraries

- **🦀 Rust**: `azolla-client = { version = "0.1.0", features = ["macros"] }` - Type-safe with proc macro support  
  **[📖 Rust Documentation](clients/rust/README.md)**
- **🐍 Python**: `pip install azolla` - Modern async/await with type hints and retry policies  
  **[📖 Python Documentation](clients/python/README.md)**
- **🟨 JavaScript**: Coming soon - `npm install azolla-client`
- **🌐 Any Language**: Use gRPC directly

See [`clients/`](clients/) directory for all available and planned client libraries.

---

## 🤝 Community

- **💬 [Discussions](https://github.com/azolla-io/azolla/discussions)**: Questions, ideas, showcase your projects
- **🐛 [Issues](https://github.com/azolla-io/azolla/issues)**: Bug reports and feature requests
- **📖 [Contributing](CONTRIBUTING.md)**: Join our growing community

---

## 📝 License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.
