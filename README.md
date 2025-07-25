# Azolla

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org) [![PostgreSQL](https://img.shields.io/badge/postgresql-12%2B-blue.svg)](https://www.postgresql.org) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> **The reliable, high-performance, yet simple async task platform**

**Azolla** delivers enterprise-grade task orchestration with breakthrough simplicity. Built in Rust, it achieves **sub-millisecond latency** and **10K+ tasks/sec** throughput while running on just PostgreSQL—no complex infrastructure required.

**Stop fighting brittle task queues. Start building with Azolla.**

---

## 🚀 Why Azolla?

**Simple**: One database dependency. No Redis, no Kafka, no complex setup.  
**Fast**: Sub-millisecond scheduling with 10K+ tasks/sec throughput.  
**Reliable**: Exactly-once execution with complete audit trails.  
**Polyglot**: Write tasks in any language via high-performance gRPC.

## ⚡ Core Innovations

### **EventStream** — Durable High-Throughput Events
Append-only event sourcing delivers blazing-fast writes with zero data loss. Every task execution is captured in an immutable log, ensuring complete durability and full audit trails.

### **TaskSet™** — Lightning-Fast Scheduling  
Inspired by ReadySet, TaskSet incrementally synchronizes in-memory state with durable database facts. This hybrid approach eliminates database latency while maintaining full persistence.

### **Push-Based Dispatching** — Ultra-Low Latency
Workers receive tasks instantly through optimized push delivery, eliminating polling overhead. Tasks are dispatched in sub-millisecond timeframes.

### **Polyglot gRPC Interface** — True Multi-Language Support
Write tasks in any language through our high-performance gRPC API. **Python and Rust officially supported**, with easy extension to other languages.

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

### 2. Submit Your First Task
```python
# Python client
import azolla

client = azolla.Client("localhost:52710")
result = client.submit_task("echo", args=["Hello, Azolla!"])
print(result)  # "Hello, Azolla!"
```

```bash
# Or via gRPC
grpcurl -plaintext -d '{
  "name": "echo",
  "domain": "default", 
  "args": ["Hello, Azolla!"]
}' localhost:52710 azolla.orchestrator.ClientService/CreateTask
```

**[📖 Full Documentation](docs/) | [🐍 Python Guide](examples/python/) | [🦀 Rust Guide](examples/rust/)**

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

### Language Support

- **🐍 Python**: `pip install azolla-client` 
- **🦀 Rust**: `cargo add azolla-client`
- **🌐 Any Language**: Use gRPC directly

---

## 📊 Production Ready

✅ **Comprehensive Testing**: 95%+ test coverage with integration tests  
✅ **Performance Benchmarks**: Continuous performance validation  
✅ **Memory Safety**: Built in Rust with zero-copy optimizations  
✅ **Transactional Guarantees**: ACID compliance through PostgreSQL  

**Status**: Beta - Core functionality complete, approaching 1.0 release

---

## 🤝 Community

- **💬 [Discussions](https://github.com/azolla-io/azolla/discussions)**: Questions, ideas, showcase your projects
- **🐛 [Issues](https://github.com/azolla-io/azolla/issues)**: Bug reports and feature requests
- **📖 [Contributing](CONTRIBUTING.md)**: Join our growing community

---

## 📝 License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

---

<div align="center">

**🚀 Ready to supercharge your async tasks?**

[**⭐ Star this repo**](https://github.com/azolla-io/azolla) | [**🍴 Fork & contribute**](https://github.com/azolla-io/azolla/fork) | [**📖 Read the docs**](docs/)

*Built with ❤️ in Rust | Powered by PostgreSQL | Designed for Scale*

</div>