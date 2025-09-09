# Azolla Examples

This directory contains usage examples for the Azolla distributed task processing platform.

## Available Examples

### 🔧 Worker Example (`worker_example.rs`)

Demonstrates how to create a worker that processes tasks using both implementation approaches:

```bash
cargo run --example worker_example
```

**Features shown:**
- Manual trait implementation (verbose but explicit)
- Proc macro implementation (`#[azolla_task]`) - convenient and type-safe
- Type-safe argument parsing
- Task registration and worker configuration

### 📡 Client Example (`client_example.rs`)

Shows how to create a client and submit tasks to the orchestrator:

```bash
cargo run --example client_example
```

**Features shown:**
- Client configuration and connection
- Task submission with typed arguments
- Retry policy configuration
- Error handling

### 🔮 Proc Macro Tasks Example (`proc_macro_tasks.rs`)

Focused example of the `#[azolla_task]` procedural macro functionality:

```bash
cargo run --example proc_macro_tasks
```

**Features shown:**
- Function-to-task conversion using `#[azolla_task]`
- Automatic type-safe argument parsing
- Error handling in macro-generated tasks
- Integration with worker and client

## Implementation Approaches

### 1. Proc Macro (`#[azolla_task]`)
✅ **Pros:**
- Less boilerplate code
- Automatic struct generation  
- Type-safe argument parsing
- Clean, readable function definitions

❌ **Cons:**
- Less control over argument parsing
- Macro complexity for debugging

### 2. Manual Trait Implementation
✅ **Pros:**
- Full control over argument types
- Explicit and debuggable
- Custom argument parsing logic
- Better for complex task requirements

❌ **Cons:**
- More verbose boilerplate
- Manual struct and trait implementations

## Running Examples

All examples can be run from the repository root using:

```bash
cargo run --example <example_name>
```

The examples will attempt to connect to local Azolla services but will gracefully handle connection failures, showing you how the library works even without a full deployment.

## Next Steps

After exploring these examples:

1. **[📖 Read the Full Documentation](../docs/client_library.md)** for comprehensive guides
2. **Start with the worker example** to understand task processing
3. **Try the client example** to learn task submission
4. **Experiment with proc macros** for rapid development

For production deployment, you'll need to start the Azolla orchestrator and configure your workers to connect to it.