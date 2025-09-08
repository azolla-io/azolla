# Azolla Client Libraries - Developer Guide

This directory contains client library implementations for different programming languages. This document provides development and publishing guidance for maintainers.

## Directory Structure

```
clients/
├── README.md                       # This file - development guide
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
│   │   │   └── worker.rs         # Worker building (stub for examples)
│   │   ├── proto/                # Protocol buffer definitions
│   │   │   ├── orchestrator.proto
│   │   │   └── common.proto
│   │   ├── examples/             # Usage examples
│   │   ├── LICENSE-APACHE        # Apache 2.0 license
│   │   └── PUBLISHING.md         # Publishing instructions
│   └── azolla-macros/            # Procedural macro support crate
│       ├── Cargo.toml           # Proc macro package metadata
│       ├── README.md            # Macro-specific documentation
│       ├── src/lib.rs           # Proc macro implementation
│       └── LICENSE-APACHE       # Apache 2.0 license
└── python/                       # Python client (planned)
    └── .gitkeep                  # Placeholder for future implementation
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

## Development Procedures

### Setting Up Development Environment

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

### Code Organization Guidelines

#### azolla-macros/src/lib.rs
- Single file containing the `azolla_task` proc macro implementation
- Generates PascalCase task structs (e.g., `GreetUserTask` from `greet_user`)
- Handles typed argument extraction from JSON using `FromJsonValue` trait
- Provides compile-time type checking and validation

#### azolla-client/src/ Structure
- **lib.rs**: Main library exports and feature-gated re-exports
- **client.rs**: Core gRPC client implementation with retry logic
- **error.rs**: Comprehensive error types (`AzollaError`, `TaskError`)
- **retry_policy.rs**: Configurable retry policies with exponential backoff
- **task.rs**: Task trait definition and execution context
- **worker.rs**: Worker builder for examples (minimal implementation)

### Adding New Features

1. **For proc macro changes**: Edit `azolla-macros/src/lib.rs`
2. **For client features**: Add to appropriate module in `azolla-client/src/`
3. **For new features**: Add feature flag to `azolla-client/Cargo.toml`
4. **Update documentation**: Both README files and inline docs
5. **Add tests**: Unit tests and integration tests
6. **Update examples**: Ensure examples demonstrate new features

## Publishing to crates.io

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
2. **Updates** azolla-client dependency to use published version
3. **Tests** all feature combinations thoroughly
4. **Publishes** azolla-macros first, then azolla-client after crates.io propagation
5. **Reverts** to development path dependencies
6. **Creates** git tag for the release

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
- **Comprehensive testing**: Tests all feature combinations before publishing
- **Safe publishing**: Waits for crates.io propagation between publications
- **Development restoration**: Automatically reverts to path dependencies
- **Git integration**: Creates tagged releases
- **Error handling**: Stops on any failure with clear error messages

## Protocol Buffer Management

Both client crates use the same protocol definitions:

### Updating Protocols

1. **Update proto files**: Edit `azolla-client/proto/*.proto`
2. **Regenerate code**: `cargo build` automatically runs `build.rs` 
3. **Update client code**: Modify Rust code to match new protocols
4. **Test compatibility**: Ensure backward/forward compatibility
5. **Version bump**: Protocol changes usually require version bump