//! Main integration test suite for Azolla orchestrator.
//!
//! This file serves as the entry point for all integration tests. The actual tests
//! are organized in the `integration/` module by functionality.

#![cfg(feature = "test-harness")]

mod integration;

// Re-export all integration test modules for cargo test discovery
pub use integration::*;
