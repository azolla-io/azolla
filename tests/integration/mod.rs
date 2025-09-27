//! Integration tests for the Azolla orchestrator system.
//!
//! This module contains comprehensive end-to-end tests that verify the complete
//! functionality of the orchestrator, including task execution, retry mechanisms,
//! and shepherd management. These tests require the `test-harness` feature to be enabled.
//!
//! ## Test Organization
//!
//! - **task_execution**: Basic task creation, execution, and result handling
//! - **retry_mechanism**: Task retry policies, scheduling, and race condition handling  
//! - **shepherd_management**: Shepherd lifecycle, registration, and cluster coordination
//! - **life_cycle**: Orchestrator startup, shutdown, and graceful termination scenarios
//!
//! ## Running Integration Tests
//!
//! Due to resource contention (ports, database containers), integration tests should
//! be run sequentially:
//!
//! ```bash
//! cargo test --features test-harness -- --test-threads=1
//! ```
//!
//! ## Test Requirements
//!
//! - PostgreSQL container support (testcontainers)
//! - Available ports for orchestrator and shepherd services
//! - Worker binary (`azolla-worker`) built and available

pub mod group_routing;
pub mod life_cycle;
pub mod retry_mechanism;
pub mod shepherd_management;
pub mod task_execution;

use tokio::time::Instant;

pub async fn poll_until<T, F, Fut>(
    timeout: std::time::Duration,
    interval: std::time::Duration,
    mut check: F,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return Some(value);
        }

        if Instant::now() >= deadline {
            return None;
        }

        tokio::time::sleep(interval).await;
    }
}
