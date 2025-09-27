//! Integration test modules and utilities

pub mod client_integration;
pub mod end_to_end;
pub mod error_integration;
pub mod macro_integration;
pub mod worker_integration;

// Test utilities
pub mod test_utils;

// Re-export the real TestOrchestrator from test_utils
pub use test_utils::TestOrchestrator;

// Test utilities
use azolla_client::error::AzollaError;
use std::sync::Once;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

static INIT: Once = Once::new();

/// Initialize test environment
pub fn init_test_env() {
    INIT.call_once(|| {
        // Set up test environment
        std::env::set_var("RUST_LOG", "debug");
    });
}

/// Wait for condition with timeout
pub async fn wait_for_condition<F, Fut>(
    condition: F,
    timeout: Duration,
    check_interval: Duration,
) -> Result<(), AzollaError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if condition().await {
            return Ok(());
        }
        sleep(check_interval).await;
    }

    Err(AzollaError::InvalidConfig(
        "Timeout waiting for condition".to_string(),
    ))
}

/// Generate a random task ID
pub fn generate_task_id() -> String {
    Uuid::new_v4().to_string()
}
