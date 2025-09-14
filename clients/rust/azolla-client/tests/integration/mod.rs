//! Integration test modules and utilities

pub mod client_integration;
pub mod end_to_end;
pub mod error_integration;
pub mod macro_integration;
pub mod worker_integration;

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

/// Mock orchestrator for testing
pub struct TestOrchestrator {
    endpoint: String,
    port: u16,
}

impl TestOrchestrator {
    /// Start a mock orchestrator
    pub async fn start() -> Result<Self, AzollaError> {
        // Mock implementation - just returns a test instance
        let port = 52710; // Default test port
        let endpoint = format!("http://localhost:{port}");

        Ok(TestOrchestrator { endpoint, port })
    }

    /// Get the orchestrator endpoint
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    /// Shutdown the orchestrator
    pub async fn shutdown(self) -> Result<(), AzollaError> {
        // Mock shutdown
        Ok(())
    }
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
