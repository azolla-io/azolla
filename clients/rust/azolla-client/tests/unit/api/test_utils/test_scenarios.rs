use super::TestOrchestrator;
use anyhow::Result;
use azolla_client::client::ClientConfig;
use azolla_client::Client;
use futures::future;
use std::time::Duration;

/// Common test scenario utilities
pub struct TestScenarios;

impl TestScenarios {
    /// Create a client connected to the test orchestrator
    pub async fn create_client(orchestrator: &TestOrchestrator) -> Result<Client> {
        let config = ClientConfig {
            endpoint: orchestrator.client_endpoint(),
            domain: "test".to_string(),
            timeout: Duration::from_secs(10),
        };

        Client::with_config(config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create client: {e}"))
    }

    /// Wait for a condition with timeout
    pub async fn wait_for_condition<F, Fut>(
        condition: F,
        timeout_duration: Duration,
        check_interval: Duration,
    ) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            if condition().await {
                return Ok(());
            }
            tokio::time::sleep(check_interval).await;
        }

        anyhow::bail!("Condition not met within timeout of {timeout_duration:?}")
    }

    /// Simulate network delay
    pub async fn simulate_network_delay(delay: Duration) {
        tokio::time::sleep(delay).await;
    }

    /// Create a client with custom timeouts for testing
    pub async fn create_client_with_timeout(
        orchestrator: &TestOrchestrator,
        timeout: Duration,
    ) -> Result<Client> {
        let config = ClientConfig {
            endpoint: orchestrator.client_endpoint(),
            domain: "test".to_string(),
            timeout,
        };

        Client::with_config(config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create client with timeout: {e}"))
    }

    /// Try to create a client with invalid endpoint for error testing
    pub async fn try_create_client_with_invalid_endpoint(
    ) -> Result<Client, azolla_client::error::AzollaError> {
        let config = ClientConfig {
            endpoint: "http://invalid-host:99999".to_string(),
            domain: "test".to_string(),
            timeout: Duration::from_secs(1),
        };

        Client::with_config(config).await
    }

    /// Submit a task and wait for completion
    pub async fn submit_and_wait(
        client: &Client,
        task_name: &str,
        args: serde_json::Value,
    ) -> Result<azolla_client::client::TaskExecutionResult> {
        let handle = client.submit_task(task_name).args(args)?.submit().await?;

        handle
            .wait()
            .await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {e}"))
    }

    /// Submit multiple tasks concurrently
    pub async fn submit_concurrent_tasks(
        client: &Client,
        task_configs: Vec<(&str, serde_json::Value)>,
    ) -> Result<Vec<azolla_client::client::TaskExecutionResult>> {
        let handles = future::try_join_all(task_configs.into_iter().map(|(task_name, args)| {
            async move { client.submit_task(task_name).args(args)?.submit().await }
        }))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to submit tasks: {e}"))?;

        let results = future::try_join_all(handles.into_iter().map(|handle| handle.wait())).await?;

        Ok(results)
    }
}
