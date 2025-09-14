use azolla_client::Client;
use azolla_client::client::ClientConfig;
use azolla_client::worker::WorkerConfig;
use azolla_client::error::AzollaError;
use std::time::Duration;
use super::TestOrchestrator;
use anyhow::Result;

/// Common test scenario utilities
pub struct TestScenarios;

impl TestScenarios {
    /// Create a client connected to the test orchestrator
    pub async fn create_client(orchestrator: &TestOrchestrator) -> Result<Client> {
        let config = ClientConfig {
            endpoint: orchestrator.client_endpoint(),
            domain: "test_domain".to_string(),
            timeout: Duration::from_secs(10),
        };

        Client::with_config(config).await
            .map_err(|e| anyhow::anyhow!("Failed to create client: {e}"))
    }

    /// Create a worker config for the test orchestrator
    pub fn create_worker_config(orchestrator: &TestOrchestrator) -> WorkerConfig {
        WorkerConfig {
            orchestrator_endpoint: orchestrator.worker_endpoint(),
            domain: "test_domain".to_string(),
            shepherd_group: "test_workers".to_string(),
            max_concurrency: 5,
            heartbeat_interval: Duration::from_secs(5),
        }
    }

    /// Create worker config with tasks (worker API is different than expected)
    pub fn create_worker_config_with_tasks(orchestrator: &TestOrchestrator) -> WorkerConfig {
        Self::create_worker_config(orchestrator)
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
            domain: "test_domain".to_string(),
            timeout,
        };

        Client::with_config(config).await
            .map_err(|e| anyhow::anyhow!("Failed to create client with timeout: {e}"))
    }

    /// Try to create a client with invalid endpoint for error testing
    pub async fn try_create_client_with_invalid_endpoint() -> Result<Client, azolla_client::error::AzollaError> {
        let config = ClientConfig {
            endpoint: "http://invalid-host:99999".to_string(),
            domain: "test_domain".to_string(),
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
        let handle = client
            .submit_task(task_name)
            .args(args)?
            .submit()
            .await?;

        handle.wait().await
            .map_err(|e| anyhow::anyhow!("Task execution failed: {e}"))
    }

    /// Submit multiple tasks concurrently
    pub async fn submit_concurrent_tasks(
        client: &Client,
        task_configs: Vec<(&str, serde_json::Value)>,
    ) -> Result<Vec<azolla_client::client::TaskExecutionResult>> {
        let handles: Result<Vec<_>, AzollaError> = futures::future::try_join_all(
            task_configs
                .into_iter()
                .map(|(task_name, args)| async move {
                    client
                        .submit_task(task_name)
                        .args(args)?
                        .submit()
                        .await
                }),
        ).await;

        let handles = handles.map_err(|e| anyhow::anyhow!("Failed to submit tasks: {e}"))?;

        // Wait for all tasks to complete
        let results = futures::future::try_join_all(
            handles.into_iter().map(|h| h.wait())
        ).await?;

        Ok(results)
    }
}