use crate::error::{AzollaError, TaskError};
use crate::proto::orchestrator::{
    client_service_client::ClientServiceClient, CreateTaskRequest, WaitForTaskRequest,
};
use crate::retry_policy::RetryPolicy;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

/// Main client for interacting with Azolla
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    grpc_client: ClientServiceClient<Channel>,
    config: ClientConfig,
}

/// Configuration for the Azolla client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub domain: String,
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:52710".to_string(),
            domain: "default".to_string(),
            timeout: Duration::from_secs(30),
        }
    }
}

impl Client {
    /// Connect to Azolla orchestrator with default config
    pub async fn connect(endpoint: &str) -> Result<Self, AzollaError> {
        let config = ClientConfig {
            endpoint: endpoint.to_string(),
            ..Default::default()
        };

        Self::with_config(config).await
    }

    /// Create client with custom configuration
    pub async fn with_config(config: ClientConfig) -> Result<Self, AzollaError> {
        let grpc_client = ClientServiceClient::connect(config.endpoint.clone()).await?;

        let inner = Arc::new(ClientInner {
            grpc_client,
            config,
        });

        Ok(Self { inner })
    }

    /// Create a client builder
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Submit a task for execution
    pub fn submit_task(&self, name: &str) -> TaskSubmissionBuilder {
        TaskSubmissionBuilder::new(self, name)
    }
}

/// Builder for client configuration
#[derive(Debug, Default)]
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = endpoint.to_string();
        self
    }

    pub fn domain(mut self, domain: &str) -> Self {
        self.config.domain = domain.to_string();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub async fn build(self) -> Result<Client, AzollaError> {
        Client::with_config(self.config).await
    }
}

/// Builder for task submission
pub struct TaskSubmissionBuilder<'a> {
    client: &'a Client,
    name: String,
    args: Vec<Value>,
    retry_policy: Option<RetryPolicy>,
    shepherd_group: Option<String>,
}

impl<'a> TaskSubmissionBuilder<'a> {
    fn new(client: &'a Client, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            args: Vec::new(),
            retry_policy: None,
            shepherd_group: None,
        }
    }

    /// Set typed arguments (any serializable type)
    pub fn args<T: Serialize>(mut self, args: T) -> Result<Self, AzollaError> {
        let json_value = serde_json::to_value(args)?;
        self.args = match json_value {
            Value::Array(arr) => arr,
            single => vec![single],
        };
        Ok(self)
    }

    /// Set retry policy for this task
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Set shepherd group for targeted execution
    pub fn shepherd_group(mut self, group: &str) -> Self {
        self.shepherd_group = Some(group.to_string());
        self
    }

    /// Submit the task and get a handle
    pub async fn submit(self) -> Result<TaskHandle, AzollaError> {
        let args_json = serde_json::to_string(&self.args)?;
        let retry_policy_json = match self.retry_policy {
            Some(policy) => serde_json::to_string(&policy)?,
            None => String::new(),
        };

        let request = CreateTaskRequest {
            name: self.name.clone(),
            domain: self.client.inner.config.domain.clone(),
            retry_policy: retry_policy_json,
            args: args_json,
            kwargs: "{}".to_string(), // Not used in Rust client
            flow_instance_id: None,
            shepherd_group: self.shepherd_group,
        };

        let response = self
            .client
            .inner
            .grpc_client
            .clone()
            .create_task(request)
            .await?;

        let task_id = response.into_inner().task_id;

        Ok(TaskHandle {
            id: task_id,
            client: Arc::clone(&self.client.inner),
        })
    }
}

/// Handle to a submitted task
pub struct TaskHandle {
    id: String,
    client: Arc<ClientInner>,
}

impl TaskHandle {
    /// Get the task ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Wait for task completion
    pub async fn wait(self) -> Result<TaskExecutionResult, AzollaError> {
        let mut interval = Duration::from_millis(100);
        let max_interval = Duration::from_secs(5);

        loop {
            let request = WaitForTaskRequest {
                task_id: self.id.clone(),
                domain: self.client.config.domain.clone(),
            };

            let response = self
                .client
                .grpc_client
                .clone()
                .wait_for_task(request)
                .await?;

            let status = response.into_inner().status;

            match status.as_str() {
                "completed" => {
                    // For now, return a simple success result
                    // In a real implementation, we'd parse the actual result
                    return Ok(TaskExecutionResult::Success(Value::String(
                        "Task completed successfully".to_string(),
                    )));
                }
                "failed" => {
                    // For now, return a generic failure
                    // In a real implementation, we'd parse the actual error
                    return Ok(TaskExecutionResult::Failed(TaskError::execution_failed(
                        "Task execution failed",
                    )));
                }
                _ => {
                    // Task still running, wait and retry
                    tokio::time::sleep(interval).await;
                    interval = std::cmp::min(interval * 2, max_interval);
                }
            }
        }
    }

    /// Try to get result without blocking
    pub async fn try_result(&self) -> Result<Option<TaskExecutionResult>, AzollaError> {
        let request = WaitForTaskRequest {
            task_id: self.id.clone(),
            domain: self.client.config.domain.clone(),
        };

        let response = self
            .client
            .grpc_client
            .clone()
            .wait_for_task(request)
            .await?;

        let status = response.into_inner().status;

        match status.as_str() {
            "completed" => Ok(Some(TaskExecutionResult::Success(Value::String(
                "Task completed successfully".to_string(),
            )))),
            "failed" => Ok(Some(TaskExecutionResult::Failed(
                TaskError::execution_failed("Task execution failed"),
            ))),
            _ => Ok(None), // Still running
        }
    }
}

/// Result of task execution
#[derive(Debug, Clone)]
pub enum TaskExecutionResult {
    Success(Value),
    Failed(TaskError),
}
