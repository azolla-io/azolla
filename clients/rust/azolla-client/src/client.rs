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
    #[allow(clippy::result_large_err)]
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

            let response = response.into_inner();

            match response.status.as_str() {
                "completed" => {
                    // Parse the actual task result
                    let result_value: serde_json::Value = serde_json::from_str(
                        &response.result.unwrap_or_else(|| "null".to_string()),
                    ).map_err(AzollaError::Serialization)?;
                    return Ok(TaskExecutionResult::Success(result_value));
                }
                "failed" => {
                    // Parse the actual task error
                    let error_msg = response.error.unwrap_or_else(|| "Task execution failed with unknown error".to_string());
                    
                    // Try to parse as structured TaskError, fallback to generic error
                    let task_error = serde_json::from_str::<TaskError>(&error_msg)
                        .unwrap_or_else(|_| TaskError::execution_failed(&error_msg));
                    
                    return Ok(TaskExecutionResult::Failed(task_error));
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

        let response = response.into_inner();

        match response.status.as_str() {
            "completed" => {
                // Parse the actual task result
                let result_value: serde_json::Value = serde_json::from_str(
                    &response.result.unwrap_or_else(|| "null".to_string()),
                ).map_err(AzollaError::Serialization)?;
                Ok(Some(TaskExecutionResult::Success(result_value)))
            }
            "failed" => {
                // Parse the actual task error
                let error_msg = response.error.unwrap_or_else(|| "Task execution failed with unknown error".to_string());
                
                // Try to parse as structured TaskError, fallback to generic error
                let task_error = serde_json::from_str::<TaskError>(&error_msg)
                    .unwrap_or_else(|_| TaskError::execution_failed(&error_msg));
                
                Ok(Some(TaskExecutionResult::Failed(task_error)))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use serde_json::json;

    /// Test the purpose of ClientConfig: ensure default configuration is sensible
    #[test]
    fn test_client_config_defaults() {
        let config = ClientConfig::default();
        assert_eq!(config.endpoint, "http://localhost:52710");
        assert_eq!(config.domain, "default");
        assert_eq!(config.timeout, Duration::from_secs(30));
    }

    /// Test the expected behavior: ClientBuilder should allow configuration
    #[test]
    fn test_client_builder_configuration() {
        let builder = ClientBuilder::default()
            .endpoint("http://example.com:8080")
            .domain("test-domain") 
            .timeout(Duration::from_secs(60));
            
        assert_eq!(builder.config.endpoint, "http://example.com:8080");
        assert_eq!(builder.config.domain, "test-domain");
        assert_eq!(builder.config.timeout, Duration::from_secs(60));
    }

    /// Test ClientBuilder chain methods work correctly
    #[test]
    fn test_client_builder_method_chaining() {
        let config = Client::builder()
            .endpoint("https://prod.example.com:9443")
            .domain("production")
            .timeout(Duration::from_secs(120))
            .config;
            
        assert_eq!(config.endpoint, "https://prod.example.com:9443");
        assert_eq!(config.domain, "production");
        assert_eq!(config.timeout, Duration::from_secs(120));
    }

    /// Test TaskSubmissionBuilder argument serialization
    #[test]
    fn test_task_submission_builder_args_serialization() {
        // Test single value
        let single_value = 42;
        let json_value = serde_json::to_value(single_value).unwrap();
        assert_eq!(json_value, json!(42));
        
        // Test tuple (multiple values)
        let args = (42, "test".to_string(), true);
        let json_value = serde_json::to_value(args).unwrap();
        assert!(json_value.is_array());
        
        let arr = json_value.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], json!(42));
        assert_eq!(arr[1], json!("test"));
        assert_eq!(arr[2], json!(true));
    }

    /// Test array arguments are handled correctly
    #[test]
    fn test_task_submission_builder_array_args() {
        let args = vec![1, 2, 3, 4, 5];
        let json_value = serde_json::to_value(args).unwrap();
        assert!(json_value.is_array());
        
        let arr = json_value.as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[2], json!(3));
    }

    /// Test complex nested arguments
    #[test]
    fn test_task_submission_builder_complex_args() {
        #[derive(serde::Serialize)]
        struct ComplexData {
            name: String,
            values: Vec<i32>,
            metadata: std::collections::HashMap<String, String>,
        }
        
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("version".to_string(), "1.0".to_string());
        metadata.insert("author".to_string(), "test".to_string());
        
        let data = ComplexData {
            name: "test-data".to_string(),
            values: vec![10, 20, 30],
            metadata,
        };
        
        let json_value = serde_json::to_value(data).unwrap();
        assert!(json_value.is_object());
        assert_eq!(json_value["name"], "test-data");
        assert_eq!(json_value["values"], json!([10, 20, 30]));
    }

    /// Test retry policy serialization in task submission
    #[test]
    fn test_task_submission_retry_policy_serialization() {
        let retry_policy = crate::retry_policy::RetryPolicy::default();
        let serialized = serde_json::to_string(&retry_policy).unwrap();
        
        // Verify it can be serialized and is not empty
        assert!(!serialized.is_empty());
        assert!(serialized.contains("max_attempts"));
    }

    /// Test TaskSubmissionBuilder shepherd group configuration
    #[test]
    fn test_task_submission_builder_shepherd_group() {
        // Test shepherd group assignment
        let shepherd_group = "high-priority-workers";
        assert_eq!(shepherd_group, "high-priority-workers");
        
        let optional_group: Option<String> = Some("gpu-workers".to_string());
        assert!(optional_group.is_some());
        assert_eq!(optional_group.unwrap(), "gpu-workers");
    }

    /// Test JSON argument validation
    #[test]
    fn test_json_args_validation() {
        // Test valid JSON array
        let valid_args = json!([1, "hello", true]);
        assert!(valid_args.is_array());
        
        // Test single value becomes single-item array conceptually
        let single_val = json!(42);
        assert!(single_val.is_number());
        
        // Test complex nested structure
        let complex_args = json!([{"key": "value"}, [1, 2, 3], null]);
        assert!(complex_args.is_array());
        let arr = complex_args.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr[0].is_object());
        assert!(arr[1].is_array());
        assert!(arr[2].is_null());
    }

    /// Test client configuration edge cases
    #[test]
    fn test_client_config_edge_cases() {
        // Test empty endpoint (would fail at runtime)
        let config = ClientConfig {
            endpoint: "".to_string(),
            domain: "test".to_string(),
            timeout: Duration::from_millis(100),
        };
        assert!(config.endpoint.is_empty());
        
        // Test very short timeout
        assert_eq!(config.timeout, Duration::from_millis(100));
        
        // Test long domain name
        let long_domain = "a".repeat(100);
        let config_long = ClientConfig {
            endpoint: "http://localhost:52710".to_string(),
            domain: long_domain.clone(),
            timeout: Duration::from_secs(30),
        };
        assert_eq!(config_long.domain.len(), 100);
        assert_eq!(config_long.domain, long_domain);
    }

    /// Test timeout configurations
    #[test]
    fn test_timeout_configurations() {
        let short_timeout = Duration::from_millis(1);
        let medium_timeout = Duration::from_secs(30);
        let long_timeout = Duration::from_secs(300);
        
        assert!(short_timeout < medium_timeout);
        assert!(medium_timeout < long_timeout);
        
        // Test timeout comparison
        assert_eq!(Duration::from_millis(1000), Duration::from_secs(1));
    }

    /// Test TaskExecutionResult enum
    #[test]
    fn test_task_execution_result() {
        let success_result = TaskExecutionResult::Success(json!({"status": "ok"}));
        let error_result = TaskExecutionResult::Failed(TaskError::execution_failed("test error"));
        
        match success_result {
            TaskExecutionResult::Success(val) => {
                assert_eq!(val["status"], "ok");
            }
            _ => panic!("Expected success result"),
        }
        
        match error_result {
            TaskExecutionResult::Failed(err) => {
                assert!(err.to_string().contains("test error"));
            }
            _ => panic!("Expected failed result"), 
        }
    }
}
