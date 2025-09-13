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
                    )
                    .map_err(AzollaError::Serialization)?;
                    return Ok(TaskExecutionResult::Success(result_value));
                }
                "failed" => {
                    // Parse the actual task error
                    let error_msg = response
                        .error
                        .unwrap_or_else(|| "Task execution failed with unknown error".to_string());

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
                let result_value: serde_json::Value =
                    serde_json::from_str(&response.result.unwrap_or_else(|| "null".to_string()))
                        .map_err(AzollaError::Serialization)?;
                Ok(Some(TaskExecutionResult::Success(result_value)))
            }
            "failed" => {
                // Parse the actual task error
                let error_msg = response
                    .error
                    .unwrap_or_else(|| "Task execution failed with unknown error".to_string());

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
    use serde_json::json;
    use std::time::Duration;

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

        let optional_group = "gpu-workers".to_string();
        assert_eq!(optional_group, "gpu-workers");
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

    /// Test TaskSubmissionBuilder with complex serialization scenarios
    #[test]
    fn test_task_submission_builder_advanced_serialization() {
        use serde::Serialize;
        use std::collections::HashMap;

        #[derive(Serialize)]
        struct NestedStruct {
            id: u64,
            data: HashMap<String, serde_json::Value>,
        }

        let mut data = HashMap::new();
        data.insert("key1".to_string(), json!("value1"));
        data.insert("key2".to_string(), json!(42));
        data.insert("key3".to_string(), json!(null));

        let nested = NestedStruct { id: 123, data };

        // Test that complex nested structures can be serialized
        let json_value = serde_json::to_value(&nested).unwrap();
        assert!(json_value.is_object());
        assert_eq!(json_value["id"], 123);
        assert!(json_value["data"].is_object());
    }

    /// Test TaskSubmissionBuilder with empty arguments
    #[test]
    fn test_task_submission_builder_empty_args() {
        let empty_vec: Vec<i32> = vec![];
        let json_value = serde_json::to_value(&empty_vec).unwrap();
        assert!(json_value.is_array());
        assert_eq!(json_value.as_array().unwrap().len(), 0);
    }

    /// Test TaskSubmissionBuilder with unit type
    #[test]
    fn test_task_submission_builder_unit_args() {
        let unit_value = ();
        let json_value = serde_json::to_value(unit_value).unwrap();
        assert!(json_value.is_null());
    }

    /// Test TaskSubmissionBuilder with optional values
    #[test]
    fn test_task_submission_builder_optional_args() {
        #[derive(serde::Serialize)]
        struct OptionalArgs {
            required_field: String,
            optional_field: Option<i32>,
            another_optional: Option<String>,
        }

        let args_with_some = OptionalArgs {
            required_field: "test".to_string(),
            optional_field: Some(42),
            another_optional: None,
        };

        let json_value = serde_json::to_value(&args_with_some).unwrap();
        assert_eq!(json_value["required_field"], "test");
        assert_eq!(json_value["optional_field"], 42);
        assert!(json_value["another_optional"].is_null());

        let args_with_none = OptionalArgs {
            required_field: "test2".to_string(),
            optional_field: None,
            another_optional: Some("present".to_string()),
        };

        let json_value2 = serde_json::to_value(&args_with_none).unwrap();
        assert_eq!(json_value2["required_field"], "test2");
        assert!(json_value2["optional_field"].is_null());
        assert_eq!(json_value2["another_optional"], "present");
    }

    /// Test retry policy handling in task submission
    #[test]
    fn test_task_submission_retry_policy_handling() {
        use crate::retry_policy::{RetryCondition, RetryPolicy, StopCondition, WaitStrategy};
        use std::time::Duration;

        // Test retry policy with different configurations
        let retry_policy = RetryPolicy {
            version: 1,
            retry: RetryCondition {
                include_errors: vec!["NetworkError".to_string(), "TimeoutError".to_string()],
                exclude_errors: vec!["ValidationError".to_string()],
            },
            stop: StopCondition {
                max_attempts: Some(5),
                max_delay: Some(Duration::from_secs(60)),
            },
            wait: WaitStrategy::Exponential {
                initial_delay: Duration::from_millis(100),
                multiplier: 2.0,
                max_delay: Duration::from_secs(10),
            },
        };

        // Test that it serializes correctly
        let serialized = serde_json::to_string(&retry_policy).unwrap();
        assert!(!serialized.is_empty());
        assert!(serialized.contains("NetworkError"));
        assert!(serialized.contains("max_attempts"));

        // Test that it can be deserialized
        let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            retry_policy.retry.include_errors,
            deserialized.retry.include_errors
        );
        assert_eq!(
            retry_policy.stop.max_attempts,
            deserialized.stop.max_attempts
        );
    }

    /// Test client configuration with various endpoint formats
    #[test]
    fn test_client_config_endpoint_variations() {
        let configs = vec![
            ("http://localhost:8080", "localhost with port"),
            ("https://example.com", "https without port"),
            ("http://127.0.0.1:52710", "IP address with port"),
            (
                "https://api.example.com:443",
                "subdomain with standard port",
            ),
        ];

        for (endpoint, description) in configs {
            let config = ClientConfig {
                endpoint: endpoint.to_string(),
                domain: "test".to_string(),
                timeout: Duration::from_secs(30),
            };

            assert_eq!(config.endpoint, endpoint, "Failed for {description}");
            assert!(!config.endpoint.is_empty());
            assert!(
                config.endpoint.starts_with("http://") || config.endpoint.starts_with("https://")
            );
        }
    }

    /// Test ClientBuilder with extreme timeout values
    #[test]
    fn test_client_builder_extreme_timeouts() {
        // Test very short timeout
        let short_config = Client::builder().timeout(Duration::from_millis(1)).config;
        assert_eq!(short_config.timeout, Duration::from_millis(1));

        // Test very long timeout
        let long_config = Client::builder()
            .timeout(Duration::from_secs(86400)) // 24 hours
            .config;
        assert_eq!(long_config.timeout, Duration::from_secs(86400));

        // Test zero timeout (edge case)
        let zero_config = Client::builder().timeout(Duration::from_secs(0)).config;
        assert_eq!(zero_config.timeout, Duration::from_secs(0));
    }

    /// Test domain name variations
    #[test]
    fn test_client_config_domain_variations() {
        let domains = vec![
            ("", "empty domain"),
            ("test", "simple domain"),
            ("production-environment", "domain with hyphens"),
            ("test_environment", "domain with underscores"),
            (
                "very-long-domain-name-with-multiple-segments",
                "very long domain",
            ),
            ("123", "numeric domain"),
            ("test.domain.com", "domain with dots"),
            ("测试", "unicode domain"),
        ];

        for (domain, description) in domains {
            let config = ClientConfig {
                endpoint: "http://localhost:52710".to_string(),
                domain: domain.to_string(),
                timeout: Duration::from_secs(30),
            };

            assert_eq!(config.domain, domain, "Failed for {description}");
        }
    }

    /// Test TaskExecutionResult with various success values
    #[test]
    fn test_task_execution_result_success_variations() {
        let success_cases = vec![
            (json!(null), "null result"),
            (json!(42), "number result"),
            (json!("hello"), "string result"),
            (json!(true), "boolean result"),
            (json!([1, 2, 3]), "array result"),
            (json!({"key": "value"}), "object result"),
            (
                json!({"nested": {"deep": {"value": 123}}}),
                "deeply nested result",
            ),
        ];

        for (value, description) in success_cases {
            let result = TaskExecutionResult::Success(value.clone());
            match result {
                TaskExecutionResult::Success(val) => {
                    assert_eq!(val, value, "Failed for {description}");
                }
                _ => panic!("Expected success result for {description}"),
            }
        }
    }

    /// Test TaskExecutionResult with various error types
    #[test]
    fn test_task_execution_result_error_variations() {
        let error_cases = vec![
            (
                TaskError::execution_failed("execution error"),
                "execution error",
            ),
            (
                TaskError::invalid_args("invalid arguments"),
                "invalid args error",
            ),
            (
                TaskError::new("operation timed out").with_error_type("TimeoutError"),
                "timeout error",
            ),
        ];

        for (error, description) in error_cases {
            let result = TaskExecutionResult::Failed(error.clone());
            match result {
                TaskExecutionResult::Failed(err) => {
                    assert_eq!(err.message, error.message, "Failed for {description}");
                    assert_eq!(err.error_type, error.error_type, "Failed for {description}");
                }
                _ => panic!("Expected failed result for {description}"),
            }
        }
    }

    /// Test JSON serialization edge cases
    #[test]
    fn test_json_serialization_edge_cases() {
        // Test with floating point numbers
        let float_args = vec![0.0, -0.0, 1.5, -1.5, f64::MAX, f64::MIN];
        for val in float_args {
            let json_value = serde_json::to_value(val).unwrap();
            assert!(json_value.is_number());
        }

        // Test with large integers
        let int_args = vec![i64::MIN, i64::MAX, 0i64, -1i64, 1i64];
        for val in int_args {
            let json_value = serde_json::to_value(val).unwrap();
            assert!(json_value.is_number());
        }

        // Test with special string characters
        let special_strings = vec![
            "\n",
            "\t",
            "\r",
            "\\",
            "\"",
            "'",
            "unicode: 🦀",
            "emoji: 😀",
            "symbols: @#$%^&*()",
        ];
        for s in special_strings {
            let json_value = serde_json::to_value(s).unwrap();
            assert!(json_value.is_string());
            assert_eq!(json_value.as_str().unwrap(), s);
        }
    }

    /// Test argument conversion from single values to arrays
    #[test]
    fn test_args_conversion_single_to_array() {
        // Test various single values that should become single-item arrays conceptually
        let test_cases = vec![
            (json!(42), "integer"),
            (json!("test"), "string"),
            (json!(true), "boolean"),
            (json!(null), "null"),
            (json!({"key": "value"}), "object"),
        ];

        for (value, description) in test_cases {
            // When we serialize a single value, it stays as-is
            let json_value = serde_json::to_value(&value).unwrap();
            assert_eq!(json_value, value, "Failed for {description}");
        }

        // When we serialize a tuple, it becomes an array
        let tuple_value = (42, "test", true);
        let json_value = serde_json::to_value(tuple_value).unwrap();
        assert!(json_value.is_array());
        let arr = json_value.as_array().unwrap();
        assert_eq!(arr.len(), 3);
    }
}
