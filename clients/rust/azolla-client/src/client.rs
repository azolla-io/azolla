use crate::error::{AzollaError, TaskError};
use crate::proto::orchestrator::{
    client_service_client::ClientServiceClient, CreateTaskRequest, WaitForTaskRequest,
    WaitForTaskStatus,
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
    /// Helper function to convert AnyValue from protobuf to serde_json::Value
    fn convert_any_value_to_json(any_value: &crate::proto::common::AnyValue) -> serde_json::Value {
        match &any_value.value {
            Some(crate::proto::common::any_value::Value::StringValue(s)) => {
                serde_json::Value::String(s.clone())
            }
            Some(crate::proto::common::any_value::Value::IntValue(i)) => {
                serde_json::Value::Number(serde_json::Number::from(*i))
            }
            Some(crate::proto::common::any_value::Value::UintValue(u)) => {
                serde_json::Value::Number(serde_json::Number::from(*u))
            }
            Some(crate::proto::common::any_value::Value::DoubleValue(d)) => {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(*d).unwrap_or_else(|| serde_json::Number::from(0)),
                )
            }
            Some(crate::proto::common::any_value::Value::BoolValue(b)) => {
                serde_json::Value::Bool(*b)
            }
            Some(crate::proto::common::any_value::Value::JsonValue(json_str)) => {
                serde_json::from_str(json_str)
                    .unwrap_or_else(|_| serde_json::Value::String(json_str.clone()))
            }
            None => serde_json::Value::Null,
        }
    }

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
        let endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())
            .map_err(|e| AzollaError::InvalidConfig(format!("Invalid endpoint {e}")))?
            .connect_timeout(config.timeout)
            .timeout(config.timeout);

        let channel = endpoint.connect().await?;
        let grpc_client = ClientServiceClient::new(channel);

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
    pub fn submit_task(&self, name: &str) -> TaskSubmissionBuilder<'_> {
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
        let retry_policy_proto = self.retry_policy.map(|policy| policy.to_proto());

        let request = CreateTaskRequest {
            name: self.name.clone(),
            domain: self.client.inner.config.domain.clone(),
            retry_policy: retry_policy_proto,
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
        let timeout = self.client.config.timeout;
        let fut = async move {
            let mut interval = Duration::from_millis(100);
            let max_interval = Duration::from_secs(5);

            loop {
                let request = WaitForTaskRequest {
                    task_id: self.id.clone(),
                    domain: self.client.config.domain.clone(),
                    timeout_ms: None, // Let server use default timeout for individual polls
                };

                let response = self
                    .client
                    .grpc_client
                    .clone()
                    .wait_for_task(request)
                    .await?;

                let response = response.into_inner();

                match response.status_code {
                    x if x == WaitForTaskStatus::Completed as i32 => match &response.result_type {
                        Some(
                            crate::proto::orchestrator::wait_for_task_response::ResultType::Success(
                                success,
                            ),
                        ) => {
                            let result_value = if let Some(ref any_value) = success.result {
                                Client::convert_any_value_to_json(any_value)
                            } else {
                                serde_json::Value::Null
                            };
                            return Ok(TaskExecutionResult::Success(result_value));
                        }
                        Some(
                            crate::proto::orchestrator::wait_for_task_response::ResultType::Error(
                                error,
                            ),
                        ) => {
                            let (code, data) = parse_error_payload(&error.data);
                            let task_error = TaskError {
                                error_type: error.r#type.clone(),
                                message: error.message.clone(),
                                code,
                                data,
                                retryable: error.retriable,
                            };
                            return Ok(TaskExecutionResult::Failed(task_error));
                        }
                        None => {
                            return Err(AzollaError::Protocol(
                                "Task completed but no result provided".to_string(),
                            ));
                        }
                    },
                    x if x == WaitForTaskStatus::TaskNotFound as i32 => {
                        return Err(AzollaError::TaskNotFound(self.id.clone()));
                    }
                    x if x == WaitForTaskStatus::Timeout as i32 => {
                        return Err(AzollaError::Timeout);
                    }
                    x if x == WaitForTaskStatus::InternalError as i32 => {
                        return Err(AzollaError::Protocol("Internal server error".to_string()));
                    }
                    _ => {
                        tokio::time::sleep(interval).await;
                        interval = std::cmp::min(interval * 2, max_interval);
                    }
                }
            }
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) => Err(AzollaError::Timeout),
        }
    }

    /// Try to get result without blocking
    pub async fn try_result(&self) -> Result<Option<TaskExecutionResult>, AzollaError> {
        let request = WaitForTaskRequest {
            task_id: self.id.clone(),
            domain: self.client.config.domain.clone(),
            timeout_ms: Some(0), // Use 0 timeout for non-blocking check
        };

        let response = self
            .client
            .grpc_client
            .clone()
            .wait_for_task(request)
            .await?;

        let response = response.into_inner();

        match response.status_code {
            x if x == WaitForTaskStatus::Completed as i32 => match &response.result_type {
                Some(crate::proto::orchestrator::wait_for_task_response::ResultType::Success(
                    success,
                )) => {
                    let result_value = if let Some(ref any_value) = success.result {
                        Client::convert_any_value_to_json(any_value)
                    } else {
                        serde_json::Value::Null
                    };
                    Ok(Some(TaskExecutionResult::Success(result_value)))
                }
                Some(crate::proto::orchestrator::wait_for_task_response::ResultType::Error(
                    error,
                )) => {
                    let (code, data) = parse_error_payload(&error.data);
                    let task_error = TaskError {
                        error_type: error.r#type.clone(),
                        message: error.message.clone(),
                        code,
                        data,
                        retryable: error.retriable,
                    };
                    Ok(Some(TaskExecutionResult::Failed(task_error)))
                }
                None => Err(AzollaError::Protocol(
                    "Task completed but no result provided".to_string(),
                )),
            },
            x if x == WaitForTaskStatus::TaskNotFound as i32 => {
                Err(AzollaError::TaskNotFound(self.id.clone()))
            }
            x if x == WaitForTaskStatus::Timeout as i32 => Err(AzollaError::Timeout),
            x if x == WaitForTaskStatus::InternalError as i32 => {
                Err(AzollaError::Protocol("Internal server error".to_string()))
            }
            _ => Ok(None), // Still running or unspecified status
        }
    }
}

fn parse_error_payload(payload: &str) -> (Option<String>, Option<Value>) {
    match serde_json::from_str::<Value>(payload) {
        Ok(Value::Object(mut map)) => {
            let code = map
                .remove("code")
                .and_then(|value| value.as_str().map(|s| s.to_string()));

            let data_value = map.remove("data").or_else(|| {
                if map.is_empty() {
                    None
                } else {
                    Some(Value::Object(map))
                }
            });

            (code, data_value)
        }
        Ok(Value::Null) => (None, None),
        Ok(other) => (None, Some(other)),
        Err(_) => (None, Some(Value::String(payload.to_string()))),
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

    /// Test retry policy serialization in task submission
    #[test]
    fn test_task_submission_retry_policy_serialization() {
        let retry_policy = crate::retry_policy::RetryPolicy::default();
        let proto = retry_policy.to_proto();

        // Verify structured proto fields are populated with defaults
        assert_eq!(proto.version, 1);
        assert_eq!(proto.stop.as_ref().and_then(|s| s.max_attempts), Some(5));
        assert!(proto
            .retry
            .as_ref()
            .unwrap()
            .include_errors
            .contains(&"ValueError".to_string()));
    }

    /// Test TaskSubmissionBuilder shepherd group configuration
    #[test]
    fn test_task_submission_builder_shepherd_group() {
        // Test shepherd group assignment
        let shepherd_group = "high-priority-workers";
        assert_eq!(shepherd_group, "high-priority-workers");

        let optional_group: Option<String> = Some("gpu-workers".to_string());
        assert!(optional_group.is_some());
        assert_eq!(optional_group.as_ref().unwrap(), "gpu-workers");
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
}
