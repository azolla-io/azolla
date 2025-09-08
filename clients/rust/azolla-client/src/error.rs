use serde_json::Value;
use thiserror::Error;

/// Main error type for client operations
#[derive(Debug, Error)]
pub enum AzollaError {
    #[error("Connection error: {0}")]
    Connection(#[from] tonic::transport::Error),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Task execution failed: {0}")]
    TaskFailed(TaskError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Timeout waiting for task completion")]
    Timeout,
}

/// Specific error for task failures
#[derive(Debug, Clone, Error)]
#[error("{error_type}: {message}")]
pub struct TaskError {
    pub error_type: String,
    pub message: String,
    pub code: Option<String>,
    pub stacktrace: Option<String>,
    pub data: Option<Value>,
}

impl TaskError {
    pub fn execution_failed(message: &str) -> Self {
        Self {
            error_type: "ExecutionError".to_string(),
            message: message.to_string(),
            code: None,
            stacktrace: None,
            data: None,
        }
    }

    pub fn invalid_args(message: &str) -> Self {
        Self {
            error_type: "InvalidArguments".to_string(),
            message: message.to_string(),
            code: None,
            stacktrace: None,
            data: None,
        }
    }
}

impl From<TaskError> for AzollaError {
    fn from(error: TaskError) -> Self {
        AzollaError::TaskFailed(error)
    }
}
