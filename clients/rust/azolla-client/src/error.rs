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

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Worker error: {0}")]
    WorkerError(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),
}

/// Specific error for task failures
#[derive(Debug, Clone, Error, serde::Serialize, serde::Deserialize)]
#[error("{error_type}: {message}")]
pub struct TaskError {
    pub error_type: String,
    pub message: String,
    pub code: Option<String>,
    pub data: Option<Value>,
    pub retryable: bool,
}

impl TaskError {
    pub fn execution_failed(message: &str) -> Self {
        Self {
            error_type: "ExecutionError".to_string(),
            message: message.to_string(),
            code: None,
            data: None,
            retryable: true,
        }
    }

    pub fn invalid_args(message: &str) -> Self {
        Self {
            error_type: "InvalidArguments".to_string(),
            message: message.to_string(),
            code: None,
            data: None,
            retryable: false,
        }
    }

    /// Create a general task error with a custom message
    pub fn new(message: &str) -> Self {
        Self {
            error_type: "TaskError".to_string(),
            message: message.to_string(),
            code: None,
            data: None,
            retryable: true,
        }
    }

    /// Set the error type
    pub fn with_error_type(mut self, error_type: &str) -> Self {
        self.error_type = error_type.to_string();
        self
    }

    /// Set the error code
    pub fn with_error_code(mut self, code: &str) -> Self {
        self.code = Some(code.to_string());
        self
    }

    /// Set the retryable flag
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// Get error type for external use
    pub fn error_type(&self) -> &str {
        &self.error_type
    }

    /// Get error code for external use
    pub fn error_code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Create a validation error (non-retryable)
    pub fn validation_error(message: &str) -> Self {
        Self {
            error_type: "TaskValidationError".to_string(),
            message: message.to_string(),
            code: Some("VALIDATION_ERROR".to_string()),
            data: None,
            retryable: false,
        }
    }

    /// Create a timeout error (retryable)
    pub fn timeout_error(message: &str) -> Self {
        Self {
            error_type: "TaskTimeoutError".to_string(),
            message: message.to_string(),
            code: Some("TIMEOUT_ERROR".to_string()),
            data: None,
            retryable: true,
        }
    }
}

impl From<TaskError> for AzollaError {
    fn from(error: TaskError) -> Self {
        AzollaError::TaskFailed(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::fmt::Write;

    /// Test the purpose of AzollaError: ensure all error variants work correctly
    #[test]
    fn test_azolla_error_variants() {
        let connection_error = AzollaError::ConnectionError("Connection failed".to_string());
        let worker_error = AzollaError::WorkerError("Worker error".to_string());
        let invalid_config = AzollaError::InvalidConfig("Invalid config".to_string());
        let timeout_error = AzollaError::Timeout;

        // Test Display implementation
        assert_eq!(
            connection_error.to_string(),
            "Connection error: Connection failed"
        );
        assert_eq!(worker_error.to_string(), "Worker error: Worker error");
        assert_eq!(
            invalid_config.to_string(),
            "Invalid configuration: Invalid config"
        );
        assert_eq!(
            timeout_error.to_string(),
            "Timeout waiting for task completion"
        );
    }

    /// Test the expected behavior: errors implement standard traits
    #[test]
    fn test_azolla_error_traits() {
        let error = AzollaError::ConnectionError("test error".to_string());

        // Test Debug trait
        let debug_output = format!("{error:?}");
        assert!(debug_output.contains("ConnectionError"));
        assert!(debug_output.contains("test error"));

        // Test Error trait
        let error_trait: &dyn std::error::Error = &error;
        assert!(error_trait.to_string().contains("test error"));

        // Test Send + Sync (these traits are auto-implemented, but we can verify)
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AzollaError>();
    }

    /// Test TaskError creation and methods
    #[test]
    fn test_task_error_constructors() {
        let invalid_args_error = TaskError::invalid_args("Invalid arguments provided");
        let execution_error = TaskError::execution_failed("Execution failed");

        // Test fields
        assert_eq!(invalid_args_error.error_type, "InvalidArguments");
        assert_eq!(invalid_args_error.message, "Invalid arguments provided");
        assert!(invalid_args_error.code.is_none());

        assert_eq!(execution_error.error_type, "ExecutionError");
        assert_eq!(execution_error.message, "Execution failed");
        assert!(execution_error.code.is_none());
    }

    /// Test TaskError Display implementation
    #[test]
    fn test_task_error_display() {
        let invalid_args = TaskError::invalid_args("bad args");
        let execution_failed = TaskError::execution_failed("runtime error");

        assert_eq!(invalid_args.to_string(), "InvalidArguments: bad args");
        assert_eq!(
            execution_failed.to_string(),
            "ExecutionError: runtime error"
        );
    }

    /// Test TaskError methods
    #[test]
    fn test_task_error_methods() {
        let error = TaskError {
            error_type: "TestError".to_string(),
            message: "test message".to_string(),
            code: Some("TEST_001".to_string()),
            data: Some(serde_json::json!({"context": "test"})),
            retryable: true,
        };

        assert_eq!(error.error_type(), "TestError");
        assert_eq!(error.error_code(), Some("TEST_001"));
        assert!(error.data.is_some());
    }

    /// Test error conversions from external errors
    #[test]
    fn test_azolla_error_from_json_error() {
        // Create a JSON parsing error
        let json_result: Result<serde_json::Value, serde_json::Error> =
            serde_json::from_str("invalid json");

        assert!(json_result.is_err());
        let json_error = json_result.unwrap_err();

        // Test conversion to AzollaError
        let azolla_error: AzollaError = json_error.into();

        match azolla_error {
            AzollaError::Serialization(_) => {
                // Expected
            }
            _ => panic!("Expected Serialization variant"),
        }
    }

    /// Test error conversions from tonic Status
    #[test]
    fn test_azolla_error_from_tonic_status() {
        let tonic_error = tonic::Status::internal("Internal server error");
        let azolla_error: AzollaError = tonic_error.into();

        match azolla_error {
            AzollaError::Grpc(_) => {
                // Expected
            }
            _ => panic!("Expected Grpc variant"),
        }
    }

    /// Test TaskError to AzollaError conversion
    #[test]
    fn test_task_error_to_azolla_error_conversion() {
        let task_error = TaskError::execution_failed("Task failed");
        let azolla_error: AzollaError = task_error.clone().into();

        match azolla_error {
            AzollaError::TaskFailed(err) => {
                assert_eq!(err.message, task_error.message);
                assert_eq!(err.error_type, task_error.error_type);
            }
            _ => panic!("Expected TaskFailed variant"),
        }
    }

    /// Test TaskError serialization/deserialization
    #[test]
    fn test_task_error_serialization() {
        let error = TaskError {
            error_type: "ValidationError".to_string(),
            message: "Field validation failed".to_string(),
            code: Some("VAL_001".to_string()),
            data: Some(serde_json::json!({"field": "email", "value": "invalid"})),
            retryable: false,
        };

        // Test serialization
        let serialized = serde_json::to_string(&error).unwrap();
        assert!(!serialized.is_empty());
        assert!(serialized.contains("ValidationError"));
        assert!(serialized.contains("VAL_001"));

        // Test deserialization
        let deserialized: TaskError = serde_json::from_str(&serialized).unwrap();
        assert_eq!(error.error_type, deserialized.error_type);
        assert_eq!(error.message, deserialized.message);
        assert_eq!(error.code, deserialized.code);
        assert_eq!(error.retryable, deserialized.retryable);
    }

    /// Test error formatting and debug output
    #[test]
    fn test_error_formatting() {
        let error = AzollaError::ConnectionError("Connection refused".to_string());

        // Test Display
        let display_str = format!("{error}");
        assert_eq!(display_str, "Connection error: Connection refused");

        // Test Debug
        let debug_str = format!("{error:?}");
        assert!(debug_str.contains("ConnectionError"));
        assert!(debug_str.contains("Connection refused"));

        // Test that we can write errors to a string
        let mut buffer = String::new();
        write!(&mut buffer, "Error occurred: {error}").unwrap();
        assert_eq!(
            buffer,
            "Error occurred: Connection error: Connection refused"
        );
    }

    /// Test error combinations and nesting
    #[test]
    fn test_nested_errors() {
        // Create a task error
        let task_error = TaskError::execution_failed("Inner task failed");

        // Wrap it in an AzollaError
        let outer_error = AzollaError::TaskFailed(task_error.clone());

        // Test that the error message propagates
        assert!(outer_error.to_string().contains("Inner task failed"));

        // Test error type checking
        match outer_error {
            AzollaError::TaskFailed(err) => {
                assert_eq!(err.message, "Inner task failed");
                assert_eq!(err.error_type, "ExecutionError");
            }
            _ => panic!("Expected TaskFailed variant"),
        }
    }

    /// Test edge cases and error message handling
    #[test]
    fn test_error_edge_cases() {
        // Test empty error message
        let empty_error = TaskError::invalid_args("");
        assert_eq!(empty_error.to_string(), "InvalidArguments: ");

        // Test very long error message
        let long_message = "x".repeat(1000);
        let long_error = TaskError::execution_failed(&long_message);
        assert!(long_error.to_string().contains(&long_message));
        assert_eq!(long_error.message.len(), 1000);

        // Test error with special characters
        let special_chars = "Error with 🚨 emoji and \"quotes\" and newlines\n\t";
        let special_error = AzollaError::WorkerError(special_chars.to_string());
        assert!(special_error.to_string().contains("🚨"));
        assert!(special_error.to_string().contains("quotes"));
    }

    /// Test error with optional fields
    #[test]
    fn test_task_error_optional_fields() {
        // Test with all fields
        let full_error = TaskError {
            error_type: "FullError".to_string(),
            message: "Full error message".to_string(),
            code: Some("ERR_001".to_string()),
            data: Some(serde_json::json!({"key": "value"})),
            retryable: true,
        };

        assert!(full_error.code.is_some());
        assert!(full_error.data.is_some());
        assert!(full_error.retryable);

        // Test with minimal fields
        let minimal_error = TaskError::invalid_args("minimal");
        assert!(minimal_error.code.is_none());
        assert!(minimal_error.data.is_none());
        assert!(!minimal_error.retryable);
    }

    /// Test error propagation in Result chains
    #[test]
    fn test_error_propagation() {
        #[allow(clippy::result_large_err)]
        fn inner_function() -> Result<String, TaskError> {
            Err(TaskError::invalid_args("Invalid input"))
        }

        #[allow(clippy::result_large_err)]
        fn outer_function() -> Result<String, AzollaError> {
            let result = inner_function()?;
            Ok(result)
        }

        let result = outer_function();
        assert!(result.is_err());

        if let Err(AzollaError::TaskFailed(task_error)) = result {
            assert_eq!(task_error.error_type, "InvalidArguments");
            assert_eq!(task_error.message, "Invalid input");
        } else {
            panic!("Expected TaskFailed error");
        }
    }
}
