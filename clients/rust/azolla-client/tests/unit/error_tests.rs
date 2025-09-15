//! Unit tests for error handling functionality
//! Tests error types, conversions, serialization, and formatting

use azolla_client::error::{AzollaError, TaskError};
use serde_json::json;

/// Test error conversion chains
#[test]
fn test_error_conversion_chains() {
    // Test TaskError -> AzollaError conversion
    let task_error = TaskError::execution_failed("Execution failed");
    let azolla_error: AzollaError = task_error.clone().into();

    match azolla_error {
        AzollaError::TaskFailed(err) => {
            assert_eq!(err.message, task_error.message);
            assert_eq!(err.error_type, task_error.error_type);
        }
        _ => panic!("Expected TaskFailed variant"),
    }

    // Test serde_json::Error -> AzollaError conversion
    let json_result: Result<serde_json::Value, serde_json::Error> =
        serde_json::from_str("invalid json");
    assert!(json_result.is_err());

    let json_error = json_result.unwrap_err();
    let azolla_error: AzollaError = json_error.into();

    match azolla_error {
        AzollaError::Serialization(_) => { /* Expected */ }
        _ => panic!("Expected Serialization variant"),
    }

    // Test tonic::Status -> AzollaError conversion
    let tonic_error = tonic::Status::internal("Internal server error");
    let azolla_error: AzollaError = tonic_error.into();

    match azolla_error {
        AzollaError::Grpc(_) => { /* Expected */ }
        _ => panic!("Expected Grpc variant"),
    }
}

/// Test error serialization and deserialization consistency
#[test]
fn test_error_serialization_consistency() {
    let errors = vec![
        TaskError::execution_failed("Execution failed"),
        TaskError::invalid_args("Invalid arguments"),
        TaskError::new("Custom error")
            .with_error_type("CustomError")
            .with_error_code("CUSTOM_001"),
    ];

    for original_error in errors {
        // Test serialization
        let serialized = serde_json::to_string(&original_error).unwrap();
        assert!(!serialized.is_empty());

        // Test deserialization
        let deserialized: TaskError = serde_json::from_str(&serialized).unwrap();

        // Verify all fields match
        assert_eq!(original_error.error_type, deserialized.error_type);
        assert_eq!(original_error.message, deserialized.message);
        assert_eq!(original_error.code, deserialized.code);
        assert_eq!(original_error.data, deserialized.data);
        assert_eq!(original_error.retryable, deserialized.retryable);
    }
}

/// Test error display formatting
#[test]
fn test_error_display_formatting() {
    let task_error = TaskError::new("Test error message")
        .with_error_type("TestError")
        .with_error_code("TEST_001");

    let display_str = format!("{task_error}");
    assert_eq!(display_str, "TestError: Test error message");

    let debug_str = format!("{task_error:?}");
    assert!(debug_str.contains("TestError"));
    assert!(debug_str.contains("Test error message"));
    assert!(debug_str.contains("TEST_001"));

    // Test AzollaError display
    let azolla_error = AzollaError::TaskFailed(task_error);
    let azolla_display = format!("{azolla_error}");
    assert!(azolla_display.contains("Task execution failed"));
    assert!(azolla_display.contains("TestError"));
}

/// Test error with complex data
#[test]
fn test_error_with_complex_data() {
    let error_data = json!({
        "request_id": "req_12345",
        "user_id": 67890,
        "context": {
            "endpoint": "/api/v1/tasks",
            "method": "POST",
            "timestamp": "2023-01-01T00:00:00Z"
        },
        "validation_errors": [
            {"field": "email", "message": "Invalid format"},
            {"field": "age", "message": "Out of range"}
        ]
    });

    let task_error = TaskError {
        error_type: "ValidationError".to_string(),
        message: "Multiple validation errors occurred".to_string(),
        code: Some("VAL_001".to_string()),
        data: Some(error_data.clone()),
        retryable: false,
    };

    // Test serialization preserves complex data
    let serialized = serde_json::to_string(&task_error).unwrap();
    let deserialized: TaskError = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized.data, Some(error_data));
    assert_eq!(deserialized.code, Some("VAL_001".to_string()));
    assert!(!deserialized.retryable);
}

/// Test TaskError constructor methods
#[test]
fn test_task_error_constructors() {
    // Test execution_failed constructor
    let exec_error = TaskError::execution_failed("Task failed");
    assert_eq!(exec_error.error_type, "ExecutionError");
    assert_eq!(exec_error.message, "Task failed");
    assert!(exec_error.code.is_none());
    assert!(exec_error.retryable);

    // Test invalid_args constructor
    let arg_error = TaskError::invalid_args("Bad arguments");
    assert_eq!(arg_error.error_type, "InvalidArguments");
    assert_eq!(arg_error.message, "Bad arguments");
    assert!(arg_error.code.is_none());
    assert!(!arg_error.retryable);

    // Test new constructor
    let custom_error = TaskError::new("Custom message");
    assert_eq!(custom_error.error_type, "TaskError");
    assert_eq!(custom_error.message, "Custom message");
    assert!(custom_error.code.is_none());
    assert!(custom_error.retryable);

    // Test validation_error constructor
    let validation_error = TaskError::validation_error("Invalid input");
    assert_eq!(validation_error.error_type, "TaskValidationError");
    assert_eq!(validation_error.message, "Invalid input");
    assert_eq!(validation_error.code, Some("VALIDATION_ERROR".to_string()));
    assert!(!validation_error.retryable);

    // Test timeout_error constructor
    let timeout_error = TaskError::timeout_error("Operation timed out");
    assert_eq!(timeout_error.error_type, "TaskTimeoutError");
    assert_eq!(timeout_error.message, "Operation timed out");
    assert_eq!(timeout_error.code, Some("TIMEOUT_ERROR".to_string()));
    assert!(timeout_error.retryable);
}

/// Test TaskError builder methods
#[test]
fn test_task_error_builder_methods() {
    let error = TaskError::new("Base error")
        .with_error_type("CustomType")
        .with_error_code("CUSTOM_001");

    assert_eq!(error.error_type, "CustomType");
    assert_eq!(error.message, "Base error");
    assert_eq!(error.code, Some("CUSTOM_001".to_string()));

    // Test method chaining
    let chained_error = TaskError::execution_failed("Failed")
        .with_error_code("EXEC_001")
        .with_error_type("CustomExecution");

    assert_eq!(chained_error.error_type, "CustomExecution");
    assert_eq!(chained_error.code, Some("EXEC_001".to_string()));
}

/// Test AzollaError variants
#[test]
fn test_azolla_error_variants() {
    // Test TaskFailed variant
    let task_error = TaskError::execution_failed("Task failed");
    let azolla_error = AzollaError::TaskFailed(task_error);
    match azolla_error {
        AzollaError::TaskFailed(err) => {
            assert_eq!(err.error_type, "ExecutionError");
        }
        _ => panic!("Expected TaskFailed variant"),
    }

    // Test InvalidConfig variant
    let config_error = AzollaError::InvalidConfig("Bad config".to_string());
    match config_error {
        AzollaError::InvalidConfig(msg) => {
            assert_eq!(msg, "Bad config");
        }
        _ => panic!("Expected InvalidConfig variant"),
    }

    // Test Connection variant (test that it accepts transport errors)
    // We can't easily create a transport error, so we'll test the pattern
    match AzollaError::InvalidConfig("test".to_string()) {
        AzollaError::Connection(_) => panic!("Should not match Connection"),
        AzollaError::InvalidConfig(msg) => assert_eq!(msg, "test"),
        _ => panic!("Expected InvalidConfig variant"),
    }
}

/// Test error trait implementations
#[test]
fn test_error_traits() {
    let task_error = TaskError::execution_failed("Test error");
    let azolla_error = AzollaError::TaskFailed(task_error);

    // Test Display trait
    let display_string = format!("{azolla_error}");
    assert!(!display_string.is_empty());

    // Test Debug trait
    let debug_string = format!("{azolla_error:?}");
    assert!(!debug_string.is_empty());

    // Test that it implements std::error::Error
    let _: &dyn std::error::Error = &azolla_error;
}

/// Test error equality and cloning
#[test]
fn test_error_equality_and_cloning() {
    let error1 = TaskError::execution_failed("Test error");
    let error2 = error1.clone();

    assert_eq!(error1.error_type, error2.error_type);
    assert_eq!(error1.message, error2.message);
    assert_eq!(error1.code, error2.code);

    // Test partial equality
    let error3 = TaskError::execution_failed("Different message");
    assert_ne!(error1.message, error3.message);
}

/// Test error optional fields
#[test]
fn test_error_optional_fields() {
    let mut error = TaskError::new("Base error");
    assert!(error.code.is_none());
    assert!(error.data.is_none());
    assert!(error.retryable); // Default retryable

    error = error.with_error_code("CODE_001");
    assert_eq!(error.code, Some("CODE_001".to_string()));

    // Test setting data
    error.data = Some(json!({"key": "value"}));
    assert!(error.data.is_some());

    // Test setting retryable
    error = error.with_retryable(false);
    assert!(!error.retryable);
}

/// Test TaskError to AzollaError conversion
#[test]
fn test_task_error_to_azolla_error_conversion() {
    let task_error = TaskError::execution_failed("Task execution failed");
    let azolla_error: AzollaError = task_error.into();

    match azolla_error {
        AzollaError::TaskFailed(err) => {
            assert_eq!(err.error_type, "ExecutionError");
            assert_eq!(err.message, "Task execution failed");
        }
        _ => panic!("Expected TaskFailed variant"),
    }
}

/// Test AzollaError from JSON error
#[test]
fn test_azolla_error_from_json_error() {
    let invalid_json = "{ invalid json }";
    let json_result: Result<serde_json::Value, serde_json::Error> =
        serde_json::from_str(invalid_json);

    let json_error = json_result.unwrap_err();
    let azolla_error: AzollaError = json_error.into();

    match azolla_error {
        AzollaError::Serialization(err) => {
            let err_string = err.to_string();
            assert!(!err_string.is_empty());
        }
        _ => panic!("Expected Serialization variant"),
    }
}

/// Test AzollaError from tonic status
#[test]
fn test_azolla_error_from_tonic_status() {
    let status = tonic::Status::deadline_exceeded("Request timed out");
    let azolla_error: AzollaError = status.into();

    match azolla_error {
        AzollaError::Grpc(status) => {
            assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
            assert!(status.message().contains("Request timed out"));
        }
        _ => panic!("Expected Grpc variant"),
    }
}
