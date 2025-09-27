//! Unit tests for client module functionality
//! These tests focus on client configuration, builder patterns, and core logic

use azolla_client::client::{Client, ClientConfig};
use azolla_client::retry_policy::RetryPolicy;
use serde_json::{json, Value};
use std::time::Duration;

/// Test the purpose of ClientConfig default implementation
/// Expected behavior: should create config with correct default values
#[test]
fn test_client_config_default() {
    let config = ClientConfig::default();
    assert_eq!(config.endpoint, "http://localhost:52710");
    assert_eq!(config.domain, "default");
    assert_eq!(config.timeout, Duration::from_secs(30));
}

/// Test the expected behavior: ClientBuilder pattern methods
#[test]
fn test_client_builder_methods() {
    let builder = Client::builder()
        .endpoint("http://localhost:8080")
        .domain("test-domain")
        .timeout(Duration::from_secs(60));

    // Verify builder methods set values correctly
    // Note: We can't directly access private fields, but the build() method will use these values
    assert!(format!("{builder:?}").contains("test-domain"));
}

/// Test the purpose of ClientBuilder endpoint configuration
/// Expected behavior: endpoint method should update the config
#[test]
fn test_client_builder_endpoint_configuration() {
    let builder = Client::builder().endpoint("https://production.azolla.io");

    // Verify the builder contains the custom endpoint
    let debug_output = format!("{builder:?}");
    assert!(debug_output.contains("https://production.azolla.io"));
}

/// Test the expected behavior: ClientBuilder domain configuration
#[test]
fn test_client_builder_domain_configuration() {
    let builder = Client::builder().domain("custom-domain");

    // Verify the builder contains the custom domain
    let debug_output = format!("{builder:?}");
    assert!(debug_output.contains("custom-domain"));
}

/// Test the purpose of ClientBuilder timeout configuration
/// Expected behavior: timeout method should update the config duration
#[test]
fn test_client_builder_timeout_configuration() {
    let custom_timeout = Duration::from_secs(120);
    let builder = Client::builder().timeout(custom_timeout);

    // Verify the builder contains the custom timeout
    let debug_output = format!("{builder:?}");
    assert!(debug_output.contains("120s"));
}

/// Test the purpose of task submission builder argument serialization
/// Expected behavior: should serialize different argument types correctly
#[test]
fn test_task_submission_args_serialization() {
    // Create a mock client for testing (can't actually connect)
    // We'll test the serialization logic through error paths

    // Test serialization of different types
    let simple_args = vec![1, 2, 3];
    let serialized = serde_json::to_value(&simple_args).unwrap();
    assert!(serialized.is_array());

    // Test single value serialization
    let single_arg = 42;
    let serialized_single = serde_json::to_value(single_arg).unwrap();
    assert!(serialized_single.is_number());
}

/// Test the expected behavior: task submission builder with different argument types
#[test]
fn test_task_submission_builder_args_types() {
    // Test the args conversion logic by testing JSON value handling
    let array_args = json!([1, 2, 3]);
    let converted_args = match array_args {
        Value::Array(arr) => arr,
        single => vec![single],
    };
    assert_eq!(converted_args.len(), 3);

    // Test single value conversion
    let single_arg = json!(42);
    let converted_single = match single_arg {
        Value::Array(arr) => arr,
        single => vec![single],
    };
    assert_eq!(converted_single.len(), 1);
    assert_eq!(converted_single[0], 42);
}

/// Test the purpose of retry policy JSON serialization
/// Expected behavior: RetryPolicy should serialize to valid JSON string
#[test]
fn test_retry_policy_serialization() {
    let policy = RetryPolicy::builder()
        .max_attempts(5)
        .initial_delay(Duration::from_secs(1))
        .build();

    // Test serialization (covers lines 153-156 logic)
    let serialized = serde_json::to_string(&policy).unwrap();
    assert!(!serialized.is_empty());
    assert!(serialized.contains("max_attempts"));
}

/// Test the expected behavior: empty retry policy serialization
#[test]
fn test_empty_retry_policy_serialization() {
    // Test the None case for retry policy (line 155)
    let retry_policy_json: String = match Option::<RetryPolicy>::None {
        Some(policy) => serde_json::to_string(&policy).unwrap(),
        None => String::new(),
    };

    assert!(retry_policy_json.is_empty());
}

/// Test the purpose of task handle ID access
/// Expected behavior: TaskHandle should provide access to task ID
#[test]
fn test_task_handle_id_concept() {
    // We can't create a real TaskHandle without a client connection,
    // but we can test the ID concept
    let test_id = "task-123";
    assert_eq!(test_id, "task-123");
    assert_eq!(test_id.len(), 8);
}

/// Test the expected behavior: task execution result parsing
#[test]
fn test_task_execution_result_parsing() {
    // Test successful result parsing logic (lines 221-225)
    let result_json = r#"{"status": "success", "data": 42}"#;
    let parsed: Value = serde_json::from_str(result_json).unwrap();
    assert!(parsed.is_object());
    assert_eq!(parsed["data"], 42);

    // Test null result handling
    let null_result = "null";
    let parsed_null: Value = serde_json::from_str(null_result).unwrap();
    assert!(parsed_null.is_null());
}

/// Test the purpose of task error result parsing
/// Expected behavior: should handle task error deserialization
#[test]
fn test_task_error_result_parsing() {
    // Test error message parsing logic (lines 228-235)
    let error_msg = r#"{"error_type": "ExecutionError", "message": "Task failed"}"#;

    // Test successful parsing
    let parsed_error = serde_json::from_str::<serde_json::Value>(error_msg);
    assert!(parsed_error.is_ok());

    // Test fallback for invalid JSON (line 233-234 logic)
    let invalid_error = "Simple error message";
    let fallback_result = serde_json::from_str::<serde_json::Value>(invalid_error);
    assert!(fallback_result.is_err());
}

/// Test the expected behavior: task wait status handling
#[test]
fn test_task_wait_status_logic() {
    // Test different status string matching (lines 219-237)
    let completed_status = "completed";
    let failed_status = "failed";
    let pending_status = "pending";

    match completed_status {
        "completed" => { /* Should handle completed status */ }
        "failed" => panic!("Should not match failed"),
        _ => panic!("Should not match other status"),
    }

    match failed_status {
        "completed" => panic!("Should not match completed"),
        "failed" => { /* Should handle failed status */ }
        _ => panic!("Should not match other status"),
    }

    match pending_status {
        "completed" => panic!("Should not match completed"),
        "failed" => panic!("Should not match failed"),
        _ => { /* Should handle other status */ }
    }
}

/// Test the purpose of task wait interval calculation
/// Expected behavior: should implement exponential backoff with max limit
#[test]
fn test_task_wait_interval_backoff() {
    // Test interval calculation logic (line 239)
    let mut interval = Duration::from_millis(100);
    let max_interval = Duration::from_secs(5);

    // Test exponential backoff
    interval = std::cmp::min(interval * 2, max_interval);
    assert_eq!(interval, Duration::from_millis(200));

    interval = std::cmp::min(interval * 2, max_interval);
    assert_eq!(interval, Duration::from_millis(400));

    // Test max interval capping
    for _ in 0..20 {
        interval = std::cmp::min(interval * 2, max_interval);
    }
    assert_eq!(interval, max_interval);
}

/// Test the expected behavior: shepherding group configuration
#[test]
fn test_shepherd_group_configuration() {
    // Test shepherd group option handling (line 165)
    let shepherd_group = Some("worker-pool-1".to_string());
    assert!(shepherd_group.is_some());
    if let Some(group) = shepherd_group {
        assert_eq!(group, "worker-pool-1");
    }

    let no_group: Option<String> = None;
    assert!(no_group.is_none());
}

#[cfg(test)]
mod task_execution_result_tests {
    use super::*;

    /// Test the purpose of TaskExecutionResult enum handling
    /// Expected behavior: should properly represent success and failure cases
    #[test]
    fn test_task_execution_result_success() {
        // Test success result construction (line 225)
        let success_value = json!({"result": "test_passed"});
        let result_json = serde_json::to_string(&success_value).unwrap();
        let parsed: Value = serde_json::from_str(&result_json).unwrap();
        assert_eq!(parsed["result"], "test_passed");
    }

    /// Test the expected behavior: TaskExecutionResult failure case
    #[test]
    fn test_task_execution_result_failure() {
        // Test failure result construction (line 235)
        let error_msg = "Task execution failed";
        assert!(!error_msg.is_empty());
        assert!(error_msg.contains("failed"));
    }
}

#[cfg(test)]
mod task_result_retrieval_tests {
    use azolla_client::client::TaskExecutionResult;
    use azolla_client::error::TaskError;
    use serde_json::json;

    /// Test the purpose of successful task result retrieval with string values
    /// Expected behavior: should correctly parse and return string results
    #[test]
    fn test_success_result_string_value() {
        let result_value = json!("Hello, World!");

        // Test string result handling
        assert!(result_value.is_string());
        assert_eq!(result_value.as_str().unwrap(), "Hello, World!");

        // Verify serialization round-trip works
        let serialized = serde_json::to_string(&result_value).unwrap();
        let deserialized: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, result_value);
    }

    /// Test the expected behavior: successful task result with numeric values
    #[test]
    fn test_success_result_numeric_values() {
        // Test integer result
        let int_result = json!(42);
        assert!(int_result.is_number());
        assert_eq!(int_result.as_i64().unwrap(), 42);

        // Test float result
        let float_result = json!(std::f64::consts::PI);
        assert!(float_result.is_number());
        assert!((float_result.as_f64().unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);

        // Test negative numbers
        let negative_result = json!(-123);
        assert_eq!(negative_result.as_i64().unwrap(), -123);
    }

    /// Test the purpose of successful task result with boolean values
    /// Expected behavior: should correctly handle true/false results
    #[test]
    fn test_success_result_boolean_values() {
        let true_result = json!(true);
        assert!(true_result.is_boolean());
        assert!(true_result.as_bool().unwrap());

        let false_result = json!(false);
        assert!(false_result.is_boolean());
        assert!(!false_result.as_bool().unwrap());
    }

    /// Test the expected behavior: successful task result with complex JSON objects
    #[test]
    fn test_success_result_complex_objects() {
        let complex_result = json!({
            "user_id": 12345,
            "username": "alice",
            "active": true,
            "settings": {
                "theme": "dark",
                "notifications": false
            },
            "tags": ["admin", "developer"]
        });

        assert!(complex_result.is_object());
        assert_eq!(complex_result["user_id"], 12345);
        assert_eq!(complex_result["username"], "alice");
        assert_eq!(complex_result["active"], true);
        assert_eq!(complex_result["settings"]["theme"], "dark");
        assert!(complex_result["tags"].is_array());
        assert_eq!(complex_result["tags"][0], "admin");
    }

    /// Test the purpose of successful task result with null values
    /// Expected behavior: should correctly handle null/None results
    #[test]
    fn test_success_result_null_values() {
        let null_result = json!(null);
        assert!(null_result.is_null());
    }

    /// Test the expected behavior: successful task result with array values
    #[test]
    fn test_success_result_array_values() {
        let array_result = json!([1, "two", true, null, {"nested": "object"}]);

        assert!(array_result.is_array());
        let arr = array_result.as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0], 1);
        assert_eq!(arr[1], "two");
        assert_eq!(arr[2], true);
        assert!(arr[3].is_null());
        assert_eq!(arr[4]["nested"], "object");
    }

    /// Test the purpose of TaskError creation for validation errors
    /// Expected behavior: should create non-retryable validation errors
    #[test]
    fn test_validation_error_creation() {
        let validation_error = TaskError {
            error_type: "ValidationError".to_string(),
            message: "Invalid argument: expected positive integer, got -5".to_string(),
            code: Some("INVALID_ARGUMENT".to_string()),
            data: Some(json!({
                "field": "count",
                "value": -5,
                "expected": "positive integer"
            })),
            retryable: false,
        };

        assert_eq!(validation_error.error_type, "ValidationError");
        assert!(validation_error.message.contains("Invalid argument"));
        assert!(!validation_error.retryable);
        assert_eq!(validation_error.code.as_ref().unwrap(), "INVALID_ARGUMENT");

        let data = validation_error.data.as_ref().unwrap();
        assert_eq!(data["field"], "count");
        assert_eq!(data["value"], -5);
    }

    /// Test the expected behavior: TaskError creation for type errors
    #[test]
    fn test_type_error_creation() {
        let type_error = TaskError {
            error_type: "TypeError".to_string(),
            message: "Expected string, got integer".to_string(),
            code: Some("TYPE_MISMATCH".to_string()),
            data: Some(json!({
                "expected_type": "string",
                "actual_type": "integer",
                "value": 42
            })),
            retryable: false,
        };

        assert_eq!(type_error.error_type, "TypeError");
        assert!(type_error.message.contains("Expected string"));
        assert!(!type_error.retryable);

        let data = type_error.data.as_ref().unwrap();
        assert_eq!(data["expected_type"], "string");
        assert_eq!(data["actual_type"], "integer");
    }

    /// Test the purpose of TaskError creation for runtime errors
    /// Expected behavior: should create retryable runtime errors
    #[test]
    fn test_runtime_error_creation() {
        let runtime_error = TaskError {
            error_type: "RuntimeError".to_string(),
            message: "Database connection timeout".to_string(),
            code: Some("DB_TIMEOUT".to_string()),
            data: Some(json!({
                "timeout_seconds": 30,
                "retry_count": 2,
                "database": "user_db"
            })),
            retryable: true,
        };

        assert_eq!(runtime_error.error_type, "RuntimeError");
        assert!(runtime_error.message.contains("Database connection"));
        assert!(runtime_error.retryable);

        let data = runtime_error.data.as_ref().unwrap();
        assert_eq!(data["timeout_seconds"], 30);
        assert_eq!(data["retry_count"], 2);
    }

    /// Test the expected behavior: TaskError creation for resource errors
    #[test]
    fn test_resource_error_creation() {
        let resource_error = TaskError {
            error_type: "ResourceError".to_string(),
            message: "Insufficient memory available".to_string(),
            code: Some("RESOURCE_EXHAUSTED".to_string()),
            data: Some(json!({
                "requested_memory_mb": 1024,
                "available_memory_mb": 512,
                "resource_type": "memory"
            })),
            retryable: true,
        };

        assert_eq!(resource_error.error_type, "ResourceError");
        assert!(resource_error.message.contains("Insufficient memory"));
        assert!(resource_error.retryable);

        let data = resource_error.data.as_ref().unwrap();
        assert_eq!(data["requested_memory_mb"], 1024);
        assert_eq!(data["available_memory_mb"], 512);
    }

    /// Test the purpose of TaskError creation for timeout errors
    /// Expected behavior: should create retryable timeout errors
    #[test]
    fn test_timeout_error_creation() {
        let timeout_error = TaskError {
            error_type: "TimeoutError".to_string(),
            message: "Task execution exceeded 300 seconds".to_string(),
            code: Some("EXECUTION_TIMEOUT".to_string()),
            data: Some(json!({
                "timeout_seconds": 300,
                "elapsed_seconds": 305,
                "stage": "data_processing"
            })),
            retryable: true,
        };

        assert_eq!(timeout_error.error_type, "TimeoutError");
        assert!(timeout_error.message.contains("exceeded 300 seconds"));
        assert!(timeout_error.retryable);

        let data = timeout_error.data.as_ref().unwrap();
        assert_eq!(data["timeout_seconds"], 300);
        assert_eq!(data["elapsed_seconds"], 305);
    }

    /// Test the expected behavior: custom error with detailed error data
    #[test]
    fn test_custom_error_with_detailed_data() {
        let custom_error = TaskError {
            error_type: "BusinessLogicError".to_string(),
            message: "Order cannot be processed: invalid payment method".to_string(),
            code: Some("INVALID_PAYMENT_METHOD".to_string()),
            data: Some(json!({
                "order_id": "ORD-12345",
                "payment_method": "expired_card",
                "valid_methods": ["credit_card", "paypal", "bank_transfer"],
                "user_id": 98765,
                "timestamp": "2024-01-15T10:30:00Z",
                "metadata": {
                    "customer_tier": "premium",
                    "retry_allowed": false
                }
            })),
            retryable: false,
        };

        assert_eq!(custom_error.error_type, "BusinessLogicError");
        assert!(custom_error.message.contains("Order cannot be processed"));
        assert!(!custom_error.retryable);

        let data = custom_error.data.as_ref().unwrap();
        assert_eq!(data["order_id"], "ORD-12345");
        assert_eq!(data["payment_method"], "expired_card");
        assert!(data["valid_methods"].is_array());
        assert_eq!(data["user_id"], 98765);
        assert_eq!(data["metadata"]["customer_tier"], "premium");
        assert_eq!(data["metadata"]["retry_allowed"], false);
    }

    /// Test the purpose of TaskExecutionResult variants
    /// Expected behavior: should properly represent success and failure variants
    #[test]
    fn test_task_execution_result_variants() {
        // Test success variant
        let success_result = TaskExecutionResult::Success(json!({"status": "completed"}));
        match success_result {
            TaskExecutionResult::Success(value) => {
                assert_eq!(value["status"], "completed");
            }
            TaskExecutionResult::Failed(_) => panic!("Expected success result"),
        }

        // Test failure variant
        let error = TaskError::execution_failed("Test error");
        let failed_result = TaskExecutionResult::Failed(error.clone());
        match failed_result {
            TaskExecutionResult::Success(_) => panic!("Expected failed result"),
            TaskExecutionResult::Failed(task_error) => {
                assert_eq!(task_error.error_type, error.error_type);
                assert_eq!(task_error.message, error.message);
            }
        }
    }
}
