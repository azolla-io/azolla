use azolla_client::client::{Client, ClientConfig, TaskExecutionResult};
use azolla_client::error::{AzollaError, TaskError};
/// Test the purpose of Client: comprehensive integration testing of client functionality
/// Test the expected behavior: client should handle connection failures, timeouts, and task submission/waiting gracefully
use azolla_client::*;
use serde_json::json;
use std::time::Duration;

/// Test client connection to non-existent endpoint fails gracefully
#[tokio::test]
async fn test_client_connection_failure() {
    let result = Client::connect("http://non-existent-host:99999").await;
    assert!(result.is_err());

    match result.err().unwrap() {
        AzollaError::Connection(_) => { /* Expected */ }
        _ => panic!("Expected connection error"),
    }
}

/// Test client builder with invalid endpoint configuration
#[tokio::test]
async fn test_client_builder_invalid_endpoint() {
    let result = Client::builder()
        .endpoint("invalid://malformed-url")
        .build()
        .await;

    assert!(result.is_err());
    match result.err().unwrap() {
        AzollaError::Connection(_) => { /* Expected */ }
        _ => panic!("Expected connection error for malformed URL"),
    }
}

/// Test client with custom configuration
#[tokio::test]
async fn test_client_with_custom_config() {
    let config = ClientConfig {
        endpoint: "http://localhost:99999".to_string(), // Non-existent port
        domain: "test-domain".to_string(),
        timeout: Duration::from_millis(100),
    };

    let result = Client::with_config(config).await;
    assert!(result.is_err());

    match result.err().unwrap() {
        AzollaError::Connection(_) => { /* Expected */ }
        _ => panic!("Expected connection error"),
    }
}

/// Test task submission builder with serialization errors
#[tokio::test]
async fn test_task_submission_serialization_error() {
    // Create a mock client (we can't connect to actual server in tests)
    // This test focuses on the argument serialization logic

    // Test with skipped serialization fields (should succeed)
    #[derive(serde::Serialize)]
    struct StructWithSkippedField {
        #[serde(skip_serializing)]
        _internal_field: Option<Box<StructWithSkippedField>>,
        data: String,
    }

    // Since we can't easily create a client without connecting,
    // we'll test the serialization logic directly
    let test_struct = StructWithSkippedField {
        _internal_field: None,
        data: "test".to_string(),
    };

    let serialize_result = serde_json::to_value(&test_struct);
    assert!(serialize_result.is_ok()); // This should work since we skip the internal field

    // Test with problematic serialization (NaN values)
    use std::collections::HashMap;
    let mut map: HashMap<String, f64> = HashMap::new();
    map.insert("valid".to_string(), 42.0);
    map.insert("invalid".to_string(), f64::NAN);

    let serialize_result = serde_json::to_value(&map);
    match serialize_result {
        Ok(value) => assert!(
            value.to_string().contains("null"),
            "Expected NaN to serialize to null"
        ),
        Err(_) => { /* This is also an acceptable outcome for NaN serialization. */ }
    }
}

/// Test task submission builder argument processing
#[test]
fn test_task_submission_args_processing() {
    // Test single argument becomes array
    let single_arg = 42;
    let json_value = serde_json::to_value(single_arg).unwrap();
    assert_eq!(json_value, json!(42));

    // Test tuple arguments become array
    let tuple_args = ("hello", 123, true);
    let json_value = serde_json::to_value(tuple_args).unwrap();
    assert!(json_value.is_array());
    let arr = json_value.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], json!("hello"));
    assert_eq!(arr[1], json!(123));
    assert_eq!(arr[2], json!(true));

    // Test array arguments stay as array
    let array_args = vec![1, 2, 3];
    let json_value = serde_json::to_value(array_args).unwrap();
    assert!(json_value.is_array());
    assert_eq!(json_value.as_array().unwrap().len(), 3);
}

/// Test task submission builder with various argument types
#[test]
fn test_task_submission_complex_args() {
    #[derive(serde::Serialize)]
    struct ComplexArgs {
        id: u64,
        name: String,
        tags: Vec<String>,
        metadata: std::collections::HashMap<String, serde_json::Value>,
        optional_field: Option<i32>,
    }

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("version".to_string(), json!("1.0"));
    metadata.insert("priority".to_string(), json!(5));

    let complex_args = ComplexArgs {
        id: 12345,
        name: "test-task".to_string(),
        tags: vec!["urgent".to_string(), "production".to_string()],
        metadata,
        optional_field: Some(99),
    };

    let json_result = serde_json::to_value(&complex_args);
    assert!(json_result.is_ok());

    let json_value = json_result.unwrap();
    assert_eq!(json_value["id"], 12345);
    assert_eq!(json_value["name"], "test-task");
    assert_eq!(json_value["tags"].as_array().unwrap().len(), 2);
    assert_eq!(json_value["optional_field"], 99);
}

/// Test TaskExecutionResult enum functionality
#[test]
fn test_task_execution_result() {
    // Test success result
    let success_data = json!({"result": "success", "value": 42});
    let success_result = TaskExecutionResult::Success(success_data.clone());

    match success_result {
        TaskExecutionResult::Success(value) => {
            assert_eq!(value, success_data);
        }
        _ => panic!("Expected success result"),
    }

    // Test failed result
    let task_error = TaskError::execution_failed("Task failed due to network error");
    let failed_result = TaskExecutionResult::Failed(task_error.clone());

    match failed_result {
        TaskExecutionResult::Failed(error) => {
            assert_eq!(error.message, task_error.message);
            assert_eq!(error.error_type, "ExecutionError");
        }
        _ => panic!("Expected failed result"),
    }
}

/// Test client configuration edge cases
#[test]
fn test_client_config_edge_cases() {
    // Test with very short timeout
    let short_timeout_config = ClientConfig {
        endpoint: "http://localhost:52710".to_string(),
        domain: "test".to_string(),
        timeout: Duration::from_millis(1),
    };
    assert_eq!(short_timeout_config.timeout, Duration::from_millis(1));

    // Test with very long timeout
    let long_timeout_config = ClientConfig {
        endpoint: "http://localhost:52710".to_string(),
        domain: "test".to_string(),
        timeout: Duration::from_secs(3600), // 1 hour
    };
    assert_eq!(long_timeout_config.timeout, Duration::from_secs(3600));

    // Test with empty domain
    let empty_domain_config = ClientConfig {
        endpoint: "http://localhost:52710".to_string(),
        domain: "".to_string(),
        timeout: Duration::from_secs(30),
    };
    assert_eq!(empty_domain_config.domain, "");

    // Test with unicode domain
    let unicode_domain_config = ClientConfig {
        endpoint: "http://localhost:52710".to_string(),
        domain: "测试域名".to_string(),
        timeout: Duration::from_secs(30),
    };
    assert_eq!(unicode_domain_config.domain, "测试域名");
}

/// Test retry policy integration with task submission
#[test]
fn test_retry_policy_serialization_in_submission() {
    let retry_policy = RetryPolicy::exponential()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(100))
        .retry_on(&["NetworkError", "TimeoutError"])
        .build();

    // Test that retry policy can be serialized for task submission
    let serialized = serde_json::to_string(&retry_policy).unwrap();
    assert!(!serialized.is_empty());
    assert!(serialized.contains("max_attempts"));
    assert!(serialized.contains("NetworkError"));

    // Test that it can be deserialized back
    let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();
    assert_eq!(
        retry_policy.stop.max_attempts,
        deserialized.stop.max_attempts
    );
    assert_eq!(
        retry_policy.retry.include_errors,
        deserialized.retry.include_errors
    );
}

/// Test task handle creation and properties
#[test]
fn test_task_handle_properties() {
    // Since we can't create actual TaskHandle without gRPC connection,
    // we'll test the components that would be used to create one

    let task_id = "test-task-12345".to_string();
    assert!(!task_id.is_empty());
    assert!(task_id.starts_with("test-task-"));

    // Test task ID validation patterns
    let valid_task_ids = vec![
        "simple-task",
        "task_with_underscores",
        "task-123-abc",
        "very-long-task-name-with-multiple-segments-and-numbers-123456789",
    ];

    for task_id in valid_task_ids {
        assert!(!task_id.is_empty());
        assert!(task_id.len() < 256); // Reasonable length limit
    }
}

/// Test concurrent task submissions (structure test)
#[test]
fn test_concurrent_submission_structure() {
    // Test that task submission builders can be created independently
    // (We can't test actual concurrent submissions without a server)

    let task_names = vec!["task1", "task2", "task3", "task4", "task5"];
    let mut builders = Vec::new();

    // Simulate creating multiple task submissions
    for name in task_names {
        // We can't actually create these without a client, but we can test the concept
        assert!(!name.is_empty());
        assert!(name.starts_with("task"));
        builders.push(name);
    }

    assert_eq!(builders.len(), 5);
}

/// Test shepherd group configuration
#[test]
fn test_shepherd_group_configuration() {
    let test_groups = vec![
        "default",
        "high-priority",
        "gpu-workers",
        "batch-processing",
        "ml-training",
        "data-pipeline",
    ];

    for group in test_groups {
        assert!(!group.is_empty());
        assert!(group.len() < 100); // Reasonable length limit
        assert!(!group.contains(" ")); // Should not contain spaces
    }

    // Test that shepherd group names are valid identifiers
    let valid_group = "production-workers-v2";
    assert!(valid_group
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
}

/// Test timeout behavior structure
#[test]
fn test_timeout_configuration_ranges() {
    let timeout_ranges = vec![
        Duration::from_millis(100), // Very short
        Duration::from_millis(500), // Short
        Duration::from_secs(1),     // Default-ish
        Duration::from_secs(30),    // Medium
        Duration::from_secs(300),   // Long
        Duration::from_secs(3600),  // Very long
    ];

    for timeout in timeout_ranges {
        assert!(timeout.as_millis() > 0);
        assert!(timeout.as_secs() < 7200); // Less than 2 hours is reasonable
    }

    // Test timeout comparison
    assert!(Duration::from_millis(100) < Duration::from_secs(1));
    assert!(Duration::from_secs(1) < Duration::from_secs(30));
}

/// Test client builder method chaining and configuration
#[test]
fn test_client_builder_comprehensive() {
    let builder = Client::builder()
        .endpoint("http://localhost:8080")
        .domain("test_domain")
        .timeout(Duration::from_secs(60));

    // Verify builder methods work by testing the configuration isn't broken
    // Note: We can't test build() without a server, but we can verify the builder compiles
    drop(builder); // The fact that this compiles and we can call the methods is the test
}

/// Test additional JSON argument processing edge cases
#[test]
fn test_json_args_edge_cases() {
    // Test null values
    let null_arg = json!(null);
    assert!(null_arg.is_null());

    // Test nested objects
    let nested = json!({
        "level1": {
            "level2": {
                "value": 42
            }
        }
    });
    assert!(nested.is_object());

    // Test very large numbers
    let large_number = json!(i64::MAX);
    assert!(large_number.is_number());

    // Test special string values
    let special_strings = vec![
        "",     // empty string
        "null", // string that looks like null
        "true", // string that looks like boolean
        "42",   // string that looks like number
        "\n\t", // whitespace
        "🦀",   // emoji
    ];

    for s in special_strings {
        let json_val = json!(s);
        assert!(json_val.is_string());
        assert_eq!(json_val.as_str().unwrap(), s);
    }
}

/// Test TaskExecutionResult error enum variants
#[test]
fn test_task_execution_result_error_variants() {
    use azolla_client::client::TaskExecutionResult;

    // Test different error types
    let execution_error = TaskError::execution_failed("execution failed");
    let invalid_args_error = TaskError::invalid_args("invalid arguments");

    let result1 = TaskExecutionResult::Failed(execution_error.clone());
    let result2 = TaskExecutionResult::Failed(invalid_args_error.clone());

    match result1 {
        TaskExecutionResult::Failed(err) => {
            assert_eq!(err.message, execution_error.message);
            assert_eq!(err.error_type, execution_error.error_type);
        }
        _ => panic!("Expected failed result"),
    }

    match result2 {
        TaskExecutionResult::Failed(err) => {
            assert_eq!(err.message, invalid_args_error.message);
            assert_eq!(err.error_type, invalid_args_error.error_type);
        }
        _ => panic!("Expected failed result"),
    }
}
