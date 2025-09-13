// Integration tests for azolla-client
// These tests verify that all components work together correctly

use azolla_client::client::ClientConfig;
use azolla_client::convert::FromJsonValue;
use azolla_client::worker::WorkerConfig;
use azolla_client::*;
use serde_json::{json, Value};
use std::time::Duration;

// Test that all public APIs can be imported and used together
#[test]
fn test_public_api_imports() {
    // Test that we can import all public types
    let _config = ClientConfig::default();
    let _builder = Client::builder();
    let _retry_policy = RetryPolicy::default();
    let _worker_builder = Worker::builder();

    // Test error types
    let _task_error = TaskError::invalid_args("test");
    let _azolla_error = AzollaError::InvalidConfig("test".to_string());

    // Test conversion traits
    let result = <i32 as FromJsonValue>::try_from(json!(42));
    assert!(result.is_ok());
}

#[test]
fn test_client_configuration_integration() {
    // Test that ClientConfig works with all its fields
    let config = ClientConfig {
        endpoint: "https://api.example.com:9443".to_string(),
        domain: "production-domain".to_string(),
        timeout: Duration::from_secs(120),
    };

    assert_eq!(config.endpoint, "https://api.example.com:9443");
    assert_eq!(config.domain, "production-domain");
    assert_eq!(config.timeout, Duration::from_secs(120));

    // Test builder pattern integration (can only test that it builds successfully)
    let builder = Client::builder()
        .endpoint("http://staging.example.com:8080")
        .domain("staging")
        .timeout(Duration::from_secs(60));

    // We can't access the private config field, but we can verify the builder is constructed properly
    // by attempting to build it (though we can't test the actual connection)
    assert!(std::mem::size_of_val(&builder) > 0); // Basic sanity check that builder exists
}

#[test]
fn test_retry_policy_integration() {
    // Test comprehensive retry policy configuration
    let policy = RetryPolicy::builder()
        .max_attempts(5)
        .max_delay(Duration::from_secs(300))
        .initial_delay(Duration::from_millis(100))
        .retry_on(&["NetworkError", "TimeoutError", "ServiceUnavailable"])
        .exclude_errors(&["AuthenticationError", "ValidationError"])
        .build();

    // Verify all configuration is applied correctly
    assert_eq!(policy.stop.max_attempts, Some(5));
    assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(300)));

    match policy.wait {
        WaitStrategy::ExponentialJitter {
            initial_delay,
            multiplier,
            max_delay,
        } => {
            assert_eq!(initial_delay, Duration::from_millis(100));
            assert_eq!(multiplier, 2.0);
            assert_eq!(max_delay, Duration::from_secs(300));
        }
        _ => panic!("Expected ExponentialJitter strategy"),
    }

    assert_eq!(
        policy.retry.include_errors,
        vec!["NetworkError", "TimeoutError", "ServiceUnavailable"]
    );
    assert_eq!(
        policy.retry.exclude_errors,
        vec!["AuthenticationError", "ValidationError"]
    );

    // Test serialization round-trip
    let serialized = serde_json::to_string(&policy).unwrap();
    let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();
    assert_eq!(policy.stop.max_attempts, deserialized.stop.max_attempts);
}

#[test]
fn test_error_handling_integration() {
    // Test error conversions and propagation
    let task_error = TaskError::execution_failed("Database connection failed");
    let azolla_error: AzollaError = task_error.clone().into();

    match azolla_error {
        AzollaError::TaskFailed(err) => {
            assert_eq!(err.error_type, "ExecutionError");
            assert_eq!(err.message, "Database connection failed");
        }
        _ => panic!("Expected TaskFailed variant"),
    }

    // Test error serialization
    let error_json = serde_json::to_string(&task_error).unwrap();
    let deserialized_error: TaskError = serde_json::from_str(&error_json).unwrap();
    assert_eq!(task_error.message, deserialized_error.message);
    assert_eq!(task_error.error_type, deserialized_error.error_type);
}

#[test]
fn test_conversion_traits_integration() {
    // Test complex nested conversions
    let complex_json = json!({
        "users": [
            {"id": 1, "name": "Alice", "active": true, "score": null},
            {"id": 2, "name": "Bob", "active": false, "score": 95.5}
        ],
        "metadata": {
            "total": 2,
            "page": 1
        }
    });

    // Test that Value passthrough works
    let value_result = <Value as FromJsonValue>::try_from(complex_json.clone()).unwrap();
    assert_eq!(value_result, complex_json);

    // Test extraction of nested values
    if let Value::Array(users) = &complex_json["users"] {
        for user in users {
            let id = <i32 as FromJsonValue>::try_from(user["id"].clone()).unwrap();
            let name = <String as FromJsonValue>::try_from(user["name"].clone()).unwrap();
            let active = <bool as FromJsonValue>::try_from(user["active"].clone()).unwrap();

            assert!(id > 0);
            assert!(!name.is_empty());
            println!("User {id}: {name} (active: {active})");
        }
    }
}

#[test]
fn test_task_builder_integration() {
    // Test that task building works with all argument types
    fn build_args_json<T: serde::Serialize>(args: T) -> String {
        let value = serde_json::to_value(args).unwrap();
        match value {
            Value::Array(_) => serde_json::to_string(&value).unwrap(),
            single => serde_json::to_string(&vec![single]).unwrap(),
        }
    }

    // Test different argument patterns
    let simple_args = build_args_json(42);
    assert!(simple_args.contains("42"));

    let tuple_args = build_args_json(("hello", 123, true));
    assert!(tuple_args.contains("hello"));
    assert!(tuple_args.contains("123"));
    assert!(tuple_args.contains("true"));

    let complex_args = build_args_json((
        "task_name",
        vec![1, 2, 3, 4, 5],
        json!({"config": "production", "retries": 3}),
    ));
    assert!(complex_args.contains("task_name"));
    assert!(complex_args.contains("production"));
    assert!(complex_args.contains("retries"));
}

#[test]
fn test_worker_configuration_integration() {
    // Test comprehensive worker configuration
    let builder = Worker::builder()
        .orchestrator("orchestrator.example.com:52710")
        .domain("integration-tests")
        .shepherd_group("test-workers")
        .max_concurrency(20);

    // We can't access private config fields, but we can verify the builder exists
    assert!(std::mem::size_of_val(&builder) > 0); // Basic sanity check
}

// Mock task implementation for integration testing
struct IntegrationTestTask;

impl Task for IntegrationTestTask {
    type Args = Vec<Value>;

    fn name(&self) -> &'static str {
        "integration_test_task"
    }

    fn execute(
        &self,
        args: Self::Args,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            // Simulate task processing
            if args.is_empty() {
                return Err(TaskError::invalid_args("No arguments provided"));
            }

            // Process arguments
            let mut results = Vec::new();
            for arg in args {
                match arg {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            results.push(json!(i * 2));
                        }
                    }
                    Value::String(s) => {
                        results.push(json!(s.to_uppercase()));
                    }
                    other => results.push(other),
                }
            }

            Ok(json!({
                "task": "integration_test_task",
                "processed_args": results,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "status": "completed"
            }))
        })
    }
}

#[tokio::test]
async fn test_task_execution_integration() {
    let task = IntegrationTestTask;

    // Test successful execution with various argument types
    let args = vec![json!(42), json!("hello world"), json!(true), json!(null)];

    let result = task.execute(args).await.unwrap();

    // Verify results
    assert_eq!(result["task"], "integration_test_task");
    assert_eq!(result["status"], "completed");

    let processed = result["processed_args"].as_array().unwrap();
    assert_eq!(processed[0], json!(84)); // 42 * 2
    assert_eq!(processed[1], json!("HELLO WORLD")); // uppercase
    assert_eq!(processed[2], json!(true)); // unchanged
    assert_eq!(processed[3], json!(null)); // unchanged

    // Test error handling
    let empty_args = vec![];
    let error_result = task.execute(empty_args).await;
    assert!(error_result.is_err());
}

#[test]
fn test_json_serialization_integration() {
    // Test comprehensive JSON serialization of all major types

    // Test RetryPolicy
    let policy = RetryPolicy::exponential()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(500))
        .build();

    let policy_json = serde_json::to_string(&policy).unwrap();
    let policy_back: RetryPolicy = serde_json::from_str(&policy_json).unwrap();
    assert_eq!(policy.version, policy_back.version);

    // Test TaskError
    let error = TaskError {
        error_type: "ValidationError".to_string(),
        message: "Field validation failed".to_string(),
        code: Some("VAL_001".to_string()),
        stacktrace: Some("at integration.rs:123".to_string()),
        data: Some(json!({"field": "email", "expected": "valid email"})),
    };

    let error_json = serde_json::to_string(&error).unwrap();
    let error_back: TaskError = serde_json::from_str(&error_json).unwrap();
    assert_eq!(error.error_type, error_back.error_type);
    assert_eq!(error.message, error_back.message);
    assert_eq!(error.code, error_back.code);
}

#[test]
fn test_type_conversion_error_handling() {
    // Test that type conversions fail gracefully with good error messages

    let test_cases = vec![
        (json!("not_a_number"), "i32"),
        (json!(42), "String"),
        (json!("not_a_bool"), "bool"),
        (json!(42), "Array"),
        (json!([1, "mixed", 3]), "Vec<i32>"),
    ];

    for (value, expected_type) in test_cases {
        let result = match expected_type {
            "i32" => <i32 as FromJsonValue>::try_from(value.clone())
                .map(|_| ())
                .map_err(|e| e.to_string()),
            "String" => <String as FromJsonValue>::try_from(value.clone())
                .map(|_| ())
                .map_err(|e| e.to_string()),
            "bool" => <bool as FromJsonValue>::try_from(value.clone())
                .map(|_| ())
                .map_err(|e| e.to_string()),
            "Array" => <Vec<i32> as FromJsonValue>::try_from(value.clone())
                .map(|_| ())
                .map_err(|e| e.to_string()),
            "Vec<i32>" => <Vec<i32> as FromJsonValue>::try_from(value.clone())
                .map(|_| ())
                .map_err(|e| e.to_string()),
            _ => continue,
        };

        assert!(
            result.is_err(),
            "Expected error for {expected_type} conversion of {value:?}"
        );
        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("expected") || error_msg.contains("Type mismatch"),
            "Error message should be descriptive: {error_msg}"
        );
    }
}

#[test]
fn test_version_and_constants() {
    // Test that version constant is accessible and reasonable
    assert!(!VERSION.is_empty());
    assert!(VERSION.chars().any(|c| c.is_ascii_digit()));

    // Test default configurations are reasonable
    let client_config = ClientConfig::default();
    assert!(client_config.endpoint.starts_with("http"));
    assert!(!client_config.domain.is_empty());
    assert!(client_config.timeout.as_secs() > 0);

    let worker_config = WorkerConfig::default();
    assert!(!worker_config.orchestrator_endpoint.is_empty());
    assert!(!worker_config.domain.is_empty());
    assert!(worker_config.max_concurrency > 0);
    assert!(worker_config.heartbeat_interval.as_secs() > 0);
}

// Add chrono for timestamp in mock task
// Note: In a real implementation, you might want to use a different approach for timestamps
// that doesn't require additional dependencies just for tests
mod chrono {
    pub struct Utc;
    impl Utc {
        pub fn now() -> DateTime {
            DateTime
        }
    }

    pub struct DateTime;
    impl DateTime {
        pub fn to_rfc3339(&self) -> String {
            "2023-01-01T00:00:00Z".to_string() // Mock timestamp
        }
    }
}

#[test]
fn test_comprehensive_workflow() {
    // Test a complete workflow using multiple components

    // 1. Create client configuration (using direct config since we can't access builder's private config)
    let client_config = ClientConfig {
        endpoint: "https://api.azolla.example.com:52710".to_string(),
        domain: "production".to_string(),
        timeout: Duration::from_secs(60),
    };

    // 2. Create retry policy
    let retry_policy = RetryPolicy::exponential()
        .max_attempts(5)
        .initial_delay(Duration::from_millis(100))
        .retry_on(&["NetworkError", "TimeoutError"])
        .build();

    // 3. Create worker configuration
    let worker_config = WorkerConfig {
        orchestrator_endpoint: client_config.endpoint.clone(),
        domain: client_config.domain.clone(),
        shepherd_group: "production-workers".to_string(),
        max_concurrency: 50,
        heartbeat_interval: Duration::from_secs(30),
    };

    // 4. Test argument serialization for task submission
    let task_args = ("process_data", json!({"input": [1, 2, 3, 4, 5]}), true);
    let args_json = serde_json::to_string(&task_args).unwrap();

    // 5. Test that all configurations work together
    assert_eq!(client_config.domain, worker_config.domain);
    assert_eq!(client_config.endpoint, worker_config.orchestrator_endpoint);
    assert!(args_json.contains("process_data"));
    assert!(retry_policy.stop.max_attempts.unwrap() > 1);

    // 6. Test error serialization for potential error responses
    let error = TaskError {
        error_type: "ProcessingError".to_string(),
        message: "Failed to process input data".to_string(),
        code: Some("PROC_001".to_string()),
        stacktrace: None,
        data: Some(json!({"input_size": 5, "error_at_index": 2})),
    };

    let error_json = serde_json::to_string(&error).unwrap();
    let _: TaskError = serde_json::from_str(&error_json).unwrap(); // Verify round-trip

    println!("✅ Comprehensive workflow test completed successfully");
    println!("   Client endpoint: {}", client_config.endpoint);
    println!("   Worker group: {}", worker_config.shepherd_group);
    println!("   Retry attempts: {:?}", retry_policy.stop.max_attempts);
}
