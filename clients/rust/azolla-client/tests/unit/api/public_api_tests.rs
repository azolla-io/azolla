//! Public API compatibility tests
//! Tests that ensure public API remains stable and all types are importable

use azolla_client::client::{Client, ClientConfig};
use azolla_client::convert::FromJsonValue;
use azolla_client::error::{AzollaError, TaskError};
use azolla_client::retry_policy::RetryPolicy;
use azolla_client::task::{BoxedTask, Task, TaskResult};
use azolla_client::worker::{Worker, WorkerConfig};
use azolla_client::*;
use serde_json::json;
use std::time::Duration;

/// Test that all public APIs can be imported and used together
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

/// Test that VERSION constant is accessible
#[test]
fn test_version_constant() {
    // Test that version constant is accessible and reasonable
    assert!(!VERSION.is_empty());
    assert!(VERSION.chars().any(|c| c.is_ascii_digit()));

    println!("Azolla client version: {VERSION}");
}

/// Test public API type visibility and construction
#[test]
fn test_public_type_construction() {
    // Test ClientConfig construction
    let client_config = ClientConfig {
        endpoint: "http://test:8080".to_string(),
        domain: "test".to_string(),
        timeout: Duration::from_secs(10),
    };
    assert_eq!(client_config.endpoint, "http://test:8080");

    // Test WorkerConfig construction
    let worker_config = WorkerConfig {
        orchestrator_endpoint: "test:8080".to_string(),
        domain: "test".to_string(),
        shepherd_group: "test".to_string(),
        max_concurrency: 5,
        heartbeat_interval: Duration::from_secs(30),
    };
    assert_eq!(worker_config.max_concurrency, 5);

    // Test TaskError construction
    let task_error = TaskError {
        error_type: "TestError".to_string(),
        message: "Test message".to_string(),
        code: Some("TEST_001".to_string()),
        stacktrace: None,
        data: Some(json!({"key": "value"})),
    };
    assert_eq!(task_error.error_type, "TestError");
}

/// Test trait implementations are available
#[test]
fn test_public_trait_implementations() {
    // Test Debug trait
    let config = ClientConfig::default();
    let debug_str = format!("{config:?}");
    assert!(!debug_str.is_empty());

    // Test Clone trait
    let cloned_config = config.clone();
    assert_eq!(config.endpoint, cloned_config.endpoint);

    // Test field access (manual comparison since no PartialEq)
    assert_eq!(config.endpoint, cloned_config.endpoint);
    assert_eq!(config.domain, cloned_config.domain);
    assert_eq!(config.timeout, cloned_config.timeout);

    // Test configuration field access
    let worker_config = WorkerConfig::default();
    assert!(!worker_config.orchestrator_endpoint.is_empty());
    assert!(!worker_config.domain.is_empty());
}

/// Test error trait implementations
#[test]
fn test_error_trait_implementations() {
    let task_error = TaskError::execution_failed("test error");
    let azolla_error = AzollaError::TaskFailed(task_error);

    // Test Display trait
    let display_str = format!("{azolla_error}");
    assert!(!display_str.is_empty());

    // Test std::error::Error trait
    let _: &dyn std::error::Error = &azolla_error;

    // Test Debug trait
    let debug_str = format!("{azolla_error:?}");
    assert!(!debug_str.is_empty());
}

/// Test builder pattern APIs
#[test]
fn test_builder_patterns() {
    // Test Client builder
    let client_builder = Client::builder()
        .endpoint("http://test:9000")
        .domain("builder-test")
        .timeout(Duration::from_secs(60));

    let debug_output = format!("{client_builder:?}");
    assert!(debug_output.contains("builder-test"));

    // Test RetryPolicy builder
    let retry_policy = RetryPolicy::builder()
        .max_attempts(5)
        .initial_delay(Duration::from_millis(100))
        .retry_on(&["NetworkError", "TimeoutError"])
        .build();

    assert_eq!(retry_policy.stop.max_attempts, Some(5));

    // Test Worker builder
    let worker_builder = Worker::builder()
        .orchestrator("test:8080")
        .domain("worker-test")
        .shepherd_group("test-group")
        .max_concurrency(10);

    // Builder should be constructible
    assert!(std::mem::size_of_val(&worker_builder) > 0);
}

/// Test macro availability when feature is enabled
#[cfg(feature = "macros")]
#[test]
fn test_macro_availability() {
    // Test that azolla_task macro is available
    use azolla_client::azolla_task;

    // This test just verifies the macro can be imported
    // Actual macro functionality is tested elsewhere
    let _macro_available = true;
}

/// Test conversion trait public API
#[test]
fn test_conversion_trait_api() {
    // Test FromJsonValue implementations
    let int_result: Result<i32, _> = FromJsonValue::try_from(json!(42));
    assert_eq!(int_result.unwrap(), 42);

    let string_result: Result<String, _> = FromJsonValue::try_from(json!("hello"));
    assert_eq!(string_result.unwrap(), "hello");

    let bool_result: Result<bool, _> = FromJsonValue::try_from(json!(true));
    assert_eq!(bool_result.unwrap(), true);

    let vec_result: Result<Vec<i32>, _> = FromJsonValue::try_from(json!([1, 2, 3]));
    assert_eq!(vec_result.unwrap(), vec![1, 2, 3]);

    let option_result: Result<Option<i32>, _> = FromJsonValue::try_from(json!(null));
    assert_eq!(option_result.unwrap(), None);
}

/// Test Task trait public API
#[test]
fn test_task_trait_api() {
    struct PublicTestTask;

    impl Task for PublicTestTask {
        type Args = i32;

        fn name(&self) -> &'static str {
            "public_test_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"input": args, "doubled": args * 2})) })
        }
    }

    let task = PublicTestTask;
    assert_eq!(Task::name(&task), "public_test_task");

    // Test parse_args method
    let args = vec![json!(21)];
    let parsed = PublicTestTask::parse_args(args).unwrap();
    assert_eq!(parsed, 21);
}

/// Test BoxedTask trait public API
#[tokio::test]
async fn test_boxed_task_trait_api() {
    use std::sync::Arc;

    struct BoxedTestTask;

    impl Task for BoxedTestTask {
        type Args = String;

        fn name(&self) -> &'static str {
            "boxed_test_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"message": args, "length": args.len()})) })
        }
    }

    let task: Arc<dyn BoxedTask> = Arc::new(BoxedTestTask);
    assert_eq!(task.name(), "boxed_test_task");

    let args = vec![json!("test message")];
    let result = task.execute_json(args).await.unwrap();
    assert_eq!(result["message"], "test message");
    assert_eq!(result["length"], 12);
}

/// Test comprehensive API surface
#[test]
fn test_comprehensive_api_surface() {
    // Test that all major types are accessible
    let _client_config: ClientConfig = Default::default();
    let _worker_config: WorkerConfig = Default::default();
    let _retry_policy: RetryPolicy = Default::default();

    // Test error types
    let _task_error: TaskError = TaskError::execution_failed("test");
    let _azolla_error: AzollaError = AzollaError::InvalidConfig("test".to_string());

    // Test builder types (can't construct without async but can reference)
    let _client_builder = Client::builder();
    let _worker_builder = Worker::builder();
    let _retry_builder = RetryPolicy::builder();

    println!("All major API types are accessible");
}

/// Test re-exports and public visibility
#[test]
fn test_reexports() {
    // Test that commonly used types are re-exported at crate root
    use azolla_client::{AzollaError, Client, RetryPolicy, TaskError, Worker};

    // These should compile without qualification
    let _client_builder = Client::builder();
    let _worker_builder = Worker::builder();
    let _retry_policy = RetryPolicy::default();
    let _task_error = TaskError::invalid_args("test");
    let _azolla_error = AzollaError::InvalidConfig("test".to_string());
}

/// Test API consistency and conventions
#[test]
fn test_api_conventions() {
    // Test naming conventions
    assert_eq!(
        std::any::type_name::<ClientConfig>(),
        "azolla_client::client::ClientConfig"
    );
    assert_eq!(
        std::any::type_name::<WorkerConfig>(),
        "azolla_client::worker::WorkerConfig"
    );
    assert_eq!(
        std::any::type_name::<TaskError>(),
        "azolla_client::error::TaskError"
    );
    assert_eq!(
        std::any::type_name::<AzollaError>(),
        "azolla_client::error::AzollaError"
    );

    // Test that configurations have sensible defaults
    let client_config = ClientConfig::default();
    assert!(!client_config.endpoint.is_empty());
    assert!(!client_config.domain.is_empty());
    assert!(client_config.timeout.as_secs() > 0);

    let worker_config = WorkerConfig::default();
    assert!(!worker_config.orchestrator_endpoint.is_empty());
    assert!(!worker_config.domain.is_empty());
    assert!(!worker_config.shepherd_group.is_empty());
    assert!(worker_config.max_concurrency > 0);
    assert!(worker_config.heartbeat_interval.as_secs() > 0);
}

/// Test deprecation handling and backwards compatibility
#[test]
fn test_backwards_compatibility() {
    // Test that old API patterns still work

    // Direct configuration construction (backwards compatible)
    let config = ClientConfig {
        endpoint: "http://localhost:52710".to_string(),
        domain: "default".to_string(),
        timeout: Duration::from_secs(30),
    };
    let default_config = ClientConfig::default();
    assert_eq!(config.endpoint, default_config.endpoint);
    assert_eq!(config.domain, default_config.domain);
    assert_eq!(config.timeout, default_config.timeout);

    // Error construction (backwards compatible)
    let error1 = TaskError::execution_failed("message");
    let error2 = TaskError {
        error_type: "ExecutionError".to_string(),
        message: "message".to_string(),
        code: None,
        stacktrace: None,
        data: None,
    };
    assert_eq!(error1.error_type, error2.error_type);
    assert_eq!(error1.message, error2.message);
}

/// Test feature flag compatibility
#[test]
fn test_feature_flags() {
    // Test default features
    #[cfg(feature = "macros")]
    {
        println!("Macros feature is enabled");
        use azolla_client::azolla_task;
        let _macro_available = true;
    }

    #[cfg(not(feature = "macros"))]
    {
        println!("Macros feature is disabled");
        // Should still be able to use manual Task implementations
        struct ManualTask;
        impl Task for ManualTask {
            type Args = ();
            fn name(&self) -> &'static str {
                "manual"
            }
            fn execute(
                &self,
                _: Self::Args,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult> + Send + '_>>
            {
                Box::pin(async { Ok(json!({})) })
            }
        }
        let task = ManualTask;
        assert_eq!(Task::name(&task), "manual");
    }
}
