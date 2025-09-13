/// Test the purpose of error handling: ensure errors propagate correctly through the entire stack
/// Test the expected behavior: errors from tasks, workers, and clients should be handled consistently
use azolla_client::error::{AzollaError, TaskError};
use azolla_client::task::{BoxedTask, Task, TaskResult};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

// Mock task implementations for error testing
struct FailingTask;
struct TimeoutTask;
struct MemoryErrorTask;
struct NetworkErrorTask;
struct ValidationErrorTask;

impl Task for FailingTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "failing_task"
    }

    fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async { Err(TaskError::execution_failed("Intentional test failure")) })
    }
}

impl Task for TimeoutTask {
    type Args = u64; // Sleep duration in milliseconds

    fn name(&self) -> &'static str {
        "timeout_task"
    }

    fn execute(
        &self,
        duration_ms: Self::Args,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
            Err(TaskError::execution_failed("Task timed out"))
        })
    }
}

impl Task for MemoryErrorTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "memory_error_task"
    }

    fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async {
            Err(TaskError::new("Out of memory")
                .with_error_type("MemoryError")
                .with_error_code("MEM_001"))
        })
    }
}

impl Task for NetworkErrorTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "network_error_task"
    }

    fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async {
            Err(TaskError::new("Network connection failed")
                .with_error_type("NetworkError")
                .with_error_code("NET_001"))
        })
    }
}

#[derive(serde::Deserialize, Debug)]
struct ValidationArgs {
    email: String,
    age: i32,
}

impl Task for ValidationErrorTask {
    type Args = ValidationArgs;

    fn name(&self) -> &'static str {
        "validation_error_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            if !args.email.contains('@') {
                return Err(TaskError::invalid_args("Invalid email format"));
            }
            if args.age < 0 || args.age > 150 {
                return Err(TaskError::invalid_args("Age must be between 0 and 150"));
            }
            Ok(json!({"status": "validation_passed"}))
        })
    }
}

/// Test that task errors are properly propagated through BoxedTask execution
#[tokio::test]
async fn test_boxed_task_error_propagation() {
    let task: Arc<dyn BoxedTask> = Arc::new(FailingTask);
    let args = vec![];

    let result = task.execute_json(args).await;

    // Should get error result
    assert!(result.is_err());
    let TaskError {
        error_type,
        message,
        ..
    } = result.unwrap_err();
    assert_eq!(error_type, "ExecutionError");
    assert!(message.contains("Intentional test failure"));
}

/// Test different error types propagate with correct metadata through BoxedTask
#[tokio::test]
async fn test_error_type_propagation() {
    let test_cases: Vec<(Arc<dyn BoxedTask>, &str, &str)> = vec![
        (Arc::new(MemoryErrorTask), "MemoryError", "MEM_001"),
        (Arc::new(NetworkErrorTask), "NetworkError", "NET_001"),
    ];

    for (task_impl, expected_type, expected_code) in test_cases {
        let args = vec![];
        let result = task_impl.execute_json(args).await;

        assert!(result.is_err());
        let TaskError {
            error_type, code, ..
        } = result.unwrap_err();
        assert_eq!(error_type, expected_type);
        assert_eq!(code, Some(expected_code.to_string()));
    }
}

/// Test validation errors propagate correctly through BoxedTask
#[tokio::test]
async fn test_validation_error_propagation() {
    let validation_task: Arc<dyn BoxedTask> = Arc::new(ValidationErrorTask);

    let validation_cases = vec![
        (
            vec![json!({"email": "invalid-email", "age": 25})],
            "Invalid email format",
        ),
        (
            vec![json!({"email": "test@example.com", "age": -5})],
            "Age must be between 0 and 150",
        ),
        (
            vec![json!({"email": "test@example.com", "age": 200})],
            "Age must be between 0 and 150",
        ),
    ];

    for (args, expected_message) in validation_cases {
        let result = validation_task.execute_json(args).await;

        assert!(result.is_err());
        let TaskError {
            error_type,
            message,
            ..
        } = result.unwrap_err();
        assert_eq!(error_type, "InvalidArguments");
        assert!(message.contains(expected_message));
    }
}

/// Test successful validation case through BoxedTask
#[tokio::test]
async fn test_validation_success_case() {
    let validation_task: Arc<dyn BoxedTask> = Arc::new(ValidationErrorTask);
    let valid_args = vec![json!({"email": "test@example.com", "age": 25})];

    let result = validation_task.execute_json(valid_args).await;

    assert!(result.is_ok());
    let success_value = result.unwrap();
    assert_eq!(success_value["status"], "validation_passed");
}

/// Test argument parsing errors through Task trait
#[test]
fn test_argument_parsing_errors() {
    let invalid_arg_cases = vec![
        (
            vec![json!("not_an_object")],
            "Failed to parse single argument",
        ),
        (
            vec![json!({"invalid": "structure"})],
            "Failed to parse single argument",
        ),
        (vec![], "No arguments provided"),
    ];

    for (invalid_args, expected_error_fragment) in invalid_arg_cases {
        let result = ValidationErrorTask::parse_args(invalid_args);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains(expected_error_fragment));
    }
}

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
        assert_eq!(original_error.stacktrace, deserialized.stacktrace);
        assert_eq!(original_error.data, deserialized.data);
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
        stacktrace: Some("at validation.rs:123".to_string()),
        data: Some(error_data.clone()),
    };

    // Test serialization preserves complex data
    let serialized = serde_json::to_string(&task_error).unwrap();
    let deserialized: TaskError = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized.data, Some(error_data));
    assert_eq!(deserialized.code, Some("VAL_001".to_string()));
    assert_eq!(
        deserialized.stacktrace,
        Some("at validation.rs:123".to_string())
    );
}

/// Test error propagation in async contexts
#[tokio::test]
async fn test_async_error_propagation() {
    async fn inner_function() -> Result<String, TaskError> {
        Err(TaskError::execution_failed("Inner function failed"))
    }

    async fn middle_function() -> Result<String, TaskError> {
        inner_function().await
    }

    async fn outer_function() -> Result<String, AzollaError> {
        let result = middle_function().await?;
        Ok(result)
    }

    let result = outer_function().await;
    assert!(result.is_err());

    match result.unwrap_err() {
        AzollaError::TaskFailed(task_error) => {
            assert_eq!(task_error.error_type, "ExecutionError");
            assert_eq!(task_error.message, "Inner function failed");
        }
        _ => panic!("Expected TaskFailed error"),
    }
}

/// Test concurrent error handling
#[tokio::test]
async fn test_concurrent_error_handling() {
    let error_tasks = vec![
        Arc::new(FailingTask) as Arc<dyn BoxedTask>,
        Arc::new(MemoryErrorTask) as Arc<dyn BoxedTask>,
        Arc::new(NetworkErrorTask) as Arc<dyn BoxedTask>,
    ];

    let mut handles = Vec::new();

    for (i, task) in error_tasks.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let args = vec![];
            task.execute_json(args).await
        });
        handles.push((i, handle));
    }

    let mut error_count = 0;
    for (i, handle) in handles {
        let result = handle.await.unwrap();
        assert!(result.is_err(), "Task {i} should have failed");
        error_count += 1;
    }

    assert_eq!(error_count, 3, "All concurrent tasks should have failed");
}

/// Test error handling with timeout scenarios
#[tokio::test]
async fn test_timeout_error_handling() {
    let timeout_task = TimeoutTask;
    let boxed_task: Arc<dyn BoxedTask> = Arc::new(timeout_task);

    // Test short timeout
    let short_timeout_args = vec![json!(50)]; // 50ms
    let result = boxed_task.execute_json(short_timeout_args).await;
    assert!(result.is_err());

    let TaskError {
        error_type,
        message,
        ..
    } = result.unwrap_err();
    assert_eq!(error_type, "ExecutionError");
    assert!(message.contains("timed out"));
}

/// Test error recovery patterns
#[tokio::test]
async fn test_error_recovery_patterns() {
    // Simulate a task that fails once then succeeds
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc as StdArc;

    struct RetryableTask {
        has_failed: StdArc<AtomicBool>,
    }

    impl Task for RetryableTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "retryable_task"
        }

        fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            let has_failed = self.has_failed.clone();
            Box::pin(async move {
                if !has_failed.swap(true, Ordering::Relaxed) {
                    Err(TaskError::execution_failed("First attempt failed"))
                } else {
                    Ok(json!({"status": "success_on_retry"}))
                }
            })
        }
    }

    let retryable_task = RetryableTask {
        has_failed: StdArc::new(AtomicBool::new(false)),
    };

    // First execution should fail
    let first_result = retryable_task.execute(()).await;
    assert!(first_result.is_err());

    // Second execution should succeed
    let second_result = retryable_task.execute(()).await;
    assert!(second_result.is_ok());
    assert_eq!(second_result.unwrap()["status"], "success_on_retry");
}
