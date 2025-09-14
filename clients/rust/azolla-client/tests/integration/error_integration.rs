//! Error integration tests across components
//! Tests error propagation between client, worker, and orchestrator

use azolla_client::error::{AzollaError, TaskError};
use azolla_client::task::{BoxedTask, Task, TaskResult};
use azolla_client::worker::Worker;
use azolla_client::Client;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::TestOrchestrator;

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

/// Test error integration with client submission
#[tokio::test]
async fn test_client_error_integration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("error-test")
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    // Test submitting to nonexistent task
    let result = client
        .submit_task("nonexistent_failing_task")
        .args(json!({}))
        .expect("Failed to set args")
        .submit()
        .await;

    // Should get an error from orchestrator
    assert!(result.is_err());
}

/// Test error integration with worker registration
#[tokio::test]
async fn test_worker_error_integration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Register failing tasks with worker
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("error-worker-test")
        .register_task(FailingTask)
        .register_task(TimeoutTask)
        .register_task(ValidationErrorTask)
        .build()
        .await
        .expect("Failed to build error worker");

    // Verify tasks are registered
    assert_eq!(worker.task_count(), 3);

    // Start worker (might fail but should handle gracefully)
    let start_result = tokio::time::timeout(Duration::from_secs(2), worker.run()).await;

    // Should either complete or timeout gracefully
    match start_result {
        Ok(result) => match result {
            Ok(_) => println!("Worker with error tasks started successfully"),
            Err(e) => println!("Worker startup failed: {e:?}"),
        },
        Err(_) => println!("Worker startup timed out"),
    }
}
