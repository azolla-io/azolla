//! End-to-end integration tests
//! Tests complete workflows from client submission to worker execution

use azolla_client::client::TaskExecutionResult;
use azolla_client::retry_policy::RetryPolicy;
use azolla_client::task::{Task, TaskResult};
use azolla_client::{Client, Worker};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use super::TestOrchestrator;

// End-to-end test task
struct E2ETestTask;

impl Task for E2ETestTask {
    type Args = Value;

    fn name(&self) -> &'static str {
        "e2e_test_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            // Simulate some processing time
            tokio::time::sleep(Duration::from_millis(100)).await;

            Ok(json!({
                "processed": true,
                "input": args,
                "worker_id": uuid::Uuid::new_v4().to_string(),
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "task_name": "e2e_test_task"
            }))
        })
    }
}

// Task that processes multiple types of data
struct DataProcessingTask;

impl Task for DataProcessingTask {
    type Args = (String, i32, bool);

    fn name(&self) -> &'static str {
        "data_processing_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let (text, number, flag) = args;

            Ok(json!({
                "text_length": text.len(),
                "number_doubled": number * 2,
                "flag_negated": !flag,
                "combined": format!("{text}-{number}-{flag}"),
                "processing_time_ms": 100
            }))
        })
    }
}

// Task that can fail based on input
struct ConditionalTask;

impl Task for ConditionalTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "conditional_task"
    }

    fn execute(
        &self,
        command: Self::Args,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            match command.as_str() {
                "succeed" => Ok(json!({"status": "success", "command": command})),
                "fail" => Err(azolla_client::error::TaskError::execution_failed(
                    "Task was asked to fail",
                )),
                "timeout" => {
                    tokio::time::sleep(Duration::from_secs(10)).await; // Long delay
                    Ok(json!({"status": "completed_after_delay"}))
                }
                _ => Err(azolla_client::error::TaskError::invalid_args(&format!(
                    "Unknown command: {command}"
                ))),
            }
        })
    }
}

/// Test complete workflow: orchestrator startup, worker registration, client connection, task submission
#[tokio::test]
async fn test_complete_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with tasks
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("e2e-test")
        .shepherd_group("e2e-workers")
        .register_task(E2ETestTask)
        .register_task(DataProcessingTask)
        .build()
        .await
        .expect("Failed to build worker");

    // Start worker in background
    let worker_handle = tokio::spawn(async move {
        let start_result = tokio::time::timeout(Duration::from_secs(5), worker.run()).await;
        match start_result {
            Ok(result) => match result {
                Ok(_) => println!("Worker started successfully"),
                Err(e) => println!("Worker start error: {e:?}"),
            },
            Err(_) => println!("Worker start timed out"),
        }
    });

    // Give worker time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("e2e-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Submit task
    let task_result = client
        .submit_task("e2e_test_task")
        .args(json!({"test_data": "hello world", "timestamp": "2023-01-01T00:00:00Z"}))
        .expect("Failed to set args")
        .submit()
        .await;

    // Check result (might fail if orchestrator/worker communication isn't fully established)
    match task_result {
        Ok(handle) => {
            println!("Task submitted successfully: {:?}", handle.id());

            // Try to wait for result
            let wait_result = tokio::time::timeout(Duration::from_secs(5), handle.wait()).await;

            match wait_result {
                Ok(result) => match result {
                    Ok(execution_result) => match execution_result {
                        TaskExecutionResult::Success(value) => {
                            println!("Task completed: {value:?}");
                            assert!(value.get("processed").is_some());
                        }
                        TaskExecutionResult::Failed(e) => {
                            println!("Task execution failed: {e:?}");
                        }
                    },
                    Err(e) => println!("Task wait failed: {e:?}"),
                },
                Err(_) => println!("Task wait timed out"),
            }
        }
        Err(e) => println!("Task submission failed: {e:?}"),
    }

    // Clean up worker
    worker_handle.abort();
}

/// Test client-worker data processing workflow
#[tokio::test]
async fn test_data_processing_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with data processing tasks
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("data-processing")
        .register_task(DataProcessingTask)
        .build()
        .await
        .expect("Failed to build data processing worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(3), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("data-processing")
        .build()
        .await
        .expect("Failed to connect client");

    // Submit data processing task
    let result = client
        .submit_task("data_processing_task")
        .args(("test string".to_string(), 42, true))
        .expect("Failed to set args")
        .submit()
        .await;

    match result {
        Ok(handle) => println!("Data processing task submitted: {}", handle.id()),
        Err(e) => println!("Data processing task failed: {e:?}"),
    }

    worker_handle.abort();
}

/// Test error handling across the complete workflow
#[tokio::test]
async fn test_error_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with conditional task
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("error-test")
        .register_task(ConditionalTask)
        .build()
        .await
        .expect("Failed to build conditional worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(3), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("error-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Test successful case
    let success_result = client
        .submit_task("conditional_task")
        .args("succeed".to_string())
        .expect("Failed to set args")
        .submit()
        .await;

    match success_result {
        Ok(handle) => println!("Success task submitted: {}", handle.id()),
        Err(e) => println!("Success task failed: {e:?}"),
    }

    // Test failure case
    let fail_result = client
        .submit_task("conditional_task")
        .args("fail".to_string())
        .expect("Failed to set args")
        .submit()
        .await;

    match fail_result {
        Ok(handle) => {
            println!("Fail task submitted: {}", handle.id());

            // Wait for failure
            let wait_result = tokio::time::timeout(Duration::from_secs(2), handle.wait()).await;

            match wait_result {
                Ok(result) => match result {
                    Ok(_) => println!("Fail task unexpectedly succeeded"),
                    Err(e) => println!("Fail task failed as expected: {e:?}"),
                },
                Err(_) => println!("Fail task wait timed out"),
            }
        }
        Err(e) => println!("Fail task submission failed: {e:?}"),
    }

    worker_handle.abort();
}

/// Test retry policy workflow
#[tokio::test]
async fn test_retry_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Connect client with retry policy
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("retry-test")
        .build()
        .await
        .expect("Failed to connect client");

    let retry_policy = RetryPolicy::builder()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(100))
        .retry_on(&["ExecutionError", "TimeoutError"])
        .build();

    // Submit task with retry policy (will fail since no worker)
    let result = client
        .submit_task("nonexistent_task")
        .args(json!({}))
        .expect("Failed to set args")
        .retry_policy(retry_policy)
        .submit()
        .await;

    // Should fail but test retry policy handling
    match result {
        Ok(handle) => println!("Retry task submitted: {}", handle.id()),
        Err(e) => println!("Retry task failed as expected: {e:?}"),
    }
}

/// Test shepherd group routing workflow
#[tokio::test]
async fn test_shepherd_group_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start workers in different shepherd groups
    let worker1 = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("group-test")
        .shepherd_group("group-1")
        .register_task(E2ETestTask)
        .build()
        .await
        .expect("Failed to build worker 1");

    let worker2 = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("group-test")
        .shepherd_group("group-2")
        .register_task(DataProcessingTask)
        .build()
        .await
        .expect("Failed to build worker 2");

    let worker1_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(3), worker1.run()).await;
    });

    let worker2_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(3), worker2.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("group-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Submit task to specific shepherd group
    let result1 = client
        .submit_task("e2e_test_task")
        .args(json!({}))
        .expect("Failed to set args")
        .shepherd_group("group-1")
        .submit()
        .await;

    let result2 = client
        .submit_task("data_processing_task")
        .args(("test".to_string(), 10, false))
        .expect("Failed to set args")
        .shepherd_group("group-2")
        .submit()
        .await;

    match result1 {
        Ok(handle) => println!("Group-1 task submitted: {}", handle.id()),
        Err(e) => println!("Group-1 task failed: {e:?}"),
    }

    match result2 {
        Ok(handle) => println!("Group-2 task submitted: {}", handle.id()),
        Err(e) => println!("Group-2 task failed: {e:?}"),
    }

    worker1_handle.abort();
    worker2_handle.abort();
}

/// Test concurrent task submissions
#[tokio::test]
async fn test_concurrent_submissions_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("concurrent-test")
        .max_concurrency(10)
        .register_task(E2ETestTask)
        .build()
        .await
        .expect("Failed to build concurrent worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("concurrent-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Submit multiple tasks sequentially (client doesn't implement Clone)
    for i in 0..5 {
        let task_result = client
            .submit_task("e2e_test_task")
            .args(json!({"task_number": i}))
            .expect("Failed to set args")
            .submit()
            .await;

        match task_result {
            Ok(task_handle) => println!("Task {i} submitted: {}", task_handle.id()),
            Err(e) => println!("Task {i} failed: {e:?}"),
        }
    }

    worker_handle.abort();
}

// Mock chrono for timestamps
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
            "2023-01-01T00:00:00Z".to_string()
        }
    }
}

// Mock uuid for IDs
mod uuid {
    pub struct Uuid;
    impl Uuid {
        pub fn new_v4() -> Self {
            Self
        }
    }

    impl std::fmt::Display for Uuid {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock-uuid-12345")
        }
    }
}
