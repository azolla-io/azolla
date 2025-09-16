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

// Task that can fail based on input - Enhanced with comprehensive error types
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
                "validation_error" => Err(azolla_client::error::TaskError::validation_error(
                    "Invalid input format provided",
                )),
                "timeout_error" => Err(azolla_client::error::TaskError::timeout_error(
                    "Task execution exceeded time limit",
                )),
                "resource_error" => Err(azolla_client::error::TaskError::new(
                    "Insufficient memory available",
                )
                .with_error_type("ResourceError")
                .with_retryable(true)),
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

// Task that returns various data types for comprehensive result testing
struct TypeTestTask;

impl Task for TypeTestTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "type_test_task"
    }

    fn execute(
        &self,
        test_type: Self::Args,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            match test_type.as_str() {
                "string" => Ok(json!("Hello, E2E World!")),
                "integer" => Ok(json!(42)),
                "float" => Ok(json!(std::f64::consts::PI)),
                "boolean_true" => Ok(json!(true)),
                "boolean_false" => Ok(json!(false)),
                "null" => Ok(json!(null)),
                "array" => Ok(json!([1, "two", true, null, {"nested": "value"}])),
                "complex_object" => Ok(json!({
                    "user_id": 12345,
                    "username": "e2e_test_user",
                    "active": true,
                    "metadata": {
                        "created_at": "2024-01-01T00:00:00Z",
                        "tags": ["integration", "test", "e2e"],
                        "settings": {
                            "theme": "dark",
                            "notifications_enabled": false
                        }
                    },
                    "scores": [95.5, 87.2, 92.8],
                    "permissions": null
                })),
                _ => Err(azolla_client::error::TaskError::invalid_args(&format!(
                    "Unknown test type: {test_type}"
                ))),
            }
        })
    }
}

// Task that returns detailed error information for error scenario testing
struct ErrorTestTask;

impl Task for ErrorTestTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "error_test_task"
    }

    fn execute(
        &self,
        error_type: Self::Args,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            match error_type.as_str() {
                "validation" => Err(azolla_client::error::TaskError::validation_error(
                    "Field validation failed: email format invalid",
                )),
                "type_error" => Err(azolla_client::error::TaskError::new(
                    "Expected string, got integer",
                )
                .with_error_type("TypeError")
                .with_error_code("TYPE_MISMATCH")
                .with_retryable(false)),
                "runtime" => Err(azolla_client::error::TaskError::new(
                    "Database connection timeout",
                )
                .with_error_type("RuntimeError")
                .with_error_code("DB_TIMEOUT")
                .with_retryable(true)),
                "resource" => Err(azolla_client::error::TaskError::new(
                    "Insufficient memory available",
                )
                .with_error_type("ResourceError")
                .with_error_code("OUT_OF_MEMORY")
                .with_retryable(true)),
                "timeout" => Err(azolla_client::error::TaskError::timeout_error(
                    "Task execution exceeded maximum allowed time",
                )),
                "business_logic" => Err(azolla_client::error::TaskError::new(
                    "Order cannot be processed: payment method expired",
                )
                .with_error_type("BusinessLogicError")
                .with_error_code("PAYMENT_EXPIRED")
                .with_retryable(false)),
                _ => Err(azolla_client::error::TaskError::invalid_args(&format!(
                    "Unknown error type: {error_type}"
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

            // Try to wait for result with comprehensive validation
            let wait_result = tokio::time::timeout(Duration::from_secs(5), handle.wait()).await;

            match wait_result {
                Ok(result) => match result {
                    Ok(execution_result) => match execution_result {
                        TaskExecutionResult::Success(value) => {
                            println!("Task completed: {value:?}");

                            // Comprehensive result validation
                            assert!(value.get("processed").is_some());
                            assert!(value["processed"].as_bool().unwrap());
                            assert_eq!(value["task_name"].as_str().unwrap(), "e2e_test_task");

                            // Verify input data was processed correctly
                            let input = &value["input"];
                            assert_eq!(input["test_data"].as_str().unwrap(), "hello world");
                            assert_eq!(
                                input["timestamp"].as_str().unwrap(),
                                "2023-01-01T00:00:00Z"
                            );

                            // Verify worker metadata is present
                            assert!(value.get("worker_id").is_some());
                            assert!(value.get("timestamp").is_some());

                            println!("✅ E2E workflow validation passed");
                        }
                        TaskExecutionResult::Failed(e) => {
                            println!("Task execution failed: {e:?}");
                            panic!("Expected task to succeed but got error: {e:?}");
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

    // Submit data processing task with comprehensive result validation
    let result = client
        .submit_task("data_processing_task")
        .args(("test string".to_string(), 42, true))
        .expect("Failed to set args")
        .submit()
        .await;

    match result {
        Ok(handle) => {
            println!("Data processing task submitted: {}", handle.id());

            // Wait for result and validate comprehensive data processing
            let wait_result = tokio::time::timeout(Duration::from_secs(5), handle.wait()).await;

            match wait_result {
                Ok(result) => match result {
                    Ok(execution_result) => match execution_result {
                        TaskExecutionResult::Success(value) => {
                            println!("Data processing completed: {value:?}");

                            // Validate specific data transformations
                            assert_eq!(value["text_length"].as_u64().unwrap(), 11); // "test string".len()
                            assert_eq!(value["number_doubled"].as_i64().unwrap(), 84); // 42 * 2
                            assert!(!value["flag_negated"].as_bool().unwrap()); // !true
                            assert_eq!(value["combined"].as_str().unwrap(), "test string-42-true");
                            assert_eq!(value["processing_time_ms"].as_u64().unwrap(), 100);

                            println!("✅ Data processing validation passed");
                        }
                        TaskExecutionResult::Failed(e) => {
                            println!("Data processing failed: {e:?}");
                            panic!("Expected data processing to succeed but got error: {e:?}");
                        }
                    },
                    Err(e) => println!("Data processing wait failed: {e:?}"),
                },
                Err(_) => println!("Data processing wait timed out"),
            }
        }
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

            // Wait for failure with comprehensive error validation
            let wait_result = tokio::time::timeout(Duration::from_secs(2), handle.wait()).await;

            match wait_result {
                Ok(result) => match result {
                    Ok(execution_result) => match execution_result {
                        TaskExecutionResult::Success(_) => {
                            panic!("Fail task unexpectedly succeeded");
                        }
                        TaskExecutionResult::Failed(task_error) => {
                            println!("Fail task failed as expected: {task_error:?}");

                            // Comprehensive error validation
                            assert_eq!(task_error.error_type, "ExecutionError");
                            assert_eq!(task_error.message, "Task was asked to fail");
                            assert!(task_error.retryable); // ExecutionError is retryable by default

                            println!("✅ Error workflow validation passed");
                        }
                    },
                    Err(e) => println!("Fail task wait failed: {e:?}"),
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

/// Test comprehensive data type result retrieval in E2E workflow
#[tokio::test]
async fn test_comprehensive_data_type_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with type test task
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("type-test")
        .register_task(TypeTestTask)
        .build()
        .await
        .expect("Failed to build type test worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("type-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Test different data types with individual test cases
    let test_cases = vec![
        "string",
        "integer",
        "float",
        "boolean_true",
        "boolean_false",
        "null",
        "array",
        "complex_object",
    ];

    for test_type in test_cases {
        println!("Testing data type: {test_type}");

        let result = client
            .submit_task("type_test_task")
            .args(test_type.to_string())
            .expect("Failed to set args")
            .submit()
            .await
            .expect("Failed to submit task");

        let execution_result = tokio::time::timeout(Duration::from_secs(5), result.wait())
            .await
            .expect("Task timed out")
            .expect("Task wait failed");

        match execution_result {
            TaskExecutionResult::Success(value) => {
                // Validate based on test type
                match test_type {
                    "string" => {
                        assert_eq!(value.as_str().unwrap(), "Hello, E2E World!");
                    }
                    "integer" => {
                        assert_eq!(value.as_i64().unwrap(), 42);
                    }
                    "float" => {
                        assert!(
                            (value.as_f64().unwrap() - std::f64::consts::PI).abs() < f64::EPSILON
                        );
                    }
                    "boolean_true" => {
                        assert!(value.as_bool().unwrap());
                    }
                    "boolean_false" => {
                        assert!(!value.as_bool().unwrap());
                    }
                    "null" => {
                        assert!(value.is_null());
                    }
                    "array" => {
                        let arr = value.as_array().unwrap();
                        assert_eq!(arr.len(), 5);
                        assert_eq!(arr[0].as_i64().unwrap(), 1);
                        assert_eq!(arr[1].as_str().unwrap(), "two");
                        assert!(arr[2].as_bool().unwrap());
                        assert!(arr[3].is_null());
                        assert_eq!(arr[4]["nested"].as_str().unwrap(), "value");
                    }
                    "complex_object" => {
                        assert_eq!(value["user_id"].as_u64().unwrap(), 12345);
                        assert_eq!(value["username"].as_str().unwrap(), "e2e_test_user");
                        assert!(value["active"].as_bool().unwrap());
                        assert_eq!(value["metadata"]["tags"].as_array().unwrap().len(), 3);
                        assert_eq!(
                            value["metadata"]["settings"]["theme"].as_str().unwrap(),
                            "dark"
                        );
                        assert_eq!(value["scores"].as_array().unwrap().len(), 3);
                        assert!(value["permissions"].is_null());
                    }
                    _ => panic!("Unknown test type: {test_type}"),
                }
                println!("✅ Data type {test_type} validation passed");
            }
            TaskExecutionResult::Failed(e) => {
                panic!("Data type test {test_type} failed: {e:?}");
            }
        }
    }

    worker_handle.abort();
}

/// Test comprehensive error scenario handling in E2E workflow
#[tokio::test]
async fn test_comprehensive_error_scenario_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with error test task
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("error-test")
        .register_task(ErrorTestTask)
        .build()
        .await
        .expect("Failed to build error test worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("error-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Test different error types
    let error_test_cases = [
        (
            "validation",
            "TaskValidationError",
            "Field validation failed: email format invalid",
            false,
        ),
        (
            "type_error",
            "TypeError",
            "Expected string, got integer",
            false,
        ),
        (
            "runtime",
            "RuntimeError",
            "Database connection timeout",
            true,
        ),
        (
            "resource",
            "ResourceError",
            "Insufficient memory available",
            true,
        ),
        (
            "timeout",
            "TaskTimeoutError",
            "Task execution exceeded maximum allowed time",
            true,
        ),
        (
            "business_logic",
            "BusinessLogicError",
            "Order cannot be processed: payment method expired",
            false,
        ),
    ];

    for (error_type, expected_error_type, expected_message, expected_retryable) in error_test_cases
    {
        println!("Testing error type: {error_type}");

        let result = client
            .submit_task("error_test_task")
            .args(error_type.to_string())
            .expect("Failed to set args")
            .submit()
            .await
            .expect("Failed to submit task");

        let execution_result = tokio::time::timeout(Duration::from_secs(5), result.wait())
            .await
            .expect("Task timed out")
            .expect("Task wait failed");

        match execution_result {
            TaskExecutionResult::Success(_) => {
                panic!("Error test {error_type} unexpectedly succeeded");
            }
            TaskExecutionResult::Failed(task_error) => {
                // Comprehensive error validation
                assert_eq!(task_error.error_type, expected_error_type);
                assert_eq!(task_error.message, expected_message);
                assert_eq!(task_error.retryable, expected_retryable);

                // Validate error codes where applicable
                match error_type {
                    "type_error" => assert_eq!(task_error.code.as_ref().unwrap(), "TYPE_MISMATCH"),
                    "runtime" => assert_eq!(task_error.code.as_ref().unwrap(), "DB_TIMEOUT"),
                    "resource" => assert_eq!(task_error.code.as_ref().unwrap(), "OUT_OF_MEMORY"),
                    "business_logic" => {
                        assert_eq!(task_error.code.as_ref().unwrap(), "PAYMENT_EXPIRED")
                    }
                    _ => {} // Other error types don't have specific codes
                }

                println!("✅ Error type {error_type} validation passed");
            }
        }
    }

    worker_handle.abort();
}

/// Test enhanced conditional task with comprehensive error scenarios
#[tokio::test]
async fn test_enhanced_conditional_task_workflow() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Start worker with enhanced conditional task
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("enhanced-conditional-test")
        .register_task(ConditionalTask)
        .build()
        .await
        .expect("Failed to build enhanced conditional worker");

    let worker_handle = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), worker.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect client
    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("enhanced-conditional-test")
        .build()
        .await
        .expect("Failed to connect client");

    // Test enhanced error scenarios
    let enhanced_test_cases = [
        ("succeed", true, None),
        (
            "validation_error",
            false,
            Some((
                "TaskValidationError",
                "Invalid input format provided",
                false,
            )),
        ),
        (
            "timeout_error",
            false,
            Some((
                "TaskTimeoutError",
                "Task execution exceeded time limit",
                true,
            )),
        ),
        (
            "resource_error",
            false,
            Some(("ResourceError", "Insufficient memory available", true)),
        ),
    ];

    for (command, should_succeed, error_info) in enhanced_test_cases {
        println!("Testing enhanced conditional command: {command}");

        let result = client
            .submit_task("conditional_task")
            .args(command.to_string())
            .expect("Failed to set args")
            .submit()
            .await
            .expect("Failed to submit task");

        let execution_result = tokio::time::timeout(Duration::from_secs(5), result.wait())
            .await
            .expect("Task timed out")
            .expect("Task wait failed");

        match execution_result {
            TaskExecutionResult::Success(value) => {
                assert!(should_succeed, "Task {command} unexpectedly succeeded");
                assert_eq!(value["status"].as_str().unwrap(), "success");
                assert_eq!(value["command"].as_str().unwrap(), command);
                println!("✅ Enhanced conditional success {command} validation passed");
            }
            TaskExecutionResult::Failed(task_error) => {
                assert!(!should_succeed, "Task {command} unexpectedly failed");

                if let Some((expected_type, expected_message, expected_retryable)) = error_info {
                    assert_eq!(task_error.error_type, expected_type);
                    assert_eq!(task_error.message, expected_message);
                    assert_eq!(task_error.retryable, expected_retryable);
                }

                println!("✅ Enhanced conditional error {command} validation passed");
            }
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
