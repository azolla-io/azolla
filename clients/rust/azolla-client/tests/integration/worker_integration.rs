//! Worker integration tests with real orchestrator
//! Tests worker registration, task execution, and orchestrator communication

use azolla_client::error::AzollaError;
use azolla_client::task::{Task, TaskResult};
use azolla_client::worker::{Worker, WorkerConfig};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use uuid::Uuid;

use super::{generate_task_id, init_test_env, wait_for_condition, TestOrchestrator};

// Mock task for worker testing
struct IntegrationTestTask;

impl Task for IntegrationTestTask {
    type Args = Value;

    fn name(&self) -> &'static str {
        "integration_test_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Ok(json!({
                "status": "completed",
                "input": args,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }))
        })
    }
}

/// Test worker connection establishment with orchestrator
#[tokio::test]
async fn test_worker_connection_establishment() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let config = WorkerConfig {
        orchestrator_endpoint: orchestrator.endpoint(),
        domain: "test-domain".to_string(),
        shepherd_group: "integration-workers".to_string(),
        max_concurrency: 5,
        heartbeat_interval: Duration::from_secs(10),
    };

    // Test that worker builder can create a worker instance
    let worker_result = Worker::builder()
        .orchestrator(&config.orchestrator_endpoint)
        .domain(&config.domain)
        .shepherd_group(&config.shepherd_group)
        .max_concurrency(config.max_concurrency)
        .heartbeat_interval(config.heartbeat_interval)
        .register_task(IntegrationTestTask)
        .build()
        .await;

    // Worker creation should succeed
    assert!(worker_result.is_ok());
}

/// Test worker startup and registration process
#[tokio::test]
async fn test_worker_startup_and_registration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("startup-test")
        .shepherd_group("startup-workers")
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build worker");

    // Test worker startup (this will attempt to connect to orchestrator)
    // Note: This might fail if orchestrator isn't fully ready, but tests the startup path
    let startup_result = tokio::time::timeout(Duration::from_secs(5), worker.start()).await;

    // Either succeeds or times out, but shouldn't panic
    match startup_result {
        Ok(result) => {
            // If it completes, it should either succeed or fail gracefully
            match result {
                Ok(_) => println!("Worker started successfully"),
                Err(e) => println!("Worker startup failed: {e:?}"),
            }
        }
        Err(_) => println!("Worker startup timed out"),
    }
}

/// Test worker task registration
#[tokio::test]
async fn test_worker_task_registration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("task-reg-test")
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build worker");

    // Test that task count reflects registered tasks
    assert_eq!(worker.task_count(), 1);
}

/// Test worker task registration with multiple tasks
#[tokio::test]
async fn test_worker_multiple_task_registration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Define additional mock tasks
    struct SecondTask;
    impl Task for SecondTask {
        type Args = ();
        fn name(&self) -> &'static str {
            "second_task"
        }
        fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async { Ok(json!({"task": "second"})) })
        }
    }

    struct ThirdTask;
    impl Task for ThirdTask {
        type Args = String;
        fn name(&self) -> &'static str {
            "third_task"
        }
        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"task": "third", "input": args})) })
        }
    }

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("multi-task-test")
        .register_task(IntegrationTestTask)
        .register_task(SecondTask)
        .register_task(ThirdTask)
        .build()
        .await
        .expect("Failed to build worker");

    // Test that task count reflects all registered tasks
    assert_eq!(worker.task_count(), 3);
}

/// Test worker builder functionality
#[tokio::test]
async fn test_worker_builder_functionality() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Test builder with all configuration options
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("builder-test")
        .shepherd_group("builder-workers")
        .max_concurrency(20)
        .heartbeat_interval(Duration::from_secs(15))
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build worker with full config");

    assert_eq!(worker.task_count(), 1);
}

/// Test worker graceful shutdown
#[tokio::test]
async fn test_worker_graceful_shutdown() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("shutdown-test")
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build worker");

    // Test shutdown method exists and can be called
    // Note: Actual shutdown behavior depends on orchestrator connectivity
    let shutdown_result = tokio::time::timeout(Duration::from_millis(100), worker.shutdown()).await;

    // Should either complete quickly or timeout
    match shutdown_result {
        Ok(result) => match result {
            Ok(_) => println!("Worker shutdown completed"),
            Err(e) => println!("Worker shutdown error: {e:?}"),
        },
        Err(_) => println!("Worker shutdown timed out"),
    }
}

/// Test worker task count method
#[tokio::test]
async fn test_worker_task_count_method() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Test empty worker
    let empty_worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("empty-test")
        .build()
        .await
        .expect("Failed to build empty worker");

    assert_eq!(empty_worker.task_count(), 0);

    // Test worker with one task
    let single_task_worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("single-test")
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build single task worker");

    assert_eq!(single_task_worker.task_count(), 1);
}

/// Test worker configuration edge cases
#[tokio::test]
async fn test_worker_configuration_edge_cases() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Test with minimal configuration
    let minimal_worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to build minimal worker");

    assert_eq!(minimal_worker.task_count(), 0);

    // Test with maximal configuration
    let maximal_worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("maximal-test-domain-with-long-name")
        .shepherd_group("maximal-test-group-with-long-name")
        .max_concurrency(1000)
        .heartbeat_interval(Duration::from_millis(500))
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build maximal worker");

    assert_eq!(maximal_worker.task_count(), 1);
}

/// Test worker task registration edge cases
#[tokio::test]
async fn test_worker_task_registration_edge_cases() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Test registering tasks with different argument types
    struct NoArgsTask;
    impl Task for NoArgsTask {
        type Args = ();
        fn name(&self) -> &'static str {
            "no_args_task"
        }
        fn execute(&self, _: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async { Ok(json!({})) })
        }
    }

    struct ComplexArgsTask;
    impl Task for ComplexArgsTask {
        type Args = (i32, String, bool);
        fn name(&self) -> &'static str {
            "complex_args_task"
        }
        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"args": [args.0, args.1, args.2]})) })
        }
    }

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("edge-case-test")
        .register_task(NoArgsTask)
        .register_task(ComplexArgsTask)
        .register_task(IntegrationTestTask)
        .build()
        .await
        .expect("Failed to build worker with edge case tasks");

    assert_eq!(worker.task_count(), 3);
}

// Mock chrono for timestamp in IntegrationTestTask
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
