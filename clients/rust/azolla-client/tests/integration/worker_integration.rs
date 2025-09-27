//! Worker integration tests focused on single-task execution model

use azolla_client::task::{Task, TaskResult};
use azolla_client::{TaskError, TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

use super::test_utils::{TestOrchestrator, TestShepherd};

struct SimpleEchoTask;

impl Task for SimpleEchoTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "simple_echo"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!({"echo": args})) })
    }
}

struct AlwaysFailTask;

impl Task for AlwaysFailTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "always_fail"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Err(TaskError::new("Intentional failure")
                .with_error_type("IntegrationFailure")
                .with_retryable(false))
        })
    }
}

fn build_invocation(task_name: &str, args: serde_json::Value, endpoint: &str) -> WorkerInvocation {
    let args_json = args.to_string();
    WorkerInvocation::from_json(
        &Uuid::new_v4().to_string(),
        task_name,
        &args_json,
        "{}",
        endpoint,
    )
    .expect("valid invocation")
}

#[tokio::test]
async fn worker_executes_task_locally() {
    super::init_test_env();

    let worker = Worker::builder().register_task(SimpleEchoTask).build();
    let invocation = WorkerInvocation {
        task_id: Uuid::new_v4(),
        task_name: "simple_echo".to_string(),
        args: vec![json!("hello")],
        kwargs: json!({}),
        shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
    };

    let execution = worker.execute(&invocation).await;
    assert!(execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Success(value) => {
            assert_eq!(value["echo"], "hello");
        }
        TaskExecutionOutcome::Failed(_) => panic!("expected success"),
    }
}

#[tokio::test]
async fn worker_reports_success_to_shepherd() {
    super::init_test_env();

    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start orchestrator");
    let shepherd = TestShepherd::start(&orchestrator.endpoint())
        .await
        .expect("Failed to start shepherd");
    shepherd
        .wait_for_readiness()
        .await
        .expect("Shepherd failed to become ready");

    let worker = Worker::builder().register_task(SimpleEchoTask).build();
    let invocation = build_invocation(
        "simple_echo",
        json!("integration"),
        shepherd.worker_service_endpoint(),
    );

    let execution = worker
        .run_invocation(invocation)
        .await
        .expect("Worker should report result successfully");

    assert!(execution.is_success());
}

#[tokio::test]
async fn worker_reports_failure_to_shepherd() {
    super::init_test_env();

    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start orchestrator");
    let shepherd = TestShepherd::start(&orchestrator.endpoint())
        .await
        .expect("Failed to start shepherd");
    shepherd
        .wait_for_readiness()
        .await
        .expect("Shepherd failed to become ready");

    let worker = Worker::builder().register_task(AlwaysFailTask).build();
    let invocation = build_invocation("always_fail", json!([]), shepherd.worker_service_endpoint());

    let execution = worker
        .run_invocation(invocation)
        .await
        .expect("Worker should communicate failure");

    assert!(!execution.is_success());
}
