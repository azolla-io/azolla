//! Unit tests for the single-task worker runtime

use azolla_client::task::{Task, TaskResult};
use azolla_client::{TaskError, TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

struct NoopTask;

impl Task for NoopTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "noop_task"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!({ "status": "ok" })) })
    }
}

struct FailureTask;

impl Task for FailureTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "failure_task"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Err(TaskError::execution_failed("unit failure").with_error_type("UnitError"))
        })
    }
}

fn dummy_invocation(task_name: &str) -> WorkerInvocation {
    WorkerInvocation {
        task_id: Uuid::new_v4(),
        task_name: task_name.to_string(),
        args: Vec::new(),
        kwargs: json!({}),
        shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
    }
}

#[tokio::test]
async fn builder_registers_tasks() {
    let worker = Worker::builder()
        .register_task(NoopTask)
        .register_task(FailureTask)
        .build();

    assert_eq!(worker.task_count(), 2);
}

#[tokio::test]
async fn worker_execute_success() {
    let worker = Worker::builder().register_task(NoopTask).build();
    let execution = worker.execute(&dummy_invocation("noop_task")).await;

    assert!(execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Success(value) => {
            assert_eq!(value["status"], "ok");
        }
        TaskExecutionOutcome::Failed(_) => panic!("Expected success"),
    }
}

#[tokio::test]
async fn worker_execute_failure() {
    let worker = Worker::builder().register_task(FailureTask).build();
    let execution = worker.execute(&dummy_invocation("failure_task")).await;

    assert!(!execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Failed(error) => {
            assert_eq!(error.error_type(), "UnitError");
        }
        TaskExecutionOutcome::Success(_) => panic!("Expected failure"),
    }
}

#[test]
fn worker_invocation_parses_json_args() {
    let task_id = Uuid::new_v4();
    let invocation = WorkerInvocation::from_json(
        &task_id.to_string(),
        "json_task",
        "[1, 2, 3]",
        "{}",
        "http://127.0.0.1:50052",
    )
    .expect("invocation should parse");

    assert_eq!(invocation.args.len(), 3);
    assert_eq!(invocation.task_name, "json_task");
}

#[test]
fn worker_invocation_rejects_bad_args() {
    let err = WorkerInvocation::from_json(
        &Uuid::new_v4().to_string(),
        "bad_task",
        "not-json",
        "{}",
        "http://127.0.0.1:50052",
    )
    .expect_err("invalid JSON should error");

    match err {
        azolla_client::error::AzollaError::WorkerError(msg) => {
            assert!(msg.contains("Invalid args JSON"));
        }
        other => panic!("Unexpected error type: {other:?}"),
    }
}
