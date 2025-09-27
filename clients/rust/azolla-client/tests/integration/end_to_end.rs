//! Simplified end-to-end style tests for the single-run worker model

use azolla_client::task::{Task, TaskResult};
use azolla_client::{TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

struct JsonEchoTask;

impl Task for JsonEchoTask {
    type Args = serde_json::Value;

    fn name(&self) -> &'static str {
        "json_echo"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!({ "payload": args })) })
    }
}

struct ToggleTask;

impl Task for ToggleTask {
    type Args = bool;

    fn name(&self) -> &'static str {
        "toggle_task"
    }

    fn execute(&self, flag: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!({ "toggled": !flag })) })
    }
}

fn invocation(task: &str, args: serde_json::Value) -> WorkerInvocation {
    WorkerInvocation::from_json(
        &Uuid::new_v4().to_string(),
        task,
        &args.to_string(),
        "{}",
        "http://127.0.0.1:50052",
    )
    .expect("valid invocation")
}

#[tokio::test]
async fn worker_processes_multiple_tasks_sequentially() {
    let worker = Worker::builder()
        .register_task(JsonEchoTask)
        .register_task(ToggleTask)
        .build();

    let echo = worker
        .execute(&invocation("json_echo", json!({"key": "value"})))
        .await;
    assert!(echo.is_success());

    let toggle = worker
        .execute(&invocation("toggle_task", json!([true])))
        .await;
    assert!(toggle.is_success());

    match toggle.outcome {
        TaskExecutionOutcome::Success(value) => {
            assert_eq!(value["toggled"], false);
        }
        TaskExecutionOutcome::Failed(_) => panic!("Expected success"),
    }
}

#[tokio::test]
async fn worker_returns_error_for_unknown_task() {
    let worker = Worker::builder().register_task(JsonEchoTask).build();
    let result = worker.execute(&invocation("unknown_task", json!([]))).await;

    assert!(!result.is_success());
    match result.outcome {
        TaskExecutionOutcome::Failed(error) => {
            assert_eq!(error.error_type(), "TaskNotFound");
        }
        TaskExecutionOutcome::Success(_) => panic!("Expected failure"),
    }
}
