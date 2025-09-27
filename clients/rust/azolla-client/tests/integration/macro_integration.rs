//! Integration tests validating macro-generated tasks under the single-run worker model

use azolla_client::task::{Task, TaskResult};
use azolla_client::{TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

#[cfg(feature = "macros")]
use azolla_client::azolla_task;
#[cfg(feature = "macros")]
use azolla_client::error::TaskError;

#[cfg(feature = "macros")]
#[azolla_task]
async fn macro_add(a: i32, b: i32) -> Result<serde_json::Value, TaskError> {
    Ok(json!({ "sum": a + b }))
}

#[cfg(feature = "macros")]
#[azolla_task]
async fn macro_fail() -> Result<serde_json::Value, TaskError> {
    Err(TaskError::execution_failed("macro failure"))
}

struct ManualConcatenate;

impl Task for ManualConcatenate {
    type Args = (String, String);

    fn name(&self) -> &'static str {
        "manual_concat"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let (left, right) = args;
            Ok(json!({ "combined": format!("{left}-{right}") }))
        })
    }
}

#[cfg(feature = "macros")]
fn empty_invocation(task_name: &str) -> WorkerInvocation {
    WorkerInvocation {
        task_id: Uuid::new_v4(),
        task_name: task_name.to_string(),
        args: Vec::new(),
        kwargs: json!({}),
        shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
    }
}

fn invocation_with_args(task_name: &str, args: serde_json::Value) -> WorkerInvocation {
    WorkerInvocation::from_json(
        &Uuid::new_v4().to_string(),
        task_name,
        &args.to_string(),
        "{}",
        "http://127.0.0.1:50052",
    )
    .expect("valid invocation")
}

#[tokio::test]
async fn manual_task_executes() {
    let worker = Worker::builder().register_task(ManualConcatenate).build();
    let invocation = invocation_with_args("manual_concat", json!(["hello", "world"]));

    let execution = worker.execute(&invocation).await;
    assert!(execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Success(value) => {
            assert_eq!(value["combined"], "hello-world");
        }
        TaskExecutionOutcome::Failed(_) => panic!("Expected success"),
    }
}

#[cfg(feature = "macros")]
#[tokio::test]
async fn macro_task_executes_successfully() {
    let worker = Worker::builder().register_task(MacroAddTask).build();
    let invocation = invocation_with_args("macro_add", json!([2, 3]));

    let execution = worker.execute(&invocation).await;
    assert!(execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Success(value) => {
            assert_eq!(value["sum"], 5);
        }
        TaskExecutionOutcome::Failed(_) => panic!("Expected macro success"),
    }
}

#[cfg(feature = "macros")]
#[tokio::test]
async fn macro_task_reports_failure() {
    let worker = Worker::builder().register_task(MacroFailTask).build();
    let invocation = empty_invocation("macro_fail");

    let execution = worker.execute(&invocation).await;
    assert!(!execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Failed(error) => {
            assert_eq!(error.message, "macro failure");
        }
        TaskExecutionOutcome::Success(_) => panic!("Expected macro failure"),
    }
}
