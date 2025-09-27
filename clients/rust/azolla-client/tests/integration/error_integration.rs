//! Integration tests covering worker failure propagation and TaskError metadata

use azolla_client::proto::common::task_result::ResultType;
use azolla_client::task::{Task, TaskResult};
use azolla_client::{TaskError, TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

struct DetailedErrorTask;

impl Task for DetailedErrorTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "detailed_error"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Err(TaskError::new("Database connection timeout")
                .with_error_type("DatabaseError")
                .with_error_code("DB_TIMEOUT")
                .with_retryable(true))
        })
    }
}

struct DataPayloadTask;

impl Task for DataPayloadTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "data_payload"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Err(TaskError {
                error_type: "ValidationError".to_string(),
                message: "Invalid payload".to_string(),
                code: Some("INVALID".to_string()),
                data: Some(json!({ "field": "email" })),
                retryable: false,
            })
        })
    }
}

fn worker_invocation(task_name: &str) -> WorkerInvocation {
    WorkerInvocation {
        task_id: Uuid::new_v4(),
        task_name: task_name.to_string(),
        args: Vec::new(),
        kwargs: json!({}),
        shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
    }
}

#[tokio::test]
async fn error_metadata_is_retained() {
    let worker = Worker::builder().register_task(DetailedErrorTask).build();
    let execution = worker.execute(&worker_invocation("detailed_error")).await;

    assert!(!execution.is_success());
    match execution.outcome {
        TaskExecutionOutcome::Failed(error) => {
            assert_eq!(error.error_type(), "DatabaseError");
            assert_eq!(error.error_code(), Some("DB_TIMEOUT"));
            assert!(error.retryable);
        }
        TaskExecutionOutcome::Success(_) => panic!("Expected failure"),
    }

    match execution.task_result.result_type {
        Some(ResultType::Error(err)) => {
            assert_eq!(err.r#type, "DatabaseError");
            assert_eq!(err.message, "Database connection timeout");
            assert!(err.retriable);
        }
        _ => panic!("Expected error result type"),
    }
}

#[tokio::test]
async fn error_payload_is_serialized() {
    let worker = Worker::builder().register_task(DataPayloadTask).build();
    let execution = worker.execute(&worker_invocation("data_payload")).await;

    assert!(!execution.is_success());

    match execution.task_result.result_type {
        Some(ResultType::Error(err)) => {
            assert_eq!(err.r#type, "ValidationError");
            assert!(!err.retriable);
            assert!(err.data.contains("email"));
        }
        _ => panic!("Expected error result"),
    }
}
