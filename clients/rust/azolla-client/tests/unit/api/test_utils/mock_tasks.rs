use azolla_client::task::{Task, TaskResult};
use serde::Deserialize;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::sleep;

/// Simple task that returns success immediately
#[derive(Clone)]
pub struct InstantSuccessTask;

impl Task for InstantSuccessTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "instant_success"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Ok(json!({
                "status": "success",
                "message": "Task completed instantly"
            }))
        })
    }
}

/// Task that fails immediately with a specific error
#[derive(Clone)]
pub struct InstantFailureTask;

impl Task for InstantFailureTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "instant_failure"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Err(azolla_client::error::TaskError::execution_failed("Task failed intentionally"))
        })
    }
}

/// Task that takes arguments and echoes them back
#[derive(Clone)]
pub struct EchoTask;

#[derive(Deserialize)]
pub struct EchoArgs {
    pub message: String,
    pub number: i32,
}

impl Task for EchoTask {
    type Args = EchoArgs;

    fn name(&self) -> &'static str {
        "echo_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Ok(json!({
                "echo_message": args.message,
                "echo_number": args.number,
                "task": "echo_task"
            }))
        })
    }
}

/// Task that sleeps for a specified duration
#[derive(Clone)]
pub struct SleepTask;

#[derive(Deserialize)]
pub struct SleepArgs {
    pub duration_ms: u64,
}

impl Task for SleepTask {
    type Args = SleepArgs;

    fn name(&self) -> &'static str {
        "sleep_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            sleep(Duration::from_millis(args.duration_ms)).await;
            Ok(json!({
                "slept_ms": args.duration_ms,
                "status": "awakened"
            }))
        })
    }
}

/// Task that conditionally succeeds or fails based on input
#[derive(Clone)]
pub struct ConditionalTask;

#[derive(Deserialize)]
pub struct ConditionalArgs {
    pub should_succeed: bool,
    pub error_message: Option<String>,
}

impl Task for ConditionalTask {
    type Args = ConditionalArgs;

    fn name(&self) -> &'static str {
        "conditional_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            if args.should_succeed {
                Ok(json!({
                    "status": "success",
                    "condition_met": true
                }))
            } else {
                let error_msg = args.error_message.unwrap_or_else(|| "Condition not met".to_string());
                Err(azolla_client::error::TaskError::execution_failed(&error_msg))
            }
        })
    }
}

/// Task that panics to test error handling
#[derive(Clone)]
pub struct PanicTask;

impl Task for PanicTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "panic_task"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            panic!("This task panics intentionally");
        })
    }
}

/// Create a registry of all mock tasks
pub fn create_mock_task_registry() -> Vec<Box<dyn azolla_client::task::BoxedTask>> {
    vec![
        Box::new(InstantSuccessTask),
        Box::new(InstantFailureTask),
        Box::new(EchoTask),
        Box::new(SleepTask),
        Box::new(ConditionalTask),
        Box::new(PanicTask),
    ]
}