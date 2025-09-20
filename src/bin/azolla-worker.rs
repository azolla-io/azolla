use anyhow::Result;
use clap::{Arg, Command};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

use azolla_client::{
    task::{Task, TaskResult},
    TaskError, TaskExecutionOutcome, Worker, WorkerInvocation,
};

/// Echo task - returns the first argument
async fn echo_task(message: String) -> Result<Value, TaskError> {
    Ok(json!(message))
}

pub struct EchoTaskTask;

impl Task for EchoTaskTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "echo"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let message = args;
            let result = echo_task(message).await;
            match result {
                Ok(value) => {
                    let json_value = serde_json::to_value(value).map_err(|e| {
                        TaskError::execution_failed(&format!("Failed to serialize result: {e}"))
                    })?;
                    Ok(json_value)
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Always fail task - always fails for testing
async fn always_fail_task(should_fail: Option<bool>) -> Result<Value, TaskError> {
    let should_fail = should_fail.unwrap_or(true);

    if should_fail {
        Err(TaskError::new("This task always fails").with_error_type("TestError"))
    } else {
        Ok(json!({"status": "unexpectedly_succeeded"}))
    }
}

pub struct AlwaysFailTaskTask;

impl Task for AlwaysFailTaskTask {
    type Args = Option<bool>;

    fn name(&self) -> &'static str {
        "always_fail"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let should_fail = args;
            let result = always_fail_task(should_fail).await;
            match result {
                Ok(value) => {
                    let json_value = serde_json::to_value(value).map_err(|e| {
                        TaskError::execution_failed(&format!("Failed to serialize result: {e}"))
                    })?;
                    Ok(json_value)
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Flaky task - fails on first attempt, succeeds on retry
async fn flaky_task(fail_first_attempt: Option<bool>) -> Result<Value, TaskError> {
    let fail_first_attempt = fail_first_attempt.unwrap_or(false);

    if fail_first_attempt {
        let state_file =
            std::env::temp_dir().join(format!("flaky_task_state_{}", std::process::id()));

        let attempt_count = match std::fs::read_to_string(&state_file) {
            Ok(content) => content.trim().parse::<u32>().unwrap_or(0),
            Err(_) => 0,
        };

        let new_attempt_count = attempt_count + 1;
        let _ = std::fs::write(&state_file, new_attempt_count.to_string());

        info!("Flaky task attempt #{new_attempt_count}");

        if new_attempt_count == 1 {
            return Err(TaskError::new("First attempt failure")
                .with_error_type("TestError")
                .with_error_code("FLAKY_TASK_FIRST_ATTEMPT"));
        }
    }

    Ok(json!("Flaky task succeeded on retry"))
}

pub struct FlakyTaskTask;

impl Task for FlakyTaskTask {
    type Args = Option<bool>;

    fn name(&self) -> &'static str {
        "flaky_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let fail_first_attempt = args;
            let result = flaky_task(fail_first_attempt).await;
            match result {
                Ok(value) => {
                    let json_value = serde_json::to_value(value).map_err(|e| {
                        TaskError::execution_failed(&format!("Failed to serialize result: {e}"))
                    })?;
                    Ok(json_value)
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Arguments for math add task
#[derive(Debug, Deserialize, Serialize)]
struct MathAddArgs {
    pub a: f64,
    pub b: f64,
}

/// Math add task - adds two numbers with type safety
#[derive(Debug)]
struct MathAddTask;

impl Task for MathAddTask {
    type Args = MathAddArgs;

    fn name(&self) -> &'static str {
        "math_add"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!(args.a + args.b)) })
    }

    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        if json_args.len() > 1 {
            serde_json::from_value(json_args[1].clone())
                .map_err(|e| TaskError::invalid_args(&format!("Invalid math_add kwargs: {e}")))
        } else {
            serde_json::from_value(json_args[0].clone())
                .map_err(|e| TaskError::invalid_args(&format!("Invalid math_add args: {e}")))
        }
    }
}

/// Count args task - counts the number of arguments
#[derive(Debug)]
struct CountArgsTask;

impl Task for CountArgsTask {
    type Args = Vec<Value>;

    fn name(&self) -> &'static str {
        "count_args"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!(args.len() as i64)) })
    }

    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        Ok(json_args)
    }
}

fn build_worker() -> Worker {
    Worker::builder()
        .register_task(EchoTaskTask)
        .register_task(AlwaysFailTaskTask)
        .register_task(FlakyTaskTask)
        .register_task(MathAddTask)
        .register_task(CountArgsTask)
        .build()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = Command::new("azolla-worker")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Azolla Worker - Executes a single task invocation")
        .arg(
            Arg::new("task-id")
                .long("task-id")
                .value_name("UUID")
                .help("Task ID to execute")
                .required(true),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Task name")
                .required(true),
        )
        .arg(
            Arg::new("args")
                .long("args")
                .value_name("JSON")
                .help("Task arguments as JSON (array or single value)")
                .default_value("[]"),
        )
        .arg(
            Arg::new("kwargs")
                .long("kwargs")
                .value_name("JSON")
                .help("Task keyword arguments as JSON object")
                .default_value("{}"),
        )
        .arg(
            Arg::new("shepherd-endpoint")
                .long("shepherd-endpoint")
                .value_name("ENDPOINT")
                .help("Shepherd gRPC endpoint for result reporting")
                .required(true),
        )
        .get_matches();

    let task_id = matches.get_one::<String>("task-id").unwrap();
    let task_name = matches.get_one::<String>("name").unwrap();
    let args_json = matches.get_one::<String>("args").unwrap();
    let kwargs_json = matches.get_one::<String>("kwargs").unwrap();
    let shepherd_endpoint = matches.get_one::<String>("shepherd-endpoint").unwrap();

    let invocation = WorkerInvocation::from_json(
        task_id,
        task_name,
        args_json,
        kwargs_json,
        shepherd_endpoint,
    )
    .map_err(anyhow::Error::from)?;

    info!(
        "Starting worker for task {} ({})",
        task_name, invocation.task_id
    );

    let worker = build_worker();
    info!("Registered {} task implementations", worker.task_count());

    let execution = worker
        .run_invocation(invocation)
        .await
        .map_err(anyhow::Error::from)?;

    match &execution.outcome {
        TaskExecutionOutcome::Success(value) => {
            info!("Task completed successfully: {value}");
        }
        TaskExecutionOutcome::Failed(error) => {
            warn!(
                "Task failed: type={} message={} retryable={}",
                error.error_type(),
                error.message,
                error.retryable
            );
        }
    }

    info!("Worker finished processing task");
    Ok(())
}
