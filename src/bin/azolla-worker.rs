use anyhow::Result;
use clap::{Arg, Command};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

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

    fn parse_args(args: Vec<Value>, kwargs: Value) -> Result<Self::Args, TaskError> {
        if let Value::Object(map) = &kwargs {
            if !map.is_empty() {
                return serde_json::from_value(Value::Object(map.clone())).map_err(|e| {
                    TaskError::invalid_args(&format!("Invalid math_add kwargs: {e}"))
                });
            }
        } else if !matches!(kwargs, Value::Null) {
            return serde_json::from_value(kwargs.clone()).map_err(|e| {
                TaskError::invalid_args(&format!("Invalid math_add argument payload: {e}"))
            });
        }

        match args.len() {
            2 => {
                let a = args[0].as_f64().ok_or_else(|| {
                    TaskError::invalid_args("math_add expects numeric positional arguments")
                })?;
                let b = args[1].as_f64().ok_or_else(|| {
                    TaskError::invalid_args("math_add expects numeric positional arguments")
                })?;
                Ok(MathAddArgs { a, b })
            }
            1 => match &args[0] {
                Value::Object(_) => serde_json::from_value(args[0].clone())
                    .map_err(|e| TaskError::invalid_args(&format!("Invalid math_add args: {e}"))),
                Value::Array(items) if items.len() == 2 => {
                    let a = items[0].as_f64().ok_or_else(|| {
                        TaskError::invalid_args("math_add expects numeric positional arguments")
                    })?;
                    let b = items[1].as_f64().ok_or_else(|| {
                        TaskError::invalid_args("math_add expects numeric positional arguments")
                    })?;
                    Ok(MathAddArgs { a, b })
                }
                _ => Err(TaskError::invalid_args(
                    "math_add expects two numeric arguments or kwargs",
                )),
            },
            0 => Err(TaskError::invalid_args(
                "math_add requires positional arguments or non-empty kwargs",
            )),
            _ => Err(TaskError::invalid_args(
                "math_add expects exactly two positional arguments",
            )),
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

    fn parse_args(json_args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
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
        .register_task(BoolTask)
        .register_task(TrivialTask)
        .register_task(NullTask)
        .register_task(ObjectTask)
        .register_task(UintTask)
        .register_task(SlowTaskTask)
        .build()
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SlowTaskArgs {
    #[serde(default = "default_execution_time")]
    execution_time: f64,
}

impl Default for SlowTaskArgs {
    fn default() -> Self {
        Self {
            execution_time: default_execution_time(),
        }
    }
}

fn default_execution_time() -> f64 {
    1.0
}

pub struct SlowTaskTask;

impl Task for SlowTaskTask {
    type Args = SlowTaskArgs;

    fn name(&self) -> &'static str {
        "slow_task"
    }

    fn parse_args(args: Vec<Value>, kwargs: Value) -> Result<Self::Args, TaskError> {
        let parse_error = |msg: &str, err: &dyn std::fmt::Display| {
            TaskError::invalid_args(&format!("{msg}: {err}"))
        };

        let mut parsed = if let Value::Object(map) = &kwargs {
            if map.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_value(kwargs.clone())
                        .map_err(|e| parse_error("Invalid slow_task kwargs", &e))?,
                )
            }
        } else if !matches!(kwargs, Value::Null) {
            Some(
                serde_json::from_value(kwargs.clone())
                    .map_err(|e| parse_error("Invalid slow_task kwargs", &e))?,
            )
        } else {
            None
        };

        if parsed.is_none() {
            parsed = match args.len() {
                0 => Some(SlowTaskArgs::default()),
                1 => match &args[0] {
                    Value::Number(number) => {
                        let execution_time = number.as_f64().ok_or_else(|| {
                            TaskError::invalid_args("slow_task expects numeric execution_time")
                        })?;
                        Some(SlowTaskArgs { execution_time })
                    }
                    Value::Object(_) => Some(
                        serde_json::from_value(args[0].clone())
                            .map_err(|e| parse_error("Invalid slow_task argument", &e))?,
                    ),
                    _ => {
                        return Err(TaskError::invalid_args(
                            "slow_task expects execution_time as number or object",
                        ));
                    }
                },
                _ => {
                    return Err(TaskError::invalid_args(
                        "slow_task accepts at most one positional argument",
                    ));
                }
            };
        }

        let parsed = parsed.unwrap();
        if parsed.execution_time.is_sign_negative() {
            return Err(TaskError::invalid_args(
                "slow_task execution_time must be non-negative",
            ));
        }

        Ok(parsed)
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let execution_time = args.execution_time;
            if execution_time > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(execution_time)).await;
            }

            Ok(json!(format!(
                "slow_task completed in {execution_time:.2} seconds"
            )))
        })
    }
}

pub struct BoolTask;

impl Task for BoolTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "bool_task"
    }

    fn parse_args(args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
        if args.is_empty() {
            Ok(())
        } else {
            Err(TaskError::invalid_args(
                "bool_task does not accept arguments",
            ))
        }
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!(true)) })
    }
}

pub struct TrivialTask;

impl Task for TrivialTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "trivial_task"
    }

    fn parse_args(args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
        if args.is_empty() {
            Ok(())
        } else {
            Err(TaskError::invalid_args(
                "trivial_task does not accept positional arguments",
            ))
        }
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!(true)) })
    }
}

pub struct NullTask;

impl Task for NullTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "null_task"
    }

    fn parse_args(args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
        if args.is_empty() {
            Ok(())
        } else {
            Err(TaskError::invalid_args(
                "null_task does not accept arguments",
            ))
        }
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(Value::Null) })
    }
}

pub struct ObjectTask;

impl Task for ObjectTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "object_task"
    }

    fn parse_args(args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
        if args.is_empty() {
            Ok(())
        } else {
            Err(TaskError::invalid_args(
                "object_task does not accept arguments",
            ))
        }
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            Ok(json!({
                "message": "object task payload",
                "nested": {"flag": true, "count": 2u64},
            }))
        })
    }
}

pub struct UintTask;

impl Task for UintTask {
    type Args = u64;

    fn name(&self) -> &'static str {
        "uint_task"
    }

    fn parse_args(mut args: Vec<Value>, kwargs: Value) -> Result<Self::Args, TaskError> {
        if let Value::Object(map) = kwargs {
            if !map.is_empty() {
                return Err(TaskError::invalid_args(
                    "uint_task expects positional argument only",
                ));
            }
        } else if !matches!(kwargs, Value::Null) {
            return Err(TaskError::invalid_args(
                "uint_task expects positional argument only",
            ));
        }

        if args.len() != 1 {
            return Err(TaskError::invalid_args(
                "uint_task requires exactly one argument",
            ));
        }

        let value = args
            .pop()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| TaskError::invalid_args("uint_task requires a u64 argument"))?;

        Ok(value)
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!(args)) })
    }
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
