use anyhow::Result;
use clap::{Arg, Command};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

use azolla::proto::{common, shepherd};
use shepherd::worker_client::WorkerClient;
use shepherd::*;

// Import client library for new worker mode
use azolla_client::{Task, TaskError, TaskResult, Worker};

// Manual task implementations using the type-safe Task trait

/// Echo task - returns the first argument
async fn echo_task(message: String) -> Result<Value, TaskError> {
    Ok(json!(message))
}

pub struct EchoTaskTask;

impl Task for EchoTaskTask {
    type Args = String;

    fn name(&self) -> &'static str {
        "echo_task"
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
        "always_fail_task"
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
        // Use process ID to track attempts (simplified for this implementation)
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

// Additional type-safe task implementations with custom argument types

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
        Box::pin(async move {
            // Direct type-safe access - no more unwrapping!
            Ok(json!(args.a + args.b))
        })
    }

    // Custom parsing for the legacy kwargs format
    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        if json_args.len() > 1 {
            // Legacy format: args[1] contains kwargs object
            serde_json::from_value(json_args[1].clone())
                .map_err(|e| TaskError::invalid_args(&format!("Invalid math_add kwargs: {e}")))
        } else {
            // Direct format: single MathAddArgs object
            serde_json::from_value(json_args[0].clone())
                .map_err(|e| TaskError::invalid_args(&format!("Invalid math_add args: {e}")))
        }
    }
}

/// Count args task - counts the number of arguments (takes any array)
#[derive(Debug)]
struct CountArgsTask;

impl Task for CountArgsTask {
    type Args = Vec<Value>; // Accepts any array of JSON values

    fn name(&self) -> &'static str {
        "count_args"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            // Direct type-safe access to the array
            Ok(json!(args.len() as i64))
        })
    }

    // Custom parsing to handle the legacy format
    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        // For count_args, we want to count the actual arguments passed
        Ok(json_args)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = Command::new("azolla-worker")
        .version("0.1.0")
        .about("Azolla Worker - Executes individual tasks or runs as a service")
        .arg(
            Arg::new("mode")
                .long("mode")
                .value_name("MODE")
                .help("Worker mode: 'task' (default) for single task execution, 'service' for continuous worker service")
                .default_value("task")
                .value_parser(["task", "service"]),
        )
        .arg(
            Arg::new("task-id")
                .long("task-id")
                .value_name("UUID")
                .help("Task ID to execute (required in task mode)")
                .required_if_eq("mode", "task"),
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Task name (required in task mode)")
                .required_if_eq("mode", "task"),
        )
        .arg(
            Arg::new("args")
                .long("args")
                .value_name("JSON")
                .help("Task arguments as JSON array")
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
                .help("Shepherd gRPC endpoint for result reporting (required in task mode)")
                .required_if_eq("mode", "task"),
        )
        .arg(
            Arg::new("orchestrator-endpoint")
                .long("orchestrator-endpoint")
                .value_name("ENDPOINT")
                .help("Orchestrator gRPC endpoint (for service mode)")
                .default_value("localhost:52710"),
        )
        .arg(
            Arg::new("domain")
                .long("domain")
                .value_name("DOMAIN")
                .help("Worker domain (for service mode)")
                .default_value("default"),
        )
        .arg(
            Arg::new("shepherd-group")
                .long("shepherd-group")
                .value_name("GROUP")
                .help("Shepherd group (for service mode)")
                .default_value("default"),
        )
        .arg(
            Arg::new("max-concurrency")
                .long("max-concurrency")
                .value_name("NUM")
                .help("Maximum concurrent tasks (for service mode)")
                .default_value("10")
                .value_parser(clap::value_parser!(u32)),
        )
        .get_matches();

    let mode = matches.get_one::<String>("mode").unwrap();

    match mode.as_str() {
        "task" => {
            // Legacy single-task execution mode
            run_single_task_mode(&matches).await
        }
        "service" => {
            // New worker service mode using client library
            run_worker_service_mode(&matches).await
        }
        _ => unreachable!("Invalid mode"), // clap should prevent this
    }
}

/// Run in single-task execution mode (backward compatibility)
async fn run_single_task_mode(matches: &clap::ArgMatches) -> Result<()> {
    // Parse arguments
    let task_id = matches.get_one::<String>("task-id").unwrap();
    let task_name = matches.get_one::<String>("name").unwrap();
    let args_json = matches.get_one::<String>("args").unwrap();
    let kwargs_json = matches.get_one::<String>("kwargs").unwrap();
    let shepherd_endpoint = matches.get_one::<String>("shepherd-endpoint").unwrap();

    // Validate task ID
    let task_uuid =
        Uuid::parse_str(task_id).map_err(|e| anyhow::anyhow!("Invalid task ID: {e}"))?;

    // Parse JSON arguments
    let args: Value =
        serde_json::from_str(args_json).map_err(|e| anyhow::anyhow!("Invalid args JSON: {e}"))?;
    let kwargs: Value = serde_json::from_str(kwargs_json)
        .map_err(|e| anyhow::anyhow!("Invalid kwargs JSON: {e}"))?;

    // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
    info!("Worker starting: task_id={task_uuid}, name={task_name}, shepherd={shepherd_endpoint}");
    info!("Task args: {args}, kwargs: {kwargs}");

    // Execute the task
    let result = execute_task(task_name, &args, &kwargs).await;

    // Report result back to shepherd
    match report_result_to_shepherd(task_uuid, result, shepherd_endpoint).await {
        Ok(_) => {
            info!("Successfully reported result to shepherd");
        }
        Err(e) => {
            error!("Failed to report result to shepherd: {e}");
            std::process::exit(1);
        }
    }

    info!("Worker completed successfully");
    Ok(())
}

/// Run in worker service mode using the client library
async fn run_worker_service_mode(matches: &clap::ArgMatches) -> Result<()> {
    let orchestrator_endpoint = matches.get_one::<String>("orchestrator-endpoint").unwrap();
    let domain = matches.get_one::<String>("domain").unwrap();
    let shepherd_group = matches.get_one::<String>("shepherd-group").unwrap();
    let max_concurrency = *matches.get_one::<u32>("max-concurrency").unwrap();

    info!("Starting worker service mode");
    info!("Orchestrator: {orchestrator_endpoint}");
    info!("Domain: {domain}, Group: {shepherd_group}, Max concurrency: {max_concurrency}");

    // Build worker with type-safe task implementations
    let worker = Worker::builder()
        .orchestrator(orchestrator_endpoint)
        .domain(domain)
        .shepherd_group(shepherd_group)
        .max_concurrency(max_concurrency)
        // Basic task implementations
        .register_task(EchoTaskTask)
        .register_task(AlwaysFailTaskTask)
        .register_task(FlakyTaskTask)
        // Tasks with custom argument types
        .register_task(MathAddTask)
        .register_task(CountArgsTask)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build worker: {e}"))?;

    info!(
        "Worker built successfully with {} tasks",
        worker.task_count()
    );

    // Run the worker (this will block until shutdown)
    worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Worker failed: {e}"))?;

    info!("Worker service stopped");
    Ok(())
}

async fn execute_task(task_name: &str, args: &Value, kwargs: &Value) -> common::TaskResult {
    info!("Executing task: {task_name}");

    // Simulate task execution time
    let execution_time = match kwargs.get("sleep_duration") {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(1.0),
        _ => 1.0, // Default 1 second
    };

    tokio::time::sleep(Duration::from_secs_f64(execution_time)).await;

    // Handle special test tasks
    match task_name {
        "flaky_task" => {
            return handle_flaky_task(kwargs).await;
        }
        "always_fail" => {
            return create_error_result(task_name, "This task always fails", "ALWAYS_FAIL");
        }
        _ => {}
    }

    // Handle generic tasks with potential failure
    handle_generic_task(task_name, args, kwargs)
}

/// Handle generic tasks with potential failure simulation
fn handle_generic_task(task_name: &str, args: &Value, kwargs: &Value) -> common::TaskResult {
    // Simulate success/failure based on task name or arguments
    let should_fail = task_name.contains("fail")
        || kwargs
            .get("should_fail")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

    if should_fail {
        info!("Task {task_name} simulated failure");
        create_error_result(
            task_name,
            &format!("Task {task_name} was configured to fail"),
            "SIMULATED_FAILURE",
        )
    } else {
        info!("Task {task_name} completed successfully");

        let result_value = match task_name {
            "echo" => handle_echo_task(args),
            "math_add" => handle_math_add_task(kwargs),
            "count_args" => handle_count_args_task(args),
            _ => handle_default_task(task_name),
        };

        common::TaskResult {
            task_id: "".to_string(), // Will be set by caller
            result_type: Some(common::task_result::ResultType::Success(
                common::SuccessResult {
                    result: Some(result_value),
                },
            )),
        }
    }
}

/// Handle echo task - echo back the first argument
fn handle_echo_task(args: &Value) -> common::AnyValue {
    if let Some(arr) = args.as_array() {
        if let Some(first_arg) = arr.first() {
            match first_arg {
                Value::String(s) => common::AnyValue {
                    value: Some(common::any_value::Value::StringValue(s.clone())),
                },
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        common::AnyValue {
                            value: Some(common::any_value::Value::IntValue(i)),
                        }
                    } else {
                        common::AnyValue {
                            value: Some(common::any_value::Value::DoubleValue(
                                n.as_f64().unwrap_or(0.0),
                            )),
                        }
                    }
                }
                Value::Bool(b) => common::AnyValue {
                    value: Some(common::any_value::Value::BoolValue(*b)),
                },
                _ => common::AnyValue {
                    value: Some(common::any_value::Value::JsonValue(first_arg.to_string())),
                },
            }
        } else {
            common::AnyValue {
                value: Some(common::any_value::Value::StringValue(
                    "empty_args".to_string(),
                )),
            }
        }
    } else {
        common::AnyValue {
            value: Some(common::any_value::Value::StringValue("no_args".to_string())),
        }
    }
}

/// Handle math_add task - add two numbers from kwargs
fn handle_math_add_task(kwargs: &Value) -> common::AnyValue {
    let a = kwargs.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let b = kwargs.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
    common::AnyValue {
        value: Some(common::any_value::Value::DoubleValue(a + b)),
    }
}

/// Handle count_args task - count the number of arguments
fn handle_count_args_task(args: &Value) -> common::AnyValue {
    let count = args.as_array().map(|arr| arr.len()).unwrap_or(0) as i64;
    common::AnyValue {
        value: Some(common::any_value::Value::IntValue(count)),
    }
}

/// Handle default task - return success message
fn handle_default_task(task_name: &str) -> common::AnyValue {
    common::AnyValue {
        value: Some(common::any_value::Value::StringValue(format!(
            "Task {task_name} completed"
        ))),
    }
}

async fn handle_flaky_task(kwargs: &Value) -> common::TaskResult {
    let fail_first_attempt = kwargs
        .get("fail_first_attempt")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if fail_first_attempt {
        // Check if we have a state file to track attempts
        let state_file =
            std::env::temp_dir().join(format!("flaky_task_state_{}", std::process::id()));

        // Read attempt count from state file
        let attempt_count = match std::fs::read_to_string(&state_file) {
            Ok(content) => content.trim().parse::<u32>().unwrap_or(0),
            Err(_) => 0,
        };

        // Increment and write back
        let new_attempt_count = attempt_count + 1;
        let _ = std::fs::write(&state_file, new_attempt_count.to_string());

        info!("Flaky task attempt #{new_attempt_count}");

        // Fail on first attempt, succeed on subsequent attempts
        if new_attempt_count == 1 {
            return create_error_result(
                "flaky_task",
                "First attempt failure",
                "FLAKY_TASK_FIRST_ATTEMPT",
            );
        }
    }

    // Success case
    common::TaskResult {
        task_id: "".to_string(),
        result_type: Some(common::task_result::ResultType::Success(
            common::SuccessResult {
                result: Some(common::AnyValue {
                    value: Some(common::any_value::Value::StringValue(
                        "Flaky task succeeded on retry".to_string(),
                    )),
                }),
            },
        )),
    }
}

fn create_error_result(task_name: &str, message: &str, code: &str) -> common::TaskResult {
    common::TaskResult {
        task_id: "".to_string(), // Will be set by caller
        result_type: Some(common::task_result::ResultType::Error(
            common::ErrorResult {
                r#type: "TestError".to_string(),
                message: message.to_string(),
                data: serde_json::json!({
                    "task_name": task_name,
                    "error_code": code
                })
                .to_string(),
                retriable: true,
            },
        )),
    }
}

async fn report_result_to_shepherd(
    task_id: Uuid,
    mut result: common::TaskResult,
    shepherd_endpoint: &str,
) -> Result<()> {
    // Set the task ID in the result
    result.task_id = task_id.to_string();

    info!("Connecting to shepherd at {shepherd_endpoint}");

    // Connect to shepherd's worker service
    let mut client = WorkerClient::connect(shepherd_endpoint.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to shepherd: {e}"))?;

    // Send the result
    let request = Request::new(ReportResultRequest {
        task_id: task_id.to_string(),
        result: Some(result),
    });

    let response = client
        .report_result(request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to report result: {e}"))?;

    let report_response = response.into_inner();

    if report_response.success {
        info!("Result reported successfully");
    } else {
        error!("Shepherd rejected result: {}", report_response.message);
        return Err(anyhow::anyhow!(
            "Shepherd rejected result: {}",
            report_response.message
        ));
    }

    Ok(())
}
