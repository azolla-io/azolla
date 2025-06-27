use anyhow::Result;
use clap::{Arg, Command};
use log::{info, error};
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;
use serde_json::Value;

use azolla::proto::{common, shepherd};
use shepherd::worker_client::WorkerClient;
use shepherd::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let matches = Command::new("azolla-worker")
        .version("0.1.0")
        .about("Azolla Worker - Executes individual tasks")
        .arg(
            Arg::new("task-id")
                .long("task-id")
                .value_name("UUID")
                .help("Task ID to execute")
                .required(true)
        )
        .arg(
            Arg::new("name")
                .long("name")
                .value_name("NAME")
                .help("Task name")
                .required(true)
        )
        .arg(
            Arg::new("args")
                .long("args")
                .value_name("JSON")
                .help("Task arguments as JSON array")
                .default_value("[]")
        )
        .arg(
            Arg::new("kwargs")
                .long("kwargs")
                .value_name("JSON")
                .help("Task keyword arguments as JSON object")
                .default_value("{}")
        )
        .arg(
            Arg::new("shepherd-endpoint")
                .long("shepherd-endpoint")
                .value_name("ENDPOINT")
                .help("Shepherd gRPC endpoint for result reporting")
                .required(true)
        )
        .get_matches();

    // Parse arguments
    let task_id = matches.get_one::<String>("task-id").unwrap();
    let task_name = matches.get_one::<String>("name").unwrap();
    let args_json = matches.get_one::<String>("args").unwrap();
    let kwargs_json = matches.get_one::<String>("kwargs").unwrap();
    let shepherd_endpoint = matches.get_one::<String>("shepherd-endpoint").unwrap();
    
    // Validate task ID
    let task_uuid = Uuid::parse_str(task_id)
        .map_err(|e| anyhow::anyhow!("Invalid task ID: {}", e))?;
    
    // Parse JSON arguments
    let args: Value = serde_json::from_str(args_json)
        .map_err(|e| anyhow::anyhow!("Invalid args JSON: {}", e))?;
    let kwargs: Value = serde_json::from_str(kwargs_json)
        .map_err(|e| anyhow::anyhow!("Invalid kwargs JSON: {}", e))?;
    
    info!("Worker starting: task_id={}, name={}, shepherd={}", 
          task_uuid, task_name, shepherd_endpoint);
    info!("Task args: {}, kwargs: {}", args, kwargs);
    
    // Execute the task
    let result = execute_task(task_name, &args, &kwargs).await;
    
    // Report result back to shepherd
    match report_result_to_shepherd(task_uuid, result, shepherd_endpoint).await {
        Ok(_) => {
            info!("Successfully reported result to shepherd");
        }
        Err(e) => {
            error!("Failed to report result to shepherd: {}", e);
            std::process::exit(1);
        }
    }
    
    info!("Worker completed successfully");
    Ok(())
}

async fn execute_task(task_name: &str, args: &Value, kwargs: &Value) -> common::TaskResult {
    info!("Executing task: {}", task_name);
    
    // Simulate task execution time
    let execution_time = match kwargs.get("sleep_duration") {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(1.0),
        _ => 1.0, // Default 1 second
    };
    
    tokio::time::sleep(Duration::from_secs_f64(execution_time)).await;
    
    // Simulate success/failure based on task name or arguments
    let should_fail = task_name.contains("fail") || 
                     kwargs.get("should_fail").and_then(|v| v.as_bool()).unwrap_or(false);
    
    if should_fail {
        info!("Task {} simulated failure", task_name);
        common::TaskResult {
            task_id: "".to_string(), // Will be set by caller
            result_type: Some(common::task_result::ResultType::Error(common::ErrorResult {
                r#type: "SimulatedError".to_string(),
                message: format!("Task {} was configured to fail", task_name),
                code: "SIMULATED_FAILURE".to_string(),
                stacktrace: "".to_string(),
                data: Some(common::StructValue {
                    json_data: serde_json::json!({
                        "task_name": task_name,
                        "args": args,
                        "kwargs": kwargs
                    }).to_string(),
                }),
            })),
        }
    } else {
        info!("Task {} completed successfully", task_name);
        
        // Create a simple result based on task type
        let result_value = match task_name {
            "echo" => {
                // Echo back the first argument
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
                                        value: Some(common::any_value::Value::DoubleValue(n.as_f64().unwrap_or(0.0))),
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
                            value: Some(common::any_value::Value::StringValue("empty_args".to_string())),
                        }
                    }
                } else {
                    common::AnyValue {
                        value: Some(common::any_value::Value::StringValue("no_args".to_string())),
                    }
                }
            }
            "math_add" => {
                // Add two numbers from kwargs
                let a = kwargs.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let b = kwargs.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                common::AnyValue {
                    value: Some(common::any_value::Value::DoubleValue(a + b)),
                }
            }
            "count_args" => {
                // Count the number of arguments
                let count = args.as_array().map(|arr| arr.len()).unwrap_or(0) as i64;
                common::AnyValue {
                    value: Some(common::any_value::Value::IntValue(count)),
                }
            }
            _ => {
                // Default: return success message
                common::AnyValue {
                    value: Some(common::any_value::Value::StringValue(format!("Task {} completed", task_name))),
                }
            }
        };
        
        common::TaskResult {
            task_id: "".to_string(), // Will be set by caller
            result_type: Some(common::task_result::ResultType::Success(common::SuccessResult {
                result: Some(result_value),
            })),
        }
    }
}

async fn report_result_to_shepherd(
    task_id: Uuid,
    mut result: common::TaskResult,
    shepherd_endpoint: &str,
) -> Result<()> {
    // Set the task ID in the result
    result.task_id = task_id.to_string();
    
    info!("Connecting to shepherd at {}", shepherd_endpoint);
    
    // Connect to shepherd's worker service
    let mut client = WorkerClient::connect(shepherd_endpoint.to_string()).await
        .map_err(|e| anyhow::anyhow!("Failed to connect to shepherd: {}", e))?;
    
    // Send the result
    let request = Request::new(ReportResultRequest {
        task_id: task_id.to_string(),
        result: Some(result),
    });
    
    let response = client.report_result(request).await
        .map_err(|e| anyhow::anyhow!("Failed to report result: {}", e))?;
    
    let report_response = response.into_inner();
    
    if report_response.success {
        info!("Result reported successfully");
    } else {
        error!("Shepherd rejected result: {}", report_response.message);
        return Err(anyhow::anyhow!("Shepherd rejected result: {}", report_response.message));
    }
    
    Ok(())
}