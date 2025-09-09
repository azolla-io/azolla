use azolla_client::{azolla_task, Task, TaskError, TaskResult, Worker};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

// =============================================================================
// APPROACH 1: Proc Macro Tasks (Convenient and now working!)
// =============================================================================

// NOTE: These proc macro tasks are now working with the updated azolla_task macro.

#[azolla_task]
async fn echo_message(message: String) -> Result<Value, TaskError> {
    Ok(json!({
        "echoed": message,
        "timestamp": "2024-01-01T00:00:00Z" // Using fixed timestamp to avoid chrono dependency
    }))
}

#[azolla_task]
async fn calculate_fibonacci(n: u32) -> Result<Value, TaskError> {
    if n > 50 {
        return Err(TaskError::invalid_args(
            "Fibonacci input too large (max: 50)",
        ));
    }

    let fib = fibonacci(n);
    Ok(json!({
        "input": n,
        "result": fib
    }))
}

fn fibonacci(n: u32) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        _ => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

// =============================================================================
// APPROACH 2: Manual Task Trait Implementation (Type-safe and reliable)
// =============================================================================

// Define task argument types
#[derive(Debug, Deserialize, Serialize)]
pub struct AddNumbersArgs {
    pub a: i32,
    pub b: i32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GreetUserArgs {
    pub name: String,
    pub age: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessDataArgs {
    pub numbers: Vec<f64>,
    pub operation: String, // "sum", "average", "max", "min"
}

// Task implementation functions
async fn add_numbers(a: i32, b: i32) -> Result<Value, TaskError> {
    let sum = a + b;
    Ok(json!({
        "inputs": [a, b],
        "sum": sum,
        "operation": "addition"
    }))
}

async fn greet_user(name: String, age: Option<u32>) -> Result<Value, TaskError> {
    let greeting = match age {
        Some(age) => format!("Hello {name}, you are {age} years old!"),
        None => format!("Hello {name}!"),
    };

    Ok(json!({
        "greeting": greeting,
        "name": name,
        "age": age
    }))
}

async fn process_data(numbers: Vec<f64>, operation: String) -> Result<Value, TaskError> {
    if numbers.is_empty() {
        return Err(TaskError::invalid_args("Numbers array cannot be empty"));
    }

    let result = match operation.as_str() {
        "sum" => numbers.iter().sum(),
        "average" => numbers.iter().sum::<f64>() / numbers.len() as f64,
        "max" => numbers.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        "min" => numbers.iter().cloned().fold(f64::INFINITY, f64::min),
        _ => {
            return Err(TaskError::invalid_args(&format!(
                "Unknown operation: {operation}"
            )))
        }
    };

    Ok(json!({
        "input": numbers,
        "operation": operation,
        "result": result,
        "count": numbers.len()
    }))
}

// Task struct implementations
pub struct AddNumbersTask;

impl Task for AddNumbersTask {
    type Args = AddNumbersArgs;

    fn name(&self) -> &'static str {
        "add_numbers"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let result = add_numbers(args.a, args.b).await;
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

pub struct GreetUserTask;

impl Task for GreetUserTask {
    type Args = GreetUserArgs;

    fn name(&self) -> &'static str {
        "greet_user"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let result = greet_user(args.name, args.age).await;
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

pub struct ProcessDataTask;

impl Task for ProcessDataTask {
    type Args = ProcessDataArgs;

    fn name(&self) -> &'static str {
        "process_data"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let result = process_data(args.numbers, args.operation).await;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Azolla Worker Example");
    println!("========================");

    println!("📝 Creating worker with both task implementation approaches...");

    // Create a worker with registered tasks from both approaches
    let worker = Worker::builder()
        .orchestrator("localhost:52710")
        .domain("example-domain")
        .shepherd_group("default")
        .max_concurrency(4)
        // Manual trait implementations (currently working)
        .register_task(AddNumbersTask)
        .register_task(GreetUserTask)
        .register_task(ProcessDataTask)
        // Proc macro implementations (generated by #[azolla_task])
        .register_task(EchoMessageTask) // Generated from echo_message function
        .register_task(CalculateFibonacciTask) // Generated from calculate_fibonacci function
        .build()
        .await?;

    println!("✅ Worker created with {} tasks", worker.task_count());
    println!("\n📋 Registered Tasks (Manual Trait Implementation):");
    println!("   • add_numbers: Adds two integers and returns detailed result");
    println!("     Args: {{\"a\": i32, \"b\": i32}}");
    println!("   • greet_user: Creates personalized greeting message");
    println!("     Args: {{\"name\": String, \"age\": Option<u32>}}");
    println!("   • process_data: Statistical operations on number arrays");
    println!("     Args: {{\"numbers\": Vec<f64>, \"operation\": String}}");

    println!("\n📋 Proc Macro Generated Tasks (now working!):");
    println!("   • echo_message: Echoes message with timestamp");
    println!("     Generated from: #[azolla_task] async fn echo_message(message: String)");
    println!("   • calculate_fibonacci: Computes Fibonacci numbers");
    println!("     Generated from: #[azolla_task] async fn calculate_fibonacci(n: u32)");

    println!("\n🔄 Task Implementation Approaches:");
    println!("   1️⃣ Proc Macro (#[azolla_task]):");
    println!("      ✅ Less boilerplate code");
    println!("      ✅ Automatic struct generation");
    println!("      ✅ Type-safe argument parsing");
    println!("      ✅ Now working with updated macro!");

    println!("\n   2️⃣ Manual Trait Implementation:");
    println!("      ✅ Full control over argument types");
    println!("      ✅ Type-safe argument parsing");
    println!("      ✅ Always reliable and explicit");
    println!("      ❌ More verbose (boilerplate code)");
    println!("      💡 Best for complex task logic");

    println!("\n🚀 Worker Runtime Features:");
    println!("   ✓ Type-safe argument parsing from JSON");
    println!("   ✓ Compile-time argument validation");
    println!("   ✓ Automatic error handling and reporting");
    println!("   ✓ Concurrent task processing (max: 4)");
    println!("   ✓ Automatic retry and failure handling");
    println!("   ✓ Shepherd group targeting");

    // In a real application, you would call:
    // worker.run().await?;

    println!("\n🎉 Worker example completed!");
    println!("💡 To run in production:");
    println!("   1. Start an Azolla orchestrator");
    println!("   2. Uncomment worker.run().await?");
    println!("   3. The worker will process tasks until stopped");

    Ok(())
}
