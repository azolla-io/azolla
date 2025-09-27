# Azolla Rust Client Library - Developer Guide

This guide shows you how to build applications with the Azolla Rust client library. You'll learn to install dependencies, write type-safe tasks, compose worker processes, and submit tasks for execution.

## Installation

### Option 1: Client Only (Task Submission)

For applications that only need to submit tasks to existing Azolla workers:

```toml
[dependencies]
azolla-client = "0.1.1"
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
```

### Option 2: Full Development (Tasks + Workers)

For applications that define tasks using proc macros and run workers:

```toml
[dependencies]
azolla-client = { version = "0.1.1", features = ["macros"] }
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
```

## Writing Tasks

Tasks are functions that process arguments and return results. Azolla supports type-safe task definitions using the `#[azolla_task]` proc macro.

### Basic Task Definition

```rust
use azolla_client::{azolla_task, TaskError};
use serde_json::{json, Value};

#[azolla_task]
async fn greet_user(name: String, age: u32) -> Result<Value, TaskError> {
    if name.is_empty() {
        return Err(TaskError::invalid_args("Name cannot be empty"));
    }
    
    Ok(json!({
        "greeting": format!("Hello {name}! You are {age} years old."),
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
```

The proc macro automatically:
- Generates a `GreetUserTask` struct
- Extracts typed arguments from JSON
- Handles argument validation
- Provides compile-time type checking

### Task with Optional Parameters

```rust
#[azolla_task]
async fn process_data(
    input: String, 
    format: Option<String>, 
    validate: bool
) -> Result<Value, TaskError> {
    let format = format.unwrap_or_else(|| "json".to_string());
    
    if validate && input.len() < 5 {
        return Err(TaskError::execution_failed("Input too short"));
    }
    
    Ok(json!({
        "processed_input": input.to_uppercase(),
        "format": format,
        "validated": validate
    }))
}
```

### Task with Complex Data Types

```rust
#[azolla_task]
async fn sum_numbers(numbers: Vec<i32>) -> Result<Value, TaskError> {
    if numbers.is_empty() {
        return Err(TaskError::invalid_args("Numbers array cannot be empty"));
    }
    
    let sum: i32 = numbers.iter().sum();
    let avg = sum as f64 / numbers.len() as f64;
    
    Ok(json!({
        "numbers": numbers,
        "sum": sum,
        "average": avg,
        "count": numbers.len()
    }))
}
```

### Advanced Task with State (Trait-based)

For tasks that need to maintain state or complex initialization:

```rust
use azolla_client::{Task, TaskResult, TaskError};
use std::future::Future;
use std::pin::Pin;

pub struct DatabaseTask {
    connection_pool: sqlx::PgPool,
}

impl DatabaseTask {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = sqlx::PgPool::connect(database_url).await?;
        Ok(Self { connection_pool: pool })
    }
}

impl Task for DatabaseTask {
    type Args = (String, Option<i64>);

    fn name(&self) -> &'static str {
        "database_query"
    }
    
    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let (query, limit) = args;
            let limit = limit.unwrap_or(100);
            
            if query.is_empty() {
                return Err(TaskError::invalid_args("Query cannot be empty"));
            }
            
            let row_count = sqlx::query(&query)
                .fetch_all(&self.connection_pool)
                .await
                .map_err(|e| TaskError::execution_failed(&format!("Database error: {e}")))?
                .len();
            
            Ok(json!({
                "query": query,
                "limit": limit,
                "rows_returned": row_count
            }))
        })
    }
}
```

## Composing Worker Processes

Rust workers now execute **exactly one task per process launch**. The Azolla shepherd is responsible for spawning workers, passing invocation metadata over CLI flags, and receiving the final task result through its `ReportResult` gRPC endpoint.

### Registering Tasks

```rust
use azolla_client::{azolla_task, Worker, WorkerInvocation, TaskError};
use serde_json::{json, Value};

#[azolla_task]
async fn resize_image(url: String, width: u32, height: u32) -> Result<Value, TaskError> {
    if width == 0 || height == 0 {
        return Err(TaskError::invalid_args("Width and height must be positive"));
    }

    Ok(json!({
        "source": url,
        "target": format!("{url}?w={width}&h={height}"),
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Register one or more tasks up front
    let worker = Worker::builder()
        .register_task(ResizeImageTask)
        .build();

    println!("🚀 Worker ready with {} tasks", worker.task_count());

    // Parse invocation metadata supplied by the shepherd
    let args = clap::Command::new("image-worker")
        .arg(clap::Arg::new("task-id").long("task-id").required(true))
        .arg(clap::Arg::new("name").long("name").required(true))
        .arg(clap::Arg::new("args").long("args").default_value("[]"))
        .arg(clap::Arg::new("kwargs").long("kwargs").default_value("{}"))
        .arg(
            clap::Arg::new("shepherd-endpoint")
                .long("shepherd-endpoint")
                .required(true),
        )
        .get_matches();

    let invocation = WorkerInvocation::from_json(
        args.get_one::<String>("task-id").unwrap(),
        args.get_one::<String>("name").unwrap(),
        args.get_one::<String>("args").unwrap(),
        args.get_one::<String>("kwargs").unwrap(),
        args.get_one::<String>("shepherd-endpoint").unwrap(),
    )?;

    // Execute the task and report the result back to the shepherd
    worker.run_invocation(invocation).await?;
    Ok(())
}
```

### Why Single-Run Workers?

- **Deterministic lifecycle** – every process handles one task, making resource cleanup trivial.
- **Shepherd-native orchestration** – the shepherd controls concurrency, retries, and binary upgrades.
- **Simple testing** – workers can be executed locally using `Worker::execute` without gRPC wiring.

### Running Tasks Manually in Tests

```rust
use azolla_client::{TaskExecutionOutcome, Worker, WorkerInvocation};
use serde_json::json;
use uuid::Uuid;

let worker = Worker::builder().register_task(ResizeImageTask).build();
let invocation = WorkerInvocation {
    task_id: Uuid::new_v4(),
    task_name: "resize_image".to_string(),
    args: vec![json!("https://example.com/image.jpg"), json!(640), json!(480)],
    kwargs: json!({}),
    shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
};

let execution = worker.execute(&invocation).await;
assert!(execution.is_success());
```

When the shepherd launches your worker binary it will provide the same CLI arguments. After `run_invocation` completes, the shepherd immediately learns the task outcome and continues orchestration.

## Creating and Submitting Tasks

Use the standalone client to submit tasks to running workers.

### Basic Client Usage

```rust
use azolla_client::{Client, TaskExecutionResult};

#[tokio::main]
async fn main() -> Result<(), azolla_client::AzollaError> {
    // Connect to Azolla orchestrator
    let client = Client::connect("http://localhost:52710").await?;
    
    // Example 1: Submit a proc macro-based task
    // This corresponds to a task defined with:
    // #[azolla_task]
    // async fn greet_user(name: String, age: u32) -> Result<Value, TaskError>
    let proc_macro_task = client
        .submit_task("greet_user")           // Task name (must match worker's registered task)
        .args(("Alice".to_string(), 25u32))? // Arguments: (name: String, age: u32)
        .submit()
        .await?;
    
    println!("✅ Submitted proc macro task: {}", proc_macro_task.id());
    
    // Example 2: Submit a trait-based task 
    // This corresponds to a task registered with:
    // impl Task for DatabaseTask {
    //     fn name(&self) -> &'static str { "database_query" }
    //     ...
    // }
    let trait_task = client
        .submit_task("database_query")                          // Task name from trait implementation
        .args(("SELECT * FROM users".to_string(), 50u64))?     // Arguments: (query: String, limit: u64)
        .submit()
        .await?;
    
    println!("✅ Submitted trait-based task: {}", trait_task.id());
    
    // Wait for both tasks to complete
    match proc_macro_task.wait().await? {
        TaskExecutionResult::Success(result) => {
            println!("🎉 Proc macro task completed: {}", result);
            // Example output: {"greeting": "Hello Alice! You are 25 years old.", "timestamp": "2024-01-01T12:00:00Z"}
        },
        TaskExecutionResult::Failed(error) => {
            eprintln!("❌ Proc macro task failed: {}", error);
        }
    }
    
    match trait_task.wait().await? {
        TaskExecutionResult::Success(result) => {
            println!("🎉 Trait-based task completed: {}", result);
            // Example output: {"query": "SELECT * FROM users", "limit": 50, "rows_returned": 42}
        },
        TaskExecutionResult::Failed(error) => {
            eprintln!("❌ Trait-based task failed: {}", error);
        }
    }
    
    Ok(())
}
```

### Production Client Configuration

This example demonstrates production-ready client usage with custom endpoints, retry policies, shepherd group targeting, and non-blocking result polling.

```rust
use azolla_client::{Client, RetryPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), azolla_client::AzollaError> {
    // Create client with custom configuration
    let client = Client::builder()
        .endpoint("http://production-azolla.company.com:52710")
        .domain("image-processing")
        .timeout(Duration::from_secs(60))
        .build()
        .await?;
    
    // Create retry policy
    let retry_policy = RetryPolicy::exponential()
        .max_attempts(3)
        .build();
    
    // Submit task with retry policy and shepherd group
    let task = client
        .submit_task("process_image")
        .args((
            "https://example.com/image.jpg".to_string(),
            800u32,
            600u32
        ))?
        .retry_policy(retry_policy)
        .shepherd_group("gpu-workers")
        .submit()
        .await?;
    
    println!("📤 Submitted image processing task: {}", task.id());
    
    // Poll for result without blocking
    loop {
        match task.try_result().await? {
            Some(TaskExecutionResult::Success(result)) => {
                println!("✅ Image processed: {}", result);
                break;
            },
            Some(TaskExecutionResult::Failed(error)) => {
                eprintln!("❌ Processing failed: {}", error);
                break;
            },
            None => {
                println!("⏳ Still processing...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    
    Ok(())
}
```

### Batch Task Submission

```rust
use azolla_client::Client;
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), azolla_client::AzollaError> {
    let client = Client::connect("http://localhost:52710").await?;
    
    // Submit multiple tasks concurrently
    let tasks = vec![
        client.submit_task("sum_numbers").args(vec![1, 2, 3, 4, 5])?.submit(),
        client.submit_task("sum_numbers").args(vec![10, 20, 30])?.submit(),
        client.submit_task("greet_user").args(("Bob".to_string(), 30u32))?.submit(),
    ];
    
    let submitted_tasks = join_all(tasks).await;
    
    println!("📤 Submitted {} tasks", submitted_tasks.len());
    
    // Wait for all completions
    for (i, task_result) in submitted_tasks.into_iter().enumerate() {
        match task_result {
            Ok(task) => {
                println!("⏳ Waiting for task {}: {}", i + 1, task.id());
                match task.wait().await? {
                    azolla_client::TaskExecutionResult::Success(result) => {
                        println!("✅ Task {} completed: {}", i + 1, result);
                    },
                    azolla_client::TaskExecutionResult::Failed(error) => {
                        eprintln!("❌ Task {} failed: {}", i + 1, error);
                    }
                }
            },
            Err(e) => eprintln!("❌ Failed to submit task {}: {}", i + 1, e),
        }
    }
    
    Ok(())
}
```

## Error Handling

Both the client and worker provide comprehensive error handling with automatic retry logic based on error types.

### Task Error Types and Retry Behavior

The Rust client provides several built-in error constructors with appropriate retry behavior:

```rust
#[azolla_task]
async fn validate_email(email: String) -> Result<Value, TaskError> {
    // Validation errors (non-retryable) - input problems won't be fixed by retrying
    if email.is_empty() {
        return Err(TaskError::validation_error("Email cannot be empty"));
    }

    if !email.contains('@') {
        return Err(TaskError::validation_error("Invalid email format"));
    }

    // Execution errors (retryable) - temporary issues that might resolve
    let response = reqwest::get(&format!("https://api.emailvalidation.com/check/{}", email))
        .await
        .map_err(|e| TaskError::execution_failed(&format!("API request failed: {}", e)))?;

    if response.status() == 429 {
        // Rate limiting - should be retried after delay
        return Err(TaskError::execution_failed("Rate limited by validation service"));
    }

    if response.status() == 500 {
        // Server error - temporary, should retry
        return Err(TaskError::execution_failed("Validation service temporarily unavailable"));
    }

    if response.status() == 400 {
        // Bad request - input problem, don't retry
        return Err(TaskError::validation_error("Invalid email format rejected by service"));
    }

    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|e| TaskError::execution_failed(&format!("Invalid API response: {}", e)))?;

    Ok(json!({
        "email": email,
        "valid": result["valid"].as_bool().unwrap_or(false),
        "checked_at": chrono::Utc::now().to_rfc3339()
    }))
}
```

### Built-in Error Constructors

```rust
// Validation errors (retryable: false) - for invalid input data
TaskError::validation_error("Invalid email format")
TaskError::invalid_args("Name cannot be empty")  // Legacy constructor

// Execution errors (retryable: true) - for runtime failures
TaskError::execution_failed("Database connection failed")
TaskError::timeout_error("Operation took too long")

// Custom errors with explicit retry behavior
TaskError::new("Custom error message")
    .with_error_type("CustomError")
    .with_error_code("CUSTOM_001")
    .with_retryable(false)  // Explicitly non-retryable

// Structured error data for debugging
TaskError::execution_failed("Payment processing failed")
    .with_error_code("PAYMENT_DECLINED")
    .with_retryable(false)  // Don't retry declined payments
```

### Error Types and Retry Defaults

| Error Constructor | Retryable Default | Use Case | Example |
|------------------|-------------------|----------|---------|
| `validation_error()` | **false** | Invalid input data | Bad email format, missing fields |
| `invalid_args()` | **false** | Legacy validation errors | Empty required parameters |
| `execution_failed()` | **true** | Runtime failures | Network errors, temporary service issues |
| `timeout_error()` | **true** | Operation timeouts | Slow API responses, long computations |
| `new()` | **true** | Generic errors | Custom business logic errors |

### Consistency with Python Client

The Rust client's error handling is fully compatible with the Python client:

- **Same error types**: `TaskValidationError`, `TaskTimeoutError`, etc.
- **Same retry behavior**: Validation errors are non-retryable, execution errors are retryable
- **Same serialization format**: Errors are transmitted using identical protobuf structure
- **Type safety advantage**: Rust's type system ensures tasks must return `Result<T, TaskError>`, eliminating the need for automatic exception wrapping (unlike Python)

### Advanced Error Handling with Context Data

```rust
#[azolla_task]
async fn process_payment(amount: f64, card_token: String) -> Result<Value, TaskError> {
    if amount <= 0.0 {
        return Err(TaskError::validation_error("Amount must be positive"));
    }

    // Simulate payment processing
    let payment_result = charge_card(&card_token, amount).await;

    match payment_result {
        Ok(transaction_id) => Ok(json!({
            "transaction_id": transaction_id,
            "amount": amount,
            "status": "completed"
        })),
        Err(PaymentError::InsufficientFunds) => {
            // Don't retry - customer needs to fix their account
            Err(TaskError::validation_error("Insufficient funds")
                .with_error_code("INSUFFICIENT_FUNDS"))
        },
        Err(PaymentError::NetworkError) => {
            // Retry - network issues are temporary
            Err(TaskError::execution_failed("Payment network unavailable")
                .with_error_code("NETWORK_ERROR"))
        },
        Err(PaymentError::ServiceUnavailable) => {
            // Retry with backoff - service might recover
            Err(TaskError::execution_failed("Payment service temporarily unavailable")
                .with_error_code("SERVICE_UNAVAILABLE"))
        }
    }
}
```

### Client Error Handling

```rust
use azolla_client::{AzollaError, TaskExecutionResult};

async fn robust_task_submission(client: &Client) -> Result<(), AzollaError> {
    let task = match client
        .submit_task("validate_email")
        .args("user@example.com".to_string())
    {
        Ok(builder) => builder.submit().await?,
        Err(e) => {
            eprintln!("Failed to prepare task: {}", e);
            return Err(e);
        }
    };
    
    match task.wait().await {
        Ok(TaskExecutionResult::Success(result)) => {
            println!("Email validation result: {}", result);
        },
        Ok(TaskExecutionResult::Failed(task_error)) => {
            eprintln!("Task execution failed: {}", task_error);
            // Handle specific task failures
        },
        Err(AzollaError::Transport(transport_error)) => {
            eprintln!("Network error: {}", transport_error);
            // Handle network issues, maybe retry
        },
        Err(AzollaError::Status(grpc_status)) => {
            eprintln!("gRPC error: {}", grpc_status);
            // Handle gRPC-specific errors
        },
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
        }
    }
    
    Ok(())
}
```

## Testing Your Tasks

The Rust client includes comprehensive internal test coverage (75+ tests) ensuring library reliability and panic safety. Here's how to test your own tasks.

### Unit Testing Proc Macro Tasks

Tasks generated by the `#[azolla_task]` proc macro use **typed arguments**:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_greet_user_success() {
        let task = GreetUserTask;
        // Use typed arguments matching the function signature
        let args = ("Alice".to_string(), 25u32);
        
        let result = task.execute(args).await.unwrap();
        
        assert_eq!(result["greeting"], "Hello Alice! You are 25 years old.");
    }
    
    #[tokio::test]
    async fn test_greet_user_empty_name() {
        let task = GreetUserTask;
        let args = ("".to_string(), 25u32);
        
        let result = task.execute(args).await;
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_sum_numbers() {
        let task = SumNumbersTask;
        // Single argument is used directly, not wrapped in tuple
        let args = vec![1, 2, 3, 4, 5];
        
        let result = task.execute(args).await.unwrap();
        
        assert_eq!(result["sum"], 15);
        assert_eq!(result["count"], 5);
        assert_eq!(result["average"], 3.0);
    }
}
```

### Unit Testing Trait-Based Tasks

For tasks implementing the `Task` trait directly with JSON arguments, use the `execute_json` method:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_database_task() {
        let task = DatabaseTask::new("mock://database").await.unwrap();
        // Use typed arguments matching the Task::Args definition
        let args = ("SELECT * FROM users".to_string(), Some(50i64));
        
        let result = task.execute(args).await.unwrap();
        
        assert_eq!(result["query"], "SELECT * FROM users");
        assert_eq!(result["limit"], 50);
    }
    
    #[tokio::test]
    async fn test_database_task_with_json() {
        let task = DatabaseTask::new("mock://database").await.unwrap();
        // Alternative: test with JSON args (simulates worker execution)
        let args = vec![json!("SELECT * FROM users"), json!(50)];
        
        let result = task.execute_json(args, serde_json::Value::Null).await.unwrap();
        
        assert_eq!(result["query"], "SELECT * FROM users");
        assert_eq!(result["limit"], 50);
    }
}
```

**Understanding the Task System:**

The Rust client supports two task definition approaches:

1. **Proc Macro Tasks** (`#[azolla_task]`):
   - Generated structs implement `Task` trait with **inferred typed arguments**
   - Use `task.execute(typed_args)` for testing
   - Type-safe at compile time
   - Recommended for new development

2. **Trait-Based Tasks** (manual `Task` implementation):
   - Define `type Args` explicitly (e.g., `(String, Option<i64>)`)
   - Use `task.execute(typed_args)` for testing with typed args
   - Use `task.execute_json(json_args, serde_json::Value::Null)` to test JSON parsing (simulates worker)
   - Flexible for complex state management and custom argument parsing
   - Required for tasks with initialization logic

### Integration Testing with Test Client

Integration tests verify the full task lifecycle through the Azolla orchestrator:

```rust
#[tokio::test]
async fn test_full_task_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    // This requires a running Azolla orchestrator for integration tests
    let client = azolla_client::Client::connect("http://localhost:52710").await?;
    
    let task = client
        .submit_task("greet_user")
        .args(("Test User".to_string(), 99u32))?
        .submit()
        .await?;
    
    match task.wait().await? {
        azolla_client::TaskExecutionResult::Success(result) => {
            assert!(result["greeting"].as_str().unwrap().contains("Test User"));
            assert!(result["greeting"].as_str().unwrap().contains("99"));
        },
        azolla_client::TaskExecutionResult::Failed(error) => {
            panic!("Task should not have failed: {}", error);
        }
    }
    
    Ok(())
}
```

## Deployment Patterns

### Separate Client and Worker Processes with Shared Tasks

This pattern separates client and worker into different processes while sharing task definitions. This allows the client process to call tasks directly for testing or code reuse.

**Shared Task Library (`src/tasks.rs`):**

```rust
// Shared task definitions that both client and worker can use
use azolla_client::{azolla_task, TaskError};
use serde_json::{json, Value};

#[azolla_task]
pub async fn process_order(order_id: String, priority: Option<String>) -> Result<Value, TaskError> {
    let priority = priority.unwrap_or_else(|| "normal".to_string());
    
    // Simulate order processing
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    
    if order_id.is_empty() {
        return Err(TaskError::invalid_args("Order ID cannot be empty"));
    }
    
    Ok(json!({
        "order_id": order_id,
        "status": "processed",
        "priority": priority,
        "processed_at": chrono::Utc::now().to_rfc3339()
    }))
}

#[azolla_task]
pub async fn send_email(recipient: String, subject: String, body: String) -> Result<Value, TaskError> {
    // Simulate email sending
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    
    if !recipient.contains('@') {
        return Err(TaskError::invalid_args("Invalid email address"));
    }
    
    Ok(json!({
        "message_id": uuid::Uuid::new_v4().to_string(),
        "recipient": recipient,
        "subject": subject,
        "sent_at": chrono::Utc::now().to_rfc3339()
    }))
}
```

**Worker Process (`src/bin/worker.rs`):**

The worker binary now follows the single-invocation pattern described earlier. Parse the CLI arguments provided by the shepherd, build your `Worker`, create a `WorkerInvocation`, and call `worker.run_invocation(invocation).await`. See the "Composing Worker Processes" section for a complete example.

**Client Process (`src/bin/client.rs`):**

```rust
use azolla_client::{Client, TaskExecutionResult};
use my_app::tasks::{process_order, send_email, ProcessOrderTask, SendEmailTask};  // Import both functions and structs
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = azolla_client::Client::connect("http://localhost:52710").await?;
    
    // Option 1: Submit task to remote worker
    println!("📤 Submitting task to remote worker...");
    let remote_task = client
        .submit_task("process_order")
        .args(("ORD-12345".to_string(), Some("high".to_string())))?
        .submit()
        .await?;
    
    match remote_task.wait().await? {
        TaskExecutionResult::Success(result) => {
            println!("✅ Remote task completed: {}", result);
        },
        TaskExecutionResult::Failed(error) => {
            eprintln!("❌ Remote task failed: {}", error);
        }
    }
    
    // Option 2: Call task directly for testing or immediate execution
    println!("🔄 Calling task directly for testing...");
    match process_order("ORD-67890".to_string(), Some("urgent".to_string())).await {
        Ok(result) => {
            println!("✅ Direct task call succeeded: {}", result);
        },
        Err(error) => {
            eprintln!("❌ Direct task call failed: {}", error);
        }
    }
    
    // Option 3: Use task struct for testing with JSON args (matches worker execution)
    println!("🧪 Testing task struct with JSON args...");
    let task_instance = ProcessOrderTask;
    let args = vec![json!("ORD-11111"), json!("low")];
    
    match task_instance.execute_json(args, serde_json::Value::Null).await {
        Ok(result) => {
            println!("✅ Task struct execution succeeded: {}", result);
        },
        Err(error) => {
            eprintln!("❌ Task struct execution failed: {}", error);
        }
    }
    
    Ok(())
}
```

**Project Structure:**

```
my-app/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Re-exports tasks module
│   ├── tasks.rs            # Shared task definitions
│   └── bin/
│       ├── worker.rs       # Worker process
│       └── client.rs       # Client process
```

**Cargo.toml:**

```toml
[package]
name = "my-app"
version = "0.1.0"
edition = "2021"

[dependencies]
azolla = { path = "../azolla" }
azolla-client = { path = "../clients/rust/azolla-client" }
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
uuid = { version = "1.0", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }

[[bin]]
name = "worker"
path = "src/bin/worker.rs"

[[bin]]
name = "client"
path = "src/bin/client.rs"
```

**Running the Applications:**

```bash
# Terminal 1: Start worker
cargo run --bin worker

# Terminal 2: Run client
cargo run --bin client

# Or test tasks directly
cargo test
```

**Key Benefits:**

1. **Separation of Concerns**: Client and worker are separate processes
2. **Code Reuse**: Tasks can be called directly without going through Azolla
3. **Testing Flexibility**: Three ways to test tasks:
   - Direct function calls
   - Task struct with JSON args (matches worker)
   - Full integration through Azolla
4. **Production Ready**: Worker can be deployed independently
5. **Development Friendly**: Client can test tasks locally without worker running

### Scaling with Multiple Workers and Worker Groups

For production deployments you typically run multiple shepherd instances, each configured with a dedicated worker group (for example `gpu-workers`, `cpu-workers`, or `highmem-workers`). The shepherd controls concurrency and launches your worker binary with the invocation metadata. Your worker simply registers tasks and calls `run_invocation` using the CLI arguments it receives.

To route work to a specific pool from the client, continue to call `.shepherd_group("gpu-workers")` when submitting tasks. The orchestrator forwards the invocation to a shepherd serving that group, which then spawns your worker binary for a single-task execution.

**Client Targeting Specific Worker Groups:**

```rust
// Client that routes tasks to appropriate worker groups
use azolla_client::{Client, TaskExecutionResult, RetryPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), azolla_client::AzollaError> {
    let client = Client::connect("http://localhost:52710").await?;
    
    // Route GPU-intensive task to GPU workers
    let gpu_task = client
        .submit_task("train_model")
        .args(("/data/training_set.csv".to_string(), "cnn_config.json".to_string()))?
        .shepherd_group("gpu-workers")  // Target GPU workers specifically
        .retry_policy(RetryPolicy::exponential().max_attempts(3).build())
        .submit()
        .await?;
    
    println!("🖥️  Submitted GPU task: {}", gpu_task.id());
    
    // Route memory-intensive task to high-memory workers
    let memory_task = client
        .submit_task("process_large_dataset")
        .args(("/data/large_file.csv".to_string(), 512u64))?
        .shepherd_group("highmem-workers")  // Target high-memory workers
        .submit()
        .await?;
    
    println!("💾 Submitted memory task: {}", memory_task.id());
    
    // Route lightweight task to general CPU workers
    let cpu_task = client
        .submit_task("send_email")
        .args((
            "user@example.com".to_string(),
            "Task Completed".to_string(),
            "Your data processing is complete!".to_string()
        ))?
        .shepherd_group("cpu-workers")  // Target CPU workers
        .submit()
        .await?;
    
    println!("⚡ Submitted CPU task: {}", cpu_task.id());
    
    // Tasks can also be submitted without specifying shepherd group
    // They will be routed to any available worker that has the task registered
    let general_task = client
        .submit_task("process_webhook")
        .args(("{\"event\": \"user_signup\"}".to_string(), "user".to_string()))?
        // No .shepherd_group() - will go to any available worker
        .submit()
        .await?;
    
    println!("🔄 Submitted general task: {}", general_task.id());
    
    println!("✅ All tasks submitted successfully!");
    Ok(())
}
```

**Production Deployment Script:**

```bash
#!/bin/bash
# deploy_workers.sh - Deploy multiple specialized worker groups

echo "🚀 Deploying Azolla workers..."

# Deploy 2 GPU workers (expensive resources, fewer instances)
for i in {1..2}; do
    echo "Starting GPU worker $i..."
    RUST_LOG=info cargo run --bin gpu_worker &
done

# Deploy 4 high-memory workers (moderate resources)
for i in {1..4}; do
    echo "Starting high-memory worker $i..."
    RUST_LOG=info cargo run --bin highmem_worker &
done

# Deploy 8 CPU workers (cheap resources, more instances)
for i in {1..8}; do
    echo "Starting CPU worker $i..."
    RUST_LOG=info cargo run --bin cpu_worker &
done

echo "✅ Deployed 14 workers across 3 specialized groups:"
echo "   - 2 GPU workers (gpu-workers group)"
echo "   - 4 High-memory workers (highmem-workers group)" 
echo "   - 8 CPU workers (cpu-workers group)"
echo ""
echo "Workers will register with orchestrator and start processing tasks."
```

**Docker Compose for Multi-Worker Deployment:**

```yaml
# docker-compose-workers.yml
version: '3.8'
services:
  # GPU workers (limited replicas due to GPU constraints)
  gpu-worker:
    build: .
    command: ["cargo", "run", "--bin", "gpu_worker"]
    deploy:
      replicas: 2
    environment:
      - AZOLLA_ORCHESTRATOR=orchestrator:52710
      - AZOLLA_DOMAIN=production
      - AZOLLA_SHEPHERD_GROUP=gpu-workers
      - RUST_LOG=info
    # GPU resource allocation would go here in real deployment
    
  # High-memory workers  
  highmem-worker:
    build: .
    command: ["cargo", "run", "--bin", "highmem_worker"]
    deploy:
      replicas: 4
      resources:
        limits:
          memory: 16G
        reservations:
          memory: 8G
    environment:
      - AZOLLA_ORCHESTRATOR=orchestrator:52710
      - AZOLLA_DOMAIN=production
      - AZOLLA_SHEPHERD_GROUP=highmem-workers
      - RUST_LOG=info
      
  # CPU workers (most replicas for general tasks)
  cpu-worker:
    build: .
    command: ["cargo", "run", "--bin", "cpu_worker"]
    deploy:
      replicas: 8
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          cpus: '1.0'
          memory: 1G
    environment:
      - AZOLLA_ORCHESTRATOR=orchestrator:52710
      - AZOLLA_DOMAIN=production
      - AZOLLA_SHEPHERD_GROUP=cpu-workers
      - RUST_LOG=info
```

## Running Examples

The azolla-client crate includes example applications demonstrating different usage patterns:

### Basic Usage Example

Shows fundamental client operations:

```bash
cargo run --example basic_client_example
```

### Proc Macro Tasks Example

Demonstrates type-safe task definitions with the `#[azolla_task]` macro:

```bash
cargo run --example proc_macro_tasks
```

**Note**: All examples are now available from the repo root (see `examples/rust/`) and include proc macro functionality such as `basic_worker_example` for worker lifecycles.

These examples provide practical starting points for building your own Azolla applications.

You now have everything needed to build production Azolla applications with type-safe tasks and robust error handling.
