//! Integration tests for procedural macro functionality
//! Tests the azolla_task macro in real usage scenarios

#[cfg(feature = "macros")]
use azolla_client::task::BoxedTask;
use azolla_client::task::{Task, TaskResult};
use azolla_client::worker::Worker;
use serde_json::json;

#[cfg(feature = "macros")]
use azolla_client::error::TaskError;
#[cfg(feature = "macros")]
use serde_json::Value;
#[cfg(feature = "macros")]
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use super::TestOrchestrator;

// Define tasks using the procedural macro when available
#[cfg(feature = "macros")]
use azolla_client::azolla_task;

#[cfg(feature = "macros")]
#[azolla_task]
async fn simple_add_task(a: i32, b: i32) -> Result<Value, TaskError> {
    Ok(json!({"sum": a + b, "operation": "addition"}))
}

#[cfg(feature = "macros")]
#[azolla_task]
async fn string_processing_task(text: String, uppercase: bool) -> Result<Value, TaskError> {
    let processed = if uppercase {
        text.to_uppercase()
    } else {
        text.to_lowercase()
    };
    Ok(json!({"processed_text": processed, "original_length": text.len()}))
}

#[cfg(feature = "macros")]
#[azolla_task]
async fn complex_data_task(data: HashMap<String, Value>) -> Result<Value, TaskError> {
    let keys: Vec<String> = data.keys().cloned().collect();
    let value_count = data.len();

    Ok(json!({
        "keys": keys,
        "count": value_count,
        "has_nested": data.values().any(|v| v.is_object())
    }))
}

#[cfg(feature = "macros")]
#[azolla_task]
async fn failing_macro_task(should_fail: bool) -> Result<Value, TaskError> {
    if should_fail {
        Err(TaskError::execution_failed("Task was configured to fail"))
    } else {
        Ok(json!({"status": "success", "message": "Task completed successfully"}))
    }
}

#[cfg(feature = "macros")]
#[azolla_task]
async fn validation_macro_task(email: String, age: u32) -> Result<Value, TaskError> {
    if !email.contains('@') {
        return Err(TaskError::invalid_args("Email must contain @ symbol"));
    }

    if age > 120 {
        return Err(TaskError::invalid_args("Age seems unrealistic"));
    }

    Ok(json!({
        "validated": true,
        "email_domain": email.split('@').nth(1).unwrap_or("unknown"),
        "age_category": if age < 18 { "minor" } else { "adult" }
    }))
}

// Manual task implementations for testing when macros are not available
struct ManualAddTask;

impl Task for ManualAddTask {
    type Args = (i32, i32);

    fn name(&self) -> &'static str {
        "manual_add_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let (a, b) = args;
            Ok(json!({"sum": a + b, "operation": "addition"}))
        })
    }
}

struct ManualStringTask;

impl Task for ManualStringTask {
    type Args = (String, bool);

    fn name(&self) -> &'static str {
        "manual_string_task"
    }

    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let (text, uppercase) = args;
            let processed = if uppercase {
                text.to_uppercase()
            } else {
                text.to_lowercase()
            };
            Ok(json!({"processed_text": processed, "original_length": text.len()}))
        })
    }
}

/// Test basic macro-generated task functionality
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_generated_task_execution() {
    let task = SimpleAddTaskTask;
    let result = task.execute((10, 20)).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["sum"], 30);
    assert_eq!(value["operation"], "addition");
}

/// Test macro-generated task with string processing
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_string_processing() {
    let task = StringProcessingTaskTask;

    // Test uppercase processing
    let result = task.execute(("hello world".to_string(), true)).await;
    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["processed_text"], "HELLO WORLD");
    assert_eq!(value["original_length"], 11);

    // Test lowercase processing
    let result = task.execute(("Hello World".to_string(), false)).await;
    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["processed_text"], "hello world");
}

/// Test macro-generated task with complex data structures
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_complex_data_handling() {
    let task = ComplexDataTaskTask;

    let mut test_data = HashMap::new();
    test_data.insert("key1".to_string(), json!("value1"));
    test_data.insert("key2".to_string(), json!(42));
    test_data.insert("key3".to_string(), json!({"nested": "object"}));

    let result = task.execute(test_data).await;
    assert!(result.is_ok());

    let value = result.unwrap();
    assert_eq!(value["count"], 3);
    assert_eq!(value["has_nested"], true);

    let keys = value["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 3);
}

/// Test macro-generated task error handling
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_error_handling() {
    let task = FailingMacroTaskTask;

    // Test successful execution
    let success_result = task.execute(false).await;
    assert!(success_result.is_ok());
    assert_eq!(success_result.unwrap()["status"], "success");

    // Test error case
    let error_result = task.execute(true).await;
    assert!(error_result.is_err());

    let error = error_result.unwrap_err();
    assert_eq!(error.error_type, "ExecutionError");
    assert!(error.message.contains("configured to fail"));
}

/// Test macro-generated task with validation
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_validation_task() {
    let task = ValidationMacroTaskTask;

    // Test valid input
    let valid_result = task.execute(("user@example.com".to_string(), 25)).await;
    assert!(valid_result.is_ok());

    let value = valid_result.unwrap();
    assert_eq!(value["validated"], true);
    assert_eq!(value["email_domain"], "example.com");
    assert_eq!(value["age_category"], "adult");

    // Test invalid email
    let invalid_email_result = task.execute(("invalid-email".to_string(), 25)).await;
    assert!(invalid_email_result.is_err());

    let email_error = invalid_email_result.unwrap_err();
    assert_eq!(email_error.error_type, "InvalidArguments");
    assert!(email_error.message.contains("@ symbol"));

    // Test invalid age
    let invalid_age_result = task.execute(("user@example.com".to_string(), 150)).await;
    assert!(invalid_age_result.is_err());

    let age_error = invalid_age_result.unwrap_err();
    assert_eq!(age_error.error_type, "InvalidArguments");
    assert!(age_error.message.contains("unrealistic"));
}

/// Test macro-generated task name generation
#[cfg(feature = "macros")]
#[test]
fn test_macro_task_name_generation() {
    let add_task = SimpleAddTaskTask;
    assert_eq!(Task::name(&add_task), "simple_add_task");

    let string_task = StringProcessingTaskTask;
    assert_eq!(Task::name(&string_task), "string_processing_task");

    let complex_task = ComplexDataTaskTask;
    assert_eq!(Task::name(&complex_task), "complex_data_task");
}

/// Test macro-generated task argument parsing
#[cfg(feature = "macros")]
#[test]
fn test_macro_argument_parsing() {
    // Test successful parsing
    let args = vec![json!(5), json!(15)];
    let parsed = SimpleAddTaskTask::parse_args(args).unwrap();
    assert_eq!(parsed.0, 5);
    assert_eq!(parsed.1, 15);

    // Test parsing error
    let invalid_args = vec![json!("not_a_number"), json!(15)];
    let parse_result = SimpleAddTaskTask::parse_args(invalid_args);
    assert!(parse_result.is_err());

    let parse_error = parse_result.unwrap_err();
    assert_eq!(parse_error.error_type, "InvalidArguments");
}

/// Test macro-generated task BoxedTask compatibility
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_boxed_task_compatibility() {
    use std::sync::Arc;

    let task: Arc<dyn BoxedTask> = Arc::new(SimpleAddTaskTask);
    assert_eq!(task.name(), "simple_add_task");

    let args = vec![json!(100), json!(200)];
    let result = task.execute_json(args).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["sum"], 300);
}

/// Test macro-generated tasks in worker integration
#[cfg(feature = "macros")]
#[tokio::test]
async fn test_macro_tasks_in_worker() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("macro-integration-test")
        .register_task(SimpleAddTaskTask)
        .register_task(StringProcessingTaskTask)
        .register_task(ComplexDataTaskTask)
        .build()
        .await
        .expect("Failed to build worker with macro tasks");

    // Verify all tasks were registered
    assert_eq!(worker.task_count(), 3);
}

/// Test manual task implementation as fallback
#[tokio::test]
async fn test_manual_task_implementation() {
    let task = ManualAddTask;
    let result = task.execute((25, 75)).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["sum"], 100);
    assert_eq!(value["operation"], "addition");
}

/// Test manual string processing task
#[tokio::test]
async fn test_manual_string_task() {
    let task = ManualStringTask;
    let result = task.execute(("Test String".to_string(), false)).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["processed_text"], "test string");
    assert_eq!(value["original_length"], 11);
}

/// Test no macros scenario
#[cfg(not(feature = "macros"))]
#[test]
fn test_no_macros_fallback() {
    // When macros are not available, manual implementations should work
    let task = ManualAddTask;
    assert_eq!(Task::name(&task), "manual_add_task");

    let string_task = ManualStringTask;
    assert_eq!(Task::name(&string_task), "manual_string_task");
}

/// Test macro and manual task integration
#[tokio::test]
async fn test_mixed_task_registration() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    #[cfg(feature = "macros")]
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("mixed-task-test")
        .register_task(ManualAddTask)
        .register_task(ManualStringTask)
        .register_task(SimpleAddTaskTask)
        .register_task(StringProcessingTaskTask)
        .build()
        .await
        .expect("Failed to build mixed worker");

    #[cfg(not(feature = "macros"))]
    let worker = Worker::builder()
        .orchestrator(&orchestrator.endpoint())
        .domain("mixed-task-test")
        .register_task(ManualAddTask)
        .register_task(ManualStringTask)
        .build()
        .await
        .expect("Failed to build mixed worker");

    // Verify task count (2 manual + 2 macro if available, or just 2 manual)
    #[cfg(feature = "macros")]
    assert_eq!(worker.task_count(), 4);

    #[cfg(not(feature = "macros"))]
    assert_eq!(worker.task_count(), 2);
}
