//! Unit tests for task module functionality
//! Tests the Task trait, argument parsing, and BoxedTask execution

use azolla_client::error::TaskError;
use azolla_client::task::{BoxedTask, Task};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

// Mock task for testing
struct MockTask;

impl Task for MockTask {
    type Args = i32;

    fn name(&self) -> &'static str {
        "mock_task"
    }

    fn execute(
        &self,
        args: Self::Args,
    ) -> Pin<Box<dyn Future<Output = Result<Value, TaskError>> + Send + '_>> {
        Box::pin(async move { Ok(json!({"result": args * 2})) })
    }
}

// Mock task with multiple arguments for testing
struct MultiArgMockTask;

impl Task for MultiArgMockTask {
    type Args = (i32, String);

    fn name(&self) -> &'static str {
        "multi_arg_task"
    }

    fn execute(
        &self,
        args: Self::Args,
    ) -> Pin<Box<dyn Future<Output = Result<Value, TaskError>> + Send + '_>> {
        Box::pin(async move {
            let (num, text) = args;
            Ok(json!({"number": num, "text": text}))
        })
    }
}

// Mock task with no arguments
struct NoArgMockTask;

impl Task for NoArgMockTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "no_arg_task"
    }

    fn execute(
        &self,
        _args: Self::Args,
    ) -> Pin<Box<dyn Future<Output = Result<Value, TaskError>> + Send + '_>> {
        Box::pin(async move { Ok(json!({"status": "executed"})) })
    }
}

// Mock task that fails
struct FailingTask;

impl Task for FailingTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "failing_task"
    }

    fn execute(
        &self,
        _args: Self::Args,
    ) -> Pin<Box<dyn Future<Output = Result<Value, TaskError>> + Send + '_>> {
        Box::pin(async move { Err(TaskError::execution_failed("Task failed intentionally")) })
    }
}

/// Test the purpose of Task::name method
/// Expected behavior: should return the task name as a static string
#[test]
fn test_task_name() {
    let task = MockTask;
    assert_eq!(Task::name(&task), "mock_task");

    let multi_task = MultiArgMockTask;
    assert_eq!(Task::name(&multi_task), "multi_arg_task");

    let no_arg_task = NoArgMockTask;
    assert_eq!(Task::name(&no_arg_task), "no_arg_task");
}

/// Test the expected behavior: Task execution with success
#[tokio::test]
async fn test_task_execution_success() {
    let task = MockTask;
    let result = task.execute(42).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["result"], 84); // 42 * 2
}

/// Test the purpose of Task execution with failure
/// Expected behavior: should return TaskError when task fails
#[tokio::test]
async fn test_task_execution_failure() {
    let task = FailingTask;
    let result = task.execute(()).await;

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.error_type, "ExecutionError");
    assert!(error.message.contains("failed intentionally"));
}

/// Test the expected behavior: Task execution with no arguments
#[tokio::test]
async fn test_task_execution_no_args() {
    let task = NoArgMockTask;
    let result = task.execute(()).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["status"], "executed");
}

/// Test the purpose of Task execution with multiple arguments
/// Expected behavior: should handle tuple arguments correctly
#[tokio::test]
async fn test_task_execution_multiple_args() {
    let task = MultiArgMockTask;
    let result = task.execute((123, "test".to_string())).await;

    assert!(result.is_ok());
    let value = result.unwrap();
    assert_eq!(value["number"], 123);
    assert_eq!(value["text"], "test");
}

/// Test the expected behavior: parse_args with single argument
#[test]
fn test_parse_args_single() {
    let args = vec![json!(42)];
    let parsed = MockTask::parse_args(args, Value::Null).unwrap();
    assert_eq!(parsed, 42);
}

/// Test the purpose of parse_args with multiple arguments (tuple)
/// Expected behavior: should parse JSON array into tuple
#[test]
fn test_parse_args_multiple() {
    let args = vec![json!(123), json!("hello")];
    let parsed = MultiArgMockTask::parse_args(args, Value::Null).unwrap();
    assert_eq!(parsed.0, 123);
    assert_eq!(parsed.1, "hello");
}

/// Test the expected behavior: parse_args with no arguments
#[test]
fn test_parse_args_no_args() {
    let args = vec![];
    NoArgMockTask::parse_args(args, Value::Null).unwrap();
    // Successfully parsing empty args for no-arg task
}

/// Test the purpose of parse_args with invalid single argument
/// Expected behavior: should return error for type mismatch
#[test]
fn test_parse_args_invalid_single() {
    let args = vec![json!("not_a_number")];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.error_type, "InvalidArguments");
}

/// Test the expected behavior: parse_args with invalid multiple arguments
#[test]
fn test_parse_args_invalid_multiple() {
    let args = vec![json!("not_a_number"), json!(123)]; // Wrong types
    let result = MultiArgMockTask::parse_args(args, Value::Null);
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.error_type, "InvalidArguments");
}

/// Test the purpose of parse_args with no args for required arguments
/// Expected behavior: should return error when arguments are required but not provided
#[test]
fn test_parse_args_no_args_for_required() {
    let args = vec![];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.error_type, "InvalidArguments");
    assert!(error.message.contains("No arguments provided"));
}

/// Test the expected behavior: parse_args with empty argument list edge cases
#[test]
fn test_parse_args_empty() {
    // Test with unit type should succeed with empty args
    let args = vec![];
    let result = NoArgMockTask::parse_args(args, Value::Null);
    assert!(result.is_ok());

    // Test with non-unit type should fail with empty args
    let args = vec![];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());
}

/// Test the purpose of parse_args edge cases
/// Expected behavior: should handle various edge cases gracefully
#[test]
fn test_parse_args_edge_cases() {
    // Test with null values
    let args = vec![json!(null)];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());

    // Test with complex object
    let args = vec![json!({"key": "value"})];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());

    // Test with array
    let args = vec![json!([1, 2, 3])];
    let result = MockTask::parse_args(args, Value::Null);
    assert!(result.is_err());
}

/// Test the expected behavior: BoxedTask name access
#[test]
fn test_boxed_task_name() {
    use std::sync::Arc;
    let task: Arc<dyn BoxedTask> = Arc::new(MockTask);
    assert_eq!(task.name(), "mock_task");
}

/// Test the purpose of BoxedTask execution with success
/// Expected behavior: should execute and return JSON result
#[tokio::test]
async fn test_boxed_task_execute_json_success() {
    use std::sync::Arc;
    let task: Arc<dyn BoxedTask> = Arc::new(MockTask);
    let args = vec![json!(21)];

    let result = task.execute_json(args, Value::Null).await;
    assert!(result.is_ok());

    let value = result.unwrap();
    assert_eq!(value["result"], 42); // 21 * 2
}

/// Test the expected behavior: BoxedTask execution with failure
#[tokio::test]
async fn test_boxed_task_execute_json_failure() {
    use std::sync::Arc;
    let task: Arc<dyn BoxedTask> = Arc::new(FailingTask);
    let args = vec![];

    let result = task.execute_json(args, Value::Null).await;
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.error_type, "ExecutionError");
}

/// Test the purpose of BoxedTask execution with invalid arguments
/// Expected behavior: should return error for argument parsing failure
#[tokio::test]
async fn test_boxed_task_execute_json_invalid_args() {
    use std::sync::Arc;
    let task: Arc<dyn BoxedTask> = Arc::new(MockTask);
    let args = vec![json!("invalid")];

    let result = task.execute_json(args, Value::Null).await;
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.error_type, "InvalidArguments");
}

/// Test the expected behavior: BoxedTask execution with no arguments
#[tokio::test]
async fn test_boxed_task_execute_json_no_args() {
    use std::sync::Arc;
    let task: Arc<dyn BoxedTask> = Arc::new(NoArgMockTask);
    let args = vec![];

    let result = task.execute_json(args, Value::Null).await;
    assert!(result.is_ok());

    let value = result.unwrap();
    assert_eq!(value["status"], "executed");
}
