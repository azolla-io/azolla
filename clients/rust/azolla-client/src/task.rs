use crate::error::TaskError;
use serde::Deserialize;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

pub type TaskResult = Result<Value, TaskError>;

/// Type-safe task trait with associated argument types
pub trait Task: Send + Sync {
    /// The type of arguments this task expects
    type Args: for<'de> Deserialize<'de> + Send + Sync;

    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task with typed arguments
    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;

    /// Parse positional and keyword arguments into typed arguments (can be overridden for custom parsing)
    #[allow(clippy::result_large_err)]
    fn parse_args(args: Vec<Value>, kwargs: Value) -> Result<Self::Args, TaskError> {
        match args.len() {
            0 => {
                let mut kwargs_error = None;

                if !matches!(kwargs, Value::Null) {
                    let should_attempt = match &kwargs {
                        Value::Object(map) => !map.is_empty(),
                        _ => true,
                    };

                    if should_attempt {
                        match serde_json::from_value(kwargs.clone()) {
                            Ok(value) => return Ok(value),
                            Err(err) => kwargs_error = Some(err),
                        }
                    }
                }

                serde_json::from_value(json!(null))
                    .or_else(|_| serde_json::from_value(json!({})))
                    .map_err(|fallback_err| {
                        if let Some(kwargs_err) = kwargs_error {
                            TaskError::invalid_args(&format!(
                                "Failed to parse kwargs: {kwargs_err}; fallback parse failed: {fallback_err}"
                            ))
                        } else {
                            TaskError::invalid_args(&format!(
                                "No arguments provided and type doesn't support empty: {fallback_err}"
                            ))
                        }
                    })
            }
            1 => serde_json::from_value(args[0].clone()).map_err(|e| {
                TaskError::invalid_args(&format!("Failed to parse single argument: {e}"))
            }),
            _ => serde_json::from_value(json!(args)).map_err(|e| {
                TaskError::invalid_args(&format!("Failed to parse multiple arguments: {e}"))
            }),
        }
    }
}

/// Type-erased task wrapper for dynamic dispatch
pub trait BoxedTask: Send + Sync {
    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task with JSON positional arguments and kwargs
    fn execute_json(
        &self,
        args: Vec<Value>,
        kwargs: Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;
}

/// Implementation of BoxedTask for any Task
impl<T: Task + 'static> BoxedTask for T {
    fn name(&self) -> &'static str {
        Task::name(self)
    }

    fn execute_json(
        &self,
        args: Vec<Value>,
        kwargs: Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let typed_args = T::parse_args(args, kwargs)?;
            let future = self.execute(typed_args);
            future.await
        })
    }
}

/// Result of task execution
#[derive(Debug, Clone)]
pub enum TaskExecutionResult {
    Success(Value),
    Failed(TaskError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TaskError;
    use serde_json::json;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    // Mock task implementations for testing
    struct SimpleTask;
    struct NoArgsTask;
    struct MultiArgsTask;
    struct FailingTask;

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct SimpleArgs {
        value: i32,
    }

    impl Task for SimpleTask {
        type Args = SimpleArgs;

        fn name(&self) -> &'static str {
            "simple_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                Ok(json!({
                    "result": args.value * 2,
                    "task": "simple_task"
                }))
            })
        }
    }

    impl Task for NoArgsTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "no_args_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                Ok(json!({
                    "result": "success",
                    "timestamp": "2023-01-01T00:00:00Z"
                }))
            })
        }
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct MultiArgs(String, i32, bool);

    impl Task for MultiArgsTask {
        type Args = MultiArgs;

        fn name(&self) -> &'static str {
            "multi_args_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                Ok(json!({
                    "result": format!("{}-{}-{}", args.0, args.1, args.2),
                    "task": "multi_args_task"
                }))
            })
        }
    }

    impl Task for FailingTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "failing_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Err(TaskError::execution_failed("Task intentionally failed")) })
        }
    }

    /// Test parse_args with empty arguments for unit type
    #[test]
    fn test_parse_args_empty() {
        let empty_args = vec![];
        let result = NoArgsTask::parse_args(empty_args, Value::Null);

        assert!(result.is_ok());
        result.unwrap(); // Just ensure it returns without error
    }

    /// Test parse_args with single argument
    #[test]
    fn test_parse_args_single() {
        let single_args = vec![json!({"value": 42})];
        let result = SimpleTask::parse_args(single_args, Value::Null);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.value, 42);
    }

    /// Test parse_args with multiple arguments
    #[test]
    fn test_parse_args_multiple() {
        let multi_args = vec![json!("hello"), json!(123), json!(true)];
        let result = MultiArgsTask::parse_args(multi_args, Value::Null);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.0, "hello");
        assert_eq!(parsed.1, 123);
        assert!(parsed.2);
    }

    /// Test parse_args fallback to kwargs for structured types
    #[test]
    fn test_parse_args_from_kwargs() {
        let kwargs = json!({"value": 7});
        let result = SimpleTask::parse_args(vec![], kwargs);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.value, 7);
    }

    /// Test parse_args with invalid single argument
    #[test]
    fn test_parse_args_invalid_single() {
        let invalid_args = vec![json!("not an object")];
        let result = SimpleTask::parse_args(invalid_args, Value::Null);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains("Failed to parse single argument"));
    }

    /// Test parse_args with invalid multiple arguments
    #[test]
    fn test_parse_args_invalid_multiple() {
        let invalid_args = vec![json!("wrong"), json!("types"), json!("here")];
        let result = SimpleTask::parse_args(invalid_args, Value::Null);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains("Failed to parse multiple arguments"));
    }

    /// Test parse_args with no arguments for type that requires args
    #[test]
    fn test_parse_args_no_args_for_required() {
        let no_args = vec![];
        let result = SimpleTask::parse_args(no_args, Value::Null);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains("No arguments provided"));
    }

    /// Test Task trait name method
    #[test]
    fn test_task_name() {
        let simple_task = SimpleTask;
        assert_eq!(Task::name(&simple_task), "simple_task");

        let no_args_task = NoArgsTask;
        assert_eq!(Task::name(&no_args_task), "no_args_task");

        let multi_args_task = MultiArgsTask;
        assert_eq!(Task::name(&multi_args_task), "multi_args_task");
    }

    /// Test Task execution with valid arguments
    #[tokio::test]
    async fn test_task_execution_success() {
        let task = SimpleTask;
        let args = SimpleArgs { value: 21 };

        let result = task.execute(args).await;
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output["result"], 42);
        assert_eq!(output["task"], "simple_task");
    }

    /// Test Task execution with no arguments
    #[tokio::test]
    async fn test_task_execution_no_args() {
        let task = NoArgsTask;
        let args = ();

        let result = task.execute(args).await;
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output["result"], "success");
    }

    /// Test Task execution with multiple arguments
    #[tokio::test]
    async fn test_task_execution_multiple_args() {
        let task = MultiArgsTask;
        let args = MultiArgs("test".to_string(), 42, true);

        let result = task.execute(args).await;
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output["result"], "test-42-true");
    }

    /// Test Task execution failure
    #[tokio::test]
    async fn test_task_execution_failure() {
        let task = FailingTask;
        let args = ();

        let result = task.execute(args).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_type, "ExecutionError");
        assert!(error.message.contains("Task intentionally failed"));
    }

    /// Test BoxedTask trait object functionality
    #[test]
    fn test_boxed_task_name() {
        let simple_task: Arc<dyn BoxedTask> = Arc::new(SimpleTask);
        assert_eq!(simple_task.name(), "simple_task");

        let no_args_task: Arc<dyn BoxedTask> = Arc::new(NoArgsTask);
        assert_eq!(no_args_task.name(), "no_args_task");
    }

    /// Test BoxedTask execute_json with valid arguments
    #[tokio::test]
    async fn test_boxed_task_execute_json_success() {
        let task: Arc<dyn BoxedTask> = Arc::new(SimpleTask);
        let args = vec![json!({"value": 15})];

        let result = task.execute_json(args, Value::Null).await;
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output["result"], 30);
    }

    /// Test BoxedTask execute_json with no arguments
    #[tokio::test]
    async fn test_boxed_task_execute_json_no_args() {
        let task: Arc<dyn BoxedTask> = Arc::new(NoArgsTask);
        let args = vec![];

        let result = task.execute_json(args, Value::Null).await;
        assert!(result.is_ok());

        let output = result.unwrap();
        assert_eq!(output["result"], "success");
    }

    /// Test BoxedTask execute_json with invalid arguments
    #[tokio::test]
    async fn test_boxed_task_execute_json_invalid_args() {
        let task: Arc<dyn BoxedTask> = Arc::new(SimpleTask);
        let args = vec![json!("invalid")];

        let result = task.execute_json(args, Value::Null).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
    }

    /// Test BoxedTask execute_json with failing task
    #[tokio::test]
    async fn test_boxed_task_execute_json_failure() {
        let task: Arc<dyn BoxedTask> = Arc::new(FailingTask);
        let args = vec![];

        let result = task.execute_json(args, Value::Null).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_type, "ExecutionError");
        assert!(error.message.contains("Task intentionally failed"));
    }

    /// Test TaskExecutionResult enum variants
    #[test]
    fn test_task_execution_result_variants() {
        let success_result = TaskExecutionResult::Success(json!({"status": "ok"}));
        let failed_result = TaskExecutionResult::Failed(TaskError::execution_failed("test error"));

        match success_result {
            TaskExecutionResult::Success(value) => {
                assert_eq!(value["status"], "ok");
            }
            TaskExecutionResult::Failed(_) => panic!("Expected success result"),
        }

        match failed_result {
            TaskExecutionResult::Failed(error) => {
                assert!(error.message.contains("test error"));
                assert_eq!(error.error_type, "ExecutionError");
            }
            TaskExecutionResult::Success(_) => panic!("Expected failed result"),
        }
    }

    /// Test TaskExecutionResult clone functionality
    #[test]
    fn test_task_execution_result_clone() {
        let original_success = TaskExecutionResult::Success(json!({"data": "test"}));
        let cloned_success = original_success.clone();

        match (original_success, cloned_success) {
            (TaskExecutionResult::Success(orig), TaskExecutionResult::Success(cloned)) => {
                assert_eq!(orig, cloned);
            }
            _ => panic!("Expected both to be success results"),
        }

        let original_failed = TaskExecutionResult::Failed(TaskError::invalid_args("test"));
        let cloned_failed = original_failed.clone();

        match (original_failed, cloned_failed) {
            (TaskExecutionResult::Failed(orig), TaskExecutionResult::Failed(cloned)) => {
                assert_eq!(orig.message, cloned.message);
                assert_eq!(orig.error_type, cloned.error_type);
            }
            _ => panic!("Expected both to be failed results"),
        }
    }

    /// Test complex argument parsing edge cases
    #[test]
    fn test_parse_args_edge_cases() {
        // Test with null value
        let null_args = vec![json!(null)];
        let result = NoArgsTask::parse_args(null_args, Value::Null);
        assert!(result.is_ok());

        // Test with empty object for unit type
        let empty_object_args = vec![];
        let result = NoArgsTask::parse_args(empty_object_args, Value::Null);
        assert!(result.is_ok());

        // Test with mixed valid/invalid arguments in array
        let mixed_args = vec![json!("valid"), json!(123), json!("invalid-for-bool")];
        let result = MultiArgsTask::parse_args(mixed_args, Value::Null);
        assert!(result.is_err());
    }
}
