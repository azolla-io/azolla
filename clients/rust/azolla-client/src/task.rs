use crate::error::TaskError;
use serde::Deserialize;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

pub type TaskResult = Result<Value, TaskError>;

/// Task execution context
pub struct TaskContext {
    pub task_id: String,
    pub attempt_number: u32,
    pub max_attempts: Option<u32>,
}

/// Type-safe task trait with associated argument types
pub trait Task: Send + Sync {
    /// The type of arguments this task expects
    type Args: for<'de> Deserialize<'de> + Send + Sync;

    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task with typed arguments
    fn execute(&self, args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;

    /// Parse JSON arguments into typed arguments (can be overridden for custom parsing)
    #[allow(clippy::result_large_err)]
    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        match json_args.len() {
            0 => {
                // No arguments - try to deserialize from null or empty
                serde_json::from_value(json!(null))
                    .or_else(|_| serde_json::from_value(json!({})))
                    .map_err(|e| {
                        TaskError::invalid_args(&format!(
                            "No arguments provided and type doesn't support empty: {e}"
                        ))
                    })
            }
            1 => {
                // Single argument - deserialize directly
                serde_json::from_value(json_args[0].clone()).map_err(|e| {
                    TaskError::invalid_args(&format!("Failed to parse single argument: {e}"))
                })
            }
            _ => {
                // Multiple arguments - deserialize as array
                serde_json::from_value(json!(json_args)).map_err(|e| {
                    TaskError::invalid_args(&format!("Failed to parse multiple arguments: {e}"))
                })
            }
        }
    }
}

/// Type-erased task wrapper for dynamic dispatch
pub trait BoxedTask: Send + Sync {
    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task with JSON arguments
    fn execute_json(
        &self,
        args: Vec<Value>,
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
    ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            let typed_args = T::parse_args(args)?;
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
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Deserialize, Debug, PartialEq)]
    struct SimpleArgs {
        name: String,
        count: i32,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct TupleArgs(String, i32, bool);

    #[derive(Deserialize, Debug, PartialEq)]
    struct NoArgs;

    #[derive(Deserialize, Debug, PartialEq)]
    struct OptionalArgs {
        required: String,
        optional: Option<i32>,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct ComplexArgs {
        id: u64,
        metadata: HashMap<String, Value>,
        tags: Vec<String>,
    }

    // Mock task implementations for testing
    struct SimpleTask;
    struct TupleTask;
    struct NoArgsTask;
    struct OptionalTask;
    struct ComplexTask;
    struct ErrorTask;

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
                    "name": args.name,
                    "count": args.count,
                    "doubled": args.count * 2
                }))
            })
        }
    }

    impl Task for TupleTask {
        type Args = TupleArgs;

        fn name(&self) -> &'static str {
            "tuple_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!([args.0, args.1, args.2])) })
        }
    }

    impl Task for NoArgsTask {
        type Args = NoArgs;

        fn name(&self) -> &'static str {
            "no_args_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"status": "executed_without_args"})) })
        }
    }

    impl Task for OptionalTask {
        type Args = OptionalArgs;

        fn name(&self) -> &'static str {
            "optional_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                Ok(json!({
                    "required": args.required,
                    "optional": args.optional
                }))
            })
        }
    }

    impl Task for ComplexTask {
        type Args = ComplexArgs;

        fn name(&self) -> &'static str {
            "complex_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                Ok(json!({
                    "id": args.id,
                    "metadata_count": args.metadata.len(),
                    "tags": args.tags
                }))
            })
        }
    }

    impl Task for ErrorTask {
        type Args = SimpleArgs;

        fn name(&self) -> &'static str {
            "error_task"
        }

        fn execute(
            &self,
            args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move {
                if args.count < 0 {
                    Err(TaskError::invalid_args("Count cannot be negative"))
                } else if args.name.is_empty() {
                    Err(TaskError::execution_failed("Name cannot be empty"))
                } else {
                    Ok(json!({"status": "success"}))
                }
            })
        }
    }

    /// Test the purpose of Task::parse_args: ensure argument parsing handles single arguments correctly
    #[test]
    fn test_task_parse_args_single_argument() {
        let args = vec![json!({"name": "test", "count": 42})];
        let parsed: SimpleArgs = SimpleTask::parse_args(args).unwrap();
        assert_eq!(parsed.name, "test");
        assert_eq!(parsed.count, 42);
    }

    /// Test the expected behavior: parsing should fail gracefully with helpful errors
    #[test]
    fn test_task_parse_args_invalid_structure() {
        let args = vec![json!({"invalid": "structure"})];
        let result = SimpleTask::parse_args(args);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains("Failed to parse single argument"));
    }

    #[test]
    fn test_task_parse_args_multiple_arguments() {
        let args = vec![json!("hello"), json!(123), json!(true)];
        let parsed: TupleArgs = TupleTask::parse_args(args).unwrap();
        assert_eq!(parsed.0, "hello");
        assert_eq!(parsed.1, 123);
        assert!(parsed.2);
    }

    #[test]
    fn test_task_parse_args_no_arguments() {
        let args = vec![];
        let result: Result<NoArgs, TaskError> = NoArgsTask::parse_args(args);

        // Should either succeed with empty args or fail with descriptive error
        match result {
            Ok(_) => { /* Success case for unit types */ }
            Err(e) => {
                assert_eq!(e.error_type, "InvalidArguments");
                assert!(e.message.contains("No arguments provided"));
            }
        }
    }

    #[test]
    fn test_task_parse_args_optional_fields() {
        // Test with both fields provided
        let args_with_optional = vec![json!({"required": "test", "optional": 42})];
        let parsed: OptionalArgs = OptionalTask::parse_args(args_with_optional).unwrap();
        assert_eq!(parsed.required, "test");
        assert_eq!(parsed.optional, Some(42));

        // Test with only required field
        let args_without_optional = vec![json!({"required": "test"})];
        let parsed: OptionalArgs = OptionalTask::parse_args(args_without_optional).unwrap();
        assert_eq!(parsed.required, "test");
        assert_eq!(parsed.optional, None);

        // Test with null optional field
        let args_null_optional = vec![json!({"required": "test", "optional": null})];
        let parsed: OptionalArgs = OptionalTask::parse_args(args_null_optional).unwrap();
        assert_eq!(parsed.required, "test");
        assert_eq!(parsed.optional, None);
    }

    #[test]
    fn test_task_parse_args_complex_structure() {
        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), json!("1.0"));
        metadata.insert("priority".to_string(), json!(5));

        let args = vec![json!({
            "id": 12345u64,
            "metadata": metadata,
            "tags": ["urgent", "production"]
        })];

        let parsed: ComplexArgs = ComplexTask::parse_args(args).unwrap();
        assert_eq!(parsed.id, 12345);
        assert_eq!(parsed.metadata.len(), 2);
        assert_eq!(parsed.tags, vec!["urgent", "production"]);
    }

    #[test]
    fn test_task_parse_args_type_mismatch() {
        // Test wrong type for count field
        let args = vec![json!({"name": "test", "count": "not_a_number"})];
        let result = SimpleTask::parse_args(args);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.message.contains("Failed to parse single argument"));
    }

    #[test]
    fn test_task_parse_args_missing_required_field() {
        // Test missing required field
        let args = vec![json!({"count": 42})]; // Missing "name" field
        let result = SimpleTask::parse_args(args);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.message.contains("Failed to parse single argument"));
    }

    #[test]
    fn test_task_parse_args_extra_arguments() {
        // Test too many arguments for single-arg task
        let args = vec![json!({"name": "test", "count": 42}), json!("extra")];
        let result = SimpleTask::parse_args(args);

        // With multiple arguments, it should try to parse as array and fail for SimpleArgs
        // since SimpleArgs expects a struct, not an array
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_type, "InvalidArguments");
        assert!(error.message.contains("Failed to parse multiple arguments"));
    }

    /// Test TaskContext functionality
    #[test]
    fn test_task_context() {
        let context = TaskContext {
            task_id: "test-task-123".to_string(),
            attempt_number: 2,
            max_attempts: Some(5),
        };

        assert_eq!(context.task_id, "test-task-123");
        assert_eq!(context.attempt_number, 2);
        assert_eq!(context.max_attempts, Some(5));

        // Test with unlimited attempts
        let unlimited_context = TaskContext {
            task_id: "unlimited-task".to_string(),
            attempt_number: 1,
            max_attempts: None,
        };

        assert_eq!(unlimited_context.max_attempts, None);
    }

    /// Test BoxedTask wrapper functionality
    #[tokio::test]
    async fn test_boxed_task_execution() {
        let task = SimpleTask;
        let boxed: std::sync::Arc<dyn BoxedTask> = std::sync::Arc::new(task);

        assert_eq!(boxed.name(), "simple_task");

        let args = vec![json!({"name": "boxed_test", "count": 99})];
        let result = boxed.execute_json(args).await.unwrap();

        assert_eq!(result["name"], "boxed_test");
        assert_eq!(result["count"], 99);
        assert_eq!(result["doubled"], 198);
    }

    #[tokio::test]
    async fn test_boxed_task_error_handling() {
        let task = ErrorTask;
        let boxed: std::sync::Arc<dyn BoxedTask> = std::sync::Arc::new(task);

        // Test invalid arguments error
        let invalid_args = vec![json!({"name": "test", "count": -1})];
        let result = boxed.execute_json(invalid_args).await;
        assert!(result.is_err());

        let TaskError {
            error_type,
            message,
            ..
        } = result.unwrap_err();
        assert_eq!(error_type, "InvalidArguments");
        assert!(message.contains("Count cannot be negative"));

        // Test execution error
        let execution_error_args = vec![json!({"name": "", "count": 5})];
        let result = boxed.execute_json(execution_error_args).await;
        assert!(result.is_err());

        let TaskError {
            error_type,
            message,
            ..
        } = result.unwrap_err();
        assert_eq!(error_type, "ExecutionError");
        assert!(message.contains("Name cannot be empty"));
    }

    #[tokio::test]
    async fn test_task_execution_success_cases() {
        // Test simple task
        let simple_task = SimpleTask;
        let args = SimpleArgs {
            name: "integration_test".to_string(),
            count: 10,
        };
        let result = simple_task.execute(args).await.unwrap();
        assert_eq!(result["name"], "integration_test");
        assert_eq!(result["count"], 10);
        assert_eq!(result["doubled"], 20);

        // Test tuple task
        let tuple_task = TupleTask;
        let tuple_args = TupleArgs("hello".to_string(), 42, true);
        let result = tuple_task.execute(tuple_args).await.unwrap();
        let array = result.as_array().unwrap();
        assert_eq!(array[0], "hello");
        assert_eq!(array[1], 42);
        assert_eq!(array[2], true);

        // Test no args task
        let no_args_task = NoArgsTask;
        let result = no_args_task.execute(NoArgs).await.unwrap();
        assert_eq!(result["status"], "executed_without_args");
    }

    #[tokio::test]
    async fn test_task_execution_with_complex_data() {
        let complex_task = ComplexTask;

        let mut metadata = HashMap::new();
        metadata.insert("author".to_string(), json!("test_user"));
        metadata.insert("version".to_string(), json!("2.1.0"));
        metadata.insert("settings".to_string(), json!({"debug": true}));

        let args = ComplexArgs {
            id: 987654321,
            metadata,
            tags: vec![
                "complex".to_string(),
                "test".to_string(),
                "production".to_string(),
            ],
        };

        let result = complex_task.execute(args).await.unwrap();
        assert_eq!(result["id"], 987654321);
        assert_eq!(result["metadata_count"], 3);
        assert_eq!(result["tags"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_task_execution_result_enum() {
        // Test success result
        let success_data = json!({"result": "success", "value": 42});
        let success_result = TaskExecutionResult::Success(success_data.clone());

        match &success_result {
            TaskExecutionResult::Success(value) => {
                assert_eq!(*value, success_data);
            }
            _ => panic!("Expected success result"),
        }

        // Test failed result
        let task_error = TaskError::execution_failed("Task failed");
        let failed_result = TaskExecutionResult::Failed(task_error.clone());

        match &failed_result {
            TaskExecutionResult::Failed(error) => {
                assert_eq!(error.message, task_error.message);
                assert_eq!(error.error_type, "ExecutionError");
            }
            _ => panic!("Expected failed result"),
        }

        // Test cloning
        let cloned_success = success_result.clone();
        let cloned_failed = failed_result.clone();

        assert!(matches!(cloned_success, TaskExecutionResult::Success(_)));
        assert!(matches!(cloned_failed, TaskExecutionResult::Failed(_)));
    }

    #[test]
    fn test_parse_args_edge_cases() {
        // Test parsing empty tuple
        let empty_tuple_args: Vec<Value> = vec![];
        // This should work for unit type or fail gracefully
        let result = NoArgsTask::parse_args(empty_tuple_args);
        match result {
            Ok(_) => { /* Unit type parsed successfully */ }
            Err(e) => {
                assert!(e.message.contains("No arguments provided"));
            }
        }

        // Test parsing array as single argument
        let array_as_single = vec![json!([1, 2, 3])];
        // This should fail for SimpleTask since it expects an object
        let result = SimpleTask::parse_args(array_as_single);
        assert!(result.is_err());

        // Test parsing malformed JSON-like structure
        let malformed = vec![json!({"name": null, "count": "invalid"})];
        let result = SimpleTask::parse_args(malformed);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_boxed_task_with_different_implementations() {
        let tasks: Vec<std::sync::Arc<dyn BoxedTask>> = vec![
            std::sync::Arc::new(SimpleTask),
            std::sync::Arc::new(TupleTask),
            std::sync::Arc::new(NoArgsTask),
            std::sync::Arc::new(OptionalTask),
            std::sync::Arc::new(ComplexTask),
        ];

        for task in tasks {
            // Each task should have a unique name
            let name = task.name();
            assert!(!name.is_empty());
            assert!(name.contains("task"));
        }
    }

    #[tokio::test]
    async fn test_concurrent_task_execution() {
        let task = std::sync::Arc::new(SimpleTask);
        let mut handles = Vec::new();

        // Execute multiple tasks concurrently
        for i in 0..5 {
            let task_clone = task.clone();
            let handle = tokio::spawn(async move {
                let args = vec![json!({"name": format!("concurrent_{i}"), "count": i * 10})];
                task_clone.execute_json(args).await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap().unwrap();
            assert_eq!(result["name"], format!("concurrent_{i}"));
            assert_eq!(result["count"], i * 10);
            assert_eq!(result["doubled"], (i * 10) * 2);
        }
    }
}
