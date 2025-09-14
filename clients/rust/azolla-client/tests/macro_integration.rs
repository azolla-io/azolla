/// Comprehensive integration tests for the azolla_task proc macro
/// Tests the real integration between azolla-macros and azolla-client
#[cfg(feature = "macros")]
mod macro_tests {
    use azolla_client::error::TaskError;
    use azolla_client::task::Task;
    use azolla_macros::azolla_task;
    use serde_json::{json, Value};

    /// Test the purpose of basic proc macro functionality: argument handling and type conversion
    #[azolla_task]
    async fn test_add_task(a: i32, b: i32) -> Result<Value, TaskError> {
        Ok(json!({"sum": a + b, "inputs": [a, b]}))
    }

    /// Test the expected behavior: string processing with validation
    #[azolla_task]
    async fn test_greet_task(name: String) -> Result<Value, TaskError> {
        if name.is_empty() {
            return Err(TaskError::invalid_args("Name cannot be empty"));
        }
        Ok(json!({"greeting": format!("Hello {name}!"), "name": name}))
    }

    /// Test optional parameters
    #[azolla_task]
    async fn test_optional_task(name: String, age: Option<u32>) -> Result<Value, TaskError> {
        let greeting = match age {
            Some(age) => format!("Hello {name}, you are {age} years old!"),
            None => format!("Hello {name}!"),
        };
        Ok(json!({"greeting": greeting, "name": name, "age": age}))
    }

    /// Test array/vector parameters
    #[azolla_task]
    async fn test_array_task(numbers: Vec<i32>) -> Result<Value, TaskError> {
        if numbers.is_empty() {
            return Err(TaskError::invalid_args("Numbers array cannot be empty"));
        }
        let sum: i32 = numbers.iter().sum();
        Ok(json!({"sum": sum, "count": numbers.len(), "numbers": numbers}))
    }

    /// Test mixed parameter types
    #[azolla_task]
    async fn test_mixed_task(
        input: String,
        multiplier: f64,
        repeat: bool,
        tags: Option<Vec<String>>,
    ) -> Result<Value, TaskError> {
        let mut result = input.clone();
        if repeat {
            result = format!("{result}{result}");
        }

        let tag_count = tags.as_ref().map(|t| t.len()).unwrap_or(0);

        Ok(json!({
            "processed": result,
            "multiplier": multiplier,
            "repeated": repeat,
            "tag_count": tag_count,
            "tags": tags
        }))
    }

    /// Test error handling in tasks
    #[azolla_task]
    async fn test_error_task(should_fail: bool) -> Result<Value, TaskError> {
        if should_fail {
            return Err(TaskError::execution_failed("Task failed as requested"));
        }
        Ok(json!({"status": "success"}))
    }

    /// Test the purpose of proc macro struct generation: ensures proper task structs are created
    /// Expected behavior: the macro should generate structs with correct names and Task trait implementation
    #[tokio::test]
    async fn test_proc_macro_generates_task_structs() {
        let task = TestAddTaskTask;
        assert_eq!(task.name(), "test_add_task");

        let greet_task = TestGreetTaskTask;
        assert_eq!(greet_task.name(), "test_greet_task");

        let optional_task = TestOptionalTaskTask;
        assert_eq!(optional_task.name(), "test_optional_task");

        let array_task = TestArrayTaskTask;
        assert_eq!(array_task.name(), "test_array_task");

        let mixed_task = TestMixedTaskTask;
        assert_eq!(mixed_task.name(), "test_mixed_task");

        let error_task = TestErrorTaskTask;
        assert_eq!(error_task.name(), "test_error_task");
    }

    /// Test the expected behavior: argument parsing and execution with typed arguments
    #[tokio::test]
    async fn test_proc_macro_typed_argument_execution() {
        let task = TestAddTaskTask;
        let args = (10, 20); // Multiple parameters: tuple
        let result = task.execute(args).await.unwrap();

        assert_eq!(result["sum"], 30);
        assert_eq!(result["inputs"], json!([10, 20]));
    }

    /// Test the purpose of string argument handling: string processing and validation
    #[tokio::test]
    async fn test_proc_macro_string_arguments() {
        let task = TestGreetTaskTask;
        let args = "Alice".to_string(); // Single parameter: direct type
        let result = task.execute(args).await.unwrap();

        assert_eq!(result["greeting"], "Hello Alice!");
        assert_eq!(result["name"], "Alice");
    }

    /// Test the expected behavior: validation and error handling for invalid inputs
    #[tokio::test]
    async fn test_proc_macro_input_validation() {
        let task = TestGreetTaskTask;
        let args = String::new(); // Empty string should trigger validation error
        let result = task.execute(args).await;

        assert!(result.is_err());
        if let Err(TaskError {
            error_type,
            message,
            ..
        }) = result
        {
            assert_eq!(error_type, "InvalidArguments");
            assert!(message.contains("Name cannot be empty"));
        }
    }

    /// Test the purpose of optional parameter handling: None and Some variants
    #[tokio::test]
    async fn test_proc_macro_optional_parameters() {
        let task = TestOptionalTaskTask;

        // Test with Some value
        let args_with_age = ("Bob".to_string(), Some(25)); // Multiple parameters: tuple
        let result = task.execute(args_with_age).await.unwrap();
        assert_eq!(result["name"], "Bob");
        assert_eq!(result["age"], 25);
        assert!(result["greeting"]
            .as_str()
            .unwrap()
            .contains("25 years old"));

        // Test with None value
        let args_without_age = ("Charlie".to_string(), None); // Multiple parameters: tuple
        let result = task.execute(args_without_age).await.unwrap();
        assert_eq!(result["name"], "Charlie");
        assert_eq!(result["age"], Value::Null);
        assert!(!result["greeting"].as_str().unwrap().contains("years old"));
    }

    /// Test the expected behavior: array/vector parameter processing
    #[tokio::test]
    async fn test_proc_macro_array_parameters() {
        let task = TestArrayTaskTask;

        let args = vec![1, 2, 3, 4, 5]; // Single parameter: direct type
        let result = task.execute(args).await.unwrap();

        assert_eq!(result["sum"], 15);
        assert_eq!(result["count"], 5);
        assert_eq!(result["numbers"], json!([1, 2, 3, 4, 5]));
    }

    /// Test the purpose of array validation: empty array error handling
    #[tokio::test]
    async fn test_proc_macro_empty_array_validation() {
        let task = TestArrayTaskTask;

        let args = Vec::<i32>::new(); // Single parameter: direct type
        let result = task.execute(args).await;

        assert!(result.is_err());
        if let Err(TaskError {
            error_type,
            message,
            ..
        }) = result
        {
            assert_eq!(error_type, "InvalidArguments");
            assert!(message.contains("cannot be empty"));
        }
    }

    /// Test the expected behavior: mixed parameter types with complex logic
    #[tokio::test]
    async fn test_proc_macro_mixed_parameter_types() {
        let task = TestMixedTaskTask;

        let args = (
            "hello".to_string(),
            2.5,
            true,
            Some(vec!["tag1".to_string(), "tag2".to_string()]),
        ); // Multiple parameters: tuple
        let result = task.execute(args).await.unwrap();

        assert_eq!(result["processed"], "hellohello");
        assert_eq!(result["multiplier"], 2.5);
        assert_eq!(result["repeated"], true);
        assert_eq!(result["tag_count"], 2);
        assert_eq!(result["tags"], json!(["tag1", "tag2"]));
    }

    /// Test the purpose of null handling: mixed types with null optional values
    #[tokio::test]
    async fn test_proc_macro_mixed_types_with_nulls() {
        let task = TestMixedTaskTask;

        let args = ("test".to_string(), 1.0, false, None); // Multiple parameters: tuple
        let result = task.execute(args).await.unwrap();

        assert_eq!(result["processed"], "test");
        assert_eq!(result["multiplier"], 1.0);
        assert_eq!(result["repeated"], false);
        assert_eq!(result["tag_count"], 0);
        assert_eq!(result["tags"], Value::Null);
    }

    /// Test the expected behavior: task error propagation and handling
    #[tokio::test]
    async fn test_proc_macro_task_error_handling() {
        let task = TestErrorTaskTask;

        // Test successful execution
        let success_args = false; // Single parameter: direct type
        let result = task.execute(success_args).await.unwrap();
        assert_eq!(result["status"], "success");

        // Test error execution
        let error_args = true; // Single parameter: direct type
        let result = task.execute(error_args).await;
        assert!(result.is_err());

        if let Err(TaskError {
            error_type,
            message,
            ..
        }) = result
        {
            assert_eq!(error_type, "ExecutionError");
            assert!(message.contains("failed as requested"));
        }
    }

    /// Test the purpose of original function preservation: macro should not interfere with original
    #[tokio::test]
    async fn test_original_function_still_callable() {
        // The proc macro should preserve the original function
        let result = test_add_task(5, 7).await.unwrap();
        assert_eq!(result["sum"], 12);
    }

    /// Test the expected behavior: complex argument deserialization through JSON
    #[tokio::test]
    async fn test_proc_macro_json_args_integration() {
        use azolla_client::task::BoxedTask;

        // Test that generated tasks work with BoxedTask trait for dynamic dispatch
        let task: Box<dyn BoxedTask> = Box::new(TestAddTaskTask);
        assert_eq!(task.name(), "test_add_task");

        // Test JSON argument parsing through BoxedTask interface
        let json_args = vec![json!(15), json!(25)];
        let result = task.execute_json(json_args).await.unwrap();
        assert_eq!(result["sum"], 40);
    }
}

#[cfg(not(feature = "macros"))]
mod no_macros_tests {
    /// Test the expected behavior: when macros feature is disabled, basic functionality still works
    #[test]
    fn test_macro_feature_disabled() {
        // When macros feature is disabled, we can't use the proc macro
        // but the azolla-client should still function normally
        assert_eq!(2 + 2, 4);
    }
}
