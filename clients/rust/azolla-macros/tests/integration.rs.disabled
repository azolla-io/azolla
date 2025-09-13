use azolla_macros::azolla_task;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

// Mock the required types for testing the proc macro
// These mirror the actual types from azolla-client
mod mock {
    use serde_json::Value;
    use std::future::Future;
    use std::pin::Pin;

    #[derive(Debug, PartialEq)]
    pub enum TaskError {
        InvalidArgs(String),
        ExecutionFailed(String),
    }

    impl TaskError {
        pub fn invalid_args(msg: &str) -> Self {
            Self::InvalidArgs(msg.to_string())
        }
        
        pub fn execution_failed(msg: &str) -> Self {
            Self::ExecutionFailed(msg.to_string())
        }
    }

    impl std::fmt::Display for TaskError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TaskError::InvalidArgs(msg) => write!(f, "Invalid arguments: {msg}"),
                TaskError::ExecutionFailed(msg) => write!(f, "Execution failed: {msg}"),
            }
        }
    }

    impl std::error::Error for TaskError {}

    pub type TaskResult = Result<Value, TaskError>;

    pub trait Task {
        fn name(&self) -> &'static str;
        fn execute(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;
    }

    pub mod convert {
        use super::*;
        
        #[derive(Debug)]
        pub struct ConversionError(pub String);
        
        impl std::fmt::Display for ConversionError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Conversion error: {}", self.0)
            }
        }
        
        impl std::error::Error for ConversionError {}
        
        pub trait FromJsonValue: Sized {
            fn try_from(value: Value) -> Result<Self, ConversionError>;
        }
        
        impl FromJsonValue for i32 {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                value.as_i64()
                    .and_then(|i| i.try_into().ok())
                    .ok_or_else(|| ConversionError("Not a valid i32".to_string()))
            }
        }
        
        impl FromJsonValue for u32 {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                value.as_u64()
                    .and_then(|i| i.try_into().ok())
                    .ok_or_else(|| ConversionError("Not a valid u32".to_string()))
            }
        }
        
        impl FromJsonValue for f64 {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                value.as_f64()
                    .ok_or_else(|| ConversionError("Not a valid f64".to_string()))
            }
        }
        
        impl FromJsonValue for String {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                value.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| ConversionError("Not a valid string".to_string()))
            }
        }
        
        impl FromJsonValue for bool {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                value.as_bool()
                    .ok_or_else(|| ConversionError("Not a valid boolean".to_string()))
            }
        }
        
        impl<T: FromJsonValue> FromJsonValue for Option<T> {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                match value {
                    Value::Null => Ok(None),
                    v => T::try_from(v).map(Some),
                }
            }
        }
        
        impl<T: FromJsonValue> FromJsonValue for Vec<T> {
            fn try_from(value: Value) -> Result<Self, ConversionError> {
                match value {
                    Value::Array(arr) => {
                        let mut result = Vec::with_capacity(arr.len());
                        for item in arr {
                            result.push(T::try_from(item)?);
                        }
                        Ok(result)
                    },
                    _ => Err(ConversionError("Not a valid array".to_string())),
                }
            }
        }
    }

    pub mod task {
        pub use super::{Task, TaskResult};
    }
    
    pub mod error {
        pub use super::TaskError;
    }
}

// Set up path for proc macro testing
use mock as azolla_client;

// Test functions using the proc macro

/// Test the purpose of simple task: basic argument handling and type conversion
#[azolla_task]
async fn test_add(a: i32, b: i32) -> Result<Value, azolla_client::TaskError> {
    Ok(json!({"sum": a + b, "inputs": [a, b]}))
}

/// Test the expected behavior: string processing with validation
#[azolla_task]
async fn test_greet(name: String) -> Result<Value, azolla_client::TaskError> {
    if name.is_empty() {
        return Err(azolla_client::TaskError::invalid_args("Name cannot be empty"));
    }
    Ok(json!({"greeting": format!("Hello {name}!"), "name": name}))
}

/// Test optional parameters
#[azolla_task]  
async fn test_greet_with_age(name: String, age: Option<u32>) -> Result<Value, azolla_client::TaskError> {
    let greeting = match age {
        Some(age) => format!("Hello {name}, you are {age} years old!"),
        None => format!("Hello {name}!"),
    };
    Ok(json!({"greeting": greeting, "name": name, "age": age}))
}

/// Test array/vector parameters
#[azolla_task]
async fn test_sum_numbers(numbers: Vec<i32>) -> Result<Value, azolla_client::TaskError> {
    if numbers.is_empty() {
        return Err(azolla_client::TaskError::invalid_args("Numbers array cannot be empty"));
    }
    let sum: i32 = numbers.iter().sum();
    Ok(json!({"sum": sum, "count": numbers.len(), "numbers": numbers}))
}

/// Test mixed parameter types
#[azolla_task]
async fn test_process_data(
    input: String, 
    multiplier: f64, 
    repeat: bool, 
    tags: Option<Vec<String>>
) -> Result<Value, azolla_client::TaskError> {
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

/// Test error handling in task
#[azolla_task]
async fn test_error_task(should_fail: bool) -> Result<Value, azolla_client::TaskError> {
    if should_fail {
        return Err(azolla_client::TaskError::execution_failed("Task failed as requested"));
    }
    Ok(json!({"status": "success"}))
}

#[tokio::test]
async fn test_proc_macro_generates_task_struct() {
    // Test that proc macro generates the correct task structs with proper names
    let task = TestAddTask;
    assert_eq!(task.name(), "test_add");
    
    let greet_task = TestGreetTask;
    assert_eq!(greet_task.name(), "test_greet");
    
    let age_task = TestGreetWithAgeTask;
    assert_eq!(age_task.name(), "test_greet_with_age");
    
    let sum_task = TestSumNumbersTask; 
    assert_eq!(sum_task.name(), "test_sum_numbers");
    
    let process_task = TestProcessDataTask;
    assert_eq!(process_task.name(), "test_process_data");
    
    let error_task = TestErrorTaskTask;
    assert_eq!(error_task.name(), "test_error_task");
}

#[tokio::test]
async fn test_proc_macro_argument_extraction() {
    let task = TestAddTask;
    let args = vec![json!(10), json!(20)];
    let result = task.execute(args).await.unwrap();
    assert_eq!(result["sum"], 30);
    assert_eq!(result["inputs"], json!([10, 20]));
}

#[tokio::test]
async fn test_proc_macro_string_arguments() {
    let task = TestGreetTask;
    let args = vec![json!("Alice")];
    let result = task.execute(args).await.unwrap();
    assert_eq!(result["greeting"], "Hello Alice!");
    assert_eq!(result["name"], "Alice");
}

#[tokio::test] 
async fn test_proc_macro_missing_arguments() {
    let task = TestAddTask;
    let args = vec![json!(10)]; // Missing second argument
    let result = task.execute(args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::InvalidArgs(msg)) = result {
        assert!(msg.contains("Missing argument"));
    }
}

#[tokio::test]
async fn test_proc_macro_too_many_arguments() {
    let task = TestAddTask;
    let args = vec![json!(10), json!(20), json!(30)]; // Extra argument
    let result = task.execute(args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::InvalidArgs(msg)) = result {
        assert!(msg.contains("Too many arguments"));
    }
}

#[tokio::test]
async fn test_proc_macro_wrong_argument_types() {
    let task = TestAddTask;
    let args = vec![json!("not a number"), json!(20)];
    let result = task.execute(args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::InvalidArgs(msg)) = result {
        assert!(msg.contains("Invalid type"));
    }
}

#[tokio::test]
async fn test_proc_macro_optional_parameters() {
    let task = TestGreetWithAgeTask;
    
    // Test with age provided
    let args_with_age = vec![json!("Bob"), json!(25)];
    let result = task.execute(args_with_age).await.unwrap();
    assert_eq!(result["name"], "Bob");
    assert_eq!(result["age"], 25);
    assert!(result["greeting"].as_str().unwrap().contains("25 years old"));
    
    // Test with null age
    let args_without_age = vec![json!("Charlie"), json!(null)];
    let result = task.execute(args_without_age).await.unwrap();
    assert_eq!(result["name"], "Charlie");
    assert_eq!(result["age"], Value::Null);
    assert!(!result["greeting"].as_str().unwrap().contains("years old"));
}

#[tokio::test]
async fn test_proc_macro_array_parameters() {
    let task = TestSumNumbersTask;
    
    let args = vec![json!([1, 2, 3, 4, 5])];
    let result = task.execute(args).await.unwrap();
    assert_eq!(result["sum"], 15);
    assert_eq!(result["count"], 5);
    assert_eq!(result["numbers"], json!([1, 2, 3, 4, 5]));
}

#[tokio::test]
async fn test_proc_macro_empty_array_validation() {
    let task = TestSumNumbersTask;
    
    let args = vec![json!([])];
    let result = task.execute(args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::InvalidArgs(msg)) = result {
        assert!(msg.contains("cannot be empty"));
    }
}

#[tokio::test]
async fn test_proc_macro_mixed_parameter_types() {
    let task = TestProcessDataTask;
    
    let args = vec![
        json!("hello"),
        json!(2.5),
        json!(true),
        json!(["tag1", "tag2"])
    ];
    let result = task.execute(args).await.unwrap();
    
    assert_eq!(result["processed"], "hellohello");
    assert_eq!(result["multiplier"], 2.5);
    assert_eq!(result["repeated"], true);
    assert_eq!(result["tag_count"], 2);
    assert_eq!(result["tags"], json!(["tag1", "tag2"]));
}

#[tokio::test]
async fn test_proc_macro_mixed_types_with_nulls() {
    let task = TestProcessDataTask;
    
    let args = vec![
        json!("test"),
        json!(1.0),
        json!(false),
        json!(null)
    ];
    let result = task.execute(args).await.unwrap();
    
    assert_eq!(result["processed"], "test");
    assert_eq!(result["multiplier"], 1.0);
    assert_eq!(result["repeated"], false);
    assert_eq!(result["tag_count"], 0);
    assert_eq!(result["tags"], Value::Null);
}

#[tokio::test]
async fn test_proc_macro_task_error_handling() {
    let task = TestErrorTaskTask;
    
    // Test successful execution
    let success_args = vec![json!(false)];
    let result = task.execute(success_args).await.unwrap();
    assert_eq!(result["status"], "success");
    
    // Test error execution
    let error_args = vec![json!(true)];
    let result = task.execute(error_args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::ExecutionFailed(msg)) = result {
        assert!(msg.contains("failed as requested"));
    }
}

#[tokio::test]
async fn test_proc_macro_string_validation() {
    let task = TestGreetTask;
    
    // Test empty string validation
    let args = vec![json!("")];
    let result = task.execute(args).await;
    assert!(result.is_err());
    
    if let Err(azolla_client::TaskError::InvalidArgs(msg)) = result {
        assert!(msg.contains("cannot be empty"));
    }
}

#[tokio::test] 
async fn test_proc_macro_type_conversion_edge_cases() {
    let task = TestAddTask;
    
    // Test integer overflow
    let args = vec![json!(i64::MAX), json!(1)];
    let result = task.execute(args).await;
    assert!(result.is_err()); // Should fail conversion
    
    // Test floating point to integer
    let float_args = vec![json!(10.5), json!(20)];
    let result = task.execute(float_args).await;
    assert!(result.is_err()); // Should fail conversion
}

// Test that original function is still available
#[tokio::test]
async fn test_original_function_still_callable() {
    // The proc macro should preserve the original function
    let result = test_add(5, 7).await.unwrap();
    assert_eq!(result["sum"], 12);
}