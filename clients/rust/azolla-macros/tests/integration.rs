use azolla_macros::azolla_task;
use serde_json::{json, Value};
// (no additional prelude imports needed)
use azolla_client;
use azolla_client::Task;

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
        return Err(azolla_client::TaskError::invalid_args(
            "Name cannot be empty",
        ));
    }
    Ok(json!({"greeting": format!("Hello {name}!"), "name": name}))
}

/// Test optional parameters
#[azolla_task]
async fn test_greet_with_age(
    name: String,
    age: Option<u32>,
) -> Result<Value, azolla_client::TaskError> {
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
        return Err(azolla_client::TaskError::invalid_args(
            "Numbers array cannot be empty",
        ));
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
    tags: Option<Vec<String>>,
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
        return Err(azolla_client::TaskError::execution_failed(
            "Task failed as requested",
        ));
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
    let result = task.execute((10, 20)).await.unwrap();
    assert_eq!(result["sum"], 30);
    assert_eq!(result["inputs"], json!([10, 20]));
}

#[tokio::test]
async fn test_proc_macro_string_arguments() {
    let task = TestGreetTask;
    let result = task.execute("Alice".to_string()).await.unwrap();
    assert_eq!(result["greeting"], "Hello Alice!");
    assert_eq!(result["name"], "Alice");
}

// Removed: argument count/type mismatch is a compile-time concern with typed execute

// Removed: see note above

// Removed: see note above

#[tokio::test]
async fn test_proc_macro_optional_parameters() {
    let task = TestGreetWithAgeTask;

    // With age provided
    let result = task.execute(("Bob".to_string(), Some(25))).await.unwrap();
    assert_eq!(result["name"], "Bob");
    assert_eq!(result["age"], 25);
    assert!(result["greeting"]
        .as_str()
        .unwrap()
        .contains("25 years old"));

    // With None
    let result = task.execute(("Charlie".to_string(), None)).await.unwrap();
    assert_eq!(result["name"], "Charlie");
    assert_eq!(result["age"], Value::Null);
    assert!(!result["greeting"].as_str().unwrap().contains("years old"));
}

#[tokio::test]
async fn test_proc_macro_array_parameters() {
    let task = TestSumNumbersTask;
    let result = task.execute(vec![1, 2, 3, 4, 5]).await.unwrap();
    assert_eq!(result["sum"], 15);
    assert_eq!(result["count"], 5);
    assert_eq!(result["numbers"], json!([1, 2, 3, 4, 5]));
}

#[tokio::test]
async fn test_proc_macro_empty_array_validation() {
    let task = TestSumNumbersTask;
    let result = task.execute(Vec::<i32>::new()).await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));
}

#[tokio::test]
async fn test_proc_macro_mixed_parameter_types() {
    let task = TestProcessDataTask;
    let result = task
        .execute((
            "hello".to_string(),
            2.5,
            true,
            Some(vec!["tag1".to_string(), "tag2".to_string()]),
        ))
        .await
        .unwrap();

    assert_eq!(result["processed"], "hellohello");
    assert_eq!(result["multiplier"], 2.5);
    assert_eq!(result["repeated"], true);
    assert_eq!(result["tag_count"], 2);
    assert_eq!(result["tags"], json!(["tag1", "tag2"]));
}

#[tokio::test]
async fn test_proc_macro_mixed_types_with_nulls() {
    let task = TestProcessDataTask;
    let result = task
        .execute(("test".to_string(), 1.0, false, None))
        .await
        .unwrap();

    assert_eq!(result["processed"], "test");
    assert_eq!(result["multiplier"], 1.0);
    assert_eq!(result["repeated"], false);
    assert_eq!(result["tag_count"], 0);
    assert_eq!(result["tags"], Value::Null);
}

#[tokio::test]
async fn test_proc_macro_task_error_handling() {
    let task = TestErrorTaskTask;

    // Success
    let result = task.execute(false).await.unwrap();
    assert_eq!(result["status"], "success");

    // Failure
    let result = task.execute(true).await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("failed as requested"));
}

#[tokio::test]
async fn test_proc_macro_string_validation() {
    let task = TestGreetTask;
    let result = task.execute(String::new()).await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("cannot be empty"));
}

// Removed: conversion edge cases are compile-time or runtime parse concerns outside the macro

// Test that original function is still available
#[tokio::test]
async fn test_original_function_still_callable() {
    // The proc macro should preserve the original function
    let result = test_add(5, 7).await.unwrap();
    assert_eq!(result["sum"], 12);
}
