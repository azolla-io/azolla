//! Client module for Azolla
//!
//! This is a minimal client implementation for the main crate.
//! For the full standalone client library, see `clients/rust/azolla-client/`

// Re-export proc macro
pub use azolla_macros::azolla_task;

// Re-export common types for compatibility
pub use serde_json::Value;

// For now, include minimal types needed for examples and tests
use serde_json;
use std::future::Future;
use std::pin::Pin;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("Execution failed: {0}")]
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

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Type conversion failed: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("Conversion error: {0}")]
    Other(String),
}

impl ConversionError {
    pub fn type_mismatch(expected: &str, value: &Value) -> Self {
        Self::TypeMismatch {
            expected: expected.to_string(),
            actual: format!("{value:?}"),
        }
    }
}

pub type TaskResult = Result<Value, TaskError>;

pub trait FromJsonValue: Sized {
    fn try_from(value: Value) -> Result<Self, ConversionError>;
}

// Implement FromJsonValue for basic types
impl FromJsonValue for i32 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Number(ref n) => n
                .as_i64()
                .and_then(|i| i.try_into().ok())
                .ok_or_else(|| ConversionError::type_mismatch("i32", &value)),
            _ => Err(ConversionError::type_mismatch("number", &value)),
        }
    }
}

impl FromJsonValue for u32 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Number(ref n) => n
                .as_u64()
                .and_then(|u| u.try_into().ok())
                .ok_or_else(|| ConversionError::type_mismatch("u32", &value)),
            _ => Err(ConversionError::type_mismatch("number", &value)),
        }
    }
}

impl FromJsonValue for f64 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Number(ref n) => n
                .as_f64()
                .ok_or_else(|| ConversionError::type_mismatch("f64", &value)),
            _ => Err(ConversionError::type_mismatch("number", &value)),
        }
    }
}

impl FromJsonValue for String {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::String(s) => Ok(s),
            _ => Err(ConversionError::type_mismatch("string", &value)),
        }
    }
}

impl FromJsonValue for bool {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Bool(b) => Ok(b),
            _ => Err(ConversionError::type_mismatch("boolean", &value)),
        }
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
            }
            _ => Err(ConversionError::type_mismatch("array", &value)),
        }
    }
}

pub trait Task: Send + Sync {
    fn name(&self) -> &'static str;
    fn execute(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;
}

pub struct TaskContext {
    pub task_id: String,
    pub domain: String,
}

// Minimal Client for examples - not functional, just for compilation
pub struct Client;

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder
    }
}

pub struct ClientBuilder;

impl ClientBuilder {
    pub fn endpoint(self, _endpoint: &str) -> Self {
        self
    }
    pub fn domain(self, _domain: &str) -> Self {
        self
    }
    pub async fn build(self) -> Result<Client, Box<dyn std::error::Error>> {
        Ok(Client)
    }
}

// Minimal Worker for examples - not functional, just for compilation
pub struct Worker {
    task_count: usize,
}

pub struct WorkerBuilder {
    tasks: Vec<Box<dyn Task>>,
}

impl WorkerBuilder {
    pub fn orchestrator(self, _addr: &str) -> Self {
        self
    }
    pub fn domain(self, _domain: &str) -> Self {
        self
    }
    pub fn shepherd_group(self, _group: &str) -> Self {
        self
    }

    pub fn register_task<T: Task + 'static>(mut self, task: T) -> Self {
        self.tasks.push(Box::new(task));
        self
    }

    pub async fn build(self) -> Result<Worker, Box<dyn std::error::Error>> {
        Ok(Worker {
            task_count: self.tasks.len(),
        })
    }
}

impl Worker {
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder { tasks: Vec::new() }
    }

    pub fn task_count(&self) -> usize {
        self.task_count
    }
}

#[cfg(test)]
mod test_typed_args {
    use super::*;
    use serde_json::json;

    // Test function-based task using proc macro
    #[azolla_task]
    async fn test_add_numbers(a: i32, b: i32) -> Result<Value, TaskError> {
        Ok(json!({"sum": a + b, "inputs": [a, b]}))
    }

    #[azolla_task]
    async fn test_greet(name: String, age: Option<u32>) -> Result<Value, TaskError> {
        let greeting = match age {
            Some(age) => format!("Hello {name}, you are {age} years old!"),
            None => format!("Hello {name}!"),
        };
        Ok(json!({"greeting": greeting, "name": name, "age": age}))
    }

    #[azolla_task]
    async fn test_process_array(numbers: Vec<i32>) -> Result<Value, TaskError> {
        let sum: i32 = numbers.iter().sum();
        let avg = sum as f64 / numbers.len() as f64;
        Ok(json!({
            "numbers": numbers,
            "sum": sum,
            "average": avg,
            "count": numbers.len()
        }))
    }

    #[tokio::test]
    async fn test_type_conversion() {
        // Test basic type conversions using FromJsonValue trait
        assert!(<i32 as FromJsonValue>::try_from(json!(42)).is_ok());
        assert!(<String as FromJsonValue>::try_from(json!("hello")).is_ok());
        assert!(<bool as FromJsonValue>::try_from(json!(true)).is_ok());

        // Test array conversion
        let numbers = vec![1, 2, 3];
        let json_array = json!([1, 2, 3]);
        let converted: Vec<i32> = <Vec<i32> as FromJsonValue>::try_from(json_array).unwrap();
        assert_eq!(converted, numbers);

        // Test optional conversion
        assert!(<Option<i32> as FromJsonValue>::try_from(json!(null))
            .unwrap()
            .is_none());
        assert!(<Option<i32> as FromJsonValue>::try_from(json!(42)).unwrap() == Some(42));
    }

    #[tokio::test]
    async fn test_task_generation() {
        // Test that proc macro generates the correct task structs
        let add_task = TestAddNumbersTask;
        assert_eq!(add_task.name(), "test_add_numbers");

        let greet_task = TestGreetTask;
        assert_eq!(greet_task.name(), "test_greet");

        let array_task = TestProcessArrayTask;
        assert_eq!(array_task.name(), "test_process_array");
    }

    #[tokio::test]
    async fn test_task_execution() {
        let add_task = TestAddNumbersTask;

        // Test successful execution
        let args = vec![json!(10), json!(20)];
        let result = add_task.execute(args).await.unwrap();

        assert_eq!(result["sum"], 30);
        assert_eq!(result["inputs"], json!([10, 20]));
    }

    #[tokio::test]
    async fn test_task_execution_with_optional() {
        let greet_task = TestGreetTask;

        // Test with optional parameter present
        let args_with_age = vec![json!("Alice"), json!(25)];
        let result = greet_task.execute(args_with_age).await.unwrap();
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], 25);

        // Test with optional parameter as null
        let args_without_age = vec![json!("Bob"), json!(null)];
        let result = greet_task.execute(args_without_age).await.unwrap();
        assert_eq!(result["name"], "Bob");
        assert_eq!(result["age"], Value::Null);
    }

    #[tokio::test]
    async fn test_task_execution_with_array() {
        let array_task = TestProcessArrayTask;

        let args = vec![json!([1, 2, 3, 4, 5])];
        let result = array_task.execute(args).await.unwrap();

        assert_eq!(result["sum"], 15);
        assert_eq!(result["count"], 5);
        assert_eq!(result["average"], 3.0);
    }

    #[tokio::test]
    async fn test_worker_registration() {
        // Test that we can register proc macro generated tasks
        let worker = Worker::builder()
            .orchestrator("localhost:52710")
            .domain("test")
            .register_task(TestAddNumbersTask)
            .register_task(TestGreetTask)
            .register_task(TestProcessArrayTask)
            .build()
            .await
            .unwrap();

        assert_eq!(worker.task_count(), 3);
    }

    #[tokio::test]
    async fn test_argument_validation() {
        let add_task = TestAddNumbersTask;

        // Test missing arguments
        let empty_args = vec![];
        let result = add_task.execute(empty_args).await;
        assert!(result.is_err());

        // Test wrong type arguments
        let wrong_type_args = vec![json!("not_a_number"), json!(20)];
        let result = add_task.execute(wrong_type_args).await;
        assert!(result.is_err());
    }
}
