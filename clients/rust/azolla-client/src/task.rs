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
    fn parse_args(json_args: Vec<Value>) -> Result<Self::Args, TaskError> {
        match json_args.len() {
            0 => {
                // No arguments - try to deserialize from null or empty
                serde_json::from_value(json!(null))
                    .or_else(|_| serde_json::from_value(json!({})))
                    .map_err(|e| TaskError::invalid_args(&format!("No arguments provided and type doesn't support empty: {e}")))
            }
            1 => {
                // Single argument - deserialize directly  
                serde_json::from_value(json_args[0].clone())
                    .map_err(|e| TaskError::invalid_args(&format!("Failed to parse single argument: {e}")))
            }
            _ => {
                // Multiple arguments - deserialize as array
                serde_json::from_value(json!(json_args))
                    .map_err(|e| TaskError::invalid_args(&format!("Failed to parse multiple arguments: {e}")))
            }
        }
    }
}

/// Type-erased task wrapper for dynamic dispatch
pub trait BoxedTask: Send + Sync {
    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task with JSON arguments
    fn execute_json(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;
}

/// Implementation of BoxedTask for any Task
impl<T: Task + 'static> BoxedTask for T {
    fn name(&self) -> &'static str {
        Task::name(self)
    }

    fn execute_json(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
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
