use crate::error::TaskError;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

pub type TaskResult = Result<Value, TaskError>;

/// Task execution context
pub struct TaskContext {
    pub task_id: String,
    pub attempt_number: u32,
    pub max_attempts: Option<u32>,
}

/// Trait for task implementations
pub trait Task: Send + Sync {
    /// Task name for registration
    fn name(&self) -> &'static str;

    /// Execute the task
    fn execute(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>>;

    /// Optional: Validate arguments before execution
    #[allow(clippy::result_large_err)]
    fn validate_args(&self, _args: &[Value]) -> Result<(), TaskError> {
        Ok(())
    }
}

/// Result of task execution
#[derive(Debug, Clone)]
pub enum TaskExecutionResult {
    Success(Value),
    Failed(TaskError),
}
