use crate::error::{AzollaError, TaskError};
use crate::proto::common::{
    any_value::Value as AnyInner, AnyValue, ErrorResult, SuccessResult, TaskResult,
};
use crate::proto::shepherd::worker_client::WorkerClient;
use crate::proto::shepherd::ReportResultRequest;
use crate::task::{BoxedTask, Task};
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Represents the outcome of executing a task implementation
#[derive(Debug, Clone)]
pub enum TaskExecutionOutcome {
    Success(Value),
    Failed(TaskError),
}

/// Detailed execution result used by the worker runtime
#[derive(Debug, Clone)]
pub struct WorkerExecution {
    pub outcome: TaskExecutionOutcome,
    pub task_result: TaskResult,
}

impl WorkerExecution {
    pub fn is_success(&self) -> bool {
        matches!(self.outcome, TaskExecutionOutcome::Success(_))
    }
}

/// Invocation context passed from the shepherd when launching a worker process
#[derive(Debug, Clone)]
pub struct WorkerInvocation {
    pub task_id: Uuid,
    pub task_name: String,
    pub args: Vec<Value>,
    pub kwargs: Value,
    pub shepherd_endpoint: String,
}

impl WorkerInvocation {
    /// Build an invocation from JSON encoded CLI values
    #[allow(clippy::result_large_err)]
    pub fn from_json(
        task_id: &str,
        task_name: &str,
        args_json: &str,
        kwargs_json: &str,
        shepherd_endpoint: &str,
    ) -> Result<Self, AzollaError> {
        let task_id = Uuid::parse_str(task_id)
            .map_err(|e| AzollaError::WorkerError(format!("Invalid task ID {task_id}: {e}")))?;

        let args = if args_json.trim().is_empty() {
            Vec::new()
        } else {
            let parsed: Value = serde_json::from_str(args_json)
                .map_err(|e| AzollaError::WorkerError(format!("Invalid args JSON: {e}")))?;

            match parsed {
                Value::Null => Vec::new(),
                Value::Array(arr) => arr,
                other => vec![other],
            }
        };

        let kwargs = if kwargs_json.trim().is_empty() {
            Value::Object(Default::default())
        } else {
            serde_json::from_str(kwargs_json)
                .map_err(|e| AzollaError::WorkerError(format!("Invalid kwargs JSON: {e}")))?
        };

        Ok(Self {
            task_id,
            task_name: task_name.to_string(),
            args,
            kwargs,
            shepherd_endpoint: shepherd_endpoint.to_string(),
        })
    }

    /// Clone the positional arguments for execution
    pub(crate) fn argument_values(&self) -> Vec<Value> {
        self.args.clone()
    }
}

/// Worker runtime responsible for executing a single task invocation
pub struct Worker {
    tasks: HashMap<String, Arc<dyn BoxedTask>>,
}

impl Worker {
    /// Create a worker builder to register task implementations
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::default()
    }

    /// Number of registered task implementations
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Execute a task without reporting the result to the shepherd
    pub async fn execute(&self, invocation: &WorkerInvocation) -> WorkerExecution {
        let task_name = &invocation.task_name;
        let task_id = invocation.task_id;

        info!("Executing task {task_name} ({task_id})");
        let args_value = Value::Array(invocation.argument_values());
        debug!("Task arguments: {args_value}");
        if invocation.kwargs != Value::Object(Default::default()) {
            debug!("Task kwargs: {}", invocation.kwargs);
        }

        let start = Instant::now();
        let execution = match self.tasks.get(task_name) {
            Some(task_impl) => {
                let args = invocation.argument_values();
                let kwargs = invocation.kwargs.clone();
                Self::run_task(task_impl.clone(), task_id, args, kwargs).await
            }
            None => {
                warn!("No task implementation registered for {task_name}");
                Self::task_not_found(task_id, task_name)
            }
        };

        let elapsed = start.elapsed();
        match &execution.outcome {
            TaskExecutionOutcome::Success(_) => {
                info!("Task {task_id} completed successfully in {elapsed:?}");
            }
            TaskExecutionOutcome::Failed(error) => {
                warn!(
                    "Task {task_id} failed in {elapsed:?}: {}:{}",
                    error.error_type, error.message
                );
            }
        }

        execution
    }

    /// Execute a task and report the result back to the shepherd
    pub async fn run_invocation(
        &self,
        invocation: WorkerInvocation,
    ) -> Result<WorkerExecution, AzollaError> {
        let execution = self.execute(&invocation).await;
        self.report_to_shepherd(&invocation, &execution.task_result)
            .await?;
        Ok(execution)
    }

    async fn run_task(
        task_impl: Arc<dyn BoxedTask>,
        task_id: Uuid,
        args: Vec<Value>,
        kwargs: Value,
    ) -> WorkerExecution {
        let outcome = match task_impl.execute_json(args, kwargs).await {
            Ok(value) => TaskExecutionOutcome::Success(value),
            Err(error) => TaskExecutionOutcome::Failed(error),
        };

        Self::build_execution(task_id, outcome)
    }

    fn task_not_found(task_id: Uuid, task_name: &str) -> WorkerExecution {
        let error = TaskError {
            error_type: "TaskNotFound".to_string(),
            message: format!("No implementation registered for task {task_name}"),
            code: None,
            data: Some(json!({ "task_name": task_name })),
            retryable: false,
        };

        Self::build_execution(task_id, TaskExecutionOutcome::Failed(error))
    }

    fn build_execution(task_id: Uuid, outcome: TaskExecutionOutcome) -> WorkerExecution {
        let task_id_str = task_id.to_string();

        let task_result = match &outcome {
            TaskExecutionOutcome::Success(value) => TaskResult {
                task_id: task_id_str.clone(),
                result_type: Some(crate::proto::common::task_result::ResultType::Success(
                    SuccessResult {
                        result: Some(Self::value_to_any(value)),
                    },
                )),
            },
            TaskExecutionOutcome::Failed(error) => {
                let mut data = error
                    .data
                    .clone()
                    .unwrap_or_else(|| Value::Object(Default::default()));
                if let Some(code_value) = &error.code {
                    if let Value::Object(map) = &mut data {
                        map.insert("code".to_string(), Value::String(code_value.clone()));
                    }
                }

                let data_json = if data == Value::Object(Default::default()) {
                    "{}".to_string()
                } else {
                    data.to_string()
                };

                TaskResult {
                    task_id: task_id_str.clone(),
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: error.error_type.clone(),
                            message: error.message.clone(),
                            data: data_json,
                            retriable: error.retryable,
                        },
                    )),
                }
            }
        };

        WorkerExecution {
            outcome,
            task_result,
        }
    }

    fn value_to_any(value: &Value) -> AnyValue {
        let inner = match value {
            Value::String(v) => Some(AnyInner::StringValue(v.clone())),
            Value::Number(v) => {
                if let Some(int_value) = v.as_i64() {
                    Some(AnyInner::IntValue(int_value))
                } else if let Some(u) = v.as_u64() {
                    Some(AnyInner::UintValue(u))
                } else if let Some(f) = v.as_f64() {
                    Some(AnyInner::DoubleValue(f))
                } else {
                    Some(AnyInner::JsonValue(value.to_string()))
                }
            }
            Value::Bool(v) => Some(AnyInner::BoolValue(*v)),
            Value::Null => Some(AnyInner::JsonValue("null".to_string())),
            Value::Array(_) | Value::Object(_) => Some(AnyInner::JsonValue(value.to_string())),
        };

        AnyValue { value: inner }
    }

    async fn report_to_shepherd(
        &self,
        invocation: &WorkerInvocation,
        task_result: &TaskResult,
    ) -> Result<(), AzollaError> {
        info!(
            "Reporting result for task {} to {}",
            invocation.task_id, invocation.shepherd_endpoint
        );

        let mut client = WorkerClient::connect(invocation.shepherd_endpoint.clone())
            .await
            .map_err(|e| {
                AzollaError::ConnectionError(format!(
                    "Failed to connect to shepherd at {}: {e}",
                    invocation.shepherd_endpoint
                ))
            })?;

        let request = ReportResultRequest {
            task_id: invocation.task_id.to_string(),
            result: Some(task_result.clone()),
        };

        let response = client.report_result(request).await.map_err(|e| {
            AzollaError::WorkerError(format!(
                "Failed to report result for task {}: {e}",
                invocation.task_id
            ))
        })?;

        let ack = response.into_inner();
        if !ack.success {
            error!(
                "Shepherd rejected result for task {}: {}",
                invocation.task_id, ack.message
            );
            return Err(AzollaError::WorkerError(format!(
                "Shepherd rejected result for task {}: {}",
                invocation.task_id, ack.message
            )));
        }

        info!(
            "Result for task {} reported successfully",
            invocation.task_id
        );
        Ok(())
    }
}

/// Builder for registering worker task implementations
#[derive(Default)]
pub struct WorkerBuilder {
    tasks: HashMap<String, Arc<dyn BoxedTask>>,
}

impl WorkerBuilder {
    /// Register a task implementation with the worker
    pub fn register_task<T: Task + 'static>(mut self, task: T) -> Self {
        let name = task.name().to_string();
        if self.tasks.contains_key(&name) {
            warn!("Duplicate task registration detected for {name}, overriding previous implementation");
        }
        self.tasks.insert(name, Arc::new(task));
        self
    }

    /// Build the worker runtime
    pub fn build(self) -> Worker {
        Worker { tasks: self.tasks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::TaskResult;
    use serde_json::json;
    use std::future::Future;
    use std::pin::Pin;

    struct SimpleTask;

    impl Task for SimpleTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "simple_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async { Ok(json!({ "status": "ok" })) })
        }
    }

    struct FailingTask;

    impl Task for FailingTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "failing_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async {
                Err(TaskError::execution_failed("intentional failure")
                    .with_error_type("TestError")
                    .with_retryable(false))
            })
        }
    }

    fn test_invocation(task_name: &str) -> WorkerInvocation {
        WorkerInvocation {
            task_id: Uuid::new_v4(),
            task_name: task_name.to_string(),
            args: Vec::new(),
            kwargs: Value::Object(Default::default()),
            shepherd_endpoint: "http://127.0.0.1:50052".to_string(),
        }
    }

    #[tokio::test]
    async fn execute_success_creates_success_result() {
        let worker = Worker::builder().register_task(SimpleTask).build();
        let invocation = test_invocation("simple_task");

        let execution = worker.execute(&invocation).await;

        assert!(execution.is_success());
        match execution.outcome {
            TaskExecutionOutcome::Success(value) => {
                assert_eq!(value["status"], "ok");
            }
            TaskExecutionOutcome::Failed(_) => panic!("Expected success outcome"),
        }

        assert!(matches!(
            execution.task_result.result_type,
            Some(crate::proto::common::task_result::ResultType::Success(_))
        ));
    }

    #[tokio::test]
    async fn execute_failure_creates_error_result() {
        let worker = Worker::builder().register_task(FailingTask).build();
        let invocation = test_invocation("failing_task");

        let execution = worker.execute(&invocation).await;

        assert!(!execution.is_success());
        match execution.outcome {
            TaskExecutionOutcome::Failed(error) => {
                assert_eq!(error.error_type(), "TestError");
                assert!(!error.retryable);
            }
            TaskExecutionOutcome::Success(_) => panic!("Expected failure outcome"),
        }

        match execution.task_result.result_type {
            Some(crate::proto::common::task_result::ResultType::Error(err)) => {
                assert_eq!(err.r#type, "TestError");
                assert!(!err.retriable);
            }
            _ => panic!("Expected error result type"),
        }
    }

    #[tokio::test]
    async fn missing_task_returns_not_found_error() {
        let worker = Worker::builder().build();
        let invocation = test_invocation("unknown_task");

        let execution = worker.execute(&invocation).await;
        assert!(!execution.is_success());

        match execution.outcome {
            TaskExecutionOutcome::Failed(error) => {
                assert_eq!(error.error_type(), "TaskNotFound");
                assert!(!error.retryable);
            }
            _ => panic!("Expected failure outcome"),
        }
    }

    #[test]
    fn worker_invocation_from_json_parses_inputs() {
        let invocation = WorkerInvocation::from_json(
            &Uuid::new_v4().to_string(),
            "json_task",
            "[1,2,3]",
            "{\"flag\": true}",
            "http://127.0.0.1:50052",
        )
        .expect("invocation should parse");

        assert_eq!(invocation.args.len(), 3);
        assert_eq!(invocation.kwargs["flag"], true);
        assert_eq!(invocation.task_name, "json_task");
    }

    #[test]
    fn worker_invocation_from_json_handles_single_value() {
        let invocation = WorkerInvocation::from_json(
            &Uuid::new_v4().to_string(),
            "single_value",
            "{\"name\": \"value\"}",
            "{}",
            "http://127.0.0.1:50052",
        )
        .expect("invocation should parse");

        assert_eq!(invocation.args.len(), 1);
        assert_eq!(invocation.args[0]["name"], "value");
    }

    #[test]
    fn worker_invocation_from_json_rejects_invalid_uuid() {
        let err =
            WorkerInvocation::from_json("not-a-uuid", "task", "[]", "{}", "http://127.0.0.1:50052")
                .expect_err("expected invalid UUID error");

        assert!(matches!(err, AzollaError::WorkerError(_)));
    }
}
