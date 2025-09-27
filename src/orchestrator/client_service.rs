use crate::orchestrator::engine::Engine;
use crate::orchestrator::event_stream::EventRecord;
use crate::orchestrator::retry_policy::RetryPolicy;
use crate::orchestrator::taskset::{Task, TaskResultValue};
use anyhow::Result;
use chrono::Utc;
use log::info;
use std::time::Duration;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::common::{AnyValue, ErrorResult, SuccessResult};
use crate::proto::orchestrator;
use orchestrator::client_service_server::ClientService;
use orchestrator::*;

use crate::{EVENT_TASK_CREATED, TASK_STATUS_CREATED};

#[derive(Clone)]
pub struct ClientServiceImpl {
    engine: Engine,
}

impl ClientServiceImpl {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }

    /// Get a reference to the Engine for creating other services
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Merge events from the events table to task_instance and task_attempts tables
    /// This method can be called periodically or on-demand to sync the event log
    pub async fn merge_events_to_db(&self) -> Result<()> {
        self.engine.merge_events_to_db().await
    }

    fn convert_task_result_value_to_result_type(
        &self,
        result: &TaskResultValue,
    ) -> crate::proto::orchestrator::wait_for_task_response::ResultType {
        match result {
            TaskResultValue::Success(data) => {
                crate::proto::orchestrator::wait_for_task_response::ResultType::Success(
                    SuccessResult {
                        result: Some(self.convert_json_to_any_value(data)),
                    },
                )
            }
            TaskResultValue::Error {
                error_type,
                message,
                data,
                retriable,
            } => {
                crate::proto::orchestrator::wait_for_task_response::ResultType::Error(ErrorResult {
                    r#type: error_type.clone(),
                    message: message.clone(),
                    data: data.to_string(),
                    retriable: *retriable,
                })
            }
        }
    }

    fn convert_json_to_any_value(&self, json_value: &serde_json::Value) -> AnyValue {
        match json_value {
            serde_json::Value::String(s) => AnyValue {
                value: Some(crate::proto::common::any_value::Value::StringValue(
                    s.clone(),
                )),
            },
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    AnyValue {
                        value: Some(crate::proto::common::any_value::Value::IntValue(i)),
                    }
                } else if let Some(u) = n.as_u64() {
                    AnyValue {
                        value: Some(crate::proto::common::any_value::Value::UintValue(u)),
                    }
                } else if let Some(f) = n.as_f64() {
                    AnyValue {
                        value: Some(crate::proto::common::any_value::Value::DoubleValue(f)),
                    }
                } else {
                    AnyValue {
                        value: Some(crate::proto::common::any_value::Value::JsonValue(
                            json_value.to_string(),
                        )),
                    }
                }
            }
            serde_json::Value::Bool(b) => AnyValue {
                value: Some(crate::proto::common::any_value::Value::BoolValue(*b)),
            },
            _ => AnyValue {
                value: Some(crate::proto::common::any_value::Value::JsonValue(
                    json_value.to_string(),
                )),
            },
        }
    }
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self) {
        log::info!("ClientServiceImpl dropping");
    }
}

#[tonic::async_trait]
impl ClientService for ClientServiceImpl {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();
        info!("Creating task: {} in domain: {}", req.name, req.domain);

        let task_id = Uuid::new_v4();
        let parsed_retry_policy = match req.retry_policy.as_ref() {
            Some(policy) => RetryPolicy::from_proto(policy)
                .map_err(|e| Status::invalid_argument(format!("Invalid retry policy: {e}")))?,
            None => RetryPolicy::default(),
        };

        parsed_retry_policy.validate().map_err(|e| {
            Status::invalid_argument(format!("Retry policy validation failed: {e}"))
        })?;
        let retry_policy = serde_json::to_value(&parsed_retry_policy)
            .map_err(|e| Status::internal(format!("Failed to serialize retry policy: {e}")))?;
        let kwargs: serde_json::Value = serde_json::from_str(&req.kwargs)
            .map_err(|e| Status::invalid_argument(format!("Invalid kwargs JSON: {e}")))?;

        let flow_instance_id = match req.flow_instance_id {
            Some(ref id_str) => Some(Uuid::parse_str(id_str).map_err(|e| {
                Status::invalid_argument(format!("Invalid flow_instance_id UUID: {e}"))
            })?),
            None => None,
        };

        let event_metadata = serde_json::json!({
            "task_id": task_id,
            "task_name": req.name,
            "created_by": "grpc_api",
            "retry_policy": retry_policy,
            "args": req.args,
            "kwargs": kwargs,
            "flow_instance_id": flow_instance_id,
            "shepherd_group": req.shepherd_group.clone()
        });

        let event_record = EventRecord {
            domain: req.domain.clone(),
            task_instance_id: Some(task_id),
            flow_instance_id,
            event_type: EVENT_TASK_CREATED,
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        self.engine
            .event_stream
            .write_event(event_record)
            .await
            .map_err(|e| {
                log::error!("Failed to write event: {e}");
                Status::internal("Failed to write event")
            })?;

        // Keep args as JSON string - will be parsed by worker with proper types
        let args_json = req.args;

        let task = Task {
            id: task_id,
            name: req.name.clone(),
            created_at: Utc::now(),
            flow_instance_id,
            retry_policy,
            args: args_json,
            kwargs,
            status: TASK_STATUS_CREATED,
            attempts: Vec::new(),
            shepherd_group: req.shepherd_group,
            result: None,
            result_stored_at: None,
        };

        // Create and start the task via the SchedulerActor
        {
            let scheduler = self
                .engine
                .scheduler_registry
                .get_or_create_scheduler(&req.domain);

            // Create and start the task in a single operation
            scheduler
                .start_task_async(task_id, Some(task))
                .await
                .map_err(|e| Status::internal(format!("Failed to create and start task: {e:?}")))?;
        }

        info!(
            "Successfully created and scheduled task {} in domain {}",
            task_id, req.domain
        );

        Ok(Response::new(CreateTaskResponse {
            task_id: task_id.to_string(),
        }))
    }

    async fn wait_for_task(
        &self,
        request: Request<WaitForTaskRequest>,
    ) -> Result<Response<WaitForTaskResponse>, Status> {
        let req = request.into_inner();

        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid task_id UUID: {e}")))?;

        info!("Waiting for task {} in domain {}", task_id, req.domain);

        // Fast path: Non-blocking check when timeout_ms == 0
        if matches!(req.timeout_ms, Some(0)) {
            let scheduler = self
                .engine
                .scheduler_registry
                .get_or_create_scheduler(&req.domain);

            return match scheduler.get_task_result(task_id).await {
                Ok(Some(result)) => Ok(Response::new(WaitForTaskResponse {
                    status_code: WaitForTaskStatus::Completed.into(),
                    result_type: Some(self.convert_task_result_value_to_result_type(&result)),
                })),
                Ok(None) => Ok(Response::new(WaitForTaskResponse {
                    status_code: WaitForTaskStatus::Timeout.into(),
                    result_type: None,
                })),
                Err(_) => Ok(Response::new(WaitForTaskResponse {
                    status_code: WaitForTaskStatus::InternalError.into(),
                    result_type: None,
                })),
            };
        }

        // Poll for task completion with 200ms intervals
        let poll_interval = Duration::from_millis(200);

        // Use provided timeout or default to 5 minutes if not specified
        let max_wait_time = if let Some(timeout_ms) = req.timeout_ms {
            Duration::from_millis(timeout_ms)
        } else {
            Duration::from_secs(300) // 5 minute default timeout
        };

        let start_time = std::time::Instant::now();

        loop {
            // Check if we've exceeded the maximum wait time
            if start_time.elapsed() > max_wait_time {
                return Ok(Response::new(WaitForTaskResponse {
                    status_code: WaitForTaskStatus::Timeout.into(),
                    result_type: None,
                }));
            }

            // Get task result from scheduler efficiently (only fetches the result, not the entire task)
            let scheduler = self
                .engine
                .scheduler_registry
                .get_or_create_scheduler(&req.domain);

            match scheduler.get_task_result(task_id).await {
                Ok(Some(result)) => {
                    // Task has completed with a result
                    return Ok(Response::new(WaitForTaskResponse {
                        status_code: WaitForTaskStatus::Completed.into(),
                        result_type: Some(self.convert_task_result_value_to_result_type(&result)),
                    }));
                }
                Ok(None) => {
                    // Task either doesn't exist or hasn't completed yet
                    // We need to distinguish between these cases for proper error handling
                    // Check if task exists by trying to get it (this is unavoidable for the distinction)
                    match scheduler.get_task_for_test(task_id).await {
                        Ok(Some(_)) => {
                            // Task exists but no result yet, continue polling
                        }
                        Ok(None) => {
                            return Ok(Response::new(WaitForTaskResponse {
                                status_code: WaitForTaskStatus::TaskNotFound.into(),
                                result_type: None,
                            }));
                        }
                        Err(_) => {
                            return Ok(Response::new(WaitForTaskResponse {
                                status_code: WaitForTaskStatus::InternalError.into(),
                                result_type: None,
                            }));
                        }
                    }
                }
                Err(_) => {
                    return Ok(Response::new(WaitForTaskResponse {
                        status_code: WaitForTaskStatus::InternalError.into(),
                        result_type: None,
                    }));
                }
            }

            // Wait before next poll
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn create_flow(
        &self,
        _request: Request<CreateFlowRequest>,
    ) -> Result<Response<CreateFlowResponse>, Status> {
        let flow_id = Uuid::new_v4();
        Ok(Response::new(CreateFlowResponse {
            flow_id: flow_id.to_string(),
        }))
    }

    async fn wait_for_flow(
        &self,
        _request: Request<WaitForFlowRequest>,
    ) -> Result<Response<WaitForFlowResponse>, Status> {
        Ok(Response::new(WaitForFlowResponse {
            status: "COMPLETED".to_string(),
        }))
    }

    async fn publish_task_event(
        &self,
        _request: Request<PublishTaskEventRequest>,
    ) -> Result<Response<PublishTaskEventResponse>, Status> {
        Ok(Response::new(PublishTaskEventResponse { success: true }))
    }

    async fn publish_flow_event(
        &self,
        _request: Request<PublishFlowEventRequest>,
    ) -> Result<Response<PublishFlowEventResponse>, Status> {
        Ok(Response::new(PublishFlowEventResponse { success: true }))
    }
}
