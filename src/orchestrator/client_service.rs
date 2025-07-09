use crate::orchestrator::engine::Engine;
use crate::orchestrator::event_stream::EventRecord;
use crate::orchestrator::taskset::Task;
use anyhow::Result;
use chrono::Utc;
use log::info;
use tonic::{Request, Response, Status};
use uuid::Uuid;

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
        let retry_policy: serde_json::Value = serde_json::from_str(&req.retry_policy)
            .map_err(|e| Status::invalid_argument(format!("Invalid retry_policy JSON: {e}")))?;
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
            "flow_instance_id": flow_instance_id
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

        let task = Task {
            id: task_id,
            name: req.name.clone(),
            created_at: Utc::now(),
            flow_instance_id,
            retry_policy,
            args: req.args,
            kwargs,
            status: TASK_STATUS_CREATED,
            attempts: Vec::new(),
        };

        {
            self.engine.registry.upsert_task(&req.domain, task);
        }

        // Schedule the task using the scheduler
        {
            let scheduler = self
                .engine
                .scheduler_registry
                .get_or_create_scheduler(&req.domain);
            scheduler
                .start_task_async(task_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to start task: {e:?}")))?;
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
        _request: Request<WaitForTaskRequest>,
    ) -> Result<Response<WaitForTaskResponse>, Status> {
        Ok(Response::new(WaitForTaskResponse {
            status: "COMPLETED".to_string(),
        }))
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
