use crate::db::PgPool;
use crate::taskset::{TaskSetRegistry, Task};
use crate::event_stream::{EventStream, EventStreamConfig, EventRecord};
use anyhow::Result;
use tonic::{Request, Response, Status};
use log::info;
use uuid::Uuid;
use chrono::Utc;
use std::sync::Arc;

use crate::proto::orchestrator;
use orchestrator::azolla_server::{Azolla, AzollaServer};
use orchestrator::*;

use crate::{EVENT_TASK_CREATED, EVENT_TASK_STARTED, EVENT_TASK_ENDED, 
            EVENT_TASK_ATTEMPT_STARTED, EVENT_TASK_ATTEMPT_ENDED, EVENT_SHEPHERD_REGISTERED};

#[derive(Clone)]
pub struct MyAzollaService {
    pool: PgPool,
    registry: Arc<TaskSetRegistry>,
    event_stream: Arc<EventStream>,
}

impl MyAzollaService {
    pub fn new(pool: PgPool, event_stream_config: EventStreamConfig) -> Self {
        let event_stream = Arc::new(EventStream::new(pool.clone(), event_stream_config));
        Self { 
            pool,
            registry: Arc::new(TaskSetRegistry::new()),
            event_stream,
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        // Load existing tasks from database into the registry
        // Note: We initialize from task_instance table, but new operations only write to events table
        // A separate periodic process will sync events back to task_instance table
        self.registry.load_from_db(&self.pool).await?;
        info!("TaskSetRegistry initialized with {} domains (from task_instance table)", 
              self.registry.domains().len());
        Ok(())
    }

    /// Get a reference to the TaskSetRegistry for debugging/monitoring
    pub fn registry(&self) -> &Arc<TaskSetRegistry> {
        &self.registry
    }

    /// Merge events from the events table to task_instance and task_attempts tables
    /// This method can be called periodically or on-demand to sync the event log
    pub async fn merge_events_to_db(&self) -> Result<()> {
        self.registry.merge_events_to_db(&self.pool).await
    }
    
    /// Shutdown the service and print metrics
    pub async fn shutdown(&self) -> Result<()> {
        log::info!("Shutting down Azolla Orchestrator service...");
        self.event_stream.shutdown().await?;
        log::info!("Azolla Orchestrator service shutdown complete");
        Ok(())
    }
}

impl Drop for MyAzollaService {
    fn drop(&mut self) {
        log::info!("MyAzollaService dropping");
    }
}

#[tonic::async_trait]
impl Azolla for MyAzollaService {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();
        info!("Creating task: {} in domain: {}", req.name, req.domain);

        let task_id = Uuid::new_v4();
        let retry_policy: serde_json::Value = serde_json::from_str(&req.retry_policy)
            .map_err(|e| Status::invalid_argument(format!("Invalid retry_policy JSON: {}", e)))?;
        let kwargs: serde_json::Value = serde_json::from_str(&req.kwargs)
            .map_err(|e| Status::invalid_argument(format!("Invalid kwargs JSON: {}", e)))?;

        let flow_instance_id = match req.flow_instance_id {
            Some(ref id_str) => Some(Uuid::parse_str(id_str)
                .map_err(|e| Status::invalid_argument(format!("Invalid flow_instance_id UUID: {}", e)))?),
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

        self.event_stream
            .write_event(event_record)
            .await
            .map_err(|e| {
                log::error!("Failed to write event: {}", e);
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
            status: 0,
            attempts: Vec::new(),
        };

        {
            let domain_actor = self.registry.get_or_create_domain(&req.domain);
            domain_actor.upsert_task(task).await.map_err(|e| {
                Status::internal(format!("Failed to upsert task: {:?}", e))
            })?;
        }

        info!("Successfully created task {} in domain {} (event-sourced)", task_id, req.domain);
        
        Ok(Response::new(CreateTaskResponse { 
            task_id: task_id.to_string() 
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
        Ok(Response::new(CreateFlowResponse { flow_id: flow_id.to_string() }))
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

pub async fn create_server(pool: PgPool, event_stream_config: EventStreamConfig) -> Result<(MyAzollaService, AzollaServer<MyAzollaService>)> {
    let service = MyAzollaService::new(pool, event_stream_config);
    
    service.initialize().await?;
    
    let server = AzollaServer::new(service.clone());
    
    Ok((service, server))
} 