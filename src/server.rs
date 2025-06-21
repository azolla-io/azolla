use crate::db::PgPool;
use crate::taskset::{TaskSetRegistry, Task};
use anyhow::Result;
use tokio_postgres::types::Json;
use tonic::{Request, Response, Status};
use log::info;
use uuid::Uuid;
use chrono::Utc;
use std::sync::Arc;

// Import the generated protobuf code
pub mod azolla {
    tonic::include_proto!("azolla");
}

use azolla::azolla_server::{Azolla, AzollaServer};
use azolla::*;

// Event types for the events table
const EVENT_TASK_CREATED: i16 = 1;
const EVENT_TASK_STARTED: i16 = 2;
const EVENT_TASK_ENDED: i16 = 3;
const EVENT_TASK_ATTEMPT_STARTED: i16 = 4;
const EVENT_TASK_ATTEMPT_ENDED: i16 = 5;

pub struct MyAzollaService {
    pool: PgPool,
    registry: Arc<TaskSetRegistry>,
}

impl MyAzollaService {
    pub fn new(pool: PgPool) -> Self {
        Self { 
            pool,
            registry: Arc::new(TaskSetRegistry::new()),
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        // Load existing tasks from database into the registry
        // Note: We initialize from task_instance table, but new operations only write to events table
        // A separate periodic process will sync events back to task_instance table
        self.registry.load_from_db(&self.pool).await?;
        info!("TaskSetRegistry initialized with {} domains (from task_instance table)", 
              self.registry.domains().count());
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
}

#[tonic::async_trait]
impl Azolla for MyAzollaService {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();
        info!("Creating task: {} in domain: {}", req.name, req.domain);

        let client = self.pool.get().await.map_err(|e| {
            log::error!("Failed to get DB client: {}", e);
            Status::internal("Database error")
        })?;

        let task_id = Uuid::new_v4();
        let retry_policy: serde_json::Value = serde_json::from_str(&req.retry_policy)
            .unwrap_or(serde_json::json!({}));
        let kwargs: serde_json::Value = serde_json::from_str(&req.kwargs)
            .unwrap_or(serde_json::json!({}));

        let flow_instance_id = req.flow_instance_id
            .as_ref()
            .and_then(|id_str| Uuid::parse_str(id_str).ok());

        // Create an event in the events table (event-sourcing pattern)
        let event_metadata = serde_json::json!({
            "task_id": task_id,
            "task_name": req.name,
            "created_by": "grpc_api",
            "retry_policy": retry_policy,
            "args": req.args,
            "kwargs": kwargs,
            "flow_instance_id": flow_instance_id
        });

        client
            .execute(
                "INSERT INTO events (domain, task_instance_id, flow_instance_id, event_type, created_at, metadata) 
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[&req.domain, &task_id, &flow_instance_id, &EVENT_TASK_CREATED, &Utc::now(), &Json(&event_metadata)],
            )
            .await
            .map_err(|e| {
                log::error!("Failed to create event: {}", e);
                Status::internal("Failed to create event")
            })?;

        // Update the in-memory TaskSet (source of truth for current state)
        let task = Task {
            id: task_id,
            name: req.name.clone(),
            created_at: Utc::now(),
            flow_instance_id,
            retry_policy,
            args: req.args,
            kwargs,
            status: 0, // Created status
            attempts: Vec::new(),
        };

        // Get or create the domain and insert the task
        {
            let mut domain_ref = self.registry.get_or_create_domain(&req.domain);
            domain_ref.upsert_task(task);
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
        // Placeholder implementation
        Ok(Response::new(WaitForTaskResponse {
            status: "COMPLETED".to_string(),
        }))
    }

    async fn create_flow(
        &self,
        _request: Request<CreateFlowRequest>,
    ) -> Result<Response<CreateFlowResponse>, Status> {
        // Placeholder implementation
        let flow_id = Uuid::new_v4();
        Ok(Response::new(CreateFlowResponse { flow_id: flow_id.to_string() }))
    }

    async fn wait_for_flow(
        &self,
        _request: Request<WaitForFlowRequest>,
    ) -> Result<Response<WaitForFlowResponse>, Status> {
        // Placeholder implementation
        Ok(Response::new(WaitForFlowResponse {
            status: "COMPLETED".to_string(),
        }))
    }

    async fn publish_task_event(
        &self,
        _request: Request<PublishTaskEventRequest>,
    ) -> Result<Response<PublishTaskEventResponse>, Status> {
        // Placeholder implementation
        Ok(Response::new(PublishTaskEventResponse { success: true }))
    }

    async fn publish_flow_event(
        &self,
        _request: Request<PublishFlowEventRequest>,
    ) -> Result<Response<PublishFlowEventResponse>, Status> {
        // Placeholder implementation
        Ok(Response::new(PublishFlowEventResponse { success: true }))
    }
}

pub async fn create_server(pool: PgPool) -> Result<AzollaServer<MyAzollaService>> {
    let service = MyAzollaService::new(pool);
    
    // Initialize the TaskSetRegistry by loading from database
    service.initialize().await?;
    
    Ok(AzollaServer::new(service))
} 