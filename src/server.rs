use crate::db::PgPool;
use anyhow::Result;
use tokio_postgres::types::Json;
use tonic::{Request, Response, Status};

// Import the generated protobuf code
pub mod azolla {
    tonic::include_proto!("azolla");
}

use azolla::azolla_server::{Azolla, AzollaServer};
use azolla::*;

pub struct MyAzollaService {
    pool: PgPool,
}

impl MyAzollaService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[tonic::async_trait]
impl Azolla for MyAzollaService {
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();
        log::info!("Creating task: {}", req.name);

        let client = self.pool.get().await.map_err(|e| {
            log::error!("Failed to get DB client: {}", e);
            Status::internal("Database error")
        })?;

        let task_id: i64 = rand::random();
        let retry_policy: serde_json::Value = serde_json::from_str(&req.retry_policy)
            .unwrap_or(serde_json::json!({}));
        let kwargs: serde_json::Value = serde_json::from_str(&req.kwargs)
            .unwrap_or(serde_json::json!({}));

        client
            .execute(
                "INSERT INTO task_instance (id, name, domain, retry_policy, args, kwargs, status, flow_instance_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[&task_id, &req.name, &req.domain, &Json(&retry_policy), &req.args, &Json(&kwargs), &0i16, &req.flow_instance_id],
            )
            .await
            .map_err(|e| {
                log::error!("Failed to create task: {}", e);
                Status::internal("Failed to create task")
            })?;
        
        Ok(Response::new(CreateTaskResponse { task_id }))
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
        Ok(Response::new(CreateFlowResponse {
            flow_id: rand::random(),
        }))
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

pub fn create_server(pool: PgPool) -> AzollaServer<MyAzollaService> {
    let service = MyAzollaService::new(pool);
    AzollaServer::new(service)
} 