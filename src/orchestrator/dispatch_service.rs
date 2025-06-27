use anyhow::Result;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};
use uuid::Uuid;

use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::taskset::TaskSetRegistry;

use crate::proto::{common, orchestrator};
use orchestrator::dispatch_server::{Dispatch, DispatchServer};
use orchestrator::*;

pub struct DispatchService {
    shepherd_manager: Arc<ShepherdManager>,
    task_registry: Arc<TaskSetRegistry>,
}

impl DispatchService {
    pub fn new(
        shepherd_manager: Arc<ShepherdManager>,
        task_registry: Arc<TaskSetRegistry>,
    ) -> Self {
        Self {
            shepherd_manager,
            task_registry,
        }
    }

    pub fn into_server(self) -> DispatchServer<Self> {
        DispatchServer::new(self)
    }
}

#[tonic::async_trait]
impl Dispatch for DispatchService {
    type StreamStream = ReceiverStream<Result<ServerMsg, tonic::Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMsg>>,
    ) -> Result<Response<Self::StreamStream>, tonic::Status> {
        let client_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);

        let shepherd_manager = self.shepherd_manager.clone();
        let task_registry = self.task_registry.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_shepherd_connection(
                client_stream,
                tx.clone(),
                shepherd_manager,
                task_registry,
            )
            .await
            {
                error!("Shepherd connection error: {}", e);
                let _ = tx
                    .send(Err(tonic::Status::internal("Connection failed")))
                    .await;
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(output_stream))
    }
}

async fn handle_shepherd_connection(
    mut client_stream: Streaming<ClientMsg>,
    tx: mpsc::Sender<Result<ServerMsg, tonic::Status>>,
    shepherd_manager: Arc<ShepherdManager>,
    _task_registry: Arc<TaskSetRegistry>,
) -> Result<()> {
    let mut shepherd_uuid: Option<Uuid> = None;
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        tokio::select! {
            message = client_stream.next() => {
                match message {
                    Some(Ok(client_msg)) => {
                        if let Err(e) = handle_client_message(
                            client_msg,
                            &mut shepherd_uuid,
                            &shepherd_manager,
                            &tx
                        ).await {
                            error!("Error handling client message: {}", e);
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("Error receiving message from shepherd: {}", e);
                        break;
                    }
                    None => {
                        info!("Shepherd stream ended");
                        break;
                    }
                }
            }

            _ = ping_interval.tick() => {
                if shepherd_uuid.is_some() {
                    let ping = orchestrator::ServerMsg {
                        kind: Some(orchestrator::server_msg::Kind::Ping(orchestrator::Ping {
                            timestamp: chrono::Utc::now().timestamp(),
                        })),
                    };

                    if tx.send(Ok(ping)).await.is_err() {
                        debug!("Failed to send ping, shepherd likely disconnected");
                        break;
                    }
                }
            }
        }
    }

    if let Some(uuid) = shepherd_uuid {
        info!("Shepherd {} disconnected, cleaning up", uuid);
        shepherd_manager.remove_shepherd(uuid);
    }

    Ok(())
}

async fn handle_client_message(
    client_msg: ClientMsg,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
    _tx: &mpsc::Sender<Result<ServerMsg, tonic::Status>>,
) -> Result<()> {
    match client_msg.kind {
        Some(client_msg::Kind::Hello(hello)) => {
            handle_hello_message(hello, shepherd_uuid, shepherd_manager).await?;
        }
        Some(client_msg::Kind::Ack(ack)) => {
            handle_ack_message(ack, shepherd_uuid, shepherd_manager).await?;
        }
        Some(client_msg::Kind::Status(status)) => {
            handle_status_message(status, shepherd_uuid, shepherd_manager).await?;
        }
        Some(client_msg::Kind::TaskResult(task_result)) => {
            handle_task_result_message(task_result, shepherd_uuid, shepherd_manager).await?;
        }
        None => {
            warn!("Received empty client message");
        }
    }

    Ok(())
}

async fn handle_hello_message(
    hello: Hello,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
) -> Result<()> {
    let uuid = Uuid::parse_str(&hello.shepherd_uuid)
        .map_err(|e| anyhow::anyhow!("Invalid shepherd UUID: {}", e))?;

    info!(
        "Shepherd {} registering with max_concurrency={}",
        uuid, hello.max_concurrency
    );

    shepherd_manager
        .register_shepherd(uuid, hello.max_concurrency)
        .await?;
    *shepherd_uuid = Some(uuid);

    Ok(())
}

async fn handle_ack_message(
    ack: Ack,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
) -> Result<()> {
    if let Some(uuid) = shepherd_uuid {
        shepherd_manager.mark_shepherd_alive(*uuid);

        let task_id = Uuid::parse_str(&ack.task_id)
            .map_err(|e| anyhow::anyhow!("Invalid task ID in ack: {}", e))?;

        debug!("Received ACK for task {} from shepherd {}", task_id, uuid);

        // TODO: Update task dispatch tracking if needed
    } else {
        warn!("Received ACK from unregistered shepherd");
    }

    Ok(())
}

async fn handle_status_message(
    status: orchestrator::Status,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
) -> Result<()> {
    if let Some(uuid) = shepherd_uuid {
        shepherd_manager.update_shepherd_status(
            *uuid,
            status.current_load,
            status.available_capacity,
        );
        debug!(
            "Updated status for shepherd {}: load={}, available={}",
            uuid, status.current_load, status.available_capacity
        );
    } else {
        warn!("Received status from unregistered shepherd");
    }

    Ok(())
}

async fn handle_task_result_message(
    task_result: common::TaskResult,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
) -> Result<()> {
    if let Some(uuid) = shepherd_uuid {
        shepherd_manager.mark_shepherd_alive(*uuid);

        let task_id = Uuid::parse_str(&task_result.task_id)
            .map_err(|e| anyhow::anyhow!("Invalid task ID in result: {}", e))?;

        info!(
            "Received result for task {} from shepherd {}",
            task_id, uuid
        );

        // TODO: Process task result - write event, update TaskSet, untrack from shepherd
        // This will be handled by the scheduler component
    } else {
        warn!("Received task result from unregistered shepherd");
    }

    Ok(())
}

// TODO: Task dispatch functionality
// This will be called by the scheduler component to send tasks to shepherds
pub async fn dispatch_task_to_shepherd(
    tx: &mpsc::Sender<Result<ServerMsg, tonic::Status>>,
    task_id: Uuid,
    task_name: String,
    args: Vec<String>,
    kwargs: String,
    memory_limit: Option<u64>,
    cpu_limit: Option<u32>,
) -> Result<()> {
    let task = common::Task {
        task_id: task_id.to_string(),
        name: task_name,
        args,
        kwargs,
        memory_limit,
        cpu_limit,
    };

    let server_msg = orchestrator::ServerMsg {
        kind: Some(orchestrator::server_msg::Kind::Task(task)),
    };

    tx.send(Ok(server_msg))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send task to shepherd: {}", e))?;

    Ok(())
}
