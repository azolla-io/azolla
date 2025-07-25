use anyhow::Result;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};
use uuid::Uuid;

use crate::orchestrator::engine::Engine;
use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::orchestrator::taskset::TaskSetRegistry;

use crate::proto::{common, orchestrator};
use orchestrator::cluster_service_server::{ClusterService, ClusterServiceServer};
use orchestrator::*;

pub struct ClusterServiceImpl {
    engine: Engine,
    liveness_probe_threshold: Duration,
}

impl ClusterServiceImpl {
    pub fn new(engine: Engine) -> Self {
        Self::with_liveness_probe_threshold(engine, 1000)
    }

    pub fn with_liveness_probe_threshold(engine: Engine, liveness_probe_threshold_ms: u64) -> Self {
        Self {
            engine,
            liveness_probe_threshold: Duration::from_millis(liveness_probe_threshold_ms),
        }
    }

    pub fn into_server(self) -> ClusterServiceServer<Self> {
        ClusterServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    type StreamStream = ReceiverStream<Result<ServerMsg, tonic::Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<ClientMsg>>,
    ) -> Result<Response<Self::StreamStream>, tonic::Status> {
        let client_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);

        let shepherd_manager = self.engine.shepherd_manager.clone();
        let task_registry = self.engine.registry.clone();
        let scheduler_registry = self.engine.scheduler_registry.clone();

        let liveness_probe_threshold = self.liveness_probe_threshold;
        tokio::spawn(async move {
            if let Err(e) = handle_shepherd_connection(
                client_stream,
                tx.clone(),
                shepherd_manager,
                task_registry,
                scheduler_registry,
                liveness_probe_threshold,
            )
            .await
            {
                error!("Shepherd connection error: {e}");
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
    scheduler_registry: Arc<crate::orchestrator::scheduler::SchedulerRegistry>,
    liveness_probe_threshold: Duration,
) -> Result<()> {
    let mut shepherd_uuid: Option<Uuid> = None;
    let mut last_message_time = Instant::now();
    let mut ping_check_interval = tokio::time::interval(liveness_probe_threshold / 2);

    loop {
        tokio::select! {
            message = client_stream.next() => {
                match message {
                    Some(Ok(client_msg)) => {
                        last_message_time = Instant::now();
                        if let Some(uuid) = shepherd_uuid {
                            shepherd_manager.mark_shepherd_alive(uuid);
                        }

                        if let Err(e) = handle_client_message(
                            client_msg,
                            &mut shepherd_uuid,
                            &shepherd_manager,
                            &scheduler_registry,
                            &tx
                        ).await {
                            error!("Error handling client message: {e}");
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("Error receiving message from shepherd: {e}");
                        break;
                    }
                    None => {
                        info!("Shepherd stream ended");
                        break;
                    }
                }
            }

            _ = ping_check_interval.tick() => {
                if let Some(uuid) = shepherd_uuid {
                    let elapsed = last_message_time.elapsed();
                    if elapsed > liveness_probe_threshold {
                        let ping = orchestrator::ServerMsg {
                            kind: Some(orchestrator::server_msg::Kind::Ping(orchestrator::Ping {
                                timestamp: chrono::Utc::now().timestamp(),
                            })),
                        };

                        if tx.send(Ok(ping)).await.is_err() {
                            debug!("Failed to send ping, shepherd likely disconnected");
                            break;
                        }

                        debug!("Sent ping to shepherd {} after {}ms of inactivity", uuid, elapsed.as_millis());
                    }
                }
            }
        }
    }

    if let Some(uuid) = shepherd_uuid {
        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Shepherd {uuid} connection dropped, marking as temporarily unavailable");
        shepherd_manager.mark_shepherd_disconnected(uuid);
    }

    Ok(())
}

async fn handle_client_message(
    client_msg: ClientMsg,
    shepherd_uuid: &mut Option<Uuid>,
    shepherd_manager: &Arc<ShepherdManager>,
    scheduler_registry: &Arc<crate::orchestrator::scheduler::SchedulerRegistry>,
    tx: &mpsc::Sender<Result<ServerMsg, tonic::Status>>,
) -> Result<()> {
    match client_msg.kind {
        Some(client_msg::Kind::Hello(hello)) => {
            handle_hello_message(hello, shepherd_uuid, shepherd_manager, tx).await?;
        }
        Some(client_msg::Kind::Ack(ack)) => {
            handle_ack_message(ack, shepherd_uuid, shepherd_manager).await?;
        }
        Some(client_msg::Kind::Status(status)) => {
            handle_status_message(status, shepherd_uuid, shepherd_manager).await?;
        }
        Some(client_msg::Kind::TaskResult(task_result)) => {
            handle_task_result_message(
                task_result,
                shepherd_uuid,
                shepherd_manager,
                scheduler_registry,
            )
            .await?;
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
    tx: &mpsc::Sender<Result<ServerMsg, tonic::Status>>,
) -> Result<()> {
    let uuid = Uuid::parse_str(&hello.shepherd_uuid)
        .map_err(|e| anyhow::anyhow!("Invalid shepherd UUID: {}", e))?;

    info!(
        "Shepherd {} registering with max_concurrency={}",
        uuid, hello.max_concurrency
    );

    shepherd_manager
        .register_shepherd_with_tx(uuid, hello.max_concurrency, tx.clone())
        .await?;
    *shepherd_uuid = Some(uuid);

    Ok(())
}

async fn handle_ack_message(
    ack: Ack,
    shepherd_uuid: &mut Option<Uuid>,
    _shepherd_manager: &Arc<ShepherdManager>,
) -> Result<()> {
    if let Some(uuid) = shepherd_uuid {
        let task_id = Uuid::parse_str(&ack.task_id)
            .map_err(|e| anyhow::anyhow!("Invalid task ID in ack: {}", e))?;

        debug!("Received ACK for task {task_id} from shepherd {uuid}");

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
    _shepherd_manager: &Arc<ShepherdManager>,
    scheduler_registry: &Arc<crate::orchestrator::scheduler::SchedulerRegistry>,
) -> Result<()> {
    if let Some(uuid) = shepherd_uuid {
        let task_id = Uuid::parse_str(&task_result.task_id)
            .map_err(|e| anyhow::anyhow!("Invalid task ID in result: {}", e))?;

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Received result for task {task_id} from shepherd {uuid}");

        // TODO: Need to determine the domain for this task
        // For now, we'll iterate through all domains to find the task
        // In production, we might want to include domain in the TaskResult message
        // or maintain a task_id -> domain mapping

        let domains = scheduler_registry.domains();
        let mut task_found = false;

        for domain in domains {
            if let Some(scheduler) = scheduler_registry.get_scheduler(&domain) {
                // Try to handle the result - the scheduler will ignore if task not in this domain
                if let Err(e) = scheduler
                    .handle_task_result(task_id, task_result.clone())
                    .await
                {
                    warn!(
                        "Failed to handle task result for task {task_id} in domain {domain}: {e:?}"
                    );
                } else {
                    task_found = true;
                    break;
                }
            }
        }

        if !task_found {
            warn!("Task {task_id} not found in any domain when processing result");
        }
    } else {
        warn!("Received task result from unregistered shepherd");
    }

    Ok(())
}
