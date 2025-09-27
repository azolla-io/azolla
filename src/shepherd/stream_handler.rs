use anyhow::Result;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, sleep, Duration};
use tonic::{transport::Channel, Request, Streaming};
use uuid::Uuid;

use crate::proto::orchestrator;
use crate::shepherd::{ShepherdConfig, TaskResultMessage};
use orchestrator::cluster_service_client::ClusterServiceClient;
use orchestrator::*;

#[derive(Debug, Clone)]
pub struct IncomingTask {
    pub task_id: Uuid,
    pub name: String,
    pub args: Vec<serde_json::Value>,
    pub kwargs: String,
    pub memory_limit: Option<u64>,
    pub cpu_limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    TaskReceived(IncomingTask),
    PingReceived,
    ConnectionLost,
    Shutdown,
}

pub struct StreamHandler {
    config: ShepherdConfig,
    result_receiver: mpsc::UnboundedReceiver<TaskResultMessage>,
    event_sender: mpsc::UnboundedSender<StreamEvent>,
    shutdown_receiver: watch::Receiver<bool>,
    current_load: Arc<std::sync::atomic::AtomicU32>,
}

impl StreamHandler {
    pub fn new(
        config: ShepherdConfig,
        result_receiver: mpsc::UnboundedReceiver<TaskResultMessage>,
        event_sender: mpsc::UnboundedSender<StreamEvent>,
        shutdown_receiver: watch::Receiver<bool>,
        current_load: Arc<std::sync::atomic::AtomicU32>,
    ) -> Self {
        Self {
            config,
            result_receiver,
            event_sender,
            shutdown_receiver,
            current_load,
        }
    }

    pub async fn start(mut self) -> Result<()> {
        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Starting stream handler for shepherd {}", self.config.uuid);

        let mut retry_count = 0;
        let mut backoff = self.config.reconnect_backoff();

        loop {
            // Check for shutdown
            if *self.shutdown_receiver.borrow() {
                info!("Stream handler received shutdown signal");
                break;
            }

            match self.connect_and_handle().await {
                Ok(_) => {
                    info!("Stream handler connection ended normally");
                    retry_count = 0;
                    backoff = self.config.reconnect_backoff();
                }
                Err(e) => {
                    retry_count += 1;
                    error!("Stream handler connection failed (attempt {retry_count}): {e}");

                    if let Err(e) = self.event_sender.send(StreamEvent::ConnectionLost) {
                        error!("Failed to notify task manager of connection loss: {e}");
                    }

                    let jitter = Duration::from_millis(fastrand::u64(0..=1000));
                    let total_backoff = backoff + jitter;

                    warn!("Retrying connection in {total_backoff:?}");
                    sleep(total_backoff).await;

                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                }
            }
        }

        if let Err(e) = self.event_sender.send(StreamEvent::Shutdown) {
            error!("Failed to send shutdown event: {e}");
        }

        info!("Stream handler stopped");
        Ok(())
    }

    async fn connect_and_handle(&mut self) -> Result<()> {
        info!(
            "Connecting to orchestrator at {}",
            self.config.orchestrator_endpoint
        );

        let channel = Channel::from_shared(self.config.orchestrator_endpoint.clone())?
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .keep_alive_timeout(std::time::Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        let mut client = ClusterServiceClient::new(channel);

        let (tx, rx) = mpsc::channel(100);
        let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));

        let response = client.stream(request).await?;
        let mut response_stream = response.into_inner();

        info!("Connected to orchestrator, sending hello message");

        let hello_msg = ClientMsg {
            kind: Some(client_msg::Kind::Hello(Hello {
                shepherd_uuid: self.config.uuid.to_string(),
                max_concurrency: self.config.max_concurrency,
                domain: self.config.domain.clone(),
                shepherd_group: self.config.shepherd_group.clone(),
            })),
        };

        tx.send(hello_msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send hello: {}", e))?;
        let heartbeat_tx = tx.clone();
        let heartbeat_interval = self.config.heartbeat_interval();
        let current_load = self.current_load.clone();
        let max_concurrency = self.config.max_concurrency;

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = interval(heartbeat_interval);
            loop {
                interval.tick().await;

                let load = current_load.load(std::sync::atomic::Ordering::SeqCst);
                let available = max_concurrency.saturating_sub(load);

                let status_msg = ClientMsg {
                    kind: Some(client_msg::Kind::Status(Status {
                        current_load: load,
                        available_capacity: available,
                    })),
                };

                if heartbeat_tx.send(status_msg).await.is_err() {
                    debug!("Heartbeat channel closed, stopping heartbeat");
                    break;
                }
            }
        });

        // Handle the stream
        let result = self.handle_stream(tx.clone(), &mut response_stream).await;

        // Clean up heartbeat task
        heartbeat_handle.abort();

        result
    }

    /// Handle the bi-directional stream
    async fn handle_stream(
        &mut self,
        tx: mpsc::Sender<ClientMsg>,
        response_stream: &mut Streaming<ServerMsg>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                // Handle incoming messages from orchestrator
                message = response_stream.next() => {
                    match message {
                        Some(Ok(server_msg)) => {
                            if let Err(e) = self.handle_server_message(server_msg).await {
                                error!("Error handling server message: {e}");
                            }
                        }
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("Stream error: {}", e));
                        }
                        None => {
                            info!("Server closed the stream");
                            return Ok(());
                        }
                    }
                }

                // Handle task results from worker service
                result = self.result_receiver.recv() => {
                    match result {
                        Some(task_result) => {
                            let task_id = task_result.task_id;
                            debug!("Processing task result for task: {task_id}");
                            if let Err(e) = self.send_task_result(&tx, task_result).await {
                                error!("Failed to send task result: {e}");
                            } else {
                                debug!("Successfully sent task result for task: {task_id}");
                            }
                        }
                        None => {
                            warn!("Result receiver channel closed");
                            return Err(anyhow::anyhow!("Result receiver closed"));
                        }
                    }
                }

                // Check for shutdown
                _ = self.shutdown_receiver.changed() => {
                    if *self.shutdown_receiver.borrow() {
                        info!("Received shutdown signal in stream handler");
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Handle incoming server messages
    async fn handle_server_message(&mut self, server_msg: ServerMsg) -> Result<()> {
        match server_msg.kind {
            Some(server_msg::Kind::Task(task)) => {
                debug!("Received task: {}", task.task_id);

                let task_id = Uuid::parse_str(&task.task_id)
                    .map_err(|e| anyhow::anyhow!("Invalid task ID: {}", e))?;

                // Parse JSON args to Vec<serde_json::Value> to preserve types
                let args: Vec<serde_json::Value> = serde_json::from_str(&task.args)
                    .map_err(|e| anyhow::anyhow!("Failed to parse task args JSON: {}", e))?;

                let incoming_task = IncomingTask {
                    task_id,
                    name: task.name,
                    args,
                    kwargs: task.kwargs,
                    memory_limit: task.memory_limit,
                    cpu_limit: task.cpu_limit,
                };

                // Send task to task manager
                self.event_sender
                    .send(StreamEvent::TaskReceived(incoming_task))
                    .map_err(|e| anyhow::anyhow!("Failed to forward task: {}", e))?;
            }
            Some(server_msg::Kind::Ping(ping)) => {
                debug!("Received ping with timestamp: {}", ping.timestamp);

                // Handle ping events locally in stream_handler
                // No need to forward to task_manager
            }
            None => {
                warn!("Received empty server message");
            }
        }

        Ok(())
    }

    /// Send task result to orchestrator
    async fn send_task_result(
        &self,
        tx: &mpsc::Sender<ClientMsg>,
        task_result: TaskResultMessage,
    ) -> Result<()> {
        debug!(
            "Sending task result for task: {} to orchestrator",
            task_result.task_id
        );

        let client_msg = ClientMsg {
            kind: Some(client_msg::Kind::TaskResult(task_result.result)),
        };

        tx.send(client_msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send task result: {}", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common;

    #[tokio::test]
    async fn test_incoming_task_parsing() {
        let task = common::Task {
            task_id: Uuid::new_v4().to_string(),
            name: "test_task".to_string(),
            args: serde_json::to_string(&vec!["arg1".to_string(), "arg2".to_string()]).unwrap(),
            kwargs: r#"{"key": "value"}"#.to_string(),
            memory_limit: Some(1024),
            cpu_limit: Some(100),
        };

        let task_id = Uuid::parse_str(&task.task_id).unwrap();

        // Parse JSON args for test
        let args: Vec<serde_json::Value> = serde_json::from_str(&task.args).unwrap();

        let incoming_task = IncomingTask {
            task_id,
            name: task.name.clone(),
            args,
            kwargs: task.kwargs.clone(),
            memory_limit: task.memory_limit,
            cpu_limit: task.cpu_limit,
        };

        assert_eq!(incoming_task.task_id, task_id);
        assert_eq!(incoming_task.name, "test_task");
        assert_eq!(incoming_task.args.len(), 2);
        assert_eq!(incoming_task.memory_limit, Some(1024));
        assert_eq!(incoming_task.cpu_limit, Some(100));
    }

    #[tokio::test]
    async fn test_stream_event_creation() {
        let task_id = Uuid::new_v4();
        let incoming_task = IncomingTask {
            task_id,
            name: "test".to_string(),
            args: vec![],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
        };

        let event = StreamEvent::TaskReceived(incoming_task.clone());

        match event {
            StreamEvent::TaskReceived(task) => {
                assert_eq!(task.task_id, task_id);
                assert_eq!(task.name, "test");
            }
            _ => panic!("Expected TaskReceived event"),
        }
    }

    #[test]
    fn test_config_validation() {
        let config = ShepherdConfig {
            uuid: Uuid::new_v4(),
            orchestrator_endpoint: "http://127.0.0.1:52710".to_string(),
            max_concurrency: 4,
            worker_grpc_port: 50052,
            worker_binary_path: "./test-worker".to_string(),
            heartbeat_interval_secs: 30,
            reconnect_backoff_secs: 5,
            worker_timeout_secs: Some(300),
            log_level: Some("info".to_string()),
            domain: "test".to_string(),
            shepherd_group: "default".to_string(),
        };

        // Test that configuration values are preserved
        assert_eq!(config.max_concurrency, 4);
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(30));
        assert_eq!(config.reconnect_backoff(), Duration::from_secs(5));
    }
}
