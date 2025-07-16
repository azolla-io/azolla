pub mod config;
pub mod stream_handler;
pub mod task_manager;
pub mod worker_service;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

// Re-export commonly used types
pub use config::{load_config, ShepherdConfig};
pub use stream_handler::{IncomingTask, StreamEvent, StreamHandler};
pub use task_manager::{TaskInfo, TaskManager, TaskManagerStats, TaskStatus};
pub use worker_service::{start_worker_service, TaskResultMessage, WorkerService};

/// Handle to control a running shepherd instance
pub struct ShepherdInstance {
    pub config: ShepherdConfig,
    pub shutdown_tx: watch::Sender<bool>,
    pub current_load: Arc<std::sync::atomic::AtomicU32>,
    pub worker_service_handle: tokio::task::JoinHandle<()>,
    pub stream_handler_handle: tokio::task::JoinHandle<()>,
    pub task_manager_handle: tokio::task::JoinHandle<()>,
}

impl ShepherdInstance {
    pub fn get_current_load(&self) -> u32 {
        self.current_load.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    pub async fn join(self) -> Result<()> {
        let _ = tokio::join!(
            self.worker_service_handle,
            self.stream_handler_handle,
            self.task_manager_handle
        );
        Ok(())
    }
}

/// Start a shepherd instance with the given configuration
pub async fn start_shepherd(config: ShepherdConfig) -> Result<ShepherdInstance> {
    log::info!(
        "Starting Azolla Shepherd {} on port {} with max_concurrency={}",
        config.uuid,
        config.worker_grpc_port,
        config.max_concurrency
    );

    let (shutdown_tx, shutdown_rx1) = watch::channel(false);
    let shutdown_rx2 = shutdown_rx1.clone();
    let shutdown_rx3 = shutdown_rx1.clone();

    let (result_tx, result_rx) = mpsc::unbounded_channel();
    let (stream_event_tx, stream_event_rx) = mpsc::unbounded_channel();

    let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

    log::info!("Starting Azolla Shepherd components...");

    let worker_service_handle = {
        let port = config.worker_grpc_port;
        let result_sender = result_tx;

        tokio::spawn(async move {
            if let Err(e) = start_worker_service(port, result_sender).await {
                log::error!("Worker service error: {e}");
            }
        })
    };

    let stream_handler_handle = {
        let config = config.clone();
        let current_load = current_load.clone();

        tokio::spawn(async move {
            let stream_handler = StreamHandler::new(
                config,
                result_rx,
                stream_event_tx,
                shutdown_rx2,
                current_load,
            );

            if let Err(e) = stream_handler.start().await {
                log::error!("Stream handler error: {e}");
            }
        })
    };

    let task_manager_handle = {
        let config = config.clone();
        let current_load = current_load.clone();

        tokio::spawn(async move {
            let task_manager =
                TaskManager::new(config, stream_event_rx, shutdown_rx3, current_load);

            if let Err(e) = task_manager.start().await {
                log::error!("Task manager error: {e}");
            }
        })
    };

    Ok(ShepherdInstance {
        config,
        shutdown_tx,
        current_load,
        worker_service_handle,
        stream_handler_handle,
        task_manager_handle,
    })
}
