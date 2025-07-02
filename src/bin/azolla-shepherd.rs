use anyhow::Result;
use clap::{Arg, Command};
use log::info;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::{mpsc, watch};

use azolla::shepherd::{load_config, start_worker_service, StreamHandler, TaskManager};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = Command::new("azolla-shepherd")
        .version("0.1.0")
        .about("Azolla Shepherd - Worker daemon for executing tasks")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Configuration file path")
                .default_value("config/shepherd.toml"),
        )
        .arg(
            Arg::new("orchestrator")
                .short('o')
                .long("orchestrator")
                .value_name("ENDPOINT")
                .help("Orchestrator gRPC endpoint")
                .default_value("http://127.0.0.1:52710"),
        )
        .arg(
            Arg::new("max-concurrency")
                .short('m')
                .long("max-concurrency")
                .value_name("NUMBER")
                .help("Maximum concurrent tasks")
                .default_value("4"),
        )
        .arg(
            Arg::new("worker-port")
                .short('p')
                .long("worker-port")
                .value_name("PORT")
                .help("Port for worker gRPC service")
                .default_value("50052"),
        )
        .arg(
            Arg::new("worker-binary")
                .short('b')
                .long("worker-binary")
                .value_name("PATH")
                .help("Path to worker binary")
                .default_value("./azolla-worker"),
        )
        .arg(
            Arg::new("log-level")
                .short('l')
                .long("log-level")
                .value_name("LEVEL")
                .help("Log level (error, warn, info, debug, trace)")
                .default_value("info"),
        )
        .get_matches();

    // Load configuration from file and CLI args
    let config_path = matches.get_one::<String>("config");
    let config = load_config(config_path.map(|s| s.as_str()), &matches)?;

    info!(
        "Starting Azolla Shepherd {} with config: {:?}",
        config.uuid, config
    );

    let (shutdown_tx, shutdown_rx1) = watch::channel(false);
    let shutdown_rx2 = shutdown_rx1.clone();
    let shutdown_rx3 = shutdown_rx1.clone();

    let (result_tx, result_rx) = mpsc::unbounded_channel();
    let (stream_event_tx, stream_event_rx) = mpsc::unbounded_channel();

    let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

    info!("Starting Azolla Shepherd components...");

    let worker_service_handle = {
        let port = config.worker_grpc_port;
        let result_sender = result_tx;
        let _shutdown_rx = shutdown_rx1;

        tokio::spawn(async move {
            // TODO: Update start_worker_service to accept shutdown signal
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

    // Wait for shutdown signal
    info!(
        "Shepherd {} is running. Press Ctrl+C to shutdown.",
        config.uuid
    );
    signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping shepherd...");

    if let Err(e) = shutdown_tx.send(true) {
        log::error!("Failed to send shutdown signal: {e}");
    }

    let _ = tokio::join!(
        worker_service_handle,
        stream_handler_handle,
        task_manager_handle
    );

    info!("Azolla Shepherd shutdown complete");
    Ok(())
}
