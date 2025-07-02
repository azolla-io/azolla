use anyhow::Result;
use tokio::signal;
use tonic::transport::Server;

use azolla::orchestrator::cluster_service::ClusterServiceImpl;
use azolla::orchestrator::db::{create_pool, run_migrations, Settings};
use azolla::orchestrator::engine::Engine;
use azolla::orchestrator::event_stream::EventStreamConfig;
use azolla::orchestrator::ClientServiceImpl;
use azolla::proto::orchestrator::client_service_server::ClientServiceServer;

/// Wait for shutdown signal (CTRL+C or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            log::info!("Received CTRL+C, shutting down...");
        },
        _ = terminate => {
            log::info!("Received SIGTERM, shutting down...");
        },
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn");
    }
    env_logger::init();

    let settings = Settings::new().expect("Failed to load settings");
    log::info!("Loaded settings: {settings:?}");
    let addr = format!("[::1]:{}", settings.server.port).parse()?;

    let pool = create_pool(&settings).expect("Failed to create database pool");

    run_migrations(&pool).await?;

    let event_stream_config = EventStreamConfig::from(&settings.event_stream);

    // Create the central orchestration engine with all shared state
    let engine = Engine::new(pool, event_stream_config);
    engine.initialize().await?;

    // Create services using direct construction for consistency
    let client_service = ClientServiceImpl::new(engine.clone());
    let client_grpc_server = ClientServiceServer::new(client_service);

    let cluster_service = ClusterServiceImpl::new(engine.clone());
    let cluster_grpc_server = cluster_service.into_server();

    log::info!("Azolla Orchestrator listening on {addr}");
    log::info!("Starting both ClientService and ClusterService");

    let server_future = Server::builder()
        .add_service(client_grpc_server)
        .add_service(cluster_grpc_server)
        .serve_with_shutdown(addr, shutdown_signal());

    let result = server_future.await;

    log::info!("Orchestrator terminated, shutting down engine...");
    if let Err(e) = engine.shutdown().await {
        log::error!("Error during engine shutdown: {e}");
    }

    result?;

    Ok(())
}
