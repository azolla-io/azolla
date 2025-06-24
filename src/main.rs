use anyhow::Result;
use tonic::transport::Server;
use tokio::signal;

mod db;
mod event_stream;
mod server;
mod taskset;

use db::{create_pool, run_migrations, Settings};
use server::create_server;

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
    // Initialize logging with warn level by default
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn");
    }
    env_logger::init();
    
    // Load configuration
    let settings = Settings::new().expect("Failed to load settings");
    log::info!("Loaded settings: {:?}", settings);
    let addr = format!("[::1]:{}", settings.server.port).parse()?;
    
    // Create and connect database pool
    let pool = create_pool(&settings).expect("Failed to create database pool");
    
    // Run database migrations
    run_migrations(&pool).await?;

    // Create the gRPC server with initialized TaskSetRegistry
    let (service, grpc_server) = create_server(pool).await?;
    
    log::info!("Azolla Orchestrator listening on {}", addr);

    // Start the server with graceful shutdown
    let server_future = Server::builder()
        .add_service(grpc_server)
        .serve_with_shutdown(addr, shutdown_signal());
    
    // Wait for server to complete or shutdown signal
    let result = server_future.await;
    
    // Shutdown the service to print metrics
    log::info!("Orchestrator terminated, shutting down service...");
    if let Err(e) = service.shutdown().await {
        log::error!("Error during service shutdown: {}", e);
    }
    
    result?;

    Ok(())
}
