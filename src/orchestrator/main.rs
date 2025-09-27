use anyhow::Result;
use tokio::signal;

use azolla::orchestrator::db::Settings;
use azolla::orchestrator::startup::OrchestratorBuilder;

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

    // Build and start orchestrator using the abstraction
    let builder = OrchestratorBuilder::new(settings);
    let engine = builder
        .create_engine()
        .await
        .expect("Failed to create engine");
    let orchestrator = builder.build(engine).expect("Failed to build orchestrator");

    orchestrator.serve_with_shutdown(shutdown_signal()).await?;

    Ok(())
}
