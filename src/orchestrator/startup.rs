use anyhow::Result;
use std::net::SocketAddr;
use tokio::sync::watch;
use tonic::transport::Server;

use crate::orchestrator::client_service::ClientServiceImpl;
use crate::orchestrator::cluster_service::ClusterServiceImpl;
use crate::orchestrator::db::{create_pool, run_migrations, Settings};
use crate::orchestrator::engine::Engine;
use crate::orchestrator::event_stream::EventStreamConfig;
use crate::proto::orchestrator::client_service_server::ClientServiceServer;
use crate::proto::orchestrator::cluster_service_server::ClusterServiceServer;

/// Builder for creating orchestrator instances with common configuration
pub struct OrchestratorBuilder {
    settings: Settings,
}

impl OrchestratorBuilder {
    /// Create a new builder with the provided settings
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    /// Build and initialize an orchestrator instance
    pub async fn build(self) -> Result<OrchestratorInstance> {
        // Create database pool and run migrations
        let pool = create_pool(&self.settings)?;
        run_migrations(&pool).await?;

        // Create orchestrator engine
        let event_stream_config = EventStreamConfig::from(&self.settings.event_stream);
        let engine = Engine::new(pool, event_stream_config);
        engine.initialize().await?;

        // Create services
        let client_service = ClientServiceImpl::new(engine.clone());
        let cluster_service = ClusterServiceImpl::new(engine.clone());

        // Create gRPC servers
        let client_grpc_server = ClientServiceServer::new(client_service);
        let cluster_grpc_server = cluster_service.into_server();

        Ok(OrchestratorInstance {
            engine,
            client_grpc_server,
            cluster_grpc_server,
            settings: self.settings,
        })
    }
}

/// A fully initialized orchestrator instance ready to serve requests
pub struct OrchestratorInstance {
    pub engine: Engine,
    pub client_grpc_server: ClientServiceServer<ClientServiceImpl>,
    pub cluster_grpc_server: ClusterServiceServer<ClusterServiceImpl>,
    pub settings: Settings,
}

impl OrchestratorInstance {
    /// Start the orchestrator server and run until shutdown signal
    pub async fn serve_with_shutdown<F>(self, shutdown_signal: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        let addr = format!("[::1]:{}", self.settings.server.port).parse::<SocketAddr>()?;

        log::info!("Azolla Orchestrator listening on {addr}");
        log::info!("Starting both ClientService and ClusterService");

        let server_future = Server::builder()
            .add_service(self.client_grpc_server)
            .add_service(self.cluster_grpc_server)
            .serve_with_shutdown(addr, shutdown_signal);

        let result = server_future.await;

        log::info!("Orchestrator terminated, shutting down engine...");
        if let Err(e) = self.engine.shutdown().await {
            log::error!("Error during engine shutdown: {e}");
        }

        result?;
        Ok(())
    }

    /// Start the orchestrator server in a spawned task with a watch-based shutdown
    pub async fn serve_with_watch_shutdown(
        self,
        addr: SocketAddr,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>> {
        let shutdown_future = async move {
            shutdown_rx.changed().await.ok();
        };

        log::info!("Starting orchestrator server on {addr}");
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(self.client_grpc_server)
                .add_service(self.cluster_grpc_server)
                .serve_with_shutdown(addr, shutdown_future)
                .await
        });

        Ok(handle)
    }

    /// Get a reference to the engine for testing or other purposes
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Shutdown the orchestrator engine
    pub async fn shutdown(self) -> Result<()> {
        self.engine.shutdown().await
    }
}
