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

    /// Build an orchestrator instance with the provided engine
    pub fn build(self, engine: Engine) -> Result<OrchestratorInstance> {
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

    /// Create database pool and run migrations using the builder's settings
    pub async fn create_engine(&self) -> Result<Engine> {
        // Create database pool and run migrations
        let pool = create_pool(&self.settings)?;
        run_migrations(&pool).await?;

        // Create orchestrator engine
        let event_stream_config = EventStreamConfig::from(&self.settings.event_stream);
        let engine = Engine::new(pool, event_stream_config, self.settings.domains.clone());
        engine.initialize().await?;

        Ok(engine)
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
        let addr = format!("0.0.0.0:{}", self.settings.server.port).parse::<SocketAddr>()?;

        log::info!("Azolla Orchestrator listening on {addr}");
        log::info!("Starting both ClientService and ClusterService");

        let server_future = Server::builder()
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
            .add_service(self.client_grpc_server)
            .add_service(self.cluster_grpc_server)
            .serve_with_shutdown(addr, shutdown_signal);

        let result = server_future.await;

        log::info!("Orchestrator terminated, shutting down engine...");
        let shutdown_timeout = self.settings.shutdown.timeout_secs;
        if let Err(e) = self.engine.shutdown_with_timeout(shutdown_timeout).await {
            log::error!("Error during engine shutdown: {e}");
        }

        result?;
        Ok(())
    }

    /// Start the orchestrator server in a spawned task with a watch-based shutdown
    pub fn serve_with_watch_shutdown(
        self,
        addr: SocketAddr,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>> {
        let shutdown_future = async move {
            let mut rx = shutdown_rx;
            rx.changed().await.ok();
        };

        log::info!("Starting orchestrator server on {addr}");
        let handle = tokio::spawn(async move {
            Server::builder()
                .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
                .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
                .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
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

/// A running orchestrator instance with lifecycle management
pub struct RunningOrchestratorInstance {
    pub engine: Engine,
    pub server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    pub shutdown_tx: watch::Sender<bool>,
}

impl RunningOrchestratorInstance {
    /// Create a new running orchestrator instance
    pub fn new(orchestrator: OrchestratorInstance, addr: SocketAddr) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let engine = orchestrator.engine.clone();
        let server_handle = orchestrator.serve_with_watch_shutdown(addr, shutdown_rx)?;

        Ok(Self {
            engine,
            server_handle,
            shutdown_tx,
        })
    }

    /// Get a reference to the engine
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Shutdown the orchestrator
    pub async fn shutdown(self) -> Result<()> {
        // Signal shutdown
        let _ = self.shutdown_tx.send(true);

        // Wait for server to shutdown
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.server_handle).await;

        // Shutdown engine
        self.engine.shutdown().await
    }
}
