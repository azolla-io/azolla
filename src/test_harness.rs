use anyhow::Result;
use std::time::Duration;
use tokio::sync::watch;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::orchestrator::db::{Database, EventStream, Server as DbServer, Settings};
use crate::orchestrator::engine::Engine;
use crate::orchestrator::startup::OrchestratorBuilder;
use crate::proto::orchestrator::client_service_client::ClientServiceClient;
use crate::proto::orchestrator::cluster_service_client::ClusterServiceClient;
use crate::proto::orchestrator::CreateTaskRequest;
use crate::shepherd::{start_shepherd, ShepherdConfig};

use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

pub struct IntegrationTestEnvironment {
    pub orchestrator_addr: String,
    pub shepherd_config: ShepherdConfig,
    pub client: ClientServiceClient<Channel>,
    pub cluster_client: ClusterServiceClient<Channel>,
    pub engine: Engine,

    // Component handles for lifecycle management
    orchestrator_handle: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    shepherd_handles: Vec<crate::shepherd::ShepherdInstance>,
    shutdown_tx: Option<watch::Sender<bool>>,

    // Database container for cleanup
    #[allow(dead_code)]
    db_container: ContainerAsync<GenericImage>,
}

impl IntegrationTestEnvironment {
    pub async fn new() -> Result<Self> {
        Self::with_config(IntegrationTestConfig::default()).await
    }

    pub async fn with_config(config: IntegrationTestConfig) -> Result<Self> {
        // Start database container
        let db_container = GenericImage::new("postgres", "16-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_exposed_port(5432u16.tcp())
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_DB", "postgres")
            .start()
            .await?;

        tokio::time::sleep(Duration::from_millis(1000)).await;
        let db_port = db_container.get_host_port_ipv4(5432).await?;

        // Create database settings
        let db_url =
            format!("postgres://postgres:postgres@127.0.0.1:{db_port}/postgres?sslmode=disable");

        let settings = Settings {
            database: Database {
                url: db_url,
                pool_size: 8,
            },
            server: DbServer {
                port: config.orchestrator_port,
            },
            event_stream: EventStream::default(),
        };

        // Build orchestrator using the abstraction
        let orchestrator = OrchestratorBuilder::new(settings).build().await?;
        let engine = orchestrator.engine().clone();

        // Start orchestrator server
        let orchestrator_addr = format!("127.0.0.1:{}", config.orchestrator_port);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let orchestrator_handle = {
            let addr = orchestrator_addr.parse()?;
            orchestrator
                .serve_with_watch_shutdown(addr, shutdown_rx)
                .await?
        };

        // Wait for orchestrator to be ready and create clients with retry
        let client_addr = format!("http://127.0.0.1:{}", config.orchestrator_port);
        let (client, cluster_client) = {
            let mut retries = 0;
            loop {
                match tokio::try_join!(
                    ClientServiceClient::connect(client_addr.clone()),
                    ClusterServiceClient::connect(client_addr.clone())
                ) {
                    Ok((client, cluster_client)) => break (client, cluster_client),
                    Err(_e) if retries < 10 => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        };

        // Create shepherd config
        let shepherd_config = ShepherdConfig {
            uuid: Uuid::new_v4(),
            orchestrator_endpoint: client_addr,
            worker_grpc_port: config.shepherd_worker_port,
            worker_binary_path: config.worker_binary_path.clone(),
            max_concurrency: config.max_concurrent_tasks as u32,
            log_level: Some("info".to_string()),
            heartbeat_interval_secs: 30,
            reconnect_backoff_secs: 5,
            worker_timeout_secs: Some(300),
        };

        Ok(Self {
            orchestrator_addr: orchestrator_addr.clone(),
            shepherd_config,
            client,
            cluster_client,
            engine,
            orchestrator_handle: Some(orchestrator_handle),
            shepherd_handles: Vec::new(),
            shutdown_tx: Some(shutdown_tx),
            db_container,
        })
    }

    pub async fn start_shepherd(&mut self) -> Result<&crate::shepherd::ShepherdInstance> {
        let config = self.shepherd_config.clone();
        let shepherd_handle = start_shepherd(config).await?;
        self.shepherd_handles.push(shepherd_handle);
        Ok(self.shepherd_handles.last().unwrap())
    }

    pub async fn wait_for_task_completion(
        &self,
        task_id: &str,
        timeout_duration: Duration,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        while start.elapsed() < timeout_duration {
            // First try to merge events to ensure database is up to date
            if let Err(e) = self.engine.merge_events_to_db().await {
                log::warn!("Failed to merge events: {e}");
            }

            // Check task status from database
            let pool = &self.engine.pool;
            let client = pool.get().await?;

            let row = client
                .query_opt(
                    "SELECT status FROM task_instance WHERE id = $1",
                    &[&task_uuid],
                )
                .await?;

            if let Some(row) = row {
                let status: i16 = row.get(0);
                // Check if task is completed (succeeded or failed)
                if status == crate::TASK_STATUS_SUCCEEDED || status == crate::TASK_STATUS_FAILED {
                    return Ok(true);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(false)
    }

    pub async fn get_task_status(&self, task_id: &str) -> Result<Option<i16>> {
        let pool = &self.engine.pool;
        let client = pool.get().await?;
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        let row = client
            .query_opt(
                "SELECT status FROM task_instance WHERE id = $1",
                &[&task_uuid],
            )
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    pub async fn get_task_attempts(&self, task_id: &str) -> Result<Vec<TaskAttempt>> {
        let pool = &self.engine.pool;
        let client = pool.get().await?;
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        let rows = client
            .query(
                "SELECT attempt, status, start_time, end_time 
                 FROM task_attempts 
                 WHERE task_instance_id = $1 
                 ORDER BY attempt",
                &[&task_uuid],
            )
            .await?;

        let mut attempts = Vec::new();
        for row in rows {
            attempts.push(TaskAttempt {
                attempt_number: row.get(0),
                status: row.get(1),
                started_at: row.get(2),
                ended_at: row.get(3),
                error_message: None, // This field doesn't exist in the schema
            });
        }

        Ok(attempts)
    }

    pub async fn get_shepherd_count(&self) -> Result<i64> {
        // Shepherds are tracked in-memory by the orchestrator's cluster service
        // Since we don't have direct access to the cluster service state in tests,
        // we'll use the number of shepherd handles we've created as a proxy
        Ok(self.shepherd_handles.len() as i64)
    }

    pub async fn ensure_worker_binary(&self) -> Result<()> {
        let binary_path = &self.shepherd_config.worker_binary_path;

        if !std::path::Path::new(binary_path).exists() {
            log::info!("Worker binary not found at {binary_path}, building...");

            let output = std::process::Command::new("cargo")
                .args(["build", "--bin", "azolla-worker"])
                .output()?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "Failed to build worker binary: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        // Signal shutdown to all components
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }

        // Wait for orchestrator to shutdown
        if let Some(handle) = self.orchestrator_handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        // Wait for shepherd components to shutdown
        for handle in self.shepherd_handles.drain(..) {
            let _ = handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        }

        // Shutdown engine
        self.engine.shutdown().await?;

        Ok(())
    }
}

impl Drop for IntegrationTestEnvironment {
    fn drop(&mut self) {
        // Attempt graceful shutdown in drop
        if self.shutdown_tx.is_some() {
            let _ = futures::executor::block_on(self.shutdown());
        }
    }
}

#[derive(Debug, Clone)]
pub struct IntegrationTestConfig {
    pub orchestrator_port: u16,
    pub shepherd_worker_port: u16,
    pub worker_binary_path: String,
    pub max_concurrent_tasks: usize,
}

impl Default for IntegrationTestConfig {
    fn default() -> Self {
        Self {
            orchestrator_port: find_available_port(),
            shepherd_worker_port: find_available_port(),
            worker_binary_path: "./target/debug/azolla-worker".to_string(),
            max_concurrent_tasks: 4,
        }
    }
}

pub fn find_available_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    addr.port()
}

pub struct TaskAttempt {
    pub attempt_number: i32,
    pub status: i16,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error_message: Option<String>,
}

pub struct TaskTestData;

impl TaskTestData {
    pub fn echo_task(message: &str) -> CreateTaskRequest {
        CreateTaskRequest {
            name: "echo".to_string(),
            domain: "test".to_string(),
            args: vec![message.to_string()],
            kwargs: "{}".to_string(),
            retry_policy: r#"{"max_attempts": 1}"#.to_string(),
            flow_instance_id: None,
        }
    }

    pub fn flaky_task_that_succeeds_on_retry() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "flaky_task".to_string(),
            domain: "test".to_string(),
            args: vec![],
            kwargs: r#"{"fail_first_attempt": true}"#.to_string(),
            retry_policy: r#"{"max_attempts": 3, "backoff": "linear"}"#.to_string(),
            flow_instance_id: None,
        }
    }

    pub fn failing_task_with_retries() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "always_fail".to_string(),
            domain: "test".to_string(),
            args: vec![],
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: r#"{"max_attempts": 3, "backoff": "exponential"}"#.to_string(),
            flow_instance_id: None,
        }
    }
}
