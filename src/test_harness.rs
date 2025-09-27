use anyhow::Result;
use serde_json::json;
use std::time::Duration;
use tokio_postgres::NoTls;
use tonic::transport::Channel;
use uuid::Uuid;

// Default configuration constants
const DEFAULT_DB_STARTUP_TIMEOUT_MS: u64 = 1000;
const DEFAULT_CLIENT_CONNECTION_RETRY_INTERVAL_MS: u64 = 100;
const DEFAULT_CLIENT_CONNECTION_MAX_RETRIES: usize = 10;
const DEFAULT_TASK_COMPLETION_POLL_INTERVAL_MS: u64 = 100;
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 4;
const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 60;

use crate::orchestrator::db::{
    Database, DomainsConfig, EventStream, Server as DbServer, Settings, ShutdownConfig,
};
use crate::orchestrator::retry_policy::RetryPolicy as InternalRetryPolicy;
use crate::orchestrator::startup::{OrchestratorBuilder, RunningOrchestratorInstance};
use crate::proto::common::RetryPolicy as ProtoRetryPolicy;
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
    config: IntegrationTestConfig,

    // Component instances for lifecycle management
    orchestrator_instance: Option<RunningOrchestratorInstance>,
    shepherd_handles: Vec<crate::shepherd::ShepherdInstance>,

    database_name: Option<String>,
    db_port: u16,

    // Database container for cleanup
    #[allow(dead_code)]
    db_container: ContainerAsync<GenericImage>,
}

impl IntegrationTestEnvironment {
    /// Creates a new integration test environment with default configuration.
    ///
    /// This will start a PostgreSQL container, initialize the orchestrator,
    /// and set up all necessary components for integration testing.
    pub async fn new() -> Result<Self> {
        Self::with_config(IntegrationTestConfig::default()).await
    }

    /// Creates a new integration test environment with custom configuration.
    ///
    /// This allows you to override default timeouts, ports, and other test parameters.
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

        tokio::time::sleep(Duration::from_millis(config.db_startup_timeout_ms)).await;
        let db_port = db_container.get_host_port_ipv4(5432).await?;

        // Create per-test database for isolation
        log::info!("Integration test harness initializing postgres on port {db_port}");
        let (database_name, db_url) = Self::create_isolated_database(db_port).await?;
        log::info!(
            "Integration test harness connected to database {database_name} on port {db_port}"
        );

        let settings = Settings {
            database: Database {
                url: db_url,
                pool_size: 8,
            },
            server: DbServer {
                port: config.orchestrator_port,
            },
            event_stream: EventStream::default(),
            domains: DomainsConfig::default(),
            shutdown: ShutdownConfig::default(),
        };

        // Build and start orchestrator using the abstraction
        let builder = OrchestratorBuilder::new(settings);
        let engine = builder.create_engine().await?;
        let orchestrator = builder.build(engine.clone())?;

        // Start orchestrator server
        let orchestrator_addr = format!("127.0.0.1:{}", config.orchestrator_port);
        let addr = orchestrator_addr.parse()?;
        let orchestrator_instance = RunningOrchestratorInstance::new(orchestrator, addr)?;

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
                    Err(_e) if retries < config.client_connection_max_retries => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(
                            config.client_connection_retry_interval_ms,
                        ))
                        .await;
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
            domain: "test".to_string(),
            shepherd_group: "default".to_string(),
        };

        Ok(Self {
            orchestrator_addr: orchestrator_addr.clone(),
            shepherd_config,
            client,
            cluster_client,
            config,
            orchestrator_instance: Some(orchestrator_instance),
            shepherd_handles: Vec::new(),
            database_name: Some(database_name),
            db_port,
            db_container,
        })
    }

    pub fn engine(&self) -> &crate::orchestrator::engine::Engine {
        self.orchestrator_instance.as_ref().unwrap().engine()
    }

    /// Starts a new shepherd instance and registers it with the orchestrator.
    ///
    /// Returns a reference to the shepherd instance that can be used to
    /// check its status and configuration.
    pub async fn start_shepherd(&mut self) -> Result<&crate::shepherd::ShepherdInstance> {
        let config = self.shepherd_config.clone();
        let shepherd_handle = start_shepherd(config).await?;
        self.shepherd_handles.push(shepherd_handle);
        Ok(self.shepherd_handles.last().unwrap())
    }

    /// Waits for a task to complete (succeed or fail) within the specified timeout.
    ///
    /// This method polls the in-memory TaskSet through the SchedulerActor to check
    /// task status. Returns `true` if the task completed, `false` if it timed out.
    ///
    /// # Arguments
    /// * `task_id` - The UUID of the task to wait for
    /// * `timeout_duration` - Maximum time to wait for completion
    pub async fn wait_for_task_completion(
        &self,
        task_id: &str,
        timeout_duration: Duration,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        while start.elapsed() < timeout_duration {
            // Check task status through SchedulerActor (which owns the TaskSet)
            // TaskSet updates are synchronous, so we should see status changes immediately
            if let Some(scheduler) = self.engine().scheduler_registry.get_scheduler("test") {
                if let Ok(Some(task)) = scheduler.get_task_for_test(task_uuid).await {
                    // Check if task is completed (succeeded or failed)
                    if task.status == crate::TASK_STATUS_SUCCEEDED
                        || task.status == crate::TASK_STATUS_FAILED
                    {
                        return Ok(true);
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(
                self.config.task_completion_poll_interval_ms,
            ))
            .await;
        }

        Ok(false)
    }

    /// Gets the current status of a task from the in-memory TaskSet.
    ///
    /// Returns the task status code (e.g., TASK_STATUS_SUCCEEDED, TASK_STATUS_FAILED)
    /// or `None` if the task is not found.
    pub async fn get_task_status(&self, task_id: &str) -> Result<Option<i16>> {
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        // Check task status through SchedulerActor (which owns the TaskSet)
        // TaskSet updates are synchronous, so this should reflect current state
        if let Some(scheduler) = self.engine().scheduler_registry.get_scheduler("test") {
            if let Ok(Some(task)) = scheduler.get_task_for_test(task_uuid).await {
                return Ok(Some(task.status));
            }
        }

        // Try other domains if not found in test domain
        let domains = self.engine().scheduler_registry.domains();
        for domain in domains {
            if domain != "test" {
                if let Some(scheduler) = self.engine().scheduler_registry.get_scheduler(&domain) {
                    if let Ok(Some(task)) = scheduler.get_task_for_test(task_uuid).await {
                        return Ok(Some(task.status));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Gets all attempts for a task from the in-memory TaskSet.
    ///
    /// Returns a vector of TaskAttempt objects containing attempt details
    /// like attempt number, start/end times, and status.
    pub async fn get_task_attempts(&self, task_id: &str) -> Result<Vec<TaskAttempt>> {
        let task_uuid = uuid::Uuid::parse_str(task_id)?;

        // Get task attempts from TaskSet through SchedulerActor
        if let Some(scheduler) = self.engine().scheduler_registry.get_scheduler("test") {
            if let Ok(Some(task)) = scheduler.get_task_for_test(task_uuid).await {
                let attempts = task
                    .attempts
                    .into_iter()
                    .map(|attempt| TaskAttempt {
                        attempt_number: attempt.attempt,
                        status: 0, // TaskAttempt status is not tracked in the in-memory version
                        started_at: attempt.start_time.unwrap_or_else(chrono::Utc::now),
                        ended_at: attempt.end_time,
                        error_message: None,
                    })
                    .collect();
                return Ok(attempts);
            }
        }

        // Try other domains if not found in test domain
        let domains = self.engine().scheduler_registry.domains();
        for domain in domains {
            if domain != "test" {
                if let Some(scheduler) = self.engine().scheduler_registry.get_scheduler(&domain) {
                    if let Ok(Some(task)) = scheduler.get_task_for_test(task_uuid).await {
                        let attempts = task
                            .attempts
                            .into_iter()
                            .map(|attempt| TaskAttempt {
                                attempt_number: attempt.attempt,
                                status: 0, // TaskAttempt status is not tracked in the in-memory version
                                started_at: attempt.start_time.unwrap_or_else(chrono::Utc::now),
                                ended_at: attempt.end_time,
                                error_message: None,
                            })
                            .collect();
                        return Ok(attempts);
                    }
                }
            }
        }

        Ok(Vec::new())
    }

    pub async fn get_shepherd_count(&self) -> Result<i64> {
        // Get shepherd count from any domain manager (tests use single domain 'test')
        let stats = self
            .engine()
            .shepherd_registry
            .get_or_create_manager("test")
            .get_stats()
            .await;
        Ok(stats.connected_shepherds as i64)
    }

    /// Check if a specific shepherd is registered and connected with the orchestrator
    pub async fn is_shepherd_registered(&self, shepherd_uuid: uuid::Uuid) -> Result<bool> {
        // With per-domain ShepherdManagers, we need to check all domains
        // since we don't know which domain the shepherd is in
        let managers = self.engine().shepherd_registry.list_managers();

        for manager in managers {
            if manager.is_shepherd_registered(shepherd_uuid).await {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Wait for a specific shepherd to be registered, with timeout
    pub async fn wait_for_shepherd_registration(
        &self,
        shepherd_uuid: uuid::Uuid,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if self.is_shepherd_registered(shepherd_uuid).await? {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        Ok(false)
    }

    /// Wait for any shepherd to be registered, with timeout  
    pub async fn wait_for_any_shepherd(&self, timeout: std::time::Duration) -> Result<bool> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            let count = self.get_shepherd_count().await?;
            if count > 0 {
                return Ok(true);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        Ok(false)
    }

    /// Ensures the worker binary is built and available for testing.
    ///
    /// If the binary doesn't exist, this will run `cargo build --bin azolla-worker`
    /// to build it automatically.
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

    /// Shuts down all components of the test environment.
    ///
    /// This stops all shepherd instances, shuts down the orchestrator,
    /// and cleans up resources. The database container is automatically
    /// cleaned up when the struct is dropped.
    pub async fn shutdown(&mut self) -> Result<()> {
        log::info!("Integration test harness shutdown initiated");
        // Wait for shepherd components to shutdown
        for handle in self.shepherd_handles.drain(..) {
            let _ = handle.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        }

        // Shutdown orchestrator with extended timeout for tests
        if let Some(orchestrator) = self.orchestrator_instance.take() {
            log::info!("Stopping orchestrator engine");
            // Allow tests to control shutdown duration; default keeps teardown under a minute
            let timeout_secs = self.config.shutdown_timeout_secs;
            orchestrator
                .engine
                .shutdown_with_timeout(timeout_secs)
                .await?;
            log::info!("Orchestrator engine stopped");
        }

        if let Some(database_name) = self.database_name.take() {
            log::info!("Cleaning up database {database_name}");
            if let Err(e) = Self::drop_database(self.db_port, &database_name).await {
                log::warn!("Failed to drop test database {database_name}: {e}");
            }
            log::info!("Database cleanup completed for {database_name}");
        }

        log::info!("Integration test harness shutdown completed");
        Ok(())
    }

    /// Wait for EVENT_TASK_ATTEMPT_STARTED for a task and return its metadata JSON.
    /// Useful to assert routing fields like shepherd_group and shepherd_uuid.
    pub async fn wait_for_attempt_started_metadata(
        &self,
        task_id: &uuid::Uuid,
        domain: &str,
        timeout: Duration,
    ) -> Result<Option<serde_json::Value>> {
        let start = std::time::Instant::now();
        let pool = &self.engine().pool;
        let client = pool.get().await?;

        while start.elapsed() < timeout {
            let row = client
                .query_opt(
                    "SELECT metadata FROM events WHERE task_instance_id = $1 AND domain = $2 AND event_type = $3 ORDER BY event_id DESC LIMIT 1",
                    &[task_id, &domain, &crate::EVENT_TASK_ATTEMPT_STARTED],
                )
                .await?;

            if let Some(row) = row {
                let metadata: serde_json::Value = row.get("metadata");
                return Ok(Some(metadata));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(None)
    }

    async fn create_isolated_database(db_port: u16) -> Result<(String, String)> {
        let unique_id = Uuid::new_v4().simple().to_string();
        let database_name = format!("azolla_test_{unique_id}");
        let admin_url =
            format!("postgres://postgres:postgres@127.0.0.1:{db_port}/postgres?sslmode=disable");

        let (client, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::debug!("Postgres admin connection closed during test database creation: {e}");
            }
        });

        log::info!("Creating isolated test database {database_name} on port {db_port}");
        client
            .batch_execute(&format!("CREATE DATABASE {database_name}"))
            .await?;

        drop(client);
        if let Err(e) = connection_task.await {
            log::debug!("Postgres admin connection join error during test database creation: {e}");
        }

        let db_url = format!(
            "postgres://postgres:postgres@127.0.0.1:{db_port}/{database_name}?sslmode=disable"
        );

        Ok((database_name, db_url))
    }

    async fn drop_database(db_port: u16, database_name: &str) -> Result<()> {
        let admin_url =
            format!("postgres://postgres:postgres@127.0.0.1:{db_port}/postgres?sslmode=disable");

        let (client, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::debug!("Postgres admin connection closed during test database cleanup: {e}");
            }
        });

        log::info!("Dropping isolated test database {database_name} on port {db_port}");
        client
            .batch_execute(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database_name}'"
            ))
            .await?;
        client
            .batch_execute(&format!("DROP DATABASE IF EXISTS {database_name}"))
            .await?;

        drop(client);
        if let Err(e) = connection_task.await {
            log::debug!("Postgres admin connection join error during test database cleanup: {e}");
        }

        Ok(())
    }
}

impl Drop for IntegrationTestEnvironment {
    fn drop(&mut self) {
        // Attempt graceful shutdown in drop
        if self.orchestrator_instance.is_some() {
            log::info!("Integration test harness drop triggered");
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
    pub db_startup_timeout_ms: u64,
    pub client_connection_retry_interval_ms: u64,
    pub client_connection_max_retries: usize,
    pub task_completion_poll_interval_ms: u64,
    pub shutdown_timeout_secs: u64,
}

impl Default for IntegrationTestConfig {
    fn default() -> Self {
        Self {
            orchestrator_port: find_available_port(),
            shepherd_worker_port: find_available_port(),
            worker_binary_path: "./target/debug/azolla-worker".to_string(),
            max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
            db_startup_timeout_ms: DEFAULT_DB_STARTUP_TIMEOUT_MS,
            client_connection_retry_interval_ms: DEFAULT_CLIENT_CONNECTION_RETRY_INTERVAL_MS,
            client_connection_max_retries: DEFAULT_CLIENT_CONNECTION_MAX_RETRIES,
            task_completion_poll_interval_ms: DEFAULT_TASK_COMPLETION_POLL_INTERVAL_MS,
            shutdown_timeout_secs: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
        }
    }
}

pub fn find_available_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    addr.port()
}

#[derive(Debug)]
pub struct TaskAttempt {
    pub attempt_number: i32,
    pub status: i16,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error_message: Option<String>,
}

pub struct TaskTestData;

impl TaskTestData {
    fn build_retry_policy(json: serde_json::Value) -> Option<ProtoRetryPolicy> {
        let policy = InternalRetryPolicy::from_json(&json).expect("invalid retry policy json");
        Some(policy.to_proto())
    }

    pub fn echo_task(message: &str) -> CreateTaskRequest {
        CreateTaskRequest {
            name: "echo".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&vec![message.to_string()]).unwrap(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn flaky_task_that_succeeds_on_retry() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "flaky_task".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&Vec::<String>::new()).unwrap(),
            kwargs: r#"{"fail_first_attempt": true}"#.to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "exponential",
                    "initial_delay": 1,
                    "multiplier": 2,
                    "max_delay": 60
                },
                "retry": {"include_errors": ["ValueError", "RuntimeError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn math_add_task(a: f64, b: f64) -> CreateTaskRequest {
        CreateTaskRequest {
            name: "math_add".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&Vec::<String>::new()).unwrap(),
            kwargs: json!({"a": a, "b": b}).to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn count_args_task(args: Vec<serde_json::Value>) -> CreateTaskRequest {
        CreateTaskRequest {
            name: "count_args".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&args).unwrap(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn bool_task() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "bool_task".to_string(),
            domain: "test".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn null_task() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "null_task".to_string(),
            domain: "test".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn object_task() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "object_task".to_string(),
            domain: "test".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn uint_task(value: u64) -> CreateTaskRequest {
        CreateTaskRequest {
            name: "uint_task".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&vec![serde_json::json!(value)]).unwrap(),
            kwargs: "{}".to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 1},
                "wait": {"strategy": "fixed", "delay": 1},
                "retry": {"include_errors": ["ValueError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    pub fn failing_task_with_retries() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "always_fail".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&Vec::<String>::new()).unwrap(),
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: Self::build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "exponential_jitter",
                    "initial_delay": 1,
                    "multiplier": 2,
                    "max_delay": 60,
                    "jitter": "full"
                },
                "retry": {"include_errors": ["ValueError", "RuntimeError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }
}
