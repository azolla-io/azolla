use crate::error::AzollaError;
use crate::proto::orchestrator::cluster_service_client::ClusterServiceClient;
use crate::proto::orchestrator::{ClientMsg, ServerMsg, Hello, Ack, Status, Ping};
use crate::proto::common::{Task as ProtoTask, TaskResult as ProtoTaskResult, SuccessResult, ErrorResult, AnyValue};
use crate::task::Task;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU32, AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use serde_json::Value;
use uuid::Uuid;

/// Worker for executing tasks
pub struct Worker {
    config: WorkerConfig,
    tasks: HashMap<String, Arc<dyn Task>>,
    shepherd_uuid: String,
    current_load: Arc<AtomicU32>,
    shutdown_signal: Arc<AtomicBool>,
}

/// Worker configuration
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub orchestrator_endpoint: String,
    pub domain: String,
    pub shepherd_group: String,
    pub max_concurrency: u32,
    pub heartbeat_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            orchestrator_endpoint: "localhost:52710".to_string(),
            domain: "default".to_string(),
            shepherd_group: "rust-workers".to_string(),
            max_concurrency: 10,
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

impl Worker {
    /// Create a worker builder
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::default()
    }

    /// Get the number of registered tasks
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Run the worker with reconnection logic
    pub async fn run(self) -> Result<(), AzollaError> {
        log::info!(
            "Worker starting with {} tasks on {} (domain: {}, group: {})",
            self.tasks.len(),
            self.config.orchestrator_endpoint,
            self.config.domain,
            self.config.shepherd_group
        );

        let mut reconnect_delay = Duration::from_secs(1);
        const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

        loop {
            match self.run_connection().await {
                Ok(_) => {
                    log::info!("Worker connection terminated gracefully");
                    break;
                }
                Err(e) => {
                    if self.shutdown_signal.load(Ordering::Relaxed) {
                        log::info!("Shutdown requested, stopping worker");
                        break;
                    }
                    
                    log::error!("Worker connection failed: {e}");
                    log::info!("Reconnecting in {:?}...", reconnect_delay);
                    
                    tokio::time::sleep(reconnect_delay).await;
                    
                    // Exponential backoff with jitter
                    let jitter = 1.0 + (reconnect_delay.as_millis() as f64 % 100.0) / 1000.0;
                    reconnect_delay = std::cmp::min(
                        reconnect_delay.mul_f64(1.5 * jitter),
                        MAX_RECONNECT_DELAY
                    );
                }
            }
        }
        
        Ok(())
    }

    /// Run a single connection to the orchestrator
    async fn run_connection(&self) -> Result<(), AzollaError> {
        // Connect to orchestrator
        let mut client = self.connect().await?;
        
        // Create bidirectional stream
        let (tx, rx) = mpsc::channel(1000);
        let request_stream = ReceiverStream::new(rx);
        
        // Start the stream
        let response_stream = client.stream(request_stream).await?
            .into_inner();
        
        // Send hello message
        let hello_msg = ClientMsg {
            kind: Some(crate::proto::orchestrator::client_msg::Kind::Hello(Hello {
                shepherd_uuid: self.shepherd_uuid.clone(),
                max_concurrency: self.config.max_concurrency,
                domain: self.config.domain.clone(),
                shepherd_group: self.config.shepherd_group.clone(),
            })),
        };
        
        tx.send(hello_msg).await.map_err(|e| 
            AzollaError::WorkerError(format!("Failed to send hello message: {e}"))
        )?;
        
        log::info!("Shepherd {} registered successfully", self.shepherd_uuid);
        
        // Reset reconnect delay on successful connection
        // (This would be handled by the main loop)
        
        // Start heartbeat task
        let heartbeat_tx = tx.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let current_load = self.current_load.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        
        let heartbeat_handle = tokio::spawn(async move {
            Self::heartbeat_loop(heartbeat_tx, heartbeat_interval, current_load, shutdown_signal).await
        });
        
        // Main message processing loop
        let result = self.process_messages(response_stream, tx).await;
        
        // Cancel heartbeat task
        heartbeat_handle.abort();
        
        result
    }
    
    /// Connect to the orchestrator
    async fn connect(&self) -> Result<ClusterServiceClient<Channel>, AzollaError> {
        let endpoint = format!("http://{}", self.config.orchestrator_endpoint);
        let client = ClusterServiceClient::connect(endpoint).await
            .map_err(|e| AzollaError::ConnectionError(format!("Failed to connect to orchestrator: {e}")))?;
        Ok(client)
    }
    
    /// Process incoming messages from orchestrator
    async fn process_messages(
        &self,
        mut response_stream: tonic::Streaming<ServerMsg>,
        tx: mpsc::Sender<ClientMsg>,
    ) -> Result<(), AzollaError> {
        while let Some(message) = response_stream.message().await
            .map_err(|e| AzollaError::WorkerError(format!("Stream error: {e}")))? {
                
            match message.kind {
                Some(crate::proto::orchestrator::server_msg::Kind::Task(task)) => {
                    self.handle_task(task, tx.clone()).await?;
                }
                Some(crate::proto::orchestrator::server_msg::Kind::Ping(ping)) => {
                    self.handle_ping(ping, tx.clone()).await?;
                }
                None => {
                    log::warn!("Received message with no kind");
                }
            }
            
            if self.shutdown_signal.load(Ordering::Relaxed) {
                break;
            }
        }
        
        Ok(())
    }
    
    /// Handle incoming task from orchestrator
    async fn handle_task(&self, task: ProtoTask, tx: mpsc::Sender<ClientMsg>) -> Result<(), AzollaError> {
        log::info!("Received task: {} ({})", task.name, task.task_id);
        
        // Send acknowledgment
        let ack_msg = ClientMsg {
            kind: Some(crate::proto::orchestrator::client_msg::Kind::Ack(Ack {
                task_id: task.task_id.clone(),
            })),
        };
        
        if let Err(e) = tx.send(ack_msg).await {
            log::error!("Failed to send ack for task {}: {e}", task.task_id);
            return Err(AzollaError::WorkerError(format!("Failed to send ack: {e}")));
        }
        
        // Increment current load
        self.current_load.fetch_add(1, Ordering::Relaxed);
        
        // Execute task asynchronously
        let tasks = self.tasks.clone();
        let current_load = self.current_load.clone();
        let task_id = task.task_id.clone();
        
        tokio::spawn(async move {
            let result = Self::execute_task_impl(tasks, task).await;
            
            // Send result
            let result_msg = ClientMsg {
                kind: Some(crate::proto::orchestrator::client_msg::Kind::TaskResult(result)),
            };
            
            if let Err(e) = tx.send(result_msg).await {
                log::error!("Failed to send result for task {task_id}: {e}");
            }
            
            // Decrement current load
            current_load.fetch_sub(1, Ordering::Relaxed);
        });
        
        Ok(())
    }
    
    /// Execute a task implementation
    async fn execute_task_impl(
        tasks: HashMap<String, Arc<dyn Task>>,
        proto_task: ProtoTask,
    ) -> ProtoTaskResult {
        let task_id = proto_task.task_id.clone();
        
        // Find the task implementation
        let task_impl = match tasks.get(&proto_task.name) {
            Some(task) => task.clone(),
            None => {
                log::error!("No implementation found for task: {}", proto_task.name);
                return ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(ErrorResult {
                        r#type: "TaskNotFound".to_string(),
                        message: format!("No implementation found for task: {}", proto_task.name),
                        code: "TASK_NOT_FOUND".to_string(),
                        stacktrace: "".to_string(),
                        data: None,
                    })),
                };
            }
        };
        
        // Parse arguments
        let args = match Self::parse_task_args(&proto_task.args) {
            Ok(args) => args,
            Err(e) => {
                log::error!("Failed to parse args for task {}: {e}", task_id);
                return ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(ErrorResult {
                        r#type: "ArgumentParseError".to_string(),
                        message: format!("Failed to parse task arguments: {e}"),
                        code: "ARG_PARSE_ERROR".to_string(),
                        stacktrace: "".to_string(),
                        data: None,
                    })),
                };
            }
        };
        
        // Validate arguments
        if let Err(e) = task_impl.validate_args(&args) {
            log::error!("Argument validation failed for task {}: {e}", task_id);
            return ProtoTaskResult {
                task_id,
                result_type: Some(crate::proto::common::task_result::ResultType::Error(ErrorResult {
                    r#type: "ArgumentValidationError".to_string(),
                    message: format!("Argument validation failed: {e}"),
                    code: "ARG_VALIDATION_ERROR".to_string(),
                    stacktrace: "".to_string(),
                    data: None,
                })),
            };
        }
        
        // Execute the task
        let start_time = Instant::now();
        let execution_result = task_impl.execute(args).await;
        let execution_time = start_time.elapsed();
        
        log::info!("Task {} completed in {:?}", task_id, execution_time);
        
        match execution_result {
            Ok(result) => {
                ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Success(SuccessResult {
                        result: Some(AnyValue {
                            value: Some(crate::proto::common::any_value::Value::JsonValue(result.to_string())),
                        }),
                    })),
                }
            }
            Err(e) => {
                log::error!("Task {} failed: {e}", task_id);
                ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(ErrorResult {
                        r#type: e.error_type().unwrap_or("TaskExecutionError".to_string()),
                        message: e.to_string(),
                        code: e.error_code().unwrap_or("EXECUTION_ERROR".to_string()),
                        stacktrace: "".to_string(),
                        data: None,
                    })),
                }
            }
        }
    }
    
    /// Parse task arguments from JSON string
    fn parse_task_args(args_json: &str) -> Result<Vec<Value>, serde_json::Error> {
        if args_json.is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_str(args_json)
    }
    
    /// Handle ping message from orchestrator
    async fn handle_ping(&self, _ping: Ping, _tx: mpsc::Sender<ClientMsg>) -> Result<(), AzollaError> {
        // Pings are handled automatically by the gRPC layer
        // We could add custom ping handling here if needed
        Ok(())
    }
    
    /// Heartbeat loop for status reporting
    async fn heartbeat_loop(
        tx: mpsc::Sender<ClientMsg>,
        interval_duration: Duration,
        current_load: Arc<AtomicU32>,
        shutdown_signal: Arc<AtomicBool>,
    ) {
        let mut interval = interval(interval_duration);
        
        while !shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let current = current_load.load(Ordering::Relaxed);
            let status_msg = ClientMsg {
                kind: Some(crate::proto::orchestrator::client_msg::Kind::Status(Status {
                    current_load: current,
                    available_capacity: if current < 100 { 100 - current } else { 0 },
                })),
            };
            
            if let Err(e) = tx.send(status_msg).await {
                log::error!("Failed to send status update: {e}");
                break;
            }
        }
    }
    
    /// Signal shutdown to the worker
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

/// Builder for worker configuration
#[derive(Default)]
pub struct WorkerBuilder {
    config: WorkerConfig,
    tasks: HashMap<String, Arc<dyn Task>>,
}

impl WorkerBuilder {
    /// Set orchestrator endpoint
    pub fn orchestrator(mut self, endpoint: &str) -> Self {
        self.config.orchestrator_endpoint = endpoint.to_string();
        self
    }

    /// Set domain
    pub fn domain(mut self, domain: &str) -> Self {
        self.config.domain = domain.to_string();
        self
    }

    /// Set shepherd group
    pub fn shepherd_group(mut self, group: &str) -> Self {
        self.config.shepherd_group = group.to_string();
        self
    }

    /// Set max concurrency
    pub fn max_concurrency(mut self, concurrency: u32) -> Self {
        self.config.max_concurrency = concurrency;
        self
    }

    /// Register a task implementation
    pub fn register_task<T: Task + 'static>(mut self, task: T) -> Self {
        let name = task.name().to_string();
        self.tasks.insert(name, Arc::new(task));
        self
    }

    /// Discover tasks automatically (placeholder for proc macro integration)
    pub fn discover_tasks(self) -> Self {
        // TODO: This would use the inventory crate to discover
        // all tasks registered by the proc macro

        // For now, just return self unchanged
        log::info!("Task discovery not yet implemented");
        self
    }

    /// Build the worker
    pub async fn build(self) -> Result<Worker, AzollaError> {
        Ok(Worker {
            config: self.config,
            tasks: self.tasks,
            shepherd_uuid: Uuid::new_v4().to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        })
    }
}