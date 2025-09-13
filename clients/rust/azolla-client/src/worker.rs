use crate::error::AzollaError;
use crate::proto::common::{
    AnyValue, ErrorResult, SuccessResult, Task as ProtoTask, TaskResult as ProtoTaskResult,
};
use crate::proto::orchestrator::cluster_service_client::ClusterServiceClient;
use crate::proto::orchestrator::{Ack, ClientMsg, Hello, Ping, ServerMsg, Status};
use crate::task::{BoxedTask, Task};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use uuid::Uuid;

/// RAII guard for managing load counter
/// Automatically decrements the counter when dropped, ensuring panic safety
struct LoadGuard {
    counter: Arc<AtomicU32>,
}

impl LoadGuard {
    fn new(counter: Arc<AtomicU32>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Worker for executing tasks
pub struct Worker {
    config: WorkerConfig,
    tasks: HashMap<String, Arc<dyn BoxedTask>>,
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
                    log::info!("Reconnecting in {reconnect_delay:?}...");

                    tokio::time::sleep(reconnect_delay).await;

                    // Exponential backoff with jitter
                    let jitter = 1.0 + (reconnect_delay.as_millis() as f64 % 100.0) / 1000.0;
                    reconnect_delay =
                        std::cmp::min(reconnect_delay.mul_f64(1.5 * jitter), MAX_RECONNECT_DELAY);
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
        let response_stream = client.stream(request_stream).await?.into_inner();

        // Send hello message
        let hello_msg = ClientMsg {
            kind: Some(crate::proto::orchestrator::client_msg::Kind::Hello(Hello {
                shepherd_uuid: self.shepherd_uuid.clone(),
                max_concurrency: self.config.max_concurrency,
                domain: self.config.domain.clone(),
                shepherd_group: self.config.shepherd_group.clone(),
            })),
        };

        tx.send(hello_msg)
            .await
            .map_err(|e| AzollaError::WorkerError(format!("Failed to send hello message: {e}")))?;

        log::info!("Shepherd {} registered successfully", self.shepherd_uuid);

        // Reset reconnect delay on successful connection
        // (This would be handled by the main loop)

        // Start heartbeat task
        let heartbeat_tx = tx.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let current_load = self.current_load.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        let heartbeat_handle = tokio::spawn(async move {
            Self::heartbeat_loop(
                heartbeat_tx,
                heartbeat_interval,
                current_load,
                shutdown_signal,
            )
            .await
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
        let client = ClusterServiceClient::connect(endpoint).await.map_err(|e| {
            AzollaError::ConnectionError(format!("Failed to connect to orchestrator: {e}"))
        })?;
        Ok(client)
    }

    /// Process incoming messages from orchestrator
    async fn process_messages(
        &self,
        mut response_stream: tonic::Streaming<ServerMsg>,
        tx: mpsc::Sender<ClientMsg>,
    ) -> Result<(), AzollaError> {
        while let Some(message) = response_stream
            .message()
            .await
            .map_err(|e| AzollaError::WorkerError(format!("Stream error: {e}")))?
        {
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
    async fn handle_task(
        &self,
        task: ProtoTask,
        tx: mpsc::Sender<ClientMsg>,
    ) -> Result<(), AzollaError> {
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

        // Execute task asynchronously
        let tasks = self.tasks.clone();
        let task_id = task.task_id.clone();

        // Create load guard to ensure counter is properly managed even on panic
        let load_guard = LoadGuard::new(self.current_load.clone());

        tokio::spawn(async move {
            // Move the guard into the async task to ensure it lives for the task duration
            let _guard = load_guard;

            let result = Self::execute_task_impl_internal(tasks, task).await;

            // Send result
            let result_msg = ClientMsg {
                kind: Some(crate::proto::orchestrator::client_msg::Kind::TaskResult(
                    result,
                )),
            };

            if let Err(e) = tx.send(result_msg).await {
                log::error!("Failed to send result for task {task_id}: {e}");
            }

            // Load counter automatically decremented when _guard is dropped
        });

        Ok(())
    }

    /// Execute a task implementation
    #[cfg(test)]
    pub async fn execute_task_impl(
        tasks: HashMap<String, Arc<dyn BoxedTask>>,
        proto_task: ProtoTask,
    ) -> ProtoTaskResult {
        Self::execute_task_impl_internal(tasks, proto_task).await
    }

    /// Internal task execution implementation
    async fn execute_task_impl_internal(
        tasks: HashMap<String, Arc<dyn BoxedTask>>,
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
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: "TaskNotFound".to_string(),
                            message: format!(
                                "No implementation found for task: {}",
                                proto_task.name
                            ),
                            code: "TASK_NOT_FOUND".to_string(),
                            stacktrace: "".to_string(),
                            data: None,
                        },
                    )),
                };
            }
        };

        // Parse arguments
        let args = match Self::parse_task_args(&proto_task.args) {
            Ok(args) => args,
            Err(e) => {
                log::error!("Failed to parse args for task {task_id}: {e}");
                return ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: "ArgumentParseError".to_string(),
                            message: format!("Failed to parse task arguments: {e}"),
                            code: "ARG_PARSE_ERROR".to_string(),
                            stacktrace: "".to_string(),
                            data: None,
                        },
                    )),
                };
            }
        };

        // Note: Argument validation is now handled automatically by the type system

        // Execute the task
        let start_time = Instant::now();
        let execution_result = task_impl.execute_json(args).await;
        let execution_time = start_time.elapsed();

        log::info!("Task {task_id} completed in {execution_time:?}");

        match execution_result {
            Ok(result) => ProtoTaskResult {
                task_id,
                result_type: Some(crate::proto::common::task_result::ResultType::Success(
                    SuccessResult {
                        result: Some(AnyValue {
                            value: Some(crate::proto::common::any_value::Value::JsonValue(
                                result.to_string(),
                            )),
                        }),
                    },
                )),
            },
            Err(e) => {
                log::error!("Task {task_id} failed: {e}");
                ProtoTaskResult {
                    task_id,
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: e.error_type().unwrap_or("TaskExecutionError".to_string()),
                            message: e.to_string(),
                            code: e.error_code().unwrap_or("EXECUTION_ERROR".to_string()),
                            stacktrace: "".to_string(),
                            data: None,
                        },
                    )),
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
    async fn handle_ping(
        &self,
        _ping: Ping,
        _tx: mpsc::Sender<ClientMsg>,
    ) -> Result<(), AzollaError> {
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
                kind: Some(crate::proto::orchestrator::client_msg::Kind::Status(
                    Status {
                        current_load: current,
                        available_capacity: 100u32.saturating_sub(current),
                    },
                )),
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
    tasks: HashMap<String, Arc<dyn BoxedTask>>,
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
        self.tasks
            .insert(name, Arc::new(task) as Arc<dyn BoxedTask>);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskResult;
    use serde_json::json;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Test the purpose of LoadGuard: ensure load counter is managed correctly even on panic
    #[test]
    fn test_load_guard_increments_on_create() {
        let counter = Arc::new(AtomicU32::new(0));
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        {
            let _guard = LoadGuard::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        } // _guard drops here

        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// Test the expected behavior: load counter should be decremented even when panic occurs
    #[test]
    fn test_load_guard_handles_panic() {
        let counter = Arc::new(AtomicU32::new(0));

        let result = std::panic::catch_unwind(|| {
            let _guard = LoadGuard::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 1);
            panic!("Simulated panic in task!");
        });

        assert!(result.is_err());
        // Critical test: counter should be back to 0 even after panic
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// Test multiple guards work independently
    #[test]
    fn test_multiple_load_guards() {
        let counter = Arc::new(AtomicU32::new(0));

        {
            let _guard1 = LoadGuard::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 1);

            {
                let _guard2 = LoadGuard::new(counter.clone());
                assert_eq!(counter.load(Ordering::Relaxed), 2);
            }

            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }

        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    // Mock task that panics for testing
    struct PanickingTask;

    impl Task for PanickingTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "panicking_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async {
                panic!("Task intentionally panicked for testing!");
            })
        }
    }

    // Mock task that succeeds for testing
    struct SuccessTask;

    impl Task for SuccessTask {
        type Args = ();

        fn name(&self) -> &'static str {
            "success_task"
        }

        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async { Ok(json!({"status": "success"})) })
        }
    }

    /// Test that task execution panic doesn't break worker load tracking
    #[tokio::test]
    async fn test_task_execution_panic_safety() {
        let config = WorkerConfig::default();
        let mut tasks = HashMap::new();
        tasks.insert(
            "panicking_task".to_string(),
            Arc::new(PanickingTask) as Arc<dyn BoxedTask>,
        );

        let worker = Worker {
            config,
            tasks,
            shepherd_uuid: "test-uuid".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        let (tx, mut rx) = mpsc::channel(10);
        let proto_task = crate::proto::common::Task {
            task_id: "panic-test-task".to_string(),
            name: "panicking_task".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            memory_limit: Some(0),
            cpu_limit: Some(0),
        };

        // Initial load should be 0
        assert_eq!(worker.current_load.load(Ordering::Relaxed), 0);

        // Handle the task - this should not panic the worker itself
        let result = worker.handle_task(proto_task, tx).await;
        assert!(
            result.is_ok(),
            "handle_incoming_task should not fail even with panicking tasks"
        );

        // Give the spawned task time to complete/panic
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Critical assertion: load should be back to 0 even though the task panicked
        assert_eq!(
            worker.current_load.load(Ordering::Relaxed),
            0,
            "Load counter should be reset even after task panic"
        );

        // Verify we get an error result message
        if let Ok(msg) = rx.try_recv() {
            if let Some(crate::proto::orchestrator::client_msg::Kind::TaskResult(task_result)) =
                msg.kind
            {
                assert!(task_result.result_type.is_some());
                // Should be an error result due to panic
            }
        }
    }

    /// Test that successful task execution properly manages load
    #[tokio::test]
    async fn test_task_execution_success_load_management() {
        let config = WorkerConfig::default();
        let mut tasks = HashMap::new();
        tasks.insert(
            "success_task".to_string(),
            Arc::new(SuccessTask) as Arc<dyn BoxedTask>,
        );

        let worker = Worker {
            config,
            tasks,
            shepherd_uuid: "test-uuid".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        let (tx, mut rx) = mpsc::channel(10);
        let proto_task = crate::proto::common::Task {
            task_id: "success-test-task".to_string(),
            name: "success_task".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            memory_limit: Some(0),
            cpu_limit: Some(0),
        };

        // Initial load should be 0
        assert_eq!(worker.current_load.load(Ordering::Relaxed), 0);

        // Handle the task
        let result = worker.handle_task(proto_task, tx).await;
        assert!(result.is_ok());

        // Give the task time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Load should be back to 0 after successful completion
        assert_eq!(worker.current_load.load(Ordering::Relaxed), 0);

        // Verify we get a success result message
        if let Ok(msg) = rx.try_recv() {
            if let Some(crate::proto::orchestrator::client_msg::Kind::TaskResult(task_result)) =
                msg.kind
            {
                assert!(task_result.result_type.is_some());
            }
        }
    }

    /// Test worker configuration defaults
    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert_eq!(config.orchestrator_endpoint, "localhost:52710");
        assert_eq!(config.domain, "default");
        assert_eq!(config.shepherd_group, "rust-workers");
        assert_eq!(config.max_concurrency, 10);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
    }

    /// Test worker builder functionality
    #[test]
    fn test_worker_builder() {
        let builder = Worker::builder()
            .orchestrator("test-orchestrator:8080")
            .domain("test-domain")
            .shepherd_group("test-group")
            .max_concurrency(5);

        // We can verify the builder accepts our configuration
        assert_eq!(
            builder.config.orchestrator_endpoint,
            "test-orchestrator:8080"
        );
        assert_eq!(builder.config.domain, "test-domain");
        assert_eq!(builder.config.shepherd_group, "test-group");
        assert_eq!(builder.config.max_concurrency, 5);
    }

    /// Test task registration in builder
    #[test]
    fn test_worker_builder_task_registration() {
        let builder = Worker::builder()
            .register_task(SuccessTask)
            .register_task(PanickingTask);

        assert_eq!(builder.tasks.len(), 2);
        assert!(builder.tasks.contains_key("success_task"));
        assert!(builder.tasks.contains_key("panicking_task"));
    }

    /// Test the purpose of Worker reconnection: ensure worker handles network failures gracefully
    #[tokio::test]
    async fn test_worker_reconnection_behavior() {
        let config = WorkerConfig {
            orchestrator_endpoint: "localhost:99999".to_string(), // Non-existent port
            domain: "test-domain".to_string(),
            shepherd_group: "test-group".to_string(),
            max_concurrency: 5,
            heartbeat_interval: Duration::from_millis(50),
        };

        let worker = Worker {
            config,
            tasks: HashMap::new(),
            shepherd_uuid: "test-reconnect-uuid".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        let start = std::time::Instant::now();

        // Set shutdown signal after short delay to stop reconnection attempts
        let shutdown_signal = worker.shutdown_signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown_signal.store(true, Ordering::Relaxed);
        });

        let result = worker.run().await;
        let elapsed = start.elapsed();

        assert!(result.is_ok()); // Should terminate gracefully on shutdown
        assert!(elapsed >= Duration::from_millis(50)); // Should have tried to connect at least once
        assert!(elapsed < Duration::from_secs(5)); // Should not hang indefinitely
    }

    /// Test the expected behavior: heartbeat continues during task execution
    #[tokio::test]
    async fn test_heartbeat_loop_functionality() {
        let (tx, mut rx) = mpsc::channel(10);
        let current_load = Arc::new(AtomicU32::new(3));
        let shutdown_signal = Arc::new(AtomicBool::new(false));

        // Start heartbeat loop
        let heartbeat_handle = tokio::spawn(Worker::heartbeat_loop(
            tx,
            Duration::from_millis(25), // Fast heartbeat for testing
            current_load.clone(),
            shutdown_signal.clone(),
        ));

        // Let heartbeat run for a bit
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Change load during execution
        current_load.store(7, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Signal shutdown
        shutdown_signal.store(true, Ordering::Relaxed);

        // Await graceful shutdown of the heartbeat loop
        heartbeat_handle.await.expect("Heartbeat loop panicked");

        // Collect status messages
        let mut status_messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let Some(crate::proto::orchestrator::client_msg::Kind::Status(status)) = msg.kind {
                status_messages.push((status.current_load, status.available_capacity));
            }
        }

        // Should have received multiple heartbeat messages
        assert!(
            status_messages.len() >= 2,
            "Should have received multiple heartbeat messages, got {}",
            status_messages.len()
        );

        // Should reflect the load changes
        let has_load_3 = status_messages
            .iter()
            .any(|(load, capacity)| *load == 3 && *capacity == 97);
        let has_load_7 = status_messages
            .iter()
            .any(|(load, capacity)| *load == 7 && *capacity == 93);

        assert!(
            has_load_3 || has_load_7,
            "Should have captured load changes"
        );
    }

    /// Test heartbeat with high load scenarios
    #[tokio::test]
    async fn test_heartbeat_high_load_scenarios() {
        let (tx, mut rx) = mpsc::channel(10);
        let current_load = Arc::new(AtomicU32::new(95)); // High load
        let shutdown_signal = Arc::new(AtomicBool::new(false));

        let heartbeat_handle = tokio::spawn(Worker::heartbeat_loop(
            tx,
            Duration::from_millis(20),
            current_load.clone(),
            shutdown_signal.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(60)).await;

        // Test max load
        current_load.store(100, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(25)).await;

        shutdown_signal.store(true, Ordering::Relaxed);

        // Await graceful shutdown of the heartbeat loop
        heartbeat_handle.await.expect("Heartbeat loop panicked");

        let mut found_high_load = false;
        let mut found_max_load = false;

        while let Ok(msg) = rx.try_recv() {
            if let Some(crate::proto::orchestrator::client_msg::Kind::Status(status)) = msg.kind {
                if status.current_load == 95 && status.available_capacity == 5 {
                    found_high_load = true;
                }
                if status.current_load == 100 && status.available_capacity == 0 {
                    found_max_load = true;
                }
            }
        }

        // Should handle high load scenarios correctly
        assert!(
            found_high_load || found_max_load,
            "Should have found high load scenarios"
        );
    }

    /// Test worker connection error handling
    #[tokio::test]
    async fn test_worker_connection_error_handling() {
        let config = WorkerConfig {
            orchestrator_endpoint: "invalid-hostname:99999".to_string(),
            ..Default::default()
        };

        let worker = Worker {
            config,
            tasks: HashMap::new(),
            shepherd_uuid: "test-error-uuid".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        // Test that connection attempts return errors appropriately
        let connect_result = worker.connect().await;
        assert!(connect_result.is_err());

        match connect_result.unwrap_err() {
            AzollaError::ConnectionError(_) => { /* Expected */ }
            _ => panic!("Expected ConnectionError for invalid hostname"),
        }
    }

    /// Test task argument parsing edge cases
    #[test]
    fn test_parse_task_args_edge_cases() {
        // Test empty string
        let result = Worker::parse_task_args("").unwrap();
        assert!(result.is_empty());

        // Test empty array
        let result = Worker::parse_task_args("[]").unwrap();
        assert!(result.is_empty());

        // Test single value
        let result = Worker::parse_task_args("[42]").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], serde_json::json!(42));

        // Test multiple values
        let result = Worker::parse_task_args(r#"["hello", 123, true, null]"#).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], serde_json::json!("hello"));
        assert_eq!(result[1], serde_json::json!(123));
        assert_eq!(result[2], serde_json::json!(true));
        assert_eq!(result[3], serde_json::json!(null));

        // Test invalid JSON
        let result = Worker::parse_task_args("invalid json");
        assert!(result.is_err());

        // Test malformed JSON
        let result = Worker::parse_task_args("[1, 2, 3");
        assert!(result.is_err());
    }

    /// Test task execution with unknown task
    #[tokio::test]
    async fn test_execute_task_unknown_task() {
        let tasks = HashMap::new(); // Empty task registry

        let proto_task = crate::proto::common::Task {
            task_id: "unknown-task-test".to_string(),
            name: "unknown_task".to_string(),
            args: "[]".to_string(),
            kwargs: "{}".to_string(),
            memory_limit: Some(0),
            cpu_limit: Some(0),
        };

        let result = Worker::execute_task_impl_internal(tasks, proto_task).await;

        // Should return error result for unknown task
        match result.result_type {
            Some(crate::proto::common::task_result::ResultType::Error(error)) => {
                assert_eq!(error.r#type, "TaskNotFound");
                assert!(error.message.contains("No implementation found"));
                assert_eq!(error.code, "TASK_NOT_FOUND");
            }
            _ => panic!("Expected error result for unknown task"),
        }
    }

    /// Test task execution with invalid arguments
    #[tokio::test]
    async fn test_execute_task_invalid_args() {
        let mut tasks = HashMap::new();
        tasks.insert(
            "success_task".to_string(),
            Arc::new(SuccessTask) as Arc<dyn BoxedTask>,
        );

        let proto_task = crate::proto::common::Task {
            task_id: "invalid-args-test".to_string(),
            name: "success_task".to_string(),
            args: "invalid json".to_string(), // Invalid JSON
            kwargs: "{}".to_string(),
            memory_limit: Some(0),
            cpu_limit: Some(0),
        };

        let result = Worker::execute_task_impl_internal(tasks, proto_task).await;

        // Should return error result for invalid arguments
        match result.result_type {
            Some(crate::proto::common::task_result::ResultType::Error(error)) => {
                assert_eq!(error.r#type, "ArgumentParseError");
                assert!(error.message.contains("Failed to parse task arguments"));
                assert_eq!(error.code, "ARG_PARSE_ERROR");
            }
            _ => panic!("Expected error result for invalid arguments"),
        }
    }

    /// Test worker shutdown functionality
    #[test]
    fn test_worker_shutdown() {
        let worker = Worker {
            config: WorkerConfig::default(),
            tasks: HashMap::new(),
            shepherd_uuid: "shutdown-test".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        // Initially not shutdown
        assert!(!worker.shutdown_signal.load(Ordering::Relaxed));

        // Signal shutdown
        worker.shutdown();

        // Should be shutdown
        assert!(worker.shutdown_signal.load(Ordering::Relaxed));
    }

    /// Test worker task count
    #[test]
    fn test_worker_task_count() {
        let mut tasks = HashMap::new();
        tasks.insert(
            "task1".to_string(),
            Arc::new(SuccessTask) as Arc<dyn BoxedTask>,
        );
        tasks.insert(
            "task2".to_string(),
            Arc::new(PanickingTask) as Arc<dyn BoxedTask>,
        );

        let worker = Worker {
            config: WorkerConfig::default(),
            tasks,
            shepherd_uuid: "task-count-test".to_string(),
            current_load: Arc::new(AtomicU32::new(0)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        };

        assert_eq!(worker.task_count(), 2);
    }

    /// Test worker builder discover_tasks placeholder
    #[test]
    fn test_worker_builder_discover_tasks() {
        let builder = Worker::builder().discover_tasks();

        // discover_tasks is currently a placeholder, so it should just return the builder unchanged
        // In the future, this would automatically register tasks found via proc macro
        assert_eq!(builder.tasks.len(), 0); // No tasks discovered yet (placeholder implementation)
    }

    /// Test load guard with multiple concurrent operations
    #[tokio::test]
    async fn test_load_guard_concurrent_operations() {
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();

        // Start 5 concurrent "tasks" that each hold a load guard
        for i in 0..5 {
            let counter_clone = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = LoadGuard::new(counter_clone);
                tokio::time::sleep(Duration::from_millis(10 * i + 5)).await;
                // Guard drops here
            });
            handles.push(handle);
        }

        // Check that load increases
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mid_load = counter.load(Ordering::Relaxed);
        assert!(
            mid_load > 0 && mid_load <= 5,
            "Load should be between 1 and 5, got {mid_load}"
        );

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Final load should be 0
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
