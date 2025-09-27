use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::{mpsc, watch};
use tokio::time::Duration;
use uuid::Uuid;

use crate::shepherd::{IncomingTask, ShepherdConfig, StreamEvent};

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub task: IncomingTask,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub status: TaskStatus,
    pub process_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Queued,
    Starting,
    Running,
    Completed,
    Failed,
    TimedOut,
    Killed,
}

#[derive(Debug)]
struct TaskCompletion {
    task_id: Uuid,
    success: bool,
    timed_out: bool,
    execution_time_ms: f64,
}

pub struct TaskManager {
    config: ShepherdConfig,
    task_queue: VecDeque<IncomingTask>,
    running_tasks: HashMap<Uuid, TaskInfo>,
    event_receiver: mpsc::UnboundedReceiver<StreamEvent>,
    shutdown_receiver: watch::Receiver<bool>,
    current_load: Arc<std::sync::atomic::AtomicU32>,
    stats: TaskManagerStats,
    completion_sender: mpsc::UnboundedSender<TaskCompletion>,
    completion_receiver: mpsc::UnboundedReceiver<TaskCompletion>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskManagerStats {
    pub total_tasks_received: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub total_tasks_timed_out: u64,
    pub tasks_currently_queued: u32,
    pub tasks_currently_running: u32,
    pub max_queue_size_reached: u32,
    pub average_execution_time_ms: f64,
}

impl TaskManager {
    pub fn new(
        config: ShepherdConfig,
        event_receiver: mpsc::UnboundedReceiver<StreamEvent>,
        shutdown_receiver: watch::Receiver<bool>,
        current_load: Arc<std::sync::atomic::AtomicU32>,
    ) -> Self {
        let (completion_sender, completion_receiver) = mpsc::unbounded_channel();
        Self {
            config,
            task_queue: VecDeque::new(),
            running_tasks: HashMap::new(),
            event_receiver,
            shutdown_receiver,
            current_load,
            stats: TaskManagerStats::default(),
            completion_sender,
            completion_receiver,
        }
    }

    pub async fn start(mut self) -> Result<()> {
        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Starting task manager for shepherd {}", self.config.uuid);

        loop {
            tokio::select! {
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            if let Err(e) = self.handle_stream_event(event).await {
                                error!("Error handling stream event: {e}");
                            }
                        }
                        None => {
                            warn!("Stream event channel closed");
                            break;
                        }
                    }
                }

                completion = self.completion_receiver.recv() => {
                    match completion {
                        Some(completion) => {
                            self.handle_task_completion(completion);
                        }
                        None => {
                            warn!("Task completion channel closed");
                            break;
                        }
                    }
                }

                _ = self.shutdown_receiver.changed() => {
                    if *self.shutdown_receiver.borrow() {
                        info!("Task manager received shutdown signal");
                        break;
                    }
                }
            }

            while let Ok(completion) = self.completion_receiver.try_recv() {
                self.handle_task_completion(completion);
            }

            self.process_queue().await?;
            self.update_stats();
        }

        self.shutdown().await?;

        info!("Task manager stopped");
        Ok(())
    }

    async fn handle_stream_event(&mut self, event: StreamEvent) -> Result<()> {
        match event {
            StreamEvent::TaskReceived(task) => {
                info!("Received task: {} ({})", task.task_id, task.name);
                self.stats.total_tasks_received += 1;

                self.task_queue.push_back(task);

                let queue_size = self.task_queue.len() as u32;
                if queue_size > self.stats.max_queue_size_reached {
                    self.stats.max_queue_size_reached = queue_size;
                }

                debug!("Task queued. Queue size: {}", self.task_queue.len());
            }
            _ => {
                // TaskManager only handles TaskReceived events
                // All other StreamEvents are handled by stream_handler itself
                debug!("Ignoring non-TaskReceived stream event");
            }
        }

        Ok(())
    }

    /// Process the task queue and start tasks if we have capacity
    /// TODO: handle admission control based on memory, CPU etc.
    async fn process_queue(&mut self) -> Result<()> {
        let current_running = self.running_tasks.len() as u32;
        let available_capacity = self.config.max_concurrency.saturating_sub(current_running);

        if available_capacity == 0 {
            return Ok(());
        }

        let tasks_to_start = std::cmp::min(available_capacity, self.task_queue.len() as u32);

        for _ in 0..tasks_to_start {
            if let Some(task) = self.task_queue.pop_front() {
                self.start_task(task).await?;
            }
        }

        Ok(())
    }

    /// Start a task by spawning a worker process and monitoring task
    async fn start_task(&mut self, task: IncomingTask) -> Result<()> {
        let task_id = task.task_id;

        info!(
            "Starting worker process for task: {} ({})",
            task_id, task.name
        );

        // Create task info
        let task_info = TaskInfo {
            task: task.clone(),
            queued_at: Utc::now(),
            started_at: Some(Utc::now()),
            status: TaskStatus::Running,
            process_id: None, // Will be set by the monitoring task
        };

        // Add to running tasks
        self.running_tasks.insert(task_id, task_info);

        // Update load counter
        let new_load = self.running_tasks.len() as u32;
        self.current_load
            .store(new_load, std::sync::atomic::Ordering::SeqCst);

        // Spawn a dedicated task to handle process spawning and monitoring
        let config = self.config.clone();
        let current_load = self.current_load.clone();
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            Self::spawn_and_monitor_process(task_id, task, config, current_load, completion_sender)
                .await;
        });

        info!("Monitoring task spawned for task: {task_id}");

        Ok(())
    }

    /// Spawn and monitor a single process throughout its lifecycle
    async fn spawn_and_monitor_process(
        task_id: Uuid,
        task: IncomingTask,
        config: ShepherdConfig,
        current_load: Arc<std::sync::atomic::AtomicU32>,
        completion_sender: mpsc::UnboundedSender<TaskCompletion>,
    ) {
        let start_time = Utc::now();

        // Build command
        let mut cmd = Command::new(&config.worker_binary_path);
        cmd.arg("--task-id")
            .arg(task_id.to_string())
            .arg("--name")
            .arg(&task.name)
            .arg("--args")
            .arg(match serde_json::to_string(&task.args) {
                Ok(args_json) => args_json,
                Err(e) => {
                    error!("Failed to serialize task args for {task_id}: {e}");
                    current_load.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            })
            .arg("--kwargs")
            .arg(&task.kwargs)
            .arg("--shepherd-endpoint")
            .arg(config.worker_service_endpoint());

        if let Some(memory_limit) = task.memory_limit {
            cmd.env("AZOLLA_MEMORY_LIMIT", memory_limit.to_string());
        }
        if let Some(cpu_limit) = task.cpu_limit {
            cmd.env("AZOLLA_CPU_LIMIT", cpu_limit.to_string());
        }

        cmd.current_dir(".");
        cmd.env("RUST_LOG", "info");

        debug!(
            "Spawning worker: {} {:?}",
            config.worker_binary_path,
            cmd.as_std().get_args().collect::<Vec<_>>()
        );

        // Spawn the process
        let mut child = match cmd.spawn() {
            Ok(child) => {
                let process_id = child.id();
                info!("Worker process started for task {task_id}: PID {process_id:?}");
                child
            }
            Err(e) => {
                error!("Failed to spawn worker process for task {task_id}: {e}");
                current_load.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        };

        // Monitor the process
        let timeout_duration = config.worker_timeout().unwrap_or(Duration::from_secs(300));

        let result = tokio::select! {
            // Wait for process to complete
            status = child.wait() => {
                match status {
                    Ok(exit_status) => {
                        let success = exit_status.success();
                        let exit_code = exit_status.code();

                        info!("Process for task {task_id} exited: success={success}, code={exit_code:?}");

                        if success {
                            (true, None)
                        } else {
                            let error = format!("Process exited with code: {exit_code:?}");
                            (false, Some(error))
                        }
                    }
                    Err(e) => {
                        error!("Failed to wait for process {task_id}: {e}");
                        (false, Some(format!("Process wait failed: {e}")))
                    }
                }
            }

            // Timeout handling
            _ = tokio::time::sleep(timeout_duration) => {
                warn!("Task {task_id} timed out after {timeout_duration:?}");

                // Kill the process
                if let Err(e) = child.kill().await {
                    error!("Failed to kill timed out process for task {task_id}: {e}");
                }

                (false, Some(format!("Task timed out after {timeout_duration:?}")))
            }
        };

        // Calculate execution time
        let execution_time = Utc::now().signed_duration_since(start_time);
        let execution_ms = execution_time.num_milliseconds() as f64;

        // Update load counter
        current_load.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        let (success, error) = result;
        if success {
            info!("Task {task_id} completed successfully in {execution_ms:.2}ms");
        } else {
            let error_msg = error.clone().unwrap_or_else(|| "Unknown error".to_string());
            warn!("Task {task_id} failed after {execution_ms:.2}ms: {error_msg}");
        }

        let timed_out = matches!(error.as_deref(), Some(msg) if msg.starts_with("Task timed out"));

        let completion = TaskCompletion {
            task_id,
            success,
            timed_out,
            execution_time_ms: execution_ms,
        };

        debug!(
            "Task {task_id} completion ready for dispatch (success={success}, timed_out={timed_out})"
        );

        if let Err(send_err) = completion_sender.send(completion) {
            warn!("Failed to send task completion for {task_id}: {send_err}");
        } else {
            debug!("Task {task_id} completion enqueued (success={success}, timed_out={timed_out})");
        }
    }

    /// Update statistics
    fn update_stats(&mut self) {
        self.stats.tasks_currently_queued = self.task_queue.len() as u32;
        self.stats.tasks_currently_running = self.running_tasks.len() as u32;
    }

    fn handle_task_completion(&mut self, completion: TaskCompletion) {
        if let Some(mut info) = self.running_tasks.remove(&completion.task_id) {
            info.status = if completion.success {
                TaskStatus::Completed
            } else if completion.timed_out {
                TaskStatus::TimedOut
            } else {
                TaskStatus::Failed
            };

            debug!(
                "Task {} completion processed (success={}, timed_out={}), running_tasks remaining={}",
                completion.task_id,
                completion.success,
                completion.timed_out,
                self.running_tasks.len()
            );

            if completion.success {
                self.stats.total_tasks_completed += 1;
            } else if completion.timed_out {
                self.stats.total_tasks_timed_out += 1;
            } else {
                self.stats.total_tasks_failed += 1;
            }

            let total_finished = (self.stats.total_tasks_completed
                + self.stats.total_tasks_failed
                + self.stats.total_tasks_timed_out) as f64;

            if total_finished <= 1.0 {
                self.stats.average_execution_time_ms = completion.execution_time_ms;
            } else {
                let prev_total = total_finished - 1.0;
                self.stats.average_execution_time_ms = ((self.stats.average_execution_time_ms
                    * prev_total)
                    + completion.execution_time_ms)
                    / total_finished;
            }

            self.stats.tasks_currently_running = self.running_tasks.len() as u32;
            self.current_load.store(
                self.running_tasks.len() as u32,
                std::sync::atomic::Ordering::SeqCst,
            );
        } else {
            warn!(
                "Received completion for unknown task {} (success={}, timed_out={})",
                completion.task_id, completion.success, completion.timed_out
            );
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> TaskManagerStats {
        self.stats.clone()
    }

    /// Get current load ratio
    pub fn load_ratio(&self) -> f64 {
        if self.config.max_concurrency == 0 {
            1.0
        } else {
            self.running_tasks.len() as f64 / self.config.max_concurrency as f64
        }
    }

    /// Check if we have available capacity
    pub fn has_capacity(&self) -> bool {
        (self.running_tasks.len() as u32) < self.config.max_concurrency
    }

    /// Get detailed status information
    pub fn get_status(&self) -> TaskManagerStatus {
        TaskManagerStatus {
            queue_size: self.task_queue.len(),
            running_count: self.running_tasks.len(),
            max_concurrency: self.config.max_concurrency,
            load_ratio: self.load_ratio(),
            stats: self.stats.clone(),
            running_tasks: self.running_tasks.values().cloned().collect(),
        }
    }

    /// Graceful shutdown
    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down task manager...");

        let running_count = self.running_tasks.len();
        if running_count > 0 {
            info!("Waiting for {running_count} running tasks to complete...");

            // In the new architecture, individual monitoring tasks handle process termination
            // We just wait a reasonable time for them to finish
            let shutdown_timeout = Duration::from_secs(10);
            tokio::time::sleep(shutdown_timeout).await;

            let remaining_count = self.running_tasks.len();
            if remaining_count > 0 {
                warn!("Force shutdown with {remaining_count} tasks still tracked as running");
            }
        }

        // Clear all tasks
        let queued_count = self.task_queue.len();
        self.task_queue.clear();
        self.running_tasks.clear();

        if queued_count > 0 {
            warn!("Discarded {queued_count} queued tasks during shutdown");
        }

        info!("Task manager shutdown complete");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TaskManagerStatus {
    pub queue_size: usize,
    pub running_count: usize,
    pub max_concurrency: u32,
    pub load_ratio: f64,
    pub stats: TaskManagerStats,
    pub running_tasks: Vec<TaskInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    fn create_test_config() -> ShepherdConfig {
        ShepherdConfig {
            uuid: Uuid::new_v4(),
            orchestrator_endpoint: "http://test:52710".to_string(),
            max_concurrency: 2,
            worker_grpc_port: 50052,
            worker_binary_path: "./test-worker".to_string(),
            heartbeat_interval_secs: 30,
            reconnect_backoff_secs: 5,
            worker_timeout_secs: Some(300),
            log_level: Some("info".to_string()),
            domain: "test".to_string(),
            shepherd_group: "default".to_string(),
        }
    }

    fn create_test_task(name: &str) -> IncomingTask {
        IncomingTask {
            task_id: Uuid::new_v4(),
            name: name.to_string(),
            args: vec![], // Empty Vec<serde_json::Value>
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
        }
    }

    #[tokio::test]
    async fn test_task_queueing() {
        let config = create_test_config();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut task_manager = TaskManager::new(config, event_rx, shutdown_rx, current_load);

        // Send a task
        let task = create_test_task("test_task");
        event_tx
            .send(StreamEvent::TaskReceived(task.clone()))
            .unwrap();

        // Handle the event
        if let Ok(Some(event)) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            task_manager.event_receiver.recv(),
        )
        .await
        {
            task_manager.handle_stream_event(event).await.unwrap();
        }

        // Verify task was queued
        assert_eq!(task_manager.task_queue.len(), 1);
        assert_eq!(task_manager.stats.total_tasks_received, 1);
    }

    #[tokio::test]
    async fn test_capacity_management() {
        let config = create_test_config(); // max_concurrency = 2
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut task_manager =
            TaskManager::new(config, event_rx, shutdown_rx, current_load.clone());

        // Add 3 tasks to queue
        task_manager.task_queue.push_back(create_test_task("task1"));
        task_manager.task_queue.push_back(create_test_task("task2"));
        task_manager.task_queue.push_back(create_test_task("task3"));

        // Test queue management
        let initial_queue_size = task_manager.task_queue.len();
        assert_eq!(initial_queue_size, 3);

        // Verify initial state
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_task_tracking() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut task_manager =
            TaskManager::new(config, event_rx, shutdown_rx, current_load.clone());

        // Create a task and add it to running tasks directly for testing
        let task = create_test_task("test_task");
        let task_id = task.task_id;
        let task_info = TaskInfo {
            task: task.clone(),
            queued_at: Utc::now(),
            started_at: Some(Utc::now()),
            status: TaskStatus::Running,
            process_id: Some(12345),
        };
        task_manager.running_tasks.insert(task_id, task_info);
        task_manager
            .current_load
            .store(1, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(task_manager.running_tasks.len(), 1);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 1);

        // In the new architecture, individual monitoring tasks handle completion
        // This test just verifies task tracking works
        assert!(task_manager.running_tasks.contains_key(&task_id));
    }

    #[tokio::test]
    async fn test_event_handling() {
        let config = create_test_config();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut task_manager =
            TaskManager::new(config, event_rx, shutdown_rx, current_load.clone());

        // Test that only TaskReceived events are processed
        let task = create_test_task("test_task");
        event_tx
            .send(StreamEvent::TaskReceived(task.clone()))
            .unwrap();
        event_tx.send(StreamEvent::PingReceived).unwrap(); // Should be ignored
        event_tx.send(StreamEvent::ConnectionLost).unwrap(); // Should be ignored

        // Handle events
        let event1 = task_manager.event_receiver.recv().await.unwrap();
        task_manager.handle_stream_event(event1).await.unwrap();

        let event2 = task_manager.event_receiver.recv().await.unwrap();
        task_manager.handle_stream_event(event2).await.unwrap();

        let event3 = task_manager.event_receiver.recv().await.unwrap();
        task_manager.handle_stream_event(event3).await.unwrap();

        // Only the TaskReceived event should have been processed
        assert_eq!(task_manager.task_queue.len(), 1);
        assert_eq!(task_manager.stats.total_tasks_received, 1);
    }

    #[test]
    fn test_load_ratio_calculation() {
        let config = create_test_config(); // max_concurrency = 2
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let task_manager = TaskManager::new(config, event_rx, shutdown_rx, current_load);

        assert_eq!(task_manager.load_ratio(), 0.0);
        assert!(task_manager.has_capacity());

        // The running_tasks would be updated in real usage
        // This test just verifies the calculation logic
    }
}
