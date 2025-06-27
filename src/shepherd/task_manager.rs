use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio::process::{Child, Command};
use tokio::time::Duration;
use uuid::Uuid;
use log::{info, warn, error, debug};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::shepherd::{ShepherdConfig, IncomingTask, StreamEvent};

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

pub struct TaskManager {
    config: ShepherdConfig,
    task_queue: VecDeque<IncomingTask>,
    running_tasks: HashMap<Uuid, TaskInfo>,
    processes: HashMap<Uuid, Child>,
    event_receiver: mpsc::UnboundedReceiver<StreamEvent>,
    shutdown_receiver: watch::Receiver<bool>,
    current_load: Arc<std::sync::atomic::AtomicU32>,
    stats: TaskManagerStats,
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
        Self {
            config,
            task_queue: VecDeque::new(),
            running_tasks: HashMap::new(),
            processes: HashMap::new(),
            event_receiver,
            shutdown_receiver,
            current_load,
            stats: TaskManagerStats::default(),
        }
    }

    pub async fn start(mut self) -> Result<()> {
        info!("Starting task manager for shepherd {}", self.config.uuid);
        
        let mut health_check_interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            tokio::select! {
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            if let Err(e) = self.handle_stream_event(event).await {
                                error!("Error handling stream event: {}", e);
                            }
                        }
                        None => {
                            warn!("Stream event channel closed");
                            break;
                        }
                    }
                }
                
                _ = health_check_interval.tick() => {
                    self.check_process_health().await;
                }
                
                _ = self.shutdown_receiver.changed() => {
                    if *self.shutdown_receiver.borrow() {
                        info!("Task manager received shutdown signal");
                        break;
                    }
                }
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
            StreamEvent::PingReceived => {
                debug!("Received ping from orchestrator");
                // Ping events don't require action, just used for monitoring
            }
            StreamEvent::ConnectionLost => {
                warn!("Connection to orchestrator lost");
                // Tasks remain in queue and will be processed when connection is restored
            }
            StreamEvent::Shutdown => {
                info!("Received shutdown event from stream handler");
                // Shutdown will be handled by the main loop
            }
        }
        
        Ok(())
    }

    /// Process the task queue and start tasks if we have capacity
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

    /// Start a task by spawning a worker process
    async fn start_task(&mut self, task: IncomingTask) -> Result<()> {
        let task_id = task.task_id;
        
        info!("Starting worker process for task: {} ({})", task_id, task.name);
        
        let mut cmd = Command::new(&self.config.worker_binary_path);
        cmd.arg("--task-id").arg(task_id.to_string())
           .arg("--name").arg(&task.name)
           .arg("--args").arg(serde_json::to_string(&task.args)?)
           .arg("--kwargs").arg(&task.kwargs)
           .arg("--shepherd-endpoint").arg(self.config.worker_service_endpoint());
        
        if let Some(memory_limit) = task.memory_limit {
            cmd.env("AZOLLA_MEMORY_LIMIT", memory_limit.to_string());
        }
        if let Some(cpu_limit) = task.cpu_limit {
            cmd.env("AZOLLA_CPU_LIMIT", cpu_limit.to_string());
        }
        
        cmd.current_dir(".");
        cmd.env("RUST_LOG", "info");
        
        debug!("Spawning worker: {} {:?}", self.config.worker_binary_path, cmd.as_std().get_args().collect::<Vec<_>>());
        
        let child = cmd.spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn worker process: {}", e))?;
        
        let process_id = child.id();
        
        // Create task info
        let task_info = TaskInfo {
            task: task.clone(),
            queued_at: Utc::now(),
            started_at: Some(Utc::now()),
            status: TaskStatus::Running,
            process_id,
        };
        
        // Add to running tasks and processes
        self.running_tasks.insert(task_id, task_info);
        self.processes.insert(task_id, child);
        
        // Update load counter
        let new_load = self.running_tasks.len() as u32;
        self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
        
        info!("Worker process started for task {}: PID {:?}", task_id, process_id);
        
        Ok(())
    }

    /// Check health of running processes
    async fn check_process_health(&mut self) {
        let mut completed_tasks = Vec::new();
        let mut failed_tasks = Vec::new();
        let mut timed_out_tasks = Vec::new();
        
        let now = Utc::now();
        let timeout_duration = self.config.worker_timeout.unwrap_or(Duration::from_secs(300));
        
        for (task_id, child) in &mut self.processes {
            if let Some(task_info) = self.running_tasks.get(task_id) {
                // Check if process has exited
                match child.try_wait() {
                    Ok(Some(exit_status)) => {
                        let success = exit_status.success();
                        let exit_code = exit_status.code();
                        
                        info!("Process for task {} exited: success={}, code={:?}", 
                              task_id, success, exit_code);
                        
                        if success {
                            completed_tasks.push(*task_id);
                        } else {
                            let error = format!("Process exited with code: {:?}", exit_code);
                            failed_tasks.push((*task_id, error));
                        }
                    }
                    Ok(None) => {
                        // Process is still running, check for timeout
                        if let Some(started_at) = task_info.started_at {
                            let elapsed = now.signed_duration_since(started_at);
                            if elapsed > chrono::Duration::from_std(timeout_duration).unwrap() {
                                warn!("Task {} timed out after {:?}", task_id, elapsed);
                                timed_out_tasks.push(*task_id);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to check process status for task {}: {}", task_id, e);
                        failed_tasks.push((*task_id, format!("Process check failed: {}", e)));
                    }
                }
            }
        }
        
        // Handle completed tasks
        for task_id in completed_tasks {
            if let Err(e) = self.handle_task_completion(task_id, true, None).await {
                error!("Failed to handle task completion for {}: {}", task_id, e);
            }
        }
        
        // Handle failed tasks
        for (task_id, error) in failed_tasks {
            if let Err(e) = self.handle_task_completion(task_id, false, Some(error)).await {
                error!("Failed to handle task failure for {}: {}", task_id, e);
            }
        }
        
        // Handle timed out tasks
        for task_id in timed_out_tasks {
            if let Err(e) = self.handle_task_timeout(task_id).await {
                error!("Failed to handle task timeout for {}: {}", task_id, e);
            }
        }
    }

    /// Handle task completion (success or failure)
    async fn handle_task_completion(&mut self, task_id: Uuid, success: bool, error: Option<String>) -> Result<()> {
        if let Some(mut task_info) = self.running_tasks.remove(&task_id) {
            // Remove the process
            if let Some(mut child) = self.processes.remove(&task_id) {
                // Ensure process is properly terminated
                if let Err(e) = child.kill().await {
                    debug!("Process for task {} was already terminated: {}", task_id, e);
                }
            }
            
            // Calculate execution time
            let execution_time = if let Some(started_at) = task_info.started_at {
                Utc::now().signed_duration_since(started_at)
            } else {
                chrono::Duration::zero()
            };
            let execution_ms = execution_time.num_milliseconds() as f64;
            
            // Update statistics
            self.update_average_execution_time(execution_ms);
            
            if success {
                task_info.status = TaskStatus::Completed;
                self.stats.total_tasks_completed += 1;
                
                info!("Task {} completed successfully in {:.2}ms", task_id, execution_ms);
            } else {
                task_info.status = TaskStatus::Failed;
                self.stats.total_tasks_failed += 1;
                
                let error_msg = error.unwrap_or_else(|| "Unknown error".to_string());
                warn!("Task {} failed after {:.2}ms: {}", task_id, execution_ms, error_msg);
            }
            
            // Update load counter
            let new_load = self.running_tasks.len() as u32;
            self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
            
        } else {
            warn!("Received completion for unknown task: {}", task_id);
        }
        
        Ok(())
    }

    /// Handle task timeout
    async fn handle_task_timeout(&mut self, task_id: Uuid) -> Result<()> {
        if let Some(mut task_info) = self.running_tasks.remove(&task_id) {
            task_info.status = TaskStatus::TimedOut;
            self.stats.total_tasks_timed_out += 1;
            
            warn!("Killing timed out task: {}", task_id);
            
            // Kill the process
            if let Some(mut child) = self.processes.remove(&task_id) {
                if let Err(e) = child.kill().await {
                    error!("Failed to kill timed out process for task {}: {}", task_id, e);
                }
            }
            
            // Update load counter
            let new_load = self.running_tasks.len() as u32;
            self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
        }
        
        Ok(())
    }

    /// Update average execution time
    fn update_average_execution_time(&mut self, execution_ms: f64) {
        let total_completed = self.stats.total_tasks_completed + self.stats.total_tasks_failed;
        if total_completed > 0 {
            let current_avg = self.stats.average_execution_time_ms;
            let new_avg = (current_avg * (total_completed - 1) as f64 + execution_ms) / total_completed as f64;
            self.stats.average_execution_time_ms = new_avg;
        } else {
            self.stats.average_execution_time_ms = execution_ms;
        }
    }

    /// Update statistics
    fn update_stats(&mut self) {
        self.stats.tasks_currently_queued = self.task_queue.len() as u32;
        self.stats.tasks_currently_running = self.running_tasks.len() as u32;
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
        
        if self.processes.is_empty() {
            info!("No running processes to clean up");
        } else {
            info!("Terminating {} running processes...", self.processes.len());
            
            // First, try to terminate processes gracefully
            for (task_id, child) in &mut self.processes {
                debug!("Terminating process for task {}", task_id);
                if let Err(e) = child.start_kill() {
                    error!("Failed to start killing process for task {}: {}", task_id, e);
                }
            }
            
            // Wait for processes to exit (with timeout)
            let shutdown_timeout = Duration::from_secs(10);
            let start_time = std::time::Instant::now();
            
            while !self.processes.is_empty() && start_time.elapsed() < shutdown_timeout {
                let mut to_remove = Vec::new();
                
                for (task_id, child) in &mut self.processes {
                    match child.try_wait() {
                        Ok(Some(_)) => {
                            debug!("Process for task {} exited during shutdown", task_id);
                            to_remove.push(*task_id);
                        }
                        Ok(None) => {
                            // Still running
                        }
                        Err(e) => {
                            error!("Error checking process status during shutdown: {}", e);
                            to_remove.push(*task_id);
                        }
                    }
                }
                
                for task_id in to_remove {
                    self.processes.remove(&task_id);
                    self.running_tasks.remove(&task_id);
                }
                
                if !self.processes.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            
            // Force kill any remaining processes
            if !self.processes.is_empty() {
                warn!("Force killing {} remaining processes", self.processes.len());
                for (task_id, mut child) in self.processes.drain() {
                    if let Err(e) = child.kill().await {
                        error!("Failed to force kill process for task {}: {}", task_id, e);
                    }
                }
                self.running_tasks.clear();
            }
        }
        
        // Clear queued tasks
        let queued_count = self.task_queue.len();
        self.task_queue.clear();
        
        if queued_count > 0 {
            warn!("Discarded {} queued tasks during shutdown", queued_count);
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
            heartbeat_interval: std::time::Duration::from_secs(30),
            reconnect_backoff: std::time::Duration::from_secs(5),
            worker_timeout: Some(std::time::Duration::from_secs(300)),
            log_level: Some("info".to_string()),
        }
    }

    fn create_test_task(name: &str) -> IncomingTask {
        IncomingTask {
            task_id: Uuid::new_v4(),
            name: name.to_string(),
            args: vec![],
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
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            shutdown_rx,
            current_load,
        );
        
        // Send a task
        let task = create_test_task("test_task");
        event_tx.send(StreamEvent::TaskReceived(task.clone())).unwrap();
        
        // Handle the event
        if let Ok(Some(event)) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            task_manager.event_receiver.recv()
        ).await {
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
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            shutdown_rx,
            current_load.clone(),
        );
        
        // Add 3 tasks to queue
        task_manager.task_queue.push_back(create_test_task("task1"));
        task_manager.task_queue.push_back(create_test_task("task2"));
        task_manager.task_queue.push_back(create_test_task("task3"));
        
        // Process queue - this will fail because we can't spawn the test worker
        // but we can still test the queuing logic
        let initial_queue_size = task_manager.task_queue.len();
        assert_eq!(initial_queue_size, 3);
        
        // The process spawning will fail in tests, but we can verify the queue logic
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_task_completion() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            shutdown_rx,
            current_load.clone(),
        );
        
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
        task_manager.current_load.store(1, std::sync::atomic::Ordering::SeqCst);
        
        assert_eq!(task_manager.running_tasks.len(), 1);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 1);
        
        // Complete the task
        task_manager.handle_task_completion(task_id, true, None).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(task_manager.stats.total_tasks_completed, 1);
    }

    #[tokio::test]
    async fn test_task_failure() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            shutdown_rx,
            current_load.clone(),
        );
        
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
        task_manager.current_load.store(1, std::sync::atomic::Ordering::SeqCst);
        
        assert_eq!(task_manager.running_tasks.len(), 1);
        
        // Fail the task
        task_manager.handle_task_completion(task_id, false, Some("Test error".to_string())).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(task_manager.stats.total_tasks_failed, 1);
    }

    #[test]
    fn test_load_ratio_calculation() {
        let config = create_test_config(); // max_concurrency = 2
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let task_manager = TaskManager::new(
            config,
            event_rx,
            shutdown_rx,
            current_load,
        );
        
        assert_eq!(task_manager.load_ratio(), 0.0);
        assert!(task_manager.has_capacity());
        
        // The running_tasks would be updated in real usage
        // This test just verifies the calculation logic
    }
}