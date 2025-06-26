use anyhow::Result;
use tokio::sync::{mpsc, watch};
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub enum ProcessEvent {
    StartTask(IncomingTask),
    TaskCompleted(Uuid),
    TaskFailed(Uuid, String),
}

pub struct TaskManager {
    config: ShepherdConfig,
    task_queue: VecDeque<IncomingTask>,
    running_tasks: HashMap<Uuid, TaskInfo>,
    event_receiver: mpsc::UnboundedReceiver<StreamEvent>,
    process_sender: mpsc::UnboundedSender<ProcessEvent>,
    shutdown_receiver: watch::Receiver<bool>,
    current_load: Arc<std::sync::atomic::AtomicU32>,
    stats: TaskManagerStats,
}

#[derive(Debug, Clone, Default)]
pub struct TaskManagerStats {
    pub total_tasks_received: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub tasks_currently_queued: u32,
    pub tasks_currently_running: u32,
    pub max_queue_size_reached: u32,
}

impl TaskManager {
    pub fn new(
        config: ShepherdConfig,
        event_receiver: mpsc::UnboundedReceiver<StreamEvent>,
        process_sender: mpsc::UnboundedSender<ProcessEvent>,
        shutdown_receiver: watch::Receiver<bool>,
        current_load: Arc<std::sync::atomic::AtomicU32>,
    ) -> Self {
        Self {
            config,
            task_queue: VecDeque::new(),
            running_tasks: HashMap::new(),
            event_receiver,
            process_sender,
            shutdown_receiver,
            current_load,
            stats: TaskManagerStats::default(),
        }
    }

    pub async fn start(mut self) -> Result<()> {
        info!("Starting task manager for shepherd {}", self.config.uuid);
        
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

    /// Start a task by sending it to the process monitor
    async fn start_task(&mut self, task: IncomingTask) -> Result<()> {
        let task_id = task.task_id;
        
        info!("Starting task: {} ({})", task_id, task.name);
        
        // Create task info
        let task_info = TaskInfo {
            task: task.clone(),
            queued_at: Utc::now(),
            started_at: Some(Utc::now()),
            status: TaskStatus::Running,
        };
        
        // Add to running tasks
        self.running_tasks.insert(task_id, task_info);
        
        // Update load counter
        let new_load = self.running_tasks.len() as u32;
        self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
        
        // Send to process monitor
        self.process_sender.send(ProcessEvent::StartTask(task))
            .map_err(|e| anyhow::anyhow!("Failed to send task to process monitor: {}", e))?;
        
        debug!("Task {} sent to process monitor. Running tasks: {}", task_id, self.running_tasks.len());
        
        Ok(())
    }

    /// Handle task completion from process monitor
    pub async fn handle_task_completed(&mut self, task_id: Uuid) -> Result<()> {
        if let Some(mut task_info) = self.running_tasks.remove(&task_id) {
            task_info.status = TaskStatus::Completed;
            self.stats.total_tasks_completed += 1;
            
            info!("Task {} completed successfully", task_id);
            
            // Update load counter
            let new_load = self.running_tasks.len() as u32;
            self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
            
            debug!("Task completed. Running tasks: {}", self.running_tasks.len());
        } else {
            warn!("Received completion for unknown task: {}", task_id);
        }
        
        Ok(())
    }

    /// Handle task failure from process monitor
    pub async fn handle_task_failed(&mut self, task_id: Uuid, error: String) -> Result<()> {
        if let Some(mut task_info) = self.running_tasks.remove(&task_id) {
            task_info.status = TaskStatus::Failed;
            self.stats.total_tasks_failed += 1;
            
            warn!("Task {} failed: {}", task_id, error);
            
            // Update load counter
            let new_load = self.running_tasks.len() as u32;
            self.current_load.store(new_load, std::sync::atomic::Ordering::SeqCst);
            
            debug!("Task failed. Running tasks: {}", self.running_tasks.len());
        } else {
            warn!("Received failure for unknown task: {}", task_id);
        }
        
        Ok(())
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
        
        // Wait for running tasks to complete (with timeout)
        let shutdown_timeout = std::time::Duration::from_secs(30);
        let start_time = std::time::Instant::now();
        
        while !self.running_tasks.is_empty() && start_time.elapsed() < shutdown_timeout {
            warn!("Waiting for {} running tasks to complete...", self.running_tasks.len());
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            
            
            // TODO: In a real implementation, we would receive completion/failure events
            // from the process monitor during shutdown
        }
        
        if !self.running_tasks.is_empty() {
            warn!("Force shutdown with {} tasks still running", self.running_tasks.len());
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
        let (process_tx, _process_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            process_tx,
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
        let (process_tx, mut process_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            process_tx,
            shutdown_rx,
            current_load.clone(),
        );
        
        // Add 3 tasks to queue
        task_manager.task_queue.push_back(create_test_task("task1"));
        task_manager.task_queue.push_back(create_test_task("task2"));
        task_manager.task_queue.push_back(create_test_task("task3"));
        
        // Process queue
        task_manager.process_queue().await.unwrap();
        
        // Should start 2 tasks (max_concurrency), leave 1 in queue
        assert_eq!(task_manager.task_queue.len(), 1);
        assert_eq!(task_manager.running_tasks.len(), 2);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 2);
        
        // Verify 2 process events were sent
        let _event1 = process_rx.try_recv().unwrap();
        let _event2 = process_rx.try_recv().unwrap();
        assert!(process_rx.try_recv().is_err()); // No third event
    }

    #[tokio::test]
    async fn test_task_completion() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (process_tx, _process_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            process_tx,
            shutdown_rx,
            current_load.clone(),
        );
        
        // Start a task
        let task = create_test_task("test_task");
        let task_id = task.task_id;
        task_manager.start_task(task).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 1);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 1);
        
        // Complete the task
        task_manager.handle_task_completed(task_id).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(task_manager.stats.total_tasks_completed, 1);
    }

    #[tokio::test]
    async fn test_task_failure() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (process_tx, _process_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let mut task_manager = TaskManager::new(
            config,
            event_rx,
            process_tx,
            shutdown_rx,
            current_load.clone(),
        );
        
        // Start a task
        let task = create_test_task("test_task");
        let task_id = task.task_id;
        task_manager.start_task(task).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 1);
        
        // Fail the task
        task_manager.handle_task_failed(task_id, "Test error".to_string()).await.unwrap();
        
        assert_eq!(task_manager.running_tasks.len(), 0);
        assert_eq!(current_load.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(task_manager.stats.total_tasks_failed, 1);
    }

    #[test]
    fn test_load_ratio_calculation() {
        let config = create_test_config(); // max_concurrency = 2
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (process_tx, _process_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_load = Arc::new(std::sync::atomic::AtomicU32::new(0));
        
        let task_manager = TaskManager::new(
            config,
            event_rx,
            process_tx,
            shutdown_rx,
            current_load,
        );
        
        assert_eq!(task_manager.load_ratio(), 0.0);
        assert!(task_manager.has_capacity());
        
        // The running_tasks would be updated in real usage
        // This test just verifies the calculation logic
    }
}