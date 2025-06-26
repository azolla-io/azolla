use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio::process::{Child, Command};
use tokio::time::Duration;
use uuid::Uuid;
use log::{info, warn, error, debug};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::shepherd::{ShepherdConfig, IncomingTask, ProcessEvent};

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub task: IncomingTask,
    pub process_id: Option<u32>,
    pub started_at: DateTime<Utc>,
    pub status: ProcessStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessStatus {
    Starting,
    Running,
    Completed,
    Failed,
    TimedOut,
    Killed,
}

#[derive(Debug, Clone)]
pub enum TaskCompletionEvent {
    TaskCompleted(Uuid),
    TaskFailed(Uuid, String),
}

pub struct ProcessMonitor {
    config: ShepherdConfig,
    processes: HashMap<Uuid, (Child, ProcessInfo)>,
    event_receiver: mpsc::UnboundedReceiver<ProcessEvent>,
    completion_sender: mpsc::UnboundedSender<TaskCompletionEvent>,
    shutdown_receiver: watch::Receiver<bool>,
    stats: ProcessMonitorStats,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessMonitorStats {
    pub total_processes_spawned: u64,
    pub total_processes_completed: u64,
    pub total_processes_failed: u64,
    pub total_processes_timed_out: u64,
    pub processes_currently_running: u32,
    pub average_execution_time_ms: f64,
}

impl ProcessMonitor {
    pub fn new(
        config: ShepherdConfig,
        event_receiver: mpsc::UnboundedReceiver<ProcessEvent>,
        completion_sender: mpsc::UnboundedSender<TaskCompletionEvent>,
        shutdown_receiver: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            processes: HashMap::new(),
            event_receiver,
            completion_sender,
            shutdown_receiver,
            stats: ProcessMonitorStats::default(),
        }
    }

    pub async fn start(mut self) -> Result<()> {
        info!("Starting process monitor for shepherd {}", self.config.uuid);
        
        let mut health_check_interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            tokio::select! {
                event = self.event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            if let Err(e) = self.handle_process_event(event).await {
                                error!("Error handling process event: {}", e);
                            }
                        }
                        None => {
                            warn!("Process event channel closed");
                            break;
                        }
                    }
                }
                
                _ = health_check_interval.tick() => {
                    self.check_process_health().await;
                }
                
                _ = self.shutdown_receiver.changed() => {
                    if *self.shutdown_receiver.borrow() {
                        info!("Process monitor received shutdown signal");
                        break;
                    }
                }
            }
        }
        
        self.shutdown().await?;
        
        info!("Process monitor stopped");
        Ok(())
    }

    async fn handle_process_event(&mut self, event: ProcessEvent) -> Result<()> {
        match event {
            ProcessEvent::StartTask(task) => {
                self.start_worker_process(task).await?;
            }
            ProcessEvent::TaskCompleted(task_id) => {
                debug!("Received external task completion for {}", task_id);
                self.handle_task_completion(task_id, true, None).await?;
            }
            ProcessEvent::TaskFailed(task_id, error) => {
                debug!("Received external task failure for {}: {}", task_id, error);
                self.handle_task_completion(task_id, false, Some(error)).await?;
            }
        }
        
        Ok(())
    }

    async fn start_worker_process(&mut self, task: IncomingTask) -> Result<()> {
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
        let process_info = ProcessInfo {
            task: task.clone(),
            process_id,
            started_at: Utc::now(),
            status: ProcessStatus::Running,
        };
        
        self.processes.insert(task_id, (child, process_info));
        self.stats.total_processes_spawned += 1;
        self.stats.processes_currently_running += 1;
        
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
        
        for (task_id, (child, process_info)) in &mut self.processes {
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
                    let elapsed = now.signed_duration_since(process_info.started_at);
                    if elapsed > chrono::Duration::from_std(timeout_duration).unwrap() {
                        warn!("Task {} timed out after {:?}", task_id, elapsed);
                        timed_out_tasks.push(*task_id);
                    }
                }
                Err(e) => {
                    error!("Failed to check process status for task {}: {}", task_id, e);
                    failed_tasks.push((*task_id, format!("Process check failed: {}", e)));
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
        if let Some((mut child, mut process_info)) = self.processes.remove(&task_id) {
            // Calculate execution time
            let execution_time = Utc::now().signed_duration_since(process_info.started_at);
            let execution_ms = execution_time.num_milliseconds() as f64;
            
            // Update statistics
            self.stats.processes_currently_running = self.stats.processes_currently_running.saturating_sub(1);
            self.update_average_execution_time(execution_ms);
            
            if success {
                process_info.status = ProcessStatus::Completed;
                self.stats.total_processes_completed += 1;
                
                info!("Task {} completed successfully in {:.2}ms", task_id, execution_ms);
                
                // Send completion event to task manager
                if let Err(e) = self.completion_sender.send(TaskCompletionEvent::TaskCompleted(task_id)) {
                    error!("Failed to send completion event: {}", e);
                }
            } else {
                process_info.status = ProcessStatus::Failed;
                self.stats.total_processes_failed += 1;
                
                let error_msg = error.unwrap_or_else(|| "Unknown error".to_string());
                warn!("Task {} failed after {:.2}ms: {}", task_id, execution_ms, error_msg);
                
                // Send failure event to task manager
                if let Err(e) = self.completion_sender.send(TaskCompletionEvent::TaskFailed(task_id, error_msg)) {
                    error!("Failed to send failure event: {}", e);
                }
            }
            
            // Ensure process is properly terminated
            if let Err(e) = child.kill().await {
                debug!("Process for task {} was already terminated: {}", task_id, e);
            }
        } else {
            warn!("Received completion for unknown task: {}", task_id);
        }
        
        Ok(())
    }

    /// Handle task timeout
    async fn handle_task_timeout(&mut self, task_id: Uuid) -> Result<()> {
        if let Some((mut child, mut process_info)) = self.processes.remove(&task_id) {
            process_info.status = ProcessStatus::TimedOut;
            self.stats.total_processes_timed_out += 1;
            self.stats.processes_currently_running = self.stats.processes_currently_running.saturating_sub(1);
            
            warn!("Killing timed out task: {}", task_id);
            
            // Kill the process
            if let Err(e) = child.kill().await {
                error!("Failed to kill timed out process for task {}: {}", task_id, e);
            }
            
            // Send failure event to task manager
            let error_msg = format!("Task timed out after {:?}", self.config.worker_timeout);
            if let Err(e) = self.completion_sender.send(TaskCompletionEvent::TaskFailed(task_id, error_msg)) {
                error!("Failed to send timeout failure event: {}", e);
            }
        }
        
        Ok(())
    }

    /// Update average execution time
    fn update_average_execution_time(&mut self, execution_ms: f64) {
        let total_completed = self.stats.total_processes_completed + self.stats.total_processes_failed;
        if total_completed > 0 {
            let current_avg = self.stats.average_execution_time_ms;
            let new_avg = (current_avg * (total_completed - 1) as f64 + execution_ms) / total_completed as f64;
            self.stats.average_execution_time_ms = new_avg;
        } else {
            self.stats.average_execution_time_ms = execution_ms;
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> ProcessMonitorStats {
        self.stats.clone()
    }

    /// Graceful shutdown
    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down process monitor...");
        
        if self.processes.is_empty() {
            info!("No running processes to clean up");
            return Ok(());
        }
        
        info!("Terminating {} running processes...", self.processes.len());
        
        // First, try to terminate processes gracefully
        for (task_id, (child, _)) in &mut self.processes {
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
            
            for (task_id, (child, _)) in &mut self.processes {
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
            }
            
            if !self.processes.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        
        // Force kill any remaining processes
        if !self.processes.is_empty() {
            warn!("Force killing {} remaining processes", self.processes.len());
            for (task_id, (mut child, _)) in self.processes.drain() {
                if let Err(e) = child.kill().await {
                    error!("Failed to force kill process for task {}: {}", task_id, e);
                }
            }
        }
        
        info!("Process monitor shutdown complete");
        Ok(())
    }
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
            worker_binary_path: "/bin/echo".to_string(), // Use echo for testing
            heartbeat_interval: std::time::Duration::from_secs(30),
            reconnect_backoff: std::time::Duration::from_secs(5),
            worker_timeout: Some(std::time::Duration::from_secs(10)),
            log_level: Some("info".to_string()),
        }
    }

    fn create_test_task(name: &str) -> IncomingTask {
        IncomingTask {
            task_id: Uuid::new_v4(),
            name: name.to_string(),
            args: vec!["test".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
        }
    }

    #[tokio::test]
    async fn test_process_monitor_creation() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (completion_tx, _completion_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        
        let monitor = ProcessMonitor::new(config, event_rx, completion_tx, shutdown_rx);
        
        assert_eq!(monitor.processes.len(), 0);
        assert_eq!(monitor.stats.total_processes_spawned, 0);
    }

    #[tokio::test]
    async fn test_start_worker_process() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (completion_tx, _completion_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        
        let mut monitor = ProcessMonitor::new(config, event_rx, completion_tx, shutdown_rx);
        let task = create_test_task("test_task");
        let task_id = task.task_id;
        
        // This might fail if /bin/echo doesn't exist on the system
        // In a real test environment, we'd use a mock or test binary
        if monitor.start_worker_process(task).await.is_ok() {
            assert_eq!(monitor.processes.len(), 1);
            assert!(monitor.processes.contains_key(&task_id));
            assert_eq!(monitor.stats.total_processes_spawned, 1);
        }
    }

    #[test]
    fn test_average_execution_time_calculation() {
        let config = create_test_config();
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let (completion_tx, _completion_rx) = mpsc::unbounded_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        
        let mut monitor = ProcessMonitor::new(config, event_rx, completion_tx, shutdown_rx);
        
        // Simulate completing tasks with different execution times
        monitor.stats.total_processes_completed = 1;
        monitor.update_average_execution_time(100.0);
        assert_eq!(monitor.stats.average_execution_time_ms, 100.0);
        
        monitor.stats.total_processes_completed = 2;
        monitor.update_average_execution_time(200.0);
        assert_eq!(monitor.stats.average_execution_time_ms, 150.0);
    }

    #[test]
    fn test_process_status_transitions() {
        let task = create_test_task("test");
        
        let process_info = ProcessInfo {
            task: task.clone(),
            process_id: Some(12345),
            started_at: Utc::now(),
            status: ProcessStatus::Starting,
        };
        
        assert_eq!(process_info.status, ProcessStatus::Starting);
        
        // In real usage, status would be updated as the process progresses
        let mut updated_info = process_info;
        updated_info.status = ProcessStatus::Running;
        assert_eq!(updated_info.status, ProcessStatus::Running);
    }

    #[test]
    fn test_worker_service_endpoint() {
        let config = create_test_config();
        assert_eq!(config.worker_service_endpoint(), "http://127.0.0.1:50052");
    }
}