use crate::error::AzollaError;
use crate::task::Task;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Worker for executing tasks
pub struct Worker {
    config: WorkerConfig,
    tasks: HashMap<String, Arc<dyn Task>>,
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

    /// Run the worker (this would connect to orchestrator and process tasks)
    pub async fn run(self) -> Result<(), AzollaError> {
        // TODO: Implement actual worker logic
        // For now, just log the configuration
        log::info!(
            "Worker starting with {} tasks on {}:{} (group: {})",
            self.tasks.len(),
            self.config.orchestrator_endpoint,
            self.config.domain,
            self.config.shepherd_group
        );

        // In a real implementation, this would:
        // 1. Connect to orchestrator via gRPC
        // 2. Register as a shepherd with the task names
        // 3. Listen for task dispatches
        // 4. Execute tasks and report results

        Ok(())
    }
}

/// Builder for worker configuration
pub struct WorkerBuilder {
    config: WorkerConfig,
    tasks: HashMap<String, Arc<dyn Task>>,
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        Self {
            config: WorkerConfig::default(),
            tasks: HashMap::new(),
        }
    }
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
        })
    }
}
