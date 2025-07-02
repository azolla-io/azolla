use anyhow::Result;
use std::sync::Arc;

use crate::orchestrator::db::PgPool;
use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};
use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::orchestrator::taskset::TaskSetRegistry;

/// The central orchestration engine containing all shared state and infrastructure
#[derive(Clone)]
pub struct Engine {
    pub pool: PgPool,
    pub registry: Arc<TaskSetRegistry>,
    pub event_stream: Arc<EventStream>,
    pub shepherd_manager: Arc<ShepherdManager>,
}

impl Engine {
    /// Create a new orchestration engine with the given database pool and configuration
    pub fn new(pool: PgPool, event_stream_config: EventStreamConfig) -> Self {
        let event_stream = Arc::new(EventStream::new(pool.clone(), event_stream_config));
        let registry = Arc::new(TaskSetRegistry::new());
        let shepherd_manager =
            Arc::new(ShepherdManager::new(registry.clone(), event_stream.clone()));

        Self {
            pool,
            registry,
            event_stream,
            shepherd_manager,
        }
    }

    /// Initialize the engine by loading existing data from the database
    pub async fn initialize(&self) -> Result<()> {
        // Load existing tasks from database into the registry
        self.registry.load_from_db(&self.pool).await?;
        log::info!(
            "Engine initialized - TaskSetRegistry loaded with {} domains",
            self.registry.domains().len()
        );
        Ok(())
    }

    /// Shutdown the engine and all its components
    pub async fn shutdown(&self) -> Result<()> {
        log::info!("Shutting down orchestration engine...");
        self.event_stream.shutdown().await?;
        log::info!("Orchestration engine shutdown complete");
        Ok(())
    }

    /// Merge events from the events table to task_instance and task_attempts tables
    /// This method can be called periodically or on-demand to sync the event log
    pub async fn merge_events_to_db(&self) -> Result<()> {
        self.registry.merge_events_to_db(&self.pool).await
    }
}
