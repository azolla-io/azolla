use anyhow::Result;
use std::sync::Arc;

use crate::orchestrator::db::PgPool;
use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};
use crate::orchestrator::scheduler::{SchedulerConfig, SchedulerRegistry};
use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::orchestrator::taskset::TaskSetRegistry;

/// The central orchestration engine containing all shared state and infrastructure
#[derive(Clone)]
pub struct Engine {
    pub pool: PgPool,
    pub registry: Arc<TaskSetRegistry>,
    pub event_stream: Arc<EventStream>,
    pub shepherd_manager: Arc<ShepherdManager>,
    pub scheduler_registry: Arc<SchedulerRegistry>,
}

impl Engine {
    /// Create a new orchestration engine with the given database pool and configuration
    pub fn new(pool: PgPool, event_stream_config: EventStreamConfig) -> Self {
        Self::with_scheduler_config(pool, event_stream_config, SchedulerConfig::default())
    }

    /// Create a new orchestration engine with custom scheduler configuration
    pub fn with_scheduler_config(
        pool: PgPool,
        event_stream_config: EventStreamConfig,
        scheduler_config: SchedulerConfig,
    ) -> Self {
        let event_stream = Arc::new(EventStream::new(pool.clone(), event_stream_config));
        let registry = Arc::new(TaskSetRegistry::new());
        let shepherd_manager =
            Arc::new(ShepherdManager::new(registry.clone(), event_stream.clone()));
        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            registry.clone(),
            shepherd_manager.clone(),
            event_stream.clone(),
            scheduler_config,
        ));

        Self {
            pool,
            registry,
            event_stream,
            shepherd_manager,
            scheduler_registry,
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

        // Shutdown schedulers first
        if let Err(e) = self.scheduler_registry.shutdown_all().await {
            log::error!("Failed to shutdown scheduler registry: {e}");
        }

        // Then shutdown event stream
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
