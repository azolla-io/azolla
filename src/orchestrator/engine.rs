use anyhow::Result;
use std::sync::Arc;

use crate::orchestrator::db::{DomainsConfig, PgPool};
use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};
use crate::orchestrator::scheduler::{SchedulerConfig, SchedulerRegistry};
use crate::orchestrator::shepherd_registry::ShepherdManagerRegistry;
use crate::orchestrator::taskset::TaskSetRegistry;

/// The central orchestration engine containing all shared state and infrastructure
#[derive(Clone)]
pub struct Engine {
    pub pool: PgPool,

    /// TaskSetRegistry holds TaskSets until they are transferred to SchedulerActors.
    ///
    /// ⚠️  IMPORTANT USAGE WARNING:
    ///
    /// The TaskSetRegistry transfers ownership of TaskSets to SchedulerActors via
    /// `extract_task_set()` when schedulers are created. Once a SchedulerActor owns
    /// a TaskSet for a domain, direct access to the registry for that domain will
    /// NOT reflect the current state.
    ///
    /// CORRECT PATTERNS:
    /// - ✅ Create tasks via ClientService (delegates to SchedulerActor)
    /// - ✅ Access tasks via SchedulerActor methods (get_task_for_test, etc.)
    /// - ✅ Unit tests: Insert tasks BEFORE creating SchedulerActor
    ///
    /// INCORRECT PATTERNS:
    /// - ❌ Insert tasks directly into registry after SchedulerActor exists
    /// - ❌ Read tasks from registry when SchedulerActor owns the domain
    /// - ❌ Bypass SchedulerActor for task operations in production code
    ///
    /// For task operations, always use:
    /// 1. ClientService::create_task() (production)
    /// 2. SchedulerActor methods (testing/internal)
    /// 3. This registry directly only during engine initialization or unit test setup
    pub registry: Arc<TaskSetRegistry>,

    pub event_stream: Arc<EventStream>,
    pub shepherd_registry: Arc<ShepherdManagerRegistry>,
    pub scheduler_registry: Arc<SchedulerRegistry>,
}

impl Engine {
    /// Create a new orchestration engine with the given database pool and configuration
    pub fn new(
        pool: PgPool,
        event_stream_config: EventStreamConfig,
        domains_config: DomainsConfig,
    ) -> Self {
        Self::with_scheduler_config(
            pool,
            event_stream_config,
            domains_config,
            SchedulerConfig::default(),
        )
    }

    /// Create a new orchestration engine with custom scheduler configuration
    pub fn with_scheduler_config(
        pool: PgPool,
        event_stream_config: EventStreamConfig,
        domains_config: DomainsConfig,
        scheduler_config: SchedulerConfig,
    ) -> Self {
        let event_stream = Arc::new(EventStream::new(pool.clone(), event_stream_config));
        let registry = Arc::new(TaskSetRegistry::new());
        let domains_config_arc = Arc::new(domains_config);
        let shepherd_registry = Arc::new(ShepherdManagerRegistry::new(
            domains_config_arc.clone(),
            registry.clone(),
            event_stream.clone(),
        ));
        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            registry.clone(),
            shepherd_registry.clone(),
            event_stream.clone(),
            scheduler_config,
        ));

        Self {
            pool,
            registry,
            event_stream,
            shepherd_registry,
            scheduler_registry,
        }
    }

    /// Initialize the engine by loading existing data from the database
    pub async fn initialize(&self) -> Result<()> {
        // Load existing tasks from database into the registry
        self.registry.load_from_db(&self.pool).await?;

        // The actor-based shepherd manager is already started (includes dispatcher and health checker)

        log::info!(
            "Engine initialized - TaskSetRegistry loaded with {} domains and virtual queue dispatcher started",
            self.registry.domains().len()
        );
        Ok(())
    }

    /// Shutdown the engine and all its components with default timeout
    pub async fn shutdown(&self) -> Result<()> {
        // Use default 30 second timeout for backward compatibility
        self.shutdown_with_timeout(30).await
    }

    /// Shutdown the engine and all its components with configurable timeout
    pub async fn shutdown_with_timeout(&self, timeout_secs: u64) -> Result<()> {
        log::info!("Shutting down orchestration engine with {timeout_secs}s timeout...");

        let shutdown_future = async {
            // Phase 1: Shutdown schedulers first (with fixed timeout_future cancellation)
            log::info!("Shutting down scheduler registry...");
            if let Err(e) = self.scheduler_registry.shutdown_all().await {
                log::error!("Failed to shutdown scheduler registry: {e}");
            }

            // Phase 2: Shutdown shepherd managers
            log::info!("Shutting down shepherd managers...");
            self.shepherd_registry.shutdown_all().await;

            // Phase 3: Shutdown event stream
            log::info!("Shutting down event stream...");
            if let Err(e) = self.event_stream.shutdown().await {
                log::error!("Failed to shutdown event stream: {e}");
            }

            log::info!("Orchestration engine shutdown complete");
            Ok(())
        };

        // Apply timeout to the entire shutdown sequence
        match tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            shutdown_future,
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                log::error!("Engine shutdown timed out after {timeout_secs}s, some components may not have shut down cleanly");
                Ok(()) // Don't fail the shutdown, just log the timeout
            }
        }
    }

    /// Merge events from the events table to task_instance and task_attempts tables
    /// This method can be called periodically or on-demand to sync the event log
    pub async fn merge_events_to_db(&self) -> Result<()> {
        self.registry.merge_events_to_db(&self.pool).await
    }
}
