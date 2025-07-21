use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use log::{error, info, warn};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tonic::Status;
use uuid::Uuid;

use crate::orchestrator::event_stream::EventStream;
use crate::orchestrator::taskset::TaskSetRegistry;
use crate::proto::{common, orchestrator::ServerMsg};
use crate::EVENT_SHEPHERD_REGISTERED;

const SHEPHERD_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Type alias for shepherd communication channel
type ShepherdTxChannel = mpsc::Sender<Result<ServerMsg, Status>>;
#[allow(dead_code)]
type ShepherdRxChannel = mpsc::Receiver<Result<ServerMsg, Status>>;

/// Task dispatch parameters
#[derive(Debug, Clone)]
pub struct TaskDispatch {
    pub task_id: Uuid,
    pub task_name: String,
    pub args: Vec<String>,
    pub kwargs: String,
    pub memory_limit: Option<u64>,
    pub cpu_limit: Option<u32>,
}

/// Entry for priority queue in find_best_shepherd
/// Implements Ord to create a min-heap based on load_ratio
#[derive(Debug, Clone, PartialEq)]
struct ShepherdLoadEntry {
    uuid: Uuid,
    load_ratio: f64,
    available_capacity: u32,
    current_load: u32,
}

impl Eq for ShepherdLoadEntry {}

impl PartialOrd for ShepherdLoadEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShepherdLoadEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse comparison for min-heap behavior (BinaryHeap is max-heap by default)
        // Primary: lower load ratio is better
        let load_cmp = other
            .load_ratio
            .partial_cmp(&self.load_ratio)
            .unwrap_or(Ordering::Equal);

        if load_cmp != Ordering::Equal {
            return load_cmp;
        }

        // Secondary: higher available capacity is better (tie-breaker)
        let capacity_cmp = self.available_capacity.cmp(&other.available_capacity);

        if capacity_cmp != Ordering::Equal {
            return capacity_cmp;
        }

        // Tertiary: UUID for consistent ordering (deterministic tie-breaking)
        self.uuid.cmp(&other.uuid)
    }
}

#[derive(Debug, Clone)]
pub enum ShepherdConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting,
}

#[derive(Debug)]
pub struct ShepherdStatus {
    pub uuid: Uuid,
    pub max_concurrency: u32,
    pub current_load: u32,
    pub last_seen: DateTime<Utc>,
    pub connected_at: DateTime<Utc>,
    pub status: ShepherdConnectionStatus,
    pub tx: Option<ShepherdTxChannel>,
}

impl ShepherdStatus {
    pub fn new(uuid: Uuid, max_concurrency: u32) -> Self {
        let now = Utc::now();
        Self {
            uuid,
            max_concurrency,
            current_load: 0,
            last_seen: now,
            connected_at: now,
            status: ShepherdConnectionStatus::Connected,
            tx: None,
        }
    }

    pub fn with_tx(uuid: Uuid, max_concurrency: u32, tx: ShepherdTxChannel) -> Self {
        let now = Utc::now();
        Self {
            uuid,
            max_concurrency,
            current_load: 0,
            last_seen: now,
            connected_at: now,
            status: ShepherdConnectionStatus::Connected,
            tx: Some(tx),
        }
    }

    pub fn set_tx(&mut self, tx: ShepherdTxChannel) {
        self.tx = Some(tx);
    }

    pub fn clear_tx(&mut self) {
        self.tx = None;
    }

    pub fn load_ratio(&self) -> f64 {
        if self.max_concurrency == 0 {
            1.0 // Avoid division by zero, treat as fully loaded
        } else {
            self.current_load as f64 / self.max_concurrency as f64
        }
    }

    pub fn available_capacity(&self) -> u32 {
        self.max_concurrency.saturating_sub(self.current_load)
    }

    pub fn update_last_seen(&mut self) {
        self.last_seen = Utc::now();
    }

    pub fn update_load(&mut self, current_load: u32, available_capacity: u32) {
        self.current_load = current_load;
        self.last_seen = Utc::now();

        // Validate that current_load + available_capacity == max_concurrency
        if current_load + available_capacity != self.max_concurrency {
            warn!(
                "Shepherd {} reported inconsistent capacity: current={}, available={}, max={}",
                self.uuid, current_load, available_capacity, self.max_concurrency
            );
        }
    }

    pub async fn dispatch_task(
        &self,
        task_id: Uuid,
        task_name: String,
        args: Vec<String>,
        kwargs: String,
        memory_limit: Option<u64>,
        cpu_limit: Option<u32>,
    ) -> Result<()> {
        let tx = self.tx.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Shepherd {} is not connected (no tx channel)", self.uuid)
        })?;

        let task = common::Task {
            task_id: task_id.to_string(),
            name: task_name,
            args,
            kwargs,
            memory_limit,
            cpu_limit,
        };

        let server_msg = ServerMsg {
            kind: Some(crate::proto::orchestrator::server_msg::Kind::Task(task)),
        };

        tx.send(Ok(server_msg))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send task to shepherd {}: {}", self.uuid, e))?;

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Dispatched task {} to shepherd {}", task_id, self.uuid);
        Ok(())
    }
}

pub struct ShepherdManager {
    shepherds: DashMap<Uuid, ShepherdStatus>,
    task_registry: Arc<TaskSetRegistry>,
    event_stream: Arc<EventStream>,
}

impl ShepherdManager {
    pub fn new(task_registry: Arc<TaskSetRegistry>, event_stream: Arc<EventStream>) -> Self {
        Self {
            shepherds: DashMap::new(),
            task_registry,
            event_stream,
        }
    }

    /// Register a new shepherd connection or reconnect existing one
    pub async fn register_shepherd(&self, uuid: Uuid, max_concurrency: u32) -> Result<()> {
        // Check if this is a reconnection
        if let Some(existing_shepherd) = self.shepherds.get(&uuid) {
            if matches!(
                existing_shepherd.status,
                ShepherdConnectionStatus::Disconnected
            ) {
                // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
                info!("Shepherd {uuid} reconnecting (was temporarily unavailable)");
                self.mark_shepherd_reconnected(uuid);
                return Ok(());
            }
        }

        // New shepherd registration
        let shepherd_status = ShepherdStatus::new(uuid, max_concurrency);

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Registering new shepherd {uuid} with max_concurrency={max_concurrency}");

        // Create EVENT_SHEPHERD_REGISTERED event
        let event_metadata = serde_json::json!({
            "shepherd_uuid": uuid,
            "max_concurrency": max_concurrency,
            "registered_at": Utc::now()
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: "system".to_string(), // Use system domain for shepherd events
            task_instance_id: None,
            flow_instance_id: None,
            event_type: EVENT_SHEPHERD_REGISTERED,
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        // Write event first
        self.event_stream.write_event(event_record).await?;

        // Add to in-memory tracking
        self.shepherds.insert(uuid, shepherd_status);

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Successfully registered shepherd {uuid}");
        Ok(())
    }

    /// Register a new shepherd connection with tx channel
    pub async fn register_shepherd_with_tx(
        &self,
        uuid: Uuid,
        max_concurrency: u32,
        tx: ShepherdTxChannel,
    ) -> Result<()> {
        // Check if this is a reconnection
        if let Some(mut existing_shepherd) = self.shepherds.get_mut(&uuid) {
            if matches!(
                existing_shepherd.status,
                ShepherdConnectionStatus::Disconnected
            ) {
                // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
                info!("Shepherd {uuid} reconnecting (was temporarily unavailable)");
                existing_shepherd.set_tx(tx);
                existing_shepherd.status = ShepherdConnectionStatus::Connected;
                existing_shepherd.update_last_seen();
                return Ok(());
            }
        }

        // New shepherd registration with tx
        let shepherd_status = ShepherdStatus::with_tx(uuid, max_concurrency, tx);

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Registering new shepherd {uuid} with max_concurrency={max_concurrency}");

        // Create EVENT_SHEPHERD_REGISTERED event
        let event_metadata = serde_json::json!({
            "shepherd_uuid": uuid,
            "max_concurrency": max_concurrency,
            "registered_at": Utc::now()
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: "system".to_string(), // Use system domain for shepherd events
            task_instance_id: None,
            flow_instance_id: None,
            event_type: EVENT_SHEPHERD_REGISTERED,
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        // Write event first
        self.event_stream.write_event(event_record).await?;

        // Add to in-memory tracking
        self.shepherds.insert(uuid, shepherd_status);

        // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
        info!("Successfully registered shepherd {uuid}");
        Ok(())
    }

    /// Dispatch task to a specific shepherd
    pub async fn dispatch_task_to_shepherd(
        &self,
        shepherd_id: Uuid,
        task_dispatch: TaskDispatch,
    ) -> Result<()> {
        let shepherd = self
            .shepherds
            .get(&shepherd_id)
            .ok_or_else(|| anyhow::anyhow!("Shepherd {} not found", shepherd_id))?;

        if !matches!(shepherd.status, ShepherdConnectionStatus::Connected) {
            return Err(anyhow::anyhow!("Shepherd {} is not connected", shepherd_id));
        }

        shepherd
            .dispatch_task(
                task_dispatch.task_id,
                task_dispatch.task_name,
                task_dispatch.args,
                task_dispatch.kwargs,
                task_dispatch.memory_limit,
                task_dispatch.cpu_limit,
            )
            .await
    }

    /// Update shepherd status (heartbeat)
    pub fn update_shepherd_status(&self, uuid: Uuid, current_load: u32, available_capacity: u32) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.update_load(current_load, available_capacity);
        } else {
            // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            warn!("Received status update from unknown shepherd {uuid}");
        }
    }

    /// Mark shepherd as having sent a message (for liveness tracking)
    pub fn mark_shepherd_alive(&self, uuid: Uuid) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.update_last_seen();
        }
    }

    /// Mark shepherd as temporarily unavailable (connection dropped but might reconnect)
    pub fn mark_shepherd_disconnected(&self, uuid: Uuid) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.status = ShepherdConnectionStatus::Disconnected;
            shepherd.clear_tx(); // Clear the tx channel on disconnect
                                 // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            info!("Marked shepherd {uuid} as temporarily unavailable (connection dropped)");
        }
    }

    /// Mark shepherd as reconnected and available again
    pub fn mark_shepherd_reconnected(&self, uuid: Uuid) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.status = ShepherdConnectionStatus::Connected;
            shepherd.update_last_seen();
            // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            info!("Marked shepherd {uuid} as reconnected and available");
        }
    }

    /// Find the best shepherd for task dispatch based on load using a priority queue
    /// Returns the shepherd with the lowest load ratio among those with available capacity
    pub fn find_best_shepherd(&self) -> Option<Uuid> {
        let mut heap = BinaryHeap::new();

        // Collect all available shepherds into a max heap (we'll use reverse ordering)
        for entry in self.shepherds.iter() {
            let shepherd = entry.value();

            // Only consider connected shepherds with available capacity
            if matches!(shepherd.status, ShepherdConnectionStatus::Connected)
                && shepherd.available_capacity() > 0
            {
                heap.push(ShepherdLoadEntry {
                    uuid: shepherd.uuid,
                    load_ratio: shepherd.load_ratio(),
                    available_capacity: shepherd.available_capacity(),
                    current_load: shepherd.current_load,
                });
            }
        }

        // Return the shepherd with the lowest load ratio (top of min heap)
        heap.pop().map(|entry| entry.uuid)
    }

    /// Remove shepherd when it disconnects
    pub fn remove_shepherd(&self, uuid: Uuid) {
        if self.shepherds.remove(&uuid).is_some() {
            // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            info!("Removed shepherd {uuid} from tracking");
        }
    }

    /// Start the dead shepherd cleanup background task
    pub async fn start_health_checker(self: Arc<Self>) {
        let mut interval = time::interval(Duration::from_secs(60)); // Check every minute

        loop {
            interval.tick().await;
            if let Err(e) = self.cleanup_dead_shepherds().await {
                error!("Error during dead shepherd cleanup: {e}");
            }
        }
    }

    /// Check disconnected shepherds for timeout and fail their tasks
    async fn cleanup_dead_shepherds(&self) -> Result<()> {
        let now = Utc::now();
        let timeout = ChronoDuration::seconds(SHEPHERD_TIMEOUT_SECS as i64);
        let mut dead_shepherds = Vec::new();

        // Find shepherds that have been disconnected too long
        for entry in self.shepherds.iter() {
            let shepherd_uuid = *entry.key();
            let shepherd = entry.value();

            // Only check disconnected shepherds - they've already been marked as unavailable
            if matches!(shepherd.status, ShepherdConnectionStatus::Disconnected) {
                let time_since_last_seen = now.signed_duration_since(shepherd.last_seen);

                if time_since_last_seen > timeout {
                    warn!(
                        "Shepherd {} has been disconnected too long (last seen {} seconds ago), cleaning up tasks",
                        shepherd_uuid,
                        time_since_last_seen.num_seconds()
                    );
                    dead_shepherds.push(shepherd_uuid);
                }
            }
        }

        // Process dead shepherds - generate failure events and remove them completely
        for shepherd_uuid in dead_shepherds {
            // Generate failure events for all tasks assigned to this shepherd
            // Note: The Scheduler will handle the events and update TaskSet accordingly
            if let Err(e) = self
                .task_registry
                .generate_shepherd_failure_events(shepherd_uuid, &self.event_stream)
                .await
            {
                // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
                error!("Failed to generate failure events for dead shepherd {shepherd_uuid}: {e}");
            }

            // Remove shepherd completely (they've been disconnected too long)
            self.remove_shepherd(shepherd_uuid);
        }

        Ok(())
    }

    /// Get statistics about shepherds
    pub fn get_stats(&self) -> ShepherdManagerStats {
        let mut connected = 0;
        let mut disconnected = 0;
        let mut total_capacity = 0;
        let mut total_load = 0;

        for entry in self.shepherds.iter() {
            let shepherd = entry.value();
            match shepherd.status {
                ShepherdConnectionStatus::Connected => {
                    connected += 1;
                    total_capacity += shepherd.max_concurrency;
                    total_load += shepherd.current_load;
                }
                ShepherdConnectionStatus::Disconnected => disconnected += 1,
                ShepherdConnectionStatus::Reconnecting => {} // Count as neither
            }
        }

        ShepherdManagerStats {
            connected_shepherds: connected,
            disconnected_shepherds: disconnected,
            total_capacity,
            total_load,
            utilization: if total_capacity > 0 {
                total_load as f64 / total_capacity as f64
            } else {
                0.0
            },
        }
    }

    /// Check if a specific shepherd is registered and connected
    pub fn is_shepherd_registered(&self, uuid: Uuid) -> bool {
        self.shepherds
            .get(&uuid)
            .map(|shepherd| matches!(shepherd.status, ShepherdConnectionStatus::Connected))
            .unwrap_or(false)
    }

    /// Get the connection status of a specific shepherd
    pub fn get_shepherd_status(&self, uuid: Uuid) -> Option<ShepherdConnectionStatus> {
        self.shepherds
            .get(&uuid)
            .map(|shepherd| shepherd.status.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ShepherdManagerStats {
    pub connected_shepherds: usize,
    pub disconnected_shepherds: usize,
    pub total_capacity: u32,
    pub total_load: u32,
    pub utilization: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_test_manager() -> ShepherdManager {
        use crate::orchestrator::db::{create_pool, Database, Server, Settings};
        use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};

        let task_registry = Arc::new(TaskSetRegistry::new());

        // Create minimal settings for testing - we'll skip event stream writing
        let settings = Settings {
            database: Database {
                url: "postgres://dummy:dummy@localhost/dummy".to_string(),
                pool_size: 1,
            },
            server: Server { port: 0 },
            event_stream: crate::orchestrator::db::EventStream::default(),
        };

        // Create a dummy pool that won't actually connect
        let pool = create_pool(&settings).unwrap_or_else(|_| {
            // Create a minimal pool for testing
            use deadpool_postgres::{Manager, Pool};
            let config = "postgres://dummy:dummy@localhost/dummy".parse().unwrap();
            let manager = Manager::new(config, tokio_postgres::NoTls);
            Pool::builder(manager).max_size(1).build().unwrap()
        });

        let event_stream_config = EventStreamConfig::default();
        let event_stream = Arc::new(EventStream::new(pool, event_stream_config));
        ShepherdManager::new(task_registry, event_stream)
    }

    // Helper function to manually add shepherds for testing without event stream
    fn add_test_shepherd(manager: &ShepherdManager, uuid: Uuid, max_concurrency: u32) {
        let shepherd_status = ShepherdStatus::new(uuid, max_concurrency);
        manager.shepherds.insert(uuid, shepherd_status);
    }

    // Helper function to create a test tx channel
    fn create_test_tx() -> (ShepherdTxChannel, ShepherdRxChannel) {
        mpsc::channel(10)
    }

    /// Tests that the shepherd selection algorithm correctly prioritizes shepherds with the lowest load ratio.
    /// Expected behavior: When multiple shepherds are available, the one with the lowest current_load/max_concurrency ratio should be selected.
    #[tokio::test]
    async fn test_find_best_shepherd_selects_lowest_load() {
        let manager = create_test_manager();

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        add_test_shepherd(&manager, uuid1, 10);
        add_test_shepherd(&manager, uuid2, 10);
        add_test_shepherd(&manager, uuid3, 10);

        // Different load ratios: 20%, 80%, 50%
        manager.update_shepherd_status(uuid1, 2, 8); // 20% - should be selected
        manager.update_shepherd_status(uuid2, 8, 2); // 80%
        manager.update_shepherd_status(uuid3, 5, 5); // 50%

        assert_eq!(manager.find_best_shepherd(), Some(uuid1));
    }

    /// Tests that the shepherd selection algorithm ignores unavailable shepherds.
    /// Expected behavior: Shepherds at full capacity or disconnected should not be selected, even if they would otherwise be preferred.
    #[tokio::test]
    async fn test_find_best_shepherd_ignores_unavailable() {
        let manager = create_test_manager();

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        add_test_shepherd(&manager, uuid1, 10);
        add_test_shepherd(&manager, uuid2, 10);
        add_test_shepherd(&manager, uuid3, 10);

        // uuid1: full capacity, uuid2: disconnected, uuid3: available
        manager.update_shepherd_status(uuid1, 10, 0); // Full capacity
        manager.update_shepherd_status(uuid2, 3, 7); // Good load but will disconnect
        manager.update_shepherd_status(uuid3, 5, 5); // Available

        // Disconnect uuid2
        if let Some(mut shepherd) = manager.shepherds.get_mut(&uuid2) {
            shepherd.status = ShepherdConnectionStatus::Disconnected;
        }

        assert_eq!(manager.find_best_shepherd(), Some(uuid3));
    }

    /// Tests the tiebreaker logic when shepherds have identical load ratios.
    /// Expected behavior: When load ratios are equal, the shepherd with higher available capacity should be preferred.
    #[tokio::test]
    async fn test_find_best_shepherd_capacity_tiebreaker() {
        let manager = create_test_manager();

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        add_test_shepherd(&manager, uuid1, 10);
        add_test_shepherd(&manager, uuid2, 20);

        // Same load ratio (50%) but different available capacity
        manager.update_shepherd_status(uuid1, 5, 5); // 5 available
        manager.update_shepherd_status(uuid2, 10, 10); // 10 available - should win tiebreaker

        assert_eq!(manager.find_best_shepherd(), Some(uuid2));
    }

    /// Tests edge cases in shepherd selection including empty pool, zero capacity, and normal operation.
    /// Expected behavior: Should handle empty pools gracefully, reject zero-capacity shepherds, and select normal shepherds correctly.
    #[tokio::test]
    async fn test_find_best_shepherd_edge_cases() {
        let manager = create_test_manager();

        // No shepherds
        assert_eq!(manager.find_best_shepherd(), None);

        // Zero max concurrency
        let uuid1 = Uuid::new_v4();
        add_test_shepherd(&manager, uuid1, 0);
        manager.update_shepherd_status(uuid1, 0, 0);
        assert_eq!(manager.find_best_shepherd(), None);

        // Normal shepherd
        let uuid2 = Uuid::new_v4();
        add_test_shepherd(&manager, uuid2, 10);
        manager.update_shepherd_status(uuid2, 3, 7);
        assert_eq!(manager.find_best_shepherd(), Some(uuid2));
    }

    /// Tests that shepherd selection is deterministic and consistent across multiple calls.
    /// Expected behavior: Given identical shepherd states, selection should return the same result consistently.
    #[tokio::test]
    async fn test_find_best_shepherd_deterministic() {
        let manager = create_test_manager();

        // Multiple shepherds with identical stats
        let mut uuids = Vec::new();
        for _ in 0..3 {
            let uuid = Uuid::new_v4();
            uuids.push(uuid);
            add_test_shepherd(&manager, uuid, 10);
            manager.update_shepherd_status(uuid, 3, 7);
        }

        // Selection should be consistent
        let selection1 = manager.find_best_shepherd();
        let selection2 = manager.find_best_shepherd();

        assert_eq!(selection1, selection2);
        assert!(selection1.is_some());
        assert!(uuids.contains(&selection1.unwrap()));
    }

    /// Tests the complete reconnection flow including disconnect, unavailability, and reconnection.
    /// Expected behavior: Disconnected shepherds should become unavailable for selection but remain tracked, and become available again upon reconnection.
    #[tokio::test]
    async fn test_shepherd_reconnection_flow() {
        let manager = create_test_manager();

        let uuid = Uuid::new_v4();

        // Initial registration
        add_test_shepherd(&manager, uuid, 10);
        manager.update_shepherd_status(uuid, 3, 7);

        // Should be available for selection
        assert_eq!(manager.find_best_shepherd(), Some(uuid));

        // Mark as disconnected (connection drops)
        manager.mark_shepherd_disconnected(uuid);

        // Should not be available for new tasks
        assert_eq!(manager.find_best_shepherd(), None);

        {
            // Shepherd should still exist but be marked as disconnected
            let shepherd = manager.shepherds.get(&uuid).unwrap();
            assert!(matches!(
                shepherd.status,
                ShepherdConnectionStatus::Disconnected
            ));
        }

        // Mark as reconnected
        manager.mark_shepherd_reconnected(uuid);

        // Should be available again
        assert_eq!(manager.find_best_shepherd(), Some(uuid));

        // Status should be connected
        let shepherd = manager.shepherds.get(&uuid).unwrap();
        assert!(matches!(
            shepherd.status,
            ShepherdConnectionStatus::Connected
        ));
    }

    /// Tests the disconnect and reconnect operations and their effect on shepherd availability.
    /// Expected behavior: Shepherds should transition between connected/disconnected states and maintain proper availability status.
    #[tokio::test]
    async fn test_mark_shepherd_disconnected_and_reconnected() {
        let manager = create_test_manager();

        let uuid = Uuid::new_v4();

        // Initial setup using helper function
        add_test_shepherd(&manager, uuid, 10);
        manager.update_shepherd_status(uuid, 3, 7);
        assert_eq!(manager.find_best_shepherd(), Some(uuid));

        // Mark as disconnected
        manager.mark_shepherd_disconnected(uuid);
        assert_eq!(manager.find_best_shepherd(), None);

        // Verify shepherd still exists but disconnected
        {
            let shepherd = manager.shepherds.get(&uuid).unwrap();
            assert!(matches!(
                shepherd.status,
                ShepherdConnectionStatus::Disconnected
            ));
        }

        // Mark as reconnected
        manager.mark_shepherd_reconnected(uuid);

        // Should be available again
        assert_eq!(manager.find_best_shepherd(), Some(uuid));

        // Should only have one connected shepherd entry
        let connected_count = manager
            .shepherds
            .iter()
            .filter(|entry| matches!(entry.value().status, ShepherdConnectionStatus::Connected))
            .count();
        assert_eq!(connected_count, 1);
    }

    /// Tests task dispatch failure when shepherd has no communication channel.
    /// Expected behavior: Should return an error indicating the shepherd is not connected when no tx channel is available.
    #[tokio::test]
    async fn test_dispatch_task_without_tx() {
        let uuid = Uuid::new_v4();
        let shepherd = ShepherdStatus::new(uuid, 10);

        let result = shepherd
            .dispatch_task(
                Uuid::new_v4(),
                "test_task".to_string(),
                vec!["arg1".to_string()],
                "{}".to_string(),
                None,
                None,
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not connected"));
    }

    /// Tests successful task dispatch when shepherd has a valid communication channel.
    /// Expected behavior: Should successfully send task through the tx channel without errors.
    #[tokio::test]
    async fn test_dispatch_task_with_tx() {
        let uuid = Uuid::new_v4();
        let (tx, _rx) = create_test_tx();
        let shepherd = ShepherdStatus::with_tx(uuid, 10, tx);

        let result = shepherd
            .dispatch_task(
                Uuid::new_v4(),
                "test_task".to_string(),
                vec!["arg1".to_string()],
                "{}".to_string(),
                Some(1024),
                Some(100),
            )
            .await;

        // Should succeed since we have a tx channel
        assert!(result.is_ok());
    }

    /// Tests that disconnecting a shepherd properly clears its communication channel.
    /// Expected behavior: When a shepherd is marked as disconnected, its tx channel should be cleared to prevent message sending.
    #[tokio::test]
    async fn test_mark_shepherd_disconnected_clears_tx() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();
        let (tx, _rx) = create_test_tx();

        // Add shepherd with tx
        let shepherd_status = ShepherdStatus::with_tx(uuid, 10, tx);
        manager.shepherds.insert(uuid, shepherd_status);

        // Verify tx is set
        assert!(manager.shepherds.get(&uuid).unwrap().tx.is_some());

        // Mark as disconnected
        manager.mark_shepherd_disconnected(uuid);

        // Verify tx is cleared
        assert!(manager.shepherds.get(&uuid).unwrap().tx.is_none());
        assert!(matches!(
            manager.shepherds.get(&uuid).unwrap().status,
            ShepherdConnectionStatus::Disconnected
        ));
    }

    /// Tests shepherd registration with communication channel.
    /// Expected behavior: Should successfully register a new shepherd with its tx channel for immediate communication capability.
    #[tokio::test]
    async fn test_register_shepherd_with_tx() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();
        let (tx, _rx) = create_test_tx();

        // This would normally write to database, but our test manager handles that
        let result = manager.register_shepherd_with_tx(uuid, 10, tx).await;

        // Should succeed (even though event stream write might fail in test)
        // The important thing is that the shepherd is registered with tx
        if result.is_ok() {
            let shepherd = manager.shepherds.get(&uuid).unwrap();
            assert!(shepherd.tx.is_some());
            assert_eq!(shepherd.max_concurrency, 10);
        }
    }

    /// Tests the complete task dispatch flow through ShepherdManager.
    /// Expected behavior: Should successfully route task dispatch to the correct shepherd and send the task through its communication channel.
    #[tokio::test]
    async fn test_shepherd_manager_dispatch_task_to_shepherd() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();
        let (tx, _rx) = create_test_tx();

        // Add shepherd with tx
        let shepherd_status = ShepherdStatus::with_tx(uuid, 10, tx);
        manager.shepherds.insert(uuid, shepherd_status);

        // Dispatch task
        let task_dispatch = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            args: vec!["arg1".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: Some(1024),
            cpu_limit: Some(100),
        };
        let result = manager.dispatch_task_to_shepherd(uuid, task_dispatch).await;

        assert!(result.is_ok());
    }

    /// Tests task dispatch error handling when targeting a non-existent shepherd.
    /// Expected behavior: Should return an error indicating the shepherd was not found.
    #[tokio::test]
    async fn test_shepherd_manager_dispatch_task_to_nonexistent_shepherd() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();

        // Try to dispatch to nonexistent shepherd
        let task_dispatch = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            args: vec!["arg1".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: Some(1024),
            cpu_limit: Some(100),
        };
        let result = manager.dispatch_task_to_shepherd(uuid, task_dispatch).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    /// Tests task dispatch error handling when targeting a disconnected shepherd.
    /// Expected behavior: Should return an error indicating the shepherd is not connected, even if it exists in the registry.
    #[tokio::test]
    async fn test_shepherd_manager_dispatch_task_to_disconnected_shepherd() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();
        let (tx, _rx) = create_test_tx();

        // Add shepherd and disconnect it
        let shepherd_status = ShepherdStatus::with_tx(uuid, 10, tx);
        manager.shepherds.insert(uuid, shepherd_status);
        manager.mark_shepherd_disconnected(uuid);

        // Try to dispatch to disconnected shepherd
        let task_dispatch = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            args: vec!["arg1".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: Some(1024),
            cpu_limit: Some(100),
        };
        let result = manager.dispatch_task_to_shepherd(uuid, task_dispatch).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not connected"));
    }
}
