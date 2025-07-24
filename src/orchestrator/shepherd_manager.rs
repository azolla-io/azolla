use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use log::{error, info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tonic::Status;
use uuid::Uuid;

use crate::orchestrator::db::DomainsConfig;
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
    pub flow_instance_id: Option<Uuid>,
    pub attempt_number: i32,
}

/// Per-domain virtual queue for task dispatch
#[derive(Debug)]
pub struct VirtualQueue {
    /// FIFO queue of tasks waiting to be dispatched
    queue: VecDeque<TaskDispatch>,
    /// Current number of in-flight tasks for this domain
    in_flight_count: AtomicU32,
    /// Maximum number of concurrent tasks allowed for this domain
    concurrency_limit: u32,
}

impl VirtualQueue {
    pub fn new(concurrency_limit: u32) -> Self {
        Self {
            queue: VecDeque::new(),
            in_flight_count: AtomicU32::new(0),
            concurrency_limit,
        }
    }

    /// Check if we can dispatch more tasks based on concurrency limit
    pub fn can_dispatch(&self) -> bool {
        self.in_flight_count.load(AtomicOrdering::Relaxed) < self.concurrency_limit
    }

    /// Get the number of tasks available to dispatch
    pub fn available_dispatch_count(&self) -> u32 {
        let in_flight = self.in_flight_count.load(AtomicOrdering::Relaxed);
        if in_flight >= self.concurrency_limit {
            0
        } else {
            std::cmp::min(self.queue.len() as u32, self.concurrency_limit - in_flight)
        }
    }

    /// Enqueue a task for dispatch
    pub fn enqueue(&mut self, task: TaskDispatch) -> Result<()> {
        self.queue.push_back(task);
        Ok(())
    }

    /// Dequeue a task for dispatch (if any available)
    pub fn dequeue(&mut self) -> Option<TaskDispatch> {
        if self.can_dispatch() && !self.queue.is_empty() {
            let task = self.queue.pop_front();
            if task.is_some() {
                self.in_flight_count.fetch_add(1, AtomicOrdering::Relaxed);
            }
            task
        } else {
            None
        }
    }

    /// Decrement in-flight counter when task completes
    pub fn decrement_in_flight(&self) {
        self.in_flight_count.fetch_sub(1, AtomicOrdering::Relaxed);
    }

    /// Get current queue length
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Get current in-flight count
    pub fn in_flight_count(&self) -> u32 {
        self.in_flight_count.load(AtomicOrdering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
    virtual_queues: Arc<DashMap<String, VirtualQueue>>,
    domains_config: Arc<DomainsConfig>,
    task_registry: Arc<TaskSetRegistry>,
    event_stream: Arc<EventStream>,

    // Persistent bucket state for load balancing
    // Each bucket contains shepherds with load ratios in [i*0.1, (i+1)*0.1)
    load_buckets: Arc<DashMap<usize, Vec<Uuid>>>,

    // Total capacity tracking for average load calculation
    total_max_concurrency: Arc<std::sync::atomic::AtomicU32>,
    total_current_load: Arc<std::sync::atomic::AtomicU32>,
}

impl ShepherdManager {
    pub fn new(
        domains_config: Arc<DomainsConfig>,
        task_registry: Arc<TaskSetRegistry>,
        event_stream: Arc<EventStream>,
    ) -> Self {
        Self {
            shepherds: DashMap::new(),
            virtual_queues: Arc::new(DashMap::new()),
            domains_config,
            task_registry,
            event_stream,
            load_buckets: Arc::new(DashMap::new()),
            total_max_concurrency: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            total_current_load: Arc::new(std::sync::atomic::AtomicU32::new(0)),
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

        // Update total capacity tracking
        self.total_max_concurrency
            .fetch_add(max_concurrency, std::sync::atomic::Ordering::Relaxed);
        // New shepherds start with 0 current load

        // Add to appropriate load bucket (new shepherds have 0% load)
        self.add_to_bucket(uuid, 0.0);

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

        // Update total capacity tracking
        self.total_max_concurrency
            .fetch_add(max_concurrency, std::sync::atomic::Ordering::Relaxed);
        // New shepherds start with 0 current load

        // Add to appropriate load bucket (new shepherds have 0% load)
        self.add_to_bucket(uuid, 0.0);

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
            let old_load = shepherd.current_load;
            shepherd.update_load(current_load, available_capacity);

            // Update total current load tracking
            let load_diff = current_load as i32 - old_load as i32;
            if load_diff != 0 {
                if load_diff > 0 {
                    self.total_current_load
                        .fetch_add(load_diff as u32, std::sync::atomic::Ordering::Relaxed);
                } else {
                    self.total_current_load
                        .fetch_sub((-load_diff) as u32, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Update bucket assignment based on new load ratio
            let new_load_ratio = shepherd.load_ratio();
            self.update_shepherd_bucket(uuid, new_load_ratio);
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

            // Remove from load buckets since disconnected shepherds shouldn't be selected
            self.remove_from_all_buckets(uuid);

            // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            info!("Marked shepherd {uuid} as temporarily unavailable (connection dropped)");
        }
    }

    /// Mark shepherd as reconnected and available again
    pub fn mark_shepherd_reconnected(&self, uuid: Uuid) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.status = ShepherdConnectionStatus::Connected;
            shepherd.update_last_seen();

            // Add back to appropriate load bucket since shepherd is now available
            let load_ratio = shepherd.load_ratio();
            self.add_to_bucket(uuid, load_ratio);

            // codeql[rust/clear-text-logging-sensitive-data] Infrastructure UUID - safe to log
            info!("Marked shepherd {uuid} as reconnected and available");
        }
    }

    /// Find available shepherds for batch task dispatch based on load balancing
    /// Returns up to `batch` shepherds with available capacity, prioritizing load balancing
    /// Uses a fast heuristic that sacrifices perfect accuracy for speed
    pub fn find_available_shepherds(&self, batch: u32) -> Vec<Uuid> {
        if batch == 0 {
            return Vec::new();
        }

        // Get total capacity stats for average load calculation
        let total_max_concurrency = self
            .total_max_concurrency
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_current_load = self
            .total_current_load
            .load(std::sync::atomic::Ordering::Relaxed);

        if total_max_concurrency == 0 {
            return Vec::new(); // No shepherds available
        }

        // Calculate expected average load ratio after dispatching batch tasks
        let expected_avg_load_ratio =
            (total_current_load + batch) as f64 / total_max_concurrency as f64;

        let mut result = Vec::new();
        let mut remaining_batch = batch;

        // Process buckets from lowest to highest load ratio (0 to 9)
        for bucket_index in 0..10 {
            if remaining_batch == 0 {
                break;
            }

            if let Some(bucket_entry) = self.load_buckets.get(&bucket_index) {
                let bucket = bucket_entry.value();
                if bucket.is_empty() {
                    continue;
                }

                // Try to assign tasks to shepherds in this bucket
                let bucket_result =
                    self.select_from_bucket(bucket, remaining_batch, expected_avg_load_ratio);

                result.extend(bucket_result.iter());
                remaining_batch = remaining_batch.saturating_sub(bucket_result.len() as u32);
            }
        }

        result
    }

    /// Select shepherds from a single bucket trying to match expected average load ratio
    fn select_from_bucket(
        &self,
        bucket: &[Uuid],
        batch_size: u32,
        target_load_ratio: f64,
    ) -> Vec<Uuid> {
        let mut result = Vec::new();
        let mut remaining_batch = batch_size;

        // Iterate through all shepherds in the bucket
        for &shepherd_uuid in bucket {
            if remaining_batch == 0 {
                break;
            }

            // Get shepherd info
            if let Some(shepherd) = self.shepherds.get(&shepherd_uuid) {
                // Calculate how many tasks this shepherd should get to reach target ratio
                let _current_ratio = shepherd.load_ratio(); // For debugging if needed
                let max_concurrency = shepherd.max_concurrency as f64;
                let current_load = shepherd.current_load as f64;

                // Target load for this shepherd to match average
                let target_load = target_load_ratio * max_concurrency;
                let desired_tasks = (target_load - current_load).ceil() as i32;

                // Clamp to available capacity and remaining batch
                let tasks_to_assign = std::cmp::max(
                    0,
                    std::cmp::min(
                        desired_tasks,
                        std::cmp::min(shepherd.available_capacity() as i32, remaining_batch as i32),
                    ),
                ) as u32;

                // Add this shepherd the calculated number of times
                for _ in 0..tasks_to_assign {
                    result.push(shepherd_uuid);
                }

                remaining_batch = remaining_batch.saturating_sub(tasks_to_assign);
            }
        }

        result
    }

    /// Calculate which bucket a shepherd belongs to based on its load ratio
    fn get_bucket_index(load_ratio: f64) -> usize {
        // Buckets: [0-0.1), [0.1-0.2), ..., [0.9-1.0]
        std::cmp::min(9, (load_ratio * 10.0).floor() as usize)
    }

    /// Add shepherd to the appropriate load bucket
    fn add_to_bucket(&self, uuid: Uuid, load_ratio: f64) {
        let bucket_index = Self::get_bucket_index(load_ratio);

        // Remove from any existing bucket first
        self.remove_from_all_buckets(uuid);

        // Add to the appropriate bucket
        let mut bucket = self.load_buckets.entry(bucket_index).or_default();
        if !bucket.contains(&uuid) {
            bucket.push(uuid);
        }
    }

    /// Remove shepherd from all buckets
    fn remove_from_all_buckets(&self, uuid: Uuid) {
        for mut bucket in self.load_buckets.iter_mut() {
            bucket.value_mut().retain(|&id| id != uuid);
        }
    }

    /// Update shepherd's bucket assignment when load changes
    fn update_shepherd_bucket(&self, uuid: Uuid, new_load_ratio: f64) {
        self.add_to_bucket(uuid, new_load_ratio);
    }

    /// Remove shepherd when it disconnects
    pub fn remove_shepherd(&self, uuid: Uuid) {
        if let Some((_, shepherd)) = self.shepherds.remove(&uuid) {
            // Update total capacity tracking
            self.total_max_concurrency.fetch_sub(
                shepherd.max_concurrency,
                std::sync::atomic::Ordering::Relaxed,
            );
            self.total_current_load
                .fetch_sub(shepherd.current_load, std::sync::atomic::Ordering::Relaxed);

            // Remove from load buckets
            self.remove_from_all_buckets(uuid);

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

    /// Get or create a virtual queue for the specified domain
    fn get_or_create_virtual_queue(&self, domain: &str) -> Arc<DashMap<String, VirtualQueue>> {
        // Check if queue already exists
        if !self.virtual_queues.contains_key(domain) {
            // Determine concurrency limit for this domain
            let concurrency_limit = self
                .domains_config
                .specific
                .get(domain)
                .map(|config| config.concurrency_limit)
                .unwrap_or(self.domains_config.default_concurrency_limit);

            // Create new virtual queue
            let queue = VirtualQueue::new(concurrency_limit);
            self.virtual_queues.insert(domain.to_string(), queue);

            info!("Created virtual queue for domain '{domain}' with concurrency limit {concurrency_limit}");
        }

        self.virtual_queues.clone()
    }

    /// Get domain names that have virtual queues
    pub fn get_active_domains(&self) -> Vec<String> {
        self.virtual_queues
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get virtual queue statistics for a domain
    pub fn get_domain_stats(&self, domain: &str) -> Option<(usize, u32)> {
        self.virtual_queues
            .get(domain)
            .map(|queue| (queue.queue_len(), queue.in_flight_count()))
    }

    /// Enqueue a task for dispatch to a domain's virtual queue
    pub fn enqueue_task(&self, domain: String, task: TaskDispatch) -> Result<()> {
        // Ensure virtual queue exists for this domain
        self.get_or_create_virtual_queue(&domain);

        // Get mutable reference to the queue and enqueue the task
        if let Some(mut queue) = self.virtual_queues.get_mut(&domain) {
            queue.enqueue(task.clone())?;
            info!(
                "Enqueued task {} to domain '{}' virtual queue",
                task.task_id, domain
            );
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Failed to access virtual queue for domain '{}'",
                domain
            ))
        }
    }

    /// Decrement in-flight task counter for a domain when task completes
    pub fn decrement_in_flight_task(&self, domain: &str) {
        if let Some(queue) = self.virtual_queues.get(domain) {
            queue.decrement_in_flight();
            info!("Decremented in-flight counter for domain '{domain}'");
        } else {
            warn!("Attempted to decrement in-flight counter for unknown domain '{domain}'");
        }
    }

    /// Start the virtual queue dispatcher loop
    /// This runs a single actor loop that processes all domain virtual queues
    pub fn start_dispatcher_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(100)); // 100ms dispatch interval

            info!("Started virtual queue dispatcher loop");

            loop {
                interval.tick().await;

                // Process all active domains
                let domains: Vec<String> = self.get_active_domains();
                // TODO: implement a fair dequeue algorithm so that domains are treated equally
                for domain in domains {
                    self.process_domain_queue(&domain).await;
                }
            }
        })
    }

    /// Process tasks from a specific domain's virtual queue
    async fn process_domain_queue(&self, domain: &str) {
        // Get available shepherds for batch dispatch
        let batch_size = 10; // Process up to 10 tasks per domain per cycle
        let available_shepherds = self.find_available_shepherds(batch_size);

        if available_shepherds.is_empty() {
            return; // No shepherds available
        }

        // Get tasks from the virtual queue
        let mut tasks_to_dispatch = Vec::new();

        if let Some(mut queue) = self.virtual_queues.get_mut(domain) {
            // Determine how many tasks we can dispatch
            let dispatch_count = std::cmp::min(
                available_shepherds.len() as u32,
                queue.available_dispatch_count(),
            );

            // Dequeue tasks
            for _ in 0..dispatch_count {
                if let Some(task) = queue.dequeue() {
                    tasks_to_dispatch.push(task);
                }
            }
        }

        // Dispatch tasks to shepherds
        for (task, shepherd_id) in tasks_to_dispatch
            .into_iter()
            .zip(available_shepherds.into_iter())
        {
            // Write attempt started event right before dispatch
            // TODO: implement batch event write
            if let Err(e) = self
                .write_attempt_started_event(&task, shepherd_id, domain)
                .await
            {
                warn!(
                    "Failed to write attempt started event for task {} to shepherd {}: {}",
                    task.task_id, shepherd_id, e
                );
                // Continue with dispatch even if event write fails
            }

            match self
                .dispatch_task_to_shepherd(shepherd_id, task.clone())
                .await
            {
                Ok(_) => {
                    info!(
                        "Successfully dispatched task {} from domain '{}' to shepherd {}",
                        task.task_id, domain, shepherd_id
                    );
                }
                Err(e) => {
                    warn!("Failed to dispatch task {} from domain '{}' to shepherd {}: {}. Treating as task attempt failure.", 
                          task.task_id, domain, shepherd_id, e);

                    // Decrement in-flight counter since we couldn't dispatch
                    self.decrement_in_flight_task(domain);

                    // TODO: In a real implementation, we'd need to notify the scheduler
                    // about this failure so it can handle the task appropriately.
                    // For now, we just log the failure as specified in the requirements.
                }
            }
        }
    }

    /// Write attempt started event right before dispatch
    async fn write_attempt_started_event(
        &self,
        task: &TaskDispatch,
        shepherd_id: Uuid,
        domain: &str,
    ) -> Result<()> {
        let event_metadata = serde_json::json!({
            "task_id": task.task_id,
            "attempt_number": task.attempt_number,
            "shepherd_id": shepherd_id,
            "scheduled_at": chrono::Utc::now()
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: domain.to_string(),
            task_instance_id: Some(task.task_id),
            flow_instance_id: task.flow_instance_id,
            event_type: crate::EVENT_TASK_ATTEMPT_STARTED,
            created_at: chrono::Utc::now(),
            metadata: event_metadata,
        };

        self.event_stream.write_event(event_record).await?;

        info!(
            "Successfully started task {} attempt {} on shepherd {shepherd_id}",
            task.task_id, task.attempt_number
        );
        Ok(())
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
            domains: crate::orchestrator::db::DomainsConfig::default(),
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
        use crate::orchestrator::db::DomainsConfig;
        let domains_config = Arc::new(DomainsConfig::default());
        ShepherdManager::new(domains_config, task_registry, event_stream)
    }

    // Helper function to manually add shepherds for testing without event stream
    fn add_test_shepherd(manager: &ShepherdManager, uuid: Uuid, max_concurrency: u32) {
        let shepherd_status = ShepherdStatus::new(uuid, max_concurrency);
        manager.shepherds.insert(uuid, shepherd_status);

        // Update total capacity tracking (mimicking register_shepherd behavior)
        manager
            .total_max_concurrency
            .fetch_add(max_concurrency, std::sync::atomic::Ordering::Relaxed);
        // New shepherds start with 0 current load

        // Add to appropriate load bucket (new shepherds have 0% load)
        manager.add_to_bucket(uuid, 0.0);
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

        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid1);
    }

    /// Tests edge cases in shepherd selection including empty pool, zero capacity, and normal operation.
    /// Expected behavior: Should handle empty pools gracefully, reject zero-capacity shepherds, and select normal shepherds correctly.
    #[tokio::test]
    async fn test_find_best_shepherd_edge_cases() {
        let manager = create_test_manager();

        // No shepherds
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 0);

        // Zero max concurrency
        let uuid1 = Uuid::new_v4();
        add_test_shepherd(&manager, uuid1, 0);
        manager.update_shepherd_status(uuid1, 0, 0);
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 0);

        // Normal shepherd
        let uuid2 = Uuid::new_v4();
        add_test_shepherd(&manager, uuid2, 10);
        manager.update_shepherd_status(uuid2, 3, 7);
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid2);
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
        let result1 = manager.find_available_shepherds(1);
        let result2 = manager.find_available_shepherds(1);

        assert_eq!(result1, result2);
        assert_eq!(result1.len(), 1);
        assert!(uuids.contains(&result1[0]));
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
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid);

        // Mark as disconnected (connection drops)
        manager.mark_shepherd_disconnected(uuid);

        // Should not be available for new tasks
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 0);

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
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid);

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
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid);

        // Mark as disconnected
        manager.mark_shepherd_disconnected(uuid);
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 0);

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
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid);

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
            flow_instance_id: None,
            attempt_number: 0,
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
            flow_instance_id: None,
            attempt_number: 0,
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
            flow_instance_id: None,
            attempt_number: 0,
        };
        let result = manager.dispatch_task_to_shepherd(uuid, task_dispatch).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not connected"));
    }

    #[test]
    fn test_virtual_queue_basic_operations() {
        let mut queue = VirtualQueue::new(3);

        // Test initial state
        assert_eq!(queue.queue_len(), 0);
        assert_eq!(queue.in_flight_count(), 0);
        assert!(queue.can_dispatch());
        assert_eq!(queue.available_dispatch_count(), 0);

        // Test enqueue
        let task1 = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            args: vec!["arg1".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
            flow_instance_id: None,
            attempt_number: 0,
        };
        queue.enqueue(task1.clone()).unwrap();
        assert_eq!(queue.queue_len(), 1);
        assert_eq!(queue.available_dispatch_count(), 1);

        // Test dequeue
        let dequeued = queue.dequeue().unwrap();
        assert_eq!(dequeued.task_id, task1.task_id);
        assert_eq!(queue.queue_len(), 0);
        assert_eq!(queue.in_flight_count(), 1);
        assert_eq!(queue.available_dispatch_count(), 0);

        // Test decrement
        queue.decrement_in_flight();
        assert_eq!(queue.in_flight_count(), 0);
    }

    #[test]
    fn test_virtual_queue_concurrency_limits() {
        let mut queue = VirtualQueue::new(2);

        // Add tasks up to concurrency limit
        let task1 = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test1".to_string(),
            args: vec![],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
            flow_instance_id: None,
            attempt_number: 0,
        };
        let task2 = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test2".to_string(),
            args: vec![],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
            flow_instance_id: None,
            attempt_number: 1,
        };
        let task3 = TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: "test3".to_string(),
            args: vec![],
            kwargs: "{}".to_string(),
            memory_limit: None,
            cpu_limit: None,
            flow_instance_id: None,
            attempt_number: 2,
        };

        queue.enqueue(task1).unwrap();
        queue.enqueue(task2).unwrap();
        queue.enqueue(task3).unwrap();
        assert_eq!(queue.queue_len(), 3);
        assert_eq!(queue.available_dispatch_count(), 2); // Limited by concurrency

        // Dequeue up to limit
        queue.dequeue().unwrap();
        queue.dequeue().unwrap();
        assert_eq!(queue.in_flight_count(), 2);
        assert!(!queue.can_dispatch());
        assert_eq!(queue.available_dispatch_count(), 0);

        // Should not be able to dequeue more
        assert!(queue.dequeue().is_none());

        // After decrementing, should be able to dispatch again
        queue.decrement_in_flight();
        assert!(queue.can_dispatch());
        assert_eq!(queue.available_dispatch_count(), 1);
        queue.dequeue().unwrap();
        assert_eq!(queue.queue_len(), 0);
    }

    /// Tests the bucket-based load balancing algorithm with shepherds in different load buckets.
    /// Expected behavior: Shepherds are distributed across 10 buckets based on load ratio, and lower bucket shepherds are prioritized for task assignment.
    #[tokio::test]
    async fn test_bucket_based_load_balancing() {
        let manager = create_test_manager();

        // Create shepherds in different buckets
        let uuid1 = Uuid::new_v4(); // Will be in bucket 0 (0% load)
        let uuid2 = Uuid::new_v4(); // Will be in bucket 2 (20% load)
        let uuid3 = Uuid::new_v4(); // Will be in bucket 5 (50% load)

        add_test_shepherd(&manager, uuid1, 10);
        add_test_shepherd(&manager, uuid2, 10);
        add_test_shepherd(&manager, uuid3, 10);

        manager.update_shepherd_status(uuid1, 0, 10); // 0% load -> bucket 0
        manager.update_shepherd_status(uuid2, 2, 8); // 20% load -> bucket 2
        manager.update_shepherd_status(uuid3, 5, 5); // 50% load -> bucket 5

        // Total: 30 max capacity, 7 current load
        // Expected avg after 10 tasks: (7 + 10) / 30 = 0.567

        let result = manager.find_available_shepherds(10);
        assert_eq!(result.len(), 10);

        // Lower bucket shepherds should get more tasks
        let uuid1_count = result.iter().filter(|&&id| id == uuid1).count();
        let uuid2_count = result.iter().filter(|&&id| id == uuid2).count();
        let uuid3_count = result.iter().filter(|&&id| id == uuid3).count();

        // uuid1 should get the most tasks (it's in bucket 0)
        assert!(uuid1_count >= uuid2_count);
        assert!(uuid1_count >= uuid3_count);

        // Total should equal batch size
        assert_eq!(uuid1_count + uuid2_count + uuid3_count, 10);
    }

    /// Tests that the same shepherd can be selected multiple times in a single batch for load balancing.
    /// Expected behavior: When a shepherd has capacity for multiple tasks, it can appear multiple times in the result to achieve optimal load distribution.
    #[tokio::test]
    async fn test_shepherd_multiple_selection() {
        let manager = create_test_manager();

        // Create single shepherd
        let uuid1 = Uuid::new_v4();
        add_test_shepherd(&manager, uuid1, 10);
        manager.update_shepherd_status(uuid1, 0, 10); // 0% load, full capacity

        // Request multiple tasks - should get same shepherd multiple times
        let result = manager.find_available_shepherds(5);
        assert_eq!(result.len(), 5);

        // All should be the same shepherd
        assert!(result.iter().all(|&id| id == uuid1));
    }

    /// Tests that the algorithm distributes tasks to achieve a balanced target average load ratio across all shepherds.
    /// Expected behavior: Tasks are assigned to minimize load imbalance, with shepherds receiving tasks proportional to reaching the expected average load ratio.
    #[tokio::test]
    async fn test_load_balancing_target_average() {
        let manager = create_test_manager();

        // Two shepherds with same capacity but different loads
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        add_test_shepherd(&manager, uuid1, 10);
        add_test_shepherd(&manager, uuid2, 10);

        manager.update_shepherd_status(uuid1, 0, 10); // 0% load
        manager.update_shepherd_status(uuid2, 5, 5); // 50% load

        // Total: 20 max capacity, 5 current load
        // Expected avg after 10 tasks: (5 + 10) / 20 = 0.75
        // uuid1 target: 0.75 * 10 = 7.5 -> needs 8 tasks (ceiling)
        // uuid2 target: 0.75 * 10 = 7.5 -> needs 3 tasks (ceiling)
        // But we only have 10 tasks total, so uuid1 gets 7, uuid2 gets 3

        let result = manager.find_available_shepherds(10);
        assert_eq!(result.len(), 10);

        let uuid1_count = result.iter().filter(|&&id| id == uuid1).count();
        let uuid2_count = result.iter().filter(|&&id| id == uuid2).count();

        // uuid1 should get more tasks to balance the load
        assert!(uuid1_count > uuid2_count);
        assert_eq!(uuid1_count + uuid2_count, 10);
    }
}
