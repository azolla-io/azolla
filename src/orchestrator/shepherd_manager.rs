use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use log::{error, info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tonic::Status;
use uuid::Uuid;

use crate::orchestrator::db::DomainsConfig;
use crate::orchestrator::event_stream::EventStream;
use crate::orchestrator::taskset::TaskSetRegistry;
use crate::proto::{common, orchestrator::ServerMsg};
use crate::EVENT_SHEPHERD_REGISTERED;

const SHEPHERD_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Messages for actor-based ShepherdManager communication
#[derive(Debug)]
pub enum ShepherdManagerMessage {
    RegisterShepherd {
        uuid: Uuid,
        max_concurrency: u32,
        tx: ShepherdTxChannel,
        response_tx: oneshot::Sender<Result<()>>,
    },
    UpdateShepherdStatus {
        uuid: Uuid,
        current_load: u32,
        available_capacity: u32,
        response_tx: oneshot::Sender<Result<()>>,
    },
    MarkShepherdAlive {
        uuid: Uuid,
        response_tx: oneshot::Sender<Result<()>>,
    },
    MarkShepherdDisconnected {
        uuid: Uuid,
        response_tx: oneshot::Sender<Result<()>>,
    },
    FindAvailableShepherds {
        batch: u32,
        response_tx: oneshot::Sender<Vec<Uuid>>,
    },
    EnqueueTask {
        domain: String,
        task: TaskDispatch,
        response_tx: oneshot::Sender<Result<()>>,
    },
    DecrementInFlightTask {
        domain: String,
        response_tx: oneshot::Sender<Result<()>>,
    },
    GetStats {
        response_tx: oneshot::Sender<ShepherdManagerStats>,
    },
    IsShepherdRegistered {
        uuid: Uuid,
        response_tx: oneshot::Sender<bool>,
    },
    GetShepherdStatus {
        uuid: Uuid,
        response_tx: oneshot::Sender<Option<ShepherdConnectionStatus>>,
    },
    GetActiveDomains {
        response_tx: oneshot::Sender<Vec<String>>,
    },
    GetDomainStats {
        domain: String,
        response_tx: oneshot::Sender<Option<(usize, u32)>>,
    },
    RemoveShepherd {
        uuid: Uuid,
        response_tx: oneshot::Sender<Result<()>>,
    },
    DispatchTick,
    HealthCheckTick,
}

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
        // TODO: Add underflow protection - this should never underflow, and it's a bug if it does
        // Consider adding debug_assert or logging when count is already 0
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

    /// Register a new shepherd connection with communication channel
    pub async fn register_shepherd(
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

    /// Get a handle to communicate with the actor version of ShepherdManager
    pub fn get_handle(&self) -> ShepherdManagerHandle {
        let (command_tx, command_rx) = mpsc::channel(1000);

        // Create actor instance
        let mut actor = ActorShepherdManager::new(
            self.domains_config.clone(),
            self.task_registry.clone(),
            self.event_stream.clone(),
        );

        // Spawn actor task
        tokio::spawn(async move {
            actor.main_loop(command_rx).await;
        });

        ShepherdManagerHandle::new(command_tx)
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

/// Actor-based ShepherdManager using HashMap for better single-threaded performance
pub struct ActorShepherdManager {
    // Core data structures (HashMap instead of DashMap for single-threaded actor)
    shepherds: HashMap<Uuid, ShepherdStatus>,
    virtual_queues: HashMap<String, VirtualQueue>,

    // Configuration and shared services
    domains_config: Arc<DomainsConfig>,
    #[allow(dead_code)]
    task_registry: Arc<TaskSetRegistry>, // Reserved for future task-related operations
    event_stream: Arc<EventStream>,

    // Round-robin state for perfect scheduling
    round_robin_index: usize,

    // Total capacity tracking for load calculations
    total_max_concurrency: u32,
    total_current_load: u32,
}

impl ActorShepherdManager {
    pub fn new(
        domains_config: Arc<DomainsConfig>,
        task_registry: Arc<TaskSetRegistry>,
        event_stream: Arc<EventStream>,
    ) -> Self {
        Self {
            shepherds: HashMap::new(),
            virtual_queues: HashMap::new(),
            domains_config,
            task_registry,
            event_stream,
            round_robin_index: 0,
            total_max_concurrency: 0,
            total_current_load: 0,
        }
    }

    /// Main actor loop handling all shepherd management operations
    pub async fn main_loop(&mut self, mut command_rx: mpsc::Receiver<ShepherdManagerMessage>) {
        let mut dispatch_interval = time::interval(Duration::from_millis(100));
        let mut health_check_interval = time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // Handle incoming commands
                Some(message) = command_rx.recv() => {
                    if let Err(e) = self.handle_command(message).await {
                        error!("Error handling ShepherdManager command: {e}");
                    }
                }

                // Periodic task dispatch
                _ = dispatch_interval.tick() => {
                    if let Err(e) = self.handle_dispatch_tick().await {
                        error!("Error in dispatch tick: {e}");
                    }
                }

                // Periodic health checks
                _ = health_check_interval.tick() => {
                    if let Err(e) = self.handle_health_check_tick().await {
                        error!("Error in health check tick: {e}");
                    }
                }

                // Exit when command channel is closed
                else => {
                    info!("ShepherdManager actor loop ending - command channel closed");
                    break;
                }
            }
        }
    }

    /// Handle incoming command messages
    async fn handle_command(&mut self, message: ShepherdManagerMessage) -> Result<()> {
        match message {
            ShepherdManagerMessage::RegisterShepherd {
                uuid,
                max_concurrency,
                tx,
                response_tx,
            } => {
                let result = self
                    .register_shepherd_internal(uuid, max_concurrency, tx)
                    .await;
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::UpdateShepherdStatus {
                uuid,
                current_load,
                available_capacity,
                response_tx,
            } => {
                let result =
                    self.update_shepherd_status_internal(uuid, current_load, available_capacity);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::MarkShepherdAlive { uuid, response_tx } => {
                let result = self.mark_shepherd_alive_internal(uuid);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::MarkShepherdDisconnected { uuid, response_tx } => {
                let result = self.mark_shepherd_disconnected_internal(uuid);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::FindAvailableShepherds { batch, response_tx } => {
                let result = self.find_available_shepherds_round_robin(batch);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::EnqueueTask {
                domain,
                task,
                response_tx,
            } => {
                let result = self.enqueue_task_internal(domain, task);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::DecrementInFlightTask {
                domain,
                response_tx,
            } => {
                let result = self.decrement_in_flight_task_internal(&domain);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::GetStats { response_tx } => {
                let stats = self.get_stats_internal();
                let _ = response_tx.send(stats);
            }

            ShepherdManagerMessage::IsShepherdRegistered { uuid, response_tx } => {
                let result = self.is_shepherd_registered_internal(uuid);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::GetShepherdStatus { uuid, response_tx } => {
                let result = self.get_shepherd_status_internal(uuid);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::GetActiveDomains { response_tx } => {
                let result = self.get_active_domains_internal();
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::GetDomainStats {
                domain,
                response_tx,
            } => {
                let result = self.get_domain_stats_internal(&domain);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::RemoveShepherd { uuid, response_tx } => {
                let result = self.remove_shepherd_internal(uuid);
                let _ = response_tx.send(result);
            }

            ShepherdManagerMessage::DispatchTick => {
                if let Err(e) = self.handle_dispatch_tick().await {
                    error!("Error in manual dispatch tick: {e}");
                }
            }

            ShepherdManagerMessage::HealthCheckTick => {
                if let Err(e) = self.handle_health_check_tick().await {
                    error!("Error in manual health check tick: {e}");
                }
            }
        }
        Ok(())
    }

    /// Handle periodic task dispatch
    async fn handle_dispatch_tick(&mut self) -> Result<()> {
        // Process each domain's virtual queue
        let domains: Vec<String> = self.virtual_queues.keys().cloned().collect();

        for domain in domains {
            if let Err(e) = self.process_domain_queue(&domain).await {
                error!("Error processing domain queue for {domain}: {e}");
            }
        }

        Ok(())
    }

    /// Handle periodic health checks  
    async fn handle_health_check_tick(&mut self) -> Result<()> {
        let now = Utc::now();
        let timeout_threshold = now - ChronoDuration::seconds(SHEPHERD_TIMEOUT_SECS as i64);

        let shepherds_to_disconnect: Vec<Uuid> = self
            .shepherds
            .iter()
            .filter(|(_, status)| {
                matches!(status.status, ShepherdConnectionStatus::Connected)
                    && status.last_seen < timeout_threshold
            })
            .map(|(&uuid, _)| uuid)
            .collect();

        for uuid in shepherds_to_disconnect {
            warn!("Shepherd {uuid} timed out, marking as disconnected");
            let _ = self.mark_shepherd_disconnected_internal(uuid);
        }

        Ok(())
    }

    /// Perfect round-robin algorithm with skip logic for unavailable shepherds
    pub fn find_available_shepherds_round_robin(&mut self, batch: u32) -> Vec<Uuid> {
        let mut result = Vec::new();

        if self.shepherds.is_empty() || batch == 0 {
            return result;
        }

        // Create a list of available shepherds with their IDs and capacity
        let shepherd_list: Vec<(Uuid, &ShepherdStatus)> = self
            .shepherds
            .iter()
            .filter(|(_, status)| {
                matches!(status.status, ShepherdConnectionStatus::Connected)
                    && status.available_capacity() > 0
            })
            .map(|(uuid, status)| (*uuid, status))
            .collect();

        if shepherd_list.is_empty() {
            return result;
        }

        let shepherd_count = shepherd_list.len();
        let mut tasks_assigned = 0u32;

        // Round-robin with skip: continue until we assign all tasks or exhaust capacity
        while tasks_assigned < batch {
            let mut round_complete = true;

            for i in 0..shepherd_count {
                if tasks_assigned >= batch {
                    break;
                }

                let index = (self.round_robin_index + i) % shepherd_count;
                let (shepherd_id, shepherd_status) = shepherd_list[index];

                // Check if this shepherd still has capacity
                let current_assignments =
                    result.iter().filter(|&&id| id == shepherd_id).count() as u32;
                if current_assignments < shepherd_status.available_capacity() {
                    result.push(shepherd_id);
                    tasks_assigned += 1;
                    round_complete = false;
                }
            }

            // If we completed a full round without assigning any tasks, we're done
            if round_complete {
                break;
            }

            // Update round-robin index for next call
            self.round_robin_index = (self.round_robin_index + 1) % shepherd_count;
        }

        result
    }

    /// Internal method to register a shepherd with TX channel
    async fn register_shepherd_internal(
        &mut self,
        uuid: Uuid,
        max_concurrency: u32,
        tx: ShepherdTxChannel,
    ) -> Result<()> {
        // Register shepherd first - do the basic registration logic inline
        // Check if this is a reconnection
        if let Some(existing_shepherd) = self.shepherds.get_mut(&uuid) {
            if matches!(
                existing_shepherd.status,
                ShepherdConnectionStatus::Disconnected
            ) {
                info!("Shepherd {uuid} reconnecting (was temporarily unavailable)");
                existing_shepherd.status = ShepherdConnectionStatus::Connected;
                existing_shepherd.update_last_seen();
                // Set the TX channel for reconnected shepherd
                existing_shepherd.set_tx(tx);
                return Ok(());
            }
        }

        // New shepherd registration
        let mut shepherd_status = ShepherdStatus::new(uuid, max_concurrency);
        info!("Registering new shepherd {uuid} with max_concurrency={max_concurrency}");

        // Write registration event
        let event_metadata = serde_json::json!({
            "shepherd_uuid": uuid,
            "max_concurrency": max_concurrency,
            "registered_at": chrono::Utc::now()
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: "system".to_string(),
            task_instance_id: None,
            flow_instance_id: None,
            event_type: crate::EVENT_SHEPHERD_REGISTERED,
            created_at: chrono::Utc::now(),
            metadata: event_metadata,
        };

        // Write event if possible, but don't fail tests if database unavailable
        #[cfg(test)]
        {
            if let Err(e) = self.event_stream.write_event(event_record).await {
                // In tests, just log the error but continue
                log::debug!("Failed to write shepherd registration event in test: {e}");
            }
        }
        #[cfg(not(test))]
        {
            // In production, this is a real error
            self.event_stream.write_event(event_record).await?;
        }

        // Set the TX channel
        shepherd_status.set_tx(tx);

        // Add to in-memory tracking
        self.shepherds.insert(uuid, shepherd_status);

        info!("Successfully registered shepherd {uuid}");
        Ok(())
    }

    /// Internal method to update shepherd status
    fn update_shepherd_status_internal(
        &mut self,
        uuid: Uuid,
        current_load: u32,
        available_capacity: u32,
    ) -> Result<()> {
        if let Some(shepherd) = self.shepherds.get_mut(&uuid) {
            let old_load = shepherd.current_load;
            shepherd.update_load(current_load, available_capacity);
            shepherd.update_last_seen();

            // Update total current load
            self.total_current_load =
                self.total_current_load.saturating_sub(old_load) + current_load;

            Ok(())
        } else {
            Err(anyhow::anyhow!("Shepherd {} not found", uuid))
        }
    }

    /// Internal method to mark shepherd alive
    fn mark_shepherd_alive_internal(&mut self, uuid: Uuid) -> Result<()> {
        if let Some(shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.update_last_seen();
            Ok(())
        } else {
            Err(anyhow::anyhow!("Shepherd {} not found", uuid))
        }
    }

    /// Internal method to mark shepherd disconnected
    fn mark_shepherd_disconnected_internal(&mut self, uuid: Uuid) -> Result<()> {
        if let Some(shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.status = ShepherdConnectionStatus::Disconnected;
            shepherd.clear_tx();
            Ok(())
        } else {
            Err(anyhow::anyhow!("Shepherd {} not found", uuid))
        }
    }

    /// Internal method to enqueue task
    fn enqueue_task_internal(&mut self, domain: String, task: TaskDispatch) -> Result<()> {
        let queue = self
            .virtual_queues
            .entry(domain.clone())
            .or_insert_with(|| {
                // Get concurrency limit from domain config, fallback to default
                let concurrency_limit = self
                    .domains_config
                    .specific
                    .get(&domain)
                    .map(|config| config.concurrency_limit)
                    .unwrap_or(self.domains_config.default_concurrency_limit);
                VirtualQueue::new(concurrency_limit)
            });

        queue.enqueue(task)
    }

    /// Internal method to decrement in-flight task
    fn decrement_in_flight_task_internal(&mut self, domain: &str) -> Result<()> {
        if let Some(queue) = self.virtual_queues.get_mut(domain) {
            queue.decrement_in_flight();
            Ok(())
        } else {
            Err(anyhow::anyhow!("Domain {} not found", domain))
        }
    }

    /// Internal method to get stats
    fn get_stats_internal(&self) -> ShepherdManagerStats {
        let (connected, disconnected, total_capacity, total_load) =
            self.shepherds
                .values()
                .fold((0, 0, 0u32, 0u32), |(conn, disc, cap, load), status| {
                    let (new_conn, new_disc) = match status.status {
                        ShepherdConnectionStatus::Connected => (conn + 1, disc),
                        ShepherdConnectionStatus::Disconnected => (conn, disc + 1),
                        ShepherdConnectionStatus::Reconnecting => (conn, disc + 1), // Count as disconnected for stats
                    };
                    (
                        new_conn,
                        new_disc,
                        cap + status.max_concurrency,
                        load + status.current_load,
                    )
                });

        let utilization = if total_capacity > 0 {
            total_load as f64 / total_capacity as f64
        } else {
            0.0
        };

        ShepherdManagerStats {
            connected_shepherds: connected,
            disconnected_shepherds: disconnected,
            total_capacity,
            total_load,
            utilization,
        }
    }

    /// Internal method to check if shepherd is registered
    fn is_shepherd_registered_internal(&self, uuid: Uuid) -> bool {
        self.shepherds.contains_key(&uuid)
    }

    /// Internal method to get shepherd status
    fn get_shepherd_status_internal(&self, uuid: Uuid) -> Option<ShepherdConnectionStatus> {
        self.shepherds.get(&uuid).map(|s| s.status.clone())
    }

    /// Internal method to get active domains
    fn get_active_domains_internal(&self) -> Vec<String> {
        self.virtual_queues.keys().cloned().collect()
    }

    /// Internal method to get domain stats
    fn get_domain_stats_internal(&self, domain: &str) -> Option<(usize, u32)> {
        self.virtual_queues
            .get(domain)
            .map(|queue| (queue.queue_len(), queue.in_flight_count()))
    }

    /// Internal method to remove shepherd
    fn remove_shepherd_internal(&mut self, uuid: Uuid) -> Result<()> {
        if let Some(shepherd) = self.shepherds.remove(&uuid) {
            // Update totals
            self.total_max_concurrency = self
                .total_max_concurrency
                .saturating_sub(shepherd.max_concurrency);
            self.total_current_load = self
                .total_current_load
                .saturating_sub(shepherd.current_load);

            info!("Removed shepherd {uuid}");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Shepherd {} not found", uuid))
        }
    }

    /// Process a domain's virtual queue by dispatching available tasks
    async fn process_domain_queue(&mut self, domain: &str) -> Result<()> {
        // Check if we can dispatch any tasks and get available count
        let available_dispatches = {
            let queue = match self.virtual_queues.get(domain) {
                Some(q) => q,
                None => return Ok(()), // No queue for this domain
            };

            if !queue.can_dispatch() {
                return Ok(());
            }

            queue.available_dispatch_count()
        };

        // Find available shepherds
        let shepherds = self.find_available_shepherds_round_robin(available_dispatches);

        if shepherds.is_empty() {
            return Ok(()); // No shepherds available
        }

        // Dispatch tasks to available shepherds
        for shepherd_id in shepherds {
            let task = {
                let queue = self.virtual_queues.get_mut(domain).unwrap();
                match queue.dequeue() {
                    Some(task) => task,
                    None => break, // No more tasks in queue
                }
            };

            if let Err(e) = self
                .dispatch_task_to_shepherd_internal(&task, shepherd_id, domain)
                .await
            {
                error!(
                    "Failed to dispatch task {} to shepherd {shepherd_id}: {e}",
                    task.task_id
                );
                // Re-enqueue the task on failure
                let queue = self.virtual_queues.get_mut(domain).unwrap();
                if let Err(enqueue_err) = queue.enqueue(task) {
                    error!("Failed to re-enqueue task after dispatch failure: {enqueue_err}");
                }
            }
        }

        Ok(())
    }

    /// Dispatch a task to a specific shepherd
    async fn dispatch_task_to_shepherd_internal(
        &mut self,
        task: &TaskDispatch,
        shepherd_id: Uuid,
        domain: &str,
    ) -> Result<()> {
        // Check shepherd exists and is connected
        {
            let shepherd = self
                .shepherds
                .get(&shepherd_id)
                .ok_or_else(|| anyhow::anyhow!("Shepherd {} not found", shepherd_id))?;

            if !matches!(shepherd.status, ShepherdConnectionStatus::Connected) {
                return Err(anyhow::anyhow!("Shepherd {} is not connected", shepherd_id));
            }
        }

        // Write attempt started event
        if let Err(e) = self
            .write_attempt_started_event(task, shepherd_id, domain)
            .await
        {
            warn!(
                "Failed to write attempt started event for task {} to shepherd {}: {}",
                task.task_id, shepherd_id, e
            );
        }

        // Dispatch the task
        let shepherd = self
            .shepherds
            .get_mut(&shepherd_id)
            .ok_or_else(|| anyhow::anyhow!("Shepherd {} not found", shepherd_id))?;

        shepherd
            .dispatch_task(
                task.task_id,
                task.task_name.clone(),
                task.args.clone(),
                task.kwargs.clone(),
                task.memory_limit,
                task.cpu_limit,
            )
            .await?;

        info!(
            "Successfully started task {} attempt {} on shepherd {}",
            task.task_id, task.attempt_number, shepherd_id
        );

        Ok(())
    }

    /// Write attempt started event to event stream
    async fn write_attempt_started_event(
        &self,
        task: &TaskDispatch,
        shepherd_id: Uuid,
        domain: &str,
    ) -> Result<()> {
        let event_metadata = serde_json::json!({
            "task_id": task.task_id,
            "shepherd_uuid": shepherd_id,
            "attempt_number": task.attempt_number,
            "task_name": task.task_name,
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: domain.to_string(),
            task_instance_id: Some(task.task_id),
            flow_instance_id: None,
            event_type: 2, // ATTEMPT_STARTED
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        self.event_stream.write_event(event_record).await?;
        Ok(())
    }
}

/// Handle for async communication with the ShepherdManager actor
#[derive(Clone)]
pub struct ShepherdManagerHandle {
    command_tx: mpsc::Sender<ShepherdManagerMessage>,
}

impl ShepherdManagerHandle {
    pub fn new(command_tx: mpsc::Sender<ShepherdManagerMessage>) -> Self {
        Self { command_tx }
    }

    /// Register a shepherd with communication channel
    pub async fn register_shepherd(
        &self,
        uuid: Uuid,
        max_concurrency: u32,
        tx: ShepherdTxChannel,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::RegisterShepherd {
                uuid,
                max_concurrency,
                tx,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Update shepherd status
    pub async fn update_shepherd_status(
        &self,
        uuid: Uuid,
        current_load: u32,
        available_capacity: u32,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::UpdateShepherdStatus {
                uuid,
                current_load,
                available_capacity,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Mark shepherd as alive
    pub async fn mark_shepherd_alive(&self, uuid: Uuid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::MarkShepherdAlive { uuid, response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Mark shepherd as disconnected
    pub async fn mark_shepherd_disconnected(&self, uuid: Uuid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::MarkShepherdDisconnected { uuid, response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Find available shepherds for task assignment
    pub async fn find_available_shepherds(&self, batch: u32) -> Vec<Uuid> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::FindAvailableShepherds { batch, response_tx })
            .await
            .is_err()
        {
            return Vec::new(); // Actor not running
        }

        response_rx.await.unwrap_or_else(|_| Vec::new())
    }

    /// Enqueue a task to a domain's virtual queue
    pub async fn enqueue_task(&self, domain: String, task: TaskDispatch) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::EnqueueTask {
                domain,
                task,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Decrement in-flight task count for a domain
    pub async fn decrement_in_flight_task(&self, domain: String) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::DecrementInFlightTask {
                domain,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Get shepherd manager statistics
    pub async fn get_stats(&self) -> ShepherdManagerStats {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::GetStats { response_tx })
            .await
            .is_err()
        {
            return ShepherdManagerStats {
                connected_shepherds: 0,
                disconnected_shepherds: 0,
                total_capacity: 0,
                total_load: 0,
                utilization: 0.0,
            };
        }

        response_rx.await.unwrap_or(ShepherdManagerStats {
            connected_shepherds: 0,
            disconnected_shepherds: 0,
            total_capacity: 0,
            total_load: 0,
            utilization: 0.0,
        })
    }

    /// Check if a shepherd is registered
    pub async fn is_shepherd_registered(&self, uuid: Uuid) -> bool {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::IsShepherdRegistered { uuid, response_tx })
            .await
            .is_err()
        {
            return false;
        }

        response_rx.await.unwrap_or(false)
    }

    /// Get shepherd connection status
    pub async fn get_shepherd_status(&self, uuid: Uuid) -> Option<ShepherdConnectionStatus> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::GetShepherdStatus { uuid, response_tx })
            .await
            .is_err()
        {
            return None;
        }

        response_rx.await.unwrap_or(None)
    }

    /// Get active domains
    pub async fn get_active_domains(&self) -> Vec<String> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::GetActiveDomains { response_tx })
            .await
            .is_err()
        {
            return Vec::new();
        }

        response_rx.await.unwrap_or_else(|_| Vec::new())
    }

    /// Get domain statistics
    pub async fn get_domain_stats(&self, domain: String) -> Option<(usize, u32)> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::GetDomainStats {
                domain,
                response_tx,
            })
            .await
            .is_err()
        {
            return None;
        }

        response_rx.await.unwrap_or(None)
    }

    /// Remove a shepherd
    pub async fn remove_shepherd(&self, uuid: Uuid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::RemoveShepherd { uuid, response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor response channel closed"))?
    }

    /// Send a manual dispatch tick (for testing)
    pub async fn dispatch_tick(&self) -> Result<()> {
        self.command_tx
            .send(ShepherdManagerMessage::DispatchTick)
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))
    }

    /// Send a manual health check tick (for testing)
    pub async fn health_check_tick(&self) -> Result<()> {
        self.command_tx
            .send(ShepherdManagerMessage::HealthCheckTick)
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor is not running"))
    }
}

#[cfg(test)]
mod tests {
    //! # ShepherdManager Test Suite
    //!
    //! This comprehensive test suite covers both the legacy synchronous interface and the new
    //! actor-based asynchronous interface of the ShepherdManager. The tests are organized into
    //! logical sections:
    //!
    //! ## 1. Test Infrastructure
    //! - Helper functions for creating test managers and shepherds
    //! - Async test utilities for the actor model
    //!
    //! ## 2. Core Shepherd Management
    //! - Registration, status updates, and lifecycle management
    //! - Connection status tracking and reconnection handling
    //!
    //! ## 3. Perfect Round-Robin Scheduling
    //! - Algorithm correctness and fairness
    //! - Edge cases and capacity limits
    //! - Skip logic for unavailable shepherds
    //!
    //! ## 4. Actor Model Interface
    //! - Async message passing via ShepherdManagerHandle
    //! - Concurrent access and thread safety
    //! - Error handling and recovery
    //!
    //! ## 5. Virtual Queue Management
    //! - Task enqueueing and dispatching
    //! - Concurrency limits and flow control
    //! - Multi-domain processing
    //!
    //! ## 6. Integration Tests
    //! - End-to-end dispatcher loop functionality
    //! - Real-world scenarios and edge cases

    use super::*;
    use tokio::time::{timeout, Duration};
    use uuid::Uuid;

    // ============================================================================
    // TEST INFRASTRUCTURE
    // ============================================================================

    /// Creates a ShepherdManager for synchronous/legacy testing
    fn create_test_manager() -> ShepherdManager {
        use crate::orchestrator::db::{create_pool, Database, Server, Settings};
        use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};

        let task_registry = Arc::new(TaskSetRegistry::new());

        // Create minimal settings for testing - event writing will be handled gracefully
        let settings = Settings {
            database: Database {
                url: "postgres://test:test@localhost/test".to_string(),
                pool_size: 1,
            },
            server: Server { port: 0 },
            event_stream: crate::orchestrator::db::EventStream::default(),
            domains: crate::orchestrator::db::DomainsConfig::default(),
        };

        // Create a dummy pool that gracefully handles connection failures
        let pool = create_pool(&settings).unwrap_or_else(|_| {
            use deadpool_postgres::{Manager, Pool};
            let config = "postgres://test:test@localhost/test".parse().unwrap();
            let manager = Manager::new(config, tokio_postgres::NoTls);
            Pool::builder(manager).max_size(1).build().unwrap()
        });

        let event_stream_config = EventStreamConfig::default();
        let event_stream = Arc::new(EventStream::new(pool, event_stream_config));
        let domains_config = Arc::new(crate::orchestrator::db::DomainsConfig::default());
        ShepherdManager::new(domains_config, task_registry, event_stream)
    }

    /// Creates a ShepherdManagerHandle for actor model testing
    async fn create_test_handle() -> ShepherdManagerHandle {
        let manager = create_test_manager();
        manager.get_handle()
    }

    /// Creates a test task for enqueueing
    fn create_test_task(task_name: &str) -> TaskDispatch {
        TaskDispatch {
            task_id: Uuid::new_v4(),
            task_name: task_name.to_string(),
            args: vec!["arg1".to_string(), "arg2".to_string()],
            kwargs: "{}".to_string(),
            memory_limit: Some(1024),
            cpu_limit: Some(2),
            attempt_number: 1,
            flow_instance_id: Some(Uuid::new_v4()),
        }
    }

    /// Helper function to manually add shepherds for legacy testing (bypasses event stream)
    fn add_test_shepherd(manager: &ShepherdManager, uuid: Uuid, max_concurrency: u32) {
        let shepherd_status = ShepherdStatus::new(uuid, max_concurrency);
        manager.shepherds.insert(uuid, shepherd_status);

        // Update total capacity tracking
        manager
            .total_max_concurrency
            .fetch_add(max_concurrency, std::sync::atomic::Ordering::Relaxed);

        // Add to load bucket for legacy tests
        manager.add_to_bucket(uuid, 0.0);
    }

    /// Helper function to create a test TX channel for shepherd communication
    fn create_test_tx() -> (ShepherdTxChannel, ShepherdRxChannel) {
        mpsc::channel(10)
    }

    /// Test helper to register a shepherd without needing to handle TX channels
    async fn register_test_shepherd(
        handle: &ShepherdManagerHandle,
        uuid: Uuid,
        max_concurrency: u32,
    ) -> Result<()> {
        let (tx, _rx) = create_test_tx();
        handle.register_shepherd(uuid, max_concurrency, tx).await
    }

    // ============================================================================
    // CORE SHEPHERD MANAGEMENT TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_shepherd_registration_and_lifecycle() {
        let handle = create_test_handle().await;
        let uuid = Uuid::new_v4();

        // Test registration
        assert!(register_test_shepherd(&handle, uuid, 10).await.is_ok());

        // Test status check
        assert!(handle.is_shepherd_registered(uuid).await);
        assert_eq!(
            handle.get_shepherd_status(uuid).await,
            Some(ShepherdConnectionStatus::Connected)
        );

        // Test status update
        assert!(handle.update_shepherd_status(uuid, 5, 5).await.is_ok());

        // Test disconnection
        assert!(handle.mark_shepherd_disconnected(uuid).await.is_ok());
        assert_eq!(
            handle.get_shepherd_status(uuid).await,
            Some(ShepherdConnectionStatus::Disconnected)
        );

        // Test removal
        assert!(handle.remove_shepherd(uuid).await.is_ok());
        assert!(!handle.is_shepherd_registered(uuid).await);
    }

    #[tokio::test]
    async fn test_shepherd_stats_tracking() {
        let handle = create_test_handle().await;

        // Initial stats should be empty
        let stats = handle.get_stats().await;
        assert_eq!(stats.connected_shepherds, 0);
        assert_eq!(stats.total_capacity, 0);

        // Add shepherds and verify stats
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        register_test_shepherd(&handle, uuid1, 10).await.unwrap();
        register_test_shepherd(&handle, uuid2, 20).await.unwrap();

        let stats = handle.get_stats().await;
        assert_eq!(stats.connected_shepherds, 2);
        assert_eq!(stats.total_capacity, 30);

        // Update load and check utilization
        handle.update_shepherd_status(uuid1, 5, 5).await.unwrap();
        handle.update_shepherd_status(uuid2, 10, 10).await.unwrap();

        let stats = handle.get_stats().await;
        assert_eq!(stats.total_load, 15);
        assert_eq!(stats.utilization, 0.5); // 15/30
    }

    // ============================================================================
    // PERFECT ROUND-ROBIN SCHEDULING TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_round_robin_fairness() {
        let handle = create_test_handle().await;

        // Create 3 shepherds with equal capacity
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        register_test_shepherd(&handle, uuid1, 10).await.unwrap();
        register_test_shepherd(&handle, uuid2, 10).await.unwrap();
        register_test_shepherd(&handle, uuid3, 10).await.unwrap();

        // Request 12 tasks - should be distributed 4-4-4
        let assignments = handle.find_available_shepherds(12).await;
        assert_eq!(assignments.len(), 12);

        let uuid1_count = assignments.iter().filter(|&&id| id == uuid1).count();
        let uuid2_count = assignments.iter().filter(|&&id| id == uuid2).count();
        let uuid3_count = assignments.iter().filter(|&&id| id == uuid3).count();

        // Perfect round-robin should distribute evenly
        assert_eq!(uuid1_count, 4);
        assert_eq!(uuid2_count, 4);
        assert_eq!(uuid3_count, 4);
    }

    #[tokio::test]
    async fn test_round_robin_with_different_capacities() {
        let handle = create_test_handle().await;

        let uuid1 = Uuid::new_v4(); // capacity 5
        let uuid2 = Uuid::new_v4(); // capacity 3
        let uuid3 = Uuid::new_v4(); // capacity 2

        register_test_shepherd(&handle, uuid1, 5).await.unwrap();
        register_test_shepherd(&handle, uuid2, 3).await.unwrap();
        register_test_shepherd(&handle, uuid3, 2).await.unwrap();

        // Request 10 tasks (total capacity)
        let assignments = handle.find_available_shepherds(10).await;
        assert_eq!(assignments.len(), 10);

        let uuid1_count = assignments.iter().filter(|&&id| id == uuid1).count();
        let uuid2_count = assignments.iter().filter(|&&id| id == uuid2).count();
        let uuid3_count = assignments.iter().filter(|&&id| id == uuid3).count();

        // Should respect capacity limits
        assert_eq!(uuid1_count, 5);
        assert_eq!(uuid2_count, 3);
        assert_eq!(uuid3_count, 2);
    }

    #[tokio::test]
    async fn test_round_robin_skip_unavailable() {
        let handle = create_test_handle().await;

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        register_test_shepherd(&handle, uuid1, 10).await.unwrap();
        register_test_shepherd(&handle, uuid2, 10).await.unwrap();
        register_test_shepherd(&handle, uuid3, 10).await.unwrap();

        // Disconnect middle shepherd
        handle.mark_shepherd_disconnected(uuid2).await.unwrap();

        // Request tasks - should skip disconnected shepherd
        let assignments = handle.find_available_shepherds(10).await;

        let uuid1_count = assignments.iter().filter(|&&id| id == uuid1).count();
        let uuid2_count = assignments.iter().filter(|&&id| id == uuid2).count();
        let uuid3_count = assignments.iter().filter(|&&id| id == uuid3).count();

        // Should distribute between available shepherds only
        assert_eq!(uuid2_count, 0); // Disconnected
        assert!(uuid1_count > 0);
        assert!(uuid3_count > 0);
        assert_eq!(uuid1_count + uuid3_count, assignments.len());
    }

    // ============================================================================
    // ACTOR MODEL INTERFACE TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_handle_concurrent_operations() {
        let handle = create_test_handle().await;

        // Spawn multiple concurrent operations
        let mut handles = Vec::new();

        for i in 0..5 {
            let h = handle.clone();
            let uuid = Uuid::new_v4();
            handles.push(tokio::spawn(async move {
                register_test_shepherd(&h, uuid, 10).await.unwrap();
                h.update_shepherd_status(uuid, i, 10 - i).await.unwrap();
                uuid
            }));
        }

        // Wait for all operations to complete
        let uuids: Vec<Uuid> = futures::future::try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .collect();

        // Verify all shepherds were registered
        for uuid in uuids {
            assert!(handle.is_shepherd_registered(uuid).await);
        }

        let stats = handle.get_stats().await;
        assert_eq!(stats.connected_shepherds, 5);
    }

    #[tokio::test]
    async fn test_handle_timeout_behavior() {
        let handle = create_test_handle().await;

        // Operations should complete within reasonable time
        let uuid = Uuid::new_v4();

        let result = timeout(
            Duration::from_secs(1),
            register_test_shepherd(&handle, uuid, 10),
        )
        .await;

        assert!(result.is_ok(), "Operation should complete within timeout");
        assert!(result.unwrap().is_ok(), "Registration should succeed");
    }

    // ============================================================================
    // VIRTUAL QUEUE MANAGEMENT TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_virtual_queue_basic_operations() {
        let handle = create_test_handle().await;

        // Test basic queue operations
        let task1 = create_test_task("test_task_1");
        let task2 = create_test_task("test_task_2");

        // Enqueue tasks
        assert!(handle
            .enqueue_task("domain1".to_string(), task1)
            .await
            .is_ok());
        assert!(handle
            .enqueue_task("domain1".to_string(), task2)
            .await
            .is_ok());

        // Check domain stats
        let stats = handle.get_domain_stats("domain1".to_string()).await;
        assert!(stats.is_some());
        let (queue_len, in_flight) = stats.unwrap();
        assert_eq!(queue_len, 2);
        assert_eq!(in_flight, 0);

        // Check active domains
        let domains = handle.get_active_domains().await;
        assert!(domains.contains(&"domain1".to_string()));
    }

    #[tokio::test]
    async fn test_virtual_queue_concurrency_limits() {
        let handle = create_test_handle().await;

        // Create tasks up to concurrency limit (default is 10)
        for i in 0..15 {
            let task = create_test_task(&format!("task_{i}"));
            handle
                .enqueue_task("test_domain".to_string(), task)
                .await
                .unwrap();
        }

        let stats = handle
            .get_domain_stats("test_domain".to_string())
            .await
            .unwrap();
        let (queue_len, _) = stats;

        // Should accept all tasks in queue (dispatching limited by shepherd availability)
        assert_eq!(queue_len, 15);
    }

    #[tokio::test]
    async fn test_multi_domain_isolation() {
        let handle = create_test_handle().await;

        // Enqueue tasks to different domains
        for domain in ["domain_a", "domain_b", "domain_c"] {
            for i in 0..3 {
                let task = create_test_task(&format!("task_{i}"));
                handle.enqueue_task(domain.to_string(), task).await.unwrap();
            }
        }

        // Check each domain has its own queue
        let domains = handle.get_active_domains().await;
        assert_eq!(domains.len(), 3);

        for domain in ["domain_a", "domain_b", "domain_c"] {
            let stats = handle.get_domain_stats(domain.to_string()).await.unwrap();
            let (queue_len, _) = stats;
            assert_eq!(queue_len, 3);
        }
    }

    // ============================================================================
    // INTEGRATION TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_end_to_end_task_dispatch() {
        let handle = create_test_handle().await;

        // Register a shepherd
        let uuid = Uuid::new_v4();
        register_test_shepherd(&handle, uuid, 5).await.unwrap();

        // Enqueue some tasks
        for i in 0..3 {
            let task = create_test_task(&format!("integration_task_{i}"));
            handle
                .enqueue_task("integration".to_string(), task)
                .await
                .unwrap();
        }

        // Trigger manual dispatch
        handle.dispatch_tick().await.unwrap();

        // Give some time for async processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify tasks were processed (queue should be empty or reduced)
        let stats = handle.get_domain_stats("integration".to_string()).await;
        if let Some((queue_len, in_flight)) = stats {
            // Either dispatched (in_flight > 0) or queue is empty
            assert!(queue_len < 3 || in_flight > 0, "Tasks should be dispatched");
        }
    }

    #[tokio::test]
    async fn test_health_check_functionality() {
        let handle = create_test_handle().await;

        // Register a shepherd
        let uuid = Uuid::new_v4();
        register_test_shepherd(&handle, uuid, 10).await.unwrap();

        // Mark as alive
        handle.mark_shepherd_alive(uuid).await.unwrap();

        // Trigger health check
        handle.health_check_tick().await.unwrap();

        // Shepherd should still be connected
        assert_eq!(
            handle.get_shepherd_status(uuid).await,
            Some(ShepherdConnectionStatus::Connected)
        );
    }

    // ============================================================================
    // LEGACY COMPATIBILITY TESTS
    // ============================================================================

    #[tokio::test]
    async fn test_legacy_sync_interface_compatibility() {
        let manager = create_test_manager();
        let uuid = Uuid::new_v4();

        // Test that sync methods still work
        add_test_shepherd(&manager, uuid, 10);
        manager.update_shepherd_status(uuid, 5, 5);
        manager.mark_shepherd_alive(uuid);

        // Test selection
        let result = manager.find_available_shepherds(1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], uuid);
    }

    #[test]
    fn test_virtual_queue_standalone() {
        let mut queue = VirtualQueue::new(3);

        // Test initial state
        assert!(queue.can_dispatch());
        assert_eq!(queue.available_dispatch_count(), 0); // No tasks in queue yet

        let task = create_test_task("standalone_test");
        assert!(queue.enqueue(task.clone()).is_ok());
        assert_eq!(queue.queue_len(), 1);
        assert_eq!(queue.available_dispatch_count(), 1); // Now we have 1 task available

        // Test dequeue
        let dequeued = queue.dequeue();
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().task_name, "standalone_test");
        assert_eq!(queue.queue_len(), 0);
        assert_eq!(queue.available_dispatch_count(), 0); // Back to 0 after dequeue
    }
}
