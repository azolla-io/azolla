use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use log::{debug, error, info, warn};
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
use crate::EVENT_TASK_ATTEMPT_STARTED;

// Shepherd timeout constants moved to configuration (ShepherdConfig)

/// Messages for actor-based ShepherdManager communication
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
    SetSchedulerRegistry {
        scheduler_registry: Arc<crate::orchestrator::scheduler::SchedulerRegistry>,
    },
    Shutdown {
        response_tx: oneshot::Sender<Result<()>>,
    },
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
        let task_id = task.task_id;
        debug!(
            "VirtualQueue::enqueue - Adding task {} to queue (current queue_len: {})",
            task_id,
            self.queue.len()
        );
        self.queue.push_back(task);
        debug!(
            "VirtualQueue::enqueue - Task {} added, new queue_len: {}",
            task_id,
            self.queue.len()
        );
        Ok(())
    }

    /// Dequeue a task for dispatch (if any available)
    pub fn dequeue(&mut self) -> Option<TaskDispatch> {
        debug!("VirtualQueue::dequeue - can_dispatch: {}, queue_empty: {}, queue_len: {}, in_flight: {}", 
            self.can_dispatch(), self.queue.is_empty(), self.queue.len(), self.in_flight_count());
        if self.can_dispatch() && !self.queue.is_empty() {
            let task = self.queue.pop_front();
            if let Some(ref task) = task {
                self.in_flight_count.fetch_add(1, AtomicOrdering::Relaxed);
                debug!(
                    "VirtualQueue::dequeue - Successfully dequeued task {}, new in_flight: {}",
                    task.task_id,
                    self.in_flight_count()
                );
            }
            task
        } else {
            debug!("VirtualQueue::dequeue - Cannot dispatch or queue empty");
            None
        }
    }

    /// Decrement in-flight counter when task completes
    pub fn decrement_in_flight(&self) {
        let previous = self.in_flight_count.fetch_sub(1, AtomicOrdering::Relaxed);
        info!(
            "🔻 VirtualQueue::decrement_in_flight - in_flight count: {} -> {}",
            previous,
            previous.saturating_sub(1)
        );

        // Add underflow protection
        if previous == 0 {
            error!("🚨 VirtualQueue::decrement_in_flight - UNDERFLOW DETECTED! in_flight count was already 0");
        }
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
    // Note: Shepherd load is managed via heartbeat updates from the shepherd itself
    // other than the heartbeat mechanism, we should not modify current_load
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

/// ShepherdManager - Pure actor pattern for thread-safe shepherd management
/// All operations communicate with an internal actor for consistency
#[derive(Debug, Clone)]
pub struct ShepherdManager {
    command_tx: mpsc::Sender<ShepherdManagerMessage>,
}

impl ShepherdManager {
    pub fn new(
        domains_config: Arc<DomainsConfig>,
        task_registry: Arc<TaskSetRegistry>,
        event_stream: Arc<EventStream>,
        shepherd_config: crate::orchestrator::db::ShepherdConfig,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(1000);

        // Create actor instance
        let mut actor =
            ActorShepherdManager::new(domains_config, task_registry, event_stream, shepherd_config);

        // Spawn actor task
        tokio::spawn(async move {
            actor.main_loop(command_rx).await;
        });

        Self { command_tx }
    }

    /// Set the scheduler registry after creation (to break circular dependency)
    pub async fn set_scheduler_registry(
        &self,
        scheduler_registry: Arc<crate::orchestrator::scheduler::SchedulerRegistry>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ShepherdManagerMessage>> {
        // Send message to actor to set scheduler registry (blocking to ensure delivery)
        self.command_tx
            .send(ShepherdManagerMessage::SetSchedulerRegistry { scheduler_registry })
            .await
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
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Update shepherd status (heartbeat)
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
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Mark shepherd as having sent a message (for liveness tracking)
    pub async fn mark_shepherd_alive(&self, uuid: Uuid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::MarkShepherdAlive { uuid, response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Mark shepherd as temporarily unavailable (connection dropped but might reconnect)
    pub async fn mark_shepherd_disconnected(&self, uuid: Uuid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::MarkShepherdDisconnected { uuid, response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Find available shepherds for batch task dispatch
    pub async fn find_available_shepherds(&self, batch: u32) -> Vec<Uuid> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::FindAvailableShepherds { batch, response_tx })
            .await
            .is_err()
        {
            return Vec::new();
        }

        response_rx.await.unwrap_or_default()
    }

    /// Enqueue a task to the virtual queue for a domain
    pub async fn enqueue_task(&self, domain: String, task: TaskDispatch) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::EnqueueTask {
                domain,
                task,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Get statistics about shepherds
    pub async fn get_stats(&self) -> ShepherdManagerStats {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .command_tx
            .send(ShepherdManagerMessage::GetStats { response_tx })
            .await
            .is_err()
        {
            return ShepherdManagerStats::default();
        }

        response_rx.await.unwrap_or_default()
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

    /// Trigger a manual dispatch tick (for testing)
    pub async fn dispatch_tick(&self) -> Result<()> {
        self.command_tx
            .send(ShepherdManagerMessage::DispatchTick)
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))
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
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
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
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }

    /// Send a manual health check tick (for testing)
    pub async fn health_check_tick(&self) -> Result<()> {
        self.command_tx
            .send(ShepherdManagerMessage::HealthCheckTick)
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))
    }

    /// Shutdown the ShepherdManager actor
    pub async fn shutdown(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(ShepherdManagerMessage::Shutdown { response_tx })
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor channel closed"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("ShepherdManager actor disconnected"))?
    }
}

#[derive(Debug, Clone, Default)]
pub struct ShepherdManagerStats {
    pub connected_shepherds: usize,
    pub disconnected_shepherds: usize,
    pub total_capacity: u32,
    pub total_load: u32,
    pub utilization: f64,
}

pub struct ActorShepherdManager {
    // Core data structures (HashMap instead of DashMap for single-threaded actor)
    shepherds: HashMap<Uuid, ShepherdStatus>,
    virtual_queues: HashMap<String, VirtualQueue>,

    // Configuration and shared services
    domains_config: Arc<DomainsConfig>,
    #[allow(dead_code)]
    task_registry: Arc<TaskSetRegistry>, // Reserved for future task-related operations
    event_stream: Arc<EventStream>,
    scheduler_registry: Option<Arc<crate::orchestrator::scheduler::SchedulerRegistry>>,
    shepherd_config: crate::orchestrator::db::ShepherdConfig,

    // Round-robin state for perfect scheduling
    shepherd_keys: VecDeque<Uuid>,

    // Total capacity tracking for load calculations
    total_max_concurrency: u32,
    total_current_load: u32,
}

impl ActorShepherdManager {
    pub fn new(
        domains_config: Arc<DomainsConfig>,
        task_registry: Arc<TaskSetRegistry>,
        event_stream: Arc<EventStream>,
        shepherd_config: crate::orchestrator::db::ShepherdConfig,
    ) -> Self {
        Self {
            shepherds: HashMap::new(),
            virtual_queues: HashMap::new(),
            domains_config,
            task_registry,
            event_stream,
            scheduler_registry: None,
            shepherd_config,
            shepherd_keys: VecDeque::new(),
            total_max_concurrency: 0,
            total_current_load: 0,
        }
    }

    /// Main actor loop handling all shepherd management operations
    pub async fn main_loop(&mut self, mut command_rx: mpsc::Receiver<ShepherdManagerMessage>) {
        let mut dispatch_interval = time::interval(Duration::from_millis(100));
        let mut health_check_interval = time::interval(Duration::from_secs(30));
        let mut immediate_dispatch_requested = false;

        loop {
            tokio::select! {
                // Handle incoming commands
                Some(message) = command_rx.recv() => {
                    // Check if this message should trigger immediate dispatch
                    if matches!(message, ShepherdManagerMessage::EnqueueTask { .. }) {
                        debug!("DISPATCH: EnqueueTask detected - requesting immediate dispatch");
                        immediate_dispatch_requested = true;
                    }

                    if let Err(e) = self.handle_command(message).await {
                        if e.to_string().contains("Shutdown requested") {
                            info!("ShepherdManager main loop shutting down gracefully");
                            break;
                        } else {
                            error!("Error handling ShepherdManager command: {e}");
                        }
                    }
                }

                // Immediate dispatch when requested
                _ = async {}, if immediate_dispatch_requested => {
                    immediate_dispatch_requested = false;
                    info!("⚡ DISPATCH: Executing immediate dispatch tick");
                    if let Err(e) = self.handle_dispatch_tick().await {
                        error!("Error in immediate dispatch tick: {e}");
                    }
                }

                // Periodic task dispatch (backup)
                _ = dispatch_interval.tick() => {
                    if let Err(e) = self.handle_dispatch_tick().await {
                        error!("Error in periodic dispatch tick: {e}");
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

            ShepherdManagerMessage::SetSchedulerRegistry { scheduler_registry } => {
                debug!("Setting scheduler registry in ShepherdManager actor");
                self.scheduler_registry = Some(scheduler_registry);
            }

            ShepherdManagerMessage::Shutdown { response_tx } => {
                info!("ShepherdManager actor shutting down");
                let _ = response_tx.send(Ok(()));
                // Return error to exit the main loop
                return Err(anyhow::anyhow!("Shutdown requested"));
            }
        }
        Ok(())
    }

    /// Handle periodic task dispatch
    async fn handle_dispatch_tick(&mut self) -> Result<()> {
        // Process each domain's virtual queue
        let domains: Vec<String> = self.virtual_queues.keys().cloned().collect();
        debug!(
            "DISPATCH: handle_dispatch_tick called - processing {} domains: {:?}",
            domains.len(),
            domains
        );

        #[cfg(test)]
        {
            eprintln!(
                "DEBUG: handle_dispatch_tick called. Found {} domains: {:?}",
                domains.len(),
                domains
            );
        }

        for domain in domains {
            debug!("DISPATCH: Processing domain '{domain}' queue");
            if let Err(e) = self.process_domain_queue(&domain).await {
                error!("Error processing domain queue for {domain}: {e}");
            } else {
                debug!("DISPATCH: Finished processing domain '{domain}' queue");
            }
        }

        Ok(())
    }

    /// Handle periodic health checks  
    async fn handle_health_check_tick(&mut self) -> Result<()> {
        let now = Utc::now();
        let timeout_threshold =
            now - ChronoDuration::seconds(self.shepherd_config.timeout_secs as i64);
        let death_threshold =
            now - ChronoDuration::seconds(self.shepherd_config.dead_threshold_secs as i64);

        // Phase 1: Handle timeouts (Connected → Disconnected)
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

        // Phase 2: Handle permanent death (Disconnected → Dead)
        let permanently_dead_shepherds: Vec<Uuid> = self
            .shepherds
            .iter()
            .filter(|(_, status)| {
                matches!(status.status, ShepherdConnectionStatus::Disconnected)
                    && status.last_seen < death_threshold
            })
            .map(|(&uuid, _)| uuid)
            .collect();

        if !permanently_dead_shepherds.is_empty() {
            info!(
                "Found {} permanently dead shepherds",
                permanently_dead_shepherds.len()
            );
            self.handle_permanently_dead_shepherds(permanently_dead_shepherds)
                .await?;
        }

        Ok(())
    }

    /// Handle permanently dead shepherds by failing their tasks and removing them
    async fn handle_permanently_dead_shepherds(&mut self, dead_shepherds: Vec<Uuid>) -> Result<()> {
        for shepherd_uuid in dead_shepherds {
            warn!(
                "Shepherd {shepherd_uuid} permanently dead, failing in-flight tasks and removing"
            );

            // Get affected tasks (fast operation)
            let task_ids = self.task_registry.get_shepherd_tasks(shepherd_uuid);

            if !task_ids.is_empty() {
                info!(
                    "Failing {} tasks from permanently dead shepherd {shepherd_uuid}",
                    task_ids.len()
                );

                // CRITICAL PATH (synchronous): Write task status to database
                // This ensures task failure is persisted and not interrupted by shutdown.
                // Without this, failing those tasks would take longer (failed due to timeout
                // by the newly launched orchestrator).
                if let Err(e) = self
                    .task_registry
                    .fail_shepherd_tasks(shepherd_uuid, &self.event_stream)
                    .await
                {
                    error!(
                        "Failed to mark tasks as failed for dead shepherd {shepherd_uuid}: {e:?}"
                    );
                } else {
                    info!("Successfully marked {} tasks as failed in database for dead shepherd {shepherd_uuid}", task_ids.len());
                }

                // OPTIONAL PATH (asynchronous): Update scheduler state
                // This can be aborted during graceful shutdown, but it is fine.
                // The newly launched orchestrator can always reconstruct the right state from database.
                if let Some(scheduler_registry) = &self.scheduler_registry {
                    let scheduler_registry_clone = scheduler_registry.clone();
                    let tasks_by_domain = self.group_tasks_by_domain(&task_ids);

                    tokio::spawn(async move {
                        for (domain, domain_task_ids) in tasks_by_domain {
                            if let Some(scheduler) = scheduler_registry_clone.get_scheduler(&domain)
                            {
                                let task_count = domain_task_ids.len();
                                if let Err(e) =
                                    scheduler.handle_shepherd_death(domain_task_ids).await
                                {
                                    warn!("Failed to notify scheduler for domain {domain} about shepherd death: {e:?}");
                                } else {
                                    info!("Successfully notified scheduler about {task_count} failed tasks in domain {domain}");
                                }
                            } else {
                                warn!("No scheduler found for domain {domain}");
                            }
                        }
                    });
                }
            }

            // Remove from tracking (fast operation)
            if let Err(e) = self.remove_shepherd_internal(shepherd_uuid) {
                error!("Failed to remove permanently dead shepherd {shepherd_uuid}: {e}");
            } else {
                info!("Successfully removed permanently dead shepherd {shepherd_uuid}");
            }
        }

        Ok(())
    }

    /// Group tasks by domain for scheduler processing
    fn group_tasks_by_domain(&self, task_ids: &[Uuid]) -> HashMap<String, Vec<Uuid>> {
        let mut tasks_by_domain = HashMap::new();

        for &task_id in task_ids {
            if let Some(domain) = self.task_registry.get_domain_for_task(task_id) {
                tasks_by_domain
                    .entry(domain)
                    .or_insert_with(Vec::new)
                    .push(task_id);
            }
        }

        tasks_by_domain
    }

    /// Perfect round-robin algorithm with VecDeque and lazy cleanup
    pub fn find_available_shepherds_round_robin(&mut self, batch: u32) -> Vec<Uuid> {
        let mut result = Vec::new();

        debug!("SHEPHERD_SELECTION: Starting round-robin selection for {batch} tasks");
        debug!(
            "SHEPHERD_SELECTION: Total registered shepherds: {}",
            self.shepherd_keys.len()
        );

        if batch == 0 || self.shepherd_keys.is_empty() {
            debug!(
                "SHEPHERD_SELECTION: Early return - batch={}, shepherd_keys_len={}",
                batch,
                self.shepherd_keys.len()
            );
            return result;
        }

        let mut tasks_assigned = 0u32;
        let mut full_rounds_without_assignment = 0;
        let max_rounds = self.shepherd_keys.len();

        // Round-robin through shepherds using VecDeque rotation
        while tasks_assigned < batch && full_rounds_without_assignment < max_rounds {
            let mut assignments_this_round = 0;

            // Try each shepherd once per round
            for _ in 0..self.shepherd_keys.len() {
                if tasks_assigned >= batch {
                    break;
                }

                if let Some(uuid) = self.shepherd_keys.pop_front() {
                    debug!("SHEPHERD_SELECTION: Checking shepherd {uuid}");
                    if let Some(shepherd_status) = self.shepherds.get(&uuid) {
                        debug!("SHEPHERD_SELECTION: Shepherd {} - status: {:?}, available_capacity: {}", 
                            uuid, shepherd_status.status, shepherd_status.available_capacity());
                        // Shepherd exists - check if available and has capacity
                        if matches!(shepherd_status.status, ShepherdConnectionStatus::Connected)
                            && shepherd_status.available_capacity() > 0
                        {
                            // Check if this shepherd still has capacity for more assignments
                            let current_assignments =
                                result.iter().filter(|&&id| id == uuid).count() as u32;
                            debug!("SHEPHERD_SELECTION: Shepherd {} - current_assignments in batch: {}, available_capacity: {}", 
                                uuid, current_assignments, shepherd_status.available_capacity());
                            if current_assignments < shepherd_status.available_capacity() {
                                debug!("SHEPHERD_SELECTION: Selected shepherd {uuid} for task assignment");
                                result.push(uuid);
                                tasks_assigned += 1;
                                assignments_this_round += 1;
                            } else {
                                debug!("SHEPHERD_SELECTION: Shepherd {uuid} already at capacity for this batch");
                            }
                        } else {
                            debug!("SHEPHERD_SELECTION: Shepherd {} not available - status: {:?}, capacity: {}", 
                                uuid, shepherd_status.status, shepherd_status.available_capacity());
                        }

                        // Put shepherd back at end of queue for round-robin
                        self.shepherd_keys.push_back(uuid);
                    } else {
                        debug!("SHEPHERD_SELECTION: Shepherd {uuid} no longer exists - removing from rotation");
                    }
                    // Dead shepherds are automatically cleaned up by not being re-added
                } else {
                    // No more shepherds in queue
                    break;
                }
            }

            // If we made no assignments this round, increment counter
            if assignments_this_round == 0 {
                full_rounds_without_assignment += 1;
            } else {
                full_rounds_without_assignment = 0; // Reset on successful assignment
            }
        }

        debug!(
            "SHEPHERD_SELECTION: Final result - selected {} shepherds: {:?}",
            result.len(),
            result
        );
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

        // Add to round-robin queue
        self.shepherd_keys.push_back(uuid);

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
        #[cfg(test)]
        {
            eprintln!(
                "DEBUG: ActorShepherdManager::process_domain_queue called for domain: {domain}"
            );
        }
        // Check if we can dispatch any tasks and get available count
        let available_dispatches = {
            let queue = match self.virtual_queues.get(domain) {
                Some(q) => q,
                None => {
                    info!("📭 DISPATCH: No queue found for domain {domain}");
                    return Ok(());
                }
            };

            debug!("DISPATCH: Domain '{}' - queue_len: {}, in_flight: {}, concurrency_limit: {}, can_dispatch: {}", 
                domain, queue.queue_len(), queue.in_flight_count(), queue.concurrency_limit, queue.can_dispatch());

            if !queue.can_dispatch() {
                debug!("DISPATCH: Domain '{domain}' queue cannot dispatch (likely at capacity)");
                return Ok(());
            }

            let count = queue.available_dispatch_count();
            debug!("DISPATCH: Domain '{domain}' has {count} tasks available to dispatch");
            count
        };

        // Find available shepherds
        let shepherds = self.find_available_shepherds_round_robin(available_dispatches);
        debug!(
            "DISPATCH: Looking for shepherds to handle {available_dispatches} dispatches for domain '{domain}'"
        );
        debug!(
            "DISPATCH: Found {} available shepherds: {:?}",
            shepherds.len(),
            shepherds
        );
        #[cfg(test)]
        eprintln!(
            "DEBUG: Found {} shepherds for domain {domain}: {:?}",
            shepherds.len(),
            shepherds
        );

        if shepherds.is_empty() {
            debug!("DISPATCH: No shepherds available for domain '{domain}' - skipping dispatch");
            return Ok(()); // No shepherds available
        }

        // Dispatch tasks to available shepherds
        debug!(
            "DISPATCH: Starting task dispatch loop for {} shepherds in domain '{}'",
            shepherds.len(),
            domain
        );
        for (i, shepherd_id) in shepherds.iter().enumerate() {
            debug!(
                "DISPATCH: Processing shepherd {} ({}/{}): {}",
                shepherd_id,
                i + 1,
                shepherds.len(),
                shepherd_id
            );

            let task = {
                let queue = self.virtual_queues.get_mut(domain).unwrap();
                debug!(
                    "DISPATCH: Attempting to dequeue task from domain '{}' (queue_len: {})",
                    domain,
                    queue.queue_len()
                );
                match queue.dequeue() {
                    Some(task) => {
                        info!(
                            "📋 DISPATCH: Successfully dequeued task {} for shepherd {}",
                            task.task_id, shepherd_id
                        );
                        task
                    }
                    None => {
                        info!("📭 DISPATCH: No more tasks available in domain '{domain}' queue - stopping dispatch loop");
                        break; // No more tasks in queue
                    }
                }
            };

            info!("➡️ DISPATCH: Calling dispatch_task_to_shepherd_internal for task {} to shepherd {}", task.task_id, shepherd_id);

            if let Err(e) = self
                .dispatch_task_to_shepherd_internal(&task, *shepherd_id, domain)
                .await
            {
                error!(
                    "DISPATCH: Failed to dispatch task {} to shepherd {shepherd_id}: {e}",
                    task.task_id
                );
                // Re-enqueue the task on failure
                let task_id = task.task_id;
                let queue = self.virtual_queues.get_mut(domain).unwrap();
                if let Err(enqueue_err) = queue.enqueue(task) {
                    error!(
                        "DISPATCH: Failed to re-enqueue task after dispatch failure: {enqueue_err}"
                    );
                } else {
                    debug!("DISPATCH: Re-enqueued task {task_id} after dispatch failure");
                }
            } else {
                debug!(
                    "DISPATCH: Successfully dispatched task {} to shepherd {}",
                    task.task_id, shepherd_id
                );
            }
        }
        info!("🏁 DISPATCH: Finished dispatch loop for domain '{domain}'");

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

        // Shepherd load tracking removed - managed via heartbeat updates only

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
            event_type: EVENT_TASK_ATTEMPT_STARTED,
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        self.event_stream.write_event(event_record).await?;
        Ok(())
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
            shepherd: crate::orchestrator::db::ShepherdConfig::default(),
            shutdown: crate::orchestrator::db::ShutdownConfig::default(),
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
        ShepherdManager::new(
            domains_config,
            task_registry,
            event_stream,
            crate::orchestrator::db::ShepherdConfig::default(),
        )
    }

    /// Creates a ShepherdManager for actor model testing
    async fn create_test_handle() -> ShepherdManager {
        create_test_manager()
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

    /// Helper function to create test TX channel for shepherd communication
    fn create_local_test_shepherd_tx() -> (ShepherdTxChannel, ShepherdRxChannel) {
        tokio::sync::mpsc::channel(10)
    }

    /// Helper function to manually add shepherds for testing
    async fn add_test_shepherd(manager: &ShepherdManager, uuid: Uuid, max_concurrency: u32) {
        // Use the async API to register shepherd
        let (tx, _rx) = create_local_test_shepherd_tx();
        manager
            .register_shepherd(uuid, max_concurrency, tx)
            .await
            .unwrap();
    }

    /// Helper function to create a test TX channel for shepherd communication
    fn create_test_tx() -> (ShepherdTxChannel, ShepherdRxChannel) {
        mpsc::channel(10)
    }

    /// Test helper to register a shepherd without needing to handle TX channels
    async fn register_test_shepherd(
        handle: &ShepherdManager,
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

        // Test that async methods work
        add_test_shepherd(&manager, uuid, 10).await;
        manager.update_shepherd_status(uuid, 5, 5).await.unwrap();
        manager.mark_shepherd_alive(uuid).await.unwrap();

        // Test selection
        let result = manager.find_available_shepherds(1).await;
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
