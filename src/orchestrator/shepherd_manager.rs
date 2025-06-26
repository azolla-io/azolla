use anyhow::Result;
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use dashmap::DashMap;
use std::time::Duration;
use uuid::Uuid;
use tokio::time;
use log::{info, warn, error};
use std::sync::Arc;

use crate::taskset::TaskSetRegistry;
use crate::event_stream::EventStream;
use crate::EVENT_SHEPHERD_REGISTERED;

const SHEPHERD_TIMEOUT_SECS: u64 = 300; // 5 minutes

#[derive(Debug, Clone)]
pub enum ShepherdConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting,
}

#[derive(Debug, Clone)]
pub struct ShepherdStatus {
    pub uuid: Uuid,
    pub max_concurrency: u32,
    pub current_load: u32,
    pub last_seen: DateTime<Utc>,
    pub connected_at: DateTime<Utc>,
    pub status: ShepherdConnectionStatus,
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
        }
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
            warn!("Shepherd {} reported inconsistent capacity: current={}, available={}, max={}", 
                  self.uuid, current_load, available_capacity, self.max_concurrency);
        }
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
    
    /// Register a new shepherd connection
    pub async fn register_shepherd(&self, uuid: Uuid, max_concurrency: u32) -> Result<()> {
        let shepherd_status = ShepherdStatus::new(uuid, max_concurrency);
        
        info!("Registering shepherd {} with max_concurrency={}", uuid, max_concurrency);
        
        // Create EVENT_SHEPHERD_REGISTERED event
        let event_metadata = serde_json::json!({
            "shepherd_uuid": uuid,
            "max_concurrency": max_concurrency,
            "registered_at": Utc::now()
        });
        
        let event_record = crate::event_stream::EventRecord {
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
        
        info!("Successfully registered shepherd {}", uuid);
        Ok(())
    }
    
    /// Update shepherd status (heartbeat)
    pub fn update_shepherd_status(&self, uuid: Uuid, current_load: u32, available_capacity: u32) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.update_load(current_load, available_capacity);
        } else {
            warn!("Received status update from unknown shepherd {}", uuid);
        }
    }
    
    /// Mark shepherd as having sent a message (for liveness tracking)
    pub fn mark_shepherd_alive(&self, uuid: Uuid) {
        if let Some(mut shepherd) = self.shepherds.get_mut(&uuid) {
            shepherd.update_last_seen();
        }
    }
    
    /// Get shepherd status
    pub fn get_shepherd(&self, uuid: Uuid) -> Option<ShepherdStatus> {
        self.shepherds.get(&uuid).map(|s| s.clone())
    }
    
    /// List all connected shepherds
    pub fn list_shepherds(&self) -> Vec<ShepherdStatus> {
        self.shepherds
            .iter()
            .filter(|entry| matches!(entry.value().status, ShepherdConnectionStatus::Connected))
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Find the best shepherd for task dispatch based on load
    pub fn find_best_shepherd(&self) -> Option<Uuid> {
        let mut best_shepherd: Option<(Uuid, f64)> = None;
        
        for entry in self.shepherds.iter() {
            let shepherd = entry.value();
            
            // Only consider connected shepherds with available capacity
            if matches!(shepherd.status, ShepherdConnectionStatus::Connected) && shepherd.available_capacity() > 0 {
                let load_ratio = shepherd.load_ratio();
                
                match best_shepherd {
                    None => best_shepherd = Some((shepherd.uuid, load_ratio)),
                    Some((_, best_ratio)) => {
                        if load_ratio < best_ratio {
                            best_shepherd = Some((shepherd.uuid, load_ratio));
                        }
                    }
                }
            }
        }
        
        best_shepherd.map(|(uuid, _)| uuid)
    }
    
    /// Remove shepherd when it disconnects
    pub fn remove_shepherd(&self, uuid: Uuid) {
        if self.shepherds.remove(&uuid).is_some() {
            info!("Removed shepherd {} from tracking", uuid);
        }
    }
    
    /// Start the health checker background task
    pub async fn start_health_checker(self: Arc<Self>) {
        let mut interval = time::interval(Duration::from_secs(60)); // Check every minute
        
        loop {
            interval.tick().await;
            if let Err(e) = self.check_shepherd_health().await {
                error!("Error during shepherd health check: {}", e);
            }
        }
    }
    
    /// Check all shepherds for timeout and fail their tasks
    async fn check_shepherd_health(&self) -> Result<()> {
        let now = Utc::now();
        let timeout = ChronoDuration::seconds(SHEPHERD_TIMEOUT_SECS as i64);
        let mut dead_shepherds = Vec::new();
        
        // Find dead shepherds
        for entry in self.shepherds.iter() {
            let shepherd_uuid = *entry.key();
            let shepherd = entry.value();
            
            if matches!(shepherd.status, ShepherdConnectionStatus::Connected) {
                let time_since_last_seen = now.signed_duration_since(shepherd.last_seen);
                
                if time_since_last_seen > timeout {
                    warn!("Shepherd {} detected as dead (last seen {} seconds ago)", 
                          shepherd_uuid, time_since_last_seen.num_seconds());
                    dead_shepherds.push(shepherd_uuid);
                }
            }
        }
        
        // Process dead shepherds
        for shepherd_uuid in dead_shepherds {
            // Mark as disconnected
            if let Some(mut shepherd) = self.shepherds.get_mut(&shepherd_uuid) {
                shepherd.status = ShepherdConnectionStatus::Disconnected;
            }
            
            // Fail all tasks assigned to this shepherd
            if let Err(e) = self.task_registry.fail_shepherd_tasks(shepherd_uuid, &self.event_stream).await {
                error!("Failed to fail tasks for dead shepherd {}: {}", shepherd_uuid, e);
            }
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
}

#[derive(Debug, Clone)]
pub struct ShepherdManagerStats {
    pub connected_shepherds: usize,
    pub disconnected_shepherds: usize,
    pub total_capacity: u32,
    pub total_load: u32,
    pub utilization: f64,
}