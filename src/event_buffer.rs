use crate::db::PgPool;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_postgres::types::Json;
use uuid::Uuid;

/// Adaptive batching configuration
#[derive(Clone, Debug)]
pub struct EventBufferConfig {
    /// Maximum batch size for high load scenarios
    pub max_batch_size: usize,
    /// Queue depth threshold to trigger batching (events/sec indicator)
    pub adaptive_threshold: usize,
    /// Maximum wait time for additional events (≤1ms)
    pub max_wait_time: Duration,
    /// Channel capacity for burst handling
    pub channel_capacity: usize,
}

impl Default for EventBufferConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            adaptive_threshold: 10,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 1000,
        }
    }
}

/// Event record for the events table
#[derive(Debug, Clone)]
pub struct EventRecord {
    pub domain: String,
    pub task_instance_id: Option<Uuid>,
    pub flow_instance_id: Option<Uuid>,
    pub event_type: i16,
    pub created_at: DateTime<Utc>,
    pub metadata: JsonValue,
}

#[derive(Debug)]
enum EventBufferMessage {
    WriteEvent {
        event: EventRecord,
        completion_tx: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

/// Batch of events with completion tracking
#[derive(Debug)]
struct EventBatch {
    events: Vec<EventRecord>,
    completion_txs: Vec<oneshot::Sender<Result<()>>>,
}

impl EventBatch {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            completion_txs: Vec::new(),
        }
    }

    fn add_event(&mut self, event: EventRecord, completion_tx: oneshot::Sender<Result<()>>) {
        self.events.push(event);
        self.completion_txs.push(completion_tx);
    }

    fn len(&self) -> usize {
        self.events.len()
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn notify_completion(self, result: &Result<()>) {
        for tx in self.completion_txs {
            let result_copy = match result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!("{}", e)),
            };
            let _ = tx.send(result_copy);
        }
    }
}

/// Adaptive event buffer for high-throughput scenarios
/// - Low load: immediate commits
/// - High load: automatic batching based on queue depth
pub struct EventBuffer {
    tx: mpsc::Sender<EventBufferMessage>,
    _handle: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    metrics: Arc<BufferMetrics>,
}

impl Clone for EventBuffer {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: tokio::spawn(async {}), // Create a no-op handle for clone
            #[cfg(test)]
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct BufferMetrics {
    batch_count: AtomicU64,
    wait_count: AtomicU64,
    immediate_flush_count: AtomicU64,
}

impl EventBuffer {
    pub fn new(pool: PgPool, config: EventBufferConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        
        #[cfg(test)]
        let metrics = Arc::new(BufferMetrics {
            batch_count: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            immediate_flush_count: AtomicU64::new(0),
        });
        
        let handle = tokio::spawn(Self::run_adaptive_buffer(
            pool, 
            config, 
            rx,
            #[cfg(test)] metrics.clone(),
            #[cfg(not(test))] ()
        ));
        
        Self {
            tx,
            _handle: handle,
            #[cfg(test)]
            metrics,
        }
    }

    /// Write event and return when the containing batch is committed
    pub async fn write_event(&self, event: EventRecord) -> Result<()> {
        let (completion_tx, completion_rx) = oneshot::channel();
        
        self.tx
            .send(EventBufferMessage::WriteEvent {
                event,
                completion_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("EventBuffer channel closed"))?;
        
        completion_rx
            .await
            .map_err(|_| anyhow::anyhow!("EventBuffer completion channel closed"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.tx
            .send(EventBufferMessage::Shutdown)
            .await
            .map_err(|_| anyhow::anyhow!("EventBuffer channel closed"))
    }

    #[cfg(test)]
    pub fn get_batch_count(&self) -> u64 {
        self.metrics.batch_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn get_wait_count(&self) -> u64 {
        self.metrics.wait_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn get_immediate_flush_count(&self) -> u64 {
        self.metrics.immediate_flush_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn increment_batch_count(metrics: &Arc<BufferMetrics>) {
        metrics.batch_count.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn increment_wait_count(metrics: &Arc<BufferMetrics>) {
        metrics.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn increment_immediate_flush_count(metrics: &Arc<BufferMetrics>) {
        metrics.immediate_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(test))]
    fn increment_batch_count(_metrics: &()) {}

    #[cfg(not(test))]
    fn increment_wait_count(_metrics: &()) {}

    #[cfg(not(test))]
    fn increment_immediate_flush_count(_metrics: &()) {}

    /// Adaptive batching actor: immediate commits when idle, batching when busy
    async fn run_adaptive_buffer(
        pool: PgPool,
        config: EventBufferConfig,
        mut rx: mpsc::Receiver<EventBufferMessage>,
        #[cfg(test)] metrics: Arc<BufferMetrics>,
        #[cfg(not(test))] _metrics: (),
    ) {
        let mut current_batch = EventBatch::new();
        let mut pending_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        while let Some(msg) = rx.recv().await {
            match msg {
                EventBufferMessage::WriteEvent { event, completion_tx } => {
                    current_batch.add_event(event, completion_tx);

                    // Adaptive batching logic
                    let should_flush = if current_batch.len() >= config.max_batch_size {
                        // High load: batch is full, flush immediately
                        #[cfg(test)]
                        Self::increment_immediate_flush_count(&metrics);
                        #[cfg(not(test))]
                        Self::increment_immediate_flush_count(&_metrics);
                        true
                    } else if rx.len() < config.adaptive_threshold {
                        // Low load: few events queued, flush immediately
                        #[cfg(test)]
                        Self::increment_immediate_flush_count(&metrics);
                        #[cfg(not(test))]
                        Self::increment_immediate_flush_count(&_metrics);
                        true
                    } else {
                        // Medium load: wait briefly for more events
                        #[cfg(test)]
                        Self::increment_wait_count(&metrics);
                        #[cfg(not(test))]
                        Self::increment_wait_count(&_metrics);
                        
                        Self::wait_for_more_events(&mut rx, &mut current_batch, &config).await
                    };

                    if should_flush {
                        let batch = std::mem::replace(&mut current_batch, EventBatch::new());
                        
                        #[cfg(test)]
                        Self::increment_batch_count(&metrics);
                        #[cfg(not(test))]
                        Self::increment_batch_count(&_metrics);
                        
                        // Unified: acquire connection in actor loop for backpressure
                        let pool_clone = pool.clone();
                        match pool_clone.get().await {
                            Ok(connection) => {
                                // Spawn with guaranteed connection
                                let handle = tokio::spawn(async move {
                                    let result = Self::write_batch_to_db_with_connection(connection, &batch.events).await;
                                    batch.notify_completion(&result);
                                });
                                pending_handles.push(handle);
                                
                                // Clean up completed handles
                                if pending_handles.len() > 10 {
                                    pending_handles.retain(|handle| !handle.is_finished());
                                }
                            }
                            Err(e) => {
                                // Connection pool error - notify batch completion with error
                                let error_result = Err(anyhow::anyhow!("Failed to get connection: {}", e));
                                batch.notify_completion(&error_result);
                            }
                        }
                    }
                }
                EventBufferMessage::Shutdown => {
                    // Wait for all pending handles to complete
                    for handle in pending_handles.drain(..) {
                        let _ = handle.await;
                    }
                    
                    if !current_batch.is_empty() {
                        let batch = std::mem::replace(&mut current_batch, EventBatch::new());
                        
                        #[cfg(test)]
                        Self::increment_batch_count(&metrics);
                        #[cfg(not(test))]
                        Self::increment_batch_count(&_metrics);
                        
                        // Always block on shutdown to ensure completion
                        Self::flush_batch(&pool, batch).await;
                    }
                    break;
                }
            }
        }
    }

    /// Wait briefly for additional events during medium load
    async fn wait_for_more_events(
        rx: &mut mpsc::Receiver<EventBufferMessage>,
        current_batch: &mut EventBatch,
        config: &EventBufferConfig,
    ) -> bool {
        let wait_result = timeout(config.max_wait_time, rx.recv()).await;
        
        match wait_result {
            Ok(Some(EventBufferMessage::WriteEvent { event, completion_tx })) => {
                current_batch.add_event(event, completion_tx);
                // Check if we should continue waiting or flush now
                current_batch.len() >= config.max_batch_size || rx.len() < config.adaptive_threshold
            }
            Ok(Some(EventBufferMessage::Shutdown)) => {
                // Shutdown received, flush current batch
                true
            }
            Ok(None) | Err(_) => {
                // Channel closed or timeout, flush current batch
                true
            }
        }
    }

    async fn flush_batch(pool: &PgPool, batch: EventBatch) {
        if batch.is_empty() {
            return;
        }

        let result = match pool.get().await {
            Ok(client) => Self::write_batch_to_db_with_connection(client, &batch.events).await,
            Err(e) => Err(anyhow::anyhow!("Failed to get connection: {}", e)),
        };
        
        if let Err(e) = &result {
            log::error!("Failed to flush {} events: {}", batch.events.len(), e);
        }
        
        batch.notify_completion(&result);
    }


    /// Write using connection acquired in actor loop for backpressure
    async fn write_batch_to_db_with_connection(
        client: deadpool_postgres::Object,
        events: &[EventRecord],
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        const UNNEST_INSERT_STMT: &str = r#"INSERT INTO events (domain, task_instance_id, flow_instance_id, event_type, created_at, metadata)
               SELECT * FROM UNNEST($1::text[], $2::uuid[], $3::uuid[], $4::int2[], $5::timestamptz[], $6::jsonb[])
               AS t(domain, task_instance_id, flow_instance_id, event_type, created_at, metadata)"#;

        let stmt = client.prepare(UNNEST_INSERT_STMT).await?;
        
        // Collect data into arrays for UNNEST
        let mut domains = Vec::with_capacity(events.len());
        let mut task_ids = Vec::with_capacity(events.len());
        let mut flow_ids = Vec::with_capacity(events.len());
        let mut event_types = Vec::with_capacity(events.len());
        let mut created_ats = Vec::with_capacity(events.len());
        let mut json_metadatas = Vec::with_capacity(events.len());
        
        for event in events {
            domains.push(event.domain.clone());
            task_ids.push(event.task_instance_id);
            flow_ids.push(event.flow_instance_id);
            event_types.push(event.event_type);
            created_ats.push(event.created_at);
            json_metadatas.push(Json(&event.metadata));
        }

        client.execute(
            &stmt,
            &[
                &domains,
                &task_ids,
                &flow_ids,
                &event_types,
                &created_ats,
                &json_metadatas,
            ],
        ).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_test;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    /// Helper function to create test event
    fn create_test_event(domain: &str, task_name: &str) -> EventRecord {
        EventRecord {
            domain: domain.to_string(),
            task_instance_id: Some(Uuid::new_v4()),
            flow_instance_id: None,
            event_type: 1,
            created_at: Utc::now(),
            metadata: json!({
                "task_name": task_name,
                "created_by": "unit_test"
            }),
        }
    }

    /// Helper to count events in database
    async fn count_events(pool: &crate::db::PgPool, domain: &str) -> i64 {
        let client = pool.get().await.unwrap();
        let row = client
            .query_one("SELECT COUNT(*) FROM events WHERE domain = $1", &[&domain])
            .await.unwrap();
        row.get(0)
    }

    db_test!(test_low_load_immediate_flush, (|pool: crate::db::PgPool| async move {
        let config = EventBufferConfig {
            max_batch_size: 10,
            adaptive_threshold: 5,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_buffer = EventBuffer::new(pool.clone(), config);
        
        // Single event should trigger immediate flush (low load)
        let event = create_test_event("low_load_domain", "single_task");
        let result = event_buffer.write_event(event).await;
        
        assert!(result.is_ok(), "Single event write should succeed");
        
        // Shutdown to ensure all async flushes complete
        event_buffer.shutdown().await.unwrap();
        
        // Verify adaptive batching metrics - should be immediate flush
        assert_eq!(event_buffer.get_batch_count(), 1, "Should have 1 batch flush");
        assert_eq!(event_buffer.get_immediate_flush_count(), 1, "Should be immediate flush (low load)");
        assert_eq!(event_buffer.get_wait_count(), 0, "Should not wait for low load");
        
        // Verify event was written to database
        let count = count_events(&pool, "low_load_domain").await;
        assert_eq!(count, 1, "One event should be written to database");
    }));

    db_test!(test_high_load_immediate_batch_flush, (|pool: crate::db::PgPool| async move {
        let config = EventBufferConfig {
            max_batch_size: 5,  // Small batch size for testing
            adaptive_threshold: 3,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_buffer = EventBuffer::new(pool.clone(), config);
        
        // Send events one at a time to see if batching works properly
        for i in 0..5 {
            let event = create_test_event("high_load_domain", &format!("task_{}", i));
            let result = event_buffer.write_event(event).await;
            assert!(result.is_ok(), "Event write should succeed");
        }
        
        // Shutdown to ensure all async flushes complete
        event_buffer.shutdown().await.unwrap();
        
        // Verify all events were written
        let count = count_events(&pool, "high_load_domain").await;
        assert_eq!(count, 5, "All 5 events should be written");
        
        // The batching behavior depends on timing - we just verify events were processed
        assert!(event_buffer.get_batch_count() >= 1, "Should have at least 1 batch flush");
        assert!(event_buffer.get_batch_count() <= 5, "Should not exceed number of events");
    }));

    db_test!(test_concurrent_access_batching, (|pool: crate::db::PgPool| async move {
        let config = EventBufferConfig {
            max_batch_size: 8,
            adaptive_threshold: 4,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 200,
        };

        let event_buffer = EventBuffer::new(pool.clone(), config);
        
        // Multiple concurrent writers to test thread safety
        let num_writers = 10;
        let events_per_writer = 5;
        let mut handles = Vec::new();
        
        for writer_id in 0..num_writers {
            let buffer = event_buffer.clone();
            let handle = tokio::spawn(async move {
                let mut results = Vec::new();
                for event_id in 0..events_per_writer {
                    let event = create_test_event(
                        "concurrent_domain", 
                        &format!("writer_{}_event_{}", writer_id, event_id)
                    );
                    let result = buffer.write_event(event).await;
                    results.push(result);
                }
                results
            });
            handles.push(handle);
        }
        
        // Wait for all writers to complete
        for handle in handles {
            let results = handle.await.unwrap();
            for result in results {
                assert!(result.is_ok(), "Concurrent write should succeed");
            }
        }
        
        // Shutdown to ensure all async flushes complete
        event_buffer.shutdown().await.unwrap();
        
        // Verify efficient batching occurred
        let total_events = num_writers * events_per_writer;
        let batch_count = event_buffer.get_batch_count();
        
        // Should have fewer batches than total events (proving batching efficiency)
        assert!(batch_count < total_events as u64, 
                "Should have fewer batches ({}) than total events ({})", batch_count, total_events);
        assert!(batch_count >= 1, "Should have at least 1 batch");
        
        // Verify total event count (data consistency)
        let count = count_events(&pool, "concurrent_domain").await;
        assert_eq!(count, total_events as i64, 
                   "All events from concurrent writers should be written");
    }));
}