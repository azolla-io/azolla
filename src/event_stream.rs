use crate::db::PgPool;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration, Instant};
use tokio_postgres::types::Json;
use uuid::Uuid;

/// Adaptive batching configuration
#[derive(Clone, Debug)]
pub struct EventStreamConfig {
    /// Maximum batch size for high load scenarios
    pub max_batch_size: usize,
    /// Queue depth threshold to trigger batching (events/sec indicator)
    pub adaptive_threshold: usize,
    /// Maximum wait time for additional events (≤1ms)
    pub max_wait_time: Duration,
    /// Channel capacity for burst handling
    pub channel_capacity: usize,
}

impl Default for EventStreamConfig {
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
enum EventStreamMessage {
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

/// Adaptive event stream for high-throughput scenarios
/// - Low load: immediate commits
/// - High load: automatic batching based on queue depth
pub struct EventStream {
    tx: mpsc::Sender<EventStreamMessage>,
    _handle: tokio::task::JoinHandle<()>,
    metrics: Arc<StreamMetrics>,
}

impl Clone for EventStream {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: tokio::spawn(async {}),
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Debug)]
struct StreamMetrics {
    flushes_due_to_timeout: AtomicU64,
    flushes_due_to_max_batch_size: AtomicU64,
    flushes_due_to_low_queue: AtomicU64,
    total_events_flushed: AtomicU64,
    total_flush_count: AtomicU64,
}

impl EventStream {
    pub fn new(pool: PgPool, config: EventStreamConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        
        let metrics = Arc::new(StreamMetrics {
            flushes_due_to_timeout: AtomicU64::new(0),
            flushes_due_to_max_batch_size: AtomicU64::new(0),
            flushes_due_to_low_queue: AtomicU64::new(0),
            total_events_flushed: AtomicU64::new(0),
            total_flush_count: AtomicU64::new(0),
        });
        
        let handle = tokio::spawn(Self::run_adaptive_stream(
            pool, 
            config, 
            rx,
            metrics.clone()
        ));
        
        Self {
            tx,
            _handle: handle,
            metrics,
        }
    }

    /// Write event and return when the containing batch is committed
    pub async fn write_event(&self, event: EventRecord) -> Result<()> {
        let (completion_tx, completion_rx) = oneshot::channel();
        
        self.tx
            .send(EventStreamMessage::WriteEvent {
                event,
                completion_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("EventStream channel closed"))?;
        
        completion_rx
            .await
            .map_err(|_| anyhow::anyhow!("EventStream completion channel closed"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.print_metrics();
        
        self.tx
            .send(EventStreamMessage::Shutdown)
            .await
            .map_err(|_| anyhow::anyhow!("EventStream channel closed"))
    }

    pub fn get_flushes_due_to_timeout(&self) -> u64 {
        self.metrics.flushes_due_to_timeout.load(Ordering::Relaxed)
    }

    pub fn get_flushes_due_to_max_batch_size(&self) -> u64 {
        self.metrics.flushes_due_to_max_batch_size.load(Ordering::Relaxed)
    }

    pub fn get_flushes_due_to_low_queue(&self) -> u64 {
        self.metrics.flushes_due_to_low_queue.load(Ordering::Relaxed)
    }

    pub fn get_average_batch_size(&self) -> f64 {
        let total_events = self.metrics.total_events_flushed.load(Ordering::Relaxed);
        let total_flushes = self.metrics.total_flush_count.load(Ordering::Relaxed);
        if total_flushes == 0 {
            0.0
        } else {
            total_events as f64 / total_flushes as f64
        }
    }

    /// Print metrics summary
    pub fn print_metrics(&self) {
        let timeout_flushes = self.get_flushes_due_to_timeout();
        let max_batch_flushes = self.get_flushes_due_to_max_batch_size();
        let low_queue_flushes = self.get_flushes_due_to_low_queue();
        let avg_batch_size = self.get_average_batch_size();
        let total_flushes = timeout_flushes + max_batch_flushes + low_queue_flushes;
        
        // Use eprintln to ensure metrics are always visible, regardless of log level
        eprintln!("=== EventStream Metrics Summary ===");
        eprintln!("Total flushes: {}", total_flushes);
        eprintln!("  Due to timeout: {} ({:.1}%)", timeout_flushes, 
                  if total_flushes > 0 { (timeout_flushes as f64 / total_flushes as f64) * 100.0 } else { 0.0 });
        eprintln!("  Due to max batch size: {} ({:.1}%)", max_batch_flushes,
                  if total_flushes > 0 { (max_batch_flushes as f64 / total_flushes as f64) * 100.0 } else { 0.0 });
        eprintln!("  Due to low queue: {} ({:.1}%)", low_queue_flushes,
                  if total_flushes > 0 { (low_queue_flushes as f64 / total_flushes as f64) * 100.0 } else { 0.0 });
        eprintln!("Average batch size: {:.2}", avg_batch_size);
        eprintln!("======================================");
    }

    fn record_flush_due_to_timeout(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics.flushes_due_to_timeout.fetch_add(1, Ordering::Relaxed);
        metrics.total_events_flushed.fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.total_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flush_due_to_max_batch_size(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics.flushes_due_to_max_batch_size.fetch_add(1, Ordering::Relaxed);
        metrics.total_events_flushed.fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.total_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flush_due_to_low_queue(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics.flushes_due_to_low_queue.fetch_add(1, Ordering::Relaxed);
        metrics.total_events_flushed.fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.total_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Adaptive batching actor: immediate commits when idle, batching when busy
    async fn run_adaptive_stream(
        pool: PgPool,
        config: EventStreamConfig,
        mut rx: mpsc::Receiver<EventStreamMessage>,
        metrics: Arc<StreamMetrics>,
    ) {
        let mut current_batch = EventBatch::new();
        let mut pending_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut last_flush_time = Instant::now();

        loop {
            let msg_result = timeout(config.max_wait_time, rx.recv()).await;
            
            match msg_result {
                Ok(Some(msg)) => {
                    match msg {
                        EventStreamMessage::WriteEvent { event, completion_tx } => {
                            current_batch.add_event(event, completion_tx);

                            if current_batch.len() >= config.max_batch_size {
                                Self::record_flush_due_to_max_batch_size(&metrics, current_batch.len());
                                
                                Self::flush_batch_async(&mut current_batch, &pool, &mut pending_handles).await;
                                last_flush_time = Instant::now();
                            } else if rx.len() < config.adaptive_threshold {
                                Self::record_flush_due_to_low_queue(&metrics, current_batch.len());
                                
                                Self::flush_batch_async(&mut current_batch, &pool, &mut pending_handles).await;
                                last_flush_time = Instant::now();
                            }
                        }
                        EventStreamMessage::Shutdown => {
                            for handle in pending_handles.drain(..) {
                                let _ = handle.await;
                            }
                            
                            if !current_batch.is_empty() {
                                let batch = std::mem::replace(&mut current_batch, EventBatch::new());
                                
                                Self::flush_batch(&pool, batch).await;
                            }
                            break;
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {
                    if !current_batch.is_empty() && last_flush_time.elapsed() >= config.max_wait_time {
                        Self::record_flush_due_to_timeout(&metrics, current_batch.len());
                        
                        Self::flush_batch_async(&mut current_batch, &pool, &mut pending_handles).await;
                        last_flush_time = Instant::now();
                    }
                }
            }
        }
    }

    /// Helper to flush batch asynchronously
    async fn flush_batch_async(
        current_batch: &mut EventBatch,
        pool: &PgPool,
        pending_handles: &mut Vec<tokio::task::JoinHandle<()>>,
    ) {
        if current_batch.is_empty() {
            return;
        }
        
        let batch = std::mem::replace(current_batch, EventBatch::new());
        
        match pool.get().await {
            Ok(connection) => {
                let handle = tokio::spawn(async move {
                    let result = Self::write_batch_to_db_with_connection(connection, &batch.events).await;
                    batch.notify_completion(&result);
                });
                pending_handles.push(handle);
                
                if pending_handles.len() > 10 {
                    pending_handles.retain(|handle| !handle.is_finished());
                }
            }
            Err(e) => {
                let error_result = Err(anyhow::anyhow!("Failed to get connection: {}", e));
                batch.notify_completion(&error_result);
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
    // TODO: Fix db_test macro import issue
    // use crate::db_test;
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

    /*
    // TODO: Fix db_test macro issue - commenting out tests for now
    db_test!(test_low_load_immediate_flush, (|pool: crate::db::PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 10,
            adaptive_threshold: 5,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_stream = EventStream::new(pool.clone(), config);
        
        // Single event should trigger immediate flush (low load)
        let event = create_test_event("low_load_domain", "single_task");
        let result = event_stream.write_event(event).await;
        
        assert!(result.is_ok(), "Single event write should succeed");
        
        // Shutdown to ensure all async flushes complete
        event_stream.shutdown().await.unwrap();
        
        // Verify adaptive batching metrics - should be flush due to low queue
        assert_eq!(event_stream.get_flushes_due_to_low_queue(), 1, "Should have 1 flush due to low queue");
        assert_eq!(event_stream.get_flushes_due_to_max_batch_size(), 0, "Should not flush due to max batch size");
        assert_eq!(event_stream.get_flushes_due_to_timeout(), 0, "Should not flush due to timeout");
        assert_eq!(event_stream.get_average_batch_size(), 1.0, "Average batch size should be 1.0");
        
        // Verify event was written to database
        let count = count_events(&pool, "low_load_domain").await;
        assert_eq!(count, 1, "One event should be written to database");
    }));

    db_test!(test_high_load_immediate_batch_flush, (|pool: crate::db::PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 5,  // Small batch size for testing
            adaptive_threshold: 3,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_stream = EventStream::new(pool.clone(), config);
        
        // Send events one at a time to see if batching works properly
        for i in 0..5 {
            let event = create_test_event("high_load_domain", &format!("task_{}", i));
            let result = event_stream.write_event(event).await;
            assert!(result.is_ok(), "Event write should succeed");
        }
        
        // Shutdown to ensure all async flushes complete
        event_stream.shutdown().await.unwrap();
        
        // Verify all events were written
        let count = count_events(&pool, "high_load_domain").await;
        assert_eq!(count, 5, "All 5 events should be written");
        
        // The batching behavior depends on timing - we just verify events were processed
        let total_flushes = event_stream.get_flushes_due_to_low_queue() + 
                          event_stream.get_flushes_due_to_max_batch_size() + 
                          event_stream.get_flushes_due_to_timeout();
        assert!(total_flushes >= 1, "Should have at least 1 flush");
        assert!(total_flushes <= 5, "Should not exceed number of events");
        assert_eq!(event_stream.get_average_batch_size(), 1.0, "Average batch size should be 1.0 for individual events");
    }));

    db_test!(test_concurrent_access_batching, (|pool: crate::db::PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 8,
            adaptive_threshold: 4,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 200,
        };

        let event_stream = EventStream::new(pool.clone(), config);
        
        // Multiple concurrent writers to test thread safety
        let num_writers = 10;
        let events_per_writer = 5;
        let mut handles = Vec::new();
        
        for writer_id in 0..num_writers {
            let buffer = event_stream.clone();
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
        event_stream.shutdown().await.unwrap();
        
        // Verify efficient batching occurred
        let total_events = num_writers * events_per_writer;
        let total_flushes = event_stream.get_flushes_due_to_low_queue() + 
                          event_stream.get_flushes_due_to_max_batch_size() + 
                          event_stream.get_flushes_due_to_timeout();
        
        // Should have fewer flushes than total events (proving batching efficiency)
        assert!(total_flushes < total_events as u64, 
                "Should have fewer flushes ({}) than total events ({})", total_flushes, total_events);
        assert!(total_flushes >= 1, "Should have at least 1 flush");
        
        // Verify average batch size makes sense
        let avg_batch_size = event_stream.get_average_batch_size();
        assert!(avg_batch_size >= 1.0, "Average batch size should be at least 1.0");
        assert!(avg_batch_size <= total_events as f64, "Average batch size should not exceed total events");
        
        // Verify total event count (data consistency)
        let count = count_events(&pool, "concurrent_domain").await;
        assert_eq!(count, total_events as i64, 
                   "All events from concurrent writers should be written");
    }));
    */
}