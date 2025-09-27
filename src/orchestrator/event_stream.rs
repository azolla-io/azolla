use crate::orchestrator::db::PgPool;
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

impl From<&crate::orchestrator::db::EventStream> for EventStreamConfig {
    fn from(config: &crate::orchestrator::db::EventStream) -> Self {
        Self {
            max_batch_size: config.max_batch_size,
            adaptive_threshold: config.adaptive_threshold,
            max_wait_time: Duration::from_millis(config.max_wait_time_ms),
            channel_capacity: config.channel_capacity,
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

        let handle = tokio::spawn(Self::run_adaptive_stream(pool, config, rx, metrics.clone()));

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
        self.metrics
            .flushes_due_to_max_batch_size
            .load(Ordering::Relaxed)
    }

    pub fn get_flushes_due_to_low_queue(&self) -> u64 {
        self.metrics
            .flushes_due_to_low_queue
            .load(Ordering::Relaxed)
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
        eprintln!("Total flushes: {total_flushes}");
        eprintln!(
            "  Due to timeout: {} ({:.1}%)",
            timeout_flushes,
            if total_flushes > 0 {
                (timeout_flushes as f64 / total_flushes as f64) * 100.0
            } else {
                0.0
            }
        );
        eprintln!(
            "  Due to max batch size: {} ({:.1}%)",
            max_batch_flushes,
            if total_flushes > 0 {
                (max_batch_flushes as f64 / total_flushes as f64) * 100.0
            } else {
                0.0
            }
        );
        eprintln!(
            "  Due to low queue: {} ({:.1}%)",
            low_queue_flushes,
            if total_flushes > 0 {
                (low_queue_flushes as f64 / total_flushes as f64) * 100.0
            } else {
                0.0
            }
        );
        eprintln!("Average batch size: {avg_batch_size:.2}");
        eprintln!("======================================");
    }

    fn record_flush_due_to_timeout(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics
            .flushes_due_to_timeout
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .total_events_flushed
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.total_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flush_due_to_max_batch_size(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics
            .flushes_due_to_max_batch_size
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .total_events_flushed
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        metrics.total_flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flush_due_to_low_queue(metrics: &Arc<StreamMetrics>, batch_size: usize) {
        metrics
            .flushes_due_to_low_queue
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .total_events_flushed
            .fetch_add(batch_size as u64, Ordering::Relaxed);
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
                Ok(Some(msg)) => match msg {
                    EventStreamMessage::WriteEvent {
                        event,
                        completion_tx,
                    } => {
                        current_batch.add_event(event, completion_tx);

                        if current_batch.len() >= config.max_batch_size {
                            Self::record_flush_due_to_max_batch_size(&metrics, current_batch.len());

                            Self::flush_batch_async(
                                &mut current_batch,
                                &pool,
                                &mut pending_handles,
                            )
                            .await;
                            last_flush_time = Instant::now();
                        } else if rx.len() < config.adaptive_threshold {
                            Self::record_flush_due_to_low_queue(&metrics, current_batch.len());

                            Self::flush_batch_async(
                                &mut current_batch,
                                &pool,
                                &mut pending_handles,
                            )
                            .await;
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
                },
                Ok(None) => {
                    break;
                }
                Err(_) => {
                    if !current_batch.is_empty()
                        && last_flush_time.elapsed() >= config.max_wait_time
                    {
                        Self::record_flush_due_to_timeout(&metrics, current_batch.len());

                        Self::flush_batch_async(&mut current_batch, &pool, &mut pending_handles)
                            .await;
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
                    let result =
                        Self::write_batch_to_db_with_connection(connection, &batch.events).await;
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

        client
            .execute(
                &stmt,
                &[
                    &domains,
                    &task_ids,
                    &flow_ids,
                    &event_types,
                    &created_ats,
                    &json_metadatas,
                ],
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    // Helper function to create test events
    fn create_test_event(domain: &str, task_name: &str) -> EventRecord {
        EventRecord {
            domain: domain.to_string(),
            task_instance_id: Some(Uuid::new_v4()),
            flow_instance_id: Some(Uuid::new_v4()),
            event_type: 1, // Some test event type
            created_at: Utc::now(),
            metadata: json!({"task": task_name}),
        }
    }

    // Unit tests with no database dependency can go here
    #[test]
    fn test_event_stream_config_default() {
        let config = EventStreamConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.adaptive_threshold, 10);
        assert_eq!(config.max_wait_time, Duration::from_millis(1));
        assert_eq!(config.channel_capacity, 1000);
    }

    #[test]
    fn test_event_batch_operations() {
        let mut batch = EventBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let (tx, _rx) = tokio::sync::oneshot::channel();
        let event = create_test_event("test_domain", "test_task");
        batch.add_event(event, tx);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
    }

    // Database integration tests are in tests/event_stream_tests.rs
}
