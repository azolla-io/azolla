mod common;

use azolla::orchestrator::{
    db::PgPool,
    event_stream::{EventRecord, EventStreamConfig},
};
use chrono::Utc;
use serde_json::json;
use std::time::Duration;
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

// Helper function to count events in database
async fn count_events(pool: &PgPool, domain: &str) -> i64 {
    let client = pool.get().await.unwrap();
    let row = client
        .query_one("SELECT COUNT(*) FROM events WHERE domain = $1", &[&domain])
        .await
        .unwrap();
    row.get(0)
}

db_test!(
    test_low_load_immediate_flush,
    (|pool: PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 10,
            adaptive_threshold: 5,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_stream =
            azolla::orchestrator::event_stream::EventStream::new(pool.clone(), config);

        // Single event should trigger immediate flush (low load)
        let event = create_test_event("low_load_domain", "single_task");
        let result = event_stream.write_event(event).await;

        assert!(result.is_ok(), "Single event write should succeed");

        // Shutdown to ensure all async flushes complete
        event_stream.shutdown().await.unwrap();

        // Verify adaptive batching metrics - should be flush due to low queue
        assert_eq!(
            event_stream.get_flushes_due_to_low_queue(),
            1,
            "Should have 1 flush due to low queue"
        );
        assert_eq!(
            event_stream.get_flushes_due_to_max_batch_size(),
            0,
            "Should not flush due to max batch size"
        );
        assert_eq!(
            event_stream.get_flushes_due_to_timeout(),
            0,
            "Should not flush due to timeout"
        );
        assert_eq!(
            event_stream.get_average_batch_size(),
            1.0,
            "Average batch size should be 1.0"
        );

        // Verify event was written to database
        let count = count_events(&pool, "low_load_domain").await;
        assert_eq!(count, 1, "One event should be written to database");
    })
);

db_test!(
    test_high_load_immediate_batch_flush,
    (|pool: PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 5, // Small batch size for testing
            adaptive_threshold: 3,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 100,
        };

        let event_stream =
            azolla::orchestrator::event_stream::EventStream::new(pool.clone(), config);

        // Send events one at a time to see if batching works properly
        for i in 0..5 {
            let event = create_test_event("high_load_domain", &format!("task_{i}"));
            let result = event_stream.write_event(event).await;
            assert!(result.is_ok(), "Event write should succeed");
        }

        // Shutdown to ensure all async flushes complete
        event_stream.shutdown().await.unwrap();

        // Verify all events were written
        let count = count_events(&pool, "high_load_domain").await;
        assert_eq!(count, 5, "All 5 events should be written");

        // The batching behavior depends on timing - we just verify events were processed
        let total_flushes = event_stream.get_flushes_due_to_low_queue()
            + event_stream.get_flushes_due_to_max_batch_size()
            + event_stream.get_flushes_due_to_timeout();
        assert!(total_flushes >= 1, "Should have at least 1 flush");
        assert!(total_flushes <= 5, "Should not exceed number of events");
    })
);

db_test!(
    test_concurrent_access_batching,
    (|pool: PgPool| async move {
        let config = EventStreamConfig {
            max_batch_size: 8,
            adaptive_threshold: 4,
            max_wait_time: Duration::from_millis(1),
            channel_capacity: 200,
        };

        let event_stream = std::sync::Arc::new(
            azolla::orchestrator::event_stream::EventStream::new(pool.clone(), config),
        );

        // Multiple concurrent writers to test thread safety
        let num_writers = 10;
        let events_per_writer = 5;
        let mut handles = Vec::new();

        for writer_id in 0..num_writers {
            let event_stream_clone = event_stream.clone();
            let handle = tokio::spawn(async move {
                let mut results = Vec::new();
                for event_id in 0..events_per_writer {
                    let event = create_test_event(
                        "concurrent_domain",
                        &format!("writer_{writer_id}_event_{event_id}"),
                    );
                    let result = event_stream_clone.write_event(event).await;
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
        let total_flushes = event_stream.get_flushes_due_to_low_queue()
            + event_stream.get_flushes_due_to_max_batch_size()
            + event_stream.get_flushes_due_to_timeout();

        // Should have fewer flushes than total events (proving batching efficiency)
        assert!(
            total_flushes < total_events as u64,
            "Should have fewer flushes ({total_flushes}) than total events ({total_events})"
        );
        assert!(total_flushes >= 1, "Should have at least 1 flush");

        // Verify average batch size makes sense
        let avg_batch_size = event_stream.get_average_batch_size();
        assert!(
            avg_batch_size >= 1.0,
            "Average batch size should be at least 1.0"
        );
        assert!(
            avg_batch_size <= total_events as f64,
            "Average batch size should not exceed total events"
        );

        // Verify total event count (data consistency)
        let count = count_events(&pool, "concurrent_domain").await;
        assert_eq!(
            count, total_events as i64,
            "All events from concurrent writers should be written"
        );
    })
);
