mod common;
use azolla::orchestrator::{db::PgPool, taskset::TaskSetRegistry};
use chrono::Utc;
use serde_json::json;
use tokio_postgres::types::Json;
use uuid::Uuid;

// Helper to insert a task_instance
async fn insert_task_instance(pool: &PgPool, id: Uuid, name: &str, domain: &str, status: i16) {
    let client = pool.get().await.unwrap();
    client.execute(
        "INSERT INTO task_instance (id, name, domain, created_at, flow_instance_id, retry_policy, args, kwargs, status) \
         VALUES ($1, $2, $3, $4, NULL, '{}'::jsonb, ARRAY[]::text[], '{}'::jsonb, $5)",
        &[&id, &name, &domain, &Utc::now(), &status],
    ).await.unwrap();
}

// Helper to insert a task_attempt
async fn insert_task_attempt(
    pool: &PgPool,
    task_instance_id: Uuid,
    domain: &str,
    attempt: i32,
    status: i16,
) {
    let client = pool.get().await.unwrap();
    client.execute(
        "INSERT INTO task_attempts (task_instance_id, domain, attempt, start_time, end_time, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[&task_instance_id, &domain, &attempt, &Utc::now(), &Utc::now(), &status],
    ).await.unwrap();
}

db_test!(
    test_load_from_db_structure,
    (|pool| async move {
        // Create UUIDs for test data
        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();
        let task3_id = Uuid::new_v4();

        // Insert dummy data for two domains
        // domain_a: task 1 (2 attempts), task 2 (0 attempts)
        // domain_b: task 3 (1 attempt)
        insert_task_instance(&pool, task1_id, "task1", "domain_a", 10).await;
        insert_task_instance(&pool, task2_id, "task2", "domain_a", 20).await;
        insert_task_instance(&pool, task3_id, "task3", "domain_b", 30).await;

        insert_task_attempt(&pool, task1_id, "domain_a", 1, 100).await;
        insert_task_attempt(&pool, task1_id, "domain_a", 2, 101).await;
        insert_task_attempt(&pool, task3_id, "domain_b", 1, 200).await;

        // Load from DB
        let registry = TaskSetRegistry::new();
        registry.load_from_db(&pool).await.unwrap();

        // Check structure
        let mut domains = registry.domains();
        domains.sort();
        assert_eq!(domains, vec!["domain_a", "domain_b"]);

        // domain_a
        let domain_a = registry.get_domain("domain_a").unwrap();
        let tasks_a = domain_a.all_tasks().await.unwrap();
        let mut tasks_a_ids: Vec<_> = tasks_a.iter().map(|t| t.id).collect();
        tasks_a_ids.sort();
        let mut expected_a = vec![task1_id, task2_id];
        expected_a.sort();
        assert_eq!(tasks_a_ids, expected_a);
        let task1 = domain_a.get_task(task1_id).await.unwrap().unwrap();
        assert_eq!(task1.attempts.len(), 2);
        let task2 = domain_a.get_task(task2_id).await.unwrap().unwrap();
        assert_eq!(task2.attempts.len(), 0);

        // domain_b
        let domain_b = registry.get_domain("domain_b").unwrap();
        let tasks_b = domain_b.all_tasks().await.unwrap();
        let tasks_b_ids: Vec<_> = tasks_b.iter().map(|t| t.id).collect();
        assert_eq!(tasks_b_ids, vec![task3_id]);
        let task3 = domain_b.get_task(task3_id).await.unwrap().unwrap();
        assert_eq!(task3.attempts.len(), 1);
    })
);

db_test!(
    test_merge_events_to_db_basic,
    (|pool: PgPool| async move {
        let registry = TaskSetRegistry::new();
        let domain = "test_domain";
        let task_id = Uuid::new_v4();

        let client = pool.get().await.unwrap();
        let base_time = Utc::now();

        // Insert a simple task creation event
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task_id, &1i16, &base_time, &Json(&json!({
            "task_name": "test_task",
            "retry_policy": {},
            "args": ["arg1"],
            "kwargs": {"key": "value"}
        }))]
    ).await.unwrap();

        drop(client);

        // Run the merge
        registry.merge_events_to_db(&pool).await.unwrap();

        // Load from database into registry
        registry.load_from_db(&pool).await.unwrap();

        // Verify the task was created
        let domain_ref = registry.get_domain(domain).unwrap();
        let task = domain_ref.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(task.name, "test_task");

        // Check merge_cursor was updated
        let client = pool.get().await.unwrap();
        let cursor_rows = client
            .query("SELECT last_event_id FROM merge_cursor", &[])
            .await
            .unwrap();
        assert_eq!(cursor_rows.len(), 1);

        println!("✅ Basic merge_events_to_db test passed!");
    })
);

db_test!(
    test_merge_events_to_db_comprehensive,
    (|pool: PgPool| async move {
        let registry = TaskSetRegistry::new();
        let domain = "test_domain";

        // Generate UUIDs for 6 test tasks
        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();
        let task3_id = Uuid::new_v4();
        let task4_id = Uuid::new_v4();
        let task5_id = Uuid::new_v4();
        let task6_id = Uuid::new_v4();

        let client = pool.get().await.unwrap();
        let base_time = Utc::now();

        // Case 1: Task 1 created only
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task1_id, &1i16, &(base_time + chrono::Duration::seconds(1)), &Json(&json!({
            "task_name": "task1",
            "retry_policy": {},
            "args": ["arg1"],
            "kwargs": {"key": "value1"}
        }))]
    ).await.unwrap();

        // Case 2: Task 2 created, Task 2 started
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task2_id, &1i16, &(base_time + chrono::Duration::seconds(2)), &Json(&json!({
            "task_name": "task2", 
            "retry_policy": {},
            "args": ["arg2"],
            "kwargs": {"key": "value2"}
        }))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task2_id, &2i16, &(base_time + chrono::Duration::seconds(3)), &Json(&json!({}))]
    ).await.unwrap();

        // Case 3: Task 3 created, Task 3 started, one attempt started
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task3_id, &1i16, &(base_time + chrono::Duration::seconds(4)), &Json(&json!({
            "task_name": "task3",
            "retry_policy": {},
            "args": ["arg3"],
            "kwargs": {"key": "value3"}
        }))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task3_id, &2i16, &(base_time + chrono::Duration::seconds(5)), &Json(&json!({}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task3_id, &4i16, &(base_time + chrono::Duration::seconds(6)), &Json(&json!({"attempt": 1}))]
    ).await.unwrap();

        // Case 4: Task 4 created, Task 4 started, one attempt started and ended
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task4_id, &1i16, &(base_time + chrono::Duration::seconds(7)), &Json(&json!({
            "task_name": "task4",
            "retry_policy": {},
            "args": ["arg4"],
            "kwargs": {"key": "value4"}
        }))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task4_id, &2i16, &(base_time + chrono::Duration::seconds(8)), &Json(&json!({}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task4_id, &4i16, &(base_time + chrono::Duration::seconds(9)), &Json(&json!({"attempt": 1}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task4_id, &5i16, &(base_time + chrono::Duration::seconds(10)), &Json(&json!({"attempt": 1, "status": 1}))]
    ).await.unwrap();

        // Case 5: Task 5 created, Task 5 started, one attempt started and ended, task 5 ended
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task5_id, &1i16, &(base_time + chrono::Duration::seconds(11)), &Json(&json!({
            "task_name": "task5",
            "retry_policy": {},
            "args": ["arg5"],
            "kwargs": {"key": "value5"}
        }))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task5_id, &2i16, &(base_time + chrono::Duration::seconds(12)), &Json(&json!({}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task5_id, &4i16, &(base_time + chrono::Duration::seconds(13)), &Json(&json!({"attempt": 1}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task5_id, &5i16, &(base_time + chrono::Duration::seconds(14)), &Json(&json!({"attempt": 1, "status": 1}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task5_id, &3i16, &(base_time + chrono::Duration::seconds(15)), &Json(&json!({}))]
    ).await.unwrap();

        // Case 6: Task 6 created, Task 6 started, two attempts started and ended, task 6 ended
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &1i16, &(base_time + chrono::Duration::seconds(16)), &Json(&json!({
            "task_name": "task6",
            "retry_policy": {},
            "args": ["arg6"],
            "kwargs": {"key": "value6"}
        }))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &2i16, &(base_time + chrono::Duration::seconds(17)), &Json(&json!({}))]
    ).await.unwrap();
        // First attempt
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &4i16, &(base_time + chrono::Duration::seconds(18)), &Json(&json!({"attempt": 1}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &5i16, &(base_time + chrono::Duration::seconds(19)), &Json(&json!({"attempt": 1, "status": 2}))]
    ).await.unwrap();
        // Second attempt
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &4i16, &(base_time + chrono::Duration::seconds(20)), &Json(&json!({"attempt": 2}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &5i16, &(base_time + chrono::Duration::seconds(21)), &Json(&json!({"attempt": 2, "status": 1}))]
    ).await.unwrap();
        client.execute(
        "INSERT INTO events (domain, task_instance_id, event_type, created_at, metadata) VALUES ($1, $2, $3, $4, $5)",
        &[&domain, &task6_id, &3i16, &(base_time + chrono::Duration::seconds(22)), &Json(&json!({}))]
    ).await.unwrap();

        drop(client);

        // Run the merge
        registry.merge_events_to_db(&pool).await.unwrap();

        // Load from database into registry
        registry.load_from_db(&pool).await.unwrap();

        // Verify the results
        let domain_ref = registry.get_domain(domain).unwrap();

        // Case 1: Task 1 - only created
        let task1 = domain_ref.get_task(task1_id).await.unwrap().unwrap();
        assert_eq!(task1.name, "task1");
        assert_eq!(task1.attempts.len(), 0);

        // Case 2: Task 2 - created and started
        let task2 = domain_ref.get_task(task2_id).await.unwrap().unwrap();
        assert_eq!(task2.name, "task2");
        assert_eq!(task2.attempts.len(), 0);

        // Case 3: Task 3 - created, started, one attempt started
        let task3 = domain_ref.get_task(task3_id).await.unwrap().unwrap();
        assert_eq!(task3.name, "task3");
        assert_eq!(task3.attempts.len(), 1);
        let attempt3_1 = &task3.attempts[0];
        assert_eq!(attempt3_1.attempt, 1);
        assert!(attempt3_1.start_time.is_some());
        assert!(attempt3_1.end_time.is_none());

        // Case 4: Task 4 - created, started, one attempt started and ended
        let task4 = domain_ref.get_task(task4_id).await.unwrap().unwrap();
        assert_eq!(task4.name, "task4");
        assert_eq!(task4.attempts.len(), 1);
        let attempt4_1 = &task4.attempts[0];
        assert_eq!(attempt4_1.attempt, 1);
        assert!(attempt4_1.start_time.is_some());
        assert!(attempt4_1.end_time.is_some());
        assert_eq!(attempt4_1.status, 1);

        // Case 5: Task 5 - created, started, one attempt started and ended, task ended
        let task5 = domain_ref.get_task(task5_id).await.unwrap().unwrap();
        assert_eq!(task5.name, "task5");
        assert_eq!(task5.attempts.len(), 1);
        let attempt5_1 = &task5.attempts[0];
        assert_eq!(attempt5_1.attempt, 1);
        assert!(attempt5_1.start_time.is_some());
        assert!(attempt5_1.end_time.is_some());
        assert_eq!(attempt5_1.status, 1);

        // Case 6: Task 6 - created, started, two attempts, task ended
        let task6 = domain_ref.get_task(task6_id).await.unwrap().unwrap();
        assert_eq!(task6.name, "task6");
        assert_eq!(task6.attempts.len(), 2);

        // Sort attempts by attempt number for consistent testing
        let mut attempts = task6.attempts.clone();
        attempts.sort_by_key(|a| a.attempt);

        let attempt6_1 = &attempts[0];
        assert_eq!(attempt6_1.attempt, 1);
        assert!(attempt6_1.start_time.is_some());
        assert!(attempt6_1.end_time.is_some());
        assert_eq!(attempt6_1.status, 2); // Failed

        let attempt6_2 = &attempts[1];
        assert_eq!(attempt6_2.attempt, 2);
        assert!(attempt6_2.start_time.is_some());
        assert!(attempt6_2.end_time.is_some());
        assert_eq!(attempt6_2.status, 1); // Success

        // Verify database state directly
        let client = pool.get().await.unwrap();

        // Check task_instance table
        let task_rows = client.query(
        "SELECT id, name, start_time, end_time FROM task_instance WHERE domain = $1 ORDER BY name",
        &[&domain]
    ).await.unwrap();
        assert_eq!(task_rows.len(), 6);

        // Check task_attempts table
        let attempt_rows = client.query(
        "SELECT task_instance_id, attempt, start_time, end_time, status FROM task_attempts WHERE domain = $1 ORDER BY task_instance_id, attempt",
        &[&domain]
    ).await.unwrap();
        assert_eq!(attempt_rows.len(), 5); // task3: 1, task4: 1, task5: 1, task6: 2 = 5 total

        // Check merge_cursor was updated
        let cursor_rows = client
            .query("SELECT last_event_id FROM merge_cursor", &[])
            .await
            .unwrap();
        assert_eq!(cursor_rows.len(), 1);
        let last_event_id = cursor_rows[0].get::<_, i64>("last_event_id");
        assert!(last_event_id > 0);

        println!("✅ Comprehensive merge_events_to_db test passed!");
    })
);
