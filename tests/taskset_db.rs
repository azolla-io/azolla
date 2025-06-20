mod common;
use azolla::{db::PgPool, taskset::TaskSetRegistry};
use chrono::Utc;

// Helper to insert a task_instance
async fn insert_task_instance(pool: &PgPool, id: i64, name: &str, domain: &str, status: i16) {
    let client = pool.get().await.unwrap();
    client.execute(
        "INSERT INTO task_instance (id, name, domain, created_at, flow_instance_id, retry_policy, args, kwargs, status) \
         VALUES ($1, $2, $3, $4, NULL, '{}'::jsonb, ARRAY[]::text[], '{}'::jsonb, $5)",
        &[&id, &name, &domain, &Utc::now(), &status],
    ).await.unwrap();
}

// Helper to insert a task_attempt
async fn insert_task_attempt(pool: &PgPool, task_instance_id: i64, domain: &str, attempt: i32, status: i16) {
    let client = pool.get().await.unwrap();
    client.execute(
        "INSERT INTO task_attempts (task_instance_id, domain, attempt, start_time, end_time, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[&task_instance_id, &domain, &attempt, &Utc::now(), &Utc::now(), &status],
    ).await.unwrap();
}

db_test!(test_load_from_db_structure, (|pool| async move {
    // Insert dummy data for two domains
    // domain_a: task 1 (2 attempts), task 2 (0 attempts)
    // domain_b: task 3 (1 attempt)
    insert_task_instance(&pool, 1, "task1", "domain_a", 10).await;
    insert_task_instance(&pool, 2, "task2", "domain_a", 20).await;
    insert_task_instance(&pool, 3, "task3", "domain_b", 30).await;

    insert_task_attempt(&pool, 1, "domain_a", 1, 100).await;
    insert_task_attempt(&pool, 1, "domain_a", 2, 101).await;
    insert_task_attempt(&pool, 3, "domain_b", 1, 200).await;

    // Load from DB
    let registry = TaskSetRegistry::new();
    registry.load_from_db(&pool).await.unwrap();

    // Check structure
    let mut domains: Vec<_> = registry.domains().map(|entry| entry.key().clone()).collect();
    domains.sort();
    assert_eq!(domains, vec!["domain_a", "domain_b"]);

    // domain_a
    let domain_a = registry.get_domain("domain_a").unwrap();
    let mut tasks_a: Vec<_> = domain_a.all_tasks().map(|t| t.id).collect();
    tasks_a.sort();
    assert_eq!(tasks_a, vec![1, 2]);
    let task1 = domain_a.get_task(1).unwrap();
    assert_eq!(task1.attempts.len(), 2);
    let task2 = domain_a.get_task(2).unwrap();
    assert_eq!(task2.attempts.len(), 0);

    // domain_b
    let domain_b = registry.get_domain("domain_b").unwrap();
    let tasks_b: Vec<_> = domain_b.all_tasks().map(|t| t.id).collect();
    assert_eq!(tasks_b, vec![3]);
    let task3 = domain_b.get_task(3).unwrap();
    assert_eq!(task3.attempts.len(), 1);
})); 