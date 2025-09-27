use crate::orchestrator::db::PgPool;
use crate::{
    EVENT_TASK_ATTEMPT_ENDED, EVENT_TASK_ATTEMPT_STARTED, EVENT_TASK_CREATED, EVENT_TASK_ENDED,
    EVENT_TASK_STARTED,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::StreamExt;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use serde_json::Value as JsonValue;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio_postgres::Row;
use uuid::Uuid;

use log::{debug, info};

#[derive(Debug, PartialEq, Eq)]
pub struct ScheduledRetry {
    when: Instant,
    task_id: Uuid,
}

impl Ord for ScheduledRetry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap (earliest first)
        other.when.cmp(&self.when)
    }
}

impl PartialOrd for ScheduledRetry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct TaskAttempt {
    pub attempt: i32,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: i16,
}

#[derive(Debug, Clone)]
pub enum TaskResultValue {
    Success(serde_json::Value),
    Error {
        error_type: String,
        message: String,
        data: serde_json::Value,
        retriable: bool,
    },
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub flow_instance_id: Option<Uuid>,
    pub retry_policy: JsonValue,
    pub args: String,
    pub kwargs: JsonValue,
    pub status: i16,
    pub attempts: Vec<TaskAttempt>,
    pub shepherd_group: Option<String>,
    pub result: Option<TaskResultValue>,
    pub result_stored_at: Option<DateTime<Utc>>,
}

impl Default for Task {
    fn default() -> Self {
        Self::new()
    }
}

impl Task {
    pub fn clear(&mut self) {
        self.id = Uuid::nil();
        self.name.clear();
        self.created_at = DateTime::UNIX_EPOCH;
        self.flow_instance_id = None;
        self.retry_policy = JsonValue::Null;
        self.args.clear();
        self.kwargs = JsonValue::Null;
        self.status = 0;
        self.attempts.clear();
        self.shepherd_group = None;
        self.result = None;
        self.result_stored_at = None;
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }

    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: String::new(),
            created_at: DateTime::UNIX_EPOCH,
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 0,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        }
    }

    pub fn has_exceeded_max_delay(&self, max_delay_secs: Option<f64>) -> bool {
        if let Some(max_delay) = max_delay_secs {
            // Calculate elapsed time since task creation, not first attempt
            let elapsed = Utc::now().signed_duration_since(self.created_at);
            elapsed.num_seconds() as f64 >= max_delay
        } else {
            false
        }
    }
}

pub struct TaskSet {
    domain: String,
    tasks: Vec<Task>,
    id_to_index: HashMap<Uuid, usize>,
    gaps: Vec<usize>,
    pub retry_scheduler: BinaryHeap<ScheduledRetry>,
}

impl TaskSet {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            tasks: Vec::new(),
            id_to_index: HashMap::new(),
            gaps: Vec::new(),
            retry_scheduler: BinaryHeap::new(),
        }
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn get_task(&self, id: Uuid) -> Option<&Task> {
        self.id_to_index.get(&id).map(|&index| &self.tasks[index])
    }

    pub fn get_task_mut(&mut self, id: Uuid) -> Option<&mut Task> {
        self.id_to_index
            .get(&id)
            .map(|&index| &mut self.tasks[index])
    }

    pub fn upsert_task(&mut self, task: Task) {
        let id = task.id;

        match self.id_to_index.entry(id) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let index = *entry.get();
                self.tasks[index] = task;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = if let Some(gap_index) = self.gaps.pop() {
                    self.tasks[gap_index] = task;
                    gap_index
                } else {
                    let index = self.tasks.len();
                    self.tasks.push(task);
                    index
                };
                entry.insert(index);
            }
        }
    }

    pub fn delete_task(&mut self, id: Uuid) -> bool {
        if let Some(&index) = self.id_to_index.get(&id) {
            self.tasks[index].clear();
            self.gaps.push(index);
            self.id_to_index.remove(&id);
            true
        } else {
            false
        }
    }

    pub fn all_tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.iter().filter(|task| !task.is_empty())
    }

    pub fn all_tasks_mut(&mut self) -> impl Iterator<Item = &mut Task> {
        self.tasks.iter_mut().filter(|task| !task.is_empty())
    }

    pub fn len(&self) -> usize {
        self.id_to_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_index.is_empty()
    }

    pub fn stats(&self) -> TaskSetStats {
        TaskSetStats {
            total_slots: self.tasks.len(),
            active_tasks: self.id_to_index.len(),
            gap_slots: self.gaps.len(),
            memory_efficiency: if self.tasks.is_empty() {
                0.0
            } else {
                self.id_to_index.len() as f64 / self.tasks.len() as f64
            },
        }
    }

    pub fn compact(&mut self) {
        if self.gaps.is_empty() {
            return;
        }

        let mut new_tasks = Vec::with_capacity(self.id_to_index.len());
        let mut new_id_to_index = FxHashMap::default();

        let mut temp_task = Task::new();

        for (id, &old_index) in &self.id_to_index {
            let new_index = new_tasks.len();
            std::mem::swap(&mut temp_task, &mut self.tasks[old_index]);
            new_tasks.push(temp_task);
            temp_task = Task::new(); // Reset for next iteration
            new_id_to_index.insert(*id, new_index);
        }

        self.tasks = new_tasks;
        self.id_to_index = new_id_to_index.into_iter().collect::<HashMap<_, _>>();
        self.gaps.clear();
    }

    pub fn reset(&mut self) {
        self.tasks.iter_mut().for_each(|t| t.clear());
        self.id_to_index.clear();
        self.gaps.clear();
        self.retry_scheduler.clear();
    }

    pub fn schedule_retry(&mut self, task_id: Uuid, when: Instant) {
        self.retry_scheduler.push(ScheduledRetry { when, task_id });
    }

    pub fn get_next_retry_time(&self) -> Option<Instant> {
        self.retry_scheduler.peek().map(|retry| retry.when)
    }

    pub fn pop_due_retries(&mut self, now: Instant) -> Vec<Uuid> {
        let mut due_tasks = Vec::new();

        while let Some(retry) = self.retry_scheduler.peek() {
            if retry.when <= now {
                due_tasks.push(self.retry_scheduler.pop().unwrap().task_id);
            } else {
                break;
            }
        }

        due_tasks
    }

    pub fn store_task_result(&mut self, task_id: Uuid, result: TaskResultValue) -> bool {
        if let Some(task) = self.get_task_mut(task_id) {
            task.result = Some(result);
            task.result_stored_at = Some(Utc::now());
            true
        } else {
            false
        }
    }

    pub fn purge_old_results(&mut self, retention_duration: chrono::Duration) {
        let cutoff_time = Utc::now() - retention_duration;

        let tasks_to_remove: Vec<Uuid> = self
            .tasks
            .iter()
            .filter(|task| !task.is_empty())
            .filter(|task| {
                task.result_stored_at
                    .map(|stored_at| stored_at < cutoff_time)
                    .unwrap_or(false)
            })
            .map(|task| task.id)
            .collect();

        for task_id in tasks_to_remove {
            self.delete_task(task_id);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskSetStats {
    pub total_slots: usize,
    pub active_tasks: usize,
    pub gap_slots: usize,
    pub memory_efficiency: f64,
}

pub struct TaskSetRegistryHandle<'a> {
    registry: &'a TaskSetRegistry,
    domain: String,
}

impl<'a> TaskSetRegistryHandle<'a> {
    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub async fn get_task(&self, id: Uuid) -> Result<Option<Task>, ()> {
        Ok(self.registry.get_task(&self.domain, id))
    }

    pub async fn upsert_task(&self, task: Task) -> Result<(), ()> {
        self.registry.upsert_task(&self.domain, task);
        Ok(())
    }

    pub async fn delete_task(&self, id: Uuid) -> Result<bool, ()> {
        Ok(self.registry.delete_task(&self.domain, id))
    }

    pub async fn all_tasks(&self) -> Result<Vec<Task>, ()> {
        Ok(self.registry.all_tasks(&self.domain))
    }

    pub async fn stats(&self) -> Result<TaskSetStats, ()> {
        Ok(self.registry.stats(&self.domain).unwrap_or_default())
    }

    pub async fn len(&self) -> Result<usize, ()> {
        Ok(self.registry.len(&self.domain))
    }

    pub async fn is_empty(&self) -> Result<bool, ()> {
        Ok(self.registry.is_empty(&self.domain))
    }
}

pub struct TaskSetRegistry {
    domains: std::sync::Mutex<HashMap<String, TaskSet>>,
    task_to_shepherd: DashMap<Uuid, Uuid>, // task_id -> shepherd_uuid
    shepherd_tasks: DashMap<Uuid, HashSet<Uuid>>, // shepherd_uuid -> task_ids
}

impl TaskSetRegistry {
    pub fn new() -> Self {
        Self {
            domains: std::sync::Mutex::new(HashMap::new()),
            task_to_shepherd: DashMap::new(),
            shepherd_tasks: DashMap::new(),
        }
    }

    pub fn get_task(&self, domain: &str, id: Uuid) -> Option<Task> {
        let domains = self.domains.lock().unwrap();
        domains.get(domain)?.get_task(id).cloned()
    }

    pub fn get_task_status(&self, domain: &str, id: Uuid) -> Option<i16> {
        let domains = self.domains.lock().unwrap();
        domains.get(domain)?.get_task(id).map(|t| t.status)
    }

    pub fn upsert_task(&self, domain: &str, task: Task) {
        let mut domains = self.domains.lock().unwrap();

        if !domains.contains_key(domain) {
            domains.insert(domain.to_string(), TaskSet::new(domain.to_string()));
        }

        if let Some(taskset) = domains.get_mut(domain) {
            taskset.upsert_task(task);
        }
    }

    pub fn delete_task(&self, domain: &str, id: Uuid) -> bool {
        let mut domains = self.domains.lock().unwrap();
        domains
            .get_mut(domain)
            .is_some_and(|taskset| taskset.delete_task(id))
    }

    pub fn extract_task_set(&self, domain: &str) -> Option<TaskSet> {
        let mut domains = self.domains.lock().unwrap();
        domains.remove(domain)
    }

    pub fn all_tasks(&self, domain: &str) -> Vec<Task> {
        let domains = self.domains.lock().unwrap();
        domains
            .get(domain)
            .map_or(Vec::new(), |taskset| taskset.all_tasks().cloned().collect())
    }

    pub fn stats(&self, domain: &str) -> Option<TaskSetStats> {
        let domains = self.domains.lock().unwrap();
        domains.get(domain).map(|taskset| taskset.stats())
    }

    pub fn len(&self, domain: &str) -> usize {
        let domains = self.domains.lock().unwrap();
        domains.get(domain).map_or(0, |taskset| taskset.len())
    }

    pub fn is_empty(&self, domain: &str) -> bool {
        let domains = self.domains.lock().unwrap();
        domains.get(domain).is_none_or(|taskset| taskset.is_empty())
    }

    pub fn compact(&self, domain: &str) {
        let mut domains = self.domains.lock().unwrap();
        if let Some(taskset) = domains.get_mut(domain) {
            taskset.compact();
        }
    }

    pub fn reset(&self, domain: &str) {
        let mut domains = self.domains.lock().unwrap();
        if let Some(taskset) = domains.get_mut(domain) {
            taskset.reset();
        }
    }

    pub fn with_domain_mut<F, R>(&self, domain: &str, f: F) -> R
    where
        F: FnOnce(&mut TaskSet) -> R,
    {
        let mut domains = self.domains.lock().unwrap();

        if !domains.contains_key(domain) {
            domains.insert(domain.to_string(), TaskSet::new(domain.to_string()));
        }

        f(domains.get_mut(domain).unwrap())
    }

    pub fn store_task_result(&self, domain: &str, task_id: Uuid, result: TaskResultValue) -> bool {
        self.with_domain_mut(domain, |taskset| taskset.store_task_result(task_id, result))
    }

    pub fn purge_old_results(&self, domain: &str, retention_duration: chrono::Duration) {
        let mut domains = self.domains.lock().unwrap();
        if let Some(taskset) = domains.get_mut(domain) {
            taskset.purge_old_results(retention_duration);
        }
    }

    pub fn with_domain<F, R>(&self, domain: &str, f: F) -> Option<R>
    where
        F: FnOnce(&TaskSet) -> R,
    {
        let domains = self.domains.lock().unwrap();
        domains.get(domain).map(f)
    }

    pub fn find_task(&self, id: Uuid) -> Option<Task> {
        let domains = self.domains.lock().unwrap();

        for taskset in domains.values() {
            if let Some(task) = taskset.get_task(id) {
                return Some(task.clone());
            }
        }
        None
    }

    pub fn domains(&self) -> Vec<String> {
        let domains = self.domains.lock().unwrap();
        domains.keys().cloned().collect()
    }

    pub fn get_domain_for_task(&self, task_id: Uuid) -> Option<String> {
        let domains = self.domains.lock().unwrap();

        for (domain_name, taskset) in domains.iter() {
            if taskset.get_task(task_id).is_some() {
                return Some(domain_name.clone());
            }
        }
        None
    }

    pub fn domain_count(&self) -> usize {
        let domains = self.domains.lock().unwrap();
        domains.len()
    }

    pub fn get_or_create_domain(&self, domain: &str) -> TaskSetRegistryHandle<'_> {
        TaskSetRegistryHandle {
            registry: self,
            domain: domain.to_string(),
        }
    }

    pub fn get_domain(&self, domain: &str) -> Option<TaskSetRegistryHandle<'_>> {
        let domains = self.domains.lock().unwrap();
        if domains.contains_key(domain) {
            Some(TaskSetRegistryHandle {
                registry: self,
                domain: domain.to_string(),
            })
        } else {
            None
        }
    }

    pub fn total_tasks(&self) -> usize {
        let domains = self.domains.lock().unwrap();
        domains.values().map(|taskset| taskset.len()).sum()
    }

    pub fn all_stats(&self) -> Vec<(String, TaskSetStats)> {
        let domains = self.domains.lock().unwrap();
        domains
            .iter()
            .map(|(name, taskset)| (name.clone(), taskset.stats()))
            .collect()
    }

    pub fn track_task_dispatch(&self, task_id: Uuid, shepherd_uuid: Uuid) {
        self.task_to_shepherd.insert(task_id, shepherd_uuid);
        self.shepherd_tasks
            .entry(shepherd_uuid)
            .or_default()
            .insert(task_id);
        log::debug!("Tracked task {task_id} dispatched to shepherd {shepherd_uuid}");
    }

    pub fn untrack_task(&self, task_id: Uuid) {
        if let Some((_, shepherd_uuid)) = self.task_to_shepherd.remove(&task_id) {
            if let Some(mut tasks) = self.shepherd_tasks.get_mut(&shepherd_uuid) {
                tasks.remove(&task_id);
                if tasks.is_empty() {
                    drop(tasks);
                    self.shepherd_tasks.remove(&shepherd_uuid);
                }
            }
            log::debug!("Untracked task {task_id} from shepherd {shepherd_uuid}");
        }
    }

    pub fn get_shepherd_tasks(&self, shepherd_uuid: Uuid) -> Vec<Uuid> {
        self.shepherd_tasks
            .get(&shepherd_uuid)
            .map(|tasks| tasks.clone().into_iter().collect())
            .unwrap_or_default()
    }

    pub async fn generate_shepherd_failure_events(
        &self,
        shepherd_uuid: Uuid,
        event_stream: &crate::orchestrator::event_stream::EventStream,
    ) -> Result<Vec<Uuid>> {
        let task_ids = self.get_shepherd_tasks(shepherd_uuid);

        log::info!(
            "Generating failure events for {} tasks from dead shepherd {shepherd_uuid}",
            task_ids.len()
        );

        let mut failed_task_ids = Vec::new();

        for task_id in task_ids {
            // Use existing find_task method to get task details and determine domain
            if let Some(task) = self.find_task(task_id) {
                // Find which domain contains this task
                if let Some(domain) = self.get_domain_for_task(task_id) {
                    // Create failure event
                    let event_metadata = serde_json::json!({
                        "task_id": task_id,
                        "failure_reason": "shepherd_crashed",
                        "failed_shepherd": shepherd_uuid,
                        "failed_at": Utc::now(),
                        "error_type": "InfrastructureError",
                        "error_message": "Shepherd connection lost and timed out",
                        "auto_failed": true
                    });

                    let event_record = crate::orchestrator::event_stream::EventRecord {
                        domain: domain.clone(),
                        task_instance_id: Some(task_id),
                        flow_instance_id: task.flow_instance_id,
                        event_type: EVENT_TASK_ATTEMPT_ENDED,
                        created_at: Utc::now(),
                        metadata: event_metadata,
                    };

                    if let Err(e) = event_stream.write_event(event_record).await {
                        log::error!("Failed to write task failure event for {task_id}: {e}");
                        continue;
                    }

                    failed_task_ids.push(task_id);

                    self.untrack_task(task_id);
                }
            }
        }

        Ok(failed_task_ids)
    }

    pub async fn fail_shepherd_tasks(
        &self,
        shepherd_uuid: Uuid,
        event_stream: &crate::orchestrator::event_stream::EventStream,
    ) -> Result<()> {
        let task_ids = self.get_shepherd_tasks(shepherd_uuid);

        log::info!(
            "Failing {} tasks from dead shepherd {shepherd_uuid}",
            task_ids.len()
        );

        for task_id in task_ids {
            // Use existing find_task method to get task details and determine domain
            if let Some(task) = self.find_task(task_id) {
                // Find which domain contains this task
                if let Some(domain) = self.get_domain_for_task(task_id) {
                    // Create failure event
                    let event_metadata = serde_json::json!({
                        "task_id": task_id,
                        "failure_reason": "shepherd_crashed",
                        "failed_shepherd": shepherd_uuid,
                        "failed_at": Utc::now(),
                        "error_type": "InfrastructureError",
                        "error_message": "Shepherd connection lost and timed out",
                        "auto_failed": true
                    });

                    let event_record = crate::orchestrator::event_stream::EventRecord {
                        domain: domain.clone(),
                        task_instance_id: Some(task_id),
                        flow_instance_id: task.flow_instance_id,
                        event_type: EVENT_TASK_ATTEMPT_ENDED,
                        created_at: Utc::now(),
                        metadata: event_metadata,
                    };

                    if let Err(e) = event_stream.write_event(event_record).await {
                        log::error!("Failed to write task failure event for {task_id}: {e}");
                        continue;
                    }

                    if let Err(e) = self
                        .handle_task_failure(&domain, task_id, "shepherd_crashed")
                        .await
                    {
                        log::error!("Failed to update TaskSet for failed task {task_id}: {e}");
                    }

                    self.untrack_task(task_id);
                }
            }
        }

        Ok(())
    }

    // get_domain_for_task method already defined above

    /// Placeholder method for handling task failure
    async fn handle_task_failure(
        &self,
        domain: &str,
        task_id: Uuid,
        failure_reason: &str,
    ) -> Result<()> {
        log::info!("Task {task_id} in domain {domain} failed due to: {failure_reason}");
        // TODO: Update task status, handle retry logic integration with scheduler
        // For now, just log the failure
        Ok(())
    }

    /// Load tasks from database
    pub async fn load_from_db(&self, pool: &PgPool) -> Result<()> {
        log::info!("Starting to load tasks from database...");
        let client = pool.get().await?;

        let (tasks_by_domain, attempts_by_domain) = tokio::try_join!(
            stream_and_group_by_domain(&client, "SELECT id, name, domain, created_at, flow_instance_id, retry_policy, args, kwargs, status FROM task_instance"),
            stream_and_group_by_domain(&client, "SELECT task_instance_id, domain, attempt, start_time, end_time, status FROM task_attempts")
        )?;

        let tasks_by_domain = Arc::new(tasks_by_domain);
        let attempts_by_domain = Arc::new(attempts_by_domain);

        let mut domains: Vec<_> = tasks_by_domain.keys().cloned().collect();
        for domain in attempts_by_domain.keys() {
            if !tasks_by_domain.contains_key(domain) {
                domains.push(domain.clone());
            }
        }

        let domain_task_sets: Vec<TaskSet> = domains
            .par_iter()
            .map(|domain| {
                let mut task_set = TaskSet::new(domain.to_string());

                if let Some(task_rows) = tasks_by_domain.get(domain) {
                    for row in task_rows {
                        let task = Task {
                            id: row.get("id"),
                            name: row.get("name"),
                            created_at: row.get("created_at"),
                            flow_instance_id: row.get("flow_instance_id"),
                            retry_policy: row.get("retry_policy"),
                            args: row.get("args"),
                            kwargs: row.get("kwargs"),
                            status: row.get("status"),
                            attempts: Vec::new(),
                            shepherd_group: None,
                            result: None,
                            result_stored_at: None,
                        };
                        task_set.upsert_task(task);
                    }
                }

                if let Some(attempt_rows) = attempts_by_domain.get(domain) {
                    for row in attempt_rows {
                        let task_id: Uuid = row.get("task_instance_id");
                        if let Some(task) = task_set.get_task_mut(task_id) {
                            let attempt = TaskAttempt {
                                attempt: row.get("attempt"),
                                start_time: row.get("start_time"),
                                end_time: row.get("end_time"),
                                status: row.get("status"),
                            };
                            task.attempts.push(attempt);
                        }
                    }
                }

                task_set
            })
            .collect();

        let mut domain_map = HashMap::new();
        for task_set in domain_task_sets {
            let domain_name = task_set.domain().to_string();
            domain_map.insert(domain_name, task_set);
        }

        {
            let mut domains_guard = self.domains.lock().unwrap();
            *domains_guard = domain_map;
        }

        // TODO: Reconstruct shepherd assignments from unconsumed events
        // This will replay events after the last materialization to rebuild shepherd tracking state

        log::info!("Successfully loaded {} domains.", self.domain_count());
        Ok(())
    }

    /// Sync events to database tables
    pub async fn merge_events_to_db(&self, pool: &PgPool) -> Result<()> {
        let mut client = pool.get().await?;

        let transaction = client.transaction().await?;

        let cursor_row = transaction
            .query_opt("SELECT last_event_id FROM merge_cursor LIMIT 1", &[])
            .await?;

        let last_processed_event_id = cursor_row.map(|row| row.get::<_, i64>(0)).unwrap_or(0);

        debug!("Starting event merge from event_id: {last_processed_event_id}");

        let max_event_row = transaction
            .query_opt(
                "SELECT MAX(event_id) FROM events WHERE event_id > $1",
                &[&last_processed_event_id],
            )
            .await?;

        let max_event_id = match max_event_row {
            Some(row) => match row.get::<_, Option<i64>>(0) {
                Some(id) => id,
                None => {
                    debug!("No new events to process");
                    return Ok(());
                }
            },
            None => {
                debug!("No new events to process");
                return Ok(());
            }
        };

        info!(
            "Processing events from {last_processed_event_id} to {max_event_id} using server-side SQL"
        );

        let task_created_count = transaction
            .execute(
                r#"
                INSERT INTO task_instance (id, name, domain, created_at, retry_policy, args, kwargs, status, flow_instance_id)
                SELECT 
                    e.task_instance_id,
                    COALESCE((e.metadata->>'task_name')::text, 'unknown'),
                    e.domain,
                    e.created_at,
                    COALESCE((e.metadata->'retry_policy')::jsonb, '{}'::jsonb),
                    CASE 
                        WHEN e.metadata->'args' IS NOT NULL 
                        THEN (e.metadata->>'args')::text
                        ELSE '[]'::text
                    END,
                    COALESCE((e.metadata->'kwargs')::jsonb, '{}'::jsonb),
                    $4,
                    e.flow_instance_id
                FROM events e
                WHERE e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                ON CONFLICT (id, domain) DO NOTHING
                "#,
                &[&last_processed_event_id, &max_event_id, &EVENT_TASK_CREATED, &crate::TASK_STATUS_CREATED]
            )
            .await?;

        debug!("Merged {task_created_count} task creation events");

        let task_started_count = transaction
            .execute(
                r#"
                UPDATE task_instance 
                SET start_time = e.created_at
                FROM events e
                WHERE task_instance.id = e.task_instance_id
                  AND task_instance.domain = e.domain
                  AND e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                "#,
                &[&last_processed_event_id, &max_event_id, &EVENT_TASK_STARTED],
            )
            .await?;

        debug!("Merged {task_started_count} task started events");

        let task_ended_count = transaction
            .execute(
                r#"
                UPDATE task_instance 
                SET end_time = e.created_at
                FROM events e
                WHERE task_instance.id = e.task_instance_id
                  AND task_instance.domain = e.domain
                  AND e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                "#,
                &[&last_processed_event_id, &max_event_id, &EVENT_TASK_ENDED],
            )
            .await?;

        debug!("Merged {task_ended_count} task ended events");

        let attempt_started_count = transaction
            .execute(
                r#"
                INSERT INTO task_attempts (task_instance_id, domain, attempt, start_time, status)
                SELECT 
                    e.task_instance_id,
                    e.domain,
                    COALESCE((e.metadata->>'attempt')::int, 1),
                    e.created_at,
                    $4
                FROM events e
                WHERE e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                ON CONFLICT (task_instance_id, domain, attempt) 
                DO UPDATE SET start_time = EXCLUDED.start_time
                "#,
                &[
                    &last_processed_event_id,
                    &max_event_id,
                    &EVENT_TASK_ATTEMPT_STARTED,
                    &crate::ATTEMPT_STATUS_STARTED,
                ],
            )
            .await?;

        debug!("Merged {attempt_started_count} task attempt started events");

        let attempt_ended_count = transaction
            .execute(
                r#"
                UPDATE task_attempts 
                SET 
                    end_time = e.created_at,
                    status = COALESCE((e.metadata->>'status')::smallint, $4)
                FROM events e
                WHERE task_attempts.task_instance_id = e.task_instance_id
                  AND task_attempts.domain = e.domain
                  AND task_attempts.attempt = COALESCE((e.metadata->>'attempt')::int, 1)
                  AND e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                "#,
                &[
                    &last_processed_event_id,
                    &max_event_id,
                    &EVENT_TASK_ATTEMPT_ENDED,
                    &crate::ATTEMPT_STATUS_SUCCEEDED,
                ],
            )
            .await?;

        debug!("Merged {attempt_ended_count} task attempt ended events");

        if max_event_id > last_processed_event_id {
            transaction.execute("DELETE FROM merge_cursor", &[]).await?;

            transaction
                .execute(
                    "INSERT INTO merge_cursor (last_event_id) VALUES ($1)",
                    &[&max_event_id],
                )
                .await?;

            info!("Updated merge cursor to event_id: {max_event_id}");
        }

        transaction.commit().await?;

        let total_events = task_created_count
            + task_started_count
            + task_ended_count
            + attempt_started_count
            + attempt_ended_count;
        info!("Successfully merged {total_events} events using server-side SQL (task_created: {task_created_count}, task_started: {task_started_count}, task_ended: {task_ended_count}, attempt_started: {attempt_started_count}, attempt_ended: {attempt_ended_count})");
        Ok(())
    }
}

async fn stream_and_group_by_domain(
    client: &deadpool_postgres::Client,
    query: &str,
) -> Result<HashMap<String, Vec<Row>>> {
    let mut results: HashMap<String, Vec<Row>> = HashMap::new();
    let stmt = client.prepare_cached(query).await?;
    let stream = client.query_raw(&stmt, &[] as &[&str; 0]).await?;
    tokio::pin!(stream);

    while let Some(row) = stream.next().await {
        let row = row?;
        let domain: String = row.get("domain");
        results.entry(domain).or_default().push(row);
    }

    Ok(results)
}

impl Default for TaskSetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_upsert_and_get() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task);

        assert_eq!(task_set.len(), 1);
        assert!(task_set.get_task(task_id).is_some());
        assert!(task_set.get_task(Uuid::new_v4()).is_none());
    }

    #[test]
    fn test_delete() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task);
        assert_eq!(task_set.len(), 1);

        assert!(task_set.delete_task(task_id));
        assert_eq!(task_set.len(), 0);
        assert!(task_set.get_task(task_id).is_none());

        assert_eq!(task_set.gaps.len(), 1);
    }

    #[test]
    fn test_gap_reuse() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task1_id = Uuid::new_v4();
        let task1 = Task {
            id: task1_id,
            name: "task1".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task1);
        task_set.delete_task(task1_id);

        let task2_id = Uuid::new_v4();
        let task2 = Task {
            id: task2_id,
            name: "task2".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task2);

        assert_eq!(task_set.len(), 1);
        assert_eq!(task_set.gaps.len(), 0);
    }

    #[test]
    fn test_has_exceeded_max_delay_with_task_creation_time() {
        let mut task = Task::new();

        // Task::new() sets created_at to UNIX_EPOCH, so it will always exceed any reasonable limit
        assert!(task.has_exceeded_max_delay(Some(10.0)));
        assert!(!task.has_exceeded_max_delay(None)); // None means no time limit

        // Set task creation time to NOW to test normal behavior
        task.created_at = Utc::now();

        // Should not exceed with reasonable max_delay
        assert!(!task.has_exceeded_max_delay(Some(10.0)));

        // Set task creation time to 5 seconds ago
        task.created_at = Utc::now() - chrono::Duration::seconds(5);

        // Should not exceed with max_delay of 10 seconds
        assert!(!task.has_exceeded_max_delay(Some(10.0)));

        // Should exceed with max_delay of 3 seconds (elapsed > 3)
        assert!(task.has_exceeded_max_delay(Some(3.0)));

        // Should not exceed with None (no time limit)
        assert!(!task.has_exceeded_max_delay(None));

        // Attempts should not affect max_delay calculation (now based on creation time)
        task.attempts.push(TaskAttempt {
            attempt: 0,
            start_time: Some(Utc::now() - chrono::Duration::seconds(1)), // Recent attempt
            end_time: None,
            status: 1,
        });

        // Should still be based on task creation time (5 seconds ago), not attempt time
        assert!(task.has_exceeded_max_delay(Some(3.0)));
    }

    #[test]
    fn test_has_exceeded_max_delay_edge_cases() {
        let mut task = Task::new();

        // Set task creation time to 10 seconds ago
        task.created_at = Utc::now() - chrono::Duration::seconds(10);

        // Attempts should not affect calculation since we use task creation time
        task.attempts.push(TaskAttempt {
            attempt: 0,
            start_time: None, // Even with no start_time, should work
            end_time: None,
            status: 1,
        });

        // Should exceed with 8 seconds limit (task created 10 seconds ago)
        assert!(task.has_exceeded_max_delay(Some(8.0)));

        // Test with multiple attempts - should still use task creation time
        let recent_time = Utc::now() - chrono::Duration::seconds(2);
        task.attempts.push(TaskAttempt {
            attempt: 1,
            start_time: Some(recent_time),
            end_time: None,
            status: 1,
        });

        // Should still use task creation time (10 seconds ago), not recent attempt time
        assert!(task.has_exceeded_max_delay(Some(8.0))); // Exceeded
        assert!(!task.has_exceeded_max_delay(Some(12.0))); // Not exceeded
    }

    #[test]
    fn test_has_exceeded_max_delay_boundary_conditions() {
        let mut task = Task::new();

        // Test exactly at the boundary using task creation time
        let exact_time = Utc::now() - chrono::Duration::seconds(5);
        task.created_at = exact_time;

        // Test with max_delay exactly equal to elapsed time
        // Due to timing precision, we test within a small range
        let elapsed_secs = (Utc::now() - exact_time).num_seconds() as f64;
        assert!(task.has_exceeded_max_delay(Some(elapsed_secs - 1.0))); // Should exceed
        assert!(!task.has_exceeded_max_delay(Some(elapsed_secs + 1.0))); // Should not exceed

        // Test with zero max_delay - should immediately exceed
        assert!(task.has_exceeded_max_delay(Some(0.0)));

        // Test with very large max_delay - should not exceed
        assert!(!task.has_exceeded_max_delay(Some(f64::MAX)));

        // Test with very small positive max_delay - should exceed
        assert!(task.has_exceeded_max_delay(Some(0.001)));
    }

    #[test]
    fn test_has_exceeded_max_delay_precision() {
        let mut task = Task::new();

        // Test fractional second precision using task creation time
        let precise_time = Utc::now() - chrono::Duration::milliseconds(1500); // 1.5 seconds ago
        task.created_at = precise_time;

        // Should exceed 1.0 second limit
        assert!(task.has_exceeded_max_delay(Some(1.0)));

        // Should not exceed 2.0 second limit
        assert!(!task.has_exceeded_max_delay(Some(2.0)));
    }

    #[test]
    fn test_store_task_result_success() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task);

        let result_value = TaskResultValue::Success(serde_json::json!({"output": "test success"}));
        let success = task_set.store_task_result(task_id, result_value.clone());

        assert!(success);

        let stored_task = task_set.get_task(task_id).unwrap();
        assert!(stored_task.result.is_some());
        assert!(stored_task.result_stored_at.is_some());

        match &stored_task.result {
            Some(TaskResultValue::Success(data)) => {
                assert_eq!(data, &serde_json::json!({"output": "test success"}));
            }
            _ => panic!("Expected success result"),
        }
    }

    #[test]
    fn test_store_task_result_error() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        task_set.upsert_task(task);

        let result_value = TaskResultValue::Error {
            error_type: "ValueError".to_string(),
            message: "Invalid input".to_string(),
            data: serde_json::json!({"error_code": 400}),
            retriable: false,
        };

        let success = task_set.store_task_result(task_id, result_value.clone());

        assert!(success);

        let stored_task = task_set.get_task(task_id).unwrap();
        assert!(stored_task.result.is_some());
        assert!(stored_task.result_stored_at.is_some());

        match &stored_task.result {
            Some(TaskResultValue::Error {
                error_type,
                message,
                data,
                retriable,
            }) => {
                assert_eq!(error_type, "ValueError");
                assert_eq!(message, "Invalid input");
                assert_eq!(data, &serde_json::json!({"error_code": 400}));
                assert!(!retriable);
            }
            _ => panic!("Expected error result"),
        }
    }

    #[test]
    fn test_store_task_result_nonexistent_task() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let result_value = TaskResultValue::Success(serde_json::json!({"output": "test"}));

        let success = task_set.store_task_result(task_id, result_value);

        assert!(!success);
    }

    #[test]
    fn test_purge_old_results() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        // Create two tasks
        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();

        let task1 = Task {
            id: task1_id,
            name: "task1".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: Some(TaskResultValue::Success(
                serde_json::json!({"output": "task1"}),
            )),
            result_stored_at: Some(Utc::now() - chrono::Duration::hours(2)), // 2 hours ago
        };

        let task2 = Task {
            id: task2_id,
            name: "task2".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: Some(TaskResultValue::Success(
                serde_json::json!({"output": "task2"}),
            )),
            result_stored_at: Some(Utc::now() - chrono::Duration::minutes(30)), // 30 minutes ago
        };

        task_set.upsert_task(task1);
        task_set.upsert_task(task2);

        assert_eq!(task_set.len(), 2);

        // Purge tasks older than 1 hour
        task_set.purge_old_results(chrono::Duration::hours(1));

        // Only task1 should be purged (2 hours old), task2 should remain (30 minutes old)
        assert_eq!(task_set.len(), 1);
        assert!(task_set.get_task(task1_id).is_none());
        assert!(task_set.get_task(task2_id).is_some());
    }

    #[test]
    fn test_purge_old_results_no_result_stored_at() {
        let mut task_set = TaskSet::new("test_domain".to_string());

        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: Some(TaskResultValue::Success(
                serde_json::json!({"output": "test"}),
            )),
            result_stored_at: None, // No result_stored_at
        };

        task_set.upsert_task(task);
        assert_eq!(task_set.len(), 1);

        // Purge should not affect tasks without result_stored_at
        task_set.purge_old_results(chrono::Duration::hours(1));

        assert_eq!(task_set.len(), 1);
        assert!(task_set.get_task(task_id).is_some());
    }

    #[test]
    fn test_task_set_registry_store_result() {
        let registry = TaskSetRegistry::new();

        let domain = "test_domain";
        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        };

        registry.upsert_task(domain, task);

        let result_value = TaskResultValue::Success(serde_json::json!({"output": "registry test"}));
        let success = registry.store_task_result(domain, task_id, result_value);

        assert!(success);

        let stored_task = registry.get_task(domain, task_id).unwrap();
        assert!(stored_task.result.is_some());
        assert!(stored_task.result_stored_at.is_some());
    }

    #[test]
    fn test_task_set_registry_purge_old_results() {
        let registry = TaskSetRegistry::new();

        let domain = "test_domain";
        let task_id = Uuid::new_v4();
        let task = Task {
            id: task_id,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: String::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
            shepherd_group: None,
            result: Some(TaskResultValue::Success(
                serde_json::json!({"output": "test"}),
            )),
            result_stored_at: Some(Utc::now() - chrono::Duration::hours(2)), // 2 hours ago
        };

        registry.upsert_task(domain, task);
        assert_eq!(registry.len(domain), 1);

        // Purge tasks older than 1 hour
        registry.purge_old_results(domain, chrono::Duration::hours(1));

        assert_eq!(registry.len(domain), 0);
    }
}
