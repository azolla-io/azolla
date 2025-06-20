use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use crate::db::PgPool;
use futures::StreamExt;
use rayon::prelude::*;
use std::sync::Arc;
use tokio_postgres::Row;

/// Represents a single task attempt
#[derive(Debug, Clone)]
pub struct TaskAttempt {
    pub attempt: i32,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: i16,
}

/// Represents a task instance with embedded attempts
#[derive(Debug, Clone)]
pub struct Task {
    pub id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub flow_instance_id: Option<i64>,
    pub retry_policy: JsonValue,
    pub args: Vec<String>,
    pub kwargs: JsonValue,
    pub status: i16,
    pub attempts: Vec<TaskAttempt>,
}

impl Task {
    /// Clear the task to reclaim memory while preserving allocations
    pub fn clear(&mut self) {
        self.id = 0;
        self.name.clear(); // Reuse String allocation
        self.created_at = DateTime::UNIX_EPOCH;
        self.flow_instance_id = None;
        self.retry_policy = JsonValue::Null;
        self.args.clear(); // Reuse Vec allocation
        self.kwargs = JsonValue::Null;
        self.status = 0;
        self.attempts.clear(); // Reuse Vec allocation - big memory win!
    }

    /// Check if this task slot is available (empty)
    pub fn is_empty(&self) -> bool {
        self.id == 0
    }

    /// Create a new task with default values
    pub fn new() -> Self {
        Self {
            id: 0,
            name: String::new(),
            created_at: DateTime::UNIX_EPOCH,
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 0,
            attempts: Vec::new(),
        }
    }
}

/// In-memory data structure for tasks within a domain
/// Uses gap buffer approach for efficient insertion/deletion
pub struct TaskSet {
    domain: String,
    tasks: Vec<Task>,
    id_to_index: HashMap<i64, usize>,
    gaps: Vec<usize>,
}

impl TaskSet {
    /// Create a new TaskSet for the given domain
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            tasks: Vec::new(),
            id_to_index: HashMap::new(),
            gaps: Vec::new(),
        }
    }

    /// Get the domain this TaskSet represents
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Get a task by ID (O(1) average case)
    pub fn get_task(&self, id: i64) -> Option<&Task> {
        self.id_to_index
            .get(&id)
            .map(|&index| &self.tasks[index])
    }

    /// Get a mutable reference to a task by ID
    pub fn get_task_mut(&mut self, id: i64) -> Option<&mut Task> {
        self.id_to_index
            .get(&id)
            .map(|&index| &mut self.tasks[index])
    }

    /// Insert or update a task (O(1) average case)
    pub fn upsert_task(&mut self, task: Task) {
        let id = task.id;
        
        // If task already exists, update it
        if let Some(&index) = self.id_to_index.get(&id) {
            self.tasks[index] = task;
            return;
        }

        // Insert new task
        let index = if let Some(gap_index) = self.gaps.pop() {
            // Reuse existing allocation
            self.tasks[gap_index] = task;
            gap_index
        } else {
            // Allocate new slot
            let index = self.tasks.len();
            self.tasks.push(task);
            index
        };
        
        self.id_to_index.insert(id, index);
    }

    /// Delete a task by ID (O(1) average case)
    pub fn delete_task(&mut self, id: i64) -> bool {
        if let Some(&index) = self.id_to_index.get(&id) {
            // Clear the task to reclaim memory while preserving allocations
            self.tasks[index].clear();
            self.gaps.push(index);
            self.id_to_index.remove(&id);
            true
        } else {
            false
        }
    }

    /// Get all tasks (excluding empty slots)
    pub fn all_tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.iter().filter(|task| !task.is_empty())
    }

    /// Get all tasks with mutable access
    pub fn all_tasks_mut(&mut self) -> impl Iterator<Item = &mut Task> {
        self.tasks.iter_mut().filter(|task| !task.is_empty())
    }

    /// Get the number of active tasks
    pub fn len(&self) -> usize {
        self.id_to_index.len()
    }

    /// Check if there are no tasks
    pub fn is_empty(&self) -> bool {
        self.id_to_index.is_empty()
    }

    /// Get statistics about the TaskSet
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

    /// Compact the TaskSet by removing gaps (expensive operation)
    pub fn compact(&mut self) {
        if self.gaps.is_empty() {
            return; // Nothing to compact
        }

        // Create new compacted storage
        let mut new_tasks = Vec::with_capacity(self.id_to_index.len());
        let mut new_id_to_index = HashMap::with_capacity(self.id_to_index.len());

        // Copy active tasks to new storage
        for (id, &old_index) in &self.id_to_index {
            let new_index = new_tasks.len();
            new_tasks.push(self.tasks[old_index].clone());
            new_id_to_index.insert(*id, new_index);
        }

        // Replace old storage
        self.tasks = new_tasks;
        self.id_to_index = new_id_to_index;
        self.gaps.clear();
    }

    pub fn reset(&mut self) {
        self.tasks.iter_mut().for_each(|t| t.clear());
        self.id_to_index.clear();
        self.gaps.clear();
    }
}

/// Statistics about a TaskSet
#[derive(Debug, Clone)]
pub struct TaskSetStats {
    pub total_slots: usize,
    pub active_tasks: usize,
    pub gap_slots: usize,
    pub memory_efficiency: f64, // active_tasks / total_slots
}

/// Global registry for managing TaskSets across all domains
pub struct TaskSetRegistry {
    // TODO: is DashMap needed? Do we actually have a race condition here?
    domains: DashMap<String, TaskSet>,
}

impl TaskSetRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            domains: DashMap::new(),
        }
    }

    /// Get or create a TaskSet for the given domain
    pub fn get_or_create_domain(&self, domain: &str) -> dashmap::mapref::one::RefMut<String, TaskSet> {
        self.domains
            .entry(domain.to_string())
            .or_insert_with(|| TaskSet::new(domain.to_string()))
    }

    /// Get a TaskSet for the given domain (immutable)
    pub fn get_domain(&self, domain: &str) -> Option<dashmap::mapref::one::Ref<String, TaskSet>> {
        self.domains.get(domain)
    }

    /// Get a TaskSet for the given domain (mutable)
    pub fn get_domain_mut(&mut self, domain: &str) -> Option<dashmap::mapref::one::RefMut<String, TaskSet>> {
        self.domains.get_mut(domain)
    }

    /// Find a task across all domains
    pub fn find_task(&self, id: i64) -> Option<Task> {
        for entry in self.domains.iter() {
            if let Some(task) = entry.value().get_task(id) {
                return Some(task.clone());
            }
        }
        None
    }

    /// Get all domains
    pub fn domains(&self) -> dashmap::iter::Iter<String, TaskSet> {
        self.domains.iter()
    }

    /// Get the total number of tasks across all domains
    pub fn total_tasks(&self) -> usize {
        self.domains.iter().map(|entry| entry.value().len()).sum()
    }

    /// Get statistics for all domains
    pub fn all_stats(&self) -> Vec<(String, TaskSetStats)> {
        self.domains
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().stats()))
            .collect()
    }

    /// Efficiently load all tasks and attempts from the database using two queries.
    pub async fn load_from_db(&self, pool: &PgPool) -> Result<()> {
        log::info!("Starting to load tasks from database...");
        let client = pool.get().await?;

        // Step 1: Concurrently stream tasks and attempts, buffering them by domain.
        let (tasks_by_domain, attempts_by_domain) = tokio::try_join!(
            stream_and_group_by_domain(&client, "SELECT id, name, domain, created_at, flow_instance_id, retry_policy, args, kwargs, status FROM task_instance"),
            stream_and_group_by_domain(&client, "SELECT task_instance_id, domain, attempt, start_time, end_time, status FROM task_attempts")
        )?;

        let tasks_by_domain = Arc::new(tasks_by_domain);
        let attempts_by_domain = Arc::new(attempts_by_domain);

        // Step 2: Get all unique domains from both maps.
        let mut domains: Vec<_> = tasks_by_domain.keys().cloned().collect();
        for domain in attempts_by_domain.keys() {
            if !tasks_by_domain.contains_key(domain) {
                domains.push(domain.clone());
            }
        }

        // Step 3: Use Rayon to process each domain in parallel.
        let domain_task_sets: Vec<TaskSet> = domains
            .par_iter()
            .map(|domain| {
                let mut task_set = TaskSet::new(domain.to_string());

                // Process tasks for the current domain
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
                        };
                        task_set.upsert_task(task);
                    }
                }

                // Process attempts for the current domain
                if let Some(attempt_rows) = attempts_by_domain.get(domain) {
                    for row in attempt_rows {
                        let task_id: i64 = row.get("task_instance_id");
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

        // Step 4: Insert the fully constructed TaskSets into the DashMap.
        for task_set in domain_task_sets {
            self.domains.insert(task_set.domain().to_string(), task_set);
        }
        
        log::info!("Successfully loaded {} domains.", self.domains.len());
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
    fn test_task_creation_and_retrieval() {
        let mut task_set = TaskSet::new("test_domain".to_string());
        
        let task = Task {
            id: 123,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: vec!["arg1".to_string()],
            kwargs: JsonValue::Null,
            status: 0,
            attempts: vec![],
        };

        task_set.upsert_task(task);
        
        assert_eq!(task_set.len(), 1);
        assert!(task_set.get_task(123).is_some());
        assert!(task_set.get_task(456).is_none());
    }

    #[test]
    fn test_task_deletion() {
        let mut task_set = TaskSet::new("test_domain".to_string());
        
        let task = Task {
            id: 123,
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: vec![],
            kwargs: JsonValue::Null,
            status: 0,
            attempts: vec![],
        };

        task_set.upsert_task(task);
        assert_eq!(task_set.len(), 1);
        
        assert!(task_set.delete_task(123));
        assert_eq!(task_set.len(), 0);
        assert!(task_set.get_task(123).is_none());
        
        // Should have a gap available
        assert_eq!(task_set.gaps.len(), 1);
    }

    #[test]
    fn test_gap_reuse() {
        let mut task_set = TaskSet::new("test_domain".to_string());
        
        // Insert and delete a task
        let task1 = Task {
            id: 123,
            name: "task1".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: vec![],
            kwargs: JsonValue::Null,
            status: 0,
            attempts: vec![],
        };
        
        task_set.upsert_task(task1);
        task_set.delete_task(123);
        
        // Insert another task - should reuse the gap
        let task2 = Task {
            id: 456,
            name: "task2".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy: JsonValue::Null,
            args: vec![],
            kwargs: JsonValue::Null,
            status: 0,
            attempts: vec![],
        };
        
        task_set.upsert_task(task2);
        assert_eq!(task_set.len(), 1);
        assert_eq!(task_set.gaps.len(), 0); // Gap should be reused
    }
} 