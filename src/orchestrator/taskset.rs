/*!
# TaskSet Requirements & Design

## Requirements
1. **Write-heavy workload**: Frequent event processing updates to tasks
2. **Concurrent access**: Multiple gRPC handlers accessing same Task simultaneously
3. **High performance**: High throughput with low latency

## Design Solution
1. **Actor model**: One thread per domain eliminates write lock contention
2. **Message passing**: Concurrent access via channels, no shared locks
3. **Single-threaded processing**: No contention within domain, consistent latency
*/

use crate::orchestrator::db::PgPool;
use crate::{
    ATTEMPT_STATUS_FAILED, ATTEMPT_STATUS_STARTED, ATTEMPT_STATUS_SUCCEEDED,
    EVENT_TASK_ATTEMPT_ENDED, EVENT_TASK_ATTEMPT_STARTED, EVENT_TASK_CREATED, EVENT_TASK_ENDED,
    EVENT_TASK_STARTED, TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT, TASK_STATUS_ATTEMPT_STARTED,
    TASK_STATUS_CREATED,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::StreamExt;
use rayon::prelude::*;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::Row;
use uuid::Uuid;

use log::{debug, info};

#[derive(Debug, Clone)]
pub struct TaskAttempt {
    pub attempt: i32,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: i16,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub flow_instance_id: Option<Uuid>,
    pub retry_policy: JsonValue,
    pub args: Vec<String>,
    pub kwargs: JsonValue,
    pub status: i16,
    pub attempts: Vec<TaskAttempt>,
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
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 0,
            attempts: Vec::new(),
        }
    }
}

/// Gap buffer storage for tasks within a domain
pub struct TaskSet {
    domain: String,
    tasks: Vec<Task>,
    id_to_index: HashMap<Uuid, usize>,
    gaps: Vec<usize>,
    // Status-based indices for efficient scheduler scanning
    created_tasks: HashSet<Uuid>,        // TASK_STATUS_CREATED
    started_tasks: HashSet<Uuid>,        // TASK_STATUS_ATTEMPT_STARTED
    retry_eligible_tasks: HashSet<Uuid>, // TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
}

impl TaskSet {
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            tasks: Vec::new(),
            id_to_index: HashMap::new(),
            gaps: Vec::new(),
            created_tasks: HashSet::new(),
            started_tasks: HashSet::new(),
            retry_eligible_tasks: HashSet::new(),
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
        let status = task.status;

        match self.id_to_index.entry(id) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let index = *entry.get();
                // Remove from old status index
                self.remove_from_status_indices(id, self.tasks[index].status);
                self.tasks[index] = task;
                // Add to new status index
                self.add_to_status_index(id, status);
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
                // Add to status index
                self.add_to_status_index(id, status);
            }
        }
    }

    pub fn delete_task(&mut self, id: Uuid) -> bool {
        if let Some(&index) = self.id_to_index.get(&id) {
            // Remove from status indices
            self.remove_from_status_indices(id, self.tasks[index].status);
            self.tasks[index].clear();
            self.gaps.push(index);
            self.id_to_index.remove(&id);
            true
        } else {
            false
        }
    }

    /// Add task to appropriate status index
    fn add_to_status_index(&mut self, task_id: Uuid, status: i16) {
        match status {
            TASK_STATUS_CREATED => {
                self.created_tasks.insert(task_id);
            }
            TASK_STATUS_ATTEMPT_STARTED => {
                self.started_tasks.insert(task_id);
            }
            TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT => {
                self.retry_eligible_tasks.insert(task_id);
            }
            _ => {
                // No index needed for completed/failed states
            }
        }
    }

    /// Remove task from all status indices
    fn remove_from_status_indices(&mut self, task_id: Uuid, status: i16) {
        match status {
            TASK_STATUS_CREATED => {
                self.created_tasks.remove(&task_id);
            }
            TASK_STATUS_ATTEMPT_STARTED => {
                self.started_tasks.remove(&task_id);
            }
            TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT => {
                self.retry_eligible_tasks.remove(&task_id);
            }
            _ => {
                // Remove from all indices to be safe
                self.created_tasks.remove(&task_id);
                self.started_tasks.remove(&task_id);
                self.retry_eligible_tasks.remove(&task_id);
            }
        }
    }

    /// Get tasks in CREATED status for scheduler processing
    pub fn get_created_tasks(&self) -> Vec<Uuid> {
        self.created_tasks.iter().copied().collect()
    }

    /// Get tasks in ATTEMPT_STARTED status for timeout tracking
    pub fn get_started_tasks(&self) -> Vec<Uuid> {
        self.started_tasks.iter().copied().collect()
    }

    /// Get tasks eligible for retry
    pub fn get_retry_eligible_tasks(&self) -> Vec<Uuid> {
        self.retry_eligible_tasks.iter().copied().collect()
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
        let mut new_id_to_index = HashMap::with_capacity(self.id_to_index.len());

        // Create a temporary default task for swapping
        let mut temp_task = Task::new();

        for (id, &old_index) in &self.id_to_index {
            let new_index = new_tasks.len();
            // Use mem::swap to move instead of clone
            std::mem::swap(&mut temp_task, &mut self.tasks[old_index]);
            new_tasks.push(temp_task);
            temp_task = Task::new(); // Reset for next iteration
            new_id_to_index.insert(*id, new_index);
        }

        self.tasks = new_tasks;
        self.id_to_index = new_id_to_index;
        self.gaps.clear();

        // Rebuild status indices
        self.created_tasks.clear();
        self.started_tasks.clear();
        self.retry_eligible_tasks.clear();

        // Collect task data first to avoid borrowing issues
        let task_data: Vec<(Uuid, i16)> = self
            .tasks
            .iter()
            .filter(|task| !task.is_empty())
            .map(|task| (task.id, task.status))
            .collect();

        for (task_id, status) in task_data {
            self.add_to_status_index(task_id, status);
        }
    }

    pub fn reset(&mut self) {
        self.tasks.iter_mut().for_each(|t| t.clear());
        self.id_to_index.clear();
        self.gaps.clear();
        self.created_tasks.clear();
        self.started_tasks.clear();
        self.retry_eligible_tasks.clear();
    }
}

#[derive(Debug, Clone)]
pub struct TaskSetStats {
    pub total_slots: usize,
    pub active_tasks: usize,
    pub gap_slots: usize,
    pub memory_efficiency: f64,
}

pub enum TaskSetCommand {
    GetTask {
        id: Uuid,
        respond_to: oneshot::Sender<Option<Task>>,
    },
    GetTaskStatus {
        id: Uuid,
        respond_to: oneshot::Sender<Option<i16>>,
    },
    UpsertTask {
        task: Task,
        respond_to: oneshot::Sender<()>,
    },
    DeleteTask {
        id: Uuid,
        respond_to: oneshot::Sender<bool>,
    },
    GetStats {
        respond_to: oneshot::Sender<TaskSetStats>,
    },
    Len {
        respond_to: oneshot::Sender<usize>,
    },
    AllTasks {
        respond_to: oneshot::Sender<Vec<Task>>,
    },
    IsEmpty {
        respond_to: oneshot::Sender<bool>,
    },
    Compact {
        respond_to: oneshot::Sender<()>,
    },
    Reset {
        respond_to: oneshot::Sender<()>,
    },
    GetCreatedTasks {
        respond_to: oneshot::Sender<Vec<Uuid>>,
    },
    GetStartedTasks {
        respond_to: oneshot::Sender<Vec<Uuid>>,
    },
    GetRetryEligibleTasks {
        respond_to: oneshot::Sender<Vec<Uuid>>,
    },
}

pub struct TaskSetActor {
    sender: mpsc::Sender<TaskSetCommand>,
    domain: String,
}

#[derive(Debug)]
pub enum ActorError {
    ChannelClosed,
    ResponseLost,
}

impl TaskSetActor {
    pub fn new(domain: String) -> Self {
        let taskset = TaskSet::new(domain.clone());
        Self::from_taskset(taskset)
    }

    /// Create actor from existing TaskSet
    pub fn from_taskset(taskset: TaskSet) -> Self {
        let (sender, mut receiver) = mpsc::channel(1000);
        let domain = taskset.domain().to_string();

        tokio::spawn(async move {
            let mut taskset = taskset;

            while let Some(command) = receiver.recv().await {
                match command {
                    TaskSetCommand::GetTask { id, respond_to } => {
                        let result = taskset.get_task(id).cloned();
                        let _ = respond_to.send(result);
                    }
                    TaskSetCommand::GetTaskStatus { id, respond_to } => {
                        let status = taskset.get_task(id).map(|task| task.status);
                        let _ = respond_to.send(status);
                    }
                    TaskSetCommand::UpsertTask { task, respond_to } => {
                        taskset.upsert_task(task);
                        let _ = respond_to.send(());
                    }
                    TaskSetCommand::DeleteTask { id, respond_to } => {
                        let result = taskset.delete_task(id);
                        let _ = respond_to.send(result);
                    }
                    TaskSetCommand::GetStats { respond_to } => {
                        let stats = taskset.stats();
                        let _ = respond_to.send(stats);
                    }
                    TaskSetCommand::Len { respond_to } => {
                        let len = taskset.len();
                        let _ = respond_to.send(len);
                    }
                    TaskSetCommand::AllTasks { respond_to } => {
                        let task_ids: Vec<Uuid> = taskset.all_tasks().map(|t| t.id).collect();
                        let tasks: Vec<Task> = task_ids
                            .into_iter()
                            .filter_map(|id| taskset.get_task(id).cloned())
                            .collect();
                        let _ = respond_to.send(tasks);
                    }
                    TaskSetCommand::IsEmpty { respond_to } => {
                        let is_empty = taskset.is_empty();
                        let _ = respond_to.send(is_empty);
                    }
                    TaskSetCommand::Compact { respond_to } => {
                        taskset.compact();
                        let _ = respond_to.send(());
                    }
                    TaskSetCommand::Reset { respond_to } => {
                        taskset.reset();
                        let _ = respond_to.send(());
                    }
                    TaskSetCommand::GetCreatedTasks { respond_to } => {
                        let tasks = taskset.get_created_tasks();
                        let _ = respond_to.send(tasks);
                    }
                    TaskSetCommand::GetStartedTasks { respond_to } => {
                        let tasks = taskset.get_started_tasks();
                        let _ = respond_to.send(tasks);
                    }
                    TaskSetCommand::GetRetryEligibleTasks { respond_to } => {
                        let tasks = taskset.get_retry_eligible_tasks();
                        let _ = respond_to.send(tasks);
                    }
                }
            }
        });

        Self { sender, domain }
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub async fn get_task(&self, id: Uuid) -> Result<Option<Task>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetTask { id, respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn get_task_status(&self, id: Uuid) -> Result<Option<i16>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetTaskStatus { id, respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn upsert_task(&self, task: Task) -> Result<(), ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::UpsertTask {
            task,
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)?;
        Ok(())
    }

    pub async fn delete_task(&self, id: Uuid) -> Result<bool, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::DeleteTask { id, respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn stats(&self) -> Result<TaskSetStats, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetStats { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn len(&self) -> Result<usize, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::Len { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn all_tasks(&self) -> Result<Vec<Task>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::AllTasks { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn is_empty(&self) -> Result<bool, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::IsEmpty { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn compact(&self) -> Result<(), ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::Compact { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)?;
        Ok(())
    }

    pub async fn reset(&self) -> Result<(), ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::Reset { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)?;
        Ok(())
    }

    pub async fn get_created_tasks(&self) -> Result<Vec<Uuid>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetCreatedTasks { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn get_started_tasks(&self) -> Result<Vec<Uuid>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetStartedTasks { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }

    pub async fn get_retry_eligible_tasks(&self) -> Result<Vec<Uuid>, ActorError> {
        let (tx, rx) = oneshot::channel();
        let cmd = TaskSetCommand::GetRetryEligibleTasks { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| ActorError::ChannelClosed)?;
        rx.await.map_err(|_| ActorError::ResponseLost)
    }
}

pub struct TaskSetRegistry {
    domains: std::sync::Mutex<HashMap<String, TaskSetActor>>,
    // Shepherd tracking
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

    pub fn get_or_create_domain(&self, domain: &str) -> TaskSetActor {
        let mut domains = self.domains.lock().unwrap();

        if let Some(actor) = domains.get(domain) {
            TaskSetActor {
                sender: actor.sender.clone(),
                domain: domain.to_string(),
            }
        } else {
            let actor = TaskSetActor::new(domain.to_string());
            let actor_handle = TaskSetActor {
                sender: actor.sender.clone(),
                domain: domain.to_string(),
            };
            domains.insert(domain.to_string(), actor_handle);
            actor
        }
    }

    pub fn get_domain(&self, domain: &str) -> Option<TaskSetActor> {
        let domains = self.domains.lock().unwrap();
        domains.get(domain).map(|actor| TaskSetActor {
            sender: actor.sender.clone(),
            domain: domain.to_string(),
        })
    }

    pub async fn find_task(&self, id: Uuid) -> Option<Task> {
        let domain_actors: Vec<_> = {
            let domains = self.domains.lock().unwrap();
            domains
                .values()
                .map(|actor| TaskSetActor {
                    sender: actor.sender.clone(),
                    domain: actor.domain.clone(),
                })
                .collect()
        };

        for actor in domain_actors {
            if let Ok(Some(task)) = actor.get_task(id).await {
                return Some(task);
            }
        }
        None
    }

    pub fn domains(&self) -> Vec<String> {
        let domains = self.domains.lock().unwrap();
        domains.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        let domains = self.domains.lock().unwrap();
        domains.len()
    }

    pub fn is_empty(&self) -> bool {
        let domains = self.domains.lock().unwrap();
        domains.is_empty()
    }

    pub async fn total_tasks(&self) -> usize {
        let domain_actors: Vec<_> = {
            let domains = self.domains.lock().unwrap();
            domains
                .values()
                .map(|actor| TaskSetActor {
                    sender: actor.sender.clone(),
                    domain: actor.domain.clone(),
                })
                .collect()
        };

        let mut total = 0;
        for actor in domain_actors {
            if let Ok(len) = actor.len().await {
                total += len;
            }
        }
        total
    }

    pub async fn all_stats(&self) -> Vec<(String, TaskSetStats)> {
        let domain_actors: Vec<_> = {
            let domains = self.domains.lock().unwrap();
            domains
                .iter()
                .map(|(name, actor)| {
                    (
                        name.clone(),
                        TaskSetActor {
                            sender: actor.sender.clone(),
                            domain: actor.domain.clone(),
                        },
                    )
                })
                .collect()
        };

        let mut stats = Vec::new();
        for (name, actor) in domain_actors {
            if let Ok(actor_stats) = actor.stats().await {
                stats.push((name, actor_stats));
            }
        }
        stats
    }

    /// Track that a task has been dispatched to a shepherd
    pub fn track_task_dispatch(&self, task_id: Uuid, shepherd_uuid: Uuid) {
        self.task_to_shepherd.insert(task_id, shepherd_uuid);
        self.shepherd_tasks
            .entry(shepherd_uuid)
            .or_default()
            .insert(task_id);
        log::debug!("Tracked task {task_id} dispatched to shepherd {shepherd_uuid}");
    }

    /// Remove task from shepherd tracking (when task completes)
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

    /// Get all tasks assigned to a shepherd
    pub fn get_shepherd_tasks(&self, shepherd_uuid: Uuid) -> Vec<Uuid> {
        self.shepherd_tasks
            .get(&shepherd_uuid)
            .map(|tasks| tasks.clone().into_iter().collect())
            .unwrap_or_default()
    }

    /// Generate failure events for all tasks from a dead shepherd (events only, no TaskSet updates)
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
            if let Some(task) = self.find_task(task_id).await {
                // Find which domain contains this task
                if let Some(domain_actor) = self.get_domain_for_task(task_id).await {
                    let domain = domain_actor.domain().to_string();

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

                    // Write failure event only (no TaskSet updates)
                    if let Err(e) = event_stream.write_event(event_record).await {
                        log::error!("Failed to write task failure event for {task_id}: {e}");
                        continue;
                    }

                    failed_task_ids.push(task_id);

                    // Remove from tracking immediately (no need to wait for scheduler)
                    self.untrack_task(task_id);
                }
            }
        }

        Ok(failed_task_ids)
    }

    /// Fail all tasks from a dead shepherd (DEPRECATED - use generate_shepherd_failure_events)
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
            if let Some(task) = self.find_task(task_id).await {
                // Find which domain contains this task
                if let Some(domain_actor) = self.get_domain_for_task(task_id).await {
                    let domain = domain_actor.domain().to_string();

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

                    // Write failure event first
                    if let Err(e) = event_stream.write_event(event_record).await {
                        log::error!("Failed to write task failure event for {task_id}: {e}");
                        continue;
                    }

                    // Placeholder for task failure handling in domain actor
                    if let Err(e) = self
                        .handle_task_failure(&domain_actor, task_id, "shepherd_crashed")
                        .await
                    {
                        log::error!("Failed to update TaskSet for failed task {task_id}: {e}");
                    }

                    // Remove from tracking
                    self.untrack_task(task_id);
                }
            }
        }

        Ok(())
    }

    /// Helper to get domain actor for a specific task
    async fn get_domain_for_task(&self, task_id: Uuid) -> Option<TaskSetActor> {
        let domain_actors: Vec<_> = {
            let domains = self.domains.lock().unwrap();
            domains
                .values()
                .map(|actor| TaskSetActor {
                    sender: actor.sender.clone(),
                    domain: actor.domain.clone(),
                })
                .collect()
        };

        for actor in domain_actors {
            if let Ok(Some(_)) = actor.get_task(task_id).await {
                return Some(actor);
            }
        }
        None
    }

    /// Placeholder method for handling task failure
    async fn handle_task_failure(
        &self,
        _domain_actor: &TaskSetActor,
        task_id: Uuid,
        failure_reason: &str,
    ) -> Result<()> {
        log::info!("Task {task_id} failed due to: {failure_reason}");
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

        let mut domain_actors = HashMap::new();
        for task_set in domain_task_sets {
            let domain_name = task_set.domain().to_string();
            let actor = TaskSetActor::from_taskset(task_set);
            domain_actors.insert(domain_name, actor);
        }

        {
            let mut domains_guard = self.domains.lock().unwrap();
            *domains_guard = domain_actors;
        }

        // TODO: Reconstruct shepherd assignments from unconsumed events
        // This will replay events after the last materialization to rebuild shepherd tracking state

        log::info!("Successfully loaded {} domains.", self.len());
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
                        THEN ARRAY(SELECT jsonb_array_elements_text(e.metadata->'args'))
                        ELSE ARRAY[]::text[]
                    END,
                    COALESCE((e.metadata->'kwargs')::jsonb, '{}'::jsonb),
                    0::smallint,
                    e.flow_instance_id
                FROM events e
                WHERE e.event_id > $1 
                  AND e.event_id <= $2
                  AND e.event_type = $3
                  AND e.task_instance_id IS NOT NULL
                ON CONFLICT (id, domain) DO NOTHING
                "#,
                &[&last_processed_event_id, &max_event_id, &EVENT_TASK_CREATED]
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
                    crate::ATTEMPT_STATUS_STARTED::smallint
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
                    status = COALESCE((e.metadata->>'attempt_status')::smallint, crate::ATTEMPT_STATUS_FAILED::smallint)
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
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
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
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
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
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
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
            args: Vec::new(),
            kwargs: JsonValue::Null,
            status: 1,
            attempts: Vec::new(),
        };

        task_set.upsert_task(task2);

        assert_eq!(task_set.len(), 1);
        assert_eq!(task_set.gaps.len(), 0);
    }
}
