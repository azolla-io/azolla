# Open Issues Backup

## 84 - [low] increase testing coverage to 85%
No description provided.

## 81 - [low] azolla-local
build a lightweight, self-contained, docker based local environment for users to setup and test out azolla in 3 minutes.

## 79 - [high] task logging backend
Develop a generic logging backend that supports task logging on various storage: local disk, block storage, or OpenSearch/ElasticSearch, etc.

We'll begin with block storage (simplest to implement, no need to support retrieval from worker machines), then local/OpenSearch. The Shepherd takes over worker's stdout, and upload it to S3.

A structured S3 log file naming pattern must be defined, and preferably configurable (e.g. `{date}-{time}-{task name}-{task id}`)

Note that we will support duplicating logs to stdout so that it can be picked up log agents.

## 77 - [low] Perf optimization: support resident parent process + forkserver for Azolla Worker
No description provided.

## 75 - [low] Support TaskContext
No description provided.

## 73 - [high] Purge tasks
# Stage 1: Basic Purge
When a task meets the following condition, it should be purged from TaskSet to save memory
- Task execution finished
AND
- Task result has been fetched || Task result has been set more than TASK_RESULT_FETCH_MAX_WAIT

# Stage 2: More Aggressive Purge
When system is running out of memory, we do not need to wait for TASK_RESULT_FETCH_MAX_WAIT, but implement a more aggressive "oldest first" purge approach where we purge the the task that has result set the earliest first

## 72 - [high] Task ID should be generated from server side
- To avoid client submitting same task ID multiple times
- To avoid reimplementing ID generation across different languages
- The task ID should contain meaningful semantics, such as time/domain, so that it can uniquely identify different task runs of the same task definition

## 70 - [low] Distributed Azolla
# Scaling Model
- one domain can only be handled by one instance
- one instance can hold a variable number of domains

# Elasticity
- scale in/out: instance join/left, rebalance domain to instance mapping

# Routing
- if an instance received a request for domain that it does not own: (1) return redirect; or (2) forward to owner instance

## 63 - [low] DAG Style Workflow
- With Workflow, do we still need a TaskContext?
- How do we support dynamic DAG?
- How do we pass results from one task to the other?

## 55 - [low] handle shepherd death
prerequisite: https://github.com/azolla-io/azolla/issues/50

previous attempt: https://github.com/azolla-io/azolla/pulls?q=is%3Apr+is%3Aclosed

## 51 - [low] performance isolation: per domain rate limiting and per-task rate limiting
# Per-Domain Rate Limiting
Each domain maybe owned by different teams. A noisy neighbor can send a large number of requests and take all the bandwidth of the database.

We need to support per-domain rate limiting. Since database is the only bottleneck of the system, rate limiting by number of Events can be a perfect way to ensure fairness.

Alternative: rate limit by number of tasks created per period of time.
- pros:
  - easier for users to understand
- cons:
  - not exactly map to database usage

# Per-Task Rate Limiting
Within each domain, we don't want one task to take all / most of worker resources. There can be two approaches
1. We can achieve this by supporting a max concurrency for each task, and apply a default max concurrency. With this, users can easily avoid the one task take all worker issue. But this does not directly translate to task throughput.
2. We can do task-based rate limiting (number of tasks / s), this gives a better understanding of task throughput, but does not allow intuit settings to avoid one task take all worker issue.

We can also combine both.

## 45 - [low] doc: explain how Azolla is different from conventional DB-based scheduler
- throughput
- latency

## 39 - [critical] Perf Benchmark
create a integration test to stress test the system and report the throughput:
- create light weight tasks with random number of retries (which means some of the tasks will fail)
- create a large number of tasks (1000) and send in parallel (10 concurrent clients)
- count the number of succeeded and failed tasks and verify they are same as expected

run the benchmark on EC2/RDS. Create a k8s chart for deployment.
