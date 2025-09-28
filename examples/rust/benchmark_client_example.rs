use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use azolla_client::{Client, TaskExecutionResult};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "benchmark-client-example")]
#[command(about = "Submits trivial_task as fast as possible to measure throughput", long_about = None)]
struct BenchmarkArgs {
    /// Orchestrator gRPC endpoint
    #[arg(long, default_value = "http://localhost:52710")]
    orchestrator_endpoint: String,

    /// Target domain for task submissions
    #[arg(long, default_value = "default")]
    domain: String,

    /// Total number of task submissions to perform
    #[arg(long, default_value_t = 10_000)]
    total_requests: u64,

    /// Number of worker threads issuing requests
    #[arg(long, default_value_t = default_thread_count())]
    threads: usize,

    /// Timeout (seconds) for individual task completion
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,

    /// Interval (seconds) for throughput reporting
    #[arg(long, default_value_t = 5)]
    report_interval_secs: u64,
}

fn default_thread_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = BenchmarkArgs::parse();
    let BenchmarkArgs {
        orchestrator_endpoint,
        domain,
        total_requests,
        threads,
        request_timeout_secs,
        report_interval_secs,
    } = args;

    if total_requests == 0 {
        return Err(anyhow!("total-requests must be greater than zero"));
    }
    if threads == 0 {
        return Err(anyhow!("threads must be at least one"));
    }

    let max_threads = usize::try_from(total_requests).unwrap_or(usize::MAX);
    let thread_count = threads.min(max_threads).max(1);

    let report_interval = Duration::from_secs(report_interval_secs.max(1));
    let timeout = Duration::from_secs(request_timeout_secs.max(1));

    let endpoint = Arc::new(orchestrator_endpoint);
    let domain = Arc::new(domain);

    let endpoint_display = endpoint.as_ref();
    let domain_display = domain.as_ref();
    log::info!("Starting benchmark with total_requests={total_requests} threads={thread_count} endpoint={endpoint_display} domain={domain_display}");

    let completed = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(thread_count + 1));

    let mut handles = Vec::with_capacity(thread_count);
    let base_per_thread = total_requests / thread_count as u64;
    let remainder = (total_requests % thread_count as u64) as usize;

    for idx in 0..thread_count {
        let thread_requests = base_per_thread + if idx < remainder { 1 } else { 0 };
        if thread_requests == 0 {
            continue;
        }

        let endpoint = Arc::clone(&endpoint);
        let domain = Arc::clone(&domain);
        let completed_counter = Arc::clone(&completed);
        let start_barrier = Arc::clone(&barrier);
        let thread_shutdown = Arc::clone(&shutdown);

        let handle = thread::spawn(move || -> anyhow::Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build Tokio runtime")?;

            start_barrier.wait();

            runtime.block_on(async {
                let client = Client::builder()
                    .endpoint(endpoint.as_ref())
                    .domain(domain.as_ref())
                    .timeout(timeout)
                    .build()
                    .await
                    .context("failed to construct client")?;

                for _ in 0..thread_requests {
                    if thread_shutdown.load(Ordering::Acquire) {
                        break;
                    }

                    let handle = client
                        .submit_task("trivial_task")
                        .args(())
                        .context("failed to encode trivial_task arguments")?
                        .submit()
                        .await
                        .context("failed to submit trivial_task")?;

                    match handle
                        .wait()
                        .await
                        .context("failed to wait for task result")?
                    {
                        TaskExecutionResult::Success(value) => {
                            if value != serde_json::Value::Bool(true) {
                                return Err(anyhow!("unexpected task result: {value}"));
                            }
                        }
                        TaskExecutionResult::Failed(error) => {
                            return Err(anyhow!("task failed: {error:?}"));
                        }
                    }

                    completed_counter.fetch_add(1, Ordering::Relaxed);
                }

                Ok::<(), anyhow::Error>(())
            })
        });

        handles.push(handle);
    }

    barrier.wait();
    let start_time = Instant::now();

    let reporter_completed = Arc::clone(&completed);
    let reporter_shutdown = Arc::clone(&shutdown);
    let reporter_handle = thread::spawn(move || {
        let mut last_check = Instant::now();
        let mut last_count = 0u64;

        loop {
            thread::sleep(report_interval);

            if reporter_shutdown.load(Ordering::Acquire) {
                break;
            }

            let total = reporter_completed.load(Ordering::Relaxed);
            let elapsed = last_check.elapsed().as_secs_f64();
            let interval_count = total.saturating_sub(last_count);
            let interval_rps = if elapsed > 0.0 {
                interval_count as f64 / elapsed
            } else {
                0.0
            };
            let total_elapsed = start_time.elapsed().as_secs_f64();
            let sustained_rps = if total_elapsed > 0.0 {
                total as f64 / total_elapsed
            } else {
                0.0
            };

            log::info!("progress total_completed={total} interval_rps={interval_rps:.2} sustained_rps={sustained_rps:.2}");

            last_check = Instant::now();
            last_count = total;
        }
    });

    let mut thread_errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                thread_errors.push(err);
                shutdown.store(true, Ordering::Release);
            }
            Err(panic) => {
                shutdown.store(true, Ordering::Release);
                thread_errors.push(anyhow!("thread panicked: {panic:?}"));
            }
        }
    }

    shutdown.store(true, Ordering::Release);
    if let Err(err) = reporter_handle.join() {
        log::warn!("reporter thread panicked: {err:?}");
    }

    if !thread_errors.is_empty() {
        for err in &thread_errors {
            log::error!("thread error: {err}");
        }
        return Err(anyhow!("benchmark failed"));
    }

    let total_elapsed = start_time.elapsed().as_secs_f64();
    let total_completed = completed.load(Ordering::Relaxed);
    let sustained_rps = if total_elapsed > 0.0 {
        total_completed as f64 / total_elapsed
    } else {
        0.0
    };

    log::info!("benchmark complete total_requests={total_completed} elapsed_secs={total_elapsed:.2} sustained_rps={sustained_rps:.2}");

    Ok(())
}
