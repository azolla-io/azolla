use std::future::Future;
use std::pin::Pin;

use azolla_client::{task::Task, TaskError, TaskResult, Worker, WorkerInvocation};
use clap::Parser;
use serde_json::Value;

/// Minimal task that always returns `true`.
pub struct TrivialTask;

impl Task for TrivialTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "trivial_task"
    }

    fn parse_args(args: Vec<Value>, _kwargs: Value) -> Result<Self::Args, TaskError> {
        if args.is_empty() {
            Ok(())
        } else {
            Err(TaskError::invalid_args(
                "trivial_task does not accept positional arguments",
            ))
        }
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async { Ok(Value::Bool(true)) })
    }
}

#[derive(Parser, Debug)]
#[command(name = "benchmark-worker-example")]
#[command(about = "Runs the trivial_task implementation for benchmarking", long_about = None)]
struct Cli {
    /// Execute a single invocation locally using the provided task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Task name to execute when running a single invocation
    #[arg(long, default_value = "trivial_task")]
    task_name: String,

    /// Positional arguments as JSON
    #[arg(long, default_value = "[]")]
    args: String,

    /// Keyword arguments as JSON
    #[arg(long, default_value = "{}")]
    kwargs: String,

    /// Shepherd endpoint to report results when running a single invocation
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    shepherd_endpoint: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    let worker = Worker::builder().register_task(TrivialTask).build();
    let registered = worker.task_count();
    log::info!("Registered {registered} task implementations");

    if let Some(task_id) = cli.task_id {
        let invocation = WorkerInvocation::from_json(
            &task_id,
            &cli.task_name,
            &cli.args,
            &cli.kwargs,
            &cli.shepherd_endpoint,
        )?;
        worker.run_invocation(invocation).await?;
    } else {
        log::info!("No task invocation requested; worker is ready for shepherd-managed execution");
    }

    Ok(())
}
