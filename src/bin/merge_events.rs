use anyhow::Result;
use clap::Parser;
use std::time::Instant;

// Import the azolla modules
use azolla::orchestrator::db::{create_pool, PgPool, Settings};
use azolla::orchestrator::taskset::TaskSetRegistry;

#[derive(Parser)]
#[command(name = "merge_events")]
#[command(
    about = "Utility to merge events from events table to task_instance and task_attempts tables"
)]
struct Args {
    /// Show verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug)]
struct MergeStats {
    last_event_id: i64,
    total_events: i64,
    unprocessed_events: i64,
}

async fn get_merge_stats(pool: &PgPool) -> Result<MergeStats> {
    let client = pool.get().await?;

    // Get the current cursor position
    let cursor_row = client
        .query_opt("SELECT last_event_id FROM merge_cursor LIMIT 1", &[])
        .await?;

    let last_event_id = cursor_row.map(|row| row.get::<_, i64>(0)).unwrap_or(0);

    // Get total events count
    let total_events_row = client.query_one("SELECT COUNT(*) FROM events", &[]).await?;
    let total_events: i64 = total_events_row.get(0);

    // Get unprocessed events count
    let unprocessed_events_row = client
        .query_one(
            "SELECT COUNT(*) FROM events WHERE event_id > $1",
            &[&last_event_id],
        )
        .await?;
    let unprocessed_events: i64 = unprocessed_events_row.get(0);

    Ok(MergeStats {
        last_event_id,
        total_events,
        unprocessed_events,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    if args.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    }

    // Load configuration
    let settings = Settings::new().expect("Failed to load settings");
    if args.verbose {
        println!("Loaded settings: {settings:?}");
    }

    // Create database pool
    let pool = create_pool(&settings).expect("Failed to create database pool");
    if args.verbose {
        println!("Connected to database");
    }

    // Create the TaskSetRegistry (which contains the merge logic)
    let registry = TaskSetRegistry::new();

    println!("Starting event merge process...");

    // Get initial statistics
    let start_time = Instant::now();
    let initial_stats = get_merge_stats(&pool).await?;

    if args.verbose {
        println!("Initial state:");
        println!("  Last processed event ID: {}", initial_stats.last_event_id);
        println!("  Total events in table: {}", initial_stats.total_events);
        println!("  Unprocessed events: {}", initial_stats.unprocessed_events);
    }

    // Trigger the merge
    match registry.merge_events_to_db(&pool).await {
        Ok(()) => {
            let duration = start_time.elapsed();
            let final_stats = get_merge_stats(&pool).await?;

            let events_processed = final_stats.last_event_id - initial_stats.last_event_id;

            println!("✅ Event merge completed successfully");
            println!("📊 Statistics:");
            println!("  Duration: {:.2}s", duration.as_secs_f64());
            println!("  Events processed: {events_processed}");

            if events_processed > 0 {
                let events_per_second = events_processed as f64 / duration.as_secs_f64();
                println!("  Processing rate: {events_per_second:.0} events/second");
            }

            if args.verbose {
                println!(
                    "  Final last processed event ID: {}",
                    final_stats.last_event_id
                );
                println!(
                    "  Remaining unprocessed events: {}",
                    final_stats.unprocessed_events
                );
            }
        }
        Err(e) => {
            let duration = start_time.elapsed();
            eprintln!(
                "❌ Event merge failed after {:.2}s: {}",
                duration.as_secs_f64(),
                e
            );
            std::process::exit(1);
        }
    }

    if args.verbose {
        println!("Event merge utility finished");
    }

    Ok(())
}
