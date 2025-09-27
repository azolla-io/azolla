use anyhow::Result;
use clap::{Arg, Command};
use log::info;
use tokio::signal;

use azolla::shepherd::{load_config, start_shepherd};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = Command::new("azolla-shepherd")
        .version("0.1.0")
        .about("Azolla Shepherd - Worker daemon for executing tasks")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Configuration file path")
                .default_value("config/shepherd.toml"),
        )
        .arg(
            Arg::new("orchestrator")
                .short('o')
                .long("orchestrator")
                .value_name("ENDPOINT")
                .help("Orchestrator gRPC endpoint")
                .default_value("http://127.0.0.1:52710"),
        )
        .arg(
            Arg::new("max-concurrency")
                .short('m')
                .long("max-concurrency")
                .value_name("NUMBER")
                .help("Maximum concurrent tasks")
                .default_value("4"),
        )
        .arg(
            Arg::new("worker-port")
                .short('p')
                .long("worker-port")
                .value_name("PORT")
                .help("Port for worker gRPC service")
                .default_value("50052"),
        )
        .arg(
            Arg::new("worker-binary")
                .short('b')
                .long("worker-binary")
                .value_name("PATH")
                .help("Path to worker binary")
                .default_value("./azolla-worker"),
        )
        .arg(
            Arg::new("domain")
                .long("domain")
                .value_name("DOMAIN")
                .help("Domain this shepherd serves")
                .default_value("default"),
        )
        .arg(
            Arg::new("shepherd-group")
                .long("shepherd-group")
                .value_name("GROUP")
                .help("Shepherd group identifier")
                .default_value("default"),
        )
        .arg(
            Arg::new("log-level")
                .short('l')
                .long("log-level")
                .value_name("LEVEL")
                .help("Log level (error, warn, info, debug, trace)")
                .default_value("info"),
        )
        .get_matches();

    // Load configuration from file and CLI args
    let config_path = matches.get_one::<String>("config");
    let config = load_config(config_path.map(|s| s.as_str()), &matches)?;

    // Start shepherd using the reusable function
    let shepherd_instance = start_shepherd(config).await?;

    // Wait for shutdown signal
    info!(
        "Shepherd {} is running. Press Ctrl+C to shutdown.",
        shepherd_instance.config.uuid
    );
    signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping shepherd...");

    if let Err(e) = shepherd_instance.shutdown().await {
        log::error!("Failed to send shutdown signal: {e}");
    }

    shepherd_instance.join().await?;

    info!("Azolla Shepherd shutdown complete");
    Ok(())
}
