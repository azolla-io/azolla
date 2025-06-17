use anyhow::Result;
use tonic::transport::Server;

mod db;
mod server;

use db::{create_pool, run_migrations, Settings};
use server::create_server;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();
    
    // Load configuration
    let settings = Settings::new().expect("Failed to load settings");
    let addr = format!("[::1]:{}", settings.server.port).parse()?;

    // Create and connect database pool
    let pool = create_pool(&settings).expect("Failed to create database pool");
    
    // Run database migrations
    run_migrations(&pool).await?;

    // Create the gRPC server
    let server = create_server(pool);
    
    log::info!("Azolla server listening on {}", addr);

    // Start the server
    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
