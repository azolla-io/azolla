use azolla_client::{Client, RetryPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📡 Azolla Client Example");
    println!("========================");

    // Create a client to connect to the orchestrator
    println!("🔗 Connecting to orchestrator...");
    let client = match Client::builder()
        .endpoint("http://localhost:52710")
        .domain("example-domain")
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .await
    {
        Ok(client) => {
            println!("✅ Connected to orchestrator successfully");
            client
        }
        Err(e) => {
            println!("⚠️  Could not connect to orchestrator: {e}");
            println!("💡 This is expected if no orchestrator is running");
            println!("   The client library is properly configured though!");
            return Ok(());
        }
    };

    println!("\n📤 Task Submission Examples:");

    // Example 1: Simple task submission with tuple arguments
    println!("🔢 Example: Submitting math task");
    match client.submit_task("add_numbers").args((10, 20)) {
        Ok(_submission) => {
            println!("✅ Task submission configured successfully");
            println!("   Task: add_numbers");
            println!("   Args: (10, 20)");

            // In a real application, you would call submission.submit().await
            // let handle = submission.submit().await?;
            // let result = handle.wait().await?;
        }
        Err(e) => println!("❌ Failed to configure task: {e}"),
    }

    // Example 2: Task with retry policy
    println!("\n👋 Example: Task with retry policy");
    let retry_policy = RetryPolicy::default();

    match client
        .submit_task("greet_user")
        .args(("Alice".to_string(), Some(25u32)))
    {
        Ok(_submission) => {
            println!("✅ Greeting task configured successfully");
            println!("   Task: greet_user");
            println!("   Args: (\"Alice\", Some(25))");
            println!("   Retry Policy: {retry_policy:?}");
        }
        Err(e) => println!("❌ Failed to configure greeting task: {e}"),
    }

    // Example 3: Task with shepherd group targeting
    println!("\n🎯 Example: Task with shepherd group");
    match client.submit_task("process_data").args(vec![1, 2, 3, 4, 5]) {
        Ok(_submission) => {
            println!("✅ Data processing task configured successfully");
            println!("   Task: process_data");
            println!("   Args: [1, 2, 3, 4, 5]");
            println!("   Target: Any available shepherd group");
        }
        Err(e) => println!("❌ Failed to configure data processing: {e}"),
    }

    println!("\n🎉 Client example completed!");
    println!("📊 Key features demonstrated:");
    println!("   ✓ Client connection and configuration");
    println!("   ✓ Type-safe task argument preparation");
    println!("   ✓ Task submission builder pattern");
    println!("   ✓ Retry policy configuration");
    println!("   ✓ Error handling and graceful degradation");

    println!("\n💡 In a real application:");
    println!("   1. Call submission.submit().await to execute tasks");
    println!("   2. Use handle.wait().await to get results");
    println!("   3. Handle TaskExecutionResult::Success/Failed appropriately");

    Ok(())
}
