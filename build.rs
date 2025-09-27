fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Watch for changes in all proto files
    println!("cargo:rerun-if-changed=proto/common.proto");
    println!("cargo:rerun-if-changed=proto/shepherd.proto");
    println!("cargo:rerun-if-changed=proto/orchestrator.proto");

    // Compile proto files in dependency order
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // Enable client for benchmark tool
        .compile(
            &[
                "proto/common.proto",
                "proto/shepherd.proto",
                "proto/orchestrator.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
