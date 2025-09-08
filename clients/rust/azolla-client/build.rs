fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false) // Client only, no server code
        .build_client(true)
        .compile(
            &["proto/orchestrator.proto", "proto/common.proto"],
            &["proto"],
        )?;
    Ok(())
}
