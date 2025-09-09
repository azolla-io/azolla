fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the main proto directory from the repository root
    let proto_dir = "../../../proto";
    
    tonic_build::configure()
        .build_server(false) // Client only, no server code
        .build_client(true)
        .compile(
            &[
                &format!("{}/orchestrator.proto", proto_dir),
                &format!("{}/common.proto", proto_dir)
            ],
            &[proto_dir],
        )?;
    Ok(())
}
