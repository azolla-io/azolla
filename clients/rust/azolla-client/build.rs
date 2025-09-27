fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the main proto directory from the repository root
    let proto_dir = "../../../proto";

    tonic_build::configure()
        .build_server(false) // Client only, no server code
        .build_client(true)
        .compile(
            &[
                &format!("{proto_dir}/orchestrator.proto"),
                &format!("{proto_dir}/common.proto"),
                &format!("{proto_dir}/shepherd.proto"),
            ],
            &[proto_dir],
        )?;
    Ok(())
}
