fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/azolla.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(false) // We are only building a server
        .compile(&["proto/azolla.proto"], &["proto"])?;
    Ok(())
} 