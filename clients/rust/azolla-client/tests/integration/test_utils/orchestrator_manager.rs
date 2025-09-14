use anyhow::{Context, Result};
use azolla_client::error::AzollaError;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Manages the lifecycle of an orchestrator instance for testing
pub struct TestOrchestrator {
    process: Option<Child>,
    pub endpoint: String,
    pub port: u16,
}

impl TestOrchestrator {
    /// Find the orchestrator binary and project root
    fn find_orchestrator_info() -> Result<(String, std::path::PathBuf), String> {
        // First, check if there's an environment variable override
        if let Ok(orchestrator_path) = std::env::var("AZOLLA_ORCHESTRATOR_PATH") {
            let project_root =
                std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
            return Ok((orchestrator_path, project_root));
        }

        // Find project root by walking up until we find the main Azolla project
        let mut current_dir =
            std::env::current_dir().map_err(|_| "Could not get current directory".to_string())?;

        let mut project_root = None;
        loop {
            // Look for the main azolla project markers
            if current_dir.join("src").exists()
                && current_dir.join("src").join("orchestrator").exists()
                && current_dir.join("Cargo.toml").exists()
            {
                project_root = Some(current_dir.clone());
                break;
            }
            if let Some(parent) = current_dir.parent() {
                current_dir = parent.to_path_buf();
            } else {
                break;
            }
        }

        let project_root = project_root.ok_or_else(|| {
            "Could not find azolla project root (src/orchestrator not found)".to_string()
        })?;

        // Check for binary in multiple possible locations
        let possible_paths = [
            // Standard build locations in main project
            project_root.join("target/debug/azolla-orchestrator"),
            project_root.join("target/release/azolla-orchestrator"),
        ];

        for path in &possible_paths {
            if path.exists() {
                return Ok((path.to_string_lossy().to_string(), project_root));
            }
        }

        // If binary not found in standard locations, try PATH
        if let Ok(output) = std::process::Command::new("which")
            .arg("azolla-orchestrator")
            .output()
        {
            if output.status.success() && !output.stdout.is_empty() {
                let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path_str.is_empty() {
                    return Ok((path_str, project_root));
                }
            }
        }

        Err(format!(
            "Orchestrator binary not found. Please build it first with: cargo build\n\
            Project root: {}\n\
            Current dir: {:?}\n\
            Searched in: {:?}\n\
            Also checked PATH for azolla-orchestrator\n\
            \n\
            For CI: Make sure to build the orchestrator binary before running client tests:\n\
            - Run 'cargo build' from the project root to build azolla-orchestrator\n\
            - Or set AZOLLA_ORCHESTRATOR_PATH environment variable",
            project_root.display(),
            std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("unknown")),
            possible_paths
                .iter()
                .map(|p| p.to_string_lossy().to_string())
                .collect::<Vec<_>>()
        ))
    }

    /// Start a new orchestrator instance (compatibility method for integration tests)
    pub async fn start() -> Result<Self, AzollaError> {
        Self::start_with_anyhow().await.map_err(|e| {
            AzollaError::ConnectionError(format!("Failed to start test orchestrator: {e}"))
        })
    }

    /// Start a new orchestrator instance (internal implementation)
    async fn start_with_anyhow() -> Result<Self> {
        // Use the default port that orchestrator actually uses (52710)
        // since environment variable override doesn't seem to work
        let port = 52710;
        let endpoint = format!("http://localhost:{port}");

        // Check if orchestrator is already running by attempting to create a client connection
        if (azolla_client::Client::builder()
            .endpoint(&endpoint)
            .build()
            .await)
            .is_ok()
        {
            println!("Orchestrator already running on port {port}, reusing...");
            return Ok(Self {
                process: None, // Don't manage an existing process
                endpoint,
                port,
            });
        }

        // Start orchestrator binary
        let (orchestrator_path, project_root) = std::env::var("AZOLLA_ORCHESTRATOR_PATH")
            .map(|path| {
                (
                    path,
                    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
                )
            })
            .unwrap_or_else(|_| {
                Self::find_orchestrator_info().unwrap_or_else(|e| {
                    eprintln!("Warning: {e}");
                    (
                        "azolla-orchestrator".to_string(),
                        std::path::PathBuf::from("."),
                    ) // Fallback to PATH
                })
            });

        println!(
            "Starting orchestrator: {} on port {} from directory: {}",
            orchestrator_path,
            port,
            project_root.display()
        );

        let mut process = Command::new(&orchestrator_path)
            .current_dir(&project_root) // Set working directory to project root
            .env("RUST_LOG", "info") // Reduce log verbosity for tests
            .stdout(Stdio::piped()) // Capture stdout to debug
            .stderr(Stdio::piped()) // Capture stderr to debug
            .spawn()
            .context("Failed to start orchestrator process")?;

        // Give the process a moment to start
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Check if the process is still running
        match process.try_wait() {
            Ok(Some(status)) => {
                // Process has exited, try to capture stderr for debugging
                let mut stderr_output = String::new();
                if let Some(stderr) = process.stderr.as_mut() {
                    use std::io::Read;
                    let _ = stderr.read_to_string(&mut stderr_output);
                }
                if !stderr_output.is_empty() {
                    println!("Orchestrator stderr: {stderr_output}");
                }
                anyhow::bail!(
                    "Orchestrator process exited with status: {}. stderr: {}",
                    status,
                    stderr_output
                );
            }
            Ok(None) => {
                // Process is still running
                println!("Orchestrator process is running (PID: {})", process.id());
            }
            Err(e) => {
                anyhow::bail!("Failed to check orchestrator process status: {}", e);
            }
        }

        let orchestrator = Self {
            process: Some(process),
            endpoint,
            port,
        };

        // Wait for orchestrator to be ready
        orchestrator.wait_for_readiness().await?;

        Ok(orchestrator)
    }

    /// Wait for the orchestrator to become ready
    async fn wait_for_readiness(&self) -> Result<()> {
        println!(
            "Waiting for orchestrator to become ready at {}",
            self.endpoint
        );

        // Retry for up to 60 seconds with more frequent checks initially
        for attempt in 0..120 {
            match azolla_client::Client::builder()
                .endpoint(&self.endpoint)
                .build()
                .await
            {
                Ok(_) => {
                    println!("Orchestrator ready after {} attempts", attempt + 1);
                    return Ok(());
                }
                Err(e) => {
                    if attempt % 10 == 0 {
                        println!(
                            "Health check attempt {}: Connection error: {}",
                            attempt + 1,
                            e
                        );
                    }
                }
            }

            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("Orchestrator failed to become ready within 60 seconds")
    }

    /// Get the orchestrator endpoint (compatibility method for integration tests)
    pub fn endpoint(&self) -> String {
        // For client connections, return the full HTTP URL
        self.endpoint.clone()
    }

    /// Get the orchestrator endpoint for client connections
    pub fn client_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    /// Get the orchestrator endpoint for worker connections
    pub fn worker_endpoint(&self) -> String {
        format!("localhost:{}", self.port)
    }

    /// Gracefully shutdown the orchestrator (compatibility method for integration tests)
    pub async fn shutdown(mut self) -> Result<(), AzollaError> {
        self.shutdown_mut().await.map_err(|e| {
            AzollaError::WorkerError(format!("Failed to shutdown test orchestrator: {e}"))
        })
    }

    /// Gracefully shutdown the orchestrator (internal implementation)
    pub async fn shutdown_mut(&mut self) -> Result<()> {
        if let Some(mut process) = self.process.take() {
            // Send SIGTERM for graceful shutdown
            #[cfg(unix)]
            {
                let pid = process.id();
                let _ = Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .output();
            }

            // Wait for graceful shutdown or force kill after timeout
            let wait_result = timeout(Duration::from_secs(5), async {
                tokio::task::spawn_blocking(move || process.wait()).await
            })
            .await;

            match wait_result {
                Ok(Ok(Ok(_))) => {
                    println!("Orchestrator shutdown gracefully");
                }
                _ => {
                    println!("Force killing orchestrator (already moved)");
                    // Process was moved to the blocking task, can't kill it here
                }
            }
        }
        Ok(())
    }
}

impl Drop for TestOrchestrator {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
    }
}
