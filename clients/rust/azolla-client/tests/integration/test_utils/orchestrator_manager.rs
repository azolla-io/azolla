use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use anyhow::{Result, Context};

/// Manages the lifecycle of an orchestrator instance for testing
pub struct TestOrchestrator {
    process: Option<Child>,
    pub endpoint: String,
    pub port: u16,
}

impl TestOrchestrator {
    /// Find the orchestrator binary in common locations
    fn find_orchestrator_binary() -> Result<String, String> {
        // Find project root by walking up until we find Cargo.toml
        let mut current_dir = std::env::current_dir()
            .map_err(|_| "Could not get current directory".to_string())?;

        let mut project_root = None;
        loop {
            if current_dir.join("Cargo.toml").exists() {
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
            "Could not find project root (Cargo.toml not found)".to_string()
        })?;

        // Check common locations relative to project root
        let possible_paths = [
            project_root.join("target/debug/azolla-orchestrator"),
            project_root.join("target/release/azolla-orchestrator"),
        ];

        for path in &possible_paths {
            if path.exists() {
                return Ok(path.to_string_lossy().to_string());
            }
        }

        Err(format!(
            "Orchestrator binary not found. Please build it first with: cargo build\nSearched in: {:?}",
            possible_paths.iter().map(|p| p.to_string_lossy().to_string()).collect::<Vec<_>>()
        ))
    }

    /// Start a new orchestrator instance
    pub async fn start() -> Result<Self> {
        let port = find_free_port().await?;
        let endpoint = format!("http://localhost:{port}");

        // Start orchestrator binary
        let orchestrator_path = std::env::var("AZOLLA_ORCHESTRATOR_PATH")
            .unwrap_or_else(|_| {
                Self::find_orchestrator_binary()
                    .unwrap_or_else(|e| {
                        eprintln!("Warning: {e}");
                        "azolla-orchestrator".to_string() // Fallback to PATH
                    })
            });

        let process = Command::new(&orchestrator_path)
            .env("AZOLLA_PORT", port.to_string())
            .env("DATABASE_URL", "postgresql://localhost:5432/azolla_test")
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to start orchestrator process")?;

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
        let client = reqwest::Client::new();
        let health_url = format!("{}/_health", self.endpoint);

        // Retry for up to 30 seconds
        for attempt in 0..60 {
            if let Ok(response) = client.get(&health_url).send().await {
                if response.status().is_success() {
                    println!("Orchestrator ready after {} attempts", attempt + 1);
                    return Ok(());
                }
            }

            sleep(Duration::from_millis(500)).await;
        }

        anyhow::bail!("Orchestrator failed to become ready within 30 seconds")
    }

    /// Get the orchestrator endpoint for client connections
    pub fn client_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    /// Get the orchestrator endpoint for worker connections
    pub fn worker_endpoint(&self) -> String {
        format!("localhost:{}", self.port)
    }

    /// Gracefully shutdown the orchestrator
    pub async fn shutdown(&mut self) -> Result<()> {
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
            }).await;

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

/// Find an available port for testing
async fn find_free_port() -> Result<u16> {
    use std::net::{TcpListener, SocketAddr};

    // Try ports in the test range 15000-16000
    for port in 15000..16000 {
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
        if TcpListener::bind(addr).is_ok() {
            return Ok(port);
        }
    }

    anyhow::bail!("No free ports found in range 15000-16000")
}