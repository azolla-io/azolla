use anyhow::{anyhow, bail, Context, Result};
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

/// Manages the lifecycle of a shepherd instance for testing
pub struct TestShepherd {
    process: Option<Child>,
    pub worker_endpoint: String,
    pub port: u16,
}

impl TestShepherd {
    fn find_project_root() -> Result<PathBuf, String> {
        let mut current_dir =
            std::env::current_dir().map_err(|_| "Could not get current directory".to_string())?;

        loop {
            if current_dir.join("src").exists() && current_dir.join("Cargo.toml").exists() {
                return Ok(current_dir);
            }
            if let Some(parent) = current_dir.parent() {
                current_dir = parent.to_path_buf();
            } else {
                break;
            }
        }

        Err("Could not find azolla project root".to_string())
    }

    fn find_binary(name: &str, project_root: &Path) -> Result<String, String> {
        if let Ok(path) = env::var(format!(
            "AZOLLA_{}_PATH",
            name.to_uppercase().replace('-', "_")
        )) {
            if !path.is_empty() {
                return Ok(path);
            }
        }

        let binary_name = format!("{name}{}", env::consts::EXE_SUFFIX);
        let mut candidates: Vec<PathBuf> = Vec::new();

        for ancestor in project_root.ancestors() {
            let target_dir = ancestor.join("target");
            candidates.push(target_dir.join("debug").join(&binary_name));
            candidates.push(target_dir.join("release").join(&binary_name));
        }

        for candidate in &candidates {
            if candidate.exists() {
                return Ok(candidate.to_string_lossy().to_string());
            }
        }

        Err(format!(
            "{name} binary not found. Please build it first with: cargo build\nSearched in: {:?}\nYou can also set AZOLLA_{}_PATH",
            candidates,
            name.to_uppercase().replace('-', "_")
        ))
    }

    /// Start a new shepherd process bound to the provided orchestrator endpoint
    pub async fn start(orchestrator_endpoint: &str) -> Result<Self> {
        let project_root = Self::find_project_root().map_err(|e| anyhow!(e))?;
        let shepherd_path =
            Self::find_binary("azolla-shepherd", &project_root).map_err(|e| anyhow!(e))?;
        let worker_path =
            Self::find_binary("azolla-worker", &project_root).map_err(|e| anyhow!(e))?;

        let port: u16 = 50052;
        let worker_endpoint = format!("http://127.0.0.1:{port}");

        let mut command = Command::new(&shepherd_path);
        command
            .current_dir(&project_root)
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("--orchestrator")
            .arg(orchestrator_endpoint)
            .arg("--worker-binary")
            .arg(&worker_path)
            .arg("--worker-port")
            .arg(port.to_string());

        let mut process = command
            .spawn()
            .context("Failed to start shepherd process")?;

        sleep(Duration::from_millis(1000)).await;

        if let Some(status) = process
            .try_wait()
            .context("Failed to check shepherd status")?
        {
            let mut stderr_output = String::new();
            if let Some(stderr) = process.stderr.as_mut() {
                use std::io::Read;
                let _ = stderr.read_to_string(&mut stderr_output);
            }
            bail!("Shepherd exited immediately with status {status}. stderr: {stderr_output}");
        }

        Ok(Self {
            process: Some(process),
            worker_endpoint,
            port,
        })
    }

    pub async fn wait_for_readiness(&self) -> Result<()> {
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    pub fn worker_service_endpoint(&self) -> &str {
        &self.worker_endpoint
    }
}

impl Drop for TestShepherd {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            let _ = child.kill();
        }
    }
}
