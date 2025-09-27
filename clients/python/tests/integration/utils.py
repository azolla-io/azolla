"""
Utility functions for managing integration test processes.

This module provides helper functions to:
- Start and stop the Azolla orchestrator binary
- Manage worker processes
- Check port availability
- Collect logs for debugging
"""

import asyncio
import logging
import os
import signal
import socket
import subprocess
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def find_available_port(start_port: int = 52710, max_attempts: int = 100) -> int:
    """Find an available port starting from the given port."""
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("localhost", port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"Could not find available port in range {start_port}-{start_port + max_attempts}"
    )


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a port is open and accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


async def wait_for_port(
    host: str, port: int, timeout: float = 30.0, check_interval: float = 0.1
) -> bool:
    """Wait for a port to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_open(host, port):
            return True
        await asyncio.sleep(check_interval)
    return False


class ProcessManager:
    """Manages a subprocess with logging and cleanup."""

    def __init__(
        self,
        name: str,
        cmd: list[str],
        cwd: Optional[Path] = None,
        env: Optional[dict] = None,
    ):
        self.name = name
        self.cmd = cmd
        self.cwd = cwd
        self.env = env or {}
        self.process: Optional[subprocess.Popen] = None
        self._stdout_file: Optional[Path] = None
        self._stderr_file: Optional[Path] = None

    def start(self, stdout_file: Optional[Path] = None, stderr_file: Optional[Path] = None) -> None:
        """Start the process."""
        if self.process and self.process.poll() is None:
            raise RuntimeError(f"Process {self.name} is already running")

        self._stdout_file = stdout_file
        self._stderr_file = stderr_file

        # Prepare environment
        full_env = dict(os.environ)
        full_env.update(self.env)

        # Open output files if specified
        stdout = open(stdout_file, "w") if stdout_file else subprocess.PIPE
        stderr = open(stderr_file, "w") if stderr_file else subprocess.PIPE

        logger.info(f"Starting process {self.name}: {' '.join(self.cmd)}")

        try:
            self.process = subprocess.Popen(
                self.cmd,
                cwd=self.cwd,
                env=full_env,
                stdout=stdout,
                stderr=stderr,
                preexec_fn=os.setsid if os.name != "nt" else None,
            )
            logger.info(f"Process {self.name} started with PID {self.process.pid}")
        except Exception as e:
            if stdout != subprocess.PIPE:
                stdout.close()
            if stderr != subprocess.PIPE:
                stderr.close()
            raise RuntimeError(f"Failed to start process {self.name}: {e}") from e

    def stop(self, timeout: float = 10.0) -> bool:
        """Stop the process gracefully."""
        if not self.process or self.process.poll() is not None:
            return True

        logger.info(f"Stopping process {self.name} (PID {self.process.pid})")

        try:
            # Try graceful shutdown first
            if os.name != "nt":
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            else:
                self.process.terminate()

            # Wait for graceful shutdown
            try:
                self.process.wait(timeout=timeout)
                logger.info(f"Process {self.name} stopped gracefully")
                return True
            except subprocess.TimeoutExpired:
                logger.warning(f"Process {self.name} did not stop gracefully, forcing...")

                # Force kill
                if os.name != "nt":
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                else:
                    self.process.kill()

                self.process.wait(timeout=5.0)
                logger.info(f"Process {self.name} force killed")
                return False

        except Exception as e:
            logger.error(f"Error stopping process {self.name}: {e}")
            return False
        finally:
            # Close output files
            if hasattr(self.process.stdout, "close") and self.process.stdout:
                self.process.stdout.close()
            if hasattr(self.process.stderr, "close") and self.process.stderr:
                self.process.stderr.close()

    def is_running(self) -> bool:
        """Check if the process is running."""
        return self.process is not None and self.process.poll() is None

    def get_output(self) -> tuple[Optional[str], Optional[str]]:
        """Get stdout and stderr if available."""
        if not self.process:
            return None, None

        stdout_content = None
        stderr_content = None

        if self._stdout_file and self._stdout_file.exists():
            stdout_content = self._stdout_file.read_text()

        if self._stderr_file and self._stderr_file.exists():
            stderr_content = self._stderr_file.read_text()

        return stdout_content, stderr_content


class OrchestratorManager:
    """Manages the Azolla orchestrator binary for integration tests."""

    def __init__(self, project_root: Path, port: Optional[int] = None):
        self.project_root = project_root
        self.port = port or find_available_port()
        self.endpoint = f"localhost:{self.port}"

        # Find the orchestrator binary
        self.binary_path = self._find_orchestrator_binary()

        # Set up environment
        self.env = {
            "AZOLLA__DATABASE__URL": "postgres://postgres:postgres@localhost:5432/azolla",
            "RUST_LOG": "info",
            "AZOLLA__SERVER__PORT": str(self.port),
        }

        self.process_manager = ProcessManager(
            name="orchestrator",
            cmd=[str(self.binary_path)],
            cwd=self.project_root,
            env=self.env,
        )

    def _find_orchestrator_binary(self) -> Path:
        """Find the orchestrator binary."""
        # Check common locations
        possible_paths = [
            self.project_root / "target" / "debug" / "azolla-orchestrator",
            self.project_root / "target" / "release" / "azolla-orchestrator",
        ]

        for path in possible_paths:
            if path.exists():
                return path

        raise RuntimeError(
            f"Orchestrator binary not found. Please build it first with: cargo build\n"
            f"Searched in: {[str(p) for p in possible_paths]}"
        )

    async def start(self, timeout: float = 30.0) -> None:
        """Start the orchestrator and wait for it to be ready."""
        # Create log files
        log_dir = self.project_root / "integration_test_logs"
        log_dir.mkdir(exist_ok=True)

        stdout_file = log_dir / "orchestrator_stdout.log"
        stderr_file = log_dir / "orchestrator_stderr.log"

        # Start the process
        self.process_manager.start(stdout_file=stdout_file, stderr_file=stderr_file)

        # Wait for the orchestrator to be ready
        logger.info(f"Waiting for orchestrator to be ready on {self.endpoint}")

        if not await wait_for_port("localhost", self.port, timeout=timeout):
            _, stderr = self.process_manager.get_output()
            error_msg = f"Orchestrator failed to start on port {self.port} within {timeout}s"
            if stderr:
                error_msg += f"\nSTDERR:\n{stderr}"
            raise RuntimeError(error_msg)

        logger.info("Orchestrator is ready")

    def stop(self) -> None:
        """Stop the orchestrator."""
        self.process_manager.stop()

    def is_running(self) -> bool:
        """Check if the orchestrator is running."""
        return self.process_manager.is_running()


class WorkerManager:
    """Manages shepherd processes configured to launch the Python worker binary."""

    def __init__(self, project_root: Path, orchestrator_endpoint: str, worker_binary: Path):
        self.project_root = project_root
        self.orchestrator_endpoint = orchestrator_endpoint
        self.worker_binary = worker_binary
        self.shepherd_binary = self._find_shepherd_binary()
        self.shepherds: list[ProcessManager] = []
        self.config_files: list[Path] = []
        self.default_log_dir: Optional[Path] = None

    def _find_shepherd_binary(self) -> Path:
        candidates = [
            self.project_root / "target" / "debug" / "azolla-shepherd",
            self.project_root / "target" / "release" / "azolla-shepherd",
        ]
        for path in candidates:
            if path.exists():
                return path
        raise RuntimeError(
            f"Shepherd binary not found. Please build it first with: cargo build\n"
            f"Searched in: {[str(p) for p in candidates]}"
        )

    def start_worker(
        self,
        domain: str = "default",
        shepherd_group: str = "python-test-workers",
        worker_id: Optional[str] = None,
        max_concurrency: int = 4,
        log_dir: Optional[Path] = None,
        wait_for_ready: bool = True,
        ready_timeout: float = 10.0,
    ) -> ProcessManager:
        import tempfile
        import uuid
        from textwrap import dedent

        worker_port = find_available_port(60000)
        http_endpoint = (
            f"http://{self.orchestrator_endpoint}"
            if not self.orchestrator_endpoint.startswith("http")
            else self.orchestrator_endpoint
        )

        identifier = worker_id or f"{domain}-{shepherd_group}-{len(self.shepherds)}"

        shepherd_uuid = uuid.uuid4()

        config_text = dedent(
            f"""
            uuid = "{shepherd_uuid}"
            orchestrator_endpoint = "{http_endpoint}"
            max_concurrency = {max_concurrency}
            worker_grpc_port = {worker_port}
            worker_binary_path = "{self.worker_binary}"
            domain = "{domain}"
            shepherd_group = "{shepherd_group}"
            heartbeat_interval_secs = 30
            reconnect_backoff_secs = 5
            worker_timeout_secs = 300
            log_level = "info"
            """
        )

        tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".toml")
        tmp.write(config_text)
        tmp.flush()
        tmp.close()
        config_path = Path(tmp.name)
        self.config_files.append(config_path)

        cmd = [
            str(self.shepherd_binary),
            "--config",
            str(config_path),
            "--orchestrator",
            http_endpoint,
            "--worker-binary",
            str(self.worker_binary),
            "--worker-port",
            str(worker_port),
            "--max-concurrency",
            str(max_concurrency),
            "--domain",
            domain,
            "--shepherd-group",
            shepherd_group,
        ]

        azolla_src_dir = self.project_root / "clients" / "python" / "src"
        azolla_grpc_dir = azolla_src_dir / "azolla" / "_grpc"

        worker_env = os.environ.copy()
        pythonpath_parts = [str(azolla_src_dir), str(azolla_grpc_dir)]
        if worker_env.get("PYTHONPATH"):
            pythonpath_parts.append(worker_env["PYTHONPATH"])
        worker_env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

        venv_python_dir = self.project_root / "venv" / "bin"
        if venv_python_dir.exists():
            worker_env["PATH"] = os.pathsep.join([str(venv_python_dir), worker_env.get("PATH", "")])

        worker_env["RUST_LOG"] = "info"

        process = ProcessManager(
            name=f"shepherd-{identifier}",
            cmd=cmd,
            cwd=self.project_root,
            env=worker_env,
        )

        if log_dir is None:
            log_dir = self.default_log_dir
        if log_dir is None:
            log_dir = Path(tempfile.gettempdir()) / "azolla_shepherd_logs"
        log_dir.mkdir(exist_ok=True)

        stdout_file = log_dir / f"{process.name}_stdout.log"
        stderr_file = log_dir / f"{process.name}_stderr.log"

        process.start(stdout_file=stdout_file, stderr_file=stderr_file)
        self.shepherds.append(process)

        if wait_for_ready:
            import time

            deadline = time.time() + ready_timeout
            while time.time() < deadline:
                if is_port_open("127.0.0.1", worker_port):
                    break
                time.sleep(0.1)
            else:
                logger.warning(
                    "Shepherd %s did not open worker port %s within %.1fs",
                    process.name,
                    worker_port,
                    ready_timeout,
                )

        return process

    def stop_all(self) -> None:
        for shepherd in self.shepherds:
            shepherd.stop()
        self.shepherds.clear()

    def cleanup(self) -> None:
        for path in self.config_files:
            if path.exists():
                path.unlink()
        self.config_files.clear()


@asynccontextmanager
async def integration_test_environment(project_root: Path):
    """
    Context manager that sets up a complete integration test environment.

    This includes:
    - Starting the orchestrator
    - Providing utilities to manage workers
    - Setting up shared log directory for debugging
    - Cleaning up everything on exit
    """
    orchestrator = OrchestratorManager(project_root)
    worker_script = (
        project_root / "clients" / "python" / "tests" / "integration" / "bin" / "test_worker.py"
    )

    # Set up shared log directory for this test session
    log_dir = project_root / "integration_test_logs"
    log_dir.mkdir(exist_ok=True)

    worker_manager = WorkerManager(project_root, orchestrator.endpoint, worker_script)
    worker_manager.default_log_dir = log_dir

    try:
        # Start orchestrator
        await orchestrator.start()

        # Yield the managers for test use
        yield orchestrator, worker_manager

    finally:
        # Clean up
        logger.info("Cleaning up integration test environment")
        worker_manager.stop_all()
        worker_manager.cleanup()
        orchestrator.stop()
