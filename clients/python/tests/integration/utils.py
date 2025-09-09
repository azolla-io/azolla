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
import subprocess
import time
import socket
from pathlib import Path
from typing import Optional, List, Tuple
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


def find_available_port(start_port: int = 52710, max_attempts: int = 100) -> int:
    """Find an available port starting from the given port."""
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('localhost', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"Could not find available port in range {start_port}-{start_port + max_attempts}")


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a port is open and accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


async def wait_for_port(host: str, port: int, timeout: float = 30.0, check_interval: float = 0.1) -> bool:
    """Wait for a port to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_open(host, port):
            return True
        await asyncio.sleep(check_interval)
    return False


class ProcessManager:
    """Manages a subprocess with logging and cleanup."""
    
    def __init__(self, name: str, cmd: List[str], cwd: Optional[Path] = None, env: Optional[dict] = None):
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
        stdout = open(stdout_file, 'w') if stdout_file else subprocess.PIPE
        stderr = open(stderr_file, 'w') if stderr_file else subprocess.PIPE
        
        logger.info(f"Starting process {self.name}: {' '.join(self.cmd)}")
        
        try:
            self.process = subprocess.Popen(
                self.cmd,
                cwd=self.cwd,
                env=full_env,
                stdout=stdout,
                stderr=stderr,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            logger.info(f"Process {self.name} started with PID {self.process.pid}")
        except Exception as e:
            if stdout != subprocess.PIPE:
                stdout.close()
            if stderr != subprocess.PIPE:
                stderr.close()
            raise RuntimeError(f"Failed to start process {self.name}: {e}")
    
    def stop(self, timeout: float = 10.0) -> bool:
        """Stop the process gracefully."""
        if not self.process or self.process.poll() is not None:
            return True
        
        logger.info(f"Stopping process {self.name} (PID {self.process.pid})")
        
        try:
            # Try graceful shutdown first
            if os.name != 'nt':
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
                if os.name != 'nt':
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
            if hasattr(self.process.stdout, 'close') and self.process.stdout:
                self.process.stdout.close()
            if hasattr(self.process.stderr, 'close') and self.process.stderr:
                self.process.stderr.close()
    
    def is_running(self) -> bool:
        """Check if the process is running."""
        return self.process is not None and self.process.poll() is None
    
    def get_output(self) -> Tuple[Optional[str], Optional[str]]:
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
            "DATABASE_URL": "postgres://postgres:postgres@localhost:5432/azolla_test",
            "RUST_LOG": "info",
            "AZOLLA_CLUSTER_BIND": f"0.0.0.0:{self.port}"
        }
        
        self.process_manager = ProcessManager(
            name="orchestrator",
            cmd=[str(self.binary_path)],
            cwd=self.project_root,
            env=self.env
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
            stdout, stderr = self.process_manager.get_output()
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
    """Manages Python worker processes for integration tests."""
    
    def __init__(self, worker_script: Path, orchestrator_endpoint: str):
        self.worker_script = worker_script
        self.orchestrator_endpoint = orchestrator_endpoint
        self.workers: List[ProcessManager] = []
        self.default_log_dir: Optional[Path] = None
    
    def start_worker(
        self, 
        domain: str = "default", 
        worker_id: Optional[str] = None,
        log_dir: Optional[Path] = None,
        wait_for_ready: bool = True,
        ready_timeout: float = 30.0
    ) -> ProcessManager:
        """Start a new worker process and optionally wait for it to be ready."""
        worker_id = worker_id or f"worker-{len(self.workers)}"
        
        # Set up log directory - use default, or create temp if not available
        if log_dir is None:
            log_dir = self.default_log_dir
        if log_dir is None:
            import tempfile
            log_dir = Path(tempfile.gettempdir()) / "azolla_worker_logs"
        log_dir.mkdir(exist_ok=True)
        
        cmd = [
            "python3",
            str(self.worker_script),
            "--mode", "service",
            "--orchestrator-endpoint", self.orchestrator_endpoint,
            "--domain", domain
        ]
        
        # Set up environment to ensure azolla module can be found
        import os
        worker_env = os.environ.copy()
        
        # Add the src directory to PYTHONPATH for the worker process  
        # Path: tests/integration/bin/test_worker.py -> ../../src
        azolla_src_dir = str(self.worker_script.parent.parent.parent.parent / "src")
        existing_pythonpath = worker_env.get("PYTHONPATH", "")
        if existing_pythonpath:
            worker_env["PYTHONPATH"] = f"{azolla_src_dir}:{existing_pythonpath}"
        else:
            worker_env["PYTHONPATH"] = azolla_src_dir
        
        logger.info(f"Setting worker PYTHONPATH to: {worker_env['PYTHONPATH']}")
        logger.info(f"Azolla src directory: {azolla_src_dir}")
        logger.info(f"Worker script parent: {self.worker_script.parent}")
        
        # Verify the azolla src directory exists
        azolla_src_path = Path(azolla_src_dir)
        if not azolla_src_path.exists():
            logger.error(f"Azolla src directory does not exist: {azolla_src_dir}")
        else:
            logger.info(f"Azolla src directory exists and contains: {list(azolla_src_path.iterdir())}")
        
        worker = ProcessManager(
            name=f"worker-{worker_id}",
            cmd=cmd,
            cwd=self.worker_script.parent,
            env=worker_env
        )
        
        # Set up log files
        stdout_file = log_dir / f"{worker_id}_stdout.log"
        stderr_file = log_dir / f"{worker_id}_stderr.log"
        
        worker.start(stdout_file=stdout_file, stderr_file=stderr_file)
        self.workers.append(worker)
        
        logger.info(f"Started worker {worker_id}")
        
        # Wait for worker to be ready if requested
        if wait_for_ready:
            if self._wait_for_worker_ready(worker_id, stdout_file, stderr_file, ready_timeout):
                logger.info(f"Worker {worker_id} is ready")
            else:
                logger.warning(f"Worker {worker_id} did not become ready within {ready_timeout}s")
        
        return worker
    
    def _wait_for_worker_ready(
        self, 
        worker_id: str, 
        stdout_file: Path, 
        stderr_file: Path, 
        timeout: float
    ) -> bool:
        """Wait for worker to show ready status in logs."""
        import time
        
        start_time = time.time()
        ready_indicators = [
            "registered successfully",
            "Worker started successfully and connected to orchestrator"
        ]
        
        while time.time() - start_time < timeout:
            # Check both stdout and stderr for ready indicators
            for log_file in [stdout_file, stderr_file]:
                if log_file.exists():
                    try:
                        content = log_file.read_text()
                        if any(indicator in content for indicator in ready_indicators):
                            return True
                    except (IOError, OSError):
                        # Log file might be being written to, try again
                        pass
            
            time.sleep(0.1)  # Short sleep to avoid busy waiting
        
        return False
    
    def wait_for_shepherds_available(
        self, 
        orchestrator_log_dir: Path, 
        expected_group: str = "python-test-workers",
        timeout: float = 10.0
    ) -> bool:
        """Wait for orchestrator to show shepherds available for the given group."""
        import time
        
        start_time = time.time()
        orchestrator_stderr = orchestrator_log_dir / "orchestrator_stderr.log"
        
        while time.time() - start_time < timeout:
            if orchestrator_stderr.exists():
                try:
                    content = orchestrator_stderr.read_text()
                    # Look for absence of "No shepherd available" message for our group
                    # or presence of successful task dispatch
                    no_shepherd_msg = f"No shepherd available for group '{expected_group}'"
                    
                    # If we don't see the "no shepherd" message recently, shepherds might be available
                    lines = content.split('\n')
                    recent_lines = lines[-50:]  # Check last 50 lines
                    
                    if not any(no_shepherd_msg in line for line in recent_lines):
                        # Also check for positive indicators
                        if any("dispatched" in line and "tasks" in line for line in recent_lines):
                            return True
                    
                except (IOError, OSError):
                    pass
            
            time.sleep(0.2)
        
        return False
    
    def stop_all_workers(self) -> None:
        """Stop all worker processes."""
        for worker in self.workers:
            worker.stop()
        self.workers.clear()


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
    worker_script = project_root / "clients" / "python" / "tests" / "integration" / "bin" / "test_worker.py"
    
    # Set up shared log directory for this test session
    log_dir = project_root / "integration_test_logs"
    log_dir.mkdir(exist_ok=True)
    
    worker_manager = WorkerManager(worker_script, orchestrator.endpoint)
    # Override the default worker log directory
    worker_manager.default_log_dir = log_dir
    
    try:
        # Start orchestrator
        await orchestrator.start()
        
        # Yield the managers for test use
        yield orchestrator, worker_manager
        
    finally:
        # Clean up
        logger.info("Cleaning up integration test environment")
        worker_manager.stop_all_workers()
        orchestrator.stop()