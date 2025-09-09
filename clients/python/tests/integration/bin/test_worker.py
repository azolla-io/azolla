#!/usr/bin/env python3
"""
Python test worker that mirrors the functionality of src/bin/azolla-worker.rs.

This worker implements the same test tasks as the Rust version for integration testing:
- echo_task: Returns the input unchanged
- always_fail_task: Always fails with an error
- flaky_task: Fails on first attempt, succeeds on retry
- math_add: Adds two numbers
- count_args: Counts the number of arguments
"""
import asyncio
import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional
import uuid

# Add the src directory to the path so we can import azolla
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from azolla import Task, Worker, WorkerConfig
from azolla.exceptions import TaskError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EchoTask(Task):
    """Echo task that returns the input unchanged."""
    
    def name(self) -> str:
        return "echo"
    
    async def execute(self, args: Any, context=None) -> Any:
        logger.info(f"Echo task executing with args: {args}")
        return args


class AlwaysFailTask(Task):
    """Task that always fails with an error."""
    
    def name(self) -> str:
        return "always_fail"
    
    async def execute(self, args: Any, context=None) -> Any:
        logger.info(f"Always fail task executing with args: {args}")
        raise TaskError(
            "Task designed to always fail",
            error_code="ALWAYS_FAIL",
            error_type="TestError"
        )


class FlakyTask(Task):
    """Task that fails on first attempt, succeeds on retry using file-based state."""
    
    def name(self) -> str:
        return "flaky"
    
    async def execute(self, args: Any, context=None) -> Any:
        logger.info(f"Flaky task executing with args: {args}")
        
        # Use process ID to create unique state file (same pattern as Rust version)
        process_id = os.getpid()
        state_file = Path(tempfile.gettempdir()) / f"flaky_task_state_{process_id}"
        
        # Read current attempt count
        try:
            attempt_count = int(state_file.read_text().strip())
        except (FileNotFoundError, ValueError):
            attempt_count = 0
        
        # Increment and save attempt count
        new_attempt_count = attempt_count + 1
        state_file.write_text(str(new_attempt_count))
        
        logger.info(f"Flaky task attempt #{new_attempt_count}")
        
        # Fail on first attempt
        if new_attempt_count == 1:
            raise TaskError(
                "First attempt failure",
                error_code="FLAKY_TASK_FIRST_ATTEMPT",
                error_type="TestError"
            )
        
        # Succeed on subsequent attempts
        return "Flaky task succeeded on retry"


class MathAddTask(Task):
    """Task that adds two numbers."""
    
    def name(self) -> str:
        return "math_add"
    
    async def execute(self, args: Any, context=None) -> Any:
        logger.info(f"Math add task executing with args: {args}")
        
        # Parse arguments - expect [a, b] format
        if not isinstance(args, list) or len(args) != 2:
            raise TaskError(
                f"Math add expects exactly 2 arguments, got {len(args) if isinstance(args, list) else 'non-list'}",
                error_code="INVALID_ARGS",
                error_type="ValidationError"
            )
        
        try:
            a, b = float(args[0]), float(args[1])
            result = a + b
            logger.info(f"Math add: {a} + {b} = {result}")
            return result
        except (ValueError, TypeError) as e:
            raise TaskError(
                f"Math add requires numeric arguments: {e}",
                error_code="INVALID_NUMBER",
                error_type="ValidationError"
            )


class CountArgsTask(Task):
    """Task that counts the number of arguments."""
    
    def name(self) -> str:
        return "count_args"
    
    async def execute(self, args: Any, context=None) -> Any:
        logger.info(f"Count args task executing with args: {args}")
        
        if isinstance(args, list):
            count = len(args)
        elif isinstance(args, dict):
            count = len(args)
        elif args is None:
            count = 0
        else:
            count = 1
        
        logger.info(f"Counted {count} arguments")
        return {"count": count, "args": args}


async def run_single_task(
    task_id: str,
    task_name: str,
    task_args: str,
    task_kwargs: str,
    orchestrator_endpoint: str
) -> None:
    """Execute a single task (equivalent to Rust 'task' mode)."""
    logger.info(f"Running single task: {task_name} (ID: {task_id})")
    
    # Create task registry and register all test tasks
    tasks = [
        EchoTask(),
        AlwaysFailTask(),
        FlakyTask(),
        MathAddTask(),
        CountArgsTask()
    ]
    
    # Find the requested task
    task_instance = None
    for task in tasks:
        if task.name() == task_name:
            task_instance = task
            break
    
    if not task_instance:
        logger.error(f"Unknown task: {task_name}")
        sys.exit(1)
    
    # Parse arguments
    try:
        args = json.loads(task_args) if task_args else []
        kwargs = json.loads(task_kwargs) if task_kwargs else {}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse arguments: {e}")
        sys.exit(1)
    
    # Execute the task
    try:
        # For single task execution, we use args directly (not kwargs for now)
        result = await task_instance.execute(args)
        logger.info(f"Task completed successfully: {result}")
        print(json.dumps({"success": True, "result": result}))
    except Exception as e:
        logger.error(f"Task failed: {e}")
        error_info = {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }
        if isinstance(e, TaskError):
            error_info["error_code"] = getattr(e, "error_code", "UNKNOWN")
        print(json.dumps(error_info))
        sys.exit(1)


async def run_worker_service(orchestrator_endpoint: str, domain: str) -> None:
    """Run as a continuous worker service (equivalent to Rust 'service' mode)."""
    logger.info(f"Starting worker service: {orchestrator_endpoint}, domain: {domain}")
    
    config = WorkerConfig(
        orchestrator_endpoint=orchestrator_endpoint,
        domain=domain,
        shepherd_group="python-test-workers",
        max_concurrency=5
    )
    
    worker = Worker(config)
    
    # Register all test tasks
    tasks = [
        EchoTask(),
        AlwaysFailTask(),
        FlakyTask(),
        MathAddTask(),
        CountArgsTask()
    ]
    
    # Register tasks with error handling
    try:
        for task in tasks:
            worker.register_task(task)
            logger.debug(f"Registered task: {task.name()}")
        
        logger.info(f"Successfully registered {len(tasks)} tasks: {[task.name() for task in tasks]}")
        
    except Exception as e:
        logger.error(f"Failed to register tasks: {e}", exc_info=True)
        sys.exit(1)
    
    # Start the worker with detailed error handling
    try:
        logger.info(f"Starting worker service connecting to {orchestrator_endpoint}")
        await worker.start()
        logger.info("Worker started successfully and connected to orchestrator")
        
        # Keep running until interrupted
        await worker.wait_for_shutdown()
        
    except KeyboardInterrupt:
        logger.info("Worker interrupted, shutting down...")
    except ConnectionError as e:
        logger.error(f"Failed to connect to orchestrator at {orchestrator_endpoint}: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Worker error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        worker.shutdown()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Python test worker for Azolla integration tests")
    
    # Mode selection
    parser.add_argument(
        "--mode", 
        choices=["task", "service"], 
        default="service",
        help="Run mode: 'task' for single execution, 'service' for continuous worker"
    )
    
    # Common arguments
    parser.add_argument(
        "--orchestrator-endpoint", 
        default="localhost:52710",
        help="Orchestrator endpoint (default: localhost:52710)"
    )
    parser.add_argument(
        "--domain", 
        default="default",
        help="Task domain (default: default)"
    )
    
    # Task mode arguments
    parser.add_argument("--task-id", help="Task ID (for task mode)")
    parser.add_argument("--name", help="Task name (for task mode)")
    parser.add_argument("--args", help="Task arguments as JSON (for task mode)")
    parser.add_argument("--kwargs", help="Task keyword arguments as JSON (for task mode)")
    
    args = parser.parse_args()
    
    if args.mode == "task":
        if not all([args.task_id, args.name]):
            logger.error("Task mode requires --task-id and --name")
            sys.exit(1)
        
        asyncio.run(run_single_task(
            args.task_id,
            args.name,
            args.args or "[]",
            args.kwargs or "{}",
            args.orchestrator_endpoint
        ))
    else:
        asyncio.run(run_worker_service(args.orchestrator_endpoint, args.domain))


if __name__ == "__main__":
    main()