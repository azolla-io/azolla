"""CLI entry points for Azolla tools."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import sys

from azolla import Worker, WorkerInvocation
from azolla._internal.utils import get_logger, setup_logging
from azolla.exceptions import WorkerError
from azolla.task import Task

logger = get_logger(__name__)


async def worker_main() -> None:
    """Main entry point for azolla-worker CLI."""
    parser = argparse.ArgumentParser(
        description="Azolla Python Worker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--task-id", required=True, help="Task UUID supplied by shepherd")
    parser.add_argument("--name", required=True, help="Task name registered with worker")
    parser.add_argument("--args", default="[]", help="Task arguments encoded as JSON array")
    parser.add_argument(
        "--kwargs", default="{}", help="Task keyword arguments encoded as JSON object"
    )
    parser.add_argument(
        "--shepherd-endpoint",
        required=True,
        help="Shepherd gRPC endpoint for result reporting",
    )
    parser.add_argument(
        "--task-modules",
        nargs="*",
        help="Optional modules containing @azolla_task decorated functions or Task instances",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    worker = Worker.builder().build()

    if args.task_modules:
        register_count = discover_and_register_tasks(worker, args.task_modules)
        logger.info("Registered %d tasks from modules %s", register_count, args.task_modules)

    if worker.task_count() == 0:
        logger.warning("No tasks registered; incoming work will fail with TaskNotFound")

    try:
        invocation = WorkerInvocation.from_json(
            task_id=args.task_id,
            task_name=args.name,
            args_json=args.args,
            kwargs_json=args.kwargs,
            shepherd_endpoint=args.shepherd_endpoint,
        )
    except WorkerError as exc:
        logger.error("Failed to parse invocation: %s", exc)
        sys.exit(1)

    logger.info("Starting single task invocation")
    execution = await worker.run_invocation(invocation)

    if execution.is_success():
        logger.info("Task %s completed successfully", execution.task_id)
    else:
        error_message = execution.error.message if execution.error else "unknown error"
        logger.error("Task %s failed: %s", execution.task_id, error_message)
        sys.exit(1)


def worker_main_sync() -> None:
    """Synchronous entry point for CLI."""
    asyncio.run(worker_main())


def discover_and_register_tasks(worker: Worker, module_names: list[str]) -> int:
    """Discover and register tasks from given module names."""
    count = 0
    for module_name in module_names:
        module = importlib.import_module(module_name)
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            try:
                if hasattr(obj, "__azolla_task_instance__"):
                    worker.register_task(obj)
                    count += 1
                elif isinstance(obj, Task):
                    worker.register_task(obj)
                    count += 1
            except (AttributeError, TypeError):
                continue
    return count


__all__ = ["discover_and_register_tasks", "worker_main", "worker_main_sync"]
