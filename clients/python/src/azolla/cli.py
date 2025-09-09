"""CLI entry points for Azolla tools."""
import argparse
import asyncio
import importlib
import sys
from typing import List, Optional

from azolla import Worker
from azolla._internal.utils import setup_logging, get_logger

logger = get_logger(__name__)

async def worker_main() -> None:
    """Main entry point for azolla-worker CLI."""
    parser = argparse.ArgumentParser(
        description="Azolla Python Worker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--orchestrator",
        default="localhost:52710",
        help="Orchestrator endpoint"
    )
    
    parser.add_argument(
        "--domain",
        default="default",
        help="Worker domain"
    )
    
    parser.add_argument(
        "--shepherd-group",
        default="python-workers",
        help="Shepherd group for this worker"
    )
    
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=10,
        help="Maximum concurrent tasks"
    )
    
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        help="Heartbeat interval in seconds"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    parser.add_argument(
        "--task-modules",
        nargs="*",
        help="Python modules containing tasks to import"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Create worker
    worker = (
        Worker.builder()
        .orchestrator(args.orchestrator)
        .domain(args.domain)
        .shepherd_group(args.shepherd_group)
        .max_concurrency(args.max_concurrency)
        .heartbeat_interval(args.heartbeat_interval)
        .build()
    )
    
    # Import task modules if specified
    if args.task_modules:
        for module_name in args.task_modules:
            try:
                importlib.import_module(module_name)
                logger.info(f"Imported task module: {module_name}")
            except ImportError as e:
                logger.error(f"Failed to import task module {module_name}: {e}")
                sys.exit(1)
    
    if worker.task_count() == 0:
        logger.warning("No tasks registered! Worker may not process any work.")
        logger.info("Use --task-modules to specify modules containing @azolla_task decorated functions")
    else:
        logger.info(f"Worker configured with {worker.task_count()} tasks")
    
    # Handle shutdown signals
    import signal
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        worker.shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start worker
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Worker stopped")

def worker_main_sync() -> None:
    """Synchronous entry point for CLI."""
    asyncio.run(worker_main())