"""Basic worker example for Azolla Python library.

This example demonstrates how to create and run an Azolla worker
with different types of task implementations.
"""
import asyncio
import logging
import signal
from typing import Dict, Any, List, Optional

from azolla import azolla_task, Task, Worker, WorkerConfig
from azolla.exceptions import TaskError
from azolla.types import TaskContext

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Example 1: Decorator-based task (convenient approach)
@azolla_task
async def send_notification(
    recipient: str, 
    message: str, 
    priority: str = "normal"
) -> Dict[str, Any]:
    """Send a notification to a recipient."""
    logger.info(f"📧 Sending {priority} priority notification to {recipient}: {message}")
    
    # Simulate some work
    await asyncio.sleep(0.1)
    
    # Simulate occasional failures for demonstration
    if "fail" in message.lower():
        raise TaskError("Notification service temporarily unavailable", retryable=True)
    
    return {
        "status": "sent",
        "recipient": recipient,
        "message_id": f"msg_{hash(recipient + message) % 10000}",
        "priority": priority,
        "timestamp": asyncio.get_event_loop().time()
    }


# Example 2: Class-based task with validation (explicit approach)
class ProcessDataTask(Task):
    """Process data with validation and transformation."""
    
    from pydantic import BaseModel, field_validator
    
    class Args(BaseModel):
        data: List[Dict[str, Any]]
        operation: str
        options: Optional[Dict[str, Any]] = None
        
        @field_validator('operation')
        @classmethod
        def validate_operation(cls, v: str) -> str:
            allowed = ['sum', 'count', 'filter', 'transform']
            if v not in allowed:
                raise ValueError(f"Operation must be one of: {allowed}")
            return v
    
    async def execute(self, args: Args, context: Optional[TaskContext] = None) -> Dict[str, Any]:
        """Execute the data processing task."""
        logger.info(f"🔄 Processing {len(args.data)} items with operation: {args.operation}")
        
        if context:
            logger.info(f"Task context - ID: {context.task_id}, Attempt: {context.attempt_number}")
        
        if args.operation == "sum":
            total = sum(item.get("value", 0) for item in args.data)
            return {
                "operation": "sum", 
                "result": total, 
                "count": len(args.data),
                "processed_by": "ProcessDataTask"
            }
        
        elif args.operation == "count":
            return {
                "operation": "count", 
                "result": len(args.data),
                "processed_by": "ProcessDataTask"
            }
        
        elif args.operation == "filter":
            threshold = args.options.get("threshold", 0) if args.options else 0
            filtered = [item for item in args.data if item.get("value", 0) > threshold]
            return {
                "operation": "filter", 
                "result": filtered, 
                "count": len(filtered),
                "threshold": threshold,
                "processed_by": "ProcessDataTask"
            }
        
        elif args.operation == "transform":
            multiplier = args.options.get("multiplier", 1) if args.options else 1
            transformed = [
                {**item, "value": item.get("value", 0) * multiplier} 
                for item in args.data
            ]
            return {
                "operation": "transform", 
                "result": transformed, 
                "count": len(transformed),
                "multiplier": multiplier,
                "processed_by": "ProcessDataTask"
            }
        
        else:
            raise TaskError(f"Unsupported operation: {args.operation}")


# Example 3: Simple function-based task
@azolla_task
async def echo_task(message: Any) -> Any:
    """Simple echo task that returns its input."""
    logger.info(f"🔊 Echo: {message}")
    return message


# Example 4: Task with error handling
@azolla_task  
async def validate_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate data and return validation results."""
    logger.info(f"✅ Validating {len(data)} data items")
    
    valid_items = []
    invalid_items = []
    
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            invalid_items.append({"index": i, "reason": "Not a dictionary"})
            continue
            
        if "id" not in item:
            invalid_items.append({"index": i, "reason": "Missing 'id' field"})
            continue
            
        if "value" not in item or not isinstance(item["value"], (int, float)):
            invalid_items.append({"index": i, "reason": "Invalid or missing 'value' field"})
            continue
            
        valid_items.append(item)
    
    result = {
        "valid_count": len(valid_items),
        "invalid_count": len(invalid_items),
        "valid_items": valid_items,
        "invalid_items": invalid_items,
        "validation_passed": len(invalid_items) == 0
    }
    
    if len(invalid_items) > len(valid_items):
        raise TaskError("Validation failed: more invalid items than valid", retryable=False)
    
    return result


async def run_worker():
    """Example of running an Azolla worker."""
    logger.info("=== Azolla Worker Example ===")
    
    # Method 1: Using the builder pattern (recommended)
    worker = (
        Worker.builder()
        .orchestrator("localhost:52710")
        .domain("examples") 
        .shepherd_group("python-examples")
        .max_concurrency(5)
        .heartbeat_interval(10.0)
        .register_task(send_notification)  # Register decorated task
        .register_task(ProcessDataTask())  # Register class-based task  
        .register_task(echo_task)  # Register simple function
        .register_task(validate_data)  # Register validation task
        .build()
    )
    
    logger.info(f"🚀 Starting worker with {worker.task_count()} registered tasks:")
    for task_name in worker._task_registry.names():
        logger.info(f"  - {task_name}")
    
    # Set up graceful shutdown
    shutdown_requested = False
    
    def shutdown_handler(signum, frame):
        nonlocal shutdown_requested
        if not shutdown_requested:
            logger.info("🛑 Received shutdown signal, stopping worker gracefully...")
            worker.shutdown()
            shutdown_requested = True
        else:
            logger.warning("⚠️  Force shutdown requested")
            exit(1)
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    try:
        # Start worker (this will run until shutdown)
        logger.info("👂 Worker is now listening for tasks...")
        await worker.start()
    except KeyboardInterrupt:
        logger.info("🔌 Worker interrupted by user")
    except Exception as e:
        logger.error(f"💥 Worker error: {e}")
        raise
    finally:
        logger.info("🏁 Worker stopped")


async def alternative_worker_setup():
    """Alternative ways to set up a worker."""
    logger.info("=== Alternative Worker Setup ===")
    
    # Method 1: Using WorkerConfig
    config = WorkerConfig(
        orchestrator_endpoint="localhost:52710",
        domain="examples",
        shepherd_group="python-alt-workers",
        max_concurrency=3,
        heartbeat_interval=15.0,
        reconnect_delay=2.0,
        max_reconnect_delay=60.0
    )
    
    worker = Worker(config)
    
    # Register tasks after creation
    worker.register_task(echo_task)
    worker.register_task(send_notification)
    
    logger.info(f"Alternative worker created with {worker.task_count()} tasks")
    
    # Method 2: Check if worker is ready (useful for testing)
    # Note: This would be used when actually starting the worker
    # is_ready = await worker.wait_for_ready(timeout=10.0)
    # logger.info(f"Worker ready: {is_ready}")
    
    logger.info("Alternative worker setup complete (not started)")


if __name__ == "__main__":
    print("Azolla Worker Example")
    print("====================")
    print()
    print("This example demonstrates how to create and run an Azolla worker.")
    print("The worker will:")
    print("1. Connect to the Azolla orchestrator on localhost:52710")
    print("2. Register several example tasks")
    print("3. Listen for and execute tasks")
    print("4. Handle graceful shutdown on SIGINT/SIGTERM")
    print()
    print("Press Ctrl+C to stop the worker gracefully")
    print()
    
    try:
        # Show alternative setup first
        asyncio.run(alternative_worker_setup())
        
        print("\n" + "="*50)
        print("Starting main worker...")
        print("="*50)
        
        # Run the main worker
        asyncio.run(run_worker())
        
    except KeyboardInterrupt:
        print("\n👋 Worker example interrupted by user")
    except Exception as e:
        print(f"\n💥 Worker example failed: {e}")
        import traceback
        traceback.print_exc()