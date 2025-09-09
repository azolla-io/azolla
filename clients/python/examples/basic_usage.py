"""Basic usage example for Azolla Python client."""
import asyncio
import logging
from typing import Dict, Any

from azolla import azolla_task, Task, Client, Worker
from azolla.exceptions import TaskError
from azolla.retry import RetryPolicy, ExponentialBackoff

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
    logger.info(f"Sending {priority} priority notification to {recipient}: {message}")
    
    # Simulate some work
    await asyncio.sleep(0.1)
    
    # Simulate occasional failures for demonstration
    if "fail" in message.lower():
        raise TaskError("Notification service temporarily unavailable", retryable=True)
    
    return {
        "status": "sent",
        "recipient": recipient,
        "message_id": f"msg_{hash(recipient + message) % 10000}",
        "priority": priority
    }

# Example 2: Class-based task (explicit approach)
class ProcessDataTask(Task):
    """Process data with validation and transformation."""
    
    from pydantic import BaseModel, field_validator
    from typing import List, Optional
    
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
    
    async def execute(self, args: Args, context=None) -> Dict[str, Any]:
        """Execute the data processing task."""
        logger.info(f"Processing {len(args.data)} items with operation: {args.operation}")
        
        if args.operation == "sum":
            total = sum(item.get("value", 0) for item in args.data)
            return {"operation": "sum", "result": total, "count": len(args.data)}
        
        elif args.operation == "count":
            return {"operation": "count", "result": len(args.data)}
        
        elif args.operation == "filter":
            threshold = args.options.get("threshold", 0) if args.options else 0
            filtered = [item for item in args.data if item.get("value", 0) > threshold]
            return {"operation": "filter", "result": filtered, "count": len(filtered)}
        
        elif args.operation == "transform":
            multiplier = args.options.get("multiplier", 1) if args.options else 1
            transformed = [
                {**item, "value": item.get("value", 0) * multiplier} 
                for item in args.data
            ]
            return {"operation": "transform", "result": transformed, "count": len(transformed)}
        
        else:
            raise TaskError(f"Unsupported operation: {args.operation}")

async def client_example():
    """Example of using the Azolla client to submit tasks."""
    logger.info("=== Client Example ===")
    
    # Create client
    async with Client.builder().endpoint("http://localhost:52710").build() as client:
        
        # Submit a simple notification task
        logger.info("Submitting notification task...")
        handle1 = await (
            client.submit_task(send_notification, {
                "recipient": "user@example.com",
                "message": "Welcome to Azolla!",
                "priority": "high"
            }).submit()
        )
        
        # Submit a data processing task with retry policy
        logger.info("Submitting data processing task with retry policy...")
        retry_policy = RetryPolicy(
            max_attempts=3,
            backoff=ExponentialBackoff(initial=1.0, max_delay=30.0)
        )
        
        sample_data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20}, 
            {"id": 3, "value": 15}
        ]
        
        handle2 = await (
            client.submit_task("process_data")  # Using task name directly
            .args({
                "data": sample_data,
                "operation": "sum",
                "options": {"debug": True}
            })
            .retry_policy(retry_policy)
            .shepherd_group("data-workers")
            .submit()
        )
        
        # Wait for results
        logger.info("Waiting for task results...")
        
        result1 = await handle1.wait(timeout=30.0)
        if result1.success:
            logger.info(f"Notification result: {result1.value}")
        else:
            logger.error(f"Notification failed: {result1.error}")
        
        result2 = await handle2.wait(timeout=30.0)
        if result2.success:
            logger.info(f"Data processing result: {result2.value}")
        else:
            logger.error(f"Data processing failed: {result2.error}")

async def worker_example():
    """Example of running an Azolla worker."""
    logger.info("=== Worker Example ===")
    
    # Create and configure worker
    worker = (
        Worker.builder()
        .orchestrator("localhost:52710")
        .domain("examples")
        .shepherd_group("python-examples")
        .max_concurrency(5)
        .heartbeat_interval(10.0)
        .register_task(send_notification)  # Register decorated task
        .register_task(ProcessDataTask())  # Register explicit task
        .build()
    )
    
    logger.info(f"Starting worker with {worker.task_count()} registered tasks")
    
    # Set up graceful shutdown
    import signal
    
    def shutdown_handler(signum, frame):
        logger.info("Received shutdown signal")
        worker.shutdown()
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    try:
        # Start worker (this will run until shutdown)
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        logger.info("Worker stopped")

async def main():
    """Main example runner."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python basic_usage.py [client|worker]")
        print("  client - Run client example")
        print("  worker - Run worker example")
        return
    
    mode = sys.argv[1].lower()
    
    if mode == "client":
        await client_example()
    elif mode == "worker":
        await worker_example()
    else:
        print(f"Unknown mode: {mode}")
        print("Use 'client' or 'worker'")

if __name__ == "__main__":
    asyncio.run(main())