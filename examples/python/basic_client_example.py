"""Basic client example for Azolla Python library.

This example demonstrates how to use the Azolla client to submit tasks
to an orchestrator and wait for results.
"""
import asyncio
import logging
from typing import Dict, Any

from azolla import Client
from azolla.retry import RetryPolicy, ExponentialBackoff

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Example of using the Azolla client to submit tasks."""
    logger.info("=== Azolla Client Example ===")
    
    # Create client using the builder pattern
    async with Client.builder().endpoint("http://localhost:52710").build() as client:
        
        # Example 1: Submit a simple notification task
        logger.info("Submitting notification task...")
        handle1 = await (
            client.submit_task("send_notification")
            .args({
                "recipient": "user@example.com",
                "message": "Welcome to Azolla!",
                "priority": "high"
            })
            .submit()
        )
        
        # Example 2: Submit a data processing task with retry policy
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
            client.submit_task("process_data")
            .args({
                "data": sample_data,
                "operation": "sum",
                "options": {"debug": True}
            })
            .with_retry(retry_policy)
            .shepherd_group("data-workers")  # Target specific worker group
            .submit()
        )
        
        # Example 3: Submit task with flow instance ID (for workflow tracking)
        handle3 = await (
            client.submit_task("validate_data")
            .args({"data": sample_data})
            .flow_instance_id("workflow-123")
            .submit()
        )
        
        # Wait for results
        logger.info("Waiting for task results...")
        
        # Wait for notification task
        result1 = await handle1.wait(timeout=30.0)
        if result1.success:
            logger.info(f"✅ Notification result: {result1.value}")
        else:
            logger.error(f"❌ Notification failed: {result1.error} (code: {result1.error_code})")
        
        # Wait for data processing task
        result2 = await handle2.wait(timeout=30.0)
        if result2.success:
            logger.info(f"✅ Data processing result: {result2.value}")
        else:
            logger.error(f"❌ Data processing failed: {result2.error} (code: {result2.error_code})")
        
        # Try to get result without blocking (non-blocking check)
        result3 = await handle3.try_result()
        if result3:
            if result3.success:
                logger.info(f"✅ Validation result: {result3.value}")
            else:
                logger.error(f"❌ Validation failed: {result3.error}")
        else:
            logger.info("⏳ Validation task still running...")
            
            # Wait a bit more
            result3 = await handle3.wait(timeout=10.0)
            if result3.success:
                logger.info(f"✅ Validation result (after wait): {result3.value}")
            else:
                logger.error(f"❌ Validation failed: {result3.error}")


async def alternative_client_usage():
    """Alternative ways to create and use the client."""
    logger.info("=== Alternative Client Usage ===")
    
    # Method 1: Direct constructor with parameters (documented API)
    client1 = Client(
        orchestrator_endpoint="http://localhost:52710",
        domain="examples",
        timeout=60.0
    )
    
    # Method 2: Using ClientConfig
    from azolla import ClientConfig
    config = ClientConfig(
        endpoint="http://localhost:52710",
        domain="examples", 
        timeout=60.0,
        max_message_size=8 * 1024 * 1024  # 8MB
    )
    client2 = Client(config=config)
    
    # Method 3: Quick connect (class method)
    client3 = await Client.connect("http://localhost:52710", domain="examples")
    
    # Use any of the clients
    async with client1:
        handle = await client1.submit_task("echo").args(["Hello from client1!"]).submit()
        result = await handle.wait(timeout=10.0)
        logger.info(f"Client1 result: {result.value if result.success else result.error}")
    
    # Don't forget to close non-context-managed clients
    await client2.close()
    await client3.close()


if __name__ == "__main__":
    print("Azolla Client Example")
    print("====================")
    print()
    print("This example demonstrates how to submit tasks to an Azolla orchestrator.")
    print("Make sure you have:")
    print("1. An Azolla orchestrator running on localhost:52710")
    print("2. Workers registered to handle the submitted tasks")
    print()
    
    try:
        # Run the main example
        asyncio.run(main())
        
        print("\n" + "="*50)
        print("Running alternative usage examples...")
        
        # Run alternative examples
        asyncio.run(alternative_client_usage())
        
    except KeyboardInterrupt:
        print("\nExample interrupted by user")
    except Exception as e:
        print(f"\nExample failed: {e}")
        import traceback
        traceback.print_exc()