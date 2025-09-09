"""Client implementation for Azolla task submission."""
import asyncio
import json
from typing import Any, Dict, Optional, Union, Callable, Awaitable
from datetime import datetime
from pydantic import BaseModel, Field

import grpc
from azolla._grpc import orchestrator_pb2_grpc, orchestrator_pb2
from azolla.types import TaskResult, TaskStatus, TaskContext
from azolla.exceptions import AzollaError, ConnectionError, SerializationError
from azolla.retry import RetryPolicy

class ClientConfig(BaseModel):
    """Configuration for the Azolla client."""
    endpoint: str = Field(default="http://localhost:52710")
    domain: str = Field(default="default")
    timeout: float = Field(default=30.0, gt=0)
    max_message_size: int = Field(default=4 * 1024 * 1024)  # 4MB

class TaskSubmissionBuilder:
    """Builder for task submissions with retry policies."""
    
    def __init__(self, client: 'Client', task_name: str) -> None:
        self._client = client
        self._task_name = task_name
        self._args: Any = None
        self._retry_policy: Optional[RetryPolicy] = None
        self._shepherd_group: Optional[str] = None
        self._flow_instance_id: Optional[str] = None
    
    def args(self, args: Any) -> 'TaskSubmissionBuilder':
        """Set task arguments."""
        self._args = args
        return self
    
    def retry_policy(self, policy: RetryPolicy) -> 'TaskSubmissionBuilder':
        """Set retry policy for this task."""
        self._retry_policy = policy
        return self
    
    def with_retry(self, policy: RetryPolicy) -> 'TaskSubmissionBuilder':
        """Set retry policy for this task (alias for retry_policy)."""
        return self.retry_policy(policy)
    
    def shepherd_group(self, group: str) -> 'TaskSubmissionBuilder':
        """Set shepherd group for targeted execution."""
        self._shepherd_group = group
        return self
    
    def flow_instance_id(self, flow_id: str) -> 'TaskSubmissionBuilder':
        """Set flow instance ID if task is part of a flow."""
        self._flow_instance_id = flow_id
        return self
    
    async def submit(self) -> 'TaskHandle':
        """Submit the task and get a handle."""
        # Ensure connection is established
        await self._client._ensure_connection()
        
        try:
            # Serialize arguments
            if self._args is None:
                args_json = "[]"
            elif isinstance(self._args, (list, tuple)):
                args_json = json.dumps(list(self._args))
            elif isinstance(self._args, dict):
                args_json = json.dumps([self._args])
            else:
                args_json = json.dumps([self._args])
            
            # Serialize retry policy
            retry_policy_json = "{}"  # Default to empty JSON object
            if self._retry_policy:
                retry_policy_json = self._retry_policy.model_dump_json()
            
            # Create gRPC request
            request = orchestrator_pb2.CreateTaskRequest(
                name=self._task_name,
                domain=self._client._config.domain,
                retry_policy=retry_policy_json,
                args=args_json,
                kwargs="{}",  # Not used in Python client
                flow_instance_id=self._flow_instance_id,
                shepherd_group=self._shepherd_group,
            )
            
            # Submit task
            response = await self._client._stub.CreateTask(
                request,
                timeout=self._client._config.timeout
            )
            
            return TaskHandle(
                task_id=response.task_id,
                client=self._client
            )
            
        except grpc.RpcError as e:
            raise ConnectionError(f"Failed to submit task: {e.details()}") from e
        except (json.JSONDecodeError, TypeError) as e:
            raise SerializationError(f"Failed to serialize task arguments: {e}") from e

class TaskHandle:
    """Handle to a submitted task."""
    
    def __init__(self, task_id: str, client: 'Client') -> None:
        self.task_id = task_id
        self._client = client
    
    async def wait(self, timeout: Optional[float] = None) -> TaskResult[Any]:
        """Wait for task completion with exponential backoff polling."""
        start_time = datetime.now()
        poll_interval = 0.1  # Start with 100ms
        max_poll_interval = 5.0  # Max 5 seconds
        
        while True:
            # Check if we've exceeded timeout
            if timeout and (datetime.now() - start_time).total_seconds() > timeout:
                return TaskResult(
                    task_id=self.task_id,
                    status=TaskStatus.FAILED,
                    error="Task wait timeout exceeded",
                    error_code="TIMEOUT"
                )
            
            try:
                result = await self.try_result()
                if result is not None:
                    return result
                
                # Task still running, wait before next poll
                await asyncio.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, max_poll_interval)
                
            except Exception as e:
                return TaskResult(
                    task_id=self.task_id,
                    status=TaskStatus.FAILED,
                    error=str(e),
                    error_code="CLIENT_ERROR"
                )
    
    async def try_result(self) -> Optional[TaskResult[Any]]:
        """Try to get result without blocking."""
        # Ensure connection is established
        await self._client._ensure_connection()
        
        try:
            request = orchestrator_pb2.WaitForTaskRequest(
                task_id=self.task_id,
                domain=self._client._config.domain,
            )
            
            response = await self._client._stub.WaitForTask(
                request,
                timeout=self._client._config.timeout
            )
            
            if response.status.lower() == "completed":
                # Parse result
                result_value = None
                if response.result:
                    try:
                        result_value = json.loads(response.result)
                    except json.JSONDecodeError:
                        result_value = response.result
                
                return TaskResult(
                    task_id=self.task_id,
                    status=TaskStatus.COMPLETED,
                    value=result_value
                )
                
            elif response.status.lower() == "failed":
                error_msg = response.error or "Task execution failed"
                return TaskResult(
                    task_id=self.task_id,
                    status=TaskStatus.FAILED,
                    error=error_msg,
                    error_code="EXECUTION_ERROR"
                )
            
            elif response.status.lower() in ["pending", "running"]:
                return None  # Still in progress
            
            else:
                return TaskResult(
                    task_id=self.task_id,
                    status=TaskStatus.FAILED,
                    error=f"Unknown task status: {response.status}",
                    error_code="UNKNOWN_STATUS"
                )
                
        except grpc.RpcError as e:
            raise ConnectionError(f"Failed to get task result: {e.details()}") from e

class Client:
    """Main client for interacting with Azolla orchestrator."""
    
    def __init__(self, config: Optional[ClientConfig] = None, 
                 orchestrator_endpoint: Optional[str] = None,
                 domain: Optional[str] = None,
                 timeout: Optional[float] = None,
                 **kwargs) -> None:
        # Support both documented API and config-based API
        if config is not None:
            self._config = config
        elif orchestrator_endpoint is not None:
            # Create config from documented constructor parameters
            config_params = {"endpoint": orchestrator_endpoint}
            if domain is not None:
                config_params["domain"] = domain
            if timeout is not None:
                config_params["timeout"] = timeout
            config_params.update(kwargs)
            self._config = ClientConfig(**config_params)
        else:
            raise ValueError("Either 'config' or 'orchestrator_endpoint' must be provided")
        
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[orchestrator_pb2_grpc.ClientServiceStub] = None
    
    @classmethod
    async def connect(cls, endpoint: str, **kwargs) -> 'Client':
        """Connect to Azolla orchestrator with default config."""
        config = ClientConfig(endpoint=endpoint, **kwargs)
        client = cls(config)
        await client._ensure_connection()
        return client
    
    @staticmethod
    def builder() -> 'ClientBuilder':
        """Create a client builder."""
        return ClientBuilder()
    
    async def _ensure_connection(self) -> None:
        """Ensure gRPC connection is established."""
        if self._channel is None:
            # Parse endpoint to remove http:// prefix if present
            endpoint = self._config.endpoint
            if endpoint.startswith("http://"):
                endpoint = endpoint[7:]
            elif endpoint.startswith("https://"):
                endpoint = endpoint[8:]
            
            self._channel = grpc.aio.insecure_channel(
                endpoint,
                options=[
                    ('grpc.max_send_message_length', self._config.max_message_size),
                    ('grpc.max_receive_message_length', self._config.max_message_size),
                ]
            )
            self._stub = orchestrator_pb2_grpc.ClientServiceStub(self._channel)
    
    def submit_task(
        self, 
        task: Union[str, Callable[..., Awaitable[Any]]], 
        args: Any = None
    ) -> TaskSubmissionBuilder:
        """Submit a task for execution."""
        if isinstance(task, str):
            task_name = task
        elif hasattr(task, '__azolla_task_instance__'):
            # Decorated function
            task_name = task.__azolla_task_instance__.name()
        elif hasattr(task, 'name'):
            # Task instance
            task_name = task.name()
        else:
            # Try to get name from function
            task_name = getattr(task, '__name__', str(task))
        
        builder = TaskSubmissionBuilder(self, task_name)
        if args is not None:
            builder = builder.args(args)
        return builder
    
    async def close(self) -> None:
        """Close the gRPC connection."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None
    
    async def __aenter__(self) -> 'Client':
        """Async context manager entry."""
        await self._ensure_connection()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

class ClientBuilder:
    """Builder for client configuration."""
    
    def __init__(self) -> None:
        self._config = ClientConfig()
    
    def endpoint(self, endpoint: str) -> 'ClientBuilder':
        """Set orchestrator endpoint."""
        self._config.endpoint = endpoint
        return self
    
    def domain(self, domain: str) -> 'ClientBuilder':
        """Set domain."""
        self._config.domain = domain
        return self
    
    def timeout(self, timeout: float) -> 'ClientBuilder':
        """Set request timeout."""
        self._config.timeout = timeout
        return self
    
    def max_message_size(self, size: int) -> 'ClientBuilder':
        """Set maximum gRPC message size."""
        self._config.max_message_size = size
        return self
    
    async def build(self) -> Client:
        """Build and connect the client."""
        client = Client(self._config)
        await client._ensure_connection()
        return client