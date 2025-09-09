"""Worker implementation for Azolla task execution."""
import asyncio
import json
import logging
import uuid
import time
from typing import Dict, List, Optional, Any, Union, Set
from datetime import datetime
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field

import grpc
from azolla._grpc import orchestrator_pb2_grpc, orchestrator_pb2, common_pb2
from azolla.task import Task
from azolla.types import TaskContext, TaskStatus
from azolla.exceptions import AzollaError, ConnectionError, WorkerError, TaskError

logger = logging.getLogger(__name__)

class WorkerConfig(BaseModel):
    """Configuration for the Azolla worker."""
    orchestrator_endpoint: str = Field(default="localhost:52710")
    domain: str = Field(default="default")
    shepherd_group: str = Field(default="python-workers")
    max_concurrency: int = Field(default=10, gt=0)
    heartbeat_interval: float = Field(default=30.0, gt=0)
    reconnect_delay: float = Field(default=1.0, gt=0)
    max_reconnect_delay: float = Field(default=60.0, gt=0)

class TaskRegistry:
    """Registry for task implementations."""
    
    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
    
    def register(self, task: Union[Task, Any]) -> None:
        """Register a task implementation."""
        if hasattr(task, '__azolla_task_instance__'):
            # Decorated function
            task_instance = task.__azolla_task_instance__
            name = task_instance.name()
            self._tasks[name] = task_instance
        elif isinstance(task, Task):
            # Task instance
            name = task.name()
            self._tasks[name] = task
        else:
            raise ValueError(f"Invalid task type: {type(task)}")
    
    def get(self, name: str) -> Optional[Task]:
        """Get task by name."""
        return self._tasks.get(name)
    
    def names(self) -> Set[str]:
        """Get all registered task names."""
        return set(self._tasks.keys())
    
    def count(self) -> int:
        """Get number of registered tasks."""
        return len(self._tasks)

class LoadTracker:
    """Thread-safe load tracking with RAII guard."""
    
    def __init__(self) -> None:
        self._current_load = 0
        self._lock = asyncio.Lock()
    
    async def get_load(self) -> int:
        """Get current load."""
        async with self._lock:
            return self._current_load
    
    @asynccontextmanager
    async def track_task(self):
        """Context manager for tracking task execution."""
        async with self._lock:
            self._current_load += 1
        
        try:
            yield
        finally:
            async with self._lock:
                self._current_load = max(0, self._current_load - 1)

class Worker:
    """Worker for executing Azolla tasks."""
    
    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self._task_registry = TaskRegistry()
        self._load_tracker = LoadTracker()
        self._shepherd_uuid = str(uuid.uuid4())
        self._shutdown_event = asyncio.Event()
        self._ready_event = asyncio.Event()  # Signals when worker is connected and ready
        self._running = False
        
        # gRPC components
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[orchestrator_pb2_grpc.ClusterServiceStub] = None
        
    @classmethod
    def builder() -> 'WorkerBuilder':
        """Create a worker builder."""
        return WorkerBuilder()
    
    def task_count(self) -> int:
        """Get number of registered tasks."""
        return self._task_registry.count()
    
    def register_task(self, task: Union[Task, Any]) -> None:
        """Register a task implementation after worker creation."""
        self._task_registry.register(task)
    
    async def wait_for_ready(self, timeout: Optional[float] = None) -> bool:
        """Wait for worker to be connected and ready to receive tasks.
        
        Returns:
            bool: True if worker becomes ready within timeout, False otherwise.
        """
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    async def start(self) -> None:
        """Start the worker with reconnection logic."""
        if self._running:
            raise WorkerError("Worker is already running")
        
        self._running = True
        self._shutdown_event.clear()
        
        logger.info(
            f"Worker starting with {self.task_count()} tasks on {self._config.orchestrator_endpoint} "
            f"(domain: {self._config.domain}, group: {self._config.shepherd_group})"
        )
        
        reconnect_delay = self._config.reconnect_delay
        
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._run_connection()
                logger.info("Worker connection terminated gracefully")
                break
                
            except Exception as e:
                if self._shutdown_event.is_set():
                    logger.info("Shutdown requested, stopping worker")
                    break
                
                logger.error(f"Worker connection failed: {e}")
                logger.info(f"Reconnecting in {reconnect_delay:.2f}s...")
                
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=reconnect_delay
                    )
                    break  # Shutdown requested during wait
                except asyncio.TimeoutError:
                    pass  # Continue to reconnect
                
                # Exponential backoff with jitter
                jitter = 1.0 + (reconnect_delay * 0.1 * (2 * asyncio.get_event_loop().time() % 1 - 1))
                reconnect_delay = min(
                    reconnect_delay * 1.5 * jitter,
                    self._config.max_reconnect_delay
                )
        
        self._running = False
    
    async def _run_connection(self) -> None:
        """Run a single connection to the orchestrator."""
        # Clear ready state at start of connection attempt
        self._ready_event.clear()
        
        # Create gRPC channel
        endpoint = self._config.orchestrator_endpoint
        self._channel = grpc.aio.insecure_channel(endpoint)
        self._stub = orchestrator_pb2_grpc.ClusterServiceStub(self._channel)
        
        try:
            # Create bidirectional stream
            request_queue = asyncio.Queue(maxsize=1000)
            
            # Start the stream
            response_stream = self._stub.Stream(self._request_generator(request_queue))
            
            # Send hello message
            hello_msg = orchestrator_pb2.ClientMsg(
                hello=orchestrator_pb2.Hello(
                    shepherd_uuid=self._shepherd_uuid,
                    max_concurrency=self._config.max_concurrency,
                    domain=self._config.domain,
                    shepherd_group=self._config.shepherd_group,
                )
            )
            await request_queue.put(hello_msg)
            
            logger.info(f"Shepherd {self._shepherd_uuid} registered successfully")
            
            # Signal that worker is ready to receive tasks
            self._ready_event.set()
            
            # Start heartbeat task
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(request_queue)
            )
            
            try:
                # Process messages
                await self._process_messages(response_stream, request_queue)
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
        
        finally:
            if self._channel:
                await self._channel.close()
                self._channel = None
                self._stub = None
    
    async def _request_generator(self, request_queue: asyncio.Queue):
        """Generate requests for the gRPC stream."""
        while not self._shutdown_event.is_set():
            try:
                request = await asyncio.wait_for(request_queue.get(), timeout=1.0)
                yield request
                request_queue.task_done()
            except asyncio.TimeoutError:
                continue
    
    async def _process_messages(
        self, 
        response_stream, 
        request_queue: asyncio.Queue
    ) -> None:
        """Process incoming messages from orchestrator."""
        async for message in response_stream:
            if self._shutdown_event.is_set():
                break
                
            if message.HasField("task"):
                await self._handle_task(message.task, request_queue)
            elif message.HasField("ping"):
                await self._handle_ping(message.ping, request_queue)
            else:
                logger.warning("Received message with unknown type")
    
    async def _handle_task(
        self, 
        proto_task: common_pb2.Task, 
        request_queue: asyncio.Queue
    ) -> None:
        """Handle incoming task from orchestrator."""
        logger.info(f"Received task: {proto_task.name} ({proto_task.task_id})")
        
        # Send acknowledgment
        ack_msg = orchestrator_pb2.ClientMsg(
            ack=orchestrator_pb2.Ack(task_id=proto_task.task_id)
        )
        await request_queue.put(ack_msg)
        
        # Execute task asynchronously
        asyncio.create_task(
            self._execute_task_wrapper(proto_task, request_queue)
        )
    
    async def _execute_task_wrapper(
        self, 
        proto_task: common_pb2.Task, 
        request_queue: asyncio.Queue
    ) -> None:
        """Wrapper for task execution with load tracking."""
        async with self._load_tracker.track_task():
            result = await self._execute_task(proto_task)
            
            # Send result
            result_msg = orchestrator_pb2.ClientMsg(task_result=result)
            await request_queue.put(result_msg)
    
    async def _execute_task(self, proto_task: common_pb2.Task) -> common_pb2.TaskResult:
        """Execute a task and return the result."""
        task_id = proto_task.task_id
        start_time = time.time()
        
        try:
            # Find task implementation
            task_impl = self._task_registry.get(proto_task.name)
            if task_impl is None:
                logger.error(f"No implementation found for task: {proto_task.name}")
                return common_pb2.TaskResult(
                    task_id=task_id,
                    error=common_pb2.ErrorResult(
                        type="TaskNotFound",
                        message=f"No implementation found for task: {proto_task.name}",
                        code="TASK_NOT_FOUND",
                    )
                )
            
            # Parse arguments
            try:
                if proto_task.args:
                    args = json.loads(proto_task.args)
                else:
                    args = []
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse args for task {task_id}: {e}")
                return common_pb2.TaskResult(
                    task_id=task_id,
                    error=common_pb2.ErrorResult(
                        type="ArgumentParseError",
                        message=f"Failed to parse task arguments: {e}",
                        code="ARG_PARSE_ERROR",
                    )
                )
            
            # Create task context
            context = TaskContext(
                task_id=task_id,
                attempt_number=1,  # TODO: Get from retry info
                max_attempts=None  # TODO: Get from retry policy
            )
            
            # Execute task
            result = await task_impl._execute_with_casting(args, context)
            execution_time = time.time() - start_time
            
            logger.info(f"Task {task_id} completed in {execution_time:.3f}s")
            
            # Serialize result
            result_json = json.dumps(result) if result is not None else "null"
            
            return common_pb2.TaskResult(
                task_id=task_id,
                success=common_pb2.SuccessResult(
                    result=common_pb2.AnyValue(json_value=result_json)
                )
            )
            
        except TaskError as e:
            execution_time = time.time() - start_time
            logger.error(f"Task {task_id} failed after {execution_time:.3f}s: {e}")
            
            return common_pb2.TaskResult(
                task_id=task_id,
                error=common_pb2.ErrorResult(
                    type=e.error_type,
                    message=e.message,
                    code=e.error_code,
                )
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Task {task_id} failed after {execution_time:.3f}s: {e}", exc_info=True)
            
            return common_pb2.TaskResult(
                task_id=task_id,
                error=common_pb2.ErrorResult(
                    type="UnexpectedError",
                    message=str(e),
                    code="UNEXPECTED_ERROR",
                )
            )
    
    async def _handle_ping(
        self, 
        ping: orchestrator_pb2.Ping, 
        request_queue: asyncio.Queue
    ) -> None:
        """Handle ping from orchestrator."""
        # Pings are handled automatically by gRPC layer
        # Could add custom ping handling here if needed
        pass
    
    async def _heartbeat_loop(self, request_queue: asyncio.Queue) -> None:
        """Send periodic status updates to orchestrator."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), 
                    timeout=self._config.heartbeat_interval
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                pass  # Continue with heartbeat
            
            # Send status update
            current_load = await self._load_tracker.get_load()
            available_capacity = max(0, self._config.max_concurrency - current_load)
            
            status_msg = orchestrator_pb2.ClientMsg(
                status=orchestrator_pb2.Status(
                    current_load=current_load,
                    available_capacity=available_capacity,
                )
            )
            
            try:
                await request_queue.put(status_msg)
            except Exception as e:
                logger.error(f"Failed to send status update: {e}")
                break
    
    def shutdown(self) -> None:
        """Signal shutdown to the worker."""
        logger.info("Shutdown requested")
        self._shutdown_event.set()
    
    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()

class WorkerBuilder:
    """Builder for worker configuration."""
    
    def __init__(self) -> None:
        self._config = WorkerConfig()
        self._tasks: List[Union[Task, Any]] = []
    
    def orchestrator(self, endpoint: str) -> 'WorkerBuilder':
        """Set orchestrator endpoint."""
        self._config.orchestrator_endpoint = endpoint
        return self
    
    def domain(self, domain: str) -> 'WorkerBuilder':
        """Set domain."""
        self._config.domain = domain
        return self
    
    def shepherd_group(self, group: str) -> 'WorkerBuilder':
        """Set shepherd group."""
        self._config.shepherd_group = group
        return self
    
    def max_concurrency(self, concurrency: int) -> 'WorkerBuilder':
        """Set max concurrency."""
        self._config.max_concurrency = concurrency
        return self
    
    def heartbeat_interval(self, interval: float) -> 'WorkerBuilder':
        """Set heartbeat interval in seconds."""
        self._config.heartbeat_interval = interval
        return self
    
    def register_task(self, task: Union[Task, Any]) -> 'WorkerBuilder':
        """Register a task implementation."""
        self._tasks.append(task)
        return self
    
    def build(self) -> Worker:
        """Build the worker."""
        worker = Worker(self._config)
        
        # Register all tasks
        for task in self._tasks:
            worker._task_registry.register(task)
        
        return worker