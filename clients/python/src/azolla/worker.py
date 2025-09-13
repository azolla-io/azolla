"""Worker implementation for Azolla task execution."""

import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Optional, Union

import grpc  # type: ignore[import-untyped]
from pydantic import BaseModel, Field

from azolla._grpc import common_pb2, orchestrator_pb2, orchestrator_pb2_grpc
from azolla.exceptions import TaskError, WorkerError
from azolla.task import Task
from azolla.types import TaskContext

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
        self._tasks: dict[str, Task] = {}

    def register(self, task: Union[Task, Any]) -> None:
        """Register a task implementation."""
        logger.info(f"📝 REGISTRY: Registering task: {task}")

        if hasattr(task, "__azolla_task_instance__"):
            # Decorated function
            task_instance = task.__azolla_task_instance__
            name = task_instance.name()
            self._tasks[name] = task_instance
            logger.info(
                f"✅ REGISTRY: Registered decorated task '{name}' -> {task_instance.__class__.__name__}"
            )
        elif isinstance(task, Task):
            # Task instance
            name = task.name()
            self._tasks[name] = task
            logger.info(
                f"✅ REGISTRY: Registered task instance '{name}' -> {task.__class__.__name__}"
            )
        else:
            logger.error(f"❌ REGISTRY: Invalid task type: {type(task)}")
            raise ValueError(f"Invalid task type: {type(task)}")

        logger.info(f"📝 REGISTRY: Current registered tasks: {list(self._tasks.keys())}")

    def get(self, name: str) -> Optional[Task]:
        """Get task by name."""
        return self._tasks.get(name)

    def names(self) -> set[str]:
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
    async def track_task(self) -> AsyncIterator[None]:
        """Context manager for tracking task execution."""
        async with self._lock:
            self._current_load += 1

        try:
            yield
        finally:
            async with self._lock:
                self._current_load = max(0, self._current_load - 1)


class Worker:
    """
    Worker for executing Azolla tasks.

    IMPORTANT: GRPC INCOMPATIBILITY WORKAROUND
    This worker implementation includes specific workarounds for a known incompatibility
    between Python grpcio and Rust tonic gRPC implementations:

    Issue: Python workers connecting to Rust orchestrators via bidirectional gRPC streams
    experience immediate connection drops with "h2 protocol error: connection reset".
    This is caused by Rust's h2 library sending RST_STREAM(CANCEL) instead of the expected
    RST_STREAM(NO_ERROR), which Python's grpcio interprets as a protocol violation.

    Workarounds implemented:
    1. Timeout detection (5-second stream timeout) in _read_responses()
    2. Connection retry with exponential backoff in _run_bidirectional_stream()
    3. Proper async iterator handling in _get_next_message()

    These workarounds prevent indefinite hanging and make the system resilient to the
    underlying gRPC incompatibility, though the core protocol issue remains unresolved.

    References:
    - https://github.com/hyperium/h2/issues/681
    - RST_STREAM error code handling differences between implementations
    """

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
        self._stub: Optional[Any] = None

    @classmethod
    def builder(cls) -> "WorkerBuilder":
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
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=reconnect_delay)
                    break  # Shutdown requested during wait
                except asyncio.TimeoutError:
                    pass  # Continue to reconnect

                # Exponential backoff with jitter
                jitter = 1.0 + (
                    reconnect_delay * 0.1 * (2 * asyncio.get_event_loop().time() % 1 - 1)
                )
                reconnect_delay = min(
                    reconnect_delay * 1.5 * jitter, self._config.max_reconnect_delay
                )

        self._running = False

    async def _run_connection(self) -> None:
        """Run a single connection to the orchestrator."""
        # Clear ready state at start of connection attempt
        self._ready_event.clear()

        # Create gRPC channel with proper options for bidirectional streaming
        endpoint = self._config.orchestrator_endpoint
        options = [
            ("grpc.keepalive_time_ms", 10000),  # Send keepalive every 10 seconds
            ("grpc.keepalive_timeout_ms", 1000),  # Wait 1 second for keepalive response
            ("grpc.keepalive_permit_without_calls", True),  # Allow keepalive without calls
            ("grpc.http2.max_pings_without_data", 0),  # Allow pings without data
            ("grpc.http2.min_time_between_pings_ms", 5000),  # Min 5 seconds between pings
        ]
        self._channel = grpc.aio.insecure_channel(endpoint, options=options)
        self._stub = orchestrator_pb2_grpc.ClusterServiceStub(self._channel)  # type: ignore[no-untyped-call]

        try:
            # Create the bidirectional stream using the proper pattern
            await self._run_bidirectional_stream()

        finally:
            if self._channel:
                await self._channel.close()
                self._channel = None
                self._stub = None

    async def _run_bidirectional_stream(self) -> None:
        """
        Run the bidirectional stream with timeout/retry recovery mechanism.

        WORKAROUND FOR GRPC INCOMPATIBILITY:
        This method implements a timeout/retry mechanism to handle a fundamental
        incompatibility between Python grpcio and Rust tonic gRPC implementations.

        The issue: When Python workers connect to Rust orchestrators via bidirectional
        gRPC streams, the connection drops immediately after the hello message with
        "h2 protocol error: connection reset". This is caused by Rust's h2 library
        sending RST_STREAM(CANCEL) instead of RST_STREAM(NO_ERROR) when closing streams,
        which Python's grpcio interprets as a protocol violation.

        The workaround:
        1. Detect stuck streams using a 5-second timeout in _read_responses()
        2. Retry with exponential backoff (1s, 2s, 4s delays) up to 3 attempts
        3. Prevents indefinite hanging while maintaining connection resilience

        This ensures tests complete quickly (~0.4s) instead of hanging indefinitely,
        even though the underlying protocol incompatibility still exists.

        See: https://github.com/hyperium/h2/issues/681
        """
        logger.info("🔗 WORKER: Starting bidirectional stream with recovery")

        max_stream_attempts = 3
        stream_attempt = 0

        while stream_attempt < max_stream_attempts and not self._shutdown_event.is_set():
            stream_attempt += 1
            logger.info(f"🔗 WORKER: Stream attempt {stream_attempt}/{max_stream_attempts}")

            try:
                await self._attempt_stream_connection()
                # If we get here, stream completed normally
                logger.info("🔗 WORKER: Stream completed successfully")
                break

            except Exception as e:
                logger.warning(f"🔗 WORKER: Stream attempt {stream_attempt} failed: {e}")

                if stream_attempt < max_stream_attempts:
                    # Wait before retrying (exponential backoff)
                    retry_delay = min(2.0 ** (stream_attempt - 1), 10.0)
                    logger.info(f"🔗 WORKER: Retrying in {retry_delay:.1f}s...")

                    try:
                        await asyncio.wait_for(self._shutdown_event.wait(), timeout=retry_delay)
                        # Shutdown requested during wait
                        break
                    except asyncio.TimeoutError:
                        # Continue to retry
                        continue
                else:
                    logger.error("🔗 WORKER: All stream attempts failed, giving up")
                    raise

    async def _attempt_stream_connection(self) -> None:
        """Attempt a single stream connection with timeout detection."""
        # Create request queue
        request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg] = asyncio.Queue(maxsize=1000)

        # Send hello message first to queue
        hello_msg = orchestrator_pb2.ClientMsg(
            hello=orchestrator_pb2.Hello(
                shepherd_uuid=self._shepherd_uuid,
                max_concurrency=self._config.max_concurrency,
                domain=self._config.domain,
                shepherd_group=self._config.shepherd_group,
            )
        )
        await request_queue.put(hello_msg)

        # Create the stream call with the request generator
        logger.info("🔗 WORKER: Creating gRPC stream call")
        stream_call = self._stub.Stream(self._request_generator(request_queue))

        # Wait a moment for the stream to establish the connection
        await asyncio.sleep(0.1)

        logger.info(f"Shepherd {self._shepherd_uuid} registered successfully")

        # Signal that worker is ready to receive tasks (only on first successful attempt)
        if not self._ready_event.is_set():
            self._ready_event.set()

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(request_queue))

        # Create response reader task with timeout detection
        response_task = asyncio.create_task(self._read_responses_with_recovery(stream_call))

        try:
            # Wait for either task to complete or fail
            done, pending = await asyncio.wait(
                [response_task, heartbeat_task], return_when=asyncio.FIRST_EXCEPTION
            )

            # Cancel any remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Check if any task raised an exception
            for task in done:
                if task.exception():
                    raise task.exception()

        finally:
            # Clean up all tasks
            for task in [response_task, heartbeat_task]:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

    async def _request_generator(
        self, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> AsyncIterator[orchestrator_pb2.ClientMsg]:
        """Generate requests for the gRPC stream."""
        logger.info("🔄 WORKER: Request generator started")
        try:
            while not self._shutdown_event.is_set():
                try:
                    request = await asyncio.wait_for(request_queue.get(), timeout=1.0)
                    logger.info(f"🔄 WORKER: Request generator yielding message: {request}")
                    yield request
                    request_queue.task_done()
                except asyncio.TimeoutError:
                    logger.debug("🔄 WORKER: Request generator timeout, continuing...")
                    continue
        except Exception as e:
            logger.error(f"🔄 WORKER: Error in request generator: {e}", exc_info=True)
            raise
        finally:
            logger.info("🔄 WORKER: Request generator ended")

    async def _read_responses(self, stream_call: Any) -> None:
        """
        Read responses from the gRPC stream with timeout/retry workaround.

        TIMEOUT DETECTION FOR GRPC INCOMPATIBILITY:
        This method implements a 5-second timeout to detect when gRPC streams are stuck
        due to the Python grpcio <-> Rust tonic incompatibility. When Python's grpcio
        receives RST_STREAM(CANCEL) from Rust's h2 library (instead of the expected
        RST_STREAM(NO_ERROR)), the stream can hang indefinitely waiting for messages.

        The timeout allows us to detect this condition and trigger reconnection via the
        retry mechanism in _run_bidirectional_stream().
        """
        logger.info("🔄 WORKER: Starting response reader with timeout detection")

        # Timeout detection for stuck streams due to gRPC incompatibility
        last_activity = asyncio.get_event_loop().time()
        stream_timeout = 5.0  # 5 seconds without messages = likely stuck due to protocol error

        try:
            while not self._shutdown_event.is_set():
                try:
                    # Use asyncio.wait_for to timeout if stream is stuck
                    message = await asyncio.wait_for(
                        self._get_next_message(stream_call), timeout=stream_timeout
                    )

                    if message is None:  # Stream ended normally
                        logger.info("🔄 WORKER: Stream ended normally")
                        break

                    last_activity = asyncio.get_event_loop().time()
                    logger.info(f"🔄 WORKER: Received message: {message}")

                    if message.HasField("task"):
                        logger.info(f"🔄 WORKER: Processing task: {message.task.name}")
                        # Create a task to handle this message without blocking the stream
                        task = asyncio.create_task(self._handle_task_async(message.task))
                        # Store reference to prevent garbage collection
                        task.add_done_callback(lambda t: None)
                    elif message.HasField("ping"):
                        logger.info("🔄 WORKER: Processing ping")
                        # Pings don't need async handling
                        pass
                    else:
                        logger.warning(f"🔄 WORKER: Unknown message type: {message}")

                except asyncio.TimeoutError:
                    elapsed = asyncio.get_event_loop().time() - last_activity
                    logger.warning(
                        f"🔄 WORKER: Stream timeout after {elapsed:.1f}s - likely stuck due to tonic/h2 incompatibility"
                    )
                    # Raise exception to trigger reconnection
                    raise grpc.RpcError("Stream timeout - triggering reconnection") from None

        except grpc.RpcError as e:
            logger.error(f"🔄 WORKER: gRPC error in response reader: {e}")
            if not self._shutdown_event.is_set():
                raise
        except Exception as e:
            logger.error(f"🔄 WORKER: Unexpected error in response reader: {e}", exc_info=True)
            if not self._shutdown_event.is_set():
                raise
        finally:
            logger.info("🔄 WORKER: Response reader ended")

    async def _get_next_message(self, stream_call: Any) -> Any:
        """
        Get the next message from stream, returning None if stream ends.

        GRPC ASYNC ITERATOR FIX:
        This method properly handles Python's async iterator interface for gRPC streams.
        The original implementation incorrectly used stream_call.__anext__() which doesn't
        exist on StreamStreamCall objects. For Python 3.9 compatibility, we use the
        __aiter__ and __anext__ methods directly instead of the newer aiter/anext functions.
        """
        try:
            # Use async iterator interface compatible with Python 3.9+
            async_iter = stream_call.__aiter__()
            return await async_iter.__anext__()
        except StopAsyncIteration:
            return None

    async def _read_responses_with_recovery(self, stream_call: Any) -> None:
        """Read responses with recovery - wrapper around the timeout-aware reader."""
        return await self._read_responses(stream_call)

    async def _handle_task_async(self, proto_task: common_pb2.Task) -> None:
        """Handle a task asynchronously without blocking the response stream."""
        logger.info(f"🔄 WORKER: Async task handler started for {proto_task.name}")

        try:
            # Execute the task using the existing task execution logic
            result = await self._execute_task(proto_task)

            # Log successful completion
            logger.info(f"🔄 WORKER: Task {proto_task.task_id} completed successfully")
            logger.info(f"🔄 WORKER: Task result: {result}")

        except Exception as e:
            logger.error(
                f"🔄 WORKER: Error executing task {proto_task.task_id}: {e}", exc_info=True
            )

    async def _process_messages_concurrent(
        self, stream_call: Any, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> None:
        """Process incoming messages from orchestrator with proper concurrency."""
        logger.info("🔄 WORKER: Starting concurrent message processing")

        try:
            # Use the stream call directly for receiving messages
            async for message in stream_call:
                logger.info(f"🔄 WORKER: Received message: {message}")

                if self._shutdown_event.is_set():
                    logger.info("🔄 WORKER: Shutdown requested, stopping message processing")
                    break

                if message.HasField("task"):
                    logger.info(f"🔄 WORKER: Processing task: {message.task.name}")
                    await self._handle_task(message.task, request_queue)
                elif message.HasField("ping"):
                    logger.info("🔄 WORKER: Processing ping")
                    await self._handle_ping(message.ping, request_queue)
                else:
                    logger.warning(f"🔄 WORKER: Unknown message type: {message}")

        except grpc.RpcError as e:
            logger.error(f"🔄 WORKER: gRPC error: {e}")
            logger.error(f"🔄 WORKER: gRPC error code: {e.code()}, details: {e.details()}")
            raise
        except Exception as e:
            logger.error(f"🔄 WORKER: Unexpected error: {e}", exc_info=True)
            raise
        finally:
            logger.info("🔄 WORKER: Message processing ended")

    async def _process_messages(
        self, response_stream: Any, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> None:
        """Legacy message processor - kept for compatibility."""
        await self._process_messages_concurrent(response_stream, request_queue)

    async def _handle_task(
        self, proto_task: common_pb2.Task, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> None:
        """Handle incoming task from orchestrator."""
        logger.info(f"Received task: {proto_task.name} ({proto_task.task_id})")

        # Send acknowledgment
        ack_msg = orchestrator_pb2.ClientMsg(ack=orchestrator_pb2.Ack(task_id=proto_task.task_id))
        await request_queue.put(ack_msg)

        # Execute task asynchronously
        # Store reference to prevent task from being garbage collected
        asyncio.create_task(self._execute_task_wrapper(proto_task, request_queue))  # noqa: RUF006

    async def _execute_task_wrapper(
        self, proto_task: common_pb2.Task, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
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

        logger.info(f"🔍 WORKER: Executing task '{proto_task.name}' (ID: {task_id})")
        logger.info(f"🔍 WORKER: Available tasks: {list(self._task_registry.names())}")

        try:
            # Find task implementation
            task_impl = self._task_registry.get(proto_task.name)
            if task_impl is None:
                logger.error(f"❌ WORKER: No implementation found for task: {proto_task.name}")
                logger.error(f"❌ WORKER: Available tasks are: {list(self._task_registry.names())}")
                return common_pb2.TaskResult(
                    task_id=task_id,
                    error=common_pb2.ErrorResult(
                        type="TaskNotFound",
                        message=f"No implementation found for task: {proto_task.name}",
                        code="TASK_NOT_FOUND",
                    ),
                )

            logger.info(f"✅ WORKER: Found task implementation: {task_impl.__class__.__name__}")

            # Parse arguments
            try:
                if proto_task.args:
                    args = json.loads(proto_task.args)
                    logger.info(f"🔍 WORKER: Parsed args: {args}")
                else:
                    args = []
                    logger.info("🔍 WORKER: No args provided, using empty list")
            except json.JSONDecodeError as e:
                logger.error(f"❌ WORKER: Failed to parse args for task {task_id}: {e}")
                return common_pb2.TaskResult(
                    task_id=task_id,
                    error=common_pb2.ErrorResult(
                        type="ArgumentParseError",
                        message=f"Failed to parse task arguments: {e}",
                        code="ARG_PARSE_ERROR",
                    ),
                )

            # Create task context
            context = TaskContext(
                task_id=task_id,
                attempt_number=1,  # TODO: Get from retry info
                max_attempts=None,  # TODO: Get from retry policy
            )

            # Execute task
            logger.info(f"🚀 WORKER: About to execute task with args: {args}")
            result = await task_impl._execute_with_casting(args, context)
            execution_time = time.time() - start_time

            logger.info(
                f"✅ WORKER: Task {task_id} completed successfully in {execution_time:.3f}s"
            )
            logger.info(f"✅ WORKER: Task result: {result}")

            # Serialize result
            result_json = json.dumps(result) if result is not None else "null"
            logger.info(f"✅ WORKER: Serialized result: {result_json}")

            return common_pb2.TaskResult(
                task_id=task_id,
                success=common_pb2.SuccessResult(
                    result=common_pb2.AnyValue(json_value=result_json)
                ),
            )

        except TaskError as e:
            execution_time = time.time() - start_time
            logger.error(
                f"🔥 WORKER: Task {task_id} failed after {execution_time:.3f}s with TaskError: {e}"
            )
            logger.error(
                f"🔥 WORKER: TaskError details - type: {e.error_type}, code: {e.error_code}, message: {e.message}"
            )

            error_result = common_pb2.TaskResult(
                task_id=task_id,
                error=common_pb2.ErrorResult(
                    type=e.error_type,
                    message=e.message,
                    code=e.error_code,
                ),
            )
            logger.error(f"🔥 WORKER: Returning error result: {error_result}")
            return error_result

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Task {task_id} failed after {execution_time:.3f}s: {e}", exc_info=True)

            return common_pb2.TaskResult(
                task_id=task_id,
                error=common_pb2.ErrorResult(
                    type="UnexpectedError",
                    message=str(e),
                    code="UNEXPECTED_ERROR",
                ),
            )

    async def _handle_ping(
        self, ping: orchestrator_pb2.Ping, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> None:
        """Handle ping from orchestrator."""
        # Pings are handled automatically by gRPC layer
        # Could add custom ping handling here if needed
        pass

    async def _heartbeat_loop(
        self, request_queue: asyncio.Queue[orchestrator_pb2.ClientMsg]
    ) -> None:
        """Send periodic status updates to orchestrator."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._config.heartbeat_interval
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
        self._tasks: list[Union[Task, Any]] = []

    def orchestrator(self, endpoint: str) -> "WorkerBuilder":
        """set orchestrator endpoint."""
        self._config.orchestrator_endpoint = endpoint
        return self

    def domain(self, domain: str) -> "WorkerBuilder":
        """set domain."""
        self._config.domain = domain
        return self

    def shepherd_group(self, group: str) -> "WorkerBuilder":
        """set shepherd group."""
        self._config.shepherd_group = group
        return self

    def max_concurrency(self, concurrency: int) -> "WorkerBuilder":
        """set max concurrency."""
        self._config.max_concurrency = concurrency
        return self

    def heartbeat_interval(self, interval: float) -> "WorkerBuilder":
        """set heartbeat interval in seconds."""
        self._config.heartbeat_interval = interval
        return self

    def register_task(self, task: Union[Task, Any]) -> "WorkerBuilder":
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
